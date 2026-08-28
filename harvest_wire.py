#!/usr/bin/env python3
"""Harvest the Live Drug Underworld Map wire.

Reads feeds.json, pulls every source in parallel, applies a three-part subject gate,
geo-tags each surviving item, de-duplicates, and writes wire.json.

The output schema is the one the map reads:

    [{"title": str,      # headline
      "link":  str,      # canonical URL
      "date":  str,      # ISO 8601 UTC
      "source": str,     # publication
      "iso":   str,      # ISO3, "" when nothing matched
      "country": str,    # display name for the iso
      "region": str,     # continent-level bucket
      "subregion": str,  # UN subregion  (also written as "subs" — the page reads that)
      "snippet": str,    # first ~240 chars of the summary
      "lang":  str,      # feed language hint
      "sig":   int,      # signal strength, 0-20
      "why":   str,      # which gate path passed it, for auditing
      "v":     2},       # gate version; the page trusts v>=2 and re-filters older archives
     ...]

Nothing here decides what is true. It decides what is worth a human look.

Two faults in the first build are fixed here.

VOLUME. Google News ANDs every bare word in a query, so "drug cartel corruption
official arrested" demanded all five words in one story and returned almost nothing.
Queries now come from feeds.json as parenthesised OR groups, and they run against
Google News country editions rather than four language settings, which also gives each
item a default jurisdiction when the text does not name one.

SUBJECT. The old gate asked for a drug word plus an institution word, and its
institution list held bare enforcement verbs — seized, raid, arrested. A routine
seizure passed it. Matching was by substring, so "port" matched "report" and "general"
matched "general election". This map is about institutions being captured, so an item
now has to carry an illicit-economy ANCHOR, an institutional ACTOR, and a CAPTURE
predicate saying the actor is implicated. Enforcement verbs alone are not a capture
predicate: they qualify only next to a role that can be a defendant (officer, judge,
minister, banker), never next to a bare enforcing body (police, authorities, court).
"""

import json, re, html, hashlib, sys, time, socket, random
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus, urlparse
from concurrent.futures import ThreadPoolExecutor

try:
    import feedparser
except ImportError:
    sys.exit("pip install feedparser")

MAX_AGE_DAYS = 45
MAX_ITEMS    = 1500
WORKERS      = 8
UA = "capture-wire/2.0 (+https://welcometoyourgalaxy.com)"
socket.setdefaulttimeout(30)

# ----------------------------------------------------------------- matching
# Every list below is matched on word boundaries, not substrings. Non-Latin terms fall
# back to substring because the boundary classes do not apply to those scripts.
_LAT = "a-zà-öø-ÿ"

def _rx(terms):
    """One compiled alternation per list.

    Latin terms take a leading word boundary. Terms of seven characters or more also
    allow up to three trailing letters, so drogenhandel matches drogenhandels and
    prosecutor matches prosecutors; short terms keep both boundaries, so port does not
    match report and meth does not match method. Non-Latin scripts fall back to plain
    substring, the boundary classes not applying to them.
    """
    strict, loose, other = [], [], []
    for t in terms:
        t = t.lower()
        if not re.search(r"[a-zà-öø-ÿ]", t):
            other.append(re.escape(t))
        elif len(t) >= 7:
            loose.append(re.escape(t))
        else:
            strict.append(re.escape(t))
    parts = []
    if strict:
        parts.append(r"(?<![%s])(?:%s)(?![%s])"
                     % (_LAT, "|".join(sorted(strict, key=len, reverse=True)), _LAT))
    if loose:
        parts.append(r"(?<![%s])(?:%s)(?![%s]{4,})"
                     % (_LAT, "|".join(sorted(loose, key=len, reverse=True)), _LAT))
    if other:
        parts.append(r"(?:%s)" % "|".join(sorted(other, key=len, reverse=True)))
    return re.compile("|".join(parts), re.I | re.U)


# --- 1. the illicit economy itself -----------------------------------------
ANCHOR = [
    "cartel", "cartels", "narco", "narcos", "narcotics", "narcotic", "trafficking",
    "trafficker", "traffickers", "cocaine", "heroin", "fentanyl", "methamphetamine",
    "meth", "captagon", "opium", "opioid", "precursor", "precursors", "drug ring",
    "drug network", "drug gang", "drug trade", "drug lord", "drug money", "drug proceeds",
    "money laundering", "laundering", "laundered", "organised crime", "organized crime",
    "mafia", "mob", "kingpin", "smuggling ring", "criminal network", "criminal proceeds",
    "cártel", "cartel de", "narcotráfico", "narcotrafico", "tráfico de drogas",
    "tráfico de droga", "estupefacientes", "blanqueo", "lavado de dinero",
    "lavado de activos", "cocaína", "cocaina", "crimen organizado", "narcomenudeo",
    "lavagem de dinheiro", "tráfico de drogas", "crime organizado", "facção", "milícia",
    "trafic de drogue", "trafic de stupéfiants", "narcotrafic", "blanchiment",
    "stupéfiants", "cocaïne", "crime organisé",
    "drogenhandel", "geldwäsche", "kokain", "drogengeld", "organisierte kriminalität",
    "traffico di droga", "narcotraffico", "riciclaggio", "cocaina", "ndrangheta",
    "camorra", "cosa nostra",
    "drugshandel", "witwassen", "drugsgeld", "georganiseerde misdaad", "ondermijning",
    "наркотик", "наркотраф", "наркоторговл", "отмывание", "кокаин", "наркодоход",
    "наркотрафік", "наркоторгівл", "відмивання",
    "مخدرات", "كبتاجون", "تهريب", "غسل الأموال", "كوكايين",
    "uyuşturucu", "kaçakçılık", "kara para", "kokain",
    "narkoba", "sabu", "pencucian uang", "peredaran narkoba",
    "dawa za kulevya", "utakatishaji",
    "ναρκωτικ", "ξέπλυμα", "narkotyk", "pranie pieniędzy", "droguri", "spălare de bani",
    "дрог", "прање новца", "narkotika", "penningtvätt", "ยาเสพติด", "ฟอกเงิน",
    "ma túy", "rửa tiền", "סמים", "הלבנת הון",
    # the people, not only the trade. A headline often names only the dealer.
    "drug dealer", "drug smuggler", "narcotrafficker", "narcotraficante", "traficante",
    "narcotrafiquant", "trafiquant de drogue", "drogenhändler", "drogenhandel",
    "drogenring", "rauschgift", "spacciatore", "trafficante", "drugsbende",
    "drugsdealer", "drugscriminaliteit", "наркоторгов", "наркобарон", "наркогруппиров",
    "наркоділ", "تاجر مخدرات", "شبكة تهريب", "uyuşturucu satıcı", "uyuşturucu çetesi",
    "pengedar narkoba", "bandar narkoba", "jaringan narkoba", "mfanyabiashara wa dawa",
    "narcotráfico", "narcoestado", "narco-state", "narco state",
]

# --- 2. who can be captured -------------------------------------------------
# ROLE: a person or firm that can sit in the dock. A judicial outcome next to one of
# these is itself evidence of capture.
ROLE = [
    "official", "officials", "officer", "officers", "agent", "agents", "inspector",
    "commissioner", "constable", "sergeant", "detective", "customs officer",
    "border guard", "immigration officer", "prison guard", "warden", "soldier",
    "colonel", "general", "commander", "admiral", "captain", "lieutenant",
    "judge", "magistrate", "prosecutor", "clerk of court", "notary", "lawyer",
    "attorney", "mayor", "governor", "senator", "congressman", "lawmaker",
    "legislator", "councillor", "councilman", "minister", "deputy minister",
    "president", "vice president", "diplomat", "ambassador", "spy chief",
    "banker", "executive", "chief executive", "chairman", "board member",
    "accountant", "auditor", "regulator", "port official", "harbourmaster",
    "police officer", "policeman", "ex-police", "former officer", "former official",
    "funcionario", "funcionarios", "agente", "comisario", "oficial", "expolicía",
    "policial", "delegado", "juez", "jueza", "fiscal", "magistrado", "alcalde",
    "gobernador", "senador", "diputado", "ministro", "coronel", "aduanero",
    "escribano", "empresario", "banquero", "servidor público",
    "funcionário", "promotor", "prefeito", "vereador", "deputado", "policial civil",
    "policier", "gendarme", "douanier", "magistrat", "procureur", "juge", "maire",
    "ministre", "député", "banquier", "fonctionnaire", "élu",
    "beamter", "beamten", "polizist", "zollbeamter", "richter", "staatsanwalt",
    "bürgermeister", "abgeordneter", "bankier",
    "funzionario", "poliziotto", "carabiniere", "doganiere", "giudice", "magistrato",
    "sindaco", "deputato", "banchiere",
    "ambtenaar", "politieman", "douanier", "rechter", "burgemeester", "wethouder",
    "чиновник", "полицейск", "таможенник", "судья", "прокурор", "депутат", "генерал",
    "посадовец", "митник", "суддя",
    "ضابط", "مسؤول", "قاض", "نائب", "وزير", "جمركي",
    "memur", "polis memuru", "hakim", "savcı", "belediye başkanı", "milletvekili",
    "pejabat", "oknum", "jaksa", "hakim", "bupati", "kapolres", "anggota dprd",
    "afisa", "mbunge", "jaji", "diwani",
]
# ORG: an institution or firm. Capture shows up as an institution being fined,
# sanctioned, infiltrated or used to launder, not as it arresting someone.
ORG = [
    "police", "police force", "police department", "police unit", "military", "army", "navy",
    "air force", "coast guard", "national guard", "gendarmerie", "customs", "customs service",
    "border force", "immigration service", "intelligence service", "security service",
    "prison", "penitentiary", "judiciary", "attorney general's office", "ministry",
    "government", "municipality", "city hall", "parliament", "congress", "senate",
    "ruling party", "campaign", "bank", "banks", "lender", "financial institution",
    "credit union", "money transfer", "remittance", "casino", "junket", "exchange",
    "brokerage", "law firm", "accounting firm", "shell company", "company", "firm",
    "corporation", "conglomerate", "port", "port authority", "airport", "terminal",
    "shipping line", "freight forwarder", "airline", "logistics",
    "policía", "ejército", "armada", "aduana", "aduanas", "fuerza pública", "gobierno",
    "municipio", "banco", "empresa", "compañía", "puerto", "aeropuerto", "casino",
    "fiscalía", "guardia nacional", "penal",
    "polícia", "alfândega", "prefeitura", "banco", "empresa", "porto", "aeroporto",
    "police nationale", "douane", "mairie", "banque", "entreprise", "société", "port",
    "aéroport", "gendarmerie",
    "polizei", "zoll", "behörde", "bank", "unternehmen", "hafen", "flughafen",
    "polizia", "guardia di finanza", "dogana", "comune", "banca", "azienda", "porto",
    "politie", "douane", "gemeente", "bank", "bedrijf", "haven",
    "полиция", "таможня", "прокуратура", "банк", "компания", "порт", "правительство",
    "поліція", "митниця", "банк",
    "شرطة", "جمارك", "بنك", "مصرف", "شركة", "ميناء", "حكومة",
    "polis", "gümrük", "banka", "şirket", "liman", "emniyet",
    "polisi", "bea cukai", "kepolisian", "bank", "perusahaan", "pelabuhan", "kejaksaan",
    "polisi", "forodha", "benki", "kampuni", "bandari",
]
# Bodies that normally appear as the enforcer. On their own they never satisfy the
# defendant test — "police arrested a trafficker" is not a capture story.
ENFORCER_ONLY = _rx([
    "police", "authorities", "prosecutors", "investigators", "court", "courts",
    "task force", "agency", "dea", "fbi", "interpol", "europol", "guardia civil",
    "policía", "autoridades", "fiscales", "tribunal", "juzgado", "polícia", "polizia",
    "politie", "polizei", "police", "полиция", "поліція", "الشرطة", "polis", "polisi",
])

# --- 3. the capture predicate ----------------------------------------------
# STRONG: says on its own that an institution or its people are implicated.
CAPTURE_STRONG = [
    "bribe", "bribes", "bribed", "bribery", "kickback", "kickbacks", "corrupt",
    "corruption", "collusion", "collude", "colluded", "complicity", "complicit",
    "on the payroll", "payroll", "protection racket", "protection money", "tipped off",
    "leaked information", "sold information", "infiltrate", "infiltrated",
    "infiltration", "state capture", "captured by", "embezzle", "embezzlement",
    "misconduct", "abuse of office", "abuse of power", "cover-up", "obstruction",
    "conflict of interest", "shell company", "front company", "money laundering",
    "laundering", "laundered", "sanctioned", "designated", "blacklisted", "fined",
    "penalty", "forfeiture", "deferred prosecution", "impeached", "graft",
    "soborno", "sobornos", "cohecho", "corrupción", "corrupto", "complicidad",
    "coima", "mordida", "encubrimiento", "peculado", "enriquecimiento ilícito",
    "lavado", "blanqueo", "sancionado", "multa",
    "suborno", "propina", "corrupção", "conivência", "lavagem", 
    "corruption", "pot-de-vin", "pots-de-vin", "complicité", "blanchiment",
    "prise illégale", "trafic d'influence", "amende", "sanctionné",
    "korruption", "bestechung", "beihilfe", "geldwäsche", "unterwandert", "verstrickt",
    "corruzione", "tangente", "tangenti", "concussione", "complicità", "riciclaggio",
    "infiltrazione", "collusione",     "corruptie", "omkoping", "medeplichtig", "witwassen", "ondermijning", "banden met",
    "коррупц", "взятк", "крышевание", "пособничество", "отмывание", 
    "корупц", "хабар", "відмивання",
    "فساد", "رشوة", "تواطؤ", "غسل", "اختراق", 
    "rüşvet", "yolsuzluk", "aklama", "iş birliği", 
    "suap", "korupsi", "gratifikasi", "bekingan", "pencucian uang", "terlibat",
    "rushwa", "ufisadi", "utakatishaji", "kuhusika",
    "διαφθορά", "δωροδοκία", "korupcja", "łapówka", "corupție", "mită",
    "корупција", "мито", "korruption", "muta", "ทุจริต", "สินบน",
    "tham nhũng", "hối lộ", "שוחד", "שחיתות",
]
# WEAK: a judicial or disciplinary outcome. Counts only within 70 characters of a ROLE,
# so "former customs officer sentenced" passes and "police seized two tonnes" does not.
CAPTURE_WEAK = [
    "ties to", "links to", "linked to", "connections to", "vínculos con", "nexos con",
    "vínculos com", "liens avec", "legami con", "banden met", "связи с", "صلات",
    "bağlantı", "convicted", "conviction", "indicted", "indictment", "sentenced", "jailed",
    "pleaded guilty", "guilty plea", "charged", "accused", "arrested", "detained",
    "suspended", "dismissed", "fired", "sacked", "removed from office", "resigned",
    "under investigation", "probe", "raided", "extradited", "on trial", "stands trial",
    "condenado", "condena", "detenido", "imputado", "procesado", "acusado",
    "destituido", "separado del cargo", "vinculado a proceso", "sentenciado",
    "condenado", "indiciado", "preso", "afastado", "denunciado",
    "condamné", "mis en examen", "écroué", "révoqué", "interpellé", "jugé",
    "verurteilt", "angeklagt", "festgenommen", "entlassen", "suspendiert",
    "condannato", "arrestato", "indagato", "rimosso", "sospeso",
    "veroordeeld", "aangehouden", "verdacht", "ontslagen", "geschorst",
    "осуждён", "осужден", "задержан", "обвинён", "уволен", "отстранён", "арестован",
    "засуджений", "затриманий", "звільнений",
    "إدانة", "توقيف", "اتهام", "إقالة", "محاكمة",
    "tutuklandı", "mahkum", "gözaltına", "görevden", "hakkında dava",
    "ditangkap", "divonis", "tersangka", "dipecat", "dinonaktifkan",
    "kukamatwa", "kuhukumiwa", "kushtakiwa", "kufukuzwa",
]

# --- 4. what never belongs here --------------------------------------------
VETO = [
    # fiction and entertainment. "Corrupción en Miami" is the Spanish title of Miami
    # Vice and reached the old wire tagged as a United States story.
    "corrupción en miami", "miami vice", "narcos season", "netflix", "hbo", "disney+",
    "prime video", "series", "season", "episode", "episodes", "trailer", "box office",
    "biopic", "spin-off", "streaming", "soundtrack", "starring", "cast of", "premiere",
    "serie", "series de", "temporada", "capítulo", "estreno", "película", "telenovela",
    "novela", "reparto", "banda sonora", "episódio", "filme", "film", "saison",
    "épisode", "bande-annonce", "staffel", "folge", "stagione", "puntata",
    "videojuego", "video game", "sinopsis", "review", "reseña",
    # appointments and career moves. "Head of FinCEN leaving to join Citibank" is a
    # personnel item, not an allegation.
    "leaving to join", "joins as", "appointed", "appointment of", "named as", "named to",
    "steps down", "stepping down", "to lead", "new chief executive", "takes over as",
    "sworn in", "nominated", "nomination", "confirmation hearing", "obituary",
    # medicine and pharma, which share the word "drug"
    "clinical trial", "drug trial", "fda approves", "drug shortage", "generic drug",
    "drugmaker", "pharma", "prescription", "weight loss drug", "cancer drug",
    "drug store", "drugstore", "pharmacy", "opioid settlement", "overdose prevention",
    "harm reduction", "rehab centre", "rehab center", "addiction treatment",
    # frauds with no drug nexus, which the old blanket DOJ feed poured in
    "health care fraud", "healthcare fraud", "medicare", "medicaid", "excessive force",
    "civil rights violation", "ponzi", "insider trading", "identity theft", "tax fraud",
    "unemployment fraud", "ppp loan", "child support", "counterfeit airbags",
    "bank robbery", "carjacking", "romance scam", "elder fraud",
    # sport, lifestyle, listicles
    "football", "soccer", "premier league", "nba", "nfl", "cricket score", "recipe",
    "horoscope", "fashion week", "transfer window", "how to watch", "coupon",
    "discount code", "stock forecast", "price prediction", "casino bonus", "gift guide",
    "best of", "what to know", "live updates", "explainer", "opinion:", "editorial:",
]

# --- 5. scoring -------------------------------------------------------------
# Weights only order the feed; the gate above decides membership. Scoring counts
# categories rather than a table of English words, because the first build scored a
# Russian or Indonesian item at zero and dropped it after it had passed the gate.
HIGH_OFFICE = [
    "minister", "president", "governor", "senator", "lawmaker", "judge", "magistrate",
    "general", "colonel", "admiral", "mayor", "police chief", "commissioner",
    "chief executive", "bank", "central bank", "port", "customs", "army", "navy",
    "intelligence", "ministry", "parliament", "congress", "supreme court",
    "ministro", "presidente", "gobernador", "senador", "diputado", "juez", "alcalde",
    "general", "aduana", "puerto", "banco", "fiscalía", "ejército",
    "ministre", "juge", "maire", "banque", "douane", "port", "armée",
    "minister", "richter", "bürgermeister", "bank", "zoll", "hafen",
    "ministro", "giudice", "sindaco", "banca", "dogana", "porto",
    "minister", "rechter", "burgemeester", "bank", "douane", "haven",
    "министр", "губернатор", "судья", "генерал", "мэр", "банк", "таможн", "депутат",
    "وزير", "قاض", "بنك", "جمارك", "نائب", "bakan", "hakim", "banka", "gümrük",
    "menteri", "bupati", "hakim", "bank", "waziri", "jaji", "benki",
]

MIN_SIG_STRONG  = 7   # anchor + actor + an explicit capture predicate
MIN_SIG_WEAK    = 8   # anchor + role + an outcome verb standing next to that role
MIN_SIG_TRUSTED = 5   # publications whose whole output is this subject

RX_ANCHOR   = _rx(ANCHOR)
RX_ROLE     = _rx(ROLE)
RX_ORG      = _rx(ORG)
RX_STRONG   = _rx(CAPTURE_STRONG)
RX_WEAK     = _rx(CAPTURE_WEAK)
RX_VETO     = _rx(VETO)
RX_HIGH     = _rx(HIGH_OFFICE)


def clean(t):
    t = html.unescape(t or "")
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _role_spans(text):
    """Role matches that are not merely part of a bare enforcing body.

    "police officer sentenced" carries a defendant-capable role; "police arrested
    three" carries only the force. Both contain the word police, so a role match is
    kept only when it is not wholly inside an ENFORCER_ONLY match.
    """
    bodies = [(m.start(), m.end()) for m in ENFORCER_ONLY.finditer(text)]
    out = []
    for m in RX_ROLE.finditer(text):
        if not any(a <= m.start() and m.end() <= b for a, b in bodies):
            out.append(m.start())
    return out


def gate(title, summary, trusted=False):
    """Return (sig, why). sig 0 means the item does not belong on this wire.

    Three parts, all required: an illicit-economy anchor, an institutional actor, and
    a predicate saying that actor is implicated. The weak path exists because a
    judicial outcome standing beside a role — "former customs officer sentenced" — is
    itself the allegation. It has to stand beside the role: an outcome verb next to a
    bare police force is a bust report, which is not what this map is about.
    """
    text = (title + " . " + summary).lower()
    if RX_VETO.search(text):
        return 0, "veto"
    if not RX_ANCHOR.search(text):
        return 0, "no anchor"

    roles = _role_spans(text)
    has_org = bool(RX_ORG.search(text))
    if not roles and not has_org and not trusted:
        return 0, "no actor"

    tl = title.lower()
    def n(rx, hay):
        return len({m.group(0).lower() for m in rx.finditer(hay)})
    s  = 3 * min(n(RX_ANCHOR, text), 2)        # the illicit economy
    s += 4 * min(n(RX_STRONG, text), 2)        # the capture predicate
    s += 2 if roles else 0
    s += 1 if has_org else 0
    s += 2 * min(n(RX_HIGH, text), 2)          # how high the institution sits
    s += 2 if RX_ANCHOR.search(tl) else 0      # in the headline, so it is the subject
    s += 2 if RX_STRONG.search(tl) else 0
    s += 3 if trusted else 0                   # the publication is the subject
    s  = min(s, 20)

    if RX_STRONG.search(text):
        return (s, "capture") if s >= MIN_SIG_STRONG else (0, "capture/low")
    if roles and any(abs(m.start() - p) <= 70 for m in RX_WEAK.finditer(text) for p in roles):
        return (s, "role+outcome") if s >= MIN_SIG_WEAK else (0, "outcome/low")
    if trusted and s >= MIN_SIG_TRUSTED and (RX_STRONG.search(text) or RX_WEAK.search(text)):
        return s, "trusted source"
    return 0, "no capture predicate"


# ------------------------------------------------------- geo tagging (ISO3 map)
COUNTRIES = {
 "AFG":("Afghanistan","Asia","Southern Asia"),"ALB":("Albania","Europe","Southern Europe"),
 "DZA":("Algeria","Africa","Northern Africa"),"AGO":("Angola","Africa","Middle Africa"),
 "ARG":("Argentina","Americas","South America"),"AUS":("Australia","Oceania","Australia and New Zealand"),
 "AUT":("Austria","Europe","Western Europe"),"AZE":("Azerbaijan","Asia","Western Asia"),
 "BHS":("Bahamas","Americas","Caribbean"),"BGD":("Bangladesh","Asia","Southern Asia"),
 "BLR":("Belarus","Europe","Eastern Europe"),"BEL":("Belgium","Europe","Western Europe"),
 "BLZ":("Belize","Americas","Central America"),"BEN":("Benin","Africa","Western Africa"),
 "BOL":("Bolivia","Americas","South America"),"BIH":("Bosnia and Herzegovina","Europe","Southern Europe"),
 "BRA":("Brazil","Americas","South America"),"BGR":("Bulgaria","Europe","Eastern Europe"),
 "BFA":("Burkina Faso","Africa","Western Africa"),"KHM":("Cambodia","Asia","South-Eastern Asia"),
 "CMR":("Cameroon","Africa","Middle Africa"),"CAN":("Canada","Americas","North America"),
 "CPV":("Cabo Verde","Africa","Western Africa"),"CAF":("Central African Republic","Africa","Middle Africa"),
 "TCD":("Chad","Africa","Middle Africa"),"CHL":("Chile","Americas","South America"),
 "CHN":("China","Asia","Eastern Asia"),"COL":("Colombia","Americas","South America"),
 "COD":("Congo, Dem. Rep.","Africa","Middle Africa"),"COG":("Congo, Rep.","Africa","Middle Africa"),
 "CRI":("Costa Rica","Americas","Central America"),"CIV":("Cote d'Ivoire","Africa","Western Africa"),
 "HRV":("Croatia","Europe","Southern Europe"),"CUB":("Cuba","Americas","Caribbean"),
 "CYP":("Cyprus","Asia","Western Asia"),"CZE":("Czech Republic","Europe","Eastern Europe"),
 "DNK":("Denmark","Europe","Northern Europe"),"DOM":("Dominican Republic","Americas","Caribbean"),
 "ECU":("Ecuador","Americas","South America"),"EGY":("Egypt","Africa","Northern Africa"),
 "SLV":("El Salvador","Americas","Central America"),"EST":("Estonia","Europe","Northern Europe"),
 "ETH":("Ethiopia","Africa","Eastern Africa"),"FIN":("Finland","Europe","Northern Europe"),
 "FRA":("France","Europe","Western Europe"),"GEO":("Georgia","Asia","Western Asia"),
 "DEU":("Germany","Europe","Western Europe"),"GHA":("Ghana","Africa","Western Africa"),
 "GRC":("Greece","Europe","Southern Europe"),"GTM":("Guatemala","Americas","Central America"),
 "GIN":("Guinea","Africa","Western Africa"),"GNB":("Guinea-Bissau","Africa","Western Africa"),
 "GUY":("Guyana","Americas","South America"),"HTI":("Haiti","Americas","Caribbean"),
 "HND":("Honduras","Americas","Central America"),"HUN":("Hungary","Europe","Eastern Europe"),
 "IND":("India","Asia","Southern Asia"),"IDN":("Indonesia","Asia","South-Eastern Asia"),
 "IRN":("Iran","Asia","Southern Asia"),"IRQ":("Iraq","Asia","Western Asia"),
 "IRL":("Ireland","Europe","Northern Europe"),"ISR":("Israel","Asia","Western Asia"),
 "ITA":("Italy","Europe","Southern Europe"),"JAM":("Jamaica","Americas","Caribbean"),
 "JPN":("Japan","Asia","Eastern Asia"),"JOR":("Jordan","Asia","Western Asia"),
 "KAZ":("Kazakhstan","Asia","Central Asia"),"KEN":("Kenya","Africa","Eastern Africa"),
 "KGZ":("Kyrgyzstan","Asia","Central Asia"),"LAO":("Laos","Asia","South-Eastern Asia"),
 "LVA":("Latvia","Europe","Northern Europe"),"LBN":("Lebanon","Asia","Western Asia"),
 "LBR":("Liberia","Africa","Western Africa"),"LBY":("Libya","Africa","Northern Africa"),
 "LTU":("Lithuania","Europe","Northern Europe"),"LUX":("Luxembourg","Europe","Western Europe"),
 "MDG":("Madagascar","Africa","Eastern Africa"),"MWI":("Malawi","Africa","Eastern Africa"),
 "MYS":("Malaysia","Asia","South-Eastern Asia"),"MLI":("Mali","Africa","Western Africa"),
 "MLT":("Malta","Europe","Southern Europe"),"MRT":("Mauritania","Africa","Western Africa"),
 "MUS":("Mauritius","Africa","Eastern Africa"),"MEX":("Mexico","Americas","Central America"),
 "MDA":("Moldova","Europe","Eastern Europe"),"MNG":("Mongolia","Asia","Eastern Asia"),
 "MNE":("Montenegro","Europe","Southern Europe"),"MAR":("Morocco","Africa","Northern Africa"),
 "MOZ":("Mozambique","Africa","Eastern Africa"),"MMR":("Myanmar","Asia","South-Eastern Asia"),
 "NAM":("Namibia","Africa","Southern Africa"),"NPL":("Nepal","Asia","Southern Asia"),
 "NLD":("Netherlands","Europe","Western Europe"),"NZL":("New Zealand","Oceania","Australia and New Zealand"),
 "NIC":("Nicaragua","Americas","Central America"),"NER":("Niger","Africa","Western Africa"),
 "NGA":("Nigeria","Africa","Western Africa"),"MKD":("North Macedonia","Europe","Southern Europe"),
 "NOR":("Norway","Europe","Northern Europe"),"OMN":("Oman","Asia","Western Asia"),
 "PAK":("Pakistan","Asia","Southern Asia"),"PAN":("Panama","Americas","Central America"),
 "PNG":("Papua New Guinea","Oceania","Melanesia"),"PRY":("Paraguay","Americas","South America"),
 "PER":("Peru","Americas","South America"),"PHL":("Philippines","Asia","South-Eastern Asia"),
 "POL":("Poland","Europe","Eastern Europe"),"PRT":("Portugal","Europe","Southern Europe"),
 "QAT":("Qatar","Asia","Western Asia"),"ROU":("Romania","Europe","Eastern Europe"),
 "RUS":("Russia","Europe","Eastern Europe"),"SAU":("Saudi Arabia","Asia","Western Asia"),
 "SEN":("Senegal","Africa","Western Africa"),"SRB":("Serbia","Europe","Southern Europe"),
 "SLE":("Sierra Leone","Africa","Western Africa"),"SGP":("Singapore","Asia","South-Eastern Asia"),
 "SVK":("Slovakia","Europe","Eastern Europe"),"SVN":("Slovenia","Europe","Southern Europe"),
 "SOM":("Somalia","Africa","Eastern Africa"),"ZAF":("South Africa","Africa","Southern Africa"),
 "KOR":("Korea, Rep.","Asia","Eastern Asia"),"PRK":("Korea, DPR","Asia","Eastern Asia"),
 "SSD":("South Sudan","Africa","Eastern Africa"),"ESP":("Spain","Europe","Southern Europe"),
 "LKA":("Sri Lanka","Asia","Southern Asia"),"SDN":("Sudan","Africa","Northern Africa"),
 "SUR":("Suriname","Americas","South America"),"SWE":("Sweden","Europe","Northern Europe"),
 "CHE":("Switzerland","Europe","Western Europe"),"SYR":("Syria","Asia","Western Asia"),
 "TJK":("Tajikistan","Asia","Central Asia"),"TZA":("Tanzania","Africa","Eastern Africa"),
 "THA":("Thailand","Asia","South-Eastern Asia"),"TGO":("Togo","Africa","Western Africa"),
 "TTO":("Trinidad and Tobago","Americas","Caribbean"),"TUN":("Tunisia","Africa","Northern Africa"),
 "TUR":("Turkiye","Asia","Western Asia"),"TKM":("Turkmenistan","Asia","Central Asia"),
 "UGA":("Uganda","Africa","Eastern Africa"),"UKR":("Ukraine","Europe","Eastern Europe"),
 "ARE":("United Arab Emirates","Asia","Western Asia"),"GBR":("United Kingdom","Europe","Northern Europe"),
 "USA":("United States","Americas","North America"),"URY":("Uruguay","Americas","South America"),
 "UZB":("Uzbekistan","Asia","Central Asia"),"VEN":("Venezuela","Americas","South America"),
 "VNM":("Vietnam","Asia","South-Eastern Asia"),"YEM":("Yemen","Asia","Western Asia"),
 "ZMB":("Zambia","Africa","Eastern Africa"),"ZWE":("Zimbabwe","Africa","Eastern Africa"),
}

# extra surface forms -> ISO3. Cities and demonyms carry a lot of the headlines.
ALIASES = {
 "u.s.":"USA","us ":"USA","united states":"USA","american":"USA","washington":"USA",
 "new york":"USA","texas":"USA","california":"USA","florida":"USA","dea":"USA",
 "new jersey":"USA","miami":"USA","chicago":"USA","los angeles":"USA","brooklyn":"USA",
 "manhattan":"USA","arizona":"USA","san diego":"USA","el paso":"USA","fincen":"USA",
 "justice department":"USA","southern district":"USA","eastern district":"USA","ofac ":"USA",
 "uk":"GBR","britain":"GBR","british":"GBR","london":"GBR","england":"GBR","scotland":"GBR",
 "mexican":"MEX","sinaloa":"MEX","jalisco":"MEX","tijuana":"MEX","juarez":"MEX","culiacan":"MEX",
 "colombian":"COL","bogota":"COL","medellin":"COL","cali":"COL",
 "brazilian":"BRA","sao paulo":"BRA","rio de janeiro":"BRA","pcc":"BRA",
 "ecuadorian":"ECU","guayaquil":"ECU","quito":"ECU",
 "peruvian":"PER","lima":"PER","bolivian":"BOL","la paz":"BOL",
 "venezuelan":"VEN","caracas":"VEN","paraguayan":"PRY","asuncion":"PRY",
 "argentine":"ARG","argentinian":"ARG","buenos aires":"ARG","rosario":"ARG",
 "chilean":"CHL","santiago":"CHL","uruguayan":"URY","montevideo":"URY",
 "honduran":"HND","tegucigalpa":"HND","guatemalan":"GTM","salvadoran":"SLV",
 "panamanian":"PAN","costa rican":"CRI","nicaraguan":"NIC","haitian":"HTI",
 "jamaican":"JAM","kingston":"JAM","dominican":"DOM","cuban":"CUB","havana":"CUB",
 "trinidad":"TTO","surinamese":"SUR","paramaribo":"SUR","guyanese":"GUY",
 "belgian":"BEL","antwerp":"BEL","brussels":"BEL",
 "dutch":"NLD","netherlands":"NLD","rotterdam":"NLD","amsterdam":"NLD",
 "spanish":"ESP","madrid":"ESP","barcelona":"ESP","galicia":"ESP","algeciras":"ESP",
 "french":"FRA","paris":"FRA","marseille":"FRA",
 "german":"DEU","berlin":"DEU","hamburg":"DEU","italian":"ITA","rome":"ITA",
 "sicily":"ITA","calabria":"ITA","ndrangheta":"ITA","camorra":"ITA","naples":"ITA",
 "albanian":"ALB","tirana":"ALB","serbian":"SRB","belgrade":"SRB",
 "montenegrin":"MNE","bulgarian":"BGR","romanian":"ROU","greek":"GRC","athens":"GRC",
 "turkish":"TUR","turkey":"TUR","istanbul":"TUR","ankara":"TUR",
 "swedish":"SWE","stockholm":"SWE","malmo":"SWE","danish":"DNK","copenhagen":"DNK",
 "norwegian":"NOR","oslo":"NOR","finnish":"FIN","irish":"IRL","dublin":"IRL",
 "portuguese":"PRT","lisbon":"PRT","swiss":"CHE","zurich":"CHE","geneva":"CHE",
 "austrian":"AUT","vienna":"AUT","polish":"POL","warsaw":"POL","czech":"CZE","prague":"CZE",
 "russian":"RUS","moscow":"RUS","ukrainian":"UKR","kyiv":"UKR","kiev":"UKR",
 "moroccan":"MAR","rabat":"MAR","casablanca":"MAR","tangier":"MAR","rif ":"MAR",
 "algerian":"DZA","tunisian":"TUN","libyan":"LBY","tripoli":"LBY","egyptian":"EGY","cairo":"EGY",
 "nigerian":"NGA","lagos":"NGA","abuja":"NGA","ghanaian":"GHA","accra":"GHA",
 "senegalese":"SEN","dakar":"SEN","malian":"MLI","bamako":"MLI","bissau":"GNB",
 "ivorian":"CIV","abidjan":"CIV","guinean":"GIN","conakry":"GIN",
 "kenyan":"KEN","nairobi":"KEN","mombasa":"KEN","tanzanian":"TZA","dar es salaam":"TZA",
 "ugandan":"UGA","kampala":"UGA","ethiopian":"ETH","somali":"SOM","mogadishu":"SOM",
 "south african":"ZAF","johannesburg":"ZAF","cape town":"ZAF","durban":"ZAF",
 "mozambican":"MOZ","maputo":"MOZ","zimbabwean":"ZWE","zambian":"ZMB",
 "afghan":"AFG","kabul":"AFG","kandahar":"AFG","helmand":"AFG","taliban":"AFG",
 "pakistani":"PAK","karachi":"PAK","islamabad":"PAK","quetta":"PAK",
 "indian":"IND","mumbai":"IND","delhi":"IND","punjab":"IND",
 "iranian":"IRN","tehran":"IRN","iraqi":"IRQ","baghdad":"IRQ","basra":"IRQ",
 "syrian":"SYR","damascus":"SYR","lebanese":"LBN","beirut":"LBN","hezbollah":"LBN",
 "jordanian":"JOR","amman":"JOR","saudi":"SAU","riyadh":"SAU","jeddah":"SAU",
 "emirati":"ARE","dubai":"ARE","abu dhabi":"ARE","qatari":"QAT","doha":"QAT",
 "israeli":"ISR","tel aviv":"ISR","yemeni":"YEM","omani":"OMN",
 "myanmar":"MMR","burmese":"MMR","yangon":"MMR","shan state":"MMR","wa state":"MMR",
 "thai":"THA","bangkok":"THA","laos":"LAO","vientiane":"LAO",
 "cambodian":"KHM","phnom penh":"KHM","sihanoukville":"KHM",
 "vietnamese":"VNM","hanoi":"VNM","ho chi minh":"VNM",
 "malaysian":"MYS","kuala lumpur":"MYS","singaporean":"SGP",
 "indonesian":"IDN","jakarta":"IDN","bali":"IDN",
 "filipino":"PHL","philippine":"PHL","manila":"PHL",
 "chinese":"CHN","beijing":"CHN","shanghai":"CHN","guangdong":"CHN","hong kong":"CHN",
 "japanese":"JPN","tokyo":"JPN","yakuza":"JPN",
 "south korean":"KOR","seoul":"KOR","north korean":"PRK","pyongyang":"PRK",
 "tajik":"TJK","dushanbe":"TJK","kyrgyz":"KGZ","bishkek":"KGZ",
 "kazakh":"KAZ","uzbek":"UZB","tashkent":"UZB","turkmen":"TKM",
 "australian":"AUS","sydney":"AUS","melbourne":"AUS","new zealand":"NZL","auckland":"NZL",
 "canadian":"CAN","toronto":"CAN","vancouver":"CAN","montreal":"CAN",
}

# Agency names imply a forum, not a scene. "OFAC sanctions a Dubai refiner" is a story
# about the UAE, so these lose to any real place name that also appears.
WEAK_ALIAS = {"dea", "fincen", "ofac ", "justice department", "southern district",
              "eastern district", "us ", "american"}


# Aliases are matched on word boundaries too. Under the old substring match "cali"
# scored inside "California" and "us " inside any word ending in us.
_ALIAS_RX = {a: _rx([a.strip()]) for a in ALIASES}
_NAME_RX  = {iso: _rx([n[0].lower().split(",")[0]]) for iso, n in COUNTRIES.items()
             if len(n[0].split(",")[0]) > 3}


def geotag(text, default_iso=""):
    """Longest match wins; a formal country name outranks a demonym or a city."""
    low = " " + text.lower() + " "
    best, best_len = "", 0
    for iso, rx in _NAME_RX.items():
        if rx.search(low):
            n = len(COUNTRIES[iso][0].split(",")[0]) + 5
            if n > best_len:
                best, best_len = iso, n
    for alias, rx in _ALIAS_RX.items():
        if not rx.search(low):
            continue
        score = len(alias.strip()) - (6 if alias in WEAK_ALIAS else 0)
        if score > best_len:
            best, best_len = ALIASES[alias], score
    return best or default_iso


def gnews(q, hl="en-US", gl="US", ceid="US:en"):
    return ("https://news.google.com/rss/search?q=%s&hl=%s&gl=%s&ceid=%s"
            % (quote_plus(q), hl, gl, ceid))


def fetch(url, tries=2):
    """feedparser with one retry. Google News throttles a burst of requests."""
    last = None
    for n in range(tries):
        try:
            d = feedparser.parse(url, agent=UA)
            if getattr(d, "entries", None):
                return d
            last = "empty"
        except Exception as e:
            last = str(e)[:90]
        time.sleep(1.5 + random.random() * 1.5)
    return last


def read_source(src):
    """One source -> (name, list of items, note). Runs on a worker thread."""
    name, url = src["name"], src["url"]
    lang, default_iso = src.get("lang", "en"), src.get("iso", "")
    trusted = bool(src.get("trusted"))
    d = fetch(url)
    if not hasattr(d, "entries"):
        return name, [], "unreachable (%s)" % d
    out, seen_gate = [], 0
    for e in d.entries:
        title = clean(getattr(e, "title", ""))
        link = getattr(e, "link", "") or ""
        if not title or not link:
            continue
        summary = clean(getattr(e, "summary", ""))[:240]
        source = name
        if src.get("kind") == "gnews":
            # Google News appends " - Publisher" to the headline; lift it out
            if getattr(e, "source", None) and getattr(e.source, "title", None):
                source = clean(e.source.title)
            else:
                m = re.search(r"\s+-\s+([^-]{2,40})$", title)
                if m:
                    source = m.group(1).strip()
                    title = title[: m.start()].strip()
        sig, why = gate(title, summary, trusted)
        if not sig:
            seen_gate += 1
            continue
        t = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        dt = (datetime.fromtimestamp(time.mktime(t), tz=timezone.utc)
              if t else datetime.now(timezone.utc))
        if datetime.now(timezone.utc) - dt > timedelta(days=MAX_AGE_DAYS):
            continue
        iso = geotag(title + " " + summary, default_iso)
        cname, region, sub = COUNTRIES.get(iso, ("", "", ""))
        out.append({
            "title": title, "link": link,
            "date": dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": source or urlparse(link).netloc,
            "iso": iso, "country": cname, "region": region,
            "subregion": sub, "subs": sub,
            "snippet": summary, "lang": lang, "sig": sig, "why": why, "v": 2,
        })
    return name, out, "%d kept, %d off subject" % (len(out), seen_gate)


def build_sources(cfg):
    """Flatten feeds.json into one list of fetchable sources."""
    qs, out = cfg.get("query_sets", {}), []
    for ed in cfg.get("editions", []):
        lang = ed.get("lang", "en")
        for i, q in enumerate(qs.get(lang, qs.get("en", []))):
            out.append({"name": "GN %s/%s #%d" % (ed["gl"], lang, i + 1),
                        "url": gnews(q, ed.get("hl", "en-US"), ed["gl"], ed["ceid"]),
                        "lang": lang, "iso": ed.get("iso", ""), "kind": "gnews"})
    for i, s in enumerate(cfg.get("searches", [])):
        lang = s.get("lang", "en")
        out.append({"name": "GN global #%d" % (i + 1),
                    "url": gnews(s["q"], s.get("hl", "en-US"), s.get("gl", "US"),
                                 s.get("ceid", "US:en")),
                    "lang": lang, "iso": s.get("iso", ""), "kind": "gnews"})
    for f in cfg.get("feeds", []):
        out.append(dict(f, kind="feed"))
    return out


def main():
    cfg = json.load(open("feeds.json"))
    sources = build_sources(cfg)
    print("reading %d sources with %d workers\n" % (len(sources), WORKERS))

    items, report = [], []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for name, got, note in ex.map(read_source, sources):
            items += got
            report.append((name, len(got), note))

    # de-duplicate on a normalised headline and on the bare URL, keeping the
    # highest-signal copy. The same story arrives from several country editions.
    seen = {}
    for it in items:
        u = urlparse(it["link"])
        norm = re.sub(r"\s+-\s+[^-]{2,40}$", "", it["title"]).lower()
        keys = [hashlib.sha1(re.sub(r"[^a-z0-9]+", " ", norm)[:110].encode()).hexdigest(),
                (u.netloc + u.path).lower()]
        prev = [k for k in keys if k in seen]
        if prev and seen[prev[0]]["sig"] >= it["sig"]:
            continue
        for k in keys:
            seen[k] = it
    uniq = {id(v): v for v in seen.values()}.values()
    out = sorted(uniq, key=lambda x: x["date"], reverse=True)[:MAX_ITEMS]

    json.dump(out, open("wire.json", "w"), ensure_ascii=False, indent=1)

    dead = [r for r in report if "unreachable" in r[2]]
    for name, n, note in sorted(report, key=lambda r: -r[1])[:25]:
        print("  %-22s %s" % (name, note))
    if dead:
        print("\nunreachable, prune or replace these in feeds.json:")
        for name, _n, note in dead:
            print("  %-22s %s" % (name, note))

    tagged = sum(1 for i in out if i["iso"])
    paths = {}
    for i in out:
        paths[i["why"]] = paths.get(i["why"], 0) + 1
    print("\n%d items -> wire.json  (%d geo-tagged, %d untagged, %d countries)"
          % (len(out), tagged, len(out) - tagged, len({i["iso"] for i in out if i["iso"]})))
    print("gate paths: " + ", ".join("%s %d" % kv for kv in sorted(paths.items())))
    if out:
        print("newest: %s  %s" % (out[0]["date"], out[0]["title"][:70]))


if __name__ == "__main__":
    main()
