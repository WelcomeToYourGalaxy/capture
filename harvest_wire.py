#!/usr/bin/env python3
"""Harvest the Capture wire.

Reads feeds.json, pulls every source, filters on subject keywords, geo-tags each item
to a jurisdiction and a region, de-duplicates, and writes wire.json.

The output schema is the one the map reads:

    [{"title": str,      # headline
      "link":  str,      # canonical URL
      "date":  str,      # ISO 8601 UTC
      "source": str,     # publication
      "iso":   str,      # ISO3, "" when nothing matched
      "country": str,    # display name for the iso
      "region": str,     # continent-level bucket
      "subregion": str,  # UN subregion
      "snippet": str,    # first ~240 chars of the summary
      "lang":  str,      # feed language hint
      "sig":   int},     # keyword signal strength, 0-20
     ...]

Nothing here decides what is true. It decides what is worth a human look.
"""

import json, re, html, hashlib, sys, time
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus, urlparse

try:
    import feedparser
except ImportError:
    sys.exit("pip install feedparser")

MAX_AGE_DAYS = 45
MAX_ITEMS    = 1200
UA = "capture-wire/1.0 (+https://welcometoyourgalaxy.com)"

# ---------------------------------------------------------------- subject gate
KEEP = {
    "cartel": 4, "narco": 4, "trafficking": 3, "traffickers": 3, "cocaine": 3,
    "heroin": 3, "fentanyl": 3, "methamphetamine": 3, "captagon": 4, "opium": 3,
    "precursor": 3, "money laundering": 4, "laundering": 3, "bribe": 3, "bribery": 3,
    "corruption": 2, "corrupt": 2, "smuggling": 2, "organised crime": 3,
    "organized crime": 3, "mafia": 3, "kingpin": 3, "extradited": 3, "extradition": 2,
    "indicted": 3, "indictment": 3, "convicted": 3, "sentenced": 2, "pleaded guilty": 3,
    "prosecutor": 2, "seizure": 2, "seized": 2, "customs": 2, "port": 1,
    "police officer": 3, "sanctioned": 2, "ofac": 3, "asset forfeiture": 3,
    "state capture": 4, "protection racket": 3, "junket": 3, "shell company": 2,
}
DROP = [
    "football", "soccer", "premier league", "nba", "nfl", "cricket score", "box office",
    "recipe", "horoscope", "celebrity", "fashion week", "transfer window", "how to watch",
    "coupon", "discount code", "stock forecast", "price prediction", "casino bonus",
    "review: ", "obituary", "weather forecast",
]
MIN_SIG = 3

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

WORD = re.compile(r"[a-z][a-z\.\s'-]+")


def clean(t):
    t = html.unescape(t or "")
    t = re.sub(r"<[^>]+>", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def signal(text):
    """Keyword strength. Returns 0 when the item is off-topic or explicitly excluded."""
    low = " " + text.lower() + " "
    for d in DROP:
        if d in low:
            return 0
    s = 0
    for k, w in KEEP.items():
        if k in low:
            s += w
    return min(s, 20)


def geotag(text, default_iso=""):
    """Longest-match wins, and an explicit country name beats a demonym or city."""
    low = " " + text.lower() + " "
    best, best_len = "", 0
    for iso, (name, _r, _s) in COUNTRIES.items():
        n = name.lower().split(",")[0]
        if len(n) > 3 and n in low and len(n) > best_len:
            best, best_len = iso, len(n) + 5     # bonus: formal name outranks an alias
    for alias, iso in ALIASES.items():
        if alias not in low:
            continue
        score = len(alias) - (6 if alias in WEAK_ALIAS else 0)
        if score > best_len:
            best, best_len = iso, score
    return best or default_iso


def gnews(q, lang="en"):
    hl = {"en": "en-US", "es": "es-419", "fr": "fr", "pt": "pt-BR"}.get(lang, "en-US")
    ceid = {"en": "US:en", "es": "US:es-419", "fr": "FR:fr", "pt": "BR:pt-419"}.get(lang, "US:en")
    gl = ceid.split(":")[0]
    return ("https://news.google.com/rss/search?q=%s&hl=%s&gl=%s&ceid=%s"
            % (quote_plus(q), hl, gl, ceid))


def parse(url, src_name, lang, default_iso=""):
    out = []
    try:
        d = feedparser.parse(url, agent=UA)
    except Exception as e:
        print("  ! %s: %s" % (src_name, e))
        return out
    for e in d.entries:
        title = clean(getattr(e, "title", ""))
        link = getattr(e, "link", "") or ""
        if not title or not link:
            continue
        summary = clean(getattr(e, "summary", ""))[:240]
        # Google News appends " - Publisher" to the headline; lift it out as the source
        src = src_name
        m = re.search(r"\s+-\s+([^-]{2,40})$", title)
        if src_name == "Google News":
            if getattr(e, "source", None) and getattr(e.source, "title", None):
                src = clean(e.source.title)
            elif m:
                src = m.group(1).strip()
                title = title[: m.start()].strip()
        blob = title + " " + summary
        sig = signal(blob)
        if sig < MIN_SIG:
            continue
        t = getattr(e, "published_parsed", None) or getattr(e, "updated_parsed", None)
        dt = (datetime.fromtimestamp(time.mktime(t), tz=timezone.utc)
              if t else datetime.now(timezone.utc))
        if datetime.now(timezone.utc) - dt > timedelta(days=MAX_AGE_DAYS):
            continue
        iso = geotag(blob, default_iso)
        name, region, sub = COUNTRIES.get(iso, ("", "", ""))
        out.append({
            "title": title, "link": link,
            "date": dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source": src or urlparse(link).netloc,
            "iso": iso, "country": name, "region": region, "subregion": sub,
            "snippet": summary, "lang": lang, "sig": sig,
        })
    return out


def main():
    cfg = json.load(open("feeds.json"))
    items = []
    for s in cfg.get("searches", []):
        lang = s.get("lang", "en")
        print("· search: %s" % s["q"])
        items += parse(gnews(s["q"], lang), "Google News", lang, s.get("iso", ""))
    for f in cfg.get("feeds", []):
        print("· feed:   %s" % f["name"])
        items += parse(f["url"], f["name"], f.get("lang", "en"), f.get("iso", ""))

    # de-duplicate on a normalised headline, keeping the highest-signal copy
    seen = {}
    for it in items:
        k = hashlib.sha1(re.sub(r"[^a-z0-9]+", " ",
                                it["title"].lower())[:110].encode()).hexdigest()
        if k not in seen or it["sig"] > seen[k]["sig"]:
            seen[k] = it
    out = sorted(seen.values(), key=lambda x: x["date"], reverse=True)[:MAX_ITEMS]

    json.dump(out, open("wire.json", "w"), ensure_ascii=False, indent=1)
    tagged = sum(1 for i in out if i["iso"])
    print("\n%d items -> wire.json  (%d geo-tagged, %d untagged)"
          % (len(out), tagged, len(out) - tagged))
    if out:
        print("newest: %s  %s" % (out[0]["date"], out[0]["title"][:70]))


if __name__ == "__main__":
    main()
