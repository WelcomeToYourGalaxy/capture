import sys
sys.path.insert(0, ".")
import harvest_wire as h

# (should_keep, title, summary[, trusted])
CASES = [
    # --- the eight items now in wire.json -----------------------------------
    (True,  "Funcionario judicial habría vendido datos a narcos: detenido por cohecho",
            "Funcionario judicial habría vendido datos a narcos: detenido por cohecho, violación de secreto"),
    (True,  "Zelenskyy Dismisses Senior Aide Suspected in $3.3 Million Money Laundering Case",
            "Ukrainian investigators say senior officials and lawmakers used a state-owned bank and shell firms to launder money"),
    (False, 'Acción policial en la lucha contra el narcotráfico en "Corrupción en Miami"',
            'Acción policial en la lucha contra el narcotráfico en "Corrupción en Miami" Canal Sur'),
    (True,  "Condenan a banda policial por uno de los mayores casos de corrupción y narcotráfico",
            "Condenan a banda policial por uno de los mayores casos de corrupción y narcotráfico en la Policía"),
    (True,  "Alleged Irish Crime Boss Charged After UAE Extradition",
            "The alleged leader of a major Irish organized crime group was extradited from Dubai and charged", True),
    # the same item from an ordinary newspaper names no captured institution
    (False, "Alleged Irish Crime Boss Charged After UAE Extradition",
            "The alleged leader of a major Irish organized crime group was extradited from Dubai and charged"),
    (True,  "Man linked to Toronto police corruption probe has global drug-trafficking ties",
            "Man linked to Toronto police corruption probe has global drug-trafficking ties, court documents show"),
    (False, "Head of FinCEN Leaving to Join Citibank as Global Head of Sanctions",
            "Andrea Gacki has served as director of FinCEN since 2023. She is the second top anti-money laundering official to leave"),
    (True,  "Former Greenville police officer sentenced for drug trafficking scheme",
            "Former Greenville police officer sentenced for drug trafficking scheme"),

    # --- routine enforcement, which the old gate passed ----------------------
    (False, "Police seize two tonnes of cocaine at Rotterdam port",
            "Authorities said the container was intercepted during a routine inspection"),
    (False, "Navy intercepts semi-submersible carrying 1,200 kilos of cocaine",
            "The vessel was detained off the Pacific coast, the navy said"),
    (False, "Man arrested with heroin at bus terminal",
            "Police arrested a 34-year-old man carrying 200 grams of heroin"),
    (False, "Prosecutors charge 14 in fentanyl distribution ring",
            "A federal grand jury indicted fourteen alleged members of a distribution network"),

    # --- capture, which must survive ----------------------------------------
    (True,  "Customs officer jailed for waving through cocaine containers at Antwerp",
            "The officer accepted payments to clear containers for a smuggling network"),
    (True,  "Bank fined $3bn for laundering cartel proceeds",
            "The bank admitted it processed drug proceeds for Mexican cartels over a decade"),
    (True,  "Army general indicted over ties to drug trafficking network",
            "Prosecutors allege the general provided protection to traffickers for a decade"),
    (True,  "Treasury sanctions Dubai company over cartel money laundering network",
            "OFAC designated the firm for laundering proceeds on behalf of a trafficking organisation"),
    (True,  "Juez es acusado de recibir sobornos de una red de narcotráfico",
            "La fiscalía acusó al juez de recibir sobornos para liberar a detenidos"),
    (True,  "Polizist wegen Bestechung durch Drogenhändler verurteilt",
            "Der Beamte soll Informationen an einen Drogenhändlerring verkauft haben"),
    (True,  "Politieman veroordeeld voor corruptie met drugshandel",
            "De agent lekte informatie aan een cocaïnebende in de haven"),
    (True,  "Полицейский осуждён за крышевание наркоторговли",
            "Сотрудник получал взятки от наркоторговцев"),
    (True,  "ضابط جمارك متهم بتلقي رشوة من شبكة تهريب مخدرات",
            "وجهت النيابة تهمة الفساد إلى الضابط"),
    (True,  "Oknum polisi ditangkap karena bekingan peredaran narkoba",
            "Pejabat itu diduga menerima suap dari jaringan narkoba"),
    (True,  "Delegado é condenado por corrupção e tráfico de drogas",
            "O delegado recebia propina de uma facção para avisar sobre operações"),

    # --- noise the old lists let through ------------------------------------
    (False, "General election result leaves cartel policy unresolved",
            "The new government will report on drug policy next year"),
    (False, "FDA approves new cancer drug after clinical trial",
            "The drugmaker said the treatment will be available next quarter"),
    (False, "Netflix series about a cocaine cartel returns for a third season",
            "The show's new season follows the corruption of a fictional police unit"),
    (False, "Hospital executive convicted in health care fraud scheme",
            "Prosecutors said the firm billed Medicare for services never provided"),
    (False, "Premier League transfer window: club fined over agent payments",
            "The club was fined by the league for irregular payments"),
    (False, "Mayor charged in kickback scheme over waste contracts",
            "Prosecutors say the mayor took kickbacks from a waste hauling firm"),
    (False, "Customs seizes record shipment of counterfeit cigarettes",
            "Officers found the cargo hidden in a container at the port"),
    (False, "Two police officers shot dead in cartel ambush",
            "The officers were killed while patrolling a highway"),
    (True,  "Port union boss and two customs agents charged over cocaine pipeline",
            "The indictment says agents were paid to disable scanners at the terminal"),
    (True,  "Prison warden took bribes to let heroin into the jail, court hears",
            "The warden is accused of accepting payments from a trafficking gang"),
    (True,  "Casino group loses licence after laundering drug cash for syndicate",
            "The regulator found the operator processed proceeds for an organised crime syndicate"),
]


def run():
    bad = 0
    for case in CASES:
        want, title, summary = case[0], case[1], case[2]
        sig, why = h.gate(title, summary, len(case) > 3 and case[3])
        got = sig > 0
        mark = "ok " if got == want else "FAIL"
        if got != want:
            bad += 1
        print("%s want=%-5s got=%-5s sig=%-3s %-16s %s"
              % (mark, want, got, sig, why, title[:62]))
    print("\n%d/%d correct" % (len(CASES) - bad, len(CASES)))
    return bad


if __name__ == "__main__":
    sys.exit(1 if run() else 0)
