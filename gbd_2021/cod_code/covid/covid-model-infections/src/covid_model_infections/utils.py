# deaths/hosp limit comes from /home/j/WORK/12_bundle/lri_corona/9263/01_input_data/01_lit/mrbrt/final_asymptomatic_proportion_draws_by_age.csv
CEILINGS = {
    'deaths': 0.65,
    'cases': 0.80,
    'hospitalizations': 0.65,
}

SUB_LOCATIONS = [
    7,    # Democratic People's Republic of Korea
    39,   # Tajikistan
    40,   # Turkmenistan
    131,  # Nicaragua
    133,  # Venezuela (Bolivarian Republic of)
    189,  # United Republic of Tanzania
]

TRIM_LOCATIONS = [
    # ...
]

DEPLETION_LOCATIONS = {
    'moderate': [
        36,     # Kazakhstan
        59,     # Latvia
        60,     # Lithuania
        62,     # Russian Federation
        182,    # Malawi
        193,    # Botswana
        194,    # Lesotho
        195,    # Namibia
        4758,   # Goiás
        4865,   # Goa
        4855,   # Jharkhand
        4857,   # Kerala
        4861,   # Manipur
        4862,   # Meghalaya
    ],
    'severe': [
        38,     # Mongolia
        90,     # Norway
        186,    # Seychelles
        4863,   # Mizoram
        53617,  # Gilgit-Baltistan
    ],
}
