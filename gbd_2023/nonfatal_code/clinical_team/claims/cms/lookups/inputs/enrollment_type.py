'''
Define the MDCR / MAX enrollement types

ENTLMT_RSN:
    https://www.resdac.org/cms-data/variables/original-reason-entitlement-code
    
    Note that there is a second indicator variable that is used per month of 
    eligibilty, however this code is derived from age and ENTLMT_RSN:
        https://www.resdac.org/cms-data/variables/medicare-status-code-january

MAX_ELGBLTY_CD_LTST:
    https://www.resdac.org/cms-data/variables/max-eligibility-most-recent-ps
    
'''

MDCR_ENTLMT_RSN = {
    0 : 'Old age and survivor’s insurance (OASI)',
    1 : 'Disability insurance benefits (DIB)',
    2 : 'End-stage renal disease (ESRD)',
    3 : 'Both DIB and ESRD'
}

MAX_ELGBLTY_CD_LTST = {
    '00' : 'NOT ELIGIBLE',
    '11' : 'AGED, CASH',
    '12' : 'BLIND/DISABLED, CASH',
    '14' : 'CHILD (NOT CHILD OF UNEMPLOYED ADULT, NOT FOSTER CARE CHILD), ELIGIBLE UNDER SECTION 1931 OF THE ACT',
    '15' : 'ADULT (NOT BASED ON UNEMPLOYMENT STATUS), ELIGIBLE UNDER SECTION 1931 OF THE ACT',
    '16' : 'CHILD OF UNEMPLOYED ADULT, ELIGIBLE UNDER SECTION 1931 OF THE ACT',
    '17' : 'UNEMPLOYED ADULT, ELIGIBLE UNDER SECTION 1931 OF THE ACT',
    '21' : 'AGED, Medically Needy',
    '22' : 'BLIND/DISABLED, Medically Needy',
    '24' : 'CHILD, Medically Needy (FORMERLY AFDC CHILD, Medically Needy)',
    '25' : 'ADULT, Medically Needy (FORMERLY AFDC ADULT, Medically Needy)',
    '31' : 'AGED, POVERTY',
    '32' : 'BLIND/DISABLED, POVERTY',
    '34' : 'CHILD, POVERTY (INCLUDES MEDICAID EXPANSION CHIP CHILDREN)',
    '35' : 'ADULT, POVERTY',
    '3A' : 'INDIVIDUAL COVERED UNDER THE BREAST AND CERVICAL CANCER PREVENTION ACT OF 2000, POVERTY',
    '41' : 'OTHER AGED',
    '42' : 'OTHER BLIND/DISABLED',
    '44' : 'OTHER CHILD',
    '45' : 'OTHER ADULT',
    '48' : 'FOSTER CARE CHILD',
    '51' : 'AGED, SECTION 1115 DEMONSTRATION EXPANSION',
    '52' : 'DISABLED, SECTION 1115 DEMONSTRATION EXPANSION',
    '54' : 'CHILD, SECTION 1115 DEMONSTRATION EXPANSION',
    '55' : 'ADULT, SECTION 1115 DEMONSTRATION EXPANSION',
    '99' : 'UNKNOWN ELIGIBILITY',
    'ZZ' : 'ASSIGNED IN RECORDS STARTING JANUARY 1, 2014, FOR MONTHS AN INDIVIDUAL WAS REPORTED IN MSIS WITH A VALID T-MSIS ELIGIBILITY GROUP, BUT NOT REPORTED WITH A MASBOE ASSIGNMENT. THIS VALUE WILL NOT BE USED ON A DUMMY RECORD.',
}
