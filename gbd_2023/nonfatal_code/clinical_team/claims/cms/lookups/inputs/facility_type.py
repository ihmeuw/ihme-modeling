
# First define which lookups are needed for each source and file type
fac_source_file = {
    'max': {
        'inpatient': None,
        'outpatient': 'PLC_OF_SRVC_CD'
        },

    'medicare': {
        'inpatient': 'CLM_FAC_TYPE_CD',
        'outpatient': 'CLM_FAC_TYPE_CD',
        'hha': 'CLM_FAC_TYPE_CD',
        'hospice': 'CLM_FAC_TYPE_CD',
        'bcarrier': None
    }
}

# then define the actual lookups
fac_lookup = {

    # source https://www.resdac.org/sites/resdac.umn.edu/files/Place%20of%20Service%20Code%20Table.txt
    'PLC_OF_SRVC_CD': {
        3: 'SCHOOL (*)',
        4: 'HOMELESS SHELTER (*)',
        5: 'INDIAN HEALTH SERVICE FREE-STANDING FACILITY (*)',
        6: 'INDIAN HEALTH SERVIE PROVIDER-BASED FACILITY (*)',
        7: 'TRIBAL 638 FREE-STANDING FACILITY (*)',
        8: 'TRIBAL 638 PROVIDER-BASED FACILITY (*)',
        11: 'OFFICE',
        12: "PATIENT'S HOME",
        15: 'MOBILE UNIT (*)',
        20: 'URGENT CARE FACILITY (*)',
        21: 'INPATIENT HOSPITAL',
        22: 'OUTPATIENT HOSPITAL',
        23: 'EMERGENCY ROOM - HOSPITAL',
        24: 'AMBULATORY SURGERY CENTER',
        25: 'BIRTHING CENTER',
        26: 'MILITARY TREATMENT FACILITY',
        32: 'NURSING FACILITY',
        33: 'CUSTODIAL CARE FACILITY',
        34: 'HOSPICE',
        41: 'AMBULANCE - LAND',
        42: 'AMBULANCE - AIR OR WATER',
        50: 'FEDERALLY QUALIFIED HEALTH CENTER',
        51: 'INPATIENT PSYCHIATRIC FACILITY',
        52: 'PSYCHIATRIC FACILITY PARTIAL HOSPITALIZATION',
        53: 'COMMUNITY MENTAL HEALTH CENTER',
        54: 'INTERMEDIATE CARE FACILITY FOR THE MENTALLY RETARDED',
        55: 'RESIDENTIAL SUBSTANCE ABUSE TREATMENT FACILITY',
        56: 'PSYCHIATRIC RESIDENTIAL TREATMENT CENTER',
        60: 'MASS IMMUNIZATION CENTER (*)',
        61: 'COMPREHENSIVE INPATIENT REHABILITATION FACILITY',
        62: 'COMPREHENSIVE OUTPATIENT REHABILITATION FACILITY',
        65: 'END STAGE RENAL DISEASE TREATMENT FACILITY',
        71: 'STATE OR LOCAL PUBLIC HEALTH CLINIC',
        72: 'RURAL HEALTH CLINIC',
        81: 'INDEPENDENT LABORATORY',
        88: 'NOT APPLICABLE (USED WITH TYPE OF SERVICE 20, 21 OR 22)',
        99: 'OTHER (NOT LISTED ABOVE) OR UNKNOWN',
        },

    # source https://www.resdac.org/cms-data/variables/claim-facility-type-code-ffs
    'CLM_FAC_TYPE_CD': {
        1: 'Hospital',
        2: 'Skilled nursing facility (SNF)',
        3: 'Home health agency (HHA)',
        4: 'Religious Nonmedical (Hospital) (eff. 8/1/00); prior to 8/00 referenced Christian Science (CS)',
        6: 'Intermediate care',
        7: 'Clinic or hospital-based renal dialysis facility',
        8: 'Special facility or ASC surgery',
        },
}

# These tables are present in both MAX files but are only tangential to a
# facility_id
type_of_service_lookup = {
        # source https://www.resdac.org/sites/resdac.umn.edu/files/MAX%20TOS%20Table.txt
    'MAX_TOS': {
        1: 'INPATIENT HOSPITAL',
        2: 'MENTAL HOSPITAL SERVICES FOR THE AGED',
        4: 'INPATIENT PSYCHIATRIC FACILITY FOR INDIVIDUALS UNDER THE AGE OF 21',
        5: 'INTERMEDIATE CARE FACILITY (ICF) FOR THE MENTALLY RETARDED',
        7: 'NURSING FACILITY SERVICES (NFS) - ALL OTHER',
        8: 'PHYSICIANS',
        9: 'DENTAL',
        10: 'OTHER PRACTITIONERS',
        11: 'OUTPATIENT HOSPITAL',
        12: 'CLINIC',
        13: 'HOME HEALTH',
        15: 'LAB AND X-RAY',
        16: 'DRUGS',
        19: 'OTHER SERVICES',
        20: 'CAPITATED PAYMENTS TO HMO, HIO, OR PACE PLANS',
        21: 'CAPITATED PAYMENTS TO PREPAID HEALTH PLANS - PHPs',
        22: 'CAPITATED PAYMENTS FOR PRIMARY CARE CASE MANAGEMENT - PCCM',
        24: 'STERILIZATIONS',
        25: 'ABORTIONS',
        26: 'TRANSPORTATION SERVICES',
        30: 'PERSONAL CARE SERVICES',
        31: 'TARGETED CASE MANAGEMENT',
        33: 'REHABILITATION SERVICES',
        34: 'PT, OT, SPEECH, HEARING SERVICES',
        35: 'HOSPICE BENEFITS',
        36: 'NURSE MIDWIFE SERVICES',
        37: 'NURSE PRACTITIONER SERVICES',
        38: 'PRIVATE DUTY NURSING',
        39: 'RELIGIOUS NON-MEDICAL HEALTH CARE INSTITUTIONS',
        51: 'DURABLE MEDICAL EQUIPMENT AND SUPPLIES (INCLUDING EMERGENCY RESPONSE SYSTEMS AND HOME MODIFICATIONS)',
        52: 'RESIDENTIAL CARE (DEFINITION CHANGED FOR 2003 AND LATER YEARS - ADDITIONAL INFORMATION IS AVAILABLE ON REQUEST)',
        53: 'PSYCHIATRIC SERVICES (EXCLUDING ADULT DAY CARE)',
        54: 'ADULT DAY CARE',
        99: 'UNKNOWN',
        },

    # source https://www.resdac.org/sites/resdac.umn.edu/files/MSIS%20TOS%20Table.txt
    'MSIS_TOS': {
        1: 'INPATIENT HOSPITAL',
        2: 'MENTAL HOSPITAL SERVICES FOR THE AGED',
        4: 'INPATIENT PSYCHIATRIC FACILITY FOR INDIVIDUALS UNDER THE AGE OF 21',
        5: 'INTERMEDIATE CARE FACILITY (ICF) FOR THE MENTALLY RETARDED',
        7: 'NURSING FACILITY SERVICES (NFS) - ALL OTHER',
        8: 'PHYSICIANS',
        9: 'DENTAL',
        10: 'OTHER PRACTITIONERS',
        11: 'OUTPATIENT HOSPITAL',
        12: 'CLINIC',
        13: 'HOME HEALTH',
        15: 'LAB AND X-RAY',
        16: 'PRESCRIBED DRUGS',
        19: 'OTHER SERVICES',
        20: 'CAPITATED PAYMENTS TO HMO, HIO, OR PACE PLANS',
        21: 'CAPITATED PAYMENTS TO PREPAID HEALTH PLANS - PHPs',
        22: 'CAPITATED PAYMENTS FOR PRIMARY CARE CASE MANAGEMENT - PCCM',
        24: 'STERILIZATIONS',
        25: 'ABORTIONS',
        26: 'TRANSPORTATION SERVICES',
        30: 'PERSONAL CARE SERVICES',
        31: 'TARGETED CASE MANAGEMENT',
        33: 'REHABILITATION SERVICES',
        34: 'PT, OT, SPEECH, HEARING SERVICES',
        35: 'HOSPICE BENEFITS',
        36: 'NURSE MIDWIFE SERVICES',
        37: 'NURSE PRACTITIONER SERVICES',
        38: 'PRIVATE DUTY NURSING',
        39: 'RELIGIOUS NON-MEDICAL HEALTH CARE INSTITUTIONS',
        99: 'UNKNOWN ',
        },
}