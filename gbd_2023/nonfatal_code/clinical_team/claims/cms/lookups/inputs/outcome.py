import pandas as pd

PATIENT_STATUS_CD = pd.DataFrame(
    data=[
        (1, "DISCHARGED TO HOME OR SELF CARE (ROUTINE DISCHARGE)"),
        (2, "DISCHARGED/TRANSFERRED TO ANOTHER SHORT-TERM HOSPITAL"),
        (3, "DISCHARGED/TRANSFERRED TO A NURSING FACILITY"),
        (4, "DISCHARGED/TRANSFERRED TO AN INTERMEDIATE CARE FACILITY"),
        (
            5,
            "DISCHARGED/TRANSFERRED TO ANOTHER TYPE INSTITUTION (INCLUDING DISTINCT PARTS) OR REFERRED FOR OUTPATIENT SERVICES TO ANOTHER INSTITUTION",
        ),
        (
            6,
            "DISCHARGED/TRANSFERRED TO HOME UNDER CARE OF ORGANIZED HOME HEALTH SERVICE ORGANIZATION",
        ),
        (7, "LEFT AGAINST MEDICAL ADVICE OR DISCONTINUED CARE"),
        (8, "DISCHARGED/TRANSFERRED TO HOME UNDER CARE OF A HOME IV DRUG THERAPY PROVIDER"),
        (9, "ADMITTED AS AN INPATIENT TO THIS HOSPITAL"),
        (20, "EXPIRED (OR DID NOT RECOVER - CHRISTIAN SCIENCE) PATIENT"),
        (30, "STILL A PATIENT"),
        (40, "EXPIRED AT HOME (HOSPICE CLAIMS ONLY)"),
        (
            41,
            "EXPIRED IN A MEDICAL FACILITY SUCH AS A HOSPITAL, NF OR FREE- STANDING HOSPICE (HOSPICE CLAIMS ONLY)",
        ),
        (42, "EXPIRED - PLACE UNKNOWN (HOSPICE CLAIMS ONLY)"),
        (43, "Discharged/transferred to a federal hospital"),
        (50, "HOSPICE - HOME"),
        (51, "HOSPICE - MEDICAL FACILITY"),
        (61, "DISCHARGED TO A HOSPITAL-BASED MEDICARE APPROVED SWING BED"),
        (62, "DISCHARGED/TRANSFERRED TO ANOTHER REHAB FACILITY/REHAB UNIT OF A HOSPITAL"),
        (63, "DISCHARGED/TRANSFERRED TO A LONG-TERM CARE HOSPITAL"),
        (65, "DISCHARGED/TRANSFERRED TO A PSYCH HOSPITAL/PSYCH UNIT OF A HOSPITAL"),
        (66, "DISCHARGED TO CRITICAL ACCESS HOSPITAL"),
        (71, "DISCHARGED/TRANSFERRED TO ANOTHER INSTITUTION FOR OUTPATIENT SERVICES"),
        (72, "DISCHARGED/TRANSFERRED TO THIS INSTITUTION FOR OUTPATIENT SERVICES"),
        (99, "UNKNOWN"),
    ],
    columns=["patient_status_cd", "patient_status_value"],
)


PTNT_DSCHRG_STUS_CD = pd.DataFrame(
    data=[
        (0, "Unknown Value (but present in data)"),
        (1, "Discharged to home/self-care (routine charge)."),
        (2, "Discharged/transferred to other short term general hospital for inpatient care."),
        (
            3,
            "Discharged/transferred to skilled nursing facility (SNF) with Medicare certification in anticipation of covered skilled care â€” (For hospitals with an approved swing bed arrangement, use Code 61 â€” swing bed. For reporting discharges/transfers to a non-certified SNF, the hospital must use Code 04 â€” ICF.",
        ),
        (4, "Discharged/transferred to intermediate care facility (ICF)."),
        (
            5,
            "Discharged/transferred to another type of institution for inpatient care (including distinct parts). NOTE: Effective 1/2005, psychiatric hospital or psychiatric distinct part unit of a hospital will no longer be identified by this code. New code is '65'.",
        ),
        (
            6,
            "Discharged/transferred to home care of organized home health service organization.",
        ),
        (7, "Left against medical advice or discontinued care."),
        (
            8,
            "Discharged/transferred to home under care of a home IV drug therapy provider. (discontinued effective 10/1/2005)",
        ),
        (
            9,
            "Admitted as an inpatient to this hospital (effective 3/1/1991). In situations where a patient is admitted before midnight of the third day following the day of an outpatient service, the outpatient services are considered inpatient.",
        ),
        (20, "Expired (patient did not recover)."),
        (21, "Discharged/transferred to court/law enforcement."),
        (30, "Still patient."),
        (40, "Expired at home (hospice claims only)"),
        (
            41,
            "Expired in a medical facility such as hospital, SNF, ICF, or freestanding hospice. (Hospice claims only)",
        ),
        (42, "Expired â€” place unknown (Hospice claims only)"),
        (43, "Discharged/transferred to a federal hospital (eff. 10/1/2003)"),
        (50, "Discharged/transferred to a Hospice â€” home."),
        (51, "Discharged/transferred to a Hospice â€” medical facility."),
        (
            61,
            "Discharged/transferred within this institution to a hospital-based Medicare approved swing bed (eff. 9/2001)",
        ),
        (
            62,
            "Discharged/transferred to an inpatient rehabilitation facility including distinct parts units of a hospital. (eff. 1/2002)",
        ),
        (63, "Discharged/transferred to a long-term care hospital. (eff. 1/2002)"),
        (
            64,
            "Discharged/transferred to a nursing facility certified under Medicaid but not under Medicare (eff. 10/2002)",
        ),
        (
            65,
            "Discharged/Transferred to a psychiatric hospital or psychiatric distinct unit of a hospital (these types of hospitals were pulled from patient/discharge status code '05' and given their own code). (eff. 1/2005).",
        ),
        (66, "Discharged/transferred to a Critical Access Hospital (CAH) (eff. 1/1/2006)"),
        (
            69,
            "Discharged/transferred to a designated disaster alternative care site (starting 10/2013; applies only to particular MS-DRGs*)",
        ),
        (
            70,
            "Discharged/transferred to another type of health care institution not defined elsewhere in code list.",
        ),
        (
            71,
            "Discharged/transferred/referred to another institution for outpatient services as specified by the discharge plan of care (eff. 9/2001) (discontinued eff. 10/1/2005)",
        ),
        (
            72,
            "Discharged/transferred/referred to this institution for outpatient services as specified by the discharge plan of care (eff. 9/2001) (discontinued eff. 10/1/2005)) (The following codes apply only to particular MS-DRGs*, and were new in 10/2013:",
        ),
        (
            81,
            "Discharged to home or self-care with a planned acute care hospital inpatient readmission.",
        ),
        (
            82,
            "Discharged/transferred to a short-term general hospital for inpatient care with a planned acute care hospital inpatient readmission.",
        ),
        (
            83,
            "Discharged/transferred to a skilled nursing facility (SNF) with Medicare certification with a planned acute care hospital inpatient readmission.",
        ),
        (
            84,
            "Discharged/transferred to a facility that provides custodial or supportive care with a planned acute care hospital inpatient readmission.",
        ),
        (
            85,
            "Discharged/transferred to a designated cancer center or childrenâ€™s hospital with a planned acute care hospital inpatient readmission.",
        ),
        (
            86,
            "Discharged/transferred to home under care of organized home health service organization with a planned acute care hospital inpatient readmission.",
        ),
        (
            87,
            "Discharged/transferred to court/law enforcement with a planned acute care hospital inpatient readmission.",
        ),
        (
            88,
            "Discharged/transferred to a federal health care facility with a planned acute care hospital inpatient readmission.",
        ),
        (
            89,
            "Discharged/transferred to a hospital-based Medicare approved swing bed with a planned acute care hospital inpatient readmission.",
        ),
        (
            90,
            "Discharged/transferred to an inpatient rehabilitation facility (IRF) including rehabilitation distinct part units of a hospital with a planned acute care hospital inpatient readmission.",
        ),
        (
            91,
            "Discharged/transferred to a Medicare certified long term care hospital (LTCH) with a planned acute care hospital inpatient readmission.",
        ),
        (
            92,
            "Discharged/transferred to a nursing facility certified under Medicaid but not certified under Medicare with a planned acute care hospital inpatient readmission.",
        ),
        (
            93,
            "Discharged/transferred to a psychiatric distinct part unit of a hospital with a planned acute care hospital inpatient readmission.",
        ),
        (
            94,
            "Discharged/transferred to a critical access hospital (CAH) with a planned acute care hospital inpatient readmission.",
        ),
        (
            95,
            "Discharged/transferred to another type of health care institution not defined elsewhere in this code list with a planned acute care hospital inpatient readmission.",
        ),
        (
            460.25,
            "Unknown but present in data",
        ),
    ],
    columns=["ptnt_dschrg_stus_cd", "ptnt_dschrg_stus_value"],
)
