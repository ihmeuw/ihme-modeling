import pandas as pd
import glob
import platform

if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

file_list = ["FILEPATH",
             "FILEPATH",
             "FILEPATH",
             "FILEPATH",
             "FILEPATH",
             "FILEPATH"]
















chunksize = 10000
df_list =[]
for file_name in file_list:
    for chunk in pd.read_csv(root + file_name, chunksize=chunksize, usecols=['STDPLAC']):
        chunk = chunk.STDPLAC.value_counts(dropna=False).reset_index()
        chunk.fillna("MISSING")
        chunk = chunk.groupby("index").agg({"STDPLAC":"sum"}).reset_index()
        df_list.append(chunk)

result = pd.concat(df_list)
result.rename(columns={'index': 'STDPLAC', 'STDPLAC':'counts'}, inplace=True)
result = result.groupby("STDPLAC").agg({"counts":"sum"}).reset_index()
result.to_excel(root + r"FILEPATH", index=False)


stdplac_dict = {
    1: "Pharmacy (1)",
    35: "Adult Living Care Facility",
    3: "School",
    41: "Ambulance (land)",
    4: "Homeless Shelter",
    42: "Ambulance (air or water)",
    11: "Office",
    49: "Independent Clinic",
    12: "Patient Home",
    50: "Federally Qualified Health Ctr",
    13: "Assisted Living Facility",
    51: "Inpatient Psychiatric Facility",
    14: "Group Home",
    52: "Psych Facility Partial Hosp",
    15: "Mobile Unit",
    53: "Community Mental Health Center",
    16: "Temporary Lodging",
    54: "Intermed Care/Mental Retarded",
    17: "Walk-in Retail Health Clinic",
    55: "Residential Subst Abuse Facil",
    20: "Urgent Care Facility",
    56: "Psych Residential Treatmnt Ctr",
    21: "Inpatient Hospital",
    57: "Non-resident Subst Abuse Facil",
    22: "Outpatient Hospital",
    60: "Mass Immunization Center",
    23: "Emergency Room - Hospital",
    61: "Comprehensive Inpt Rehab Fac",
    24: "Ambulatory Surgical Center",
    62: "Comprehensive Outpt Rehab Fac",
    25: "Birthing Center",
    65: "End-Stage Renal Disease Facil",
    26: "Military Treatment Facility",
    71: "State/Local Public Health Clin",
    27: "Inpatient Long-Term Care (NEC)",
    72: "Rural Health Clinic",
    28: "Other Inpatient Care (NEC)",
    81: "Independent Laboratory",
    31: "Skilled Nursing Facility",
    95: "Outpatient (NEC)",
    32: "Nursing Facility",
    98: "Pharmacy (2)",
    33: "Custodial Care Facility",
    99: "Other Unlisted Facility",
    34: "Hospice"}

result['place name'] = result['STDPLAC'].map(stdplac_dict)
result = result.fillna("MISSING")

result.to_excel(root + r"FILEPATH", index=False)
