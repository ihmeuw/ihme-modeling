filedir = "FILEPATH"

filepath = (
    f"{filedir}FILEPATH"
    "combined_Y2024M04D09_Y2024M05D06.xlsx"
)
outpath = f"{filedir}/NLD_NIVEL_PCD_IHME_wide-format_intermediate.xlsx"
outpath2 = (
    f"{filedir}/NLD_NIVEL_PCD_IHME_wide-format_" "intermediate-harmonized.xlsx"
)

denom_str = "denominators 2011-2022"
desc_str = "selection Nivel"

year_strings = [
    "2011",
    "2012",
    "2013",
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
    "2021",
    "2022",
]

wider_bins_columns = ["5_14", "15_49", "50_69", "70_plus"]
narrower_bins_columns = [
    "5_9",
    "10_14",
    "15_19",
    "20_24",
    "25_29",
    "30_34",
    "35_39",
    "40_44",
    "45_49",
    "50_54",
    "55_59",
    "60_64",
    "65_69",
    "70_74",
    "75_79",
    "80_84",
    "85_plus",
]

all_bins_columns = [
    "0_4",
    "5_14",
    "15_49",
    "50_69",
    "70_plus",
    "5_9",
    "10_14",
    "15_19",
    "20_24",
    "25_29",
    "30_34",
    "35_39",
    "40_44",
    "45_49",
    "50_54",
    "55_59",
    "60_64",
    "65_69",
    "70_74",
    "75_79",
    "80_84",
    "85_plus",
]

# Define the mappings between wider and narrower bins
age_bin_mappings = {
    "5_14": ["5_9", "10_14"],
    "15_49": ["15_19", "20_24", "25_29", "30_34", "35_39", "40_44", "45_49"],
    "50_69": ["50_54", "55_59", "60_64", "65_69"],
    "70_plus": ["70_74", "75_79", "80_84", "85_plus"],
}

age_bin_groups = [
    ["5_9", "10_14"],
    ["15_19", "20_24", "25_29", "30_34", "35_39", "40_44", "45_49"],
    ["50_54", "55_59", "60_64", "65_69"],
    ["70_74", "75_79", "80_84", "85_plus"],
]

# Constants specific to 01_NLD_NIVEL_combine-data.py:

# Define the path for input Excel files
file_path1 = f"{filedir}/NLD_NIVEL_PRIMARY_CARE_DATABASE_2011_2022_Y2024M04D09.XLSX"
file_path2 = f"{filedir}/NLD_NIVEL_PRIMARY_CARE_DATABASE_2011_2022_Y2024M05D06.XLSX"
file_path3 = f"{filedir}/selection-Nivel_I-P_descriptions.xlsx"

# Define the output Excel file path
output_file_path = (
    f"{filedir}/prepped_outputs/NLD_NIVEL_PRIMARY_CARE_DATABASE_2011_2022_"
    "combined_Y2024M04D09_Y2024M05D06.xlsx"
)

specified_age_bins = [
    "age_5_9",
    "age_10_14",
    "age_15_19",
    "age_20_24",
    "age_25_29",
    "age_30_34",
    "age_35_39",
    "age_40_44",
    "age_45_49",
    "age_50_54",
    "age_55_59",
    "age_60_64",
    "age_65_69",
    "age_70_74",
    "age_75_79",
    "age_80_84",
    "age_85_up",
]

raw_age_bin_groups = [
    ["age_5_9", "age_10_14"],
    [
        "age_15_19",
        "age_20_24",
        "age_25_29",
        "age_30_34",
        "age_35_39",
        "age_40_44",
        "age_45_49",
    ],
    ["age_50_54", "age_55_59", "age_60_64", "age_65_69"],
    ["age_70_74", "age_75_79", "age_80_84", "age_85_up"],
]

raw_age_bin_mapping = {
    "age_5_14": ["age_5_9", "age_10_14"],
    "age_15_49": [
        "age_15_19",
        "age_20_24",
        "age_25_29",
        "age_30_34",
        "age_35_39",
        "age_40_44",
        "age_45_49",
    ],
    "age_50_69": ["age_50_54", "age_55_59", "age_60_64", "age_65_69"],
    "age_70_up": ["age_70_74", "age_75_79", "age_80_84", "age_85_up"],
}

# Constants specific to 03_NLD_NIVEL_adjusted-aggregate.py:

read_path = f"{filedir}/NLD_NIVEL_PCD_IHME_wide-format_intermediate.xlsx"
write_path = f"{filedir}/NLD_NIVEL_PCD_IHME_wide-format_adjusted.xlsx"

# Constants specific to 06_NLD_NIVEL_disaggregate.py:

adjusted_path = f"{filedir}/NLD_NIVEL_PCD_IHME_long-format_adjusted.csv"
intermediate_path = (
    f"{filedir}/NLD_NIVEL_PCD_IHME_long-format_" "intermediate-harmonized.csv"
)
unadjusted_path = f"{filedir}/NLD_NIVEL_PCD_IHME_long-format_unadjusted.csv"

age_group_id_mappings = {
    23: [6, 7],
    24: [8, 9, 10, 11, 12, 13, 14],
    25: [15, 16, 17, 18],
    26: [19, 20, 30, 160],
}
