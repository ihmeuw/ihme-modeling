import os as os
import sys
from pathlib import Path

import pandas as pd
import numpy as np

import copy as copy
from io import StringIO
from multiprocessing import Pool

import glob

import requests
from db_queries import get_model_results, get_population
from scipy import stats

# from db_connector.database import Database # need to be in clinical environment 
from clinical_db_tools.db_connector.database import Database



def reader(data_dir: str) -> pd.DataFrame:
    df = pd.read_hdf(data_dir)
    return df


def format_data(df: pd.DataFrame) -> pd.DataFrame:
    # only want inpatient
    df = df.loc[(df.facility_id == "inpatient unknown") | (df.facility_id == "hospital")]

    # only want primary diagnoses
    df = df.loc[df.diagnosis_id == 1]

    # get columns we want
    df = df[["location_id", "year_start", "year_end", "sex_id", "age_group_id", "val", "nid"]]
    # groupby to get the sum of values. Gets total sum of primary admissions at a certain age
    df = (
        df.groupby(["age_group_id", "sex_id", "year_start", "year_end", "location_id", "nid"])
        .sum()
        .reset_index()
    )

    # Get population, merge on and calculate IP admissions per capita
    # Will maybe need to make decomp_step and gbd_round_id not hard-coded
    pop = get_population(
        location_id=df.location_id.unique().tolist(),
        year_id=df.year_start.unique().tolist(),
        age_group_id=df.age_group_id.unique().tolist(),
        sex_id=[1, 2, 3],
        release_id=16
    )
    df = df.merge(
        pop[["age_group_id", "sex_id", "location_id", "population", "year_id"]],
        left_on=["age_group_id", "sex_id", "year_start", "location_id"],
        right_on=["age_group_id", "sex_id", "year_id", "location_id"],
        how="left",
    )

    df["mean"] = df["val"] / df["population"]

    # rename for uncertainty
    df.rename(columns={"population": "sample_size"}, inplace=True)
    df.rename(columns={"val": "cases"}, inplace=True)

    # These should be whole numbers
    df = df.round({"cases": 0, "sample_size": 0})

    # Get males and females if not already done
    df = df.loc[df.sex_id.isin([1, 2, 3])]

    # Format for ST-GPR
    df["measure"] = "continuous"

    # get age group info
    db = Database()
    db.load_odbc("clinical")
    ages = db.query(
        "SELECT age_group_id, age_group_years_start, age_group_years_end FROM shared.age_group"
    )
    df = pd.merge(df, ages)
    df = df.rename(
        columns={"age_group_years_start": "age_start", "age_group_years_end": "age_end"}
    )

    return df

# Read in run ID(s) from the given path
path_to_config = "FILEPATH"
config = pd.read_excel(path_to_config)

# Filter config for 'current' status runs
filtered_config = config[config['status'] == 'current']

# Ensure there's exactly one unique 'run_id' for 'current' status runs
if len(filtered_config) == 1:
    # Pull 'run_id' from the filtered result
    env_run_id = filtered_config['run_id'].iloc[0]
else:
    raise ValueError(f"There should be exactly one unique 'run_id' with status 'current', found {len(filtered_config)}.")

# Pull column values to loop over. Can be length 1 or more
inp_run_id_str = filtered_config['inpatient_run_ids'].iloc[0]

# Split the string by ";" and convert each part to an integer
inp_run_id_vec = [int(x) for x in inp_run_id_str.split(';')]



for inp_run_id in inp_run_id_vec:
    # Indicate path to run with run ID
    path_to_run = f"FILEPATH"

    # Pull file names ending with .H5
    file_names = [file for file in os.listdir(path_to_run) if file.lower().endswith('.h5')]
    file_names.sort()

    # Dynamically generate write_dir
    write_dir = f"FILEPATH"
    # write_dir = os.path.join(base_write_dir, f"inpatient_run_{run_id}")

    # Create the directory if it does not exist
    os.makedirs(write_dir, exist_ok=True)

    # Remove ".H5" from each file name
    names = [name.replace(".H5", "") for name in file_names]

    # Print the modified list to confirm the list of available clinical inpatient sources
    for name in names:
        print(name)

    # Iterate over file names to format and save
    for name in names:
        file_name = os.path.join(path_to_run, name + '.H5')

        df = reader(file_name)  # Assuming a function 'reader' exists that reads the H5 file into a DataFrame
        df = format_data(df)  # Assuming a function 'format_data' exists that formats the DataFrame as needed

        output_file_path = os.path.join(write_dir, name + '.csv')  # Construct full output file path for each CSV
        df.to_csv(output_file_path, index=False)

        print(f"{name} Done for run ID {inp_run_id}")

    print(f"Processing complete for run ID {inp_run_id}.")
