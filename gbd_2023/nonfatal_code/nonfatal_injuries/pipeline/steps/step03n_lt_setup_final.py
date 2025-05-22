"""
Prepare data to go through DisMod ODE.

**Context**:


  After splitting ecode incidence into ecode/ncode incidence the
  pipeline makes two different estimates, short term estimates of injuries
  and long term estimates or injuries. We define long term injury as
  any injury leading to a year or more of disability. This script begins the
  long term injury estimation process.

  Long term incidence is calculated by multiplying our incidence by the 
  probability that an ecode/ncode results in a year or more of disability. 
  These probabilities are calculated in a `separate process

  Essentially, we use follow up surveys from individuals that were injured over
  a year ago as well as expert opinion to calculate the probability of long term
  disability.

  We calculate the prevalence of long term injuries after computing long term
  incidence. To do this, we use a tool called DisMod ODE. A majority of the work
  done in this script is to format long term incidence so it can be read in by
  the DisMod ODE program.

**Output**:

  containing many different files needed to run DisMod ODE


**Overview**:

  1. Load demographics

  2. Load Long term probabilities and SMR

  3. For each year

     4. For each sex

        5. Calculate long term incidence using long term probabilities

        6. *Does the ncode use EMR* Yes:

           6.1. Calculate EMR from SMR

        7. Create data in file for the given ecode, year and sex by location
           (There are a lot of files being made). Store the results for the next
           step.

  8. Make value in using all of the data sfrom the previous step.
"""
import pandas as pd
import sys
import xarray as xr
import time
import warnings

# Suppress performance warnings from Pandas
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

import db_queries as db

from FILEPATH.pipeline import config
from FILEPATH.pipeline.helpers import (utilities, load_measures,
                                      calculate_measures, inj_info, paths,
                                      demographics, input_manager)
from FILEPATH.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)

def make_data_in(measures, ecode, version, ncode, platform, loc_dict, year_id, sex_id):
    """ Creates a data in folder with the given data to be used by DisMod ODE"""
    df_list = []
    for measure, df in list(measures.items()):
        df['meas_stdev'] = df['meas_stdev'].where(df['meas_stdev'] <= df['meas_value'] / 2, df['meas_value'] / 2)

        # We cannot have standard deviation that is less than or equal to 0
        # Below we pick another value, called minval, to replace all standard
        # deviations that are less than or equal to 0
        if (df['meas_stdev'] <= 0).values.any():
            # If we have a standard deviation less than or equal to 0,
            # store the smallest one. If all our values are 0 or less,
            # store a very small number as the minimum value.
            try:
                minval = df['meas_stdev'].values[(df['meas_stdev']>0).values].min()
            except ValueError as e:
                log.warning('The whole df is 0, setting stdev to be 1e-12')
                minval = .00000001
            # Replace 0/negative standard deviations with a very small number
            df['meas_stdev'] = df['meas_stdev'].clip(min=minval/10000)

        # Save our data as a dataframe and prep it for DisMod ODE
        frame = df.to_dataframe().reset_index()[['location_id','year_id','sex_id','age_group_id','meas_value','meas_stdev']]
        frame['integrand'] = measure
        df_list.append(frame)

    # Combine all years and sexes and replace our age_group_ids with ages in
    # years
    df = pd.concat(df_list)

    # Add age bins
    df.loc[:, 'age_lower'] = df.age_group_id.replace(demographics.AGE_GROUP_ID_TO_YEARS_LOWER)
    df.loc[:, 'age_upper'] = df.age_group_id.replace(demographics.AGE_GROUP_ID_TO_YEARS_UPPER)
    
    df.drop(columns=['age_group_id'], inplace=True)

    for hierarchy in ["subreg", "region", "super"]:
        df[hierarchy] = "none"

    df["x_ones"] = 1

    df.drop(['sex_id', 'year_id'], inplace=True, axis=1)
    df.set_index('location_id', inplace=True)

    # Save data by location
    for locn in loc_dict:
        dm_out_dir = ("FILEPATH")

        filepath = "FILEPATH"
        if not filepath.exists():
            try:
                filepath.mkdir(parents=True)
            except:
                pass

        # write the CSV for the DisMod ODE to read in
        df.loc[locn].to_csv(filepath/f"data_in_{ncode}_{platform}.csv", index=False)

    return df[['integrand', 'meas_value']]


def condense(df, ncode):
    """Collapses data over each integrand - incidence and excess
       mortality/mtexcess (if the df contains excess mortality)
    """
    df = df.loc[df['meas_value'] > 0]

    if len(df.loc[df['integrand']=='incidence']) == 0:
        df.loc[len(df)] = {'integrand': 'incidence', 'meas_value': 0}

    if ncode in inj_info.EMR_NCODES and len(df.loc[df['integrand']=='mtexcess']) == 0:
        df.loc[len(df)] = {'integrand': 'mtexcess', 'meas_value': 0}

    df = df.groupby('integrand').median().reset_index()
    # we want eta to be 1% of the non-zero median of the integrand values
    df['meas_value'] = df['meas_value'] * 0.01
    df['name'] = "eta_" + df['integrand']
    df.drop('integrand', inplace = True, axis = 1)
    df.rename(columns = {'meas_value': 'value'}, inplace = True)

    return df

def get_value(ecode):
    """Pulls value-in parameters from the best initial DisMod model version.
    """
    me_id = utilities.get_me(ecode)
    model_version = db.get_best_model_versions(
        entity = "modelable_entity",
        ids = me_id,
        status = "best",
        release_id=config.RELEASE_ID)["model_version_id"].iloc[0].astype(str)

    filepath = "FILEPATH"
    value = pd.read_csv(filepath)

    cond = value['name'].str.contains("eta")
    value.drop(value[cond].index.values, inplace = True)
    value.loc[value['name'] == "data_like", 'value'] = "log_gaussian"

    return value

def make_value_in(df, ecode, ncode, platform, dm_out_dir):
    """Makes the value-in files for the DisMod ODE.
    """
    log.debug("Making value in file")

    # format data frame
    raw = pd.concat(df)
    collapsed = condense(raw, ncode)

    # format value file and append to collapsed values
    value = get_value(ecode)
    result = collapsed.append(value)

    # output the results to value_in directory
    folder = dm_out_dir/"value_in"
    if not folder.exists():
        try:
            folder.mkdir(parents=True)
        except:
            pass

    filepath = folder/f"value_in_{ncode}_{platform}.csv"
    result.to_csv(filepath, index = False)
    log.debug(f"Saved to {filepath}")


def main(ecode, ncode, platform, version):

    flat_version = str(config.DEM_RUN_ID)

    # get demographics
    metaloc = db.get_location_metadata(location_set_id=config.LOCATION_SET_ID, 
                                       release_id=config.RELEASE_ID)

    locations = utilities.ihme_loc_id_dict(metaloc, demographics.LOCATIONS)

    # get long-term probabilities that will be used and
    # standardized-mortality ratios
    lt_probs = calculate_measures.long_term_probs_combined(ncode)

    smr = load_measures.smr(ncode)

    # For every year and sex create data in and keep track of all the results
    # in the value_data list.
    value_data = []
    for year in demographics.YEARS_EPI:
        for sex in demographics.SEXES:
            log.debug(f"Running year {year} and sex {sex}")

            measures = {}
            
            # get incidence
            incidence = calculate_measures.long_term_incidence(ecode, version, ncode, platform, year, sex, lt_probs)

            inc_mean = incidence.mean(dim='draw')
            # if the value is less then one in a trillion, set to 0. Otherwise, DisMod can have an overflow issue where
            #    it sets prevalence to 100%
            inc_summary = xr.merge([inc_mean.where(inc_mean > .000000000001, 0).rename('meas_value'),
                                    incidence.std(dim='draw').rename('meas_stdev')])
            measures['incidence'] = inc_summary
            if ncode in inj_info.EMR_NCODES:
                emr = calculate_measures.emr(smr, year, sex, flat_version)
                emr_summary = xr.merge([emr.mean(dim='draw').rename('meas_value'),
                                        emr.std(dim='draw').rename('meas_stdev')])
                measures['mtexcess'] = emr_summary

            data = make_data_in(measures, ecode, version, ncode, platform, locations, year, sex)

            value_data.append(data)

    # Create value in and save
    dm_out_dir = paths.DATA_DIR/config.DECOMP/inj_info.ECODE_PARENT[ecode]/str(version)/ecode/"dismod_ode"/f"{ecode}"
    make_value_in(value_data, ecode, ncode, platform, dm_out_dir)


if __name__ == '__main__':
    
    log.debug(f"Step Arguments: {sys.argv}")
    ecode    = str(sys.argv[1])
    ncode    = str(sys.argv[2])
    platform = str(sys.argv[3])
    version  = str(sys.argv[4])

    log.info("START")
    start = time.time()

    main(ecode, ncode, platform, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")