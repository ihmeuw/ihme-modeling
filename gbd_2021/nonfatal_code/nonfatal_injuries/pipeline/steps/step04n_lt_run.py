"""
Launch the DisMod ODE Step for a ecode, ncode, platform, location, year
and sex

**Output**:

  A singel NetCDF file for the given ecode, ncode, platform and year.

**Overview**:

  1. Get locations to run DisMod ODE for

  2. Setup filepaths

  3. For each location

     4. For each sex

        5. *Is eta incidence 0?* Yes:

           5.1. Set all prevalence to 0

        5. *Is eta incidence 0?* No:

           5.2. Run DisMod ODE for the given ecode, location, sex, year...

  6. Save the Combined DisMod ODE results for all locations and sexes
"""
from multiprocessing import Pool
from itertools import product
import numpy as np
import os
import pandas as pd
import subprocess
import sys
import time

import db_queries as db

from gbd_inj.pipeline import config
from gbd_inj.pipeline.helpers import demographics, inj_info, paths, converters, utilities, input_manager
from gbd_inj.pipeline.helpers.monitor import PipelineMonitor
import gbd_inj.pipeline.infrastructure.pipeline_steps as steps
from gbd_inj.pipeline.helpers.pipeline_logging import InjuryLogger as logging

log = logging.getLogger(__name__)
logging.printConfig(log)


def run_dismod_ode(draw_in, data_in, value_in, plain_in, rate_in, effect_in, draw_out, n_draws):
    """ Launches the model_injury program"""
    if not draw_out.parent.exists():
        try:
            draw_out.parent.mkdir(parents=True)
        except:
            pass
    cmd = FILEPATH
    process = subprocess.run(cmd.split(), stdout=subprocess.DEVNULL)
    if process.returncode != 0:
        log.warning("model_injuries command failed. See error above.")
        return False

    print(".", end='')
    sys.stdout.flush()
    return True


def write_path(ecode, ncode, platform, year, version):
    return (FILEPATH
    )


def write_results(df, ecode, ncode, platform, year, version):
    """Write the results that are appended from the DisMod ODE."""
    # define filepaths
    out_dir = FILEPATH
    if not out_dir.exists():
        try:
            out_dir.mkdir(parents=True)
        except:
            pass

    # Convert from years to year_group_id
    df['age_group_id'] = df.age.replace(demographics.YEARS_TO_AGE_GROUP_ID)
    df.drop(columns=['age'], inplace=True)
    
    df.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id', 'platform'], inplace = True)
    arr = converters.df_to_xr(df, wide_dim_name='draw', fill_value=np.nan)

    # Save results
    filepath = write_path(ecode, ncode, platform, year, version)
    arr.to_netcdf(filepath)


def main(ecode, ncode, platform, year, version):
    # Load Flat & Demographic settings
    flat_version = input_manager.input_with_id(version).demographics_version
    
    dems = db.get_demographics(gbd_team="epi", gbd_round_id=config.GBD_ROUND)
    metaloc = db.get_location_metadata(location_set_id=config.LOCATION_SET_ID,
                                       gbd_round_id=config.GBD_ROUND)
    locations = utilities.ihme_loc_id_dict(metaloc, dems['location_id'])

    # Setup file paths
    dm_dir = FILEPATH
    print(f"Using {dm_dir}")

    # Dismod files that are used for all DisMod ODE runs:
    value_in  = FILEPATH
    draw_in   = FILEPATH
    plain_in  = FILEPATH
    effect_in = FILEPATH
    # Dismod files that change by location and sex:
    data_in_files = [
        FILEPATH
        for location in locations.values()
        for sex in [1,2]
    ]
    rate_in_name = "rate_in_emr.csv" if ncode in inj_info.EMR_NCODES else "rate_in_no_emr.csv"            
    rate_in_files = [
        FILEPATH
        for location in locations.values()
        for sex in [1,2]
    ]
    draw_out_files = [
        FILEPATH
        for location in locations.values()
        for sex in [1,2]
    ]

   
    v_in = pd.read_csv(value_in)
    if float(v_in.loc[v_in['name']=='eta_incidence','value'][0]) == 0:
        log.debug("eta incidence is 0 so all incidence should be 0. Skipping ODE")
        # Build a datafarme with all the age/sex/location/year/platform
        result = pd.DataFrame(
            product(
                demographics.AGE_DF.age_years_start,
                [1,2], # Sex
                demographics.LOCATIONS,
                [year],
                [platform]
            ),
            columns = ["age", "sex_id", "location_id", "year_id", "platform"]
        )
        # Make draw columns and set them to 0
        result = result.assign(**{d: 0 for d in utilities.drawcols()})
        # Save the results & finish running
        write_results(result, ecode, ncode, platform, year, version)
        return

    # Combine all the files, the result is a list of arguments to pass to DisMod ODE
    arguments = (
        (draw_in, data_in_files[i], value_in, plain_in, rate_in_files[i], effect_in, draw_out_files[i], config.DRAWS)
        for i in range(len(data_in_files))
    )

    # Run DisMod ODE in parallel, running as many jobs as we have threads for
    # this step
    log.info(f"Running DisMod ODE for {2*len(locations)} sex/locations combos. Each `.` signals a single combo has finished.")
    with Pool(steps.step_5.resources.threads) as pool:
        did_complete = pool.starmap(run_dismod_ode, arguments)
        if not all(did_complete):
            log.info("Some DisMod ODE steps failed to run. Saving results that did finish.")
    log.info("Finished DisMod ODE")
    
    # Cleanup results, add location and sex back onto them:
    log.info("Aggregating results...")
    dfs = []
    for loc_id, location in locations.items():
        for sex in [1,2]:
            draw_out_file = FILEPATH
            if not draw_out_file.exists():
                log.info(f"{location}, sex={sex} did not finish running")
            tmp = pd.read_csv(draw_out_file)
            tmp['location_id'] = loc_id
            tmp['sex_id'] = sex
            dfs.append(tmp)
    df = pd.concat(dfs)
    df['year_id'] = int(year)
    df['platform'] = platform

    log.info("Saving...")
    write_results(df, ecode, ncode, platform, year, version)
    
if __name__ == '__main__':
    log.debug(f"Step Arguments: {sys.argv}")
    ecode    = str(sys.argv[1])
    ncode    = str(sys.argv[2])
    platform = str(sys.argv[3])
    year     = int(sys.argv[4])
    version  = int(sys.argv[5])

    log.info("START")
    start = time.time()
    
    main(ecode, ncode, platform, year, version)

    log.info(f"TOTAL TIME: {time.time() - start: 0.2f}")
