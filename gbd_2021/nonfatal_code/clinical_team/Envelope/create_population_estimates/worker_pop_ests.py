"""
Work through all the methods of the PopEstimates class
any real debugging should be done in PopEstimates.py
"""
import time
import sys
import pandas as pd

from clinical_info.Envelope.create_population_estimates import PopEstimates
from clinical_info.Functions import hosp_prep


if __name__ == "__main__":
    start = time.time()
    age = int(sys.argv[1])
    sex = int(sys.argv[2])
    run_id = int(sys.argv[3])
    decomp_step = sys.argv[4]
    gbd_round_id = int(sys.argv[5])
    env_path = sys.argv[6]

    # Initialize PopEstimates subclasses
    hue = PopEstimates.HUE(
        name="hue",
        run_id=run_id,
        age=age,
        sex=sex,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        env_path=env_path,
    )

    full_cover = PopEstimates.FullCoverage(
        name="full_coverage",
        run_id=run_id,
        age=age,
        sex=sex,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
    )

    # Run the main func of each class
    full_cover.FullCoverMain()
    hue.HueMain()

    # write an hdf file to disk
    fbase = f"FILEPATH"

    # write hue and fully covered data
    hosp_prep.write_hosp_file(
        pd.concat([hue.df, hue.mat_df], sort=False, ignore_index=True),
        f"{fbase}_df_env.H5",
        backup=False,
    )
    hosp_prep.write_hosp_file(
        pd.concat([full_cover.df, full_cover.mat_df], sort=False, ignore_index=True),
        f"{fbase}_df_fc.H5",
        backup=False,
    )

    stop = time.time()
    rt = round((stop - start) / 60, 1)
    print(f"run time was {rt} minutes")
