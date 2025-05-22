"""
Work through all the methods of the PopEstimates class
any real debugging should be done in PopEstimates.py
"""
import sys
import time

from crosscutting_functions import general_purpose

from inpatient.Envelope.create_population_estimates import PopEstimates

if __name__ == "__main__":
    start = time.time()
    age = int(sys.argv[1])
    sex = int(sys.argv[2])
    run_id = int(sys.argv[3])
    draws = int(sys.argv[4])
    env_path = sys.argv[5]

    # Initialize PopEstimates subclasses
    hue = PopEstimates.HUE(
        name="hue",
        run_id=run_id,
        age=age,
        sex=sex,
        draws=draws,
        env_path=env_path,
    )

    full_cover = PopEstimates.FullCoverage(
        name="full_coverage",
        run_id=run_id,
        age=age,
        sex=sex,
        draws=draws,
    )

    # Run the main func of each class
    full_cover.FullCoverMain()
    hue.HueMain()

    # write an hdf file to disk
    fbase = (FILEPATH
    )

    # write hue and fully covered data
    if hue.df is not None:
        general_purpose.write_hosp_file(
            hue.df,
            f"{fbase}_df_env.H5",
            backup=False,
        )
    if full_cover.df is not None:
        general_purpose.write_hosp_file(
            full_cover.df,
            f"{fbase}_df_fc.H5",
            backup=False,
        )

    stop = time.time()
    rt = round((stop - start) / 60, 1)
    print(f"run time was {rt} minutes")
