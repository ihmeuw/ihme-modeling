
import pickle
import sys
import pandas as pd
from clinical_info.NoiseReduction import ClinicalNR


if __name__ == '__main__':

    # testing in development
    if sys.argv[1] == '-f':
        bundle_id = 258
        estimate_id = 21
        sex_id = 2
        group_location_id = 102

        model_group = f'{bundle_id}_{estimate_id}_{sex_id}_{group_location_id}'
        run_id = int('22')
    else:
        model_group = sys.argv[1]
        run_id = int(sys.argv[2])

    base = FILEPATH
    read_path = FILEPATH
    csv_path = FILEPATH
    pickle_path = FILEPATH

    nr = ClinicalNR.ClinicalNR(
            name='whoami',
            run_id=run_id,
            subnational=True,
            df_path=read_path,
            model_group=model_group,
            model_type='Poisson',
            model_failure_sub='fill_average',
            cols_to_nr=['ms_mean'])

    # read method
    nr.ReadDF()
    pre = len(nr.df)

    # agg to national
    nr.create_national_df()

    # but actually they need to be assigned to our standard df obj
    nr.df, nr.df_model_object = nr.fit_model(nr.df.copy())
    nr.national_df, nr.nat_df_model_object = nr.fit_model(nr.national_df.copy())

    # do noise reduction
    nr.df = nr.noise_reduce(nr.df)
    nr.national_df = nr.noise_reduce(nr.national_df)


    nr.df['is_nat'] = 0
    nr.national_df['is_nat'] = 1
    nr.raked_df = pd.concat([nr.df, nr.national_df], sort=False)

    # do raking
    nr.raking()
    # drop the input columns
    nr.raked_df.drop(nr.nr_cols, axis=1, inplace=True)

    # extract the subnational raked data
    nr.df = nr.raked_df.query("is_nat == 0").copy()
    assert len(nr.df) == pre

    # apply the floor!
    pre_min = nr.df.ms_mean_final.min()
    nr.apply_floor()
    post_min = nr.df.loc[nr.df.ms_mean_final > 0, 'ms_mean_final'].min()
    assert post_min >= pre_min, "The floor application seems to have failed"


    floor_path = f'{base}FILEPATH/'
    frev = nr.df.query("ms_mean_final == 0 and ms_mean > 0")
    if len(frev) > 0:
        frev.to_csv(f"FILEPATH", index=False)

    # store a pickle with everything!
    pickle.dump(nr, open(f"{pickle_path}/FILEPATH{nr.model_group}", "wb"))

    # store a csv of just df obj
    nr.df.to_csv(f"{csv_path}/FILEPATH{nr.model_group}", index=False)
