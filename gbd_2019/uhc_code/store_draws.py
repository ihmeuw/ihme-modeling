import numpy as np
import pandas as pd
import getpass
import argparse

from uhc_estimation import specs, misc, uhc_io


def scale_coverage(df, draw_parameters, log_scale=False, min_pctile=0.025, max_pctile=0.975):
    '''
    Turn draws of outcomes into a 0-1 scale (which we are calling coverage).
    '''
    if log_scale:
        df[specs.DRAW_COLS] = np.log(df[specs.DRAW_COLS])
    scale_df = df.loc[df.location_id.isin(draw_parameters['scale_location_id'])]
    low_scalar = scale_df[specs.DRAW_COLS].quantile(min_pctile)
    # low_scalar = 0
    high_scalar = scale_df[specs.DRAW_COLS].quantile(max_pctile)
    df[specs.DRAW_COLS] = (
        1 - (df[specs.DRAW_COLS] - low_scalar) / (high_scalar - low_scalar)
    ).clip(lower=0, upper=1)

    return df


def delete_attributable(df, draw_parameters, uhc_version_dir, uhc_id, spec_args, service_proxy):
    '''
    Where we have overlap in health gains, remove attributable burden from
    outcome proxy cause based on the specified risks.
    '''
    draw_parameters['to_check'] = ['location_id', 'year_id']
    rei_dfs = []
    for rei in specs.PAF_DELETION[service_proxy]:
        # add rei_id to kwargs for get_draws call
        rei_spec_args = spec_args.copy()
        rei_spec_args['gbd_id'] = rei_spec_args['gbd_id'] + [rei]
        rei_spec_args['gbd_id_type'] = rei_spec_args['gbd_id_type'] + ['rei_id']

        # retrieve attributable count, convert to PAF (doing it this way because it is easier than adding metric_id 2 to uhc_io)
        rei_df = uhc_io.fetch_burden_draws(
            draw_parameters, uhc_version_dir, uhc_id, **rei_spec_args
        )
        rei_df = misc.draw_math(
            [rei_df, df], specs.ID_COLS, specs.DRAW_COLS, '/'
        )

        # convert to (1 - PAF) for use in multiplicative formula below
        rei_df[specs.DRAW_COLS] = 1 - rei_df[specs.DRAW_COLS]
        rei_dfs.append(rei_df)

    # combine using 1 - (1 - PAF) * (1 - PAF)...
    rei_df = misc.draw_math(
        rei_dfs, specs.ID_COLS, specs.DRAW_COLS, '*'
    )
    rei_df[specs.DRAW_COLS] = 1 - rei_df[specs.DRAW_COLS]

    # convert back to metric_id 1, then remove from cause DALYs
    rei_df = misc.draw_math(
        [df, rei_df], specs.ID_COLS, specs.DRAW_COLS, '*'
    )
    df = misc.draw_math(
        [df, rei_df], specs.ID_COLS, specs.DRAW_COLS, '-'
    )

    return df


def birth_rate_to_yll(df, uhc_version_dir):
    '''
    Take a birth rate, convert to death numbers, convert to YLLs.

    NOT IDEAL: manually enter theoretical minimum risk life expectancy at birth
        that is returned by the below R functionality (02/26/2020).

    library(mortdb, lib.loc="/ihme/mortality/shared/r/")
    df <- get_mort_outputs(model_name="theoretical minimum risk life table",
                           model_type="estimate",
                           gbd_year = 2019)
    df[life_table_parameter_id == 5 & estimate_stage_id == 6 & precise_age == 0]$mean
    [1] 88.8719
    '''
    yll_exp = 88.8719
    birth_df = pd.read_hdf(FILEPATH)
    birth_df = birth_df.loc[(birth_df.age_group_id == 164) & (birth_df.sex_id == 3)]
    df = df.merge(birth_df)
    df[specs.DRAW_COLS] = (df[specs.DRAW_COLS].values.transpose() * df['population'].values).transpose() * yll_exp

    return df[specs.ID_COLS + specs.DRAW_COLS]


def store_uhc_data():
    '''
    For a given service/population cell, get some stuff.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_type', help='What is this type of data.', type=str
    )
    parser.add_argument(
        '--uhc_id', help='Indicates service_proxy-service_population', type=int
    )
    parser.add_argument(
        '--uhc_version', help='Version number for run.', type=int
    )
    parser.add_argument(
        '--value_type', help='What are we storing,', type=str
    )
    args = parser.parse_args()

    # get tracers and draw parameters for service pop-proxy
    uhc_version_dir = specs.UHC_DIR + '/{}'.format(args.uhc_version)
    # The line below assumes you are storing the uhc indicator file and code in your H drive
    uhc_df = pd.read_excel(FILEPATH)
    uhc_df = uhc_df.query("uhc_id == {}".format(args.uhc_id))
    service_proxy = uhc_df.service_proxy.values.item()
    service_population = uhc_df.service_population.values.item()
    draw_parameters = uhc_io.get_draw_parameters(
        service_proxy, service_population, uhc_version_dir, args.value_type
    )

    # collect draws
    spec_args = specs.PROXIES[args.data_type][service_proxy]
    if 'age_group_id' in spec_args.keys():
        draw_parameters['age_group_id'] = spec_args['age_group_id']
    if args.value_type == 'coverage':
        print('Reading draws')
        print(spec_args)
        df = uhc_io.COVERAGE_READER[args.data_type](
            draw_parameters, uhc_version_dir, args.uhc_id, **spec_args
        )
        print('Summarizing draws')
        summary_df = misc.summarize(df, specs.DRAW_COLS)
        summary_df['units'] = 'natural'
        if args.data_type in specs.RESCALE:
            print('Scaling draws')
            if args.data_type in specs.LOG_SCALE:
                df = scale_coverage(df, draw_parameters, True)
            else:
                df = scale_coverage(df, draw_parameters)
            scale_summary_df = misc.summarize(df, specs.DRAW_COLS)
            scale_summary_df['units'] = 'scaled'
            summary_df = summary_df.append(scale_summary_df)


    elif args.value_type == 'observed_burden':
        print('Reading draws')
        if args.data_type == 'vaccines':
            dfs = []
            for gbd_id, gbd_id_type in zip(spec_args['gbd_id'], spec_args['gbd_id_type']):
                df = uhc_io.fetch_burden_draws(
                    draw_parameters, uhc_version_dir, args.uhc_id, gbd_id=gbd_id, gbd_id_type=gbd_id_type
                )
                dfs.append(df)
            df = pd.concat(dfs)
            df = df.groupby(specs.ID_COLS, as_index=False)[specs.DRAW_COLS].sum()

        elif args.data_type == 'birth_mort':
            df = uhc_io.COVERAGE_READER[args.data_type](
                draw_parameters, uhc_version_dir, args.uhc_id, **spec_args
            )
            df = birth_rate_to_yll(df, uhc_version_dir)
        else:
            print(spec_args)
            df = uhc_io.fetch_burden_draws(
                draw_parameters, uhc_version_dir, args.uhc_id, **spec_args
            )

        # PAF deletion
        # if service_proxy in specs.PAF_DELETION.keys():
        #     df = delete_attributable(df, draw_parameters, uhc_version_dir, args, spec_args, service_proxy)

        # # split overlap
        # if service_proxy in specs.SPLIT_OVERLAP.keys():
        #     df[specs.DRAW_COLS] = df[specs.DRAW_COLS] * specs.SPLIT_OVERLAP[service_proxy]

        print('Summarizing draws')
        summary_df = misc.summarize(df, specs.DRAW_COLS)

    # store
    print('Storing data')
    df.to_hdf(FILEPATH)
    summary_df.to_csv(FILEPATH)


if __name__ == '__main__':
    store_uhc_data()