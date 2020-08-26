import pandas as pd
import getpass
import argparse

from uhc_estimation import specs, misc


def calc_counterfactual_burden():
    '''
    For a given service/population cell, get the risk-adjusted death rate for
    relevant indicators.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--uhc_id', help='Indicates tracer-service_population', type=int
    )
    parser.add_argument(
        '--uhc_version', help='Version number for run.', type=int
    )
    parser.add_argument(
        '--value_type', help='What are we storing,', type=str
    )
    args = parser.parse_args()

    # get tracers and service pop
    uhc_version_dir = FILEPATH

    # get efficacy tier
    uhc_df = pd.read_excel(FILEPATH)
    uhc_df = uhc_df.query("uhc_id == {}".format(args.uhc_id))
    efficacy = 1. - 0.2 * uhc_df['efficacy_tier'].values.item() + 0.1

    # read coverage and observed burden draws
    coverage_df = pd.read_hdf(FILEPATH)
    burden_df = pd.read_hdf(FILEPATH)

    # perform calculation:
    # counterfactual 0 is the burden we'd see if not for the intervention
    # couterfactual0 = observed / (1 - coverage * efficacy)
    # counterfactual 1 is the burden we'd see with 100% coverage of the intervention
    # couterfactual1 = couterfactual0 * (1 - 1 * efficacy)
    # the health gain weight is the difference
    # health gain weight = couterfactual0 - couterfactual1
    if len(coverage_df) + len(burden_df) == 0:
        summary_df = pd.DataFrame(columns=specs.ID_COLS + ['mean', 'lower', 'upper'])
    else:
        coverage_df[specs.DRAW_COLS] = (1 - coverage_df[specs.DRAW_COLS] * efficacy)
        burden_df =  misc.draw_math(
            [burden_df, coverage_df], specs.ID_COLS, specs.DRAW_COLS, '/'
        )
        burden_df[specs.DRAW_COLS] = burden_df[specs.DRAW_COLS] - (burden_df[specs.DRAW_COLS] * (1 - efficacy))
        burden_df['efficacy'] = efficacy
        summary_df = misc.summarize(burden_df, specs.DRAW_COLS)

    # store
    burden_df.to_hdf(FILEPATH)
    summary_df.to_csv(FILEPATH)


if __name__ == '__main__':
    calc_counterfactual_burden()
