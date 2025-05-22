import pandas as pd

from ihme_cc_paf_aggregator.lib import aggregation


def aggregate_pafs(
    total_paf_df: pd.DataFrame,
    risk_hierarchy_df: pd.DataFrame,
    mediation_matrix: pd.DataFrame,
    unmediated_paf_df: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregates PAFs up a risk hierarchy.

    Invariants:
        Input data shouldn't have aggregate REIs
        Input data must contain all children REIs
        rei_id column present in input data
        at least one column present in input data that starts with 'draw'

    Arguments:
        total_paf_df: dataframe of input draws. Total PAFs are the standard output of the
            PAF calculator, produced for every risk-cause pair and uploaded using
            save_results. These PAFs represent the total estimate of direct and
            indirect risk.
        risk_hierarchy_df: dataframe of the risk hierarchy to aggregate
        mediation_matrix: dataframe that defines what risk-cause pairs are mediated
        unmediated_paf_df: dataframe of unmediated pafs. When mediation
            applies to a risk-cause pair, the PAF calculator outputs a second set of
            estimates, the unmediated PAFs, representing the portion that is due to
            exposure to the risk itself rather than mediated through exposure to
            another risk.

    Returns:
        dataframe with same schema as total_paf_df, but contains aggregate REI ids
        instead of children REI ids.
    """
    return aggregation.aggregate_pafs(
        total_paf_df=total_paf_df,
        risk_hierarchy_df=risk_hierarchy_df,
        unmediated_paf_df=unmediated_paf_df,
        mediation_matrix=mediation_matrix,
    )
