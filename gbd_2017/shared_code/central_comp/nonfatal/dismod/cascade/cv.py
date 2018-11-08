def enforce_min_cv(priors, integrand_bounds, child_level):
    '''
    Given a dataframe of priors and a dataframe of integrand bounds,
    ensure that each row of priors obeys the appropriate min_cv setting
    for the given location level.


    Arguments:
        priors (pandas.dataframe): A dataframe of priors. The following columns
        must exist: integrand, meas_stdev, and meas_value.

        integrand_bounds (pandas.dataframe): A dataframe of location level
        specific bounds per integrand. Each row represents a different
        integrand, and the dataframe is wide on location level (ie has
        columns 'atom', 'region', etc)

        child_level (Str): The child location level of the priors dataset.
        IE if the priors dataset is from a 'subreg' country, then child_level
        would be 'atom' (aka a subnational node).


     Returns:
        A dataframe of priors, possibly with mutated meas_stdev
        values. The dataframe will have 2 additional columns:
        min_loc_cv, cv.

    '''
    min_cvs = integrand_bounds[['integrand', child_level]]
    min_cvs.rename(columns={child_level: 'min_loc_cv'}, inplace=True)
    priors = priors.merge(min_cvs, on='integrand', how='left')

    priors['cv'] = priors['min_loc_cv']

    def calc_stdev(row):
        val = row['meas_value']
        cv = row['cv']
        igd = row['integrand']
        eta = float(integrand_bounds.ix[
            integrand_bounds.integrand == igd, 'eta'].values[0])
        cv_std = cv * (val + eta)
        std = max(cv_std, row['meas_stdev'])
        return std

    priors['meas_stdev'] = priors.apply(
        calc_stdev, axis=1)
    return priors
