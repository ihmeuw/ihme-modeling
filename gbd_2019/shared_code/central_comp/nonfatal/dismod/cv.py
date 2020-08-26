def enforce_min_cv(priors, integrand_bounds, child_level):
    '''
    Given a dataframe of priors and a dataframe of integrand bounds,
    ensure that each row of priors obeys the appropriate min_cv setting
    for the given location level.

    There are 2 points where this function is used:

        After each dismod call, if there are child locations,
        the cascade must summarize draws of priors to be used by child nodes.
        During that summarization, one operation
        is to make sure that the coefficient of variation for each datapoint
        meets some minimum bound defined by model settings.

        For subnational locations, there is a mean shifting operation (see
        cascade_loc.shift_priors). The mean shift operation could undo the
        min_cv enforcement so this function is called again immediately
        after the shift.

    Algorithm description:
        For every row of data, we calculate what the standard deviation should
        be given the min CV model settings. Then, for every row, we update the
        meas_stdev column iff the existing meas_stdev value is smaller than
        this calculated stdev based on settings.

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
        '''
        Calculate what the meas_stdev should be, given the
        new cv value. Then replace meas_stdev with this new stdev
        value for any row where the new value is higher'''
        val = row['meas_value']
        cv = row['cv']
        igd = row['integrand']
        eta = float(integrand_bounds.loc[
            integrand_bounds.integrand == igd, 'eta'].values[0])
        cv_std = cv * (val + eta)
        std = max(cv_std, row['meas_stdev'])
        return std

    priors['meas_stdev'] = priors.apply(
        calc_stdev, axis=1)
    return priors


def enforce_hybrid_cv(priors, integrand_bounds, child_level):
    r"""
    The hybrid enforcement of min_cv is takes the standard
    deviation, :math:`\sigma`, the cv, :math:`\nu_{ae}` for integrand
    :math:`e` and area :math:`a`, and the integrand-specific :math:`\eta_e`,

    .. math::

        \sigma' = \mbox{max}(\sigma, \nu_{ae} * \sigma, \nu_{ae} * \eta_e)

    which assigns a new standard deviation.

    Args:
        priors (pd.DataFrame): Must have ``meas_value`` and ``meas_stdev``.
        integrand_bounds (pd.DataFrame): These are model parameters by
            integrand and level. It should have columns that are
            ``integrand``, the given child_level, and ``eta``.
        child_level (str): One of the area types: super, region, subreg, atom.

    Returns:
        pd.DataFrame: The priors, with the ``meas_stdev`` column
        modified as described above and additional columns for ``cv``
        and ``min_loc_cv`` which equals the ``cv`` column.
    """
    min_cvs = integrand_bounds[['integrand', child_level, 'eta']]
    min_cvs = min_cvs.rename(columns={child_level: 'cv'})
    min_cvs = min_cvs.assign(
        eta=min_cvs.eta.astype(float),
        cv=min_cvs.cv.astype(float)
    )
    # The input may, or may not, have cv and eta columns. Drop them.
    duplicate_columns = {'cv', 'eta'} & set(priors.columns)
    if duplicate_columns:
        priors = priors.drop(columns=duplicate_columns)
    working = priors.merge(min_cvs, on='integrand', how='left', sort=False)
    working = working.assign(
        min_cv=working.cv * working.meas_value,
        eta_cv=working.cv * working.eta,
    )
    largest = working[['meas_stdev', 'min_cv', 'eta_cv']].max(axis=1)
    working = working.assign(meas_stdev=largest, min_loc_cv=working.cv)
    working = working.drop(columns=["min_cv", "eta_cv", "eta"])
    return working
