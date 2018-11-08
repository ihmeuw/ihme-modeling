# This file contains all the functions that pull or create some kind of measure. These should all return dataframes
# indexed by some combination of demographics (location, year, age, sex, ecode, ncode, platform) and should mostly have
# 1000 draws.
# Examples include fake long term, durations, e-n matrices, probability long term, short term incidence, etc


def short_term_incidence_unsplit(raw_incidence, remission, emr, outpatient_cov):
    inpatient = raw_incidence * np.exp(-1 * (emr / remission))
    outpatient = inpatient * outpatient_cov
    combo = xr.concat([inpatient, outpatient], pd.Index(['inpatient', 'outpatient'], name='platform'))
    return combo


def pct_treated(min_treat=0.1, max_haqi=75, year_id='all'):
    """Function to pull the covariate value for HAQI Index. Used in both
    short-term durations and the long-term disability weights.

    Min Treat: What is the minimum percent treated for their injuries anywhere in the world?
                (across all years, so the lowest % treated in the current year is likely higher)
    Max HAQI: What is the maximum value of HAQI to cap it at? We don't want large variations among high income.
                (all HAQI values above this will have 100% treated)"""
    dems = db.get_demographics(gbd_team='epi', gbd_round_id=help.GBD_ROUND)
    locations = dems['location_id']
    haqi = db.get_covariate_estimates(covariate_id=1099, location_id=locations, gbd_round_id=help.GBD_ROUND)
    
    # get mean and SE from upper and lower
    haqi["se"] = (haqi["upper_value"] - haqi["lower_value"]) / 3.92
    
    # generate draws of HAQI
    np.random.seed(10191948)
    haqi[help.drawcols()] = pd.DataFrame(np.random.normal(haqi['mean_value'], haqi['se'], size=(1000, len(haqi))).T)

    haqi.drop(['mean_value', 'se', 'lower_value', 'upper_value', 'model_version_id', 'covariate_id',
               'covariate_name_short', 'location_name', 'age_group_id', 'age_group_name', 'sex_id'],
              axis=1, inplace=True)
    
    # Transform the draws so that it is the % treated:
    haqi.set_index(['location_id', 'year_id'], inplace=True)
    haqi[haqi > max_haqi] = max_haqi
    xhaq = etl.df_to_xr(haqi, wide_dim_name='draw', fill_value=np.nan)
    dims = ['location_id', 'year_id']
    p_treated = min_treat + ((1-min_treat) * (xhaq - xhaq.min(dim=dims)) / (xhaq.max(dim=dims) - xhaq.min(dim=dims)))
    
    if year_id == 'all':
        return p_treated.loc[{'year_id': dems['year_id']}]
    elif year_id == 'full':
        return p_treated
    else:
        return p_treated.loc[{'year_id': [year_id]}]


def get_durations(pct_treated, ncode='all'):
    """ Gets short-term duration for a specific ncode, platform, and year.
    
    :param pct_treated: xarray DataArray with coordinates location_id, year_id, and draw
    :return: xarray DataArray with coordinates location_id, year_id, draw, ncode, platform
    """

    treated_dur = etl.df_to_xr(pd.read_csv(os.path.join(paths.INPUT_DIR,'FILEPATH.csv')).set_index(['ncode','platform']),
                               wide_dim_name='draw', fill_value=np.nan)
    untreat_dur = etl.df_to_xr(pd.read_csv(os.path.join(paths.INPUT_DIR, 'FILEPATH.csv')).set_index(['ncode','platform']),
                               wide_dim_name='draw', fill_value=np.nan)
    
    if ncode != 'all':
        ncode = np.atleast_1d(ncode).tolist()
        treated_dur = treated_dur.loc[{'ncode': ncode}]
        untreat_dur = treated_dur.loc[{'ncode': ncode}]
    
    durations = (pct_treated * treated_dur) + ((1 - pct_treated) * untreat_dur)
    return durations


def compute_prevalence(incidence, durations):
    """Computes prevalence from incidence and durations. Simple multiplication by N-Code."""
    return (incidence * durations) / (1 + (incidence * durations))


def short_term_ylds(prevalence, dws):
    """Computes YLDs from prevalence and disability weights. Simple multiplication."""
    return prevalence * dws


def long_term_probs_treated(ncode):
    """ Gets the raw long-term probabilities from a previous step output (not re-run here) and formats them
    to be merged with the short-term draws."""
    probs = pd.read_csv("FILEPATH.csv")
    
    # dropping "all long term" ncodes because they all have probabilities of 1 -- rows correspond
    # to all long-term N-codes
    probs = probs[np.logical_not(probs["n_code"].isin([x for x in inj_info.LT_NCODES if x not in inj_info.ST_NCODES]))]
    probs["inpatient"] = probs["inpatient"].astype(str)
    plat_dict = {'1': "inpatient", '0': "outpatient"}
    probs["inpatient"] = probs["inpatient"].replace(plat_dict)
    probs.rename(columns={"inpatient": "platform", "n_code": "ncode"}, inplace=True)
    ages = probs.age_gr.unique()
    inp_grid = transformations.create_lt_grid("inpatient", ages)
    otp_grid = transformations.create_lt_grid("outpatient", ages)
    grid = inp_grid.append(otp_grid)
    allprobs = probs.append(grid)
    allprobs.drop("draw_", axis=1, inplace=True)
    allprobs.set_index(['ncode', 'platform', 'age_gr'], inplace=True)
    allprobs["draw_sum"] = allprobs.sum(axis=1)
    allprobs.reset_index(inplace=True)
    allprobs["total_sum"] = allprobs.groupby(['ncode', 'platform'])["draw_sum"].transform('sum')
    finalprobs = allprobs.loc[allprobs["total_sum"] != 0].reset_index()
    finalprobs.drop(['total_sum', 'draw_sum'], inplace=True, axis=1)
    finalprobs.rename(columns={"age_gr": "age"}, inplace=True)
    finalprobs = help.convert_to_age_group_id(finalprobs, collapsed_0=True)
    finalprobs = help.expand_under_1(finalprobs.drop('index', axis=1))
    finalprobs = finalprobs.loc[finalprobs['ncode']==ncode]
    finalprobs.set_index(['ncode', 'platform', 'age_group_id'], inplace=True)
    
    x_probs = etl.df_to_xr(finalprobs, wide_dim_name='draw', fill_value=np.nan)
    
    return x_probs


def long_term_probs_untreated_multiplier(
        input_dir="FILEPATH"):
    """Gets the expert-driven multipliers for long-term *untreated* probabilities."""
    df = pd.read_excel(os.path.join(input_dir, "FILEPATH.xlsx"), skiprows=[0])
    df = df[['N-code', 'mean', 'LL', 'UL']]
    df.rename(columns={'N-code': 'ncode'}, inplace=True)
    
    df.loc[df["mean"] == "same", 'mean'] = 1
    df.loc[df["mean"] == 1, 'UL'] = 1
    df.loc[df["mean"] == 1, 'LL'] = 1
    df = df.loc[-df["mean"].isna()]
    df.loc[df["mean"] == "unlikely to survive", 'mean'] = "all"
    
    # create list of all-lt probs
    all = df.loc[df["mean"] == "all"].ncode.unique().tolist()
    all = [str(x) for x in all]
    
    df_mults = df.loc[np.logical_not(df["ncode"].isin(all))]
    df_mults["se"] = (df_mults["UL"] - df_mults["LL"]) / 3.92
    df_mults.drop(['UL', 'LL'], inplace=True, axis=1)
    df_mults["mean"] = df_mults["mean"].astype(float)
    df_mults["se"] = df_mults["se"].astype(float)
    
    df_mults.reset_index(drop=True, inplace=True)  # reset index so random draws line up
    np.random.seed(541916)
    df_mults[help.drawcols()] = pd.DataFrame(np.random.normal(df_mults['mean'], df_mults['se'], size=(1000, len(df_mults))).T)
    
    df_mults.drop(['mean', 'se'], inplace=True, axis=1)
    
    df_all = pd.DataFrame(columns=df_mults.columns)
    for i in range(len(all)):
        df_all.loc[i, 'ncode'] = all[i]
    
    df = df_mults.append(df_all)
    df['ncode'] = df['ncode'].astype(str)
    
    df.set_index('ncode', inplace=True)
    arr = etl.df_to_xr(df, wide_dim_name='draw', fill_value=np.nan)
    
    return arr


def long_term_probs_combined(ncode, year_id='all'):
    """Merge the multipliers onto the long-term probabilities."""
    ut_mults = long_term_probs_untreated_multiplier()
    t_probs = long_term_probs_treated(ncode)
    
    # grab percent treated
    print("Getting HAQI pct treated values...")
    pct_t = pct_treated(min_treat=0.1, max_haqi=75,
                       year_id=year_id)
    ut_probs = t_probs * ut_mults
    if 'outpatient' in ut_probs.platform.values:
        ut_probs.loc[{'platform': 'outpatient'}] = ut_probs.loc[{'platform': 'outpatient'}].fillna(
            t_probs.loc[{'platform': 'outpatient'}])
    
    ut_probs = ut_probs.fillna(1)

    agg_prob = t_probs * pct_t + ut_probs * (1 - pct_t)
    agg_prob.values[agg_prob.values > 1] = 1
    
    return agg_prob


def long_term_incidence(ecode, version, ncode, platform, year_id, sex_id, lt_probs, location_id='all'):
    st_inc = load_measures.short_term_incidence_split(ecode, version, ncode, year_id, sex_id, platform, location_id)
    return st_inc * lt_probs.loc[{'ncode': [ncode]}]


def emr(smr, year_id, sex_id, flat_version, location_id='all'):
    mort = load_measures.mortality(flat_version, year_id, sex_id, location_id)
    return mort * (smr - 1)
