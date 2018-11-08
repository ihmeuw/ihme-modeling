####################################################################
# Author: USERNAME
# Date: 8/22/2017
# Purpose: Child Script for Creating Shock Incidence
##################################################################


def get_shock_mort(ecode, pops, locs, ages, year_id, sex_id):
    """Function to get shock mortality from CODEm."""
    print('calling get_draws for mortality')
    sys.stdout.flush()
    cause_id = help.get_cause(ecode)
    draws = gd.get_draws(
        gbd_id_type="cause_id",
        gbd_id=cause_id,
        location_id=locs,
        year_id=year_id,
        sex_id=sex_id,
        age_group_id=ages,
        status="best",
        version_id=89,
        source="codcorrect",
        num_workers=40,
        measure_id=1,
        gbd_round_id=help.GBD_ROUND)
    draws.drop(['cause_id', 'measure_id', 'metric_id'], axis=1, inplace=True)
    draws.set_index(['location_id', 'year_id', 'sex_id', 'age_group_id'], inplace=True)
    mort = etl.df_to_xr(draws, wide_dim_name='draw', fill_value=np.nan)
    mort = mort / pops['population']  # gets it into rate space
    return mort


def get_ratios(ratio_file, ecode, water=False):
    """Function to read the ratio file for the correct e-codes and
    collapse them if necessary."""
    if ecode == "inj_war_warterror":
        sub = ["inj_trans_road",'inj_homicide']
    elif ecode == "inj_war_execution":
        sub = ["inj_homicide"]
    elif ecode == 'inj_disaster':
        if water:
            sub = ["inj_trans_road", "inj_drowning"]
        else:
            sub = ["inj_trans_road"]
    else:
        raise ValueError('Invalid ecode. Must be one of inj_war_warterror, inj_war_execution, or inj_disaster')
    
    # take the mean over the ecodes -- only changes things if function pulls
    # multiple e-codes (road/drowning)
    return ratio_file.loc[{'ecode': sub}].mean(dim='ecode')
    

def aqua(locs, year_id):
    """Function that returns a logical vector for each location in this year:
    did the location-year have a location-related disaster?"""
    disasters = pd.read_csv('FILEPATH.csv')
    water_causes = [988, 989]  # cataclysmic storm, flood
    locations = disasters.loc[(disasters['cause_id'].isin(water_causes)) & (disasters['year_id'] == year_id), 'location_id'].unique()
    return {'water': [x for x in locs if x in locations], 'no_water': [x for x in locs if x not in locations]}


def get_shock_inc(ecode, pops, dems, year_id, sex_id, ratio_file, regmap):
    """Function to impute shock incidence based on shock mortality and incidence:mortality ratios.
    The EMDAT info for water years may be at the national level only, but we don't necessarily want to
    propogate that to every subnational, so only assert the water-related disaster part for national (or
    subnationally-coded) locations."""
    mort = get_shock_mort(ecode, pops, dems['location_id'], dems['age_group_id'], year_id, sex_id)
    print('got mortality, now applying ratios')
    sys.stdout.flush()
    
    if ecode == 'inj_disaster':
        dis_type = aqua(dems['location_id'], year_id)
        
        if len(dis_type['water']) > 0 and len(dis_type['no_water']) > 0:
            aqua_ratio = get_ratios(ratio_file, ecode, water=True)
            noaq_ratio = get_ratios(ratio_file, ecode, water=False)
            
            incidence = xr.concat([mort.loc[{'location_id': dis_type['water']}].groupby(regmap.loc[{'location_id': dis_type['water']}]) * aqua_ratio,
                                  mort.loc[{'location_id': dis_type['no_water']}].groupby(regmap.loc[{'location_id': dis_type['no_water']}]) * noaq_ratio], dim='location_id')
        elif len(dis_type['no_water']) > 0:
            noaq_ratio = get_ratios(ratio_file, ecode, water=False)
            incidence = mort.groupby(regmap) * noaq_ratio
        elif len(dis_type['water']) > 0:
            aqua_ratio = get_ratios(ratio_file, ecode, water=True)
            incidence = mort.groupby(regmap) * aqua_ratio
        else:
            raise ValueError('Something went wrong, there\'s nothing in dis_type')
    else:
        ratio = get_ratios(ratio_file, ecode)
        incidence = mort.groupby(regmap) * ratio
    
    incidence = incidence.drop('super_region_id')
    # replace incidence with .5 if it is greater than .5. we do this because sometimes the ratios can create unrealistic incidence if the
    # mortality is really high -- like with genocides.
    incidence = incidence.where(incidence < .5, .5)
    
    return incidence


def get_region_map(location_ids):
    reg = db.get_location_metadata(location_set_id=35, gbd_round_id=help.GBD_ROUND)[['location_id', 'super_region_id']]
    reg = reg.loc[reg['location_id'].isin(location_ids)]
    return reg.set_index('location_id')['super_region_id'].to_xarray()


def write_results(arr, ecode, version, year_id, sex_id):
    filename = "FILEPATH.nc".format(ecode, year_id, sex_id)
    folder = os.path.join("FILEPATH")
    if not os.path.exists(folder):
        try:
            os.makedirs(folder)
        except OSError as e:
            if e.errno != os.errno.EEXIST:
                raise
            pass
    filepath = os.path.join(folder, filename)
    print("Writing netCDF")
    arr.to_netcdf(filepath)


def main(ecode, year_id, sex_id, version):
    tic = time.time()

    dems = db.get_demographics(gbd_team="epi", gbd_round_id=help.GBD_ROUND)

    pops = xr.open_dataset(os.path.join('FILEPATH.nc'))
    pops = pops.loc[{'year_id': [year_id], 'sex_id': [sex_id]}]
    
    # get the ratio file
    ratio_file = xr.open_dataarray(
        os.path.join('FILEPATH.nc'))

    regmap = get_region_map(dems['location_id'])

    print("Set up complete, now pulling incidence for shocks")
    sys.stdout.flush()  # write to log file
    incidence = get_shock_inc(ecode, pops, dems, year_id,
                              sex_id, ratio_file, regmap)

    write_results(incidence, ecode, version, year_id, sex_id)
    toc = time.time()
    total = toc - tic
    print("Total time was {} seconds".format(total))


if __name__ == '__main__':
    ecode = 'inj_disaster'
    year_id = 1980
    sex_id = 1
    version = "1"
    repo = "FILEPATH"

    main(ecode, year_id, sex_id, version, repo)
