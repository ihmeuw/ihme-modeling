##########################################
## EMR Code for TB                      ##
## Date : June 13 2017                  ##
## Description : Uses prevalence        ##
## to calculate EMR based on CSMR,      ##
## Remission, ACMR, and Predicted EMR   ##
##########################################

# Import Libraries
from elmo import run
import pandas as pd
import numpy as np
from db_queries import get_envelope, get_model_results
from db_tools import ezfuncs
from db_tools.ezfuncs import query
import envs

########################################################################################################################
#Pulls prevalence data (not model results)

def get_cov_adjusted_data(model_version_id):
    # Internal function for cleaning up data
    def drop_zeros_nulls(df, mean_col, lower_col, upper_col):
        df = df.ix[df[mean_col] > 0]  # get rid of 0 means
        df = df.ix[
            (df[mean_col].notnull()) &
            (df[lower_col].notnull()) &
            (df[upper_col].notnull())]  # mean upper and lower not null
        return df

    _data_key = "input_data_key"

    # PART 1: Get metadata from the epi database
    demo_query = """
    SELECT
        t3mvd.model_version_dismod_id as {data_key},
        t3mvd.location_id,
        t3mvd.sex_id,
        t3mvd.year_start,
        t3mvd.year_end,
        t3mvd.age_start,
        t3mvd.age_end,
        t3mvd.measure_id,
        t3mvd.nid,
        t3mvd.underlying_nid,
        t3mvd.outlier_type_id
    FROM
        epi.t3_model_version_dismod t3mvd
    WHERE
        t3mvd.model_version_id = {model_version_id}
    """.format(data_key=_data_key, model_version_id=model_version_id)
    meta_df = query(demo_query, conn_def="epi")

    # subset out demographics
    meta_df = meta_df.ix[meta_df["sex_id"] != 3]  # get rid of both sex
    meta_df = meta_df.ix[
        ((meta_df["age_end"] - meta_df["age_start"]) < 20) |  # > 20 age group
        (meta_df["age_start"] >= 80)]  # or terminal

    if meta_df.empty:
        raise NoNonZeroValues

    # Add a year_id column
    meta_df['year_mid'] = (meta_df['year_start'] + meta_df['year_end'])/2
    gbd_years = [1990, 1995, 2000, 2005, 2010, 2016]
    meta_df['year_id'] = meta_df['year_mid'].apply(lambda x: min(gbd_years, key=lambda y: abs(y-x)))

    meta_df = meta_df.drop(labels=['year_mid'],axis=1)
    # set index
    meta_df[_data_key] = meta_df[_data_key].astype(int)
    meta_df = meta_df.set_index(_data_key)

    # PART 2: Get the actual covariate-adjusted data from the Epi file structure

    mean_col = "mean"
    se_col = "se"
    path = ("FILEPATH").format(mv=model_version_id)
    data_df = pd.read_csv(path)
    data_df = data_df[[_data_key, "mean", "lower", "upper"]]

    # subset
    data_df.drop_duplicates(inplace=True)
    data_df = drop_zeros_nulls(data_df, "mean", "lower", "upper")

    if data_df.empty:
        raise NoNonZeroValues

    # set index
    data_df[_data_key] = data_df[_data_key].astype(int)
    data_df = data_df.set_index(_data_key)

    # PART 3: Join raw data to metadata using the data key
    combined_df = pd.merge(left=meta_df, right=data_df, left_index=True, right_index=True)

    return combined_df

########################################################################################################################

#Define data template adjustment function (from Logan/Nat's code)

def span_intersect(span_1_low, span_1_high, span_2_low, span_2_high):
    if span_1_low >= span_2_low and span_1_high <= span_2_high:
        return 1
    elif span_1_low <= span_2_low and span_1_high > span_2_low:
        return 1
    elif span_1_high > span_2_high and span_1_low <= span_2_low:
        return 1
    else:
        # This means that there is no intersection between the two spans
        return 0

def get_span_lower(span_1_low, span_1_high, span_2_low, span_2_high):
    # Returns the proper lower span based on three potential cases
    # Case 1: The in span lower bound falls somewhere within the map span
    if span_1_low >= span_2_low and span_1_low <= span_2_high:
        # The proper lower bound comes from the in span
        return span_1_low
    # Case 2: the in span's upper bound falls within the map span, but the
    #  lower bound falls outside
    elif span_1_low < span_2_low and span_1_high >= span_2_low:
        # The proper lower bound comes from the map span
        return span_2_low
    # Otherwise, the in span does not intersect with the map span
    # Return 0
    else:
        return 0

def get_span_higher(span_1_low, span_1_high, span_2_low, span_2_high):
    # Returns the proper upper span based on three potential cases
    # Case 1: the in span upper bound falls somewhere within the map span
    if span_1_high <= span_2_high and span_1_high >= span_2_low:
        # The proper upper span comes from the in span
        return span_1_high
    # Case 2: the in span's lower bound falls within the map span, but the 
    #  upper bound falls outside
    elif span_1_low <= span_2_high and span_1_high > span_2_high:
        # The proper upper span comes from the map span
        return span_2_high
    # Case 3: the in span and the map span do not intersect
    else:
        # Return 0
        return 0


# Adjusts the span of the in_df to cross-cut the out_df
def adjust_span(in_df, in_span,
                map_df, map_span):
    in_span_low = in_span[0]
    in_span_high = in_span[1]
    map_span_low = map_span[0]
    map_span_high = map_span[1]
    
    in_df['cross'] = 1
    map_df['cross'] = 1
    # Cross multiply
    crossed = in_df.merge(map_df, on='cross')
    crossed['intersect'] = crossed.apply(lambda x: span_intersect(x[in_span_low],x[in_span_high],
                                                                  x[map_span_low],x[map_span_high]),
                                                                  axis=1)
    crossed['new_span_low'] = crossed.apply(lambda x: get_span_lower(x[in_span_low],x[in_span_high],
                                                                     x[map_span_low],x[map_span_high]),
                                                                     axis=1)
    crossed['new_span_high'] = crossed.apply(lambda x: get_span_higher(x[in_span_low],x[in_span_high],
                                                                      x[map_span_low],x[map_span_high]),
                                                                      axis=1)
    crossed = crossed.ix[crossed['intersect'] == 1]
    # drop the old span columns and the intersect column
    crossed = crossed.drop(labels=[in_span_low, in_span_high, 
                                    map_span_low, map_span_high,
                                    'intersect','cross'], axis=1)
    # Rename the new span columns
    crossed = crossed.rename(columns = {'new_span_low':in_span_low,
                                        'new_span_high':in_span_high})
    return crossed
    

def adj_data_template(df):
    # Returns the closest year that contains GBD results
    def set_gbd_year(in_year):
        if in_year < 1990: 
            return 1990
        elif in_year >= 2010:
            if in_year <= 2013:
                return 2010
            else:
                return 2016
        else:
            rounded_year = int(np.round(in_year/5) * 5)
            return rounded_year

    # Create the year_id column and set it to a year that contains GBD results
    df['year_id'] = df[['year_start','year_end']].mean(axis=1)
    df['year_id'] = df.apply(lambda x: set_gbd_year(x['year_id']),axis=1)

    # subset out demographics
    if 'sex_id' in df.columns:
        sex_dict = {1:'Male', 2:'Female', 3:'Both'}
        df['sex'] = df.apply(lambda x: sex_dict[x['sex_id']], axis=1)
    else:
        sex_dict = {'Male':1, 'male':1,
                    'Female':2, 'female':2,
                    'Both':3, 'both':3}
        df['sex_id'] = df.apply(lambda x: sex_dict[x_idx['sex']],axis=1)
    
    if 'age_start' in df.columns:
        df = df.ix[
            ((df["age_end"] - df["age_start"]) < 20) |  # > 20 age group
            (df["age_start"] >= 80)]  # or terminal
            
        #issue with oldest ages being dropped should be corrected with this.        
        df['age_end_copy'] = df['age_end'].copy()
        df['age_end'] = df['age_end'].apply(lambda x: 125 if x > 95 else x)

        # get age mapping
        ages_query = """
        SELECT
            age_group_id, age_group_years_start, age_group_years_end
        FROM
            shared.age_group
        WHERE
            age_group_id IN (2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,33);
        """
        age_map = ezfuncs.query(
            ages_query, conn_def=envs.Environment.get_odbc_key())
    
        # find the intersection between the in dataframe and the age group df
        df = adjust_span(df, ('age_start','age_end'),
                         age_map, ('age_group_years_start','age_group_years_end'))

        #replace age_end with original age_end data
        df['age_end'] = df['age_end_copy'].copy()
        df = df.drop(labels=['age_end_copy'],axis=1)
    else:
        pass

        if df.empty:
            raise NoNonZeroValues
	
    return df
 
#############################################################################

def all_fourplus_locs(df):
    # Get two dataframes containing 0-3 star and 4-5 star geographies
    fourplus = pd.read_csv("FILEPATH")
    fourplus_list = fourplus['location_id'].unique().tolist()
    df_sub_fourplus = df.loc[df['location_id'].apply(lambda x: x in fourplus_list)]
    return (df_sub_fourplus)

#don't currently use the threelesss function anywhere
def all_threeless_locs(df):
    fourplus = pd.read_csv("FILEPATH")
    fourplus_list = fourplus['location_id'].unique().tolist()
    df_sub_three = df.loc[df['location_id'].apply(lambda x: x not in fourplus_list)]
    return (df_sub_three)

################################################################################################

def get_emr_pred(model_version_id):
    path = ("/FILEPATH"
        "FILEPATH")
    path = path.format(mv=model_version_id)
    emrpred = pd.read_csv(path)
    emrpred = emrpred.ix[emrpred["measure_id"] == 9]
    emrpred["pred_se"] = (emrpred["pred_upper"] - emrpred["pred_lower"]) / (2*1.96)
    return emrpred

#can also use model_version_id instead of gbd_id
def combined_get_model_results(prev_filepath=None, inc_filepath=None, model_version_id=152216):
    age_ids = [2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,33]
    year_ids = [1990,1995,2000,2005,2010,2016]
    sex_ids = [1,2]
    location_id = [6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 22, 23, 24, 25, 26, 27, 28, 29, 30, 33, 34,
    35, 36, 37, 38, 39, 40, 41, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 57, 58, 59, 60, 61, 62, 63, 66, 
    67, 68, 69, 71, 72, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 97,
    98, 99, 101, 102, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 121, 122, 123, 125,
    126, 127, 128, 129, 130, 131, 132, 133, 135, 136, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150,
    151, 152, 153, 154, 155, 156, 157, 160, 161, 162, 163, 164, 165, 168, 169, 170, 171, 172, 173, 175, 176, 177,
    178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 189, 190, 191, 193, 194, 195, 196, 197, 198, 200, 201, 202,
    203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 298, 305, 349, 351, 354, 376,
    385, 422, 433, 434, 435, 521, 522, 523, 524, 525, 526, 527, 528, 529, 530, 531, 532, 533, 534, 535, 536, 537,
    538, 539, 540, 541, 542, 543, 544, 545, 546, 547, 548, 549, 550, 551, 552, 553, 554, 555, 556, 557, 558,
    559, 560, 561, 562, 563, 564, 565, 566, 567, 568, 569, 570, 571, 572, 573, 4636, 4643, 4644, 4645, 4646, 
    4647, 4648, 4650, 4651, 4652, 4653, 4655, 4656, 4657, 4658, 4659, 4660, 4661, 4662, 4663, 4664, 4665, 4666,
    4667, 4668, 4669, 4670, 4671, 4672, 4673, 4674, 4753, 4756, 4757, 4758, 4760, 4761, 4762, 4765, 4768, 4770,
    4771, 4772, 4773, 4775, 4940, 4944, 35424, 35425, 35426, 35427, 35428, 35429, 35430, 35431, 35432, 35433, 
    35434, 35435, 35436, 35437, 35438, 35439, 35440, 35441, 35442, 35443, 35444, 35445, 35446, 35447, 35448, 
    35449, 35450, 35451, 35452, 35453, 35454, 35455, 35456, 35457, 35458, 35459, 35460, 35461, 35462, 35463, 
    35464, 35465, 35466, 35467, 35468, 35469, 35470, 44643, 44644, 44645, 44646, 44647, 44648, 44649, 44650, 
    44651, 44652, 44653, 44654, 44655, 44656, 44657, 44658, 44659, 44660, 44661, 44662, 44663, 44664, 44665,
    44666, 44667, 44668, 44669, 44670, 44671, 44672, 44673, 44674, 44675, 44676, 44677, 44678, 44679, 44680, 
    44681, 44682, 44683, 44684, 44685, 44686, 44687, 44688, 44689, 44690, 44691, 44692, 44693, 44694, 44695, 
    44696, 44697, 44698, 44699, 44700, 44701, 44702, 44703, 44704, 44705, 44706, 44707, 44708, 44709, 44710, 
    44711, 44712, 44713, 44714, 44715, 44716, 44717, 44718, 44719, 44720, 44721, 44722, 44723, 44724, 44725, 
    44726, 44727, 44728, 44729, 44730, 44731, 44732, 44733, 44734, 44735, 44736, 44737, 44738, 44739, 44740,
    44741, 44742, 44743, 44744, 44745, 44746, 44747, 44748, 44749, 44750, 44751, 44752, 44753, 44754, 44755,
    44756, 44757, 44758, 44759, 44760, 44761, 44762, 44763, 44764, 44765, 44766, 44767, 44768, 44769, 44770, 
    44771, 44772,  44773, 44774, 44775, 44776, 44777, 44778, 44779, 44780, 44781, 44782, 44783, 44784, 
    44785, 44786, 44787, 44788, 44789, 44790, 44791, 44792]

    #get incidence and prevalence data
    if (prev_filepath):
        print("Using file {} for prev" % prev_filepath)
        prev = pd.read_excel(prev_filepath)
        # get excel
    else:
        print("querying epi database for prev...")
        all_covariate_adjusted_data = get_cov_adjusted_data(model_version_id)
        prev = all_covariate_adjusted_data.loc[all_covariate_adjusted_data['measure_id']==5]
        
    if (inc_filepath):
        print("Using file {} for inc" % inc_filepath)
        inc = pd.read_excel(inc_filepath)
        # get excel
    else:
        print("querying epi database for inc...")
        all_covariate_adjusted_data = get_cov_adjusted_data(model_version_id)
        inc = all_covariate_adjusted_data.loc[all_covariate_adjusted_data['measure_id']==6].copy()
         
    prev = prev.loc[prev['location_id'].isin(location_id)]
    prev = prev.loc[prev['outlier_type_id'] == 0]
    prev['prev_se'] = (prev["upper"] - prev["lower"]) / (2*1.96)
    prev = prev.rename(columns={'mean':'prev_mean', 'lower':'prev_lower', 'upper':'prev_upper'})
    prev = adj_data_template(df=prev)
    
    inc = inc.loc[inc['location_id'].isin(location_id)]
    inc = all_fourplus_locs(inc)
    inc = inc.loc[inc['outlier_type_id'] == 0]
    inc['inc_se'] = (inc["upper"] - inc["lower"]) / (2*1.96)
    inc = inc.rename(columns={'mean':'inc_mean', 'lower':'inc_lower', 'upper':'inc_upper'})
    inc = adj_data_template(df=inc)
    
    #load custom (HIV-neg + HIV-pos) csmr
    print("loading custom csmr data...")
    csmr = pd.read_csv("FILEPATH")
    csmr['csmr_se'] = (csmr["upper"] - csmr["lower"]) / (2*1.96)
    csmr = csmr.rename(columns={'mean':'csmr_mean', 'lower':'csmr_lower', 'upper':'csmr_upper'})
    csmr = csmr[['age_group_id', 'location_id', 'year_id', 'sex_id', 'csmr_mean', 'csmr_se', 'csmr_lower', 'csmr_upper']].copy()
    
    #get acmr data
    print("querying get_envelope for acmr...")
    acmr = get_envelope(age_group_id=age_ids, location_id=location_id, year_id=year_ids, sex_id=sex_ids, gbd_round_id=4, status='best', rates=1)
    acmr['acmr_se'] = (acmr["upper"] - acmr["lower"]) / (2*1.96)
    acmr = acmr.rename(columns={'mean':'acmr_mean', 'lower':'acmr_lower', 'upper':'acmr_upper'})
    

    #get emr-predicted data
    print('pulling global emr-pred numbers...')
    emrpred = get_emr_pred(model_version_id)

    print('merging dataframes...')
    merge_inc = pd.merge(left=inc, right=csmr, on=['age_group_id', 'sex_id', 'year_id', 'location_id'], how='inner')
    merge_inc = pd.merge(left=merge_inc, right=acmr, on=['age_group_id', 'sex_id', 'year_id', 'location_id'], how='inner')
    merge_inc = pd.merge(left=merge_inc, right=emrpred, on=['age_group_id', 'sex_id', 'year_id'], how='inner')
    merge_inc['location_id']=merge_inc['location_id_x'] 
    #required since emr-pred is only global, location_id=1

    #remission should equal 2. upper and lower bounds 1.8-2.2
    merge_inc['rem_mean'] = 2
    merge_inc['rem_se'] = .1020408
        #merge data required for incidence-based emr calculation

    merge_prev = pd.merge(left=prev, right=csmr, on=['age_group_id', 'sex_id', 'year_id', 'location_id'], how='inner')
        #merge data required for prevalence-based emr calculation
      
    return (merge_prev, merge_inc)

##Final calculations
def compute_emr_from_inc(merge_inc):
    print('emr from incidence calculations...')
    merge_inc["mean"] = (merge_inc["csmr_mean"] *
                (merge_inc["rem_mean"] + ( merge_inc["acmr_mean"] - merge_inc["csmr_mean"] ) + merge_inc["pred_mean"]) /
                merge_inc["inc_mean"])

    merge_inc["se"] = (merge_inc["mean"] *
                np.sqrt(
                    (merge_inc["inc_se"] / merge_inc["inc_mean"])**2 +
                    (merge_inc["csmr_se"] / merge_inc["csmr_mean"])**2 +
                    (merge_inc["acmr_se"] / merge_inc["acmr_mean"])**2 +
                    (merge_inc["pred_se"] / merge_inc["pred_mean"])**2 +
                    (merge_inc["rem_se"] / merge_inc["rem_mean"])**2 
                    ))
    merge_inc = merge_inc[['age_group_id', 'location_id', 'year_id', 'sex_id', 'sex', 'nid', 'underlying_nid','outlier_type_id', 
    'year_start', 'mean', 'se']].copy()
    merge_inc['emr_calc'] = "incidence"
    merge_inc['measure_id'] = '9'
    merge_inc['response_rate'] = ''
    merge_inc.to_csv("FILEPATH", index=False)
    return merge_inc

def compute_emr_from_prev(merge_prev):
    print('emr from prevalence calculations...')
    merge_prev["mean"] = merge_prev["csmr_mean"] / merge_prev["prev_mean"]
    merge_prev["se"] = (merge_prev["mean"] *
                np.sqrt( (merge_prev["prev_se"] / merge_prev["prev_mean"])**2 + (merge_prev["csmr_se"] / merge_prev["csmr_mean"])**2) )
    merge_prev = merge_prev[['age_group_id', 'location_id', 'year_id', 'sex_id', 'sex', 'nid', 'underlying_nid','outlier_type_id', 
    'year_start', 'mean', 'se']].copy()
    merge_prev['emr_calc'] = "prevalence"
    merge_prev['measure_id_emr'] = '9'
    merge_prev['response_rate'] = ''
    merge_prev.to_csv("FILEPATH", index=False)
    return merge_prev
###########################################################

def get_emr_results(model_version_id=152216):

    write_to_excel = True

    (merge_prev, merge_inc) = combined_get_model_results(model_version_id=model_version_id)
    emr_prev = compute_emr_from_prev(merge_prev)
    emr_inc = compute_emr_from_inc(merge_inc)
    emr = pd.concat([emr_prev,emr_inc])
    
    print('concatenating and exporting excel file...')
    if write_to_excel:
        emr.to_excel("FILEPATH", index=False)

