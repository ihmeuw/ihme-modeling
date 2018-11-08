#############################################
# Adjust for unrecorded and scale proportions
#############################################

#Load datasets and reduce to required columns. Reshape long.
  
  lpc              = pd.read_csv(input_dir+"/adj_alc_lpc_2017.csv")
  if include_unrecorded==True:
    unrecorded       = pd.read_csv(input_dir+"/unrecorded_samples_2016.csv")
  current_drinkers = interpolate("modelable_entity_id", 3364, "epi", age_group_ids=ages, location_ids=location, sex_ids=sexes, _n=1000, resample=False)
  abstainers       = interpolate("modelable_entity_id", 3365, "epi", age_group_ids=ages, location_ids=location, sex_ids=sexes, _n=1000, resample=False)
  gday             = interpolate("modelable_entity_id", 3360, "epi", age_group_ids=ages, location_ids=location, sex_ids=sexes, measure_ids=[19], _n=1000, resample=False, version = 200915)
  
  populations      = get_population(age_group_id = ages, location_id = location, year_id = years, sex_id = sexes)
  populations      = populations[['location_id', 'year_id', 'sex_id', 'age_group_id', 'population']]
  
  #Add dataframes to a list for easy operations later. 
  alc = {'current_drinkers':current_drinkers, 
    'abstainers':abstainers, 
    'gday':gday}
  
  def clean(df):
    return(df.drop(["measure_id", "modelable_entity_id", "n_draws"], axis=1, inplace=True))
  
  def melter(df, key):
    return(pd.melt(df, id_vars = [x for x in df.columns if "draw" not in x], var_name="draw", value_name=key)) 
  
  #For each dataset, remove some columns, melt to long, and merge together.
  
  for key, df in alc.iteritems():
    alc[key] = clean(df)
  alc[key] = melter(df, key)
  
  #Merge dataframes onto each other, using the first as the base.
  if key != alc.keys()[0]:
    alc[alc.keys()[0]] = pd.merge(alc[alc.keys()[0]], alc[key], on=["location_id", "year_id", 
                                                                    "age_group_id", "sex_id", "draw"], how="left")      
  del current_drinkers
  del abstainers
  del gday
  
  #Make final dataset from all of the merged datasets, merge on lpc and populations
  alc = alc[alc.keys()[0]]
  
  alc = pd.merge(alc, lpc, on=['year_id', 'location_id', 'draw'], how="left")
  del lpc
  if include_unrecorded==True:
    alc = pd.merge(alc, unrecorded, on=['location_id', 'draw'], how='left')
  
  #If no data on unrecorded, assume unrecorded is non-existent (i.e. adjustment factor = 1)
  alc.loc[pd.isnull(alc.unrecorded_rate), 'unrecorded_rate'] = 1
  
  del unrecorded
  
  alc = pd.merge(alc, populations, on=['year_id', 'location_id', 'age_group_id', 'sex_id'], how='left')
  del populations
  
  #Scale current_drinkers, abstainers, and former drinkers to 1. Place cap on former drinkers and reapportion to abstainers
  
  if include_former == True:
    alc["total"]            = alc.current_drinkers+alc.abstainers+alc.former
  alc["current_drinkers"] = alc.current_drinkers/alc.total
  alc["abstainers"]       = alc.abstainers/alc.total
  alc["former"]           = alc.former/alc.total
  
  alc.loc[(alc.former >= former_cap), 'abstainers'] = (alc.loc[(alc.former >= former_cap), 'abstainers'].values 
                                                       + alc.loc[(alc.former >= former_cap), 'former'].values 
                                                       - former_cap)
  
  alc.loc[(alc.former >= former_cap), 'former']     = former_cap
  
  else:
    alc["total"]            = alc.current_drinkers+alc.abstainers
  alc["current_drinkers"] = alc.current_drinkers/alc.total
  alc["abstainers"]       = alc.abstainers/alc.total
  
  #Determine percentage of total liters per capita to distribute to each group, based on gday relationships and population.
  
  #Calculate total stock in a given population, based on survey g/day
  alc["drinker_population"] = alc.current_drinkers * alc.population
  alc["population_consumption"] = alc.drinker_population * alc.gday
  
  #Calculate total stock overall from summing across age_groups for each sex
  alc["total_gday"] = alc.groupby(["location_id", "year_id", "draw"])["population_consumption"].transform(sum)
  
  #Calculate percentage of total stock each invidual-level population consumes
  alc["percentage_total_consumption"] = alc.population_consumption/alc.total_gday
  
  #Now, using LPC data, calculate total stock and use above calculated percentages to split this 
  #stock into lpc drinkers for each population
  
  alc["population_consumption"] = (alc.population * alc.alc_lpc)
  if include_unrecorded == True:
    alc["population_consumption"] = alc.population_consumption * alc.unrecorded_rate
  alc["total_consumption"] = alc.groupby(["location_id", "year_id", "draw"])["population_consumption"].transform(sum)
  alc["drinker_lpc"] = (alc.percentage_total_consumption * alc.total_consumption)/alc.drinker_population
  
  #Convert lpc/year to g/day
  alc["drink_gday"] = alc.drinker_lpc * 1000/365
  
  #Hold onto relevant columns and export
  alc = alc[['location_id', 'sex_id', 'age_group_id', 'year_id', 'draw', 'current_drinkers', 
             'abstainers','alc_lpc', 'drink_gday', 'population']]

  ## Scale subnationals

  #Get list of locations with parent-child relationship.
  locations <- get_location_metadata(location_set_id=35, gbd_round_id=5)
  locations <- locations[, .(location_id, parent_id, level, most_detailed, location_name)]
  
  parents   <- locations[level>=3 & most_detailed==0]
  children  <- locations[level>3 & most_detailed==1]
  
  scale_subnat <- function(loc){
    
    print(loc)
    
    #Read in lower_level files & scale to parent
    level <- rbind(parents, children)
    level <- level[(location_id == loc | parent_id == loc)]
    
    #Build list of filenames
    files <- paste0(io, "alc_exp_", level[parent_id == loc, location_id], ".csv")   
    
    #Using % current drinker and drink_gday, calculate total stock in both parent and children for specific
    #year, sex, age. Make sure children sum up to parent. If not, adjust children estimates by scaling factor.
    
    parent  <- fread(paste0(io, "alc_exp_", loc, ".csv"))
    parent[, total_stock := population*drink_gday]
    parent <- parent[, .(sex_id, age_group_id, year_id, draw, total_stock)]
    
    subnats <- rbindlist(lapply(files, fread))
    subnats[, subnat_stock := population * drink_gday]
    subnats[, subnat_stock := sum(subnat_stock), by=c("year_id", "sex_id", "age_group_id", "draw")]
    
    subnats <- join(subnats, parent, by=c("year_id", "sex_id", "age_group_id", "draw"), type="left")
    subnats[, drink_gday := drink_gday*(total_stock/subnat_stock)]
    
    #Overwrite old unscaled children
    for (location in unique(subnats$location_id)){
      
      hold <- subnats[location_id==location, c('location_id', 'sex_id', 'age_group_id', 'year_id', 'draw', 
                                               'current_drinkers', 'abstainers', 'alc_lpc', 'drink_gday', 
                                               'population'), with=F]
      write.csv(hold, paste0(io, "alc_exp_", location, ".csv"), row.names=FALSE)
      print(head(hold))
    }
  }
  
  #Start at highest level of parents and work your way down, scaling children.
  #Perform function only if parent actually exists
  
  levels <- sort(unique(parents$level), decreasing = T)
  
  for (l in sort(levels)){
    
    submit <- parents[(level==l & location_id!=6),]
    l_ply(submit$location_id, scale_subnat)
    
  }
  