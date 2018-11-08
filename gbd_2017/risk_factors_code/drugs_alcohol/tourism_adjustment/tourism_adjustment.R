############################
# Adjust LPC for tourism
############################

if (tourism_lpc){
  
  #The two files below were produced using the UNWTO data. This is the raw data, reformatted to be accessible for ST-GPR/AMELIA. 
  #See the python notebook if there's a need to re-extract. Tourism totals are imputted using ST-GPR, see supplemental documentation for more discussion.
  
  tourism_totals      <- fread(sprintf("FILEPATH/tourist_totals.csv", main_dir))
  tourism_proportions <- fread(sprintf("FILEPATH/tourist_proportions.csv", main_dir))
  lpc_unadjusted      <- fread(sprintf("FILEPATH", main_dir))
  
  #Which tourism steps to run?
  run_amelia <- F
  adjust_tourism <- F
  
  #Submits jobs to run Amelia in parallel. Code being run can be found here:
  if (run_amelia){
    
    source("FILEPATH/get_location_metadata.R")
    
    dir  <- sprintf("FILEPATH", main_dir)
    locs <- get_location_metadata(location_set_id = 1)[, .(location_id, location_name)]
    
    #Load tourist data
    df <- tourism_proportions
    df[, id:=paste(df$location_name, df$visiting_country, sep=",")]
    
    #Load covariate data
    covar <- fread(paste0(dir, "covariates.csv"))
    covar[, cigarettes_pc:=NULL]
    
    #Load alcohol predictions
    alc <- copy(lpc_unadjusted)
    alc <- join(alc, locs, by='location_id', type='left')
    alc <- melt(alc, id.vars=c("location_id", "location_name", "year_id"), value.name='alc_lpc', variable.name='draw')
    alc <- alc[, mean(alc_lpc, na.rm=T), by=c("location_name", "year_id")]
    setnames(alc, "V1", "alc_lpc")
    
    #Combine covariates with alcohol, merge onto tourist data.
    predictors <- join(covar, alc, by=c("location_name", "year_id"), type="left")
    predictors[, data:=NULL]
    predictors <- predictors[!duplicated(predictors),]
    
    final <- join(df, predictors, by=c("location_name", "year_id"), type="left")
    
    #Make location id
    locations <- data.table(location_id = seq(1, length(unique(final$location_name))), location_name = unique(final$location_name))
    final <- join(final, locations, by="location_name", type="left")
    
    #Overimpute for observed cases, parallelized on cluster
    write.csv(final, paste0(dir, "final.csv"))
    R_shell <- paste0(dir,"r_shell.sh")
    error_path <- paste0("FILEPATH", "FILEPATH")
    
    for (loc in locations$location_id){
      
      #Build qsub
      name <- paste0("impute_case_",i)
      script <- paste0(dir,"impute.R")
      arguments <- paste(dir, i)
      
      qsub <- paste0("qsub -N ", name, 
                     " -pe multi_slot ", 2,
                     " -l mem_free=", 4,
                     error_path
      )
      #Run qsub
      system(paste(qsub, R_shell, script, arguments))
      i <- i+1
      
    }
  if (adjust_tourism)
    rm(list=ls())
    arg <- commandArgs()[-(1:3)]
    
    library(plyr)
    library(data.table)
    library(dplyr)
    
    #Read in needed inputs
    loc <- as.numeric(arg[1])
    
    main_dir   <- "FILEPATH"
    input_dir  <- paste0(main_dir, "inputs/")
    output_dir <- paste0(main_dir, "outputs/")
    final_dir  <- "FILEPATH"
    
    template            <- fread(paste0(input_dir, "tourism_template.csv"))
    tourist_proportions <- fread(paste0(input_dir, "tourist_proportions.csv"))
    tourist_totals      <- fread(paste0(main_dir, "stgpr/tourism_gpr_2016.csv"))
    trip_duration       <- fread(paste0(input_dir, "length_of_stay.csv"))
    alc_lpc             <- fread(paste0(main_dir, "stgpr/alc_gpr_2017.csv"))
    
    loc_name <- template[location_id==loc, unique(location_name)]
    
    #Get alcohol lpc and tourist totals from gpr. Reshape long and hold onto needed columns, discarding rest. Add on ids.
    alc_lpc <-  join(alc_lpc, template, by=c("location_id", "year_id"), type="left") %>%
      melt(., id.vars=c("location_id", "location_name", "year_id", "pop_scaled"), 
           measure.vars=paste0("draw_", 0:999), value.name='alc_lpc', variable.name="draw") %>%
      .[!is.na(location_name)]
    
    tourist_totals <- melt(tourist_totals, id.vars = c("location_id", "year_id"), measure.vars = paste0("draw_", 0:999),
                           value.name='total_tourists', variable.name='draw')
    
    tourist_proportions <- setnames(tourist_proportions, "mean", "tourist_proportion") %>%
      .[(location_name == loc_name | location_id_visitor == loc),] %>%
      unique %>%
      .[!is.na(location_id_visitor)]
    
    trip_duration <- join(trip_duration, template, by=c("location_name", "year_id"), type = "left") %>%
      .[, .(location_name, location_id, year_id, length_of_stay)] %>%
      .[!is.na(location_id)]
    
    population <- template[location_id == loc, .(location_id, year_id, pop_scaled)]
    
    #Separate proportions into consumption that locals consume while abroad and consumption tourists consume while visiting locally
    abroad   <- tourist_proportions[location_id_visitor == loc, ] %>%
      join(., template, by=c("location_name", "year_id"), type = "left") %>%
      .[, .(location_name, location_id, year_id, visiting_country, location_id_visitor, tourist_proportion)] %>%
      .[, year_id := as.numeric(year_id)] %>%
      .[!is.na(location_id)]
    
    domestic <- tourist_proportions[location_name == loc_name, ] %>%
      join(., template, by=c("location_name", "year_id"), type = "left") %>%
      .[, .(location_name, location_id, year_id, visiting_country, location_id_visitor, tourist_proportion)] %>%
      .[, year_id := as.numeric(year_id)]
    
    #Calculate average length of stay and fill in for missing observations. Convert to fraction of year
    average_duration <- trip_duration[, mean(length_of_stay, na.rm=TRUE)]
    trip_duration    <- trip_duration[is.na(length_of_stay), length_of_stay := average_duration]
    
    #Construct additive and subtractive measure of tourists for given location:
    tourist_consumption <- function(h, y, v, tourist_prop, tourist_total, lpc, duration){
      
      #For a given host country, visiting country, and year, calculate 1000 draws of tourist consumption
      
      tourist_prop   <- tourist_prop[J(h, v, y), tourist_proportion]
      tourist_total  <- tourist_total[J(h, y), total_tourists]
      lpc            <- lpc[J(v,y), alc_lpc]
      duration       <- trip_duration[J(v, y), length_of_stay]
      
      consumption <- lpc * tourist_prop * tourist_total * (duration/365)
      tourist_consumption <- data.table(host = h, visitor = v, year_id = y, 
                                        draw = paste0("draw_", 0:999), tourist_consumption = consumption)
      
      return(tourist_consumption)
    }
    
    setkey(abroad, location_id, location_id_visitor, year_id)
    setkey(domestic, location_id, location_id_visitor, year_id)
    setkey(tourist_totals, location_id, year_id)
    setkey(alc_lpc, location_id, year_id)
    setkey(trip_duration, location_id, year_id)
    
    args <- unique(abroad[, .(location_id, year_id)])
    tourist_consumption_abroad <- mdply(cbind(h = as.numeric(args$location_id), y = as.numeric(args$year_id)), 
                                        tourist_consumption, v = loc, tourist_prop = abroad, tourist_total = tourist_totals, 
                                        duration = trip_duration, lpc = alc_lpc) %>% 
      as.data.table %>%
      .[, sum(.SD$tourist_consumption, na.rm=T), by=c('year_id', 'draw')] %>%
      setnames(., "V1", "tourist_consumption") %>%
      join(., population, by="year_id", type="left") %>%
      .[, tourist_consumption := tourist_consumption/pop_scaled]
    
    args <- unique(domestic[, .(location_id_visitor, year_id)])
    tourist_consumption_domestic <-  mdply(cbind(v = as.numeric(args$location_id), y = as.numeric(args$year_id)), 
                                           tourist_consumption, h = loc, tourist_prop = domestic, tourist_total = tourist_totals, 
                                           duration = trip_duration, lpc = alc_lpc) %>% 
      as.data.table %>%
      .[, sum(.SD$tourist_consumption, na.rm=T), by=c('year_id', 'draw')] %>%
      setnames(., "V1", "tourist_consumption") %>%
      join(., population, by="year_id", type="left") %>%
      .[, tourist_consumption := tourist_consumption/pop_scaled]
    
    net_tourism <- tourist_consumption_abroad$tourist_consumption - tourist_consumption_domestic$tourist_consumption
    
    net <- expand.grid(location_id = loc, location_name = loc_name, year_id = unique(tourist_consumption_domestic$year_id),
                       draw = unique(tourist_consumption_domestic$draw)) %>% cbind(., net_tourism) %>% data.table
    
    write.csv(net, paste0(final_dir, "net_adjustment_", loc, ".csv"), row.names=F)
    
    from multiprocessing import Pool
    
    tourism_adjustments = pd.read_csv('{}/tourism_adjustments.csv'.format(outgoing))
    backcast = tourism_adjustments.groupby(['location_id', 'draw', 'location_name'])
    
    def caster(params):
      
      keys = params[0]
    df = params[1]
    
    addon = pd.DataFrame({'year_id':years})
    addon['location_id'] = keys[0]
    addon['draw'] = keys[1]
    addon['location_name'] = keys[2]
    addon['net_tourism'] = df[df.year_id.isin([1995,1996,1997])]['net_tourism'].mean()
    addon.loc[(addon.year_id >= 2015), 'net_tourism'] = df[df.year_id.isin([2012,2013,2014])]['net_tourism'].mean()
    
    return(pd.concat([df, addon], ignore_index=True))
    
    #Bind parameters together (pool requires passing single arguments) based on keys of groupby and constituent dataframes
    params = []
    for keys, df in backcast:
      
      params.append((keys, df))
    
    p = Pool(40)
    final_adjustment = p.map(caster, params)
    final_adjustment = pd.concat(final_adjustment, ignore_index=True)
    final_adjustment = final_adjustment.loc[final_adjustment.net_tourism == final_adjustment.net_tourism]
    
    final_adjustment.to_csv('{}/final_tourist_adjustment.csv'.format(outgoing), index=False)
    
    alc_lpc = pd.read_csv("FILEPATH/alc_gpr_2017.csv")
    alc_lpc = alc_lpc[['year_id','location_id']+draws]
    alc_lpc = alc_lpc[alc_lpc['year_id'] >= 1990]
    alc_lpc = pd.melt(alc_lpc, id_vars=['year_id', 'location_id'], var_name="draw", value_name="alc_lpc")
    
    #Merge with adjustments, add in net. Only use adjustment if above 0 after applying adjustment (could not be the case due to assumptions above & poor data in alc lpc FAO)
    adj_alc = pd.merge(alc_lpc, final_adjustment, how="left", on=["location_id", "year_id", "draw"])
    adj_alc['net_tourism'].fillna(0, inplace=True)
    adj_alc['alc_lpc_new'] = adj_alc["alc_lpc"] + adj_alc["net_tourism"]
    adj_alc.loc[(adj_alc.alc_lpc_new <= 0), 'alc_lpc_new'] = adj_alc.loc[(adj_alc.alc_lpc_new <= 0), 'alc_lpc']
    
    adj_alc['alc_lpc'] = adj_alc['alc_lpc_new']
    
    #Clean up dataframe and send off
    adj_alc = adj_alc[['year_id', 'location_id', 'draw', 'alc_lpc', 'net_tourism']]
    adj_alc.to_csv("{}/adj_alc_lpc_2017.csv".format(outgoing), index=False)
    
  }
}