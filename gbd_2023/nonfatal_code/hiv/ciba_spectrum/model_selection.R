
################################################################################
## Purpose: Assess the difference between Spectrum and VR deaths for different spectrum runs
## Date created: 
## Date modified:
## Author: 
## Run instructions: Run along with Spectrum to help select ART/incidence combinations
## Notes: The sex splits are taken from the last PJNZ file that was available from India, i.e. 2013.  However we were able to extract new ART data up to the most
#recent year from reports. As it is not sex-specific, we use the 2013 proportions to split them. Further, there is an older location mapping in 2013 that gets imported
#to be able to map the 2013 data.
################################################################################

#' @import data.table, assertable, library(mortdb, lib = "FILEPATH"), Metrics, MASS
#' @param run.name string, new run name (example: 191224_trumpet)
#' @param stage string, Spectrum stage to pull ART and deaths from (example: "stage_1")
#' @param loc.id, integer, GBD location id (example: 4841)

#' @return list of model selection metrics and model summarys

get_cod = function(loc.id){
  
  age.table <- data.table(get_age_map(type="all"))
  cod.data <- fread( "FILEPATH",'gpr_results.csv')[location_id==loc.id,.(location_id,year_id,age_group_id,sex_id,gpr_mean,gpr_lower,gpr_upper)]
  
  ext.dt <- list()
  for(y in 1970:1980){
    ext.dt = copy(cod.data)[year_id==1981]
    ext.dt[,c('year_id','gpr_mean','gpr_lower','gpr_upper') := .(y,0,0,0)] 
    cod.data <- rbind(cod.data,ext.dt)
  }
  
  pop.data <- get_population(age_group_id = c(2,3, 388, 389, 34, 6:20, 30:32,235, 238), 
                             location_id = loc.id, year_id = -1, 
                             sex_id = c(1,2), location_set_id = 79,
                             gbd_round_id = 7,
                             decomp_step = "iterative")
  pop.data[, process_version_map_id:=NULL]
  pop.data <- merge(pop.data, loc.table[,.(location_id, ihme_loc_id)], by="location_id", all.x=T)
  pop.data <- pop.data[,.(pop=sum(population)), by=.(ihme_loc_id, year_id, sex_id, age_group_id, location_id)]
  
  cod.data <- cod.data[order(location_id,year_id,age_group_id)]
  cod.pop.merged <- merge(cod.data,pop.data[location_id == loc.id], by=c('location_id', 'year_id','age_group_id','sex_id'))
  # Get counts of GPR deaths
  cod.pop.merged[,deaths := pop*gpr_mean/100]
  cod.pop.merged[age_group_id  %in% c(2, 3, 388, 389, 34, 238), age_group_id := 1]  ### group all under 5 together
  cod.pop.merged[age_group_id > 20, age_group_id := 21]  ### group all 80+ together
  cod.pop.merged <- merge(cod.pop.merged, age.table[,.(age_group_id, age_group_years_start)], by="age_group_id", all.x=T)
  setnames(cod.pop.merged, "age_group_years_start", "age")
  cod.pop.merged[, c("ihme_loc_id", "location_id"):=.(loc, loc.id)]
  
  #Aggregate to 15 to 80
  cod_data = cod.pop.merged[age %in% 15:80]
  cod_data = cod_data[,list(deaths = sum(deaths)), by = .(year = year_id, sex = sex_id)]
  
  return(cod_data)
}


mean_average_error = function(v1,v2){
  error = v1 - v2
  average = sum(error)/length(error)
}


trend_fit = function(pre_dat,post_dat) {
  
  # To do this, for the test data we compute where 
  #possible the log death rate in year t minus the log death rate in 
  #year t-1. We also compute the same metric for the prediction. We then count the
  # percentage of cases for which the model predicts a trend in the same direction as the test data.
  
  dts = rbind(pre_dat,post_dat)
  dts = list(dts,pre_dat,post_dat)
  #dat = dat[[2]]
  perc = lapply(dts, function(dat){
    
              dat[,first_d_spec := spec_deaths - shift(spec_deaths,1), by = "sex"]
              dat[,first_d_vr := deaths - shift(deaths,1), by = "sex"]
              
              ##When first different is in same direction, count as 1
              dat[first_d_spec < 0 & first_d_vr < 0, same_trend := 1]
              dat[first_d_spec > 0 & first_d_vr > 0, same_trend := 1]
              dat[is.na(same_trend), same_trend := 0]
              dat[year == min(dat$year), same_trend := 1]
              
              ##Calculate percent of 1s
              perc = dat[,list(perc = sum(same_trend)/(max(dat$year) - min(dat$year))), by = .(sex)]
              perc = mean(perc$perc)
              
              return(perc)
              
          })
  
  names(perc) <- c("trend_fit","trend_fit_pre","trend_fit_post")
  
  return(perc)
  
}

assess_fit = function(run.name,stage,loc,out_folder){

    loc.id = loc.table[ihme_loc_id==loc,location_id]
    cod_data = get_cod(loc.id)
    
   
    
    if(grepl("grid",run.name)){
      
      spec_data = fread(paste0( "FILEPATH",loc,"_ART_data.csv"))
      
    } else {
      
      spec_data = fread(paste0( "FILEPATH",loc,"_ART_data.csv"))
    }
    
    ##Extract adult Spectrum deaths
    deaths_data = copy(spec_data)[age >= 15]
    deaths_data = deaths_data[,list(spec_deaths = sum(hiv_deaths)), by = c("year","sex","run_num")]
    deaths_data = deaths_data[,list(spec_deaths = mean(spec_deaths)), by = c("year","sex")]
    deaths_data[, sex := ifelse(sex=="female",2,1)]
  
     ##Extract adult ART data
    art_data = copy(spec_data)[age >= 15]
    art_data = art_data[,list(pop_art = sum(pop_art)), by = c("year","sex","run_num")]
    art_data = art_data[,list(pop_art = mean(pop_art)), by = c("year","sex")]
    art_data[, sex := ifelse(sex=="female",2,1)]
    
    cod_data_merge = merge(cod_data, deaths_data, by = c("sex","year"))
    cod_data_merge = merge(cod_data_merge,art_data, by = c('sex','year'))
    cod_data_merge = cod_data_merge[year >= 1981]
    
    
    ##Define pre-ART period as first non 0 ART observation
    cod_data_merge[,pre_art := ifelse(pop_art > 0,0,1)]

    pre_dat = cod_data_merge[pre_art == 1]
    post_dat = cod_data_merge[pre_art == 0]
    
    ##Assessing level fit
    rmse_pre = rmse(pre_dat$spec_deaths, pre_dat$deaths)
    rmse_post = rmse(post_dat$spec_deaths, post_dat$deaths)
    
    ##Assessing trend fit
    trend_f = trend_fit(pre_dat = pre_dat, 
                        post_dat = post_dat)
    trend_fit_pre = trend_f$trend_fit_pre
    trend_fit_post = trend_f$trend_fit_post
    trend_f = trend_f$trend_fit
    
    
    return(list(rmse_spec_pre = rmse_pre,
                rmse_spec_post = rmse_post,
                trend_fit_pre = trend_fit_pre,
                trend_fit_post = trend_fit_post,
                trend_fit = trend_f,
                pre_art_yrs = length(unique(pre_dat$year)),
                dat = cod_data_merge))
  
}




##END