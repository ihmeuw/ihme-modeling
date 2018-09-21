
## Purpose: Create new Lifetables based off of post-ensemble envelope results


###############################################################################################################
## Set up settings
rm(list=ls())

if (Sys.info()[1]=="Windows") {
  root <- "filepath" 
  user <- Sys.getenv("USERNAME")


} else {
  root <- "filepath"
  user <- Sys.getenv("USER")
  
  country <- commandArgs()[3]
  loc_id <- as.integer(commandArgs()[4])
  group <- paste0(commandArgs()[5]) # Enforce it as string
  spec_name <- commandArgs()[6]
  new_upload_version <- commandArgs()[7]
  
}

#############################################################################################
## Prep Libraries and other miscellany
## libraries
library(haven)
library(dplyr)
library(data.table)
library(assertable)

## Get age map that can apply to Env and LT results
age_map <- data.table(get_age_map(type="lifetable"))
## Under-1, 5-year age groups through 110, then 110+
setnames(age_map,"age_group_name_short","age")
age_map <- age_map[,list(age_group_id,age)]
age_map[,age:=as.numeric(age)]

## Grab lt_get_pops function to get appropriate populations to pop-weight by for ages 95+ granular both sexes (use the highest age that has male/female pops for each country/year combination)

## Set years to import from envelope files
  years = c(1970:2016)

## Get populations to use for mx and ax sex-weighting to create a both sexes category
weight_pop <- data.table(get_population(status = "recent", location_id = loc_id, year_id = years, sex_id = c(1:3), age_group_id = c(unique(age_map$age_group_id), 235)))
setnames(weight_pop, "population", "pop")
hiv_pop <- weight_pop[,list(sex_id,year_id,age_group_id,pop)]
weight_pop <- merge(weight_pop,age_map,by="age_group_id", all.x = T)

if(nrow(weight_pop[is.na(pop)]) > 0 )stop("Pops missing")
weight_pop <- weight_pop[,list(pop,location_id,year_id,age,sex_id, age_group_id)]

## Get map to re-sort draws of non-HIV deaths
## Use draw maps to scramble draws so that they are not correlated over time
## This is because Spectrum output is semi-ranked due to Ranked Draws into EPP
## Then, it propogates into here which would screw up downstream processes
## This is done here as opposed to raw Spectrum output because we want to preserve the draw-to-draw matching of Spectrum and lifetables, then scramble them after they've been merged together
draw_map <- fread(paste0("filepath"))
draw_map <- draw_map[location_id==loc_id,list(old_draw,new_draw)]
setnames(draw_map,"old_draw","draw")


#############################################################################################
## Bring in appropriate LTs and Envelopes

## Bring in envelopes post-ensemble
raw_env_del <- rbindlist(lapply(years, load_hdf, filepath = paste0(filepath)))
assert_values(raw_env_del, colnames(raw_env_del), "not_na")
assert_values(raw_env_del, c("mx_hiv_free"),"gte", 0 )
id_vars <- list(year_id = years, sex_id = c(1, 2), age_group_id = c(2:20, 30:32, 235), sim = c(0:999))
assert_ids(raw_env_del, id_vars)
raw_env_whiv <- rbindlist(lapply(years, load_hdf, filepath = paste0(filepath)))
assert_values(raw_env_whiv, colnames(raw_env_whiv), "not_na")
assert_values(raw_env_whiv, c("mx_hiv_free"),"gte", 0 )
assert_ids(raw_env_whiv, id_vars)

## Bring in scalars from HIV-deleted to with-HIV
env_scalar <- data.table(read_dta(paste0(filepath)))
env_scalar <- env_scalar[age_group_id == 235,]
assert_values(env_scalar, colnames(env_scalar), "not_na")
assert_values(env_scalar, c("scalar_del_to_all"), "gt", 0)
id_vars <- list(year_id = years, sex_id = c(1,2), age_group_id = c(235), sim = c(0:999))
assert_ids(env_scalar, id_vars)

## Bring in lifetables pre-ensemble
lt_del <- data.table(fread(paste0(filepath)))
lt_whiv <- data.table(fread(paste0(filepath)))

lt_del[,sex:=as.integer(sex)]
lt_whiv[,sex:=as.integer(sex)]
lt_del <- lt_del[year >= 1970,]
lt_whiv <- lt_whiv[year >= 1970,]

## Bring in HIV results and populations to convert HIV deaths to rates
hiv_results <- data.table(fread(paste0(filepath))) 
assert_values(hiv_results, colnames(hiv_results), "not_na")

## Bring in raw Spectrum HIV results to get non-HIV deaths from Spectrum to apply the ratio of all-cause to HIV-free
## Only used to calculate Group 1 lifetables
spec_hiv_results <- data.table(fread(paste0(filepath)))

spec_hiv_results <- spec_hiv_results[,list(sex_id,year_id,age_group_id,run_num,non_hiv_deaths)]
setnames(spec_hiv_results,"run_num","draw")
spec_hiv_results[,draw:=draw-1]
spec_hiv_results <- merge(spec_hiv_results,draw_map,by=c("draw"))
spec_hiv_results[,draw:=new_draw]
spec_hiv_results[,new_draw:=NULL]

assert_values(spec_hiv_results, colnames(spec_hiv_results), "not_na")


#############################################################################################
## Define functions for formatting data appropriately for append/merging

## Format lifetables to be GBD-ized
format_lts <- function(data) {
  data <- data[,list(sex,year,age,draw,ax,mx,qx)]
  setnames(data,c("sex","year"),c("sex_id","year_id"))
  data[sex_id==1,sex:="male"]
  data[sex_id==2,sex:="female"]
  data[sex_id==3,sex:="both"]
  data <- merge(data,age_map,by="age")

  assert_values(data, c("mx"), "gte", 0)
  assert_values(data, colnames(data), "not_na") 
  id_vars <-  list(sex_id = unique(data$sex_id), year_id = years, age_group_id = c(5:20, 28, 30:33, 44, 45, 148), draw = c(0:999))
  assert_ids(data, id_vars)

  return(data)
}

## Extract ax values to merge onto the final dataset
get_lt_ax <- function(data) {
  data <- data[,list(sex_id,year_id,age_group_id,draw,ax)]
  return(data)
}

## Extract under-5 values from pre-reckoning lifetables
get_u5_lt <- function(data) {
  data <- data[(age_group_id == 5 | age_group_id == 28) & year_id >=1970,list(sex_id,year_id,age_group_id,draw,mx)]
  return(data)
}

## Extract HIV estimates for under-5 to add or subtract from pre-reckoning lifetables
get_u5_hiv <- function(data) {
  data <- data[age_group_id <= 5,]
  setnames(data,"sim","draw")
  
  ## Collapse NN granular to under-1 
  data[age_group_id <=4, age_group_id := 28]
  data <- data.table(data)[,lapply(.SD,sum),.SDcols="hiv_deaths",
                           by=c("age_group_id","sex_id","year_id","draw")] 
  
  ## Convert from deaths to rate
  data <- merge(data,hiv_pop,by=c("sex_id","year_id","age_group_id"))
  data[,mx_hiv:=hiv_deaths/pop]
  data <- data[,list(age_group_id,sex_id,year_id,draw,mx_hiv)]
  return(data)
}

    
## Pull out under-5 values from LTs, apply HIV, and output mx values for both, using get_u5_lt and get_u5_hiv functions
## This is because we want to preserve U5 results from MLT as opposed to using U5 mx values derived from age-sex results, which are inconsistent
create_u5_mx <- function(data) {
  ## Input is lifetable from get_u5_lt, either with-HIV or HIV-free depending on the group
  ## For all groups, we want to preserve the mx values from the with-HIV lifetable, then subtract out HIV
  ## Otherwise, we want to preserve the mx from the HIV-free lifetable and add HIV to it to create with-HIV
  
  hiv_u5 <- get_u5_hiv(hiv_results)
  data <- merge(data,hiv_u5,by=c("age_group_id","sex_id","year_id","draw"))
  
  ## Here, data is whiv and we subtract HIV to get hiv_deleted
  data[mx_hiv > (.9*mx), mx_hiv := .9*mx] # Impose a cap on HIV so that it doesn't exceed 90% of the lifetable mx (similar to what is done in the ensemble model for envelope)
  data[,mx := mx - mx_hiv]
  data[,mx_hiv := NULL]
  data <- data[year_id >= 1970,]

  return(data)
}

## GROUP 1 COUNTRIES: Pull out under-5 values from LTs
## Use this instead of get_u5_lt and create_u5_mx only in the case of Group 1 countries
## Extract under-5 values from pre-reckoning lifetables

get_u5_g1_hiv <- function(data) {
  data <- data[age_group_id <= 5,]
  setnames(data,"sim","draw")
  
  ## Collapse NN granular to under-1 
  data[age_group_id <=4, age_group_id := 28]
  data <- data.table(data)[,lapply(.SD,sum),.SDcols="hiv_deaths",
                           by=c("age_group_id","sex_id","year_id","draw")] 
  
  ## Convert from deaths to rate
  data <- merge(data,hiv_pop,by=c("sex_id","year_id","age_group_id"))
  data[,mx_hiv:=hiv_deaths/pop]
  data <- data[,list(age_group_id,sex_id,year_id,draw,mx_hiv)]
  return(data)
}

## Extract non-HIV death estimates for under-5 to add or subtract from pre-reckoning lifetables
get_u5_g1_hiv_free <- function(data) {
  data <- data[age_group_id <= 5,]
  
  ## Collapse NN granular to under-1 
  data[age_group_id <=4, age_group_id := 28]
  data <- merge(data,hiv_pop,by=c("sex_id","year_id","age_group_id"))
  data[,non_hiv_deaths:=non_hiv_deaths*pop]
  
  data <- data.table(data)[,lapply(.SD,sum),.SDcols=c("non_hiv_deaths","pop"),
                           by=c("age_group_id","sex_id","year_id","draw")] 
  
  ## Convert from deaths to rate
  data[,mx_spectrum_free:=non_hiv_deaths/pop]
  data <- data[,list(age_group_id,sex_id,year_id,draw,mx_spectrum_free)]
  return(data)
}
      
create_u5_mx_group1 <- function(data) {
  ## We want to use the ratio of Spectrum HIV-deleted to with-HIV to convert lifetable with-HIV to lifetable HIV-free
  ## data is with-HIV dataset from get_u5_lt, already under-1 non-granular
  ## We want to take in the spectrum NN results, collapse to under-1, merge on with the u5 HIV, calculate the 
  hiv_u5 <- get_u5_g1_hiv(hiv_results)
  spec_free <- get_u5_g1_hiv_free(spec_hiv_results)
  
  lt_u5 <- merge(data,hiv_u5,by=c("age_group_id","sex_id","year_id","draw"))
  
  ## First, convert the non-HIV Spectrum deaths and Envelope HIV-free from the rate space to numbers
  lt_u5 <- merge(lt_u5,spec_free,by=c("age_group_id","sex_id","year_id","draw"))

  ## Add Spectrum HIV-free and HIV together to get Spectrum all-cause
  lt_u5[,spec_whiv := mx_hiv+mx_spectrum_free]
  
  ## Create ratio to Spectrum HIV-free from Spectrum with-HIV
  lt_u5[,ratio_free_whiv := mx_spectrum_free/spec_whiv]
  
  ## Apply this ratio to lifetable with-HIV to generate lifetable HIV-free
  lt_u5[,mx := mx * ratio_free_whiv]

  ## Bring it back onto the primary dataset
  lt_u5 <- lt_u5[,list(age_group_id,sex_id,year_id,draw,mx)]
  lt_u5 <- lt_u5[year_id >= 1970,]
  return(lt_u5)
} 

## Extract lifetable over-95 granular results to be scaled using envelope scalars
get_lt_mx <- function(data) {
  data <- data[as.numeric(age) >= 95,] # Only need 95 and over granular age groups
  data <- data[year_id >= 1970,list(age_group_id,sex_id,year_id,draw,mx)]
  return(data)
}

## Create scaled LT over-95 results
## Depending on the group, we want to either go from all-cause to HIV-free or HIV-free to all-cause (Group 1A)
scale_lt_over95 <- function(lt_data,scalar_data) {
  scalar_data <- scalar_data[,list(sex_id,year_id,sim,scalar_del_to_all)] # Drop age_group_id because we apply 95+ scalar equally to all over-95 granular groups
  setnames(scalar_data,c("sim","scalar_del_to_all"),c("draw","scalar"))
  lt_data <- merge(lt_data,scalar_data,by=c("sex_id","year_id","draw"))
  
  ## If group is anything except 1A, convert to HIV-free by all-cause/scalar
  ## Otherwise, convert to all-cause by HIV-free * scalar
  if(group != "1A") {
    lt_data[,mx:=mx/scalar]
  } else {
    lt_data[,mx:=mx*scalar]
  }
  lt_data <- lt_data[,list(sex_id,year_id,draw,age_group_id,mx)]
  lt_data <- lt_data[year_id >=1970,]
  return(lt_data)
}

## Format the envelope to rbind appropriately
  format_env <- function(data, type) {
    # data <- melt(data,id.vars=c("location_id","sex_id","year_id","age_group_id","pop"),variable.name="draw",variable.factor=F)
    # data[,draw:=as.numeric(gsub("env_","",draw))]
    # data_under1 <- get_env_under1(data)
    data <- data[age_group_id > 5 & age_group_id != 21,]
    setnames(data, "sim", "draw")
    # data <- rbind(data,data_under1)
    if(type == "hiv_free"){
       data[,mx := mx_hiv_free/pop]
    }
    else{
      data[,mx := mx_avg_whiv/pop]
    }
   
    data <- data[,list(sex_id,year_id,age_group_id,mx,draw)]
    return(data)
  }

## Add both sexes to the dataset
prep_both_sexes <- function(data,type) {
  data <- merge(data,age_map,by="age_group_id")
  data <- merge(data,weight_pop,by=c("age_group_id", "age","sex_id","year_id"),all.x=T)
  data[,location_id:=loc_id]
  setnames(data,"year_id","year")
  data_pops <- data.table(lt_get_pops(as.data.frame(data[,list(age,sex_id,location_id,year,draw,pop)]),agg_var="sex_id",draws=T,idvars=c("location_id","year")))
  data_pops <- data_pops[year_id > 1969,]
  setnames(data,"year","year_id")
  data[,pop:=NULL]
  data <- merge(data,data_pops,by=c("age","sex_id","location_id","year_id","draw"),all.x=T)
  if (nrow(data[is.na(pop),]) > 0) stop("missing pops")
  data[,age:=NULL]

  ## Here, pop-weight mx and death-weight ax to collapse to both sexes, then unweight and add back onto the original dataset
  both <- copy(data)
  both[,mx:=mx*pop]
  both[,ax:=ax*mx]
  setkey(both,age_group_id,location_id,year_id,draw)
  both <- both[,list(mx=sum(mx),ax=sum(ax),pop=sum(pop)),by=key(both)]
  both[,ax:=ax/mx]
  both[,mx:=mx/pop]
  both[,sex_id:=3]

  data <- rbindlist(list(data,both),use.names=T)
  data[,c("pop","location_id"):=NULL]

## We want to preserve the U5 results for with-HIV for both sexes from the original prematch file
if(type == "whiv") {
  # p <- data[1,process_version_map_id]
  # whiv_both_u5[,process_version_map_id:= p]
  data <- data[(age_group_id != 5 & age_group_id != 28) | sex_id != 3,]
  data <- rbindlist(list(data,whiv_both_u5),use.names=T)
}
 
return(data)
}

## Save MX and AX output as separate HDFs -- one per country, grouped by year
save_mx_ax <- function(data,type) {
  data <- data[,list(age_group_id,sex_id,year_id,draw,mx,ax)]
  data[,location_id:=loc_id]

  if(type == "whiv") {
  filepath = paste0(filepath)
}
  else{ 
  filepath = paste0(filepath)
}
  file.remove(filepath)
  h5createFile(filepath)
  lapply(years, save_hdf, data=data, filepath=filepath,by_var="year_id")
}

## Format combined file for LT function
format_for_lt <- function(data) {
  data$qx <- 0
  # data[sex_id==1,sex:="male"]
  # data[sex_id==2,sex:="female"] 
  # data[sex_id==3,sex:="both"]
  # data <- merge(data,age_map,by="age_group_id")
  data[,id:=draw]
  setnames(data,"year_id","year")
  
  return(data)
}

## Generate ENN/LNN/PNN qx values by converting mx to qx and rescaling to under-1 qx
extract_nn_qx <- function(env_data,lt_data, type) {
  lt_data <- data.table(lt_data)
  
  # env_data <- melt(env_data,id.vars=c("location_id","sex_id","year_id","age_group_id","pop"),variable.name="draw",variable.factor=F)
  # env_data[,draw:=as.numeric(gsub("env_","",draw))]
  if(type == "whiv"){
    env_data[,mx := mx_avg_whiv/pop]
  }else {
    env_data[,mx := mx_hiv_free/pop]
  }
  
  
  ## Convert envelope mx values to qx
  env_data <- env_data[age_group_id <= 4,]
  env_data[age_group_id==2,time:=7/365]
  env_data[age_group_id==3,time:=21/365]
  env_data[age_group_id==4,time:=(365-21-7)/365]

  env_data[,qx:= 1 - exp(-1 * time * mx)]
  env_data[,c("time","mx","pop"):=NULL]
  
  ## Scale qx values to under-1 qx from the LT
  env_data[,age_group_id:=paste0("qx_",age_group_id)]
  setnames(env_data, "sim", "draw")
  qx_nn <- data.table(dcast(env_data,sex_id+year_id+draw~age_group_id,value.var="qx"))
  
  ## Merge on under-1 qx from LT process
  lt_data <- lt_data[age==0,list(age,sex_id,year,qx,draw)]
  setnames(lt_data,"qx","qx_under1")
  setnames(lt_data,"year","year_id")
  lt_data[,age:=NULL]
  
  ## Generate proportions of under-1 deaths that happened in each specific NN age group
  qx_nn <- merge(qx_nn,lt_data,by=c("sex_id","year_id","draw"))
  qx_nn[,prob_2:=qx_2/qx_under1]
  qx_nn[,prob_3:=(1-qx_2) * qx_3/qx_under1]
  qx_nn[,prob_4:=(1-qx_3) * (1-qx_2) * qx_4/qx_under1]
  
  qx_nn[,scale:=1/(prob_2+prob_3+prob_4)]
  qx_nn[,prob_2:=prob_2*scale]
  qx_nn[,prob_3:=prob_3*scale]
  qx_nn[,prob_4:=prob_4*scale]
  
  qx_nn[,qx_2:=qx_under1 * prob_2]
  qx_nn[,qx_3:=(qx_under1 * prob_3)/(1-qx_2)]
  qx_nn[,qx_4:=(qx_under1 * prob_4)/((1-qx_2)*(1-qx_3))]
  
  qx_nn <- qx_nn[,list(sex_id,year_id,draw,qx_2,qx_3,qx_4)]
  
  ## Output Results
  qx_nn <- melt(qx_nn,id=c("sex_id","year_id","draw"))
  qx_nn[,age_group_id:=as.numeric(gsub("qx_","",variable))]
  qx_nn[,variable:=NULL]
  setnames(qx_nn,"value","qx")
  return(qx_nn)
}


#############################################################################################
## Format data appropriately for append/merging

## Format lifetables
lt_del <- format_lts(lt_del)
lt_del <- lt_del[sex_id != 3,] 
lt_whiv <- format_lts(lt_whiv)
# Steal both sexes under-5 from here so that this will be guaranteed to preserve with-HIV 5q0 for both sexes
whiv_both_u5 <- lt_whiv[sex_id == 3 & (age_group_id == 5 | age_group_id == 28),list(sex_id,year_id,age_group_id,draw,mx,ax)]
lt_whiv <- lt_whiv[sex_id != 3,]


## Get ax values
del_ax <- get_lt_ax(lt_del)
whiv_ax <- get_lt_ax(lt_whiv)

## Get u5 mx values for all lifetables using pre-reckoning lifetables of those we want to stay the same, combined with HIV values from ensemble
if(group %in% c("1A","1B")) {
  whiv_u5 <- get_u5_lt(lt_whiv)
  del_u5 <- create_u5_mx_group1(whiv_u5)
} else {
  whiv_u5 <- get_u5_lt(lt_whiv)
  del_u5 <- create_u5_mx(whiv_u5)
}

## Also pull out over-95 mx
if(group == "1A") {
  del_95plus <- get_lt_mx(lt_del)
  whiv_95plus <- scale_lt_over95(del_95plus,env_scalar)
} else {
  whiv_95plus <- get_lt_mx(lt_whiv)
  del_95plus <- scale_lt_over95(whiv_95plus,env_scalar)
}

## Format envelopes for rbinding
env_del <- format_env(raw_env_del, "hiv_free")
env_whiv <- format_env(raw_env_whiv, "with_hiv")

## Combine envelope and over-80 mx results
env_del <- rbind(env_del,del_95plus,del_u5)
env_del <- merge(env_del,del_ax,by=c("age_group_id","sex_id","year_id","draw"))
env_del <- prep_both_sexes(env_del,"hiv_del") ## Create both-sexes aggregates of mx and ax
assert_values(env_del, colnames(env_del), "not_na")
id_vars <- list(year_id = years, sex_id = c(1:3), age_group_id = c(5:20, 28, 30:33, 44, 45, 148), draw = c(0:999))
assert_ids(env_del, id_vars)

save_mx_ax(env_del,"hiv_free")
env_del <- format_for_lt(env_del)

## Create both-sexes aggregates
env_whiv <- rbind(env_whiv,whiv_95plus,whiv_u5)
env_whiv <- merge(env_whiv,whiv_ax,by=c("age_group_id","sex_id","year_id","draw"))
env_whiv <- prep_both_sexes(env_whiv,"whiv") ## Create both-sexes aggregates of mx and ax
assert_values(env_whiv, colnames(env_whiv), "not_na")
assert_ids(env_whiv, id_vars)

save_mx_ax(env_whiv,"whiv")
env_whiv <- format_for_lt(env_whiv)
  

#############################################################################################
## Run LT function
lt_new_del <- lifetable(env_del,cap_qx=1)
lt_new_del$id <- NULL
lt_new_del <- merge(lt_new_del,age_map,by="age_group_id")

lt_new_whiv <- lifetable(env_whiv,cap_qx=1)
lt_new_whiv$id <- NULL
lt_new_whiv <- merge(lt_new_whiv,age_map,by="age_group_id")

## Create and output with-HIV neonatal granular qx values based on the lifetables above
nn_qx_del <- extract_nn_qx(raw_env_del,lt_new_del, type = "hiv_free")
write.csv(nn_qx_del,paste0(filepath),row.names=F)

nn_qx_whiv <- extract_nn_qx(raw_env_whiv,lt_new_whiv, type = "whiv")
write.csv(nn_qx_whiv,paste0(filepath),row.names=F)


#############################################################################################
## Write LT files
write.csv(lt_new_del,paste0(filepath),row.names=F)
write.csv(lt_new_whiv,paste0(filepath),row.names=F)


#############################################################################################
## Calculate mean 5q0 and mean 45q15 values
## HIV-free
mean_5q0 <- calc_qx(lt_new_del,age_start=0,age_end=5,id_vars=c("sex_id","year","draw"))
setnames(mean_5q0,"qx_5q0","mean_5q0")
mean_5q0 <- mean_5q0[,lapply(.SD,mean),.SDcols="mean_5q0", by=c("sex_id","year")]
mean_5q0[,location_id:=loc_id]

mean_45q15 <- calc_qx(lt_new_del,age_start=15,age_end=60,id_vars=c("sex_id","year","draw"))
setnames(mean_45q15,"qx_45q15","mean_45q15")
mean_45q15 <- mean_45q15[,lapply(.SD,mean),.SDcols="mean_45q15",by=c("sex_id","year")]
mean_45q15[,location_id:=loc_id]

write.csv(mean_5q0,paste0(filepath),row.names=F)
write.csv(mean_45q15,paste0(filepath),row.names=F)

## With-HIV
mean_5q0 <- calc_qx(lt_new_whiv,age_start=0,age_end=5,id_vars=c("sex_id","year","draw"))
setnames(mean_5q0,"qx_5q0","mean_5q0")
mean_5q0 <- mean_5q0[,lapply(.SD,mean),.SDcols="mean_5q0", by=c("sex_id","year")]
mean_5q0[,location_id:=loc_id]

mean_45q15 <- calc_qx(lt_new_whiv,age_start=15,age_end=60,id_vars=c("sex_id","year","draw"))
setnames(mean_45q15,"qx_45q15","mean_45q15")
mean_45q15 <- mean_45q15[,lapply(.SD,mean),.SDcols="mean_45q15",by=c("sex_id","year")]
mean_45q15[,location_id:=loc_id]

write.csv(mean_5q0,paste0(filepath),row.names=F)
write.csv(mean_45q15,paste0(filepath),row.names=F)


#############################################################################################
## Summarize the LT files
summarize_lt <- function(data) {
  varnames <- c("ax","mx","qx")
  data <- data[,lapply(.SD,mean),.SDcols=varnames,
                                by=c("age_group_id","sex_id","year","n")]
  data[,id:=1]
  # Rerun lifetable function to recalculate life expectancy and other values based on the mean lifetable
  data <- lifetable(data,cap_qx=1)
  data$id <- NULL
  
  return(data)
}

lt_new_del <- summarize_lt(lt_new_del)
lt_new_del$location_id <- loc_id
lt_new_whiv <- summarize_lt(lt_new_whiv)
lt_new_whiv$location_id <- loc_id

write.csv(lt_new_del,paste0(filepath),row.names=F)
write.csv(lt_new_whiv,paste0(filepath),row.names=F)

