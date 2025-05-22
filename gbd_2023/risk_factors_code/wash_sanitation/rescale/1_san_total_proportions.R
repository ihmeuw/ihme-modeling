# Calculate Sanitation proportions 

rm(list=ls())

# Libraries ########################
library(data.table)

# Shared functions #####################
source("FILEPATH/get_draws.R")
source("FILEPATH/get_demographics_template.R")
source("FILEPATH/save_results_epi.R")

# Variables ###################
imp_me<-24808
piped_me<-23529
release<-16
gbd<-"GBD2023"
sex<-c(1,2)
draw_col<-c(paste0("draw_",0:999))
measure<-18 #measure id, proportion
best<-T #do you want to mark it best?
descrip<-"location fixes for some sources"
years<-c(1990:2024)

#me ids, bundle ids, cw ids
# piped
piped_me_epi<-28867
piped_bundle<-4730
piped_cw<-47183	

#imp 
imp_me_epi<-8879
imp_bundle<-4733
imp_cw<-47184

#unimp
unimp_me<-9369

#get age groups
dem<-get_demographics_template(gbd_team = "epi", release_id = 16)
ages<-unique(dem$age_group_id)
locs<-unique(dem$location_id)

# Draws ########################
# Get draws from STGPR
imp<-get_draws("modelable_entity_id", imp_me, source = "stgpr",release_id = release)[,-c("stgpr_model_version_id","modelable_entity_id","age_group_id","sex_id","measure_id")]
piped<-get_draws("modelable_entity_id", piped_me, source = "stgpr",release_id = release)[,-c("stgpr_model_version_id","modelable_entity_id","age_group_id","sex_id","measure_id")]

# Change draw column names
names(imp)<-sub("draw_","imp_",names(imp))
names(piped)<-sub("draw_","piped_",names(piped))

# Merge ########################
#merge the 2 draws
stgpr<-merge(imp,piped,by=c("location_id","year_id"))

# Estimate total improved ################
total_imp<-stgpr[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  imp_col<-paste0("imp_",n)
  piped_col<-paste0("piped_",n)

  total_imp<-total_imp[,paste0("total_imp_",n):=stgpr[,..imp_col]*(1-stgpr[,..piped_col])]

}

# Estimate unimproved ############
total_unimp<-stgpr[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  imp_col<-paste0("total_imp_",n)
  piped_col<-paste0("piped_",n)

  total_unimp<-total_unimp[,paste0("total_unimp_",n):= 1 - (total_imp[,..imp_col] + stgpr[,..piped_col])]

}

# Replace anything >1 and <0 ########

#create a function that will replace the >1 and <0's
limit_ends <- function(val) {
  new_val <- ifelse(val < 0, 0.0001,
                    ifelse(val > 1, 0.999, val))
  return(new_val)
}

#get draw columns
t_imp_col<-c(paste0("total_imp_",0:999))
t_unimp_col<-c(paste0("total_unimp_",0:999))
t_piped_col<-c(paste0("piped_",0:999))

total_imp[, (t_imp_col) := lapply(.SD, limit_ends), .SDcols = t_imp_col]
total_unimp[, (t_unimp_col) := lapply(.SD, limit_ends), .SDcols = t_unimp_col]
stgpr[, (t_piped_col) := lapply(.SD, limit_ends), .SDcols = t_piped_col]


# Rescale the draws ################
total<-stgpr[,.(location_id,year_id)]

for (n in 0:999){
  #n<-0
  imp_col<-paste0("total_imp_",n)
  unimp_col<-paste0("total_unimp_",n)
  piped_col<-paste0("piped_",n)
  total_col<-paste0("total_",n)

  total<-total[,paste0("total_",n):= (total_imp[,..imp_col] + total_unimp[,..unimp_col] + stgpr[,..piped_col])]

  total_unimp<-total_unimp[,paste0("total_unimp_",n):= (total_unimp[,..unimp_col] / total[,..total_col])]
  total_imp<-total_imp[,paste0("total_imp_",n):= (total_imp[,..imp_col] / total[,..total_col])]
  stgpr<-stgpr[,paste0("piped_",n):= (stgpr[,..piped_col] / total[,..total_col])]

}

# Get the imp + piped values #################
imp_piped<-stgpr[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  imp_col<-paste0("total_imp_",n)
  piped_col<-paste0("piped_",n)

  imp_piped<-imp_piped[,paste0("imp_piped_",n):= (total_imp[,..imp_col] + stgpr[,..piped_col])]

}


# Summarize data ################
# Get the mean, upper and lower CI of the draws

# improved
total_imp_sum<-copy(total_imp)[, "mean_value" := apply(.SD, 1, mean, na.rm = T), .SDcols=t_imp_col]
total_imp_sum[, "lower_value" := apply(.SD, 1, quantile, c(.025), na.rm = T), .SDcols=t_imp_col]
total_imp_sum[, "upper_value" := apply(.SD, 1, quantile, c(.975), na.rm = T), .SDcols=t_imp_col]

total_imp_sum<-total_imp_sum[,.(location_id, year_id, mean_value, lower_value, upper_value)]

# unimproved
total_unimp_sum<-copy(total_unimp)[, "mean_value" := apply(.SD, 1, mean, na.rm = T), .SDcols=t_unimp_col]
total_unimp_sum[, "lower_value" := apply(.SD, 1, quantile, c(.025), na.rm = T), .SDcols=t_unimp_col]
total_unimp_sum[, "upper_value" := apply(.SD, 1, quantile, c(.975), na.rm = T), .SDcols=t_unimp_col]

total_unimp_sum<-total_unimp_sum[,.(location_id, year_id, mean_value, lower_value, upper_value)]

# piped
total_piped_sum<-copy(stgpr)

total_piped_sum[, "mean_value" := apply(.SD, 1, mean, na.rm = T), .SDcols=t_piped_col]
total_piped_sum[, "lower_value" := apply(.SD, 1, quantile, c(.025), na.rm = T), .SDcols=t_piped_col]
total_piped_sum[, "upper_value" := apply(.SD, 1, quantile, c(.975), na.rm = T), .SDcols=t_piped_col]

total_piped_sum<-total_piped_sum[,.(location_id, year_id, mean_value, lower_value, upper_value)]

# imp + piped
imp_piped_col<-c(paste0("imp_piped_",0:999))

imp_piped_sum<-copy(imp_piped)[, "mean_value" := apply(.SD, 1, mean, na.rm = T), .SDcols=imp_piped_col]
imp_piped_sum[, "lower_value" := apply(.SD, 1, quantile, c(.025), na.rm = T), .SDcols=imp_piped_col]
imp_piped_sum[, "upper_value" := apply(.SD, 1, quantile, c(.975), na.rm = T), .SDcols=imp_piped_col]

imp_piped_sum<-imp_piped_sum[,.(location_id, year_id, mean_value, lower_value, upper_value)]

# Prep data for upload ###########

#improved
total_imp_sum[,':='(age_group_id=22, sex_id=3)]

#unimproved
total_unimp_sum[,':='(age_group_id=22, sex_id=3)]

#piped
total_piped_sum[,':='(age_group_id=22, sex_id=3)]

#imp + piped
imp_piped_sum[,':='(age_group_id=22, sex_id=3)]

# Export summaries #########################
#Save to outputs
write.csv(total_imp_sum,paste0("FILEPATH/",gbd,"/total_san_imp.csv"))
write.csv(total_unimp_sum,paste0("FILEPATH/",gbd,"/total_san_unimp.csv"))
write.csv(total_piped_sum,paste0("FILEPATH/",gbd,"/total_san_piped.csv"))

#save to covariates for improved + piped (sanitation improved covariate)
write.csv(imp_piped_sum,"FILEPATH/total_san_imp_piped.csv")

# Draw edits ############################
#To upload the draws to the epi database we have to make the data the same for all of the required age groups and sexes

## piped ##############

#grab just the piped draws
cols_to_keep <- c("location_id", "year_id", grep("^piped_", names(stgpr), value = TRUE))
total_piped<-stgpr[, ..cols_to_keep]

piped_draws<-rbindlist(lapply(ages,function(age){
  print(paste0(age))
  temp_M<-copy(total_piped)
  temp_M[,':='(age_group_id=age,
               sex_id=1,
               measure_id=measure)]

  temp_F<-copy(total_piped)
  temp_F[,':='(age_group_id=age,
               sex_id=2,
               measure_id=measure)]

  temp<-rbind(temp_F,temp_M)

  setnames(temp,t_piped_col,draw_col)

}))

#save RDS
saveRDS(piped_draws,paste0("FILEPATH/",gbd,"/FILEPATH/piped_draws.RDS"))

## imp #################
total_imp_col<-c(paste0("total_imp_",0:999))

total_imp_draws<-rbindlist(lapply(ages,function(age){
  print(paste0(age))
  temp_M<-copy(total_imp)
  temp_M[,':='(age_group_id=age,
               sex_id=1,
               measure_id=measure)]

  temp_F<-copy(total_imp)
  temp_F[,':='(age_group_id=age,
               sex_id=2,
               measure_id=measure)]

  temp<-rbind(temp_F,temp_M)

  setnames(temp,total_imp_col,draw_col)

}))

#save RDS
saveRDS(total_imp_draws,paste0("FILEPATH/",gbd,"/FILEAPTH/imp_draws.RDS"))

## unimp #################
total_unimp_col<-c(paste0("total_unimp_",0:999))

total_unimp_draws<-rbindlist(lapply(ages,function(age){
  print(paste0(age))
  temp_M<-copy(total_unimp)
  temp_M[,':='(age_group_id=age,
               sex_id=1,
               measure_id=measure)]

  temp_F<-copy(total_unimp)
  temp_F[,':='(age_group_id=age,
               sex_id=2,
               measure_id=measure)]

  temp<-rbind(temp_F,temp_M)

  setnames(temp,total_unimp_col,draw_col)

}))

#save RDS
saveRDS(total_unimp_draws,paste0("FILEPATH/",gbd,"/FILEPATH/unimp_draws.RDS"))

## export ##################
lapply(locs, function(loc){
  print(loc)
  imp_temp<-total_imp_draws[location_id==loc,]
  piped_temp<-piped_draws[location_id==loc,]
  unimp_temp<-total_unimp_draws[location_id==loc,]

  write.csv(imp_temp,paste0("FILEPATH/",gbd,"/FILEPATH/",loc,".csv"),row.names = F)
  write.csv(piped_temp,paste0("FILEPATH/",gbd,"/FILEPATH/",loc,".csv"),row.names = F)
  write.csv(unimp_temp,paste0("FILEPATH/",gbd,"/FILEPATH/",loc,".csv"),row.names = F)
})

# Upload to database ###############################
#save the exp estimates to the database

## piped ##################
piped_save <- save_results_epi(
  input_dir = paste0("FILEPATH/",gbd,"/FILEPATH"),
  input_file_pattern = '{location_id}.csv',
  modelable_entity_id = piped_me_epi,
  description = descrip,
  measure_id = measure,
  release_id = release,
  mark_best = best,
  bundle_id = piped_bundle,
  crosswalk_version_id = piped_cw,
  year_id = years
)

## imp ##################
imp_save <- save_results_epi(
  input_dir = paste0("FILEPATH/",gbd,"/FILEPATH"),
  input_file_pattern = '{location_id}.csv',
  modelable_entity_id = imp_me_epi,
  description = descrip,
  measure_id = measure,
  release_id = release,
  mark_best = best,
  bundle_id = imp_bundle,
  crosswalk_version_id = imp_cw,
  year_id = years
)

## unimp ##################
unimp_save <- save_results_epi(
  input_dir = paste0("FILEPATH/",gbd,"/FILEPATH"),
  input_file_pattern = '{location_id}.csv',
  modelable_entity_id = unimp_me,
  description = descrip,
  measure_id = measure,
  release_id = release,
  mark_best = best,
  bundle_id = imp_bundle,
  crosswalk_version_id = imp_cw,
  year_id = years
)
