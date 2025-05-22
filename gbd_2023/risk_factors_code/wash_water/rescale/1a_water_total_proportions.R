# Calculate water proportions 

rm(list=ls())

# Libraries ########################
library(data.table)
library(readr)
library(fst)

# Shared functions #####################
source("FILEPATH/get_draws.R")
source("FILEPATH/get_demographics_template.R")

# Variables ###################
# args from launch
args <- commandArgs(trailingOnly = TRUE)
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))

release <- args[1]
gbd <- args[2] #for filepaths
loc <- args[3]
out.dir <- args[4]


imp_me<-23525
piped_me<-23524
no_treat_me<-23526
treated_me<-23527
fecal_me<-23528
measure<-18 #measure id, proportion
draw_col<-c(paste0("draw_",0:999))

# Draws ########################
# Get draws from STGPR
imp<-get_draws("modelable_entity_id", imp_me, source = "stgpr",release_id = release,location_id=loc)[,-c("stgpr_model_version_id","modelable_entity_id","age_group_id","sex_id","measure_id", "metric_id")]
piped<-get_draws("modelable_entity_id", piped_me, source = "stgpr",release_id = release,location_id=loc)[,-c("stgpr_model_version_id","modelable_entity_id","age_group_id","sex_id","measure_id", "metric_id")]
no_treat<-get_draws("modelable_entity_id", no_treat_me, source = "stgpr",release_id = release,location_id=loc)[,-c("stgpr_model_version_id","modelable_entity_id","age_group_id","sex_id","measure_id", "metric_id")]
treated<-get_draws("modelable_entity_id", treated_me, source = "stgpr",release_id = release,location_id=loc)[,-c("stgpr_model_version_id","modelable_entity_id","age_group_id","sex_id","measure_id", "metric_id")]
fecal<-get_draws("modelable_entity_id", fecal_me, source = "stgpr",release_id = release,location_id=loc)[,-c("stgpr_model_version_id","modelable_entity_id","age_group_id","sex_id","measure_id", "metric_id")]

# Change draw column names
names(imp)<-sub("draw_","imp_",names(imp))
names(piped)<-sub("draw_","piped_",names(piped))
names(no_treat)<-sub("draw_","no_treat_",names(no_treat))
names(treated)<-sub("draw_","treated_",names(treated))
names(fecal)<-sub("draw_","fecal_",names(fecal))

# Merge ########################
#merge the draws, so that the locations and years line up
draws<-merge(imp,piped,by=c("location_id","year_id"))
draws<-merge(draws,no_treat,by=c("location_id","year_id"))
draws<-merge(draws,treated,by=c("location_id","year_id"))
draws<-merge(draws,fecal,by=c("location_id","year_id"))

#remove ones that we don't need
rm(imp)
rm(piped)
rm(no_treat)
rm(treated)

# total improved ################
# total imp = imp * (1 - piped)
total_imp<-draws[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  print(n)
  imp_col<-paste0("imp_",n)
  piped_col<-paste0("piped_",n)
  
  total_imp<-total_imp[,paste0("total_imp_",n):=draws[,..imp_col]*(1-draws[,..piped_col])]
  
}

# total unimproved #################
# total unimproved = 1 - (total_imp + piped)
total_unimp<-draws[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  imp_col<-paste0("total_imp_",n)
  piped_col<-paste0("piped_",n)
  
  total_unimp<-total_unimp[,paste0("total_unimp_",n):= 1 - (total_imp[,..imp_col] + draws[,..piped_col])]
  
}

# boil/filter ####################
# boil/filter = treated * (1 - no_treat)
total_bf<-draws[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  treat_col<-paste0("treated_",n)
  no_treat_col<-paste0("no_treat_",n)
  
  total_bf<-total_bf[,paste0("total_bf_",n):= draws[,..treat_col] * (1 - draws[,..no_treat_col])]
  
}

# solar/chlorine #######################
# solar/chlorine = 1 - (boil/filter + no_treat)
total_sc<-draws[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  bf_col<-paste0("total_bf_",n)
  no_treat_col<-paste0("no_treat_",n)
  
  total_sc<-total_sc[,paste0("total_sc_",n):= 1-(total_bf[,..bf_col] + draws[,..no_treat_col])]
  
}

# treatment of each source #############
# determine the proportion of the source (piped, imp, unimp) is treated with each treatment type

combo<-draws[,.(location_id,year_id)]

for (n in 0:999){
  #n<-1
  print(n)
  imp_col<-paste0("total_imp_",n)
  piped_col<-paste0("piped_",n)
  unimp_col<-paste0("total_unimp_",n)
  bf_col<-paste0("total_bf_",n)
  sc_col<-paste0("total_sc_",n)
  no_treat_col<-paste0("no_treat_",n)
  
  combo<-combo[,paste0("total_imp_bf_",n):= total_bf[,..bf_col] * total_imp[,..imp_col]]
  combo<-combo[,paste0("total_unimp_bf_",n):= total_bf[,..bf_col] * total_unimp[,..unimp_col]]
  combo<-combo[,paste0("total_piped_bf_",n):= total_bf[,..bf_col] * draws[,..piped_col]]
  
  combo<-combo[,paste0("total_imp_sc_",n):= total_sc[,..sc_col] * total_imp[,..imp_col]]
  combo<-combo[,paste0("total_unimp_sc_",n):= total_sc[,..sc_col] * total_unimp[,..unimp_col]]
  combo<-combo[,paste0("total_piped_sc_",n):= total_sc[,..sc_col] * draws[,..piped_col]]
  
  combo<-combo[,paste0("total_imp_no_treat_",n):= draws[,..no_treat_col] * total_imp[,..imp_col]]
  combo<-combo[,paste0("total_unimp_no_treat_",n):= draws[,..no_treat_col] * total_unimp[,..unimp_col]]
  combo<-combo[,paste0("total_piped_no_treat_",n):= draws[,..no_treat_col] * draws[,..piped_col]]
  
}

# Replace anything >1 and <0 ########
#create a function that will replace the >1 and <0's
limit_ends <- function(val) {
  new_val <- ifelse(val < 0, 0.0001,
                    ifelse(val >= 1, 0.9998, val))
  return(new_val)
}

#get draw columns
imp_bf_col<-c(paste0("total_imp_bf_",0:999))
unimp_bf_col<-c(paste0("total_unimp_bf_",0:999))
piped_bf_col<-c(paste0("total_piped_bf_",0:999))

imp_sc_col<-c(paste0("total_imp_sc_",0:999))
unimp_sc_col<-c(paste0("total_unimp_sc_",0:999))
piped_sc_col<-c(paste0("total_piped_sc_",0:999))

imp_no_treat_col<-c(paste0("total_imp_no_treat_",0:999))
unimp_no_treat_col<-c(paste0("total_unimp_no_treat_",0:999))
piped_no_treat_col<-c(paste0("total_piped_no_treat_",0:999))

# combine the columns into one variable
all_col<-c(imp_bf_col, unimp_bf_col, piped_bf_col, 
           imp_sc_col, unimp_sc_col, piped_sc_col,
           imp_no_treat_col, unimp_no_treat_col, piped_no_treat_col)


# Force the numbers to be 1 or 0 if they exceed the boundaries
combo[, (all_col) := lapply(.SD, limit_ends), .SDcols = all_col]

# Rescale ###########################
#ensure that everything will add up to 1

#Get the totals
totals<-draws[,.(location_id,year_id)]

for (n in 0:999){
  
  print(n)
  imp_bf_col<-c(paste0("total_imp_bf_",n))
  unimp_bf_col<-c(paste0("total_unimp_bf_",n))
  piped_bf_col<-c(paste0("total_piped_bf_",n))
  
  imp_sc_col<-c(paste0("total_imp_sc_",n))
  unimp_sc_col<-c(paste0("total_unimp_sc_",n))
  piped_sc_col<-c(paste0("total_piped_sc_",n))
  
  imp_no_treat_col<-c(paste0("total_imp_no_treat_",n))
  unimp_no_treat_col<-c(paste0("total_unimp_no_treat_",n))
  piped_no_treat_col<-c(paste0("total_piped_no_treat_",n))
  

  totals<-totals[,paste0("total_",n):= (combo[,..imp_bf_col] + combo[,..unimp_bf_col] + combo[,..piped_bf_col] +
                                          combo[,..imp_sc_col] + combo[,..unimp_sc_col] + combo[,..piped_sc_col] +
                                          combo[,..imp_no_treat_col] + combo[,..unimp_no_treat_col] + combo[,..piped_no_treat_col])]

}

# merge totals and combo
combo<-merge(combo,totals,by=c("location_id","year_id"))

#Now rescale 
combo2<-draws[,.(location_id,year_id)]

for (n in 0:999){
  
  print(n)
  
  imp_bf_col<-c(paste0("total_imp_bf_",n))
  unimp_bf_col<-c(paste0("total_unimp_bf_",n))
  piped_bf_col<-c(paste0("total_piped_bf_",n))
  
  imp_sc_col<-c(paste0("total_imp_sc_",n))
  unimp_sc_col<-c(paste0("total_unimp_sc_",n))
  piped_sc_col<-c(paste0("total_piped_sc_",n))
  
  imp_no_treat_col<-c(paste0("total_imp_no_treat_",n))
  unimp_no_treat_col<-c(paste0("total_unimp_no_treat_",n))
  piped_no_treat_col<-c(paste0("total_piped_no_treat_",n))
  
  total_col<-c(paste0("total_",n))

  
  combo2<-combo[,c(imp_bf_col, unimp_bf_col, piped_bf_col,
                   imp_sc_col, unimp_sc_col, piped_sc_col,
                   imp_no_treat_col, unimp_no_treat_col, piped_no_treat_col) := list(get(imp_bf_col) / get(total_col),
                                                              get(unimp_bf_col) / get(total_col),
                                                              get(piped_bf_col) / get(total_col),
                                                              get(imp_sc_col) / get(total_col),
                                                              get(unimp_sc_col) / get(total_col),
                                                              get(piped_sc_col) / get(total_col),
                                                              get(imp_no_treat_col) / get(total_col),
                                                              get(unimp_no_treat_col) / get(total_col),
                                                              get(piped_no_treat_col) / get(total_col))]
  
}


# Add fecal proportions ###########
## Replace 0s and 1s ####
#Place all of the fecal columns into 1 variable
fecal_col<-paste0("fecal_",0:999)

# Force the numbers to be 1 or 0 if they exceed the boundaries
fecal[, (fecal_col) := lapply(.SD, limit_ends), .SDcols = fecal_col]

## Seperate basic and HQ piped ###########
#Merge fecal with combo2
combo2<-merge(combo2, fecal, by=c("location_id","year_id"),all.x=T)

# Basic piped
#Find the proportion of  basic piped for EACH treatment type
# basic piped = piped * fecal contamination proportion

for (n in 0:999){
  
  print(n)
  
  piped_bf_col<-c(paste0("total_piped_bf_",n))
  piped_sc_col<-c(paste0("total_piped_sc_",n))
  piped_no_treat_col<-c(paste0("total_piped_no_treat_",n))
  
  fecal_col<-c(paste0("fecal_",n))
  
  #I found out the using [,..col] runs WAAAYYYYY faster than get(), so doing it this way
  combo2<-combo2[,paste0("total_basic_piped_bf_",n):= combo2[,..piped_bf_col] * combo2[,..fecal_col]]
  combo2<-combo2[,paste0("total_basic_piped_sc_",n):= combo2[,..piped_sc_col] * combo2[,..fecal_col]]
  combo2<-combo2[,paste0("total_basic_piped_no_treat_",n):= combo2[,..piped_no_treat_col] * combo2[,..fecal_col]]

}

# HQ piped
# HQ piped = (piped_bf - basic piped bf) + (piped_sc - basic piped sc) + (piped_no_treat - basic piped no treat)

for (n in 0:999){
  
  print(n)
  
  piped_bf_col<-c(paste0("total_piped_bf_",n))
  piped_sc_col<-c(paste0("total_piped_sc_",n))
  piped_no_treat_col<-c(paste0("total_piped_no_treat_",n))
  
  b_piped_bf_col<-c(paste0("total_basic_piped_bf_",n))
  b_piped_sc_col<-c(paste0("total_basic_piped_sc_",n))
  b_piped_no_treat_col<-c(paste0("total_basic_piped_no_treat_",n))
  
  
  combo2<-combo2[,paste0("total_hq_piped_bf_",n):= (combo2[,..piped_bf_col] - combo2[,..b_piped_bf_col] +
                                                    combo2[,..piped_sc_col] - combo2[,..b_piped_sc_col] +
                                                    combo2[,..piped_no_treat_col] - combo2[,..b_piped_no_treat_col])]
  combo2<-combo2[,paste0("total_hq_piped_sc_",n):= 0]
  combo2<-combo2[,paste0("total_hq_piped_no_treat_",n):= 0]
  
}

# Save FST ##################################
#save just in case it dies
write_fst(combo2,paste0(out.dir,loc,".fst"))






