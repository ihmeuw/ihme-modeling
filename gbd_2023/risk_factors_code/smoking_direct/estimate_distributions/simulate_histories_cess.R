###############################################################################################################
## Purpose: Recreate smoking histories, avoiding assumption that amount smoked now is amount smoked in the past
###############################################################################################################

# Source libraries
message(Sys.time())
message("Preparing Environment")
source(FILEPATH)

# setting up for array jobs
task_id <- 768
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(paste0('task_id: ', task_id))
parameters <- fread(paste0(FILEPATH, "array_cess_hist.csv"))

l <- parameters[task_id, location_id]
s <- parameters[task_id, sex_id]
output_path <-  parameters[task_id, output_path]
year_list <-  parameters[task_id, year_list]
cess_runid <-  parameters[task_id, cess_runid]
weights_root <- parameters[task_id, weights_root]

# Set some useful objects
year_list <- as.numeric(strsplit(year_list,",")[[1]])
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:249))

message(paste0(cess_runid))
print("Inside script")
# Pull location hierarchy
locs<-get_location_metadata(22, release_id=16)
age_map <- get_age_metadata(24, release_id=16)

# addition for code efficiency
me_name = "cess_age_smoking"
region_weights<-fread(paste0(FILEPATH, me_name, "_2017/region_weights.csv"))
super_region_weights<-fread(paste0(FILEPATH, me_name, "_2017/super_region_weights.csv"))
global_weights <- fread(paste0(FILEPATH, me_name, "_2017/global_weights.csv"))


# Calculate an exp function for distribution of years since quitting (based on age of cessation)
message("Simulating Years Since Quitting")
mean_cess<-fread(paste0(FILEPATH, cess_runid, "/draws_temp_0/", l, ".csv"))
mean_cess<-rep_term_ages(mean_cess)
mean_cess<-melt(mean_cess, id.vars = idvars, measure.vars = drawvars, value.name = "mean")

sd_cess<-fread(paste0(FILEPATH, "/standard_deviation/cess_age/", l, ".csv"))

sd_cess<-rep_term_ages(sd_cess)
sd_cess<-melt(sd_cess, id.vars = idvars, measure.vars = drawvars, value.name = "sd")

mean_sd_cess<-as.data.table(merge(mean_cess, sd_cess, by = c(idvars, "variable")))

ages <- c(9:20, 30, 31, 32, 235)

for (y in year_list) {
  message(paste(y))
  for (a in ages) { 
    message(paste(a))
      temp<-mean_sd_cess[age_group_id==a & sex_id == s & year_id == y]
      # subset these for the location of interest
      region_weights_loc<-region_weights[age_group_id==a & sex_id == s & region_name==locs$region_name[locs$location_id==l]]
      super_region_weights_loc<-super_region_weights[age_group_id==a & sex_id == s & super_region_name==locs$super_region_name[locs$location_id==l]]
      
      # figure out which weights are most appropriate to use:
      if(nrow(region_weights_loc) != 1){
        if(nrow(super_region_weights_loc) != 1){
          weights <- global_weights
        } else{
          weights <- super_region_weights_loc
        }
      } else{
        weights <- region_weights_loc
      }
      
      weights <- weights %>% dplyr::ungroup() %>% dplyr::select(exp, gamma, invgamma, llogis, gumbel, weibull, lnorm, norm, betasr, mgamma, mgumbel) %>% dplyr::slice(1)
      
       
      assign(paste0("cess_", a, "_", s, "_", y), mapply(fit_dist_v2, location = l, year = y, age = a, sex = s, me_name = "cess_age_smoking",
                                                        mean = temp$mean, sd = temp$sd, minval = 5, 
                                                        maxval = age_map[age_group_id==a, age_group_years_end], 
                                                        n_samples = 5000, 
                                                        subtract_age = T, return_fxn = T, w = list(weights)))
  }
}

# Save the image to be loaded in in PAF calculation (this reduces computation time because we only need to re-simulate exposure when exposures change (as opposed to when RR change))
message("Saving Image")

dir.create(paste0(FILEPATH,"/sim_histories/"),showWarnings = F)
dir.create(paste0(FILEPATH,"/sim_histories/cess/"),showWarnings = F)
dir.create(paste0(FILEPATH,"/sim_histories/cess/sex_",s,"/"),showWarnings = F)

save(list = ls()[grepl("cess_", ls())], file = paste0(FILEPATH,"/sim_histories/cess/sex_",s,"/", l, ".RData"))
message("Simulating Smoking Histories Complete!!")
message(Sys.time())
