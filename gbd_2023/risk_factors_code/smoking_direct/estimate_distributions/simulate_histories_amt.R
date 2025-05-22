###############################################################################################################
## Purpose: Recreate smoking histories, avoiding assumption that amount smoked now is amount smoked in the past
###############################################################################################################


# Source libraries
message(Sys.time())
message("Preparing Environment")
source(FILEPATH)

# setting up for array jobs
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(paste0('task_id: ', task_id))
parameters <- fread(paste0(FILEPATH, "array_amt.csv"))

l <- parameters[task_id, location_id]
s <- parameters[task_id, sex_id]
output_path <-  parameters[task_id, output_path]
year_list <-  parameters[task_id, year_list]
weights_root <- parameters[task_id, weights_root]

year_list <- as.numeric(strsplit(year_list,",")[[1]])
# Set some useful objects
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:249))

print("Inside script")
# Pull location hierarchy
locs<-get_location_metadata(22, release_id=16)
age_map <- get_age_metadata(24, release_id=16)

# addition for code efficiency
me_name = "total_amt_smoked"
region_weights<-fread(paste0(FILEPATH, me_name, "_2017/region_weights.csv")) 
super_region_weights<-fread(paste0(FILEPATH, me_name, "_2017/super_region_weights.csv"))
global_weights <- fread(paste0(FILEPATH, me_name, "_2017/global_weights.csv"))

# Calculate an exp function for distribution of amount smoked
message("Simulating Cigarettes Per Smoker Per Day")
mean_amt<-fread(paste0(FILEPATH, "holt_outliered/scaled_supply_side_draws/averaged/draws/", l, ".csv")) 
mean_amt<-melt(mean_amt, id.vars = idvars, measure.vars = drawvars, value.name = "mean")
sd_amt<-fread(paste0(FILEPATH, "standard_deviation/total_amt_smoked/", l, ".csv"))
sd_amt<-melt(sd_amt, id.vars = idvars, measure.vars = drawvars, value.name = "sd")

mean_sd_amt<-merge(mean_amt, sd_amt, by = c(idvars, "variable"))


for (y in year_list) {
  message(paste0(y))
  for (a in c(9:20, 30, 31, 32, 235)) {
      temp<-mean_sd_amt[age_group_id==a & sex_id == s & year_id == y]
      
      # subset these for the location of interest
      region_weights_loc<-region_weights[age_group_id==a & sex_id == s & region_name==locs$region_name[locs$location_id==l]]
      super_region_weights_loc<-super_region_weights[age_group_id==a & sex_id == s & super_region_name==locs$super_region_name[locs$location_id==l]]
      
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
      
      assign(paste0("amt_", a, "_", s, "_", y), mapply(fit_dist_v2, 
                                                       location = l, year = y, age = a, sex = s, me_name = "total_amt_smoked", mean = temp$mean, sd = temp$sd, minval = .01,
                                                       maxval = 60, n_samples = 5000, subtract_age = F, return_fxn = T,
                                                       w = list(weights)))
  }
}

# Save the image to be loaded in in PAF calculation (this reduces computation time because we only need to re-simulate exposure when exposures change (as opposed to when RR change))
message("Saving Image")

dir.create(paste0(FILEPATH,"/sim_histories/"),showWarnings = F)
dir.create(paste0(FILEPATH,"/sim_histories/amt/"),showWarnings = F)
dir.create(paste0(FILEPATH,"/sim_histories/amt/sex_",s,"/"),showWarnings = F)

# ONLY SAVE OBJECTS STARTING WITH "AMT_"
save(list = ls()[grepl("amt_", ls())], file = paste0(FILEPATH,"/sim_histories/amt/sex_",s,"/", l, ".RData"))
message("Simulating Smoking Histories Complete!!")
message(Sys.time())


