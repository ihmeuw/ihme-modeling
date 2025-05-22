rm(list=ls())
message("Preparing Environment")
source(FILEPATH)


# # Read the location as an argument
# setting up for array jobs
task_id <- 1
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(paste0('task_id: ', task_id))
parameters <- fread(FILEPATH)

l <- parameters[task_id, location_id]
y <- parameters[task_id, year_list]

output_path <-  parameters[task_id, output_path]
weights_root <- parameters[task_id, weights_root]


# Set some useful objects
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:249))

# addition for code efficiency
me_name = "total_amt_smoked"
region_weights<-fread(FILEPATH, "region_weights.csv")
super_region_weights<-fread(FILEPATH, "super_region_weights.csv")
global_weights <- fread(FILEPATH, "global_weights.csv")

locs<-get_location_metadata(22, release_id=16)
age_map <- get_age_metadata(24, release_id=16)

ages <- c(7:20, 30, 31, 32, 235)

mean_amt<-fread(paste0(FILEPATH, "draws/", l, ".csv")) 
mean_amt<-melt(mean_amt, id.vars = idvars, measure.vars = drawvars, value.name = "mean")
sd_amt<-fread(paste0(FILEPATH, "total_amt_smoked/", l, ".csv"))
sd_amt<-melt(sd_amt, id.vars = idvars, measure.vars = drawvars, value.name = "sd")

mean_sd_amt<-as.data.table(merge(mean_amt, sd_amt, by = c(idvars, "variable")))

preobs <- c()

for (s in c(1,2)){
  for (a  in ages){
    # Precompute the amount smoked in preparation for pack-year reconstruction
    message("Precomputing Amount Smoked in Preparation for Pack-Year Reconstruction")
    message(paste0("Sex ",s))
    
    message(paste0("Computing the amount smoked distribution for location: ",l,", sex ",s,", age group ID ",a,", and year ",y))
    
    temp<-mean_sd_amt[age_group_id==a & sex_id == s & year_id == y]
    
    
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
    
    
    assign(paste0("vec_amt_", a, "_", s, "_", y), mapply(fit_dist_v2, 
                                                         location = l, year = y, age = a, sex = s, me_name = "total_amt_smoked",
                                                         mean = temp$mean, sd = temp$sd, minval = 0.01, maxval = 60, n_samples = 5000, subtract_age = F, return_fxn = F,
                                                         w = list(weights))) # does the maximum value of 60 matter?
    
  }

}
message(Sys.time())

message("Saving Image")
dir.create(paste0(FILEPATH,"amt_smoked_dist_files/"),showWarnings = F)
dir.create(paste0(FILEPATH,"amt_smoked_dist_files/",l),showWarnings = F)
save(list = ls()[grepl("vec_amt_", ls())], file = paste0(FILEPATH,"amt_smoked_dist_files/",l,"/vec_amt_", l,"_",y, ".RData"))
message("Simulating Smoking Histories Complete!!")


