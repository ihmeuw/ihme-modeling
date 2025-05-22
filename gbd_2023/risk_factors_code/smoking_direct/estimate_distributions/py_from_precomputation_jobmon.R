###############################################################################################################
## Purpose: Recreate smoking histories, avoiding assumption that amount smoked now is amount smoked in the past
###############################################################################################################
# Source libraries
message(Sys.time())
message("Preparing Environment")
source(FILEPATH)

# setting up for array jobs
task_id <- 11
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
print(paste0('task_id: ', task_id))
parameters <- fread(paste0(FILEPATH, "array_py.csv"))

l <- parameters[task_id, location_id]
y <- parameters[task_id, year_list]
s <- parameters[task_id, sex_id]
init_runid <- parameters[task_id, init_runid]
output_path <-  parameters[task_id, output_path]
weights_root <- parameters[task_id, weights_root]

ages <- c(9:20, 30, 31, 32, 235)

l <- as.numeric(l)
y <- as.numeric(y)
s <- as.numeric(s)

# Set some useful objects
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:249))

# Pull location hierarchy
locs<-get_location_metadata(22, release_id=16)
age_map <- get_age_metadata(24, release_id=16)

# addition for code efficiency
me_name = "init_age_smoking"
region_weights<-fread(paste0(weights_root, me_name, "_2017/region_weights.csv"))
super_region_weights<-fread(paste0(weights_root, me_name, "_2017/super_region_weights.csv"))
global_weights <- fread(paste0(weights_root, me_name, "_2017/global_weights.csv"))

# Calculate an exp function for distribution of years smoked --> update to either run at the mean level or at the draw level... this is an intermediate for efficiency right now
message("Simulating Pack-Years")
mean_init<-fread(paste0(FILEPATH, init_runid, "/draws_temp_0/", l, ".csv"))
mean_init<-rep_term_ages(mean_init)
mean_init<-melt(mean_init, id.vars = idvars, measure.vars = drawvars, value.name = "mean")
sd_init <- fread(paste0(FILEPATH, "standard_deviation/init_age/", l, ".csv"))
sd_init <- rep_term_ages(sd_init)
sd_init <- melt(sd_init, id.vars = idvars, measure.vars = drawvars, value.name = "sd")

mean_sd_init<-as.data.table(merge(mean_init, sd_init, by = c(idvars, "variable")))

# save year ID because it gets changed later down:
y_og <- y
ages_new <- ages

  for (a in ages_new){
    # need to save these because they are getting changed when I read in the RData files below
    age_og <- a
    s_og <- s
    temp<-mean_sd_init[age_group_id==a & sex_id == s & year_id == y]
    
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
    
    
    years_smoked_vec<-round(as.vector(mapply(fit_dist_v2, 
                                             location = l, year = y, age = a, sex = s, me_name = "init_age_smoking",
                                             mean = temp$mean, sd = temp$sd, minval = 10, 
                                             maxval = age_map[age_group_id==a, age_group_years_end],
                                             n_samples = 100, subtract_age = T, return_fxn = F,w = list(weights))),0)
    
    years_smoked_vec<-sample(years_smoked_vec, size = 5000)
    # Simulate the smoking history of a given location-year-age-sex population and calculate true pack-years
    sim_pop<-NULL
    for (i in unique(years_smoked_vec)) {
      n_samples<-length(years_smoked_vec[years_smoked_vec==i])
      amts_out<-form_cohort_list(start_year = y, start_age = a, sex = s, years_smoked = i)
      obs<-ls()
      
      years_to_get <- c()
      amts_out_to_get <- setdiff(amts_out,obs) 
      for(j in amts_out_to_get){
        y_new <- strsplit(j,"_")[[1]][5]
        years_to_get <- unique(c(years_to_get,y_new))
      }
      
      
      for (yr in unique(years_to_get)){
        print(yr)
          dt <- load(paste0(FILEPATH, "amt_smoked_dist_files/", l, "/vec_amt_", l, "_", yr, ".RData"))
      }
      source(FILEPATH)
      a <- age_og
      s <- s_og
      y <- y_og
      
      sim_history<-calc_packyear(amts=mget(amts_out), n_samples = n_samples)
      sim_pop<-rbind(sim_pop, sim_history)

    }
    out<-apply(sim_pop, function(x) density(x, from = .01, to = 200, n=2^12), MARGIN = 2)  # cap for py is 225 though
    assign(paste0("py_", a, "_", s, "_", y), lapply(out, function(temp) approxfun(temp$x, temp$y, method = "linear", rule = 2)))

  }
  
  # Cleanup workspace
  # Save the image to be loaded in in PAF calculation (this reduces computation time because we only need to re-simulate exposure when exposures change (as opposed to when RR change))
  message("Saving Image")
  task_id <- 11
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  message(paste("end id:", task_id))
  parameters <- fread(paste0(FILEPATH, "array_py.csv"))
  output_path <-  parameters[task_id, output_path]
  l <- parameters[task_id, location_id]
  y <- parameters[task_id, year_list]
  s <- parameters[task_id, sex_id]
  
  dir.create(paste0(FILEPATH,"/sim_histories/"),showWarnings = F)
  dir.create(paste0(FILEPATH,"/sim_histories/py/"),showWarnings = F)
  dir.create(paste0(FILEPATH,"/sim_histories/py/sex_",s,"/"),showWarnings = F)
  dir.create(paste0(FILEPATH,"/sim_histories/py/sex_",s,"/",l,"/"),showWarnings = T)
  
  rm(list=ls(pattern="vec_amt"))
  message("Simulating Smoking Histories Complete!!")

  save(list = ls()[grepl("py_", ls())], file = paste0(FILEPATH,"/sim_histories/py/sex_",s,"/",l,"/", l,"_",y, ".RData"))
  message(Sys.time())
  







