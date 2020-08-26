###############################################################################################################
## Purpose: Recreate smoking histories, avoiding assumption that amount smoked now is amount smoked in the past
###############################################################################################################

# Source libraries
message(Sys.time())
message("Preparing Environment")
source('FILEPATH')

# Read the location as an argument
args <- commandArgs(trailingOnly = TRUE)
l <- ifelse(!is.na(args[1]),args[1], 44538) # take location_id
s <- ifelse(!is.na(args[2]),args[2], 2) # take sex_id

# Set some useful objects
idvars<-c("location_id", "year_id", "age_group_id", "sex_id")
drawvars<-c(paste0("draw_", 0:999))
year_list<-c(1980, 1985, 1990, 1995, 1997, 2000, 2002, 2005, 2007, 2010, 2012, 2017)
init_runid<-43955

# Pull location hierarchy
locs<-get_location_metadata(22)
age_map<-get_age_metadata(12)

# Precompute the amount smoked in preparation for pack-year reconstruction
message("Precomputing Amount Smoked in Preparation for Pack-Year Reconstruction")
mean_amt<-fread(paste0('FILEPATH'))
mean_amt<-melt(mean_amt, id.vars = idvars, measure.vars = drawvars, value.name = "mean")
sd_amt<-fread(paste0('FILEPATH'))
sd_amt<-melt(sd_amt, id.vars = idvars, measure.vars = drawvars, value.name = "sd")
mean_sd_amt<-merge(mean_amt, sd_amt, by = c(idvars, "variable"))
for (a in c(7:20, 30, 31, 32, 235)) {
  for (y in c(seq(1960, 2010, 5), seq(1962, 2017, 5))) {
    temp<-mean_sd_amt[age_group_id==a & sex_id == s & year_id == y]
    assign(paste0("vec_amt_", a, "_", s, "_", y), mapply(fit_dist, location = l, year = y, age = a, sex = s, me_name = "total_amt_smoked", mean = temp$mean, sd = temp$sd, minval = 0.01, maxval = 60, n_samples = 5000, subtract_age = F, return_fxn = F))
  }
}

# Calculate an exp function for distribution of years smoked
message("Simulating Pack-Years")
mean_init<-fread(paste0('FILEPATH'))
mean_init<-rep_term_ages(mean_init)
mean_init<-melt(mean_init, id.vars = idvars, measure.vars = drawvars, value.name = "mean")
sd_init<-fread(paste0('FILEPATH'))
sd_init<-rep_term_ages(sd_init)
sd_init<-melt(sd_init, id.vars = idvars, measure.vars = drawvars, value.name = "sd")
mean_sd_init<-merge(mean_init, sd_init, by = c(idvars, "variable"))
for (y in year_list) {
  message(paste0(y))
  for (a in c(9:20, 30, 31, 32, 235)) {
      temp<-mean_sd_init[age_group_id==a & sex_id == s & year_id == y]
      years_smoked_vec<-round(as.vector(mapply(fit_dist, location = l, year = y, age = a, sex = s, me_name = "init_age_smoking", mean = temp$mean, sd = temp$sd, minval = 5, maxval = max_return(a), n_samples = 100, subtract_age = T, return_fxn = F)),0)
      years_smoked_vec<-sample(years_smoked_vec, size = 5000)
      # Simulate the smoking history of a given location-year-age-sex population and calculate true pack-years
      sim_pop<-NULL
      for (i in unique(years_smoked_vec)) {
        n_samples<-length(years_smoked_vec[years_smoked_vec==i])
        amts_out<-form_cohort_list(start_year = y, start_age = a, sex = s, years_smoked = i)
        sim_history<-calc_packyear(amts=mget(amts_out), n_samples = n_samples)
        sim_pop<-rbind(sim_pop, sim_history)
      }
      out<-apply(sim_pop, function(x) density(x, from = 0.01, to = 200), MARGIN = 2)
      assign(paste0("py_", a, "_", s, "_", y), lapply(out, function(temp) approxfun(temp$x, temp$y, method = "linear", rule = 2)))
  }
}

# Cleanup workspace
rm(list = ls(pattern = "vec_amt"))

# Save the image to be loaded in in PAF calculation
message("Saving Image")
save.image(file = paste0('FILEPATH'))
message("Simulating Smoking Histories Complete")
message(Sys.time())


