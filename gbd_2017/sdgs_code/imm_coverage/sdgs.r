#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Generate full coverage indicator for SDGs indicator 3.b.1
#       	 SDGs Indicator 3.b.1 corresponds to full vaccination coverage, or the 
#		       percent of the population covered by all vaccines in the national schedule.
#		       We calculate this as the geometric mean of 8 vaccines (dpt, mcv, bcg, polio, 
#	     	   hepb, hib, pcv, rota), given their respective introduction for that country-year.
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### set up
#rm(list=ls())
pacman::p_load(data.table, dplyr)

username <- Sys.info()[["user"]]
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- paste0("FILEPATH/", username)
}

### source paths
source(paste0(j, "FILEPATH/get_location_metadata.R"))
source("FILEPATH/sql_query.R")
#***********************************************************************************************************************


#----SETUP--------------------------------------------------------------------------------------------------------------
# Set up introduction frame to merge on to the draw frame; used to 
# set coverage to NA if the particular vaccine is not in the national
# schedule at for that country year. This is most relevant for 
# the new vaccines (HepB, Hib, PCV, Rota) prior to their introduction
# and for BCG where countries have removed it from the national schedule.
# For newer vaccines, we allow for 2 years of delay (following the year of 
# introduction) such that countries have time to ramp up the vaccination so
# the geometric mean isn't too jagged.

setup.intro <- function() {
  ### set up intro frame
  intro <- readRDS(paste0(data_root, "FILEPATH/vaccine_intro.rds"))
  intro <- intro[, me_name := paste0(me_name, "_intro")]
  # China 
  intro <- intro[grepl("CHN", ihme_loc_id) & grepl("hib|hepb|pcv|rota", me_name), cv_intro := 9999]
  # create cv_intro_years frame to represent the number of years
  intro <- intro[, cv_intro_years := ifelse((year_id - (cv_intro - 1)) >= 4, year_id - (cv_intro - 1), 0)] ## 2 years of delay for vaccine intro
  intro <- intro[!is.na(cv_outro), cv_intro_years := ifelse((cv_outro - year_id) > 0, year_id - 1980 + 1, 0)]
  # reshape wide
  intro.w <- dcast(intro, location_id + year_id ~ me_name, value.var='cv_intro_years')
  return(intro.w)
}
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------
# Function to calculate the geometric mean by location.
# Pulls draws from the draws_root folder, merges and sets
# introduction dates, then takes the geometric mean
# depending on the number of non-NA vaccinations.
# Function returns a summary mean, lower, upper
# or 1,000 draws.

calc.geo_mean <- function(loc, draws=FALSE, mes=c("vacc_dpt3", "vacc_mcv1", "vacc_bcg", "vacc_polio3", "vacc_hib3", "vacc_hepb3", "vacc_pcv3", "vacc_rotac")) {
  
  ### set intro 
  intro.w <- setup.intro()
  
  ### load
  df.list <- lapply(mes, function(me) {
      path <- paste0(draws_root, "FILEPATH", loc, ".csv")
      df <- fread(path)[, c("location_id", "year_id", paste0("draw_", 0:999)), with=FALSE] %>% unique
      df[, age_group_id := 22]
      df[, sex_id := 3]
      key <- c("location_id", "year_id", "age_group_id", "sex_id")
      df <- melt(df, id.vars=key, measure=patterns("^draw"), variable.name="draw",  value.name=me)
  })
      
  ### merge
  for (i in 1:length(mes)) {
      if (i == 1) {
         df <- df.list[[i]]  
      } else {
         df <- merge(df, df.list[[i]], by=c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
      }
  }
      
  ### set introduction years
  df <- merge(df, intro.w, by=c("location_id", "year_id"))
  for (me in mes[mes %in% c("vacc_hepb3", "vacc_pcv3", "vacc_hib3", "vacc_rotac", "vacc_bcg", "vacc_mcv2")]) {
      df <- df[, (me) := ifelse(get(paste0(me, "_intro")) < 1, NA , get(me))]
  }
  
  ### calculate geometric mean
  df <- df[, n := Reduce(`+`, lapply(.SD,function(x) !is.na(x))), .SDcols=mes]
  df <- df[, vacc_full := Reduce(`*`, lapply(.SD, function(x) ifelse(is.na(x), 1, x))), .SDcols=mes]
  df <- df[, vacc_full := vacc_full ^ (1 / n)]
  ### reshape
  if (draws) {
  df.w <- dcast(df, location_id + year_id + age_group_id + sex_id ~ draw, value.var="vacc_full")
  return(df.w) 
  }
  
  ### compute summary
  if (!draws) {
  key <- c("location_id", "year_id", "age_group_id", "sex_id")
  df <- df[, mean := mean(vacc_full), by=key]
  df <- df[, lower := quantile(vacc_full, 0.025), by=key]
  df <- df[, upper := quantile(vacc_full, 0.975), by=key]
  df.s <- df[, .(location_id, year_id, age_group_id, sex_id, mean, lower, upper)] %>% unique
  return(df.s)
  }
  
}
#***********************************************************************************************************************


#----CALCULATE----------------------------------------------------------------------------------------------------------
### calculate arithmetic mean and save
full_vax_mes <- c("vacc_dpt3", "vacc_mcv1", "vacc_polio3", "vacc_hib3", "vacc_hepb3", "vacc_pcv3", "vacc_rotac", "vacc_mcv2") 
path <- paste0(draws_root, "FILEPATH")
unlink(path, recursive=TRUE)
if (!dir.exists(path)) dir.create(path)
locs <- get_location_metadata(22)[level >= 3]$location_id
mclapply(locs, function(x) {
    df <- calc.arith_mean(x, draws=TRUE, mes=full_vax_mes)
    df[, covariate_id := me_db[me_name=="vacc_fullgeo", covariate_id]]
    write.csv(df, paste0(path, x, ".csv"), na="", row.names=FALSE)
}, mc.cores=10)
#***********************************************************************************************************************


#----UPLOAD INDICATORS--------------------------------------------------------------------------------------------------
### get model versions
model_versions <- sql_query(query="",
                            host="",
                            dbname="")
data_versions <- sql_query(query="",
                            host="",
                            dbname="")
versions <- merge(model_versions, data_versions, by="data_version_id", all.x=TRUE)

### save for sdg upload
sdg_indicators <- c("vacc_fullgeo", "vacc_dpt3", "vacc_polio3", "vacc_mcv1")
goalkeepers <- c("vacc_mcv2", "vacc_pcv3", "vacc_dpt3")
indicators <- c(sdg_indicators, goalkeepers) %>% unique
lapply(indicators, function(x) {
  
  ### save_results
  indicator_id <- me_db[me_name==x, sdg_id]
  gbd_id <- me_db[me_name==x, covariate_id]
  if (x != "vacc_fullgeo") model_vers <- versions[covariate_id==gbd_id, model_version_id] else model_vers <- 5
  print(paste0("Uploading model version ", model_vers, " for ", x))
  path <- paste0(draws_root, "FILEPATH", x)
  
  job <- paste0("qsub -N save_sdg_", x, " -pe multi_slot 10 -P PROJECT -o FILEPATH/", username, " -e FILEPATH/", username, 
                " FILEPATH/r_shell.sh FILEPATH/save_results_wrapper.r",
                " --args",
                " --type sdg",
                " --me_id ", indicator_id, 
                " --input_directory ", path,
                " --descript ", model_vers,
                " --id_type covariate_id",
                " --secondary_id ", gbd_id)
  system(job); print(job)
  
})
#***********************************************************************************************************************