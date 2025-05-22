################################################################################
# Purpose: Chagas disease apply PAR adjustment to the data

# # Script purpose: Adjust input data to population at risk.
################################################################################
source("FILEPATH/get_draws.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/get_bundle_version.R")

################################################################################
# set paths and vars
bundle_version <- ADDRESS
release_id <- ADDRESS
date <- Sys.Date()
path <- paste0("FILEPATH")

################################################################################
# pull bundle data
dt_chagas <- get_bundle_version(bundle_version)

dt_chagas2 <- copy(dt_chagas )
dt_chagas2[, sample_size:=  mean*(1-mean)/standard_error^2]

dt_chagas2[, sample_size := ceiling(sample_size)]

# generate draws for input data
dt_chagas2[, paste0("draw_", 0:999) := lapply(0:999, function(x){
  rbinom(n = nrow(dt_chagas2), size = ceiling(sample_size), prob = mean) / ceiling(sample_size)
})]

dt_chagas2[sample_size <1, sample_size :=1]

# read in PAR estimates
chagas_par <- fread(paste0(path,'FILEPATH'))
setnames(chagas_par, c(paste0("draw_",0:999)), c(paste0("draw_par_",0:999))) 

# get locations to adjust
loc_par <- unique(chagas_par$location_id)
dt_chagas2_tocorrect <- subset(dt_chagas2, location_id %in% loc_par)
dt_chagas2_keep <- subset(dt_chagas, !(location_id %in% loc_par))

# loop through locations and apply adjustment
dt_corrected <- data.table()
i =1
for (i in 1:nrow(dt_chagas2_tocorrect)){
  
  dt <- dt_chagas2_tocorrect[i,]
  
  age_start <- unique(dt$age_start)
  age_end <- unique(dt$age_end)
  
  age <- round((age_start+age_end)/2, digits =0 )
  loc <- unique(dt$location_id)
  study_year_start <- unique(dt$year_start) 
  study_year_end<- unique(dt$year_end) 
  study_year <- round((study_year_start+study_year_end)/2, digits =0)
  
  years <- study_year - age
  atrisk <- subset(chagas_par, location_id  == loc)
  
  if(years <1980){
    atrisk80 <- subset(atrisk, year_id == 1980 )
    atrisk80 <- subset(atrisk80, select = -c(year_id))
    new_rows_years <- as.data.table(expand.grid( year_id = (years:1979)))
    new_rows_years <- cbind(new_rows_years, atrisk80)
    new_rows_years[, location_id := loc]
    
    atrisk <- rbind(atrisk, new_rows_years)
  }
  
  atrisk2 <- subset(atrisk, year_id >= years & year_id <= study_year )
  
  mean_at_risk <- copy(atrisk2)
  mean_at_risk <- mean_at_risk[, lapply(.SD, mean), by = c('location_id' ),
                               .SDcols = c(paste0("draw_par_", c(0:999)))]
  
  dt_merge <- merge(dt, mean_at_risk, by = c('location_id'))
  dt_merge[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("draw_par_", x)))]
  
  dt_corrected <- rbind(dt_corrected, dt_merge)
  print(i)
}

aa1 <- subset(dt_chagas2_tocorrect, is.na(draw_0))

# quick fix NA
fix <- subset(dt_corrected, is.na(draw_0))
fixrows<- fix$seq

# Identify columns starting with "draw_"
draw_cols <- grep("^draw_", names(fix), value = TRUE)

# Remove these columns by setting them to NULL
fix[, (draw_cols) := NULL]

dt_corrected2 <- subset(dt_corrected, !(seq %in% fixrows))
dt_correctedm <- summaries(dt_corrected2)

draw_cols2 <- grep("^draw_", names(dt_correctedm), value = TRUE)

# Remove these columns by setting them to NULL
dt_correctedm[, (draw_cols2) := NULL]
dt_correctedm2 <- rbind(dt_correctedm, fix)

dt_correctedm2[, standard_error := NA]
dt_correctedm2[, sample_size := NA]

dt_correctedm2[, note_modeler := paste0(note_modeler , " Adjusted for PaR")]

dt_corrected_final <- rbind(dt_correctedm2,dt_chagas2_keep )

dt_corrected_final[,crosswalk_parent_seq:=seq]
dt_corrected_final[,seq:=NA]

dt_corrected_final[is.na(upper), uncertainty_type_value := NA]
dt_corrected_final[!(is.na(upper)), uncertainty_type_value :=95]
dt_corrected_final[upper<=lower, upper:= mean+ 0.001]

dt_corrected_final[, geo_notes  := substr(geo_notes , start = 1, stop = 500)]
dt_corrected_final[, note_SR   := substr(note_SR  , start = 1, stop = 500)]

# save out results
openxlsx::write.xlsx(dt_corrected_final, file = paste0(path, 'FILEPATH'), sheetName = "extraction", 
                     colNames = TRUE, rowNames = F, append = FALSE)
################################################################################