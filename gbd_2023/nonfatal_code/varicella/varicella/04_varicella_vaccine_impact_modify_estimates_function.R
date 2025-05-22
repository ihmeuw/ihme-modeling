################################################################################
# VARICELLA VACCINE EFFICACY MODEL
# PURPOSE: SCRIPT_03: MODIFY INCIDENCE ESTAIMATES BY APPLYING VACCINE CORRECTION
# DATE 7-29-2024
# AUTHOR: REDACTED
# NB THIS script is launched from the varicella 01 script as a function
################################################################################

################################################################################
# PART 4: APPLY VACCINE EFFICACY TO INCIDENCE ESTIMATES ####
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

prep_vaccine <- function(...){

#*******************************************************************************
# 1. PREPARE DATA ####
#*******************************************************************************

# Read vaccine effectiveness MRBRT model predictions
prediction_frame <- fread(paste0("/FILEPATH", "/mrbrt_pred_frame_vaccine_efficacy.csv"))
setDT(prediction_frame)
prediction_frame <- prediction_frame[, V1 := NULL]

# Read in vaccine year of introduction data
intro <- fread("/FILEPATH/varicella_year_vax_intro_2024_09_25.csv")

# Rename column
setnames(intro, "vax_discontinue_year", "vax_last_year")

# Create country code variable: this will be used to apply effectiveness scale to subnational locations
locations[, country_code := substr(ihme_loc_id, 1, 3)]

# Identify countries with vaccine programs and without
locs_vax <- unique(locations$location_id[locations$country_code %in% unique(intro$ihme_loc_id)])
locs_no_vax <- locations$location_id[!locations$location_id %in% locs_vax]

# Check to make sure that there is no overlap between non vaccine locs and vaccine locs 
problem <- intersect(locs_vax, locs_no_vax)
if(length(problem) > 0) {
  message("There is a problem with location assignment")
}else{
  print("Locations assigned correctly, proceed with interpolating results")
}
  
# Define maximal year for vaccine introduction report
max_rep <- max(intro$year_id)
  
# Identify deimplementation locations and years
deimplementation_locs <- intro$ihme_loc_id[intro$vax_last_year != max_rep] %>% unique()
implementation_years <- intro$vax_intro_year[intro$ihme_loc_id %in% deimplementation_locs]
deimplementation_years <- intro$vax_last_year[intro$ihme_loc_id %in% deimplementation_locs]
stop_loc_years <- list(deimplementation_locs, implementation_years, deimplementation_years)

# Get subnational and national locations where vaccination was discontinued
stop_loc_ids <- locations$location_id[locations$country_code %in% deimplementation_locs]

# Add locations where vaccine was introduced then discontinued to non vaccine locations!
locs_no_vax <- c(locs_no_vax, stop_loc_ids)

#*******************************************************************************
# 2. INTERPOLATE DATA FOR VACCINE LOCATIONS ####
#*******************************************************************************
  
  # Get interpolated prevalence data for vaccine locations
print("Interpolating varicella seroprevalence for vaccine locations")
tic()
interpolate_prev <- interpolate(
    gbd_id_type = "modelable_entity_id",
    gbd_id = 1439,
    source = "epi",
    measure_id = 5, 
    release_id = release_id,
    location_id = locs_vax,
    sex_id = "all", 
    reporting_year_start = 1990,
    reporting_year_end = year_end,
    num_workers = 10)
  message("Varicella prevalence draws interpolated")
  toc()
  
# Get interpolated incidence data for vaccine locations  
print("Interpolating varicella incidence hazard for vaccine locations")
  tic()
  interpolate_inc <- interpolate(
    gbd_id_type = "modelable_entity_id",
    gbd_id = 1439,
    source = "epi",
    measure_id = 6, 
    release_id = release_id,
    location_id = locs_vax,
    # location_id = standard_locations, 
    sex_id = "all", 
    reporting_year_start = 1990,
    reporting_year_end = year_end,
    # n_draws = 1000, 
    # downsample = T, 
    num_workers = 10
  )
  message("Varicella incidence draws interpolated")
  toc()
  
# Save interpolated draws  
fwrite(interpolate_inc, paste0(j.version.dir, "/03a_interpolated_varicella_incidence_results.csv")) 
fwrite(interpolate_prev, paste0(j.version.dir, "/03b_interpolated_varicella_prevalence_results.csv"))# 
message("Interpolated results saved")
  
# Save model version
cat(paste0("Varicella DisMod model (me_id 1439) for prevalence - model version ", unique(interpolate_prev$model_version_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
cat(paste0("Varicella DisMod model (me_id 1439) for incidence - model version ", unique(interpolate_inc$model_version_id)),
    file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
#*******************************************************************************
# 3. CALCULATE WITHOUT VACCINE INCIDENCE ####
#*******************************************************************************
  
# Prepare data for calculations
draw_nums_int    <- 0:999 
draw_int_upload <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_int))
  
# Prep prevalence draws for incidence rate calculation
var_prev2 <- interpolate_prev[, draw_int_upload, with=FALSE]
colnames(var_prev2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_int))
  
# Prep incidence draws for incidence rate calculation
var_inc2 <- interpolate_inc[, draw_int_upload, with=FALSE]
colnames(var_inc2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("hazard_draw_", draw_nums_int))
  
# Merge incidence and prevalence draws
var_nonfatal_vax <- merge(var_prev2, var_inc2, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  
# Remove large draw files and clear memory
rm(interpolate_prev, interpolate_inc)
gc(T)
  
#*******************************************************************************
# 4. CALCULATE VACCINE-MODIFIED INCIDENCE ####
#*******************************************************************************
  
# Calculate incidence from hazards: incidence = hazard * (1 - prevalence)
draw_cols_gbd <- paste0("draw_", draw_nums_int) 
var_nonfatal_vax[, (draw_cols_gbd) := lapply(draw_nums_int, function(ii) get(paste0("hazard_draw_", ii)) * ( 1 - get(paste0("prev_draw_", ii)) ) )]
  
# Keep needed columns
inc_vax <- subset(var_nonfatal_vax, select = draw_int_upload)
  
# Merge with locations to get ihme_loc_id in otherwise subnationals will not be merged with vaccine efficacy estimates
inc_vax <- merge(inc_vax, locations[, .(location_id, ihme_loc_id)], by = "location_id", all.x =T)
  
# Keep only first 3 letters of ihme_loc_id for merge with national parent
inc_vax[, ihme_loc_id := substr(ihme_loc_id, 1, 3)]
  
# Merge the vaccination locations with intro year
intro <- intro[, c("location_id", "year_id", "nat_vax_intro_year") := NULL]
with_vax <- merge(inc_vax, intro, by = c("ihme_loc_id"), all.x = T)
  
# Create a year_post_uvv columns
with_vax[, num_years_post_uvv := year_id - vax_intro_year] 
with_vax[, num_years_post_uvv := ifelse(num_years_post_uvv > 10, 10, num_years_post_uvv)]
  
# Keep incidence constant post year of vax introduction 
mean_inc_year_0 <- with_vax[num_years_post_uvv == 0, ] # year zero is year of introduction
mean_inc_year_0 <- mean_inc_year_0[, c("num_years_post_uvv", "year_id") := NULL]
  
# subset vaccine locations to years pre and post vax
pre_vax <- with_vax[num_years_post_uvv < 0, ]
post_vax <- with_vax[num_years_post_uvv >= 0, ]
  
# Keep only non draw column in the post vaccine years so that when merged with mean_inc at year 0 will apply same draws to all years
post_vax <- dplyr::select(post_vax, "location_id", "year_id", "sex_id", "age_group_id", "num_years_post_uvv")
  
# merge post vax with mean year inc zero full merge but excluding year column 
test <- merge(post_vax, mean_inc_year_0, by = c("location_id", "sex_id", "age_group_id"), all.x = T)
  
# bind post vax years with prevax years
capped_post_vax <- rbind(pre_vax, test) 
  
  # Check data
  if(sum(is.na(capped_post_vax)) > 0) {
    message("STOP! you have missing data!")
  } else {
    print("Continue, there are no missing data in capped post vaccination dataframe")
  }
  
# Merge vaccine locations with vaccine efficacy prediction frame
all_vax_merged <- merge(capped_post_vax, prediction_frame, by = "num_years_post_uvv", all.x = T)

# Create inverse efficacy variable for calculation
all_vax_merged[, inv_efficacy := 1 - predicted_efficacy]

# Fill in prevaccine years inverse efficacy = 1 includin year of vaccine implementation!
# NB this line assumes that efficacy is 0 during year of introduction! 
all_vax_merged[, inv_efficacy := ifelse(is.na(inv_efficacy), 1, inv_efficacy)]
  
#  Check data for missingness
if(sum(is.na(all_vax_merged$inv_efficacy)) > 0) {
  message("STOP! you have missing data!")
} else {
  print("Continue there are no missing data in vaccine effectiveness dataframe")
}

# Calculate post vaccine incidence
adjusted_draws <- paste0("adj_draw_", draw_nums_int)
  
draw_cols <- grep("draw", colnames(all_vax_merged))
predictions_inc_save_vax <- all_vax_merged[, (adjusted_draws) := lapply(.SD, function(x) x * inv_efficacy), .SDcols = draw_cols]
  
# Save file containing both adjusted and unadjusted draws
fwrite(predictions_inc_save_vax, paste0(j.version.dir, "/03c_adjusted_and_unadjusted_incidence_draws.csv"))
print("Adjusted and unadjusted incidence for vaccine locations saved")

#*******************************************************************************
# 5. GET DATA FROM NON-VACCINE AND DEIMPLEMENTATION LOCATIONS ####
#*******************************************************************************
  
var_nf   <- get_draws(gbd_id_type="modelable_entity_id", 
                        gbd_id=1439, 
                        # status="best", 
                        version_id = dismod_seroprevalence_version, 
                        location_id = locs_no_vax, 
                        release_id = release_id,
                        source="epi", 
                        measure_id=c(5, 6))
message("Varicella draws loaded for non vaccine locations")

# Divide data into separate incidence and prevelance draws
var_inc  <- var_nf[measure_id==6, ]
var_prev <- var_nf[measure_id==5, ]
  
# Save model version
cat(paste0("Varicella DisMod model (me_id 1439) for prevalence and incidence - model version ", unique(var_prev$model_version_id)), 
      file=file.path(j.version.dir.logs, "input_model_version_ids.txt"), sep="\n", append=TRUE)
  
# Prep prevalence draws for incidence rate calculation
var_prev2 <- var_prev[, draw_cols_upload, with=FALSE]
colnames(var_prev2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("prev_draw_", draw_nums_gbd))
  
# Prep incidence draws for incidence rate calculation
var_inc2 <- var_inc[, draw_cols_upload, with=FALSE]
colnames(var_inc2) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("hazard_draw_", draw_nums_gbd))
  
# Merge incidence and prevalence draws
var_nonfatal <- merge(var_prev2, var_inc2, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  
# Clear memory
rm(var_nf)
gc(T)
  
# Calculate incidence from hazards
var_nonfatal[, (draw_cols_gbd) := lapply(draw_nums_gbd, function(ii) get(paste0("hazard_draw_", ii)) * ( 1 - get(paste0("prev_draw_", ii)) ) )]
predictions_inc_save_non_vax <- subset(var_nonfatal, select=draw_cols_upload)
  
#*******************************************************************************
# 6. RUN DIAGNOSTIC PLOTS ON MODIFIED LOCATIONS ####
#*******************************************************************************
  
# Copy vaccine locations for diagnostic plots
diagnostics <- copy(predictions_inc_save_vax)
  
# Get locations data for diagnostic plotting
locations_plot <- get_location_metadata(location_set_id = 22, 
                                        release_id = release_id)

# Get age metadata
ages <- get_age_metadata(release_id = release_id)
ages <- ages[, .(age_group_id, age_group_name, age_group_years_start, age_group_years_end)]
setorder(ages, "age_group_years_start")  

# Get the population estimates for plotting locations
population_plot <- get_population(release_id = release_id, 
                                  location_id = locs_vax,
                                  age_group_id = unique(diagnostics$age_group_id), 
                                  sex_id = c(1:2), 
                                  year_id = unique(diagnostics$year_id))

# Merge predictions with population to calculate incidence
for_plot <- merge(diagnostics, population_plot, by = c("location_id", "year_id", 
                                                       "sex_id", "age_group_id"), all.x = T)

# Rename columns
unadjusted_inc <- paste0("inc_draw_", draw_nums_int)
adjusted_inc <- paste0("adj_inc_draw_", draw_nums_int)

# Mutltiply by population
for_plot[, (unadjusted_inc) := lapply(.SD, function(x) x * population), .SDcols = draw_cols_gbd]
for_plot[, (adjusted_inc) := lapply(.SD, function(x) x * population), .SDcols = adjusted_draws]

# merge with ages
for_plot <- merge(for_plot, ages, by = "age_group_id", all.x = T)

# Create sex variable
for_plot[, sex := ifelse(sex_id == 2, "Female", "Male")]

# Keep national locations only 
national <- for_plot[level == 3, ]
setorder(national, location_name)

# Time line plots for single draw [in future calculate a mean of draws]
pdf(paste0(j.version.dir.logs, "/vaccine_efficacy_incidence_plots.pdf"), width = 11.5, height = 8, onefile = T)
for (loc in unique(national$location_name)){
    d <- national[location_name == loc & age_group_years_start <10, ]
    message(paste0("Printing plot for ", loc))
    p <- ggplot(data = d) +
      geom_point(aes(x = year_id, y = inc_draw_0, color = "Unadjusted incidence"), size = 2)+
      geom_point(aes(x = year_id, y = adj_inc_draw_0, color = "Adjusted incidence"), size = 1)+
      scale_color_manual(values = c("Unadjusted incidence" ="magenta", 
                                    "Adjusted incidence" ="darkblue"))+
      facet_grid(sex ~ reorder(age_group_name, age_group_years_start))+
      geom_vline(xintercept = unique(d$vax_intro_year), color = "red", linewidth = 0.25)+
      # scale_shape_manual(values = c(1, 19))+
      labs(title = paste0("Varicella incidence with and without vaccination by age and sex for location: ", loc), 
           x = "Year", 
           y = "Number of cases", 
           color = "", 
           shape = "Sex")+
      theme_bw()
    print(p)
}
dev.off()
  
#*******************************************************************************
# 7. COMBINE ALL LOCATION INCIDENCE DATA AND CALCUALTE PREVALENCE ####
#*******************************************************************************

# Keep only adjusted draws for upload
predictions_inc_save_vax <- dplyr::select(predictions_inc_save_vax, "location_id", "year_id", "age_group_id", "sex_id", contains("adj"))

# Rename columns
colnames(predictions_inc_save_vax) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", draw_nums_gbd))

# Keep only modelled years for vaccine locations
predictions_inc_save_vax <- predictions_inc_save_vax[year_id %in% unique(predictions_inc_save_non_vax$year_id), ]

# Remove data in before introduction and after deimplemented location years
temp_vax <- predictions_inc_save_vax[!(location_id %in% stop_loc_ids & year_id < stop_loc_years[[2]]) &
                                     !(location_id %in% stop_loc_ids & year_id > stop_loc_years[[3]])]

# Remove vaccination years from the non vaccine data base for deimplementation locations
temp_non_vax <- predictions_inc_save_non_vax[!(location_id %in% stop_loc_ids & year_id >= stop_loc_years[[2]]) |
                                               !(location_id %in% stop_loc_ids & year_id <= stop_loc_years[[3]])]


# Combine vaccine and non vaccine locations
predictions_prepped <- rbind(temp_vax, temp_non_vax)

message("Vaccine and non-vaccine location incidence estimates have been combined")

# Return data 
return(predictions_prepped)

}

