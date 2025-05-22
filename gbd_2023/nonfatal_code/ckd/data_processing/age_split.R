##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Age-splitting CKD data, using pydisagg package developed by MSCA
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

library(data.table)
library(dplyr)
library(knitr)
library(kableExtra)
library(purrr)
library(ggplot2)

# Pull in bundle version and crosswalk version id map (must manually update ids first!)
process_cv_ref_map_path <- paste0(j_root,"FILEPATH/process_cv_reference_map.csv")
bvids<-fread(process_cv_ref_map_path)
cvids <- bvids

release_id <- 16

# to save (1) or not (0) a fully processed and age split crosswalk version
save_cx_post_age_split <- 1

# Set bundles to run for
# ckd_bids<-c(670, 182, 183, 186, 760, 185, 184, 551, 3110, 3113, 552, 553, 554)
ckd_bids<-c(184, 185)

for (bid in ckd_bids){
  
  # SET OBJECTS -------------------------------------------------------------
  # set objects
  bvid<-bvids[bundle_id==bid, bundle_version_id]
  cvid<-cvids[bundle_id==bid, cvid_not_age_split]
  me_id<-bvids[bundle_id==bid, me_id]
  age_pattern_dismod_id<-bvids[bundle_id==bid, age_pattern_dismod_id] # this needs to be manually entered in csv
  dialysis_population_dismod_id<-bvids[bundle_id==bid, dialysis_population_dismod_id] # this needs to be manually entered in csv, only for etiology bundles
  acause<-bvids[bundle_id==bid, acause]
  output_file_name<-paste0("post_age_split_fully_processed_bid_", bid, "_bvid_",bvid)
  
  location_set_id <- 9
  age_group_set_id <- 32
  
  year_id <- 2010 # year id you want the age pattern from
  
  draws <- paste0("draw_", 0:999)
  
  message(paste("Bundle version id is", bvid))
  message(paste(bid, bvid, cvid, acause, age_pattern_dismod_id, release_id, output_file_name, save_cx_post_age_split))
  
  output_path <- paste0(j_root,"FILEPATH")
  
  # dt for tracking # datapoints involved in each processing step
  tracking_dt <- data.table()
  tracking_dt[, bundle_id := bid]
  tracking_dt[, bundle_version_id := bvid]
  
  # SOURCE FUNCTIONS ------------------------------------------------
  invisible(sapply(list.files("FILEPATH", full.names = T), source))
  
  library(reticulate)
  reticulate::use_python("FILEPATH/python")
  splitter <- import("pydisagg.ihme.splitter")
  
  # GET DATA, PATTERN, POPULATION ----------------------------------------------------------------
  # pulling crosswalk to age split and subsetting to data that needs age splitting
  message(paste0("getting crosswalk version ", cvid, " for bid ", bid))
  dt <- as.data.table(get_crosswalk_version(crosswalk_version_id = cvid, export = FALSE)) 
  
  dt_tosplit <- dt[((age_end-age_start)>25) & !(nid %in% c(286858, 286857)) &
                     measure %in% c("incidence", "prevalence", "proportion") & 
                     !(mean == 0) & !(mean == 1 & measure %in% c("prevalence", "proportion")), ] 
  
  dt_nosplit <- setdiff(dt, dt_tosplit)
  
  ## calculate needed columns & subset
  if (bid %in% c(551, 3110, 3113, 552, 553, 554)) { # for etiology proportions, use dialysis dismod model as population which only has a subset of years available
    dt_tosplit[, year_id := floor((year_start+year_end)/2)]
    dt_tosplit[, year_id := round(year_id/5)*5]
    dt_tosplit[year_id < 1990, year_id := 1990]
    dt_tosplit[year_id > 2020, year_id := 2020]
  } else {
    dt_tosplit[, year_id := floor((year_start+year_end)/2)]
  }
  
  dt_tosplit[sex == "Male", sex_id := 1]
  dt_tosplit[sex == "Female", sex_id := 2]
  
  dt_tosplit_subset <- subset(dt_tosplit, select = c(nid, seq, sex_id, age_start, age_end, location_id,
                                                     year_id, measure, mean, standard_error))
  
  ## add number datapoints to be age split to tracking dt 
  tracking_dt[, age_split_input_datapoints := nrow(dt_tosplit)]
  
  
  # pulling age split dismod model for pattern, merge with metadata
  loc_dt <- as.data.table(get_location_metadata(location_set_id = location_set_id, release_id = release_id))
  
  measure_dt <- as.data.table(get_ids("measure"))
  measure_dt <- measure_dt[measure %in% dt_tosplit$measure, ]
  
  if (bid %in% c(551, 3110, 3113, 552, 553, 554)) { # for etiology proportions, use age pattern from High income super region because lots of data there, and age pattern in super regions without data loses its shape which affects global pattern
    pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = me_id, source = "epi",
                         measure_id = measure_dt$measure_id, location_id = 64, year_id = year_id,
                         sex_id = c(1,2), metric_id = 3, version_id = age_pattern_dismod_id,
                         release_id = release_id)
    
    loc_dt_merge <- subset(loc_dt, select = c(location_id))
    loc_dt_merge[, merge := 1]
    pattern[, merge := 1]
    pattern <- subset(pattern, select = -c(location_id))
    
    pattern <- merge(pattern, loc_dt_merge, by = "merge", allow.cartesian = TRUE)
    pattern <- subset(pattern, select = -c(merge))
  } else {
    pattern <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = me_id, source = "epi",
                         measure_id = measure_dt$measure_id, location_id = loc_dt[level == 1, ]$location_id, year_id = year_id,
                         sex_id = c(1,2), metric_id = 3, version_id = age_pattern_dismod_id,
                         release_id = release_id)
    
    loc_dt_merge <- subset(loc_dt, select = c(location_id, super_region_id))
    pattern <- rename(pattern, super_region_id = location_id)
    
    pattern <- merge(pattern, loc_dt_merge, by = "super_region_id", allow.cartesian = TRUE)
    pattern <- subset(pattern, select = -c(super_region_id))
  }
  
  age_dt <- get_age_metadata(age_group_set_id = age_group_set_id, release_id = release_id)
  age_dt <- subset(age_dt, select = c(age_group_years_start, age_group_years_end, age_group_id))
  
  pattern <- merge(pattern, age_dt, all.x = TRUE)
  
  ## subset to needed columns
  pattern <- subset(pattern, select = -c(metric_id, model_version_id, modelable_entity_id))
  
  ## removing rows where all draws are 0 (pydisagg will error out) & fixing age_start in dt to split
  pattern[, draw_mean := rowMeans(.SD), .SDcols = paste0("draw_", 0:999)]
  pattern <- pattern[!draw_mean == 0, ]
  pattern <- subset(pattern, select = -c(draw_mean))
  
  pattern_min_age <- min(pattern$age_group_years_start)
  
  dt_tosplit_subset[age_start < pattern_min_age, age_start := pattern_min_age]
  
  
  # pulling population
  # general population except for etiology bundles (use dialysis model for population since ESRD is denominator)
  if (bid %in% c(551, 3110, 3113, 552, 553, 554)) {
    pop_dialysis <- get_model_results(gbd_team = 'epi', gbd_id = 2021, measure_id = 5, sex_id = c(1,2),
                                      release_id = release_id, age_group_id = unique(pattern$age_group_id),
                                      model_version_id = dialysis_population_dismod_id)
    
    pop_count <- get_population(location_id = 'all', year_id = 'all', sex_id = c(1,2), 
                                release_id = release_id, age_group_id = unique(pattern$age_group_id))
    
    pop <- as.data.table(merge(pop_dialysis, pop_count, by = c("year_id", "sex_id", "location_id", "age_group_id")))
    pop[, population := mean*population]
    pop <- subset(pop, select = c(population, year_id, sex_id, location_id, age_group_id))
    
  } else {
    pop <- get_population(location_id = 'all', year_id = 'all', sex_id = c(1,2), 
                          release_id = release_id, age_group_id = unique(pattern$age_group_id))
    pop <- subset(pop, select = -c(run_id))
  }
  
  
  # PYDISAGG ----------------------------------------------------------------
  data_config <- splitter$AgeDataConfig(
    index=c("nid","seq", "location_id", "year_id", "sex_id"),
    age_lwr="age_start",
    age_upr="age_end",
    val="mean",
    val_sd="standard_error")
  
  draw_cols <-grep("draw_",names(pattern), value = TRUE)
  
  pattern_config <- splitter$AgePatternConfig(
    by=list("sex_id", "location_id"),
    age_key="age_group_id",
    age_lwr="age_group_years_start",
    age_upr="age_group_years_end",
    draws=draw_cols)
  
  pop_config <- splitter$AgePopulationConfig(
    index=c("age_group_id", "location_id", "year_id", "sex_id"),
    val="population")
  
  age_splitter <- splitter$AgeSplitter(
    data=data_config, 
    pattern=pattern_config, 
    population=pop_config)
  
  age_split_output <- data.table()
  
  if ("incidence" %in% dt_tosplit$measure){
    dt_tosplit_subset2 <- dt_tosplit_subset[measure == "incidence", ]
    pattern2 <- pattern[measure_id == 6, ]
    
    result <- age_splitter$split(
      data=dt_tosplit_subset2,
      pattern=pattern2,
      population=pop,
      model="rate",  
      output_type="rate")
    
    result <- as.data.table(result)
    result <- result[, measure := "incidence"]
    
    age_split_output <- rbind(age_split_output, result)
  }
  
  if ("prevalence" %in% dt_tosplit$measure){
    dt_tosplit_subset2 <- dt_tosplit_subset[measure == "prevalence", ]
    pattern2 <- pattern[measure_id == 5, ]
    
    result <- age_splitter$split(
      data=dt_tosplit_subset2,
      pattern=pattern2,
      population=pop,
      model="logodds",  
      output_type="rate")
    
    result <- as.data.table(result)
    result <- result[, measure := "prevalence"]
    
    age_split_output <- rbind(age_split_output, result)
  }
  
  if ("proportion" %in% dt_tosplit$measure){
    dt_tosplit_subset2 <- dt_tosplit_subset[measure == "proportion", ]
    pattern2 <- pattern[measure_id == 18, ]
    
    result <- age_splitter$split(
      data=dt_tosplit_subset2,
      pattern=pattern2,
      population=pop,
      model="logodds",  
      output_type="rate")
    
    result <- as.data.table(result)
    result <- result[, measure := "proportion"]
    
    age_split_output <- rbind(age_split_output, result)
  }
  
  ## add number datapoints post age split to tracking dt 
  tracking_dt[, age_split_output_datapoints := nrow(age_split_output)]
  
  
  # CLEAN UP / COMBINING ----------------------------------------------------------------
  write.csv(age_split_output, paste0(output_path, "FILEPATH/bundle_", bid, "_age_split.csv"))
  
  age_split_output[, split_cases := age_split_result*pop_population_aligned]
  age_split_output[, age_start := pat_age_group_years_start]
  age_split_output[, age_end := pat_age_group_years_end]
  age_split_output[, mean := age_split_result]
  age_split_output[, standard_error := age_split_result_se]
  
  age_split_output2 <- subset(age_split_output, select = c(seq, age_start, age_end,
                                                           mean, standard_error))
  
  dt_tosplit2 <- subset(dt_tosplit, select = -c(year_id, sex_id, age_start, age_end, mean, standard_error,
                                                lower, upper, cases, sample_size, uncertainty_type_value, variance, effective_sample_size,
                                                design_effect))
  
  dt_postsplit <- merge(dt_tosplit2, age_split_output2, by= "seq")
  
  dt_new <- rbind(dt_nosplit, dt_postsplit, fill = TRUE)
  
  
  # VETTING ----------------------------------------------------------------
  ## over age, facet pre and post age split for each super region
  age_split_output[, type := "Post split"]
  dt_tosplit_subset[, type := "Pre split"]
  
  age_split_plot <- rbind(age_split_output, dt_tosplit_subset, fill = TRUE)
  
  loc_dt_merge <- subset(loc_dt, select = c(location_id, super_region_name))
  age_split_plot <- merge(age_split_plot, loc_dt_merge, by = "location_id")
  
  for (i in unique(age_split_plot$measure)) {
    age_split_plot2 <- age_split_plot[measure == i, ]
    pdf(paste0(output_path,"FILEPATH/bundle_",bid, "_over_age_pre_post_split_", i, ".pdf"), width = 18, height = 8.5)
    for (x in unique(age_split_plot2$super_region_name)) {
      age_split_plot3 <- age_split_plot2[super_region_name == x, ]
      age_split_plot3[sex_id == 1, sex := "Male"]
      age_split_plot3[sex_id == 2, sex := "Female"]
      age_split_plot3[, age_midpoint := ((age_start+age_end)/2)]
      p <- ggplot(age_split_plot3) +
        geom_segment(aes(x = age_start, xend = age_end, y = mean, yend = mean), alpha = 0.5) +
        facet_wrap(~ sex + type)+
        labs(x="Age",
             y="Mean",
             title=paste0("Pre- and post-age split means over age: ", x, ", ", i),
             caption=paste0("Bundle ID: ", bid, ". Bundle Version: ", bvid)) +
        theme_light() + 
        theme(strip.text = element_text(size = 10))
      print(p)
    }
    dev.off()
  }
  
  
  # OUTLIERS ----------------------------------------------------------------
  dt_new[nid == 129262, is_outlier := 1]
  dt_new[urbanicity_type == "Rural" & nid == 279690, is_outlier := 1]
  if (bid == 186) {
    dt_new[nid == 269869, is_outlier := 1]
    dt_new[nid == 214658, is_outlier := 1]
    dt_new[nid == 129258, is_outlier := 1]
  }
  
  if (bid == 670) {
    dt_new[location_id == 142 & year_start == 2007, is_outlier := 1] 
  } else if (bid == 182 | bid == 183 | bid == 186 | bid == 760) {
    dt_new[location_id == 214 & year_start == 2007, is_outlier := 1]  
    dt_new[location_id == 44947 & year_start == 2015, is_outlier := 1]  
    dt_new[location_id == 25350 & year_start == 2009, is_outlier := 1]  
    dt_new[location_id == 25347 & year_start == 2008, is_outlier := 1]  
  }
  if (bid == 183 | bid == 186) {
    dt_new[location_id == 20 & year_start == 2006, is_outlier := 1]  
    dt_new[location_id == 171, is_outlier := 1]  
    dt_new[nid == 133397, is_outlier := 1]  
    dt_new[nid == 269849, is_outlier := 1]  
    dt_new[nid == 276450, is_outlier := 1]  
    dt_new[nid == 279526, is_outlier := 1]  
    dt_new[nid == 279527, is_outlier := 1]  
    dt_new[nid == 280192, is_outlier := 1]  
    dt_new[nid == 282076, is_outlier := 1]  
    dt_new[nid == 293226, is_outlier := 1]  
    dt_new[nid == 327959, is_outlier := 1]  
    dt_new[nid == 432240, is_outlier := 1]  
  }
  
  if (bid == 760) {
    dt_new[nid == 279690, is_outlier := 1]  
  }
  
  if (bid %in% c(551, 3110, 3113, 552, 553, 554)) { 
    dt_new[mean == 1, is_outlier := 1]
  }
  
  if (bid %in% c(670, 182, 183, 186, 760)) { 
    dt_new[nid == 125708, is_outlier := 1]
    dt_new[nid == 515017, is_outlier := 1]
    dt_new[nid == 126044, is_outlier := 1]
    dt_new[nid == 515607, is_outlier := 1]
    dt_new[nid == 328789, is_outlier := 1] 
    dt_new[nid == 269895, is_outlier := 1] 
    dt_new[nid == 411918, is_outlier := 1] 
  }
  
  if (bid == 185) {
    loc_oceania <- loc_dt[region_id == 21,]
    dt_new[location_id %in% loc_oceania$location_id & nid %in% c(492957, 492958), is_outlier := 1] 
  }
  
  
  # SAVING CROSSWALK ----------------------------------------------------------------
  dt_new[, seq := NA]
  
  dt_new[nchar(note_sr) > 2000, note_sr := strtrim(note_sr, 1999)]
  
  # write all data
  message("writing full processed dataset after age splitting")
  write.csv(dt_new, paste0(output_path,"FILEPATH/", output_file_name, ".csv"))
  
  # save full crosswalk version
  if(save_cx_post_age_split==1) {
    writexl::write_xlsx(list(extraction=dt_new),paste0("FILEPATH/", bid, "FILEPATH/",output_file_name, ".xlsx"))
    description <- "Fully processed"
    result <- save_crosswalk_version(bvid,data_filepath = paste0("FILEPATH/", bid, "FILEPATH/",output_file_name, ".xlsx"),
                                     description = description)
    
    bvids<-fread(process_cv_ref_map_path)
    if (bid %in% c(182, 183, 185)) {
      bvids$cvid_pre_remission[bvids$bundle_id==bid] <- result$crosswalk_version_id
    } else {
      bvids$cvid_final[bvids$bundle_id==bid] <- result$crosswalk_version_id
    }
    fwrite(bvids, file = process_cv_ref_map_path, append = FALSE)
  }
  
  # save tracking dt
  tracking_dt_all<-as.data.table(read.csv(paste0(output_path, "data_processing_tracking.csv")))
  tracking_dt_all2 <- tracking_dt_all[bundle_id == bid, ]
  if ("age_split_input_datapoints" %in% colnames(tracking_dt_all)) {
    tracking_dt_all2 <- subset(tracking_dt_all2, select = -c(age_split_input_datapoints, age_split_output_datapoints))
  }
  tracking_dt_all2 <- merge(tracking_dt_all2, tracking_dt, by = c("bundle_id", "bundle_version_id"), all.x = TRUE)
  tracking_dt_all <- tracking_dt_all[!bundle_id == bid, ]
  tracking_dt_all <- rbind(tracking_dt_all, tracking_dt_all2, fill = TRUE)
  fwrite(tracking_dt_all, file = paste0(output_path, "data_processing_tracking.csv"), append = FALSE)
  
}