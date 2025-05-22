# gets global age pattern from clinical data

get_clinical_age_pattern <- function(gbd_round_id, cause, bundle_version_id = NULL, crosswalk_version_id = NULL, aggregate = T) {
  # pull crosswalk version
  if (!is.null(bundle_version_id) & !is.null(crosswalk_version_id)) print ("use only bundle version OR crosswalk version ID")
  if (!is.null(crosswalk_version_id)){
    dt <- get_crosswalk_version(crosswalk_version_id) # 34376
  } else if (!is.null(bundle_version_id)){
    dt <- get_bundle_version(bundle_version_id, fetch = "all") # 34661
  }
  
  dt <- dt[!is.na(clinical_data_type) & clinical_data_type!=""] # subset to clinical only
  
  # check for presence of sample_size and cases everywhere
  if (any(is.na(dt$cases)) | any(is.na(dt$sample_size))){
    print ("cases and sample size are not present everywhere, filling with population")
    # get population
    pop <- get_population(location_id = unique(dt$location_id),
                          year_id = "all",
                          sex_id = c(1,2,3),
                          gbd_round_id = 7,
                          age_group_id = "all",
                          decomp_step = "iterative", 
                          status = "best")
    # convert clinical data to GBD age groups (easy since already in GBD age groups)
    ages <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
    # merge on age start, round so the merge works
    ages$age_group_years_start <- round(ages$age_group_years_start, 2)
    dt$age_start <- round(dt$age_start, 2)
    dt <- merge(dt, ages[,.(age_group_years_start, age_group_years_end, age_group_id)], 
                by.x = c("age_start"), by.y = c("age_group_years_start"))
    # merge with population
    dt[sex == "Male", sex_id := 1]
    dt[sex == "Female", sex_id := 2]
    dt[, year_id := floor((year_start+year_end)/2)]
    dt <- merge(dt, pop, by = c("location_id", "sex_id", "age_group_id", "year_id"))
    # fill sample size with population
    dt[is.na(sample_size), sample_size := population]
    dt[is.na(cases), cases := mean*sample_size]
    # now we have cases and sample size by age group
    
  } else print ("cases and sample size present everywhere!")
  
  # now aggregate by sex, year, and location to grab the global case pattern 
  overall_men <- copy(dt)
  
  if (aggregate){
    # aggregate the most granular age groups to the over 80 (100) age grps using dcast
    over80_ages <- ages[age_group_years_start >= 80]$age_group_id
    drawnames <- paste0("draw_" ,0:999)
    over_80 <- data.table::dcast(overall_men[age_group_id %in% over80_ages], measure ~ .,
                                 value.var = c("cases", "sample_size"), fun.aggregate = sum)
    over_80$age_group_id <- 21
  
    overall_men <- overall_men[!age_group_id %in% c(over80_ages)]
    # sum cases/ss by sex, year, location:
    overall_men <- data.table::dcast(overall_men, age_group_id + measure ~ .,
                                     value.var = c("cases", "sample_size"), fun.aggregate = sum)
    overall_men <- rbind(
      overall_men, over_80, fill = T)
  }
  overall_men <- overall_men[ ,.(age_group_id,cases)]
  return(overall_men)
  
}