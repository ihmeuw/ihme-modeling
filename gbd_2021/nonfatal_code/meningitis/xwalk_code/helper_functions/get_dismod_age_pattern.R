# gets global age pattern from last year's Dismod model 
# OR from a specific Dismod model version

get_dismod_age_pattern <- function(gbd_round_id, cause, model_version_id = NULL) {
  # if model version is null, pull from last year's final outputs
  if (is.null(model_version_id)){
    # get all age groups for gbd 2019
    gbd_2019_ages <- get_age_metadata(12, gbd_round_id = gbd_round_id-1)
    over80 <- gbd_2019_ages[age_group_years_start>=80]$age_group_id
    # replace all under1 sub groups with the aggregate under1 group
    gbd_2019_ages <- c(28, gbd_2019_ages$age_group_id[4:length(gbd_2019_ages$age_group_id)])
    
    age_pattern <- get_outputs(topic="cause", # risks, etiologies, and impairments
                               cause_id = cause, # meningitis
                               location_id=1,
                               age_group_id=gbd_2019_ages, 
                               sex_id=3, 
                               measure_id=6, # incident cases 
                               metric_id=1, # number
                               year_id=2010, 
                               gbd_round_id=gbd_round_id-1, 
                               decomp_step = "step5",
                               compare_version_id = 7244)
    
    # set age group id 28 to 0
    age_pattern[age_group_id == 28, age_group_id := 0]
    setnames(age_pattern, "val", "cases")
    age_pattern <- age_pattern[, c("age_group_id", "cases"), with = F]
    setorder(age_pattern, age_group_id)
    # 
    # set old age groups to 100
    old_sum <- sum(age_pattern[age_group_id %in% over80]$cases)
    age_pattern <- age_pattern[! age_group_id %in% over80]
    old_row <- data.table(age_group_id = 100, cases = old_sum)
    age_pattern <- rbind(age_pattern, old_row)
    
    return(age_pattern)
  } else {
    # use the model version ID provided to pull from a given Dismod model version
    # get age pattern for overall meningitis
    draws <- paste0("draw_", 0:999)
    overall_men <- get_draws(source = "epi",
                             gbd_id_type = "modelable_entity_id",
                             gbd_id = 1296,
                             version_id = model_version_id,
                             measure_id = 6,
                             decomp_step = ds,
                             sex_id = 3,
                             year_id = 2010,
                             # age_group_id = "all",
                             location_id = 1, 
                             gbd_round_id = gbd_round_id
    )
    
    pop <- get_population(location_id = 1,
                          year_id = 2010,
                          sex_id = 3,
                          gbd_round_id = 7,
                          age_group_id = "all",
                          decomp_step = "iterative", 
                          status = "best")
    
    pop <- pop[,.(sex_id, age_group_id, location_id, population)]
    overall_men <- merge(overall_men, pop, by = c("age_group_id", "location_id", "sex_id"), all.x = T)

    overall_men[, se_dismod := apply(.SD, 1, sd), .SDcols = draws]
    overall_men[, rate_dis := rowMeans(.SD), .SDcols = draws]
    overall_men[, (draws) := NULL]

    ## CASES AND SAMPLE SIZE
    overall_men[, cases := population * rate_dis]
    
    # aggregate the most granular age groups to the Under 1 (0), 1-5 (5) and over 80 (100) age grps using dcast
    ages <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
    # u1_ages <- ages[age_group_years_start < 1]$age_group_id
    # ages_1_5 <- ages[age_group_years_start < 5 & age_group_years_start >= 1]$age_group_id
    over80_ages <- ages[age_group_years_start >= 80]$age_group_id
    drawnames <- paste0("draw_" ,0:999)
    # under_1 <- data.table::dcast(overall_men[age_group_id %in% u1_ages], year_id + location_id + sex_id + measure_id + metric_id ~ .,
    #                              value.var = c("cases", "population"), fun.aggregate = sum)
    # dt_1_5  <- data.table::dcast(overall_men[age_group_id %in% ages_1_5], year_id + location_id + sex_id + measure_id + metric_id ~ .,
    #                              value.var = c("cases", "population"), fun.aggregate = sum)
    over_80 <- data.table::dcast(overall_men[age_group_id %in% over80_ages], year_id + location_id + sex_id + measure_id + metric_id ~ .,
                                 value.var = c("cases", "population"), fun.aggregate = sum)
    # under_1$age_group_id <- 0
    # dt_1_5$age_group_id <- 5
    over_80$age_group_id <- 21

    # copy these new aggregated rows to the original data table
    # overall_men <- overall_men[!age_group_id %in% c(over80_ages, ages_1_5, u1_ages)]
    overall_men <- overall_men[!age_group_id %in% c(over80_ages)]
    overall_men <- rbind(
      # under_1, dt_1_5, 
      overall_men, over_80, fill = T)
    
    overall_men <- overall_men[ ,.(age_group_id,cases)]
    return(overall_men)
  }
  
}