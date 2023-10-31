
# KEEP - NON-PARALLEL for troubleshooting locations

#' Time-correct booster delivery based on real data
#' 
#' Find a single date per booster course (as recent as possible) for which we
#' have data for some minimum number of locations.  Adjust each supply-model
#' delivery scheduele to that date in time, replace booster course deliver,
#' rewrite files, and plot daily vaccine delivery by vaccine and booster course.
#' 
#' Search for "KEEP - NON-PARALLEL for troubleshooting locations" to run the
#' script linear (which may help troubleshoot specific locations, which is
#' difficult in parallel).  You'll also need to comment OUT any parallel
#' operations, which are bookmarked in RStudio's Outline (Ctrl + Shift + O)
#'
#' @param vaccine_output_root # versioned vaccine results output folder
#' @param n_cores # how many cores for parallel processing
.booster_correction <- function(vaccine_output_root, n_cores) {
  
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  scenario <- model_parameters$survey_yes_responses
  scenario_reference <- model_parameters$survey_yes_responses_reference
  
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  hierarchy <- as.data.frame(hierarchy)
  
  if (scenario == scenario_reference) {
    NO_PROB <- FALSE
    reference_name <- "reference"
  } else {
    NO_PROB <- TRUE
    reference_name <- "no_prob"
  }
  
  message(glue("### Running Booster Correction with scenario = {scenario} : Loading data"))
  booster_data <- fread(file.path(vaccine_output_root, "boosters_point_estimate.csv")) # observed boosters on one common date
  booster_data[, date := as.Date(as.character(date))]
  booster_data <- as.data.frame(booster_data)
  
  reference <- as.data.frame(fread(glue("{vaccine_output_root}/uncorrected_last_shots_in_arm_by_brand_w_booster_{reference_name}.csv")))
  reference$date <- as.Date(as.character(reference$date))
  optimal <- as.data.frame(fread(glue("{vaccine_output_root}/uncorrected_last_shots_in_arm_by_brand_w_booster_optimal.csv")))
  optimal$date <- as.Date(as.character(optimal$date))
  
  ULoc_ID <- unique(reference$location_id)
  tmptext <- "booster"
  if (NO_PROB){
    tmptext <- "booster_hesitancy"
  }
  
  # Define and validate booster & vaccine courses 
  BOOSTER_COURSES <- model_parameters$booster_courses
  booster_course_names <- unique(booster_data$booster_course)
  
  if (BOOSTER_COURSES != length(booster_course_names)) {
    stop("✖ booster_correction.R : number of booster courses in model parameters does not match courses in booster_point_est.")
  }
  
  if( !all(1:model_parameters$vaccine_courses %in% unique(reference$vaccine_course)) |
      !all(1:model_parameters$vaccine_courses %in% unique(optimal$vaccine_course)) ) {
    stop("✖ booster_correction.R : Not all required vaccine courses are in uncorrected last shots in arms data")
  }
  
  reference_fixed <- data.frame(reference)
  optimal_fixed <- data.frame(optimal)
  point_est <- data.frame(booster_data)
  # point_fixed <- # KEEP - NON-PARALLEL for troubleshooting locations
  #   data.frame(
  #     location_id = integer(),
  #     vaccine_course = integer(),
  #     match_val = numeric(),
  #     match_dat = as.Date(character()),
  #     fix_dat = as.Date(character()),
  #     fix_dat_2 = as.Date(character()),
  #     obs_dat = as.Date(character())
  #   )
  
  result_list <- vector("list", length = BOOSTER_COURSES)
  # List levels for parallel results
  # 1. booster courses
  # 2. 1 = reference, 2 = optimal, 3 = points
  # 3. locations
  
  # LOOP: COURSES ----
  for (B_COURSE in 1:BOOSTER_COURSES) { # Be careful, booster courses =/= vaccine courses
    
    message(glue("Starting booster_{B_COURSE} correction"))
    V_COURSE <- B_COURSE + 1 # Define vaccine courses relative to boosters
    B_VARNAME <- paste0("booster_", B_COURSE)
    B_DATE <- unique(booster_data[booster_data$booster_course == B_VARNAME, "date"])
    
    # KEEP - NON-PARALLEL for troubleshooting locations
    # pdf(file=glue("{vaccine_output_root}/{B_VARNAME}_corrected.pdf"), height = 8, width = 14)
    
    
    
    # LOOP: LOCATIONS -----
    
    # Parallel On ----
    cl <- parallel::makeCluster(n_cores)
    doParallel::registerDoParallel(cl)
    
    combine_results <- function(x, ...) {
      lapply(seq_along(x),
             function(i) c(x[[i]], lapply(list(...), function(y) y[[i]])))
    }
    
    result_list[[B_COURSE]] <- foreach(
      
      # List levels
      # 1. booster courses
      # 2. 1 = reference, 2 = optimal, 3 = points
      # 3. locations
      
      i = 1:length(ULoc_ID), 
      .packages = c("glue"), 
      .combine='combine_results', 
      .multicombine=TRUE,
      .init=list(list(), list(), list())
      
    ) %dopar% {
      
      # for (i in 1:length(ULoc_ID)) { # KEEP - NON-PARALLEL for troubleshooting locations
      
      # find indexes of first booster course (vaccine_course == V_COURSE) for each location
      ref_indexes <- which(reference$location_id == ULoc_ID[i] & reference$vaccine_course == V_COURSE)
      
      if (length(ref_indexes)){ # check for any data
        
        booster_data_index <- which(booster_data$location_id == ULoc_ID[i] &
                                      booster_data$booster_course == B_VARNAME)
        
        if (length(booster_data_index)){ 
          
          # KEEP - NON-PARALLEL for troubleshooting locations
          # loc_name <- hierarchy[hierarchy$location_id == ULoc_ID[i],]$location_name
          # loc_text_name_id <- glue("{B_VARNAME} : {loc_name} ({ULoc_ID[i]})")
          # message(glue("{loc_text_name_id} i = {i} out of {length(ULoc_ID)}"))
          
          
          # REFERENCE SCENARIO ------------
          
          reference_i_all_rows <- reference[ref_indexes,]
          
          if (!is.na(reference_i_all_rows[1,"BNT.162"])){ # no corresponding 'else' - just a check for any data?
            
            ref_hr <- ref_agg <- reference_i_all_rows[which(reference_i_all_rows$risk_group == "hr"),]
            ref_lr <- reference_i_all_rows[which(reference_i_all_rows$risk_group == "lr"),]
            
            # Tally all boosters across brands
            # Transform to cumulative space
            # For all risk groups, then high, then low
            # create loess models
            ref_agg[,-c(1:4)] <- ref_hr[,-c(1:4)] + ref_lr[,-c(1:4)] # shot count columns
            ref_agg$daily_all_brand <- rowSums(ref_agg[,-c(1:4)])
            ref_agg$cumul_all_brand <- cumsum(ref_agg$daily_all_brand)
            ref_agg$date_numeric <- as.numeric(ref_agg$date) # temporal date (to count days)
            
            ref_hr$daily_all_brand <- rowSums(ref_hr[,-c(1:4)])
            ref_hr$cumul_all_brand <- cumsum(ref_hr$daily_all_brand)
            ref_hr$date_numeric <- as.numeric(ref_hr$date)
            
            ref_lr$daily_all_brand <- rowSums(ref_lr[,-c(1:4)])
            ref_lr$cumul_all_brand <- cumsum(ref_lr$daily_all_brand)
            ref_lr$date_numeric <- as.numeric(ref_lr$date)
            
            mod_agg_ref <- loess(cumul_all_brand ~ date_numeric, ref_agg, span = 0.05)
            mod_lr_ref <- loess(cumul_all_brand ~ date_numeric, ref_lr, span = 0.05)
            mod_hr_ref <- loess(cumul_all_brand ~ date_numeric, ref_hr, span = 0.05)
            
            
            # Get dates in 'days from 0' space for modeling
            ref_days_vec <- ref_agg$date_numeric - min(ref_agg$date_numeric)
            # loess curve in index rather than date space for scaling
            mod_agg_ref_idx <- predict(mod_agg_ref, data.frame(date_numeric = ref_days_vec + min(ref_agg$date_numeric))) 
            # point estimate booster_admin value to match
            MATCH_VAL <- booster_data$value[booster_data_index] 
            MATCH_DAT <- which(ref_agg$date_numeric == as.numeric(as.Date(B_DATE))) 
            # least positive, squared value between mod_agg_ref_idx (modeled) and MATCH_VAL (real data)
            OBS_DAT <- which.min((mod_agg_ref_idx - MATCH_VAL)^2) 
            # left/right time scale factor
            scale <- OBS_DAT/MATCH_DAT 
            # scaled dates as numeric
            ref_dates_numeric_scaled <- seq(min(ref_days_vec), max(ref_days_vec) * scale, length = length(ref_days_vec)) + min(ref_agg$date_numeric) 
            # cap time vector at last date
            ref_dates_numeric_scaled <- pmin(ref_dates_numeric_scaled, max(ref_days_vec) + min(ref_agg$date_numeric)) 
            
            # create loess curves scaled in time to the pinned day of data
            fix_lr <- predict(mod_lr_ref, data.frame(date_numeric = ref_dates_numeric_scaled)) 
            fix_hr <- predict(mod_hr_ref, data.frame(date_numeric = ref_dates_numeric_scaled))
            fix <- predict(mod_agg_ref, data.frame(date_numeric = ref_dates_numeric_scaled))
            # scrub negatives from predicted curve
            fix_lr <- pmax(fix_lr, 0)
            fix_hr <- pmax(fix_hr, 0)
            fix <- pmax(fix, 0)
            
            # which row index minimizes diff btwn scaled model and observed value
            FIX_DAT <- which.min((fix - MATCH_VAL)^2) 
            
            # create new empty matrix
            new_mat <- reference_i_all_rows
            new_mat[,-c(1:4)] <- 0
            
            # day = 1
            
            # LOOP: DATES FORWARD----
            for (day in 1:length(ref_agg$date)){
              # match reference dataframe day in lr group
              fix_row_idx <- which(reference_i_all_rows$date == round(ref_dates_numeric_scaled[day]) & reference_i_all_rows$risk_group == "lr")
              # sum of lr rows
              tmp_sum_lr <- sum(reference_i_all_rows[fix_row_idx,-c(1:4)])
              # new_mat[which(new_mat$date == ref_agg$date[day] & new_mat$risk_group == "lr"), c("BNT.162","Moderna")] <- 0.5 *  max(diff(fix_lr)[day - 1], 0)
              if (tmp_sum_lr > 0){
                # each brand's proportion of the daily 
                brand_props <- reference_i_all_rows[fix_row_idx,-c(1:4)] / sum(reference_i_all_rows[fix_row_idx,-c(1:4)])
                # INDIVIDUAL_NAME's new adjustment
                temp_shift <- ifelse(day == 1, fix_lr[day], max(diff(fix_lr)[day - 1], 0))
                # match new_mat day with aggregate data day, assign time-shifted lr value * brand proportions (or 0)
                new_mat[which(new_mat$date == ref_agg$date[day] & new_mat$risk_group == "lr"),-c(1:4)] <- brand_props *  temp_shift
              } else {
                # if there were no/negative vaccines, then you get Pfizer & Moderna?
                new_mat[which(new_mat$date == ref_agg$date[day] & new_mat$risk_group == "lr"), c("BNT.162","Moderna")] <- 0.5 *  max(diff(fix_lr)[day - 1], 0)
              }
              
              # same for hr group
              tmp_fix_loc_hr <- which(reference_i_all_rows$date == round(ref_dates_numeric_scaled[day]) & reference_i_all_rows$risk_group == "hr")
              tmp_sum_hr <- sum(reference_i_all_rows[tmp_fix_loc_hr,-c(1:4)])
              if (tmp_sum_hr > 0){
                brand_props <- reference_i_all_rows[tmp_fix_loc_hr,-c(1:4)] / sum(reference_i_all_rows[tmp_fix_loc_hr,-c(1:4)])
                temp_shift <- ifelse(day == 1, fix_hr[day], max(diff(fix_hr)[day - 1], 0)) # INDIVIDUAL_NAME's new adjustment
                new_mat[which(new_mat$date == ref_agg$date[day] & new_mat$risk_group == "hr"),-c(1:4)] <- brand_props *  temp_shift
              } else {
                new_mat[which(new_mat$date == ref_agg$date[day] & new_mat$risk_group == "hr"), c("BNT.162","Moderna")] <- 0.5 *  max(diff(fix_hr)[day - 1], 0)
              }
            }
            
            UDates <- sort(unique(new_mat$date)) # LOOP: THROUGH DATES REVERSED
            
            for (tmp_day in rev(UDates)){
              # index of data and lr group (one day, one row)
              
              tmp_lr_ref <- which(new_mat$date == tmp_day & new_mat$risk_group == "lr")
              # look if sum of all boosters is less than 0 
              if (sum(new_mat[tmp_lr_ref, -c(1:4)]) < 0){
                # find prior day index as one prior to current day
                previous <- which(new_mat$date == tmp_day - 1 & new_mat$risk_group == "lr")
                
                new_mat[previous, -c(1:4)] <- new_mat[previous, -c(1:4)] - new_mat[tmp_lr_ref, -c(1:4)]
                new_mat[tmp_lr_ref, -c(1:4)] <- 0
              }
              
              # repeat for hr group
              tmp_hr_ref <- which(new_mat$date == tmp_day & new_mat$risk_group == "hr")
              if (sum(new_mat[tmp_hr_ref, -c(1:4)]) < 0){
                previous <- which(new_mat$date == tmp_day - 1 & new_mat$risk_group == "hr")
                new_mat[previous, -c(1:4)] <- new_mat[previous, -c(1:4)] - new_mat[tmp_hr_ref, -c(1:4)]
                new_mat[tmp_hr_ref, -c(1:4)] <- 0
              }
            }
            
            new_hr <- new_agg <- new_mat[which(new_mat$risk_group == "hr"),]
            new_lr <- new_mat[which(new_mat$risk_group == "lr"),]
            new_agg[,-c(1:4)] <- new_hr[,-c(1:4)] + new_lr[,-c(1:4)]
            new_hr$daily_all_brand <- rowSums(new_hr[,-c(1:4)])
            new_hr$cumul_all_brand <- cumsum(new_hr$daily_all_brand)
            new_lr$daily_all_brand <- rowSums(new_lr[,-c(1:4)])
            new_lr$cumul_all_brand <- cumsum(new_lr$daily_all_brand)
            new_agg$daily_all_brand <- rowSums(new_agg[,-c(1:4)])
            new_agg$cumul_all_brand <- cumsum(new_agg$daily_all_brand)
            
            # reference_fixed[ref_indexes,] <- new_mat # KEEP - NON-PARALLEL for troubleshooting locations
            
            
            ### OPTIMAL SCENARIO ---------
            opt_indexes <- which(optimal$location_id == ULoc_ID[i] & optimal$vaccine_course == V_COURSE)
            optimal_i_all_rows <- optimal[opt_indexes,]
            opt_hr <- opt_agg <- optimal_i_all_rows[which(optimal_i_all_rows$risk_group == "hr"),]
            opt_lr <- optimal_i_all_rows[which(optimal_i_all_rows$risk_group == "lr"),]
            opt_agg[,-c(1:4)] <- opt_hr[,-c(1:4)] + opt_lr[,-c(1:4)]
            
            opt_agg$daily_all_brand <- rowSums(opt_agg[,-c(1:4)])
            opt_agg$cumul_all_brand <- cumsum(opt_agg$daily_all_brand)
            opt_agg$date_numeric <- as.numeric(opt_agg$date)
            
            opt_hr$daily_all_brand <- rowSums(opt_hr[,-c(1:4)])
            opt_hr$cumul_all_brand <- cumsum(opt_hr$daily_all_brand)
            opt_hr$date_numeric <- as.numeric(opt_hr$date)
            
            opt_lr$daily_all_brand <- rowSums(opt_lr[,-c(1:4)])
            opt_lr$cumul_all_brand <- cumsum(opt_lr$daily_all_brand)
            opt_lr$date_numeric <- as.numeric(opt_lr$date)
            
            
            mod_agg_2 <- loess(cumul_all_brand ~ date_numeric, opt_agg, span = 0.05)
            mod_lr_2 <- loess(cumul_all_brand ~ date_numeric, opt_lr, span = 0.05)
            mod_hr_2 <- loess(cumul_all_brand ~ date_numeric, opt_hr, span = 0.05)
            
            opt_days_vec <- opt_agg$date_numeric - min(opt_agg$date_numeric)
            obs <- predict(mod_agg_2, data.frame(date_numeric = opt_days_vec + min(opt_agg$date_numeric)))
            MATCH_VAL <- booster_data$value[booster_data_index]
            MATCH_DAT <- which(opt_agg$date_numeric == as.numeric(as.Date(B_DATE)))
            OBS_DAT <- which.min((obs - MATCH_VAL)^2)
            scale <- OBS_DAT/MATCH_DAT
            opt_dates_numeric_scaled <- seq(min(opt_days_vec), max(opt_days_vec) * scale, length = length(opt_days_vec)) + min(opt_agg$date_numeric)
            opt_dates_numeric_scaled <- pmin(opt_dates_numeric_scaled, max(opt_days_vec) + min(opt_agg$date_numeric))
            
            fix_lr_2 <- predict(mod_lr_2, data.frame(date_numeric = opt_dates_numeric_scaled))
            fix_hr_2 <- predict(mod_hr_2, data.frame(date_numeric = opt_dates_numeric_scaled))
            fix_2 <- predict(mod_agg_2, data.frame(date_numeric = opt_dates_numeric_scaled))
            # scrub negatives from predicted curve
            fix_lr_2 <- pmax(fix_lr_2, 0)
            fix_hr_2 <- pmax(fix_hr_2, 0)
            fix_2 <- pmax(fix_2, 0)
            
            FIX_DAT_2 <- which.min((fix - MATCH_VAL)^2)
            
            new_opt <- optimal_i_all_rows
            new_opt[,-c(1:4)] <- 0
            past <- which(new_opt$date <= Sys.Date())
            new_opt[past,] <- new_mat[past,]
            
            day <- which(opt_agg$date == Sys.Date() + 7) + 1
            start_hr_cumsum <- sum(new_opt[which(new_opt$date <= opt_agg$date[day] & new_opt$risk_group == "hr"),-c(1:4)])
            end_hr_cumsum <- sum(optimal_i_all_rows[which(optimal_i_all_rows$date <= opt_agg$date[day] + 30 & optimal_i_all_rows$risk_group == "hr"),-c(1:4)])
            
            gap_hr <- end_hr_cumsum - start_hr_cumsum
            
            start_lr_cumsum <- sum(new_opt[which(new_opt$date <= opt_agg$date[day] & new_opt$risk_group == "lr"),-c(1:4)])
            end_lr_cumsum <- sum(optimal_i_all_rows[which(optimal_i_all_rows$date <= opt_agg$date[day] + 30 & optimal_i_all_rows$risk_group == "lr"),-c(1:4)])
            
            gap_lr <- end_lr_cumsum - start_lr_cumsum
            
            rep_vec <- which(opt_agg$date == Sys.Date() + 8):which(opt_agg$date == Sys.Date() + 7 + 30)
            # LOOP: ? ----
            for (day in rep_vec){
              if (gap_hr > 0){
                new_opt[which(new_opt$date == opt_agg$date[day] & new_opt$risk_group == "hr"), c("BNT.162","Moderna")] <- 0.5 *  gap_hr / 30
              }
              if (gap_lr > 0){
                new_opt[which(new_opt$date == opt_agg$date[day] & new_opt$risk_group == "lr"), c("BNT.162","Moderna")] <- 0.5 *  gap_lr / 30
              }
            }
            
            future <- which(new_opt$date > Sys.Date() + 7 + 30 + 1)
            new_opt[future,] <- optimal_i_all_rows[future,]
            
            
            new_opt_hr <- new_opt[which(new_opt$risk_group == "hr"),]
            new_opt_hr$daily_all_brand <- rowSums(new_opt_hr[,-c(1:4)])
            new_opt_hr$cumul_all_brand <- cumsum(new_opt_hr$daily_all_brand)
            new_opt_lr <- new_opt[which(new_opt$risk_group == "lr"),]
            new_opt_lr$daily_all_brand <- rowSums(new_opt_lr[,-c(1:4)])
            new_opt_lr$cumul_all_brand <- cumsum(new_opt_lr$daily_all_brand)
            
            
            lr_fix <- which(new_opt_lr$cumul_all_brand < new_lr$cumul_all_brand)
            if (length(lr_fix) > 1){
              if (min(diff(new_lr$cumul_all_brand[lr_fix])) == 0){
                level <- min(which(diff(new_lr$cumul_all_brand[lr_fix]) == 0))
              } else {
                level <- length(diff(new_lr$cumul_all_brand[lr_fix]))
              }
              
              first_locs <- which(new_opt$date %in% new_opt_lr$date[lr_fix[1:level]] & new_opt$risk_group == "lr")
              new_opt[first_locs, -c(1:4)] <- new_mat[first_locs, -c(1:4)]
              if (level < length(lr_fix)){
                tmp_place_date <- new_opt_lr$date[which(new_opt_lr$date == new_lr$date[lr_fix[level]])]
                tmp_shift_date <- new_opt_lr$date[which(new_opt_lr$date == new_lr$date[max(lr_fix)])]
                tmp_shift_locs <- which(new_opt$date >= tmp_shift_date & new_opt$risk_group == "lr")
                tmp_place_locs <- sapply(seq(tmp_place_date, length = length(tmp_shift_locs), by = "1 day"),
                                         function(x){
                                           which(new_opt$date == x & new_mat$risk_group == "lr")
                                         })
                new_opt[tmp_place_locs, -c(1:4)] <- new_opt[tmp_shift_locs, -c(1:4)]
              }
            }
            
            hr_fix <- which(new_opt_hr$cumul_all_brand < new_hr$cumul_all_brand)
            if (length(hr_fix) > 1){
              if (min(diff(new_hr$cumul_all_brand[hr_fix])) == 0){
                level <- min(which(diff(new_hr$cumul_all_brand[hr_fix]) == 0))
              } else {
                level <- length(diff(new_hr$cumul_all_brand[hr_fix]))
              }
              first_locs <- which(new_opt$date %in% new_opt_hr$date[hr_fix[1:level]] & new_opt$risk_group == "hr")
              new_opt[first_locs, -c(1:4)] <- new_mat[first_locs, -c(1:4)]
              if (level < length(hr_fix)){
                tmp_place_date <- new_opt_hr$date[which(new_opt_hr$date == new_hr$date[hr_fix[level]])]
                tmp_shift_date <- new_opt_hr$date[which(new_opt_hr$date == new_hr$date[max(hr_fix)])]
                tmp_shift_locs <- which(new_opt$date >= tmp_shift_date & new_opt$risk_group == "hr")
                tmp_place_locs <- sapply(seq(tmp_place_date, length = length(tmp_shift_locs), by = "1 day"),
                                         function(x){
                                           which(new_opt$date == x & new_mat$risk_group == "hr")
                                         })
                new_opt[tmp_place_locs, -c(1:4)] <- new_opt[tmp_shift_locs, -c(1:4)]
              }
            }
            
            new_opt_hr <- new_opt_agg <- new_opt[which(new_opt$risk_group == "hr"),]
            new_opt_lr <- new_opt[which(new_opt$risk_group == "lr"),]
            new_opt_agg[,-c(1:4)] <- new_opt_hr[,-c(1:4)] + new_opt_lr[,-c(1:4)]
            new_opt_agg$daily_all_brand <- rowSums(new_opt_agg[,-c(1:4)])
            new_opt_agg$cumul_all_brand <- cumsum(new_opt_agg$daily_all_brand)
            
            # optimal_fixed[opt_indexes,] <- new_opt # KEEP - NON-PARALLEL for troubleshooting locations
            
            # BUILD POINT VALUES ----
            # For later plotting
            
            point <- data.frame(
              location_id = ULoc_ID[i],
              vaccine_course = V_COURSE,
              match_val = MATCH_VAL,
              match_dat = ref_agg$date[MATCH_DAT],
              fix_dat = ref_agg$date[FIX_DAT],
              fix_dat_2 = opt_agg$date[FIX_DAT],
              obs_dat = ref_agg$date[OBS_DAT]
            )
            
            out <- list(new_mat, new_opt, point)
            return(out)
            
            # KEEP - NON-PARALLEL for troubleshooting locations
            # if (nrow(point)){ 
            #   point_fixed <- rbind(point_fixed, point)  
            # }
            
            # KEEP - NON-PARALLEL for troubleshooting locations
            # OLD PLOTS 
            # 
            # par(mfrow=c(1,2))
            # 
            # plot(opt_agg$date, opt_agg$cumul_all_brand, col = 3, lwd = 2, type = 'l', # Green solid line
            #      xlab = "date", ylab = "Boosters Administered",main=loc_text_name_id)
            # 
            # lines(ref_agg$date, ref_agg$cumul_all_brand, col =1) # black solid line
            # points(ref_agg$date[OBS_DAT], MATCH_VAL, pch = 6) # black triangle
            # lines(ref_agg$date, fix, col = 2) # red line
            # points(ref_agg$date[FIX_DAT], MATCH_VAL, pch = 6, col = 2) # red triangle
            # points(ref_agg$date[MATCH_DAT], MATCH_VAL, pch = 8, col = 2, cex = 3) # red star - observed single value
            # lines(new_agg$date, new_agg$cumul_all_brand, col =2, lwd = 2, lty = 2)# red dashed line
            # legend("bottomright", legend = c("Old","New", "Booster"), col = 1:3, lwd = c(1,1,2))
            # abline(v=Sys.Date(), lty = 2) # vertical black dashed line
            # 
            # lines(new_opt_agg$date, new_opt_agg$cumul_all_brand, col =4, lwd = 2, lty = 2) # blue dashed "bump" (overlays red dashed until the bump)
            # 
            # plot(opt_agg$date, opt_agg$daily_all_brand, col = 3, lwd = 2, type = 'l', # green daily
            #      xlab = "date", ylab = "Boosters Administered",main=loc_text_name_id)
            # 
            # lines(ref_agg$date, ref_agg$daily_all_brand, col =1) # black daily
            # legend("topright", legend = c("Old","New", "Booster"), col = 1:3, lwd = c(1,1,2))
            # abline(v=Sys.Date(), lty = 2) # vertical black dashed line
            # lines(new_agg$date, new_agg$daily_all_brand, col =2) # red daily
            # lines(new_opt_agg$date, new_opt_agg$daily_all_brand, col =4, lwd = 2) # blue daily (overlays red daily)
            
          }
        }
      }
    }
    
    # Parallel Off ----
    parallel::stopCluster(cl)
    
    # dev.off() # KEEP - NON-PARALLEL for troubleshooting locations
  }
  
  # COMBINE RESULTS ------------------------------------------------------------
  reference_fixed <- optimal_fixed <- point_fixed <- data.frame()
  
  for(i in 1:BOOSTER_COURSES) {
    
    # List levels
    # 1. booster courses
    # 2. 1 = reference, 2 = optimal, 3 = points
    # 3. locations
    
    reference_fixed <- rbind(reference_fixed, rbindlist(result_list[[i]][[1]]))
    optimal_fixed   <- rbind(optimal_fixed,   rbindlist(result_list[[i]][[2]]))
    point_fixed     <- rbind(point_fixed,     rbindlist(result_list[[i]][[3]]))
    
  }
  
  reference_fixed <- rbind(reference_fixed, reference[reference$vaccine_course == 1, ])
  optimal_fixed <-   rbind(optimal_fixed,   optimal  [optimal$vaccine_course   == 1, ])
  
  
  # MERGE POINT VALUES ----------------------------------------------------------------
  
  point_est <- merge(point_est, point_fixed, by = c("location_id", "vaccine_course"), all.x = T)
  if(any(is.na(point_est$match_val))) {
    warning("booster_correction : Some match values for booster point estimates are missing - check final merge.")
    print("booster_correction: locations missing match_val")
    print(point_est[is.na(point_est$match_val),])
    }
  
  # WRITE FILES ----------------------------------------------------------------
  if (NO_PROB) {
    fwrite(reference_fixed, glue("{vaccine_output_root}/last_shots_in_arm_by_brand_w_booster_{reference_name}.csv"))
    fwrite(point_est, glue("{vaccine_output_root}/booster_point_estimate_for_plotting.csv"))
    
    
  } else {
    fwrite(reference_fixed, glue("{vaccine_output_root}/last_shots_in_arm_by_brand_w_booster_{reference_name}.csv"))
    fwrite(optimal_fixed, glue("{vaccine_output_root}/last_shots_in_arm_by_brand_w_booster_optimal.csv"))
    fwrite(point_est, glue("{vaccine_output_root}/booster_point_estimate_for_plots.csv"))
  }
  
  # PLOT  ----------------------------------------------------------------
  
  # Scatter plots
  plot_list <- .scatter_prep_last_shots(vaccine_output_root)
  plot_cols <- c("cumul")
  plot_percents <- c(.25, .50, .99)
  
  pdf(file.path(vaccine_output_root, 
                glue("last_shots_scatter_{.model_inputs_version}_v_{.previous_best_version}.pdf")),
      height=10, width=15)
  
  for (dt in names(plot_list)){
    
    for (col in plot_cols){
      
      for (i in plot_percents){
        
        .scatter_plot_compare(DATASET = plot_list[[dt]], PERCENT_DIFF = i, VARIABLE = col, 
                              EXTRA_LABEL = unique(plot_list[[dt]]$scenario))
      }
    }
  }
  
  dev.off()
  
  # Plot daily last shots, corrected
  
  .submit_job(
    script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "plot_additional_doses.R"),
    job_name = "plot_corrected_last_shots",
    mem = "10G",
    archiveTF = FALSE,
    threads = "4",
    runtime = "40",
    Partition = "d.q",
    Account = "proj_covid",
    args_list = list(
      "--out_fp" = paste0(vaccine_output_root, "/last_shots_corrected_plots.pdf"),
      # "--out_fp" = "FILEPATH/additional_doses_plots.pdf",
      
      "--scenario_files" = paste(
        glue("{vaccine_output_root}/last_shots_in_arm_by_brand_w_booster_reference.csv"),
        glue("{vaccine_output_root}/last_shots_in_arm_by_brand_w_booster_optimal.csv"),
        sep = ","
      )
    )
  )
  
  # Plot booster correction cumulative time series (using output files)
  .submit_job(
    script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "plot_booster_correction.R"),
    job_name = "plot_booster_correction",
    mem = "10G",
    archiveTF = FALSE,
    threads = "1",
    runtime = "40",
    Partition = "d.q",
    Account = "proj_covid",
    args_list = list(
      "--output_path" = .output_path
    )
  )
  
}

#scoping ----

# vaccine_output_root <- .output_path
# n_cores = 2
# B_COURSE = 1
# ULoc_ID <- ULoc_ID[1:20]
# i = 1
# ref_checkpoint <- data.frame(reference)
# opt_checkpoint <- data.frame(optimal)
# reference <- reference[reference$location_id %in% 1:40,]
# optimal <- optimal[optimal$location_id %in% 1:40,]
# reference <- ref_checkpoint
# optimal <- opt_checkpoint

# scoping ----