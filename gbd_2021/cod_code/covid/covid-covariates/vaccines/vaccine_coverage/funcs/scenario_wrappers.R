
.scenario_wrapper <- function(vaccine_output_root) {



  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  
  COURSES = model_parameters$vaccine_courses

  if (model_parameters$survey_yes_responses == model_parameters$survey_yes_responses_reference) {
    NO_PROB <- FALSE
    print("### Running Shots in Arms for REFERENCE scenario ###")
  } else {
    NO_PROB <- TRUE
    print("### Running Shots in Arms for HESITANCY scenario ###")
  }

  rm(model_parameters)

  # my_final_doses <- read.csv(glue("{vaccine_output_root}/final_doses_by_location_brand.csv"), as.is = TRUE)
  my_final_doses <- fread(glue("{vaccine_output_root}/final_doses_by_location_brand.csv"))
  
  # Filter out "Other Union Territories" (a subnational to India) which may be missing in slow_scenario_vaccine_covarage.csv
  my_final_doses <- my_final_doses[location_id != 44538, ]
  my_final_doses[, date := as.Date(date)]
  names(my_final_doses) <- gsub("/|'|&", ".", names(my_final_doses))
  
  my_final_doses = .drop_key_dupes(
    dt = my_final_doses,
    key_cols = c("location_id", "date"),
    dt_name = "my_final_doses"
  )
  
  my_final_doses <- as.data.frame(my_final_doses)

  # doses_in_arm <- read.csv(glue("{vaccine_output_root}/slow_scenario_vaccine_coverage.csv"), as.is = TRUE)
  doses_in_arm <- fread(glue("{vaccine_output_root}/slow_scenario_vaccine_coverage.csv"))
  doses_in_arm[, date := as.Date(date)]
  
  doses_in_arm = .drop_key_dupes(
    dt = doses_in_arm,
    key_cols = c("location_id", "date"),
    dt_name = "doses_in_arm"
  )
  
  doses_in_arm <- as.data.frame(doses_in_arm)

  doses_in_arm$lr_daily_trust <- rowSums(
    doses_in_arm[
      ,
      c(
        "lr_unprotected",
        "lr_effective_protected_wildtype",
        "lr_effective_protected_variant",
        "lr_effective_wildtype",
        "lr_effective_variant"
      )
    ],
    na.rm = T
  )

  doses_in_arm$hr_daily_trust <- rowSums(
    doses_in_arm[
      ,
      c(
        "hr_unprotected",
        "hr_effective_protected_wildtype",
        "hr_effective_protected_variant",
        "hr_effective_wildtype",
        "hr_effective_variant"
      )
    ],
    na.rm = TRUE
  )

  # This doesn't get used.
  # obs_doses_by_brand <- read.csv("FILEPATH/vaccination_brand.csv", as.is = TRUE)
  # obs_doses_by_brand <- fread("FILEPATH/vaccination_brand.csv")
  # obs_doses_by_brand[, Date := as.Date(Date)]
  # obs_doses_by_brand = as.data.frame(obs_doses_by_brand)

  # obs_lag <- read.csv("FILEPATH/empirical_lag_days.csv", as.is = TRUE)
  obs_lag <- fread("FILEPATH/empirical_lag_days.csv")
  obs_lag = unique(obs_lag)
  obs_lag <- as.data.frame(obs_lag)

  # plot(doses_in_arm$cumulative_all_fully_vaccinated / (doses_in_arm$lr_daily_trust + doses_in_arm$hr_daily_trust), ylim=c(0,2))
  # tmp <- doses_in_arm[which(doses_in_arm$location_id == 63),]
  # plot(tail(tmp$cumulative_all_fully_vaccinated, -14) / head(cumsum(tmp$lr_daily_trust + tmp$hr_daily_trust), -14), ylim=c(.8,.9))
  # tmp <- Comb_Brand_data[which(Comb_Brand_data$location_id == 146),]
  # lines(as.Date(tmp$date), cumsum(rowSums(tmp[,-c(1:2)])), lwd = 2)

  companies <- c(
    "SerumInstituteofIndiaAZD", "Janssen", "Pfizer.BioNTech", "GSK.Sanofi",
    "Moderna", "AstraZeneca", "Sinovac", "TianjinCanSino",
    "BioFarma", "Novavax", "GamaleyaResearchInstitute", "CSL",
    "BharatBiotech", "CNBGWuhan", "Valneva", "Fiocruz",
    "Dr.Reddy.s", "SerumInstituteofIndiaNVX"
  )

  companies_doses <- paste(companies, "doses", sep = "_")
  
  ULocs <- unique(my_final_doses$location_id)
  
  missing <- setdiff(ULocs, obs_lag$location_id)
  obs_lag <- rbind(obs_lag, data.frame(location_id = missing, observed_lag = 28))
  location_id <- ULocs[1]
  tmp_doses_in_arm <- my_final_doses[which(my_final_doses$location_id == location_id), ]

  #
  companies_new <- paste(companies, "new", sep = "_") # First dose, who will eventually get second
  companies_2nd <- paste(companies, "2nd", sep = "_") # Getting second
  companies_only <- paste(companies, "only", sep = "_") # Only first dose, drop outs
  companies_total_eff <- paste(companies, "total_eff", sep = "_") # Effectiveness by dose (90% n e.g.)


  last_df_cols <- c("location_id", "date", companies_doses, companies_new, companies_2nd, companies_only, companies_total_eff)

  # Which company is each dose? (Which brnads are available on the shelf)
  last_shot <- data.frame(matrix(ncol = length(last_df_cols), nrow = length(tmp_doses_in_arm[, 1] * length(ULocs)), dimnames = list(NULL, last_df_cols)))
  pb <- txtProgressBar(min = 0, max = length(ULocs), style = 3)

  message("  ## location loop #1: ")
  for (loc_num in 1:length(ULocs)) {
    # print(paste("    location #", loc_num, ": location ID ", ULocs[loc_num]))
    setTxtProgressBar(pb, loc_num)
    location_id <- ULocs[loc_num]
    tmp_final_doses <- my_final_doses[which(my_final_doses$location_id == location_id), ]
    tmp_final_doses[is.na(tmp_final_doses)] <- 0
    #
    tmp_df <- data.frame(matrix(ncol = length(last_df_cols), nrow = length(tmp_doses_in_arm[, 1]), dimnames = list(NULL, last_df_cols)))
    tmp_df$location_id <- location_id
    tmp_df$date <- tmp_final_doses$date
    #
    OL <- obs_lag$observed_lag[which(obs_lag$location_id == location_id)]
    #
    for (comp in 1:length(companies)) {
      # print(paste("    company #", comp, ": company ID ", companies[comp]))
      if (sum(tmp_final_doses[, companies_doses[comp]]) > 0) {
        tmp_df[, companies_doses[comp]] <- tmp_final_doses[, companies_doses[comp]]
        if (comp == 2) {
          # J&J
          for (d in 1:length(tmp_df$date)) {
            tmp_df[d, companies_new[comp]] <- tmp_df[d, companies_only[comp]] <- tmp_final_doses[d, companies_doses[comp]]
            if (d > 14) {
              tmp_df[d, companies_total_eff[comp]] <- tmp_df[d - 14, companies_only[comp]]
            }
          }
        } else {
          # Everyone else
          tmp_df[, companies_doses[comp]] <- tmp_final_doses[, companies_doses[comp]]
          for (d in 1:length(tmp_df$date)) {
            if (d <= OL) {
              tmp_df[d, companies_new[comp]] <- tmp_final_doses[d, companies_doses[comp]]
              tmp_df[d, companies_2nd[comp]] <- 0
            } else {
              tmp_df[d, companies_new[comp]] <- max(0, tmp_final_doses[d, companies_doses[comp]] - 0.9 * tmp_final_doses[d - OL, companies_doses[comp]])
              tmp_df[d, companies_2nd[comp]] <- 0.9 * tmp_final_doses[d - OL, companies_doses[comp]]
            }
            tmp_df[d, companies_only[comp]] <- 0.1 * tmp_df[d, companies_new[comp]]
            if (d > 14) {
              tmp_df[d, companies_total_eff[comp]] <- tmp_df[d - 14, companies_2nd[comp]] + 0.6 * tmp_df[d - 14, companies_only[comp]]
            }
          }
        }
      }
    }
    last_shot <- rbind(last_shot, tmp_df)
  }
  close(pb)

  rm(obs_lag)


  location_id <- ULocs[1]
  tmp_doses_in_arm <- doses_in_arm[which(doses_in_arm$location_id == location_id), ]



  tmp_df_cols <- c("location_id", "date", "vaccine_course", "risk_group", companies_doses)

  pb <- txtProgressBar(min = 0, max = length(ULocs), style = 3)

  # Fully vaccinated by company, or drop out (last shot in arm) (efficacy implied)
  Full_data <- data.frame(matrix(ncol = length(tmp_df_cols), nrow = 0, dimnames = list(NULL, tmp_df_cols)))
  message("  ## location loop #2: ")
  for (loc_num in 1:length(ULocs)) {
    setTxtProgressBar(pb, loc_num)
    location_id <- ULocs[loc_num]
    tmp_final_doses <- last_shot[which(last_shot$location_id == location_id), ]
    tmp_final_doses[is.na(tmp_final_doses)] <- 0
    tmp_doses_in_arm <- doses_in_arm[which(doses_in_arm$location_id == location_id), ]

    if (nrow(tmp_doses_in_arm) == 0 || nrow(tmp_final_doses) == 0) {
      message(glue("  -- Warning: Empty tmp_doses_in_arm and/or tmp_final_doses for Location ID: {location_id}"))
    } else {
      tmp_df <- data.frame(matrix(ncol = length(tmp_df_cols), nrow = 2 * length(tmp_doses_in_arm[, 1]), dimnames = list(NULL, tmp_df_cols)))
      tmp_df$location_id <- location_id
      tmp_df$date <- rep(tmp_doses_in_arm$date, each = 2)
      tmp_df$vaccine_course <- 1
      tmp_df$risk_group[2 * (1:length(tmp_doses_in_arm[, 1])) - 1] <- "hr"
      tmp_df$risk_group[2 * (1:length(tmp_doses_in_arm[, 1]))] <- "lr"
      tmp_vec <- rep(0, length(companies_total_eff))
      # Estimating in case there aren't actually doses on the shelf.
      tmp_lr_avg <- apply(tmp_final_doses[, companies_total_eff], 2, sum, na.rm = TRUE) / sum(apply(tmp_final_doses[, companies_total_eff], 2, sum, na.rm = TRUE))
      if (dim(tmp_doses_in_arm)[1] > 0 & dim(tmp_final_doses)[1]) {
        if (sum(tmp_final_doses[, companies_total_eff])) {
          for (i in 1:length(tmp_doses_in_arm[, 1])) {
            tmp_vec <- tmp_vec + tmp_final_doses[i, companies_total_eff]
            if (tmp_doses_in_arm$hr_daily_trust[i] > 0) {
              if (sum(tmp_final_doses[i, companies_total_eff])) {
                tmp_avg <- tmp_vec / sum(tmp_vec)
                new_shots <- tmp_doses_in_arm$hr_daily_trust[i]
                tmp_ans <- tmp_avg * new_shots
                tmp_df[2 * i - 1, companies_doses] <- tmp_ans
                tmp_vec <- pmax(0, tmp_vec - tmp_ans)
              } else {
                new_shots <- tmp_doses_in_arm$hr_daily_trust[i]
                tmp_ans <- tmp_lr_avg * new_shots
                tmp_df[2 * i - 1, companies_doses] <- tmp_ans
              }
            }
            if (tmp_doses_in_arm$lr_daily_trust[i] > 0) {
              if (sum(tmp_vec)) {
                tmp_avg <- tmp_vec / sum(tmp_vec)
                new_shots <- tmp_doses_in_arm$lr_daily_trust[i]
                tmp_ans <- tmp_avg * new_shots
                tmp_df[2 * i, companies_doses] <- tmp_ans
                tmp_vec <- pmax(0, tmp_vec - tmp_ans)
              } else {
                tmp_avg <- tmp_final_doses[i, companies_total_eff] / sum(tmp_final_doses[i, companies_total_eff])
                new_shots <- tmp_doses_in_arm$lr_daily_trust[i]
                tmp_ans <- tmp_avg * new_shots
                tmp_df[2 * i, companies_doses] <- tmp_ans
              }
            }
          }
        }
      }
      Full_data <- rbind(Full_data, tmp_df)
    }
  }
  close(pb)

  rm(last_shot)

  setDT(Full_data)
  # In case it isn't already ordered.
  id_cols <- c("location_id", "date", "vaccine_course", "risk_group")

  Full_data[
    ,
    `:=`(
      date = as.Date(date),
      risk_group = factor(risk_group),
      vaccine_course = as.integer(vaccine_course)
    )
  ]
  setkeyv(x = Full_data, cols = id_cols)

  if (NO_PROB) {
    fwrite(Full_data, glue("{vaccine_output_root}/last_shots_in_arm_by_company_v_np.csv"))

    # Full_data <- fread(glue("{vaccine_output_root}/last_shots_in_arm_by_company_v_np.csv"))
  } else {
    fwrite(Full_data, glue("{vaccine_output_root}/last_shots_in_arm_by_company_v_1.csv"))

    # Full_data <- fread(glue("{vaccine_output_root}/last_shots_in_arm_by_company_v_1.csv"))
  }


  ### Set brand data. ############################################################

  Full_data[is.na(Full_data)] <- 0
  # Full_data[
  #   ,
  #   `:=`(
  #     date = as.Date(date),
  #     risk_group = factor(risk_group),
  #     vaccine_course = as.integer(vaccine_course)
  #   )
  # ]
  # setkeyv(x = Full_data, cols = id_cols)

  Brand_data <- Full_data[, ..id_cols]

  Brand_data[, BNT.162 := Full_data[, Pfizer.BioNTech_doses]]
  Brand_data[, Moderna := Full_data[, Moderna_doses]]
  Brand_data[, AZD1222 := Full_data[, SerumInstituteofIndiaAZD_doses] + Full_data[, AstraZeneca_doses] + Full_data[, Fiocruz_doses]]
  Brand_data[, Janssen := Full_data[, Janssen_doses]]
  Brand_data[, c("Sputnik V") := Full_data[, GamaleyaResearchInstitute_doses] + Full_data[, Dr.Reddy.s_doses]]
  Brand_data[, Novavax := Full_data[, Novavax_doses] + Full_data[, SerumInstituteofIndiaNVX_doses]]
  Brand_data[, CoronaVac := Full_data[, Sinovac_doses] + Full_data[, BioFarma_doses]]
  Brand_data[, c("CNBG Wuhan") := Full_data[, CNBGWuhan_doses]]
  Brand_data[, c("Tianjin CanSino") := Full_data[, TianjinCanSino_doses]]
  Brand_data[, Covaxin := Full_data[, BharatBiotech_doses]]
  Brand_data[, c("mRNA Vaccine") := Full_data[, GSK.Sanofi_doses]]
  Brand_data[, Other := Full_data[, Valneva_doses] + Full_data[, CSL_doses]]

  rm(Full_data)

  fwrite(Brand_data, glue("{vaccine_output_root}/last_shots_in_arm_by_brand_v_1.csv"))

  # Brand_data <- fread(glue("{vaccine_output_root}/last_shots_in_arm_by_brand_v_1.csv"))
  # Brand_data[
  #   ,
  #   `:=`(
  #     date = as.Date(date),
  #     risk_group = factor(risk_group),
  #     vaccine_course = as.integer(vaccine_course)
  #   )
  # ]
  # setkeyv(x = Brand_data, cols = id_cols)
  
  # Brand_data = .drop_key_dupes(
  #   dt = Brand_data,
  #   key_cols = id_cols,
  #   dt_name = "Brand_data"
  # )

  Brand_data <- .strip_indices(df = Brand_data)

  brand_cols <- setdiff(names(Brand_data), id_cols)

  First_Day <- as.Date("2021-08-01")
  Flush_Day <- Sys.Date() + 4 
  Fraction_Agree_low <- 0.8
  tmp_t_vec <- seq(First_Day, as.Date(max(Brand_data$date)), by = "1 day")

  if (NO_PROB) {
    RunOptions <- FALSE
  } else {
    RunOptions <- c(TRUE, FALSE)
  }

  RunOptions <- c(F)
  
  for (RunOptim in RunOptions) {
    if (RunOptim) {
      catchup <- 30
      post_flush_lag <- 180
      ULocs_Boosters <- ULocs
    } else {
      catchup <- 60 # 
      post_flush_lag <- 180
      ULocs_Boosters <- intersect(
        ULocs,
        unique( # Gets booster locs.
          c(
            gbd_data$get_covid_modeling_hierarchy()[
              most_detailed == 1 & super_region_name == "High-income",
              location_id
            ],
            unique(
              fread(
                file.path(vaccine_output_root, "boosters_point_estimate.csv")
              )$location_id
            )
          )
        )
      )
    }

    message("Estimating additional doses. COURSES: ", COURSES, "; RunOptim: ", RunOptim)
    tic("Additional doses estimated.")
    all_scens_dt <- .estimate_additional_doses(
      Brand_data = Brand_data,
      brand_cols = brand_cols,
      ULocs_Boosters = ULocs_Boosters,
      doses_in_arm = as.data.table(doses_in_arm),
      catchup = catchup,
      First_Day = First_Day,
      Flush_Day = Flush_Day,
      Fraction_Agree_low = Fraction_Agree_low,
      courses = COURSES
    )
    toc()

    reference_filename <- ""
    if (NO_PROB) {
      reference_filename <-
        "uncorrected_last_shots_in_arm_by_brand_w_booster_no_prob.csv"
    } else {
      reference_filename <-
        "uncorrected_last_shots_in_arm_by_brand_w_booster_reference.csv"
    }

    fwrite(
      all_scens_dt[scenario == "Reference_scenario", -c("scenario")],
      glue("{vaccine_output_root}/{reference_filename}")
    )

    # Booster correction appears to always expect the optimal file.
    fwrite(
      all_scens_dt[scenario == "Optim_scenario", -c("scenario")],
      glue("{vaccine_output_root}/uncorrected_last_shots_in_arm_by_brand_w_booster_optimal.csv")
    )
  }


  # Plot.
  # .submit_job(
  #   script_path = file.path(CODE_PATHS$VACCINE_DIAGNOSTICS_ROOT, "plot_additional_doses.R"),
  #   job_name = "plot_additional_doses.R",
  #   mem = "10G",
  #   archiveTF = TRUE,
  #   threads = "4",
  #   runtime = "40",
  #   Partition = "d.q",
  #   Account = "proj_covid",
  #   args_list = list(
  #     "--out_fp" = paste0(vaccine_output_root, "/additional_doses_plots.pdf"),
  #     # "--out_fp" = "FILEPATH/additional_doses_plots.pdf",
  
  #     "--scenario_files" = paste(
  #       paste0(vaccine_output_root, "/", reference_filename),
  #       paste0(vaccine_output_root, "/uncorrected_last_shots_in_arm_by_brand_w_booster_optimal.csv"),
  #       sep = ","
  #     )
  #   )
  # )
}


################################################################################


#' @description Adds additional doses by brand (vaccine courses 2+) to data table, applying estimated lag, acceptance, and supply under different scenarios. Runs day by day, risk group by risk group, and course by course; all courses beyond the first will be administered before moving on to the next day, but the first course is already allocated in the data table that gets passed in.
#'
#' @param Brand_data [data.table] The table to add boosters to.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param ULocs_Boosters [vector(integer)] (May actually be numeric.) The location_ids of locations to add boosters to.
#' @param doses_in_arm [data.table] The count of people who got vaccinated?
#' @param catchup [integer] (May actually be numeric.) The number of days to divide the remaining dose by during the flush period.
#' @param First_Day [Date] First day course 1 began to be administered. Is used to determine the first day of additional courses.(Currently set the same for all locations before passing in.)
#' @param Flush_Day [Date] Day when projection begins, when all remaining doses are flushed. (Currently set to four days from system time before passing in.)
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param courses [integer] The max number of courses. Course 2 is the first booster, so passing 2 will add one addtional dose, or one booster.
#'
#' @return [list(data.table)] A list of data tables, one for each scenario, that are the input table with rows added for additional doses.
.estimate_additional_doses <- function(
  Brand_data,
  brand_cols,
  ULocs_Boosters,
  doses_in_arm,
  catchup,
  First_Day,
  Flush_Day,
  Fraction_Agree_low,
  courses
) {

  all_scens_dt <- data.table()


  for (scen in c("Reference_scenario", "Optim_scenario")) {
    all_scens_dt <- rbind(
      all_scens_dt,
      cbind(Brand_data, data.table(scenario = rep(scen, nrow(Brand_data))))
    )
  }

  # all_scens_dt = unique(all_scens_dt)
  
  # Start running courses day by day.
  if (is.na(courses) | courses <= 1) {
    warning("No additional courses added. courses: ", courses)
  } else {
    message("Running all scenarios on all locations.")

    r_grps <- unique(Brand_data$risk_group)

    # A list of this function's parameters to pass to foreach. (and r_grps)
    
    # param_names = names(c(as.list(environment()), list(...)))

    param_names <- c(
      "Brand_data",
      "brand_cols",
      "ULocs_Boosters",
      "doses_in_arm",
      "catchup",
      "First_Day",
      "Flush_Day",
      "Fraction_Agree_low",
      "courses"
    )

    # for (loc_id in ULocs_Boosters) {
    
    ## May require just using lapply instead.
    threads <- 20
    # For debug/optimization:
    do_locs <- unique(all_scens_dt$location_id)
    # do_locs = c(25)
    # do_locs = c(8,35510,60368,44,4662,4851,4768,30)
    # do_locs = c(8,35510,60368,44,4662,4851,4768,30,215,565,141,173,497,129,506)
    # do_locs = sample(unique(all_scens_dt$location_id), size = 100)
    # for (threads in 20:24) {
    tic(paste("Ran all scenarios on all locations. Threads: ", threads))
    cl <- makeCluster(spec = threads, type = "FORK")
    doParallel::registerDoParallel(cl)
    loc_scens_dts_list <- foreach(
      tmp_loc_scen_dt = split(x = all_scens_dt[location_id %in% do_locs], by = "location_id"),
      # To step into the block with a single location:
      # tmp_loc_scen_dt = split(x = all_scens_dt[location_id %in% do_locs], by = "location_id")[[113]],
      .packages = c("data.table"),
      .export = c(param_names, "r_grps"),
      .inorder = F,
      .verbose = F
    ) %dopar% {
      
      loc_id <- unique(tmp_loc_scen_dt$location_id)

      if (!(loc_id %in% ULocs_Boosters)) {
        warning("Location not in ULocs_Boosters. location_id: ", loc_id, ". Course columns will be added with NAs.")
      } else {
        # Set up location before running scenarios. ############################

        loc_doses <- doses_in_arm[location_id == loc_id]


        # Get long-range average.
        brand_sums <- apply(
          Brand_data[
            location_id == loc_id & vaccine_course == 1,
            ..brand_cols
          ],
          2,
          sum,
          na.rm = TRUE
        )

        longrange_avg <- brand_sums / sum(brand_sums)

        longrange_avg[is.nan(longrange_avg)] <- 0


        # Set up time series and variables for each course.
        ## First day is when people can start getting boosters.
        ## Flush day is when scenario begins (4 days from today).
        flush_schedule_dt <- data.table(date = unique(Brand_data[["date"]]))
        setkeyv(flush_schedule_dt, c("date"))

        lags <- list(
          
          first_day = 180,
          flush_day = 180,
          pre_flush = 180,
          flush = 180,
          post_flush = 120
        )

        flush_duration <- lags$flush - lags$post_flush

        course_vars <- list(
          # To store dates of the last days of each period for each course.
          period_last_days = list(
            pre_flush = list(),
            flush = list(),
            post_flush = list()
          )
        )

        for (course in 2:courses) {
          course_col <- get_course_col(course)

          course_1st_day <- get_course_first_day(
            First_Day = First_Day,
            course = course,
            course_first_day_lag = lags$first_day
          )

          course_flush_day <- get_course_flush_day(
            Flush_Day = Flush_Day,
            course = course,
            course_flush_day_lag = lags$flush_day
          )


          flush_schedule_dt[[course_col]] <- rep("", nrow(flush_schedule_dt))

          flush_schedule_dt[date == course_1st_day, c(course_col) := "first_day"]

          flush_schedule_dt[
            date > course_1st_day &
              date < course_flush_day,
            c(course_col) := "pre_flush"
          ]

          flush_schedule_dt[date == course_flush_day, c(course_col) := "flush_day"]

          flush_schedule_dt[
            date > course_flush_day &
              date <= course_flush_day + flush_duration,
            c(course_col) := "flush"
          ]

          flush_schedule_dt[
            date > course_flush_day + flush_duration,
            c(course_col) := "post_flush"
          ]

          flush_schedule_dt[
            ,
            c(course_col) := factor(
              get(course_col),
              levels = c("first_day", "pre_flush", "flush_day", "flush", "post_flush")
            )
          ]

          # This gets used in .do_low_risk_reference to determine when to set remaining doses to 0 if we started out with not enough in the first place and had to use the long-range average to estimate how many to use.
          for (period in names(course_vars$period_last_days)) {
            course_vars$period_last_days[[period]] <- max(
              flush_schedule_dt[get(course_col) == period, date],
              na.rm = T
            )
          }
        }
        #
        #
        # print(
        #   ggplot(data = flush_schedule_dt, aes(x = date)) +
        #     geom_jitter(aes(y = course_2), color = "blue") +
        #     geom_jitter(aes(y = course_3), color = "red")
        # )
        
        # To store the number of shots given on the first day if everyone who could get a shot up to the point did and we could use all the shots that had built up on the shelf, but then to spread that out over the first n days instead of having a first-day spike.
        first_day_shots = list(
          first_day = list(),
          count = list(),
          
          DIST_PERIOD = 30
        )


        # Run scenarios day by day. ##############################################

        for (scen in unique(tmp_loc_scen_dt$scenario)) {
          # Set up initial counts.
          # The number of people who will get vaccinated but haven't yet? Or remaining doses?
          # effective_rem = NA
          if (is.na(max(loc_doses$smooth_combined_yes))) {
            if (is.na(loc_doses$adult_population[1])) {
              effective_rem <- loc_doses$adult_population[1] *
                min(
                  1,
                  max(loc_doses$fully_vaccinated, na.rm = TRUE) /
                    loc_doses$population[1]
                )
            } else {
              effective_rem <- loc_doses$adult_population[1] *
                min(
                  1,
                  max(loc_doses$fully_vaccinated, na.rm = TRUE) /
                    loc_doses$adult_population[1]
                )
            }
          } else {
            effective_rem <- loc_doses$adult_population[1] *
              max(loc_doses$smooth_combined_yes, na.rm = TRUE)
          }
          # Remaining doses.
          effective_rem <- Fraction_Agree_low * effective_rem

          # People who could have but didn't (to add to optimal scenario, using Flush day). Or remaining doses?
          carry_lr <- 0
          carry_hr <- 0

          # Shots administered on first day in each period.
          shots_day1_list <- list(
            first_day = NA, # Dummy so we can use this object in any period.
            pre_flush = NA,
            flush_day = NA, # Dummy so we can use this object in any period.
            flush = NA,
            post_flush = NA
          )
          
          # Prepare new first-day counts list by group.
          for (r_grp in r_grps) {
            first_day_shots$count[[r_grp]] = list()
          }


          ######### Start making rows. ###########################################
          # Run all courses for each day so the effective_rem (how many people left who would still get vaccinated? unmet demand) and carry_lr/hr (effective doses left?) take into account all courses before going to next day. The operation varies depending on which period the course is in.

          for (day in flush_schedule_dt$date) {
            for (r_grp in r_grps) {
              for (course in 2:courses) {
                course_col <- get_course_col(course)

                course_period <- unlist(
                  flush_schedule_dt[date == day, get(course_col)]
                )

                if (!is.na(course_period)) {

                  # Add each row(s) and return effective_rem, carry_hr, and carry_lr.
                  dose_list <- run_course_day(
                    loc_id = loc_id, 
                    scenario_dt = tmp_loc_scen_dt[scenario == scen],
                    effective_rem = effective_rem,
                    carry_hr = carry_hr,
                    carry_lr = carry_lr,
                    scen = scen,
                    day = day,
                    course = course,
                    course_period = course_period,
                    flush_lag = lags[[course_period]],
                    Fraction_Agree_low = Fraction_Agree_low,
                    longrange_avg = longrange_avg,
                    brand_cols = brand_cols,
                    r_grp = r_grp,
                    shots_day1 = shots_day1_list[[course_period]],
                    period_last_day = max(
                      flush_schedule_dt[get(course_col) == course_period, date],
                      na.rm = T
                    ),
                    catchup = catchup,
                    first_day_shots = first_day_shots
                  )

                  
                  tmp_loc_scen_dt <- rbind(
                    tmp_loc_scen_dt[scenario != scen],
                    dose_list$scenario_dt
                  )
                  effective_rem <- dose_list$effective_rem
                  carry_hr <- dose_list$carry_hr
                  carry_lr <- dose_list$carry_lr
                  shots_day1_list[[course_period]] <- dose_list$shots_day1
                  first_day_shots = dose_list$first_day_shots
                } # End if (!is.na(course_period)) {
              } # End for (course in 2:courses) {
            } # End for (r_grp in r_grps) {
          } # End for (day in flush_schedule_dt$date) {
        } # End for (scen in names(scenario_vars)) {
      } # End if (!(loc_id %in% ULocs_Boosters)) {
      tmp_loc_scen_dt
    } # End %dopar% {
    stopCluster(cl)
    toc()
    # } # End for (threads in 1:10) {
  } # End if (is.na(courses) | courses > 1) {

  all_scens_dt <- rbindlist(loc_scens_dts_list, use.names = T, fill = T)

  setkeyv(
    x = all_scens_dt,
    cols = c("location_id", "date", "vaccine_course", "risk_group")
  )


  return(all_scens_dt)
}


################################################################################


get_course_col <- function(course) {
  return(paste0("course_", course))
}


################################################################################


get_course_first_day <- function(First_Day, course, course_first_day_lag) {
  return(First_Day + ((course - 2) * course_first_day_lag))
}


################################################################################


get_course_flush_day <- function(Flush_Day, course, course_flush_day_lag) {
  return(Flush_Day + ((course - 2) * course_flush_day_lag))
}


################################################################################


#' @description Adds boosters for the given day-group-course. Will call a different allocation function based on which period we're in.
#' 
#' @param loc_id [integer]
#' @param scenario_dt [data.table] The table to update.
#' @param effective_rem [numeric] Number of people who would get a shot.
#' @param carry_hr [numeric] Pool of high-risk people remaining who would but have not yet.
#' @param carry_lr [numeric] Pool of low-risk people remaining who would but have not yet.
#' @param scen [character] The scenario we're running. ("Reference_scenario", "Optim_scenario") Will treat all scenarios other than optimal scenario the same as reference scenario.
#' @param day [Date] The day we're on.
#' @param course [integer] The course we're administering. (course 2 is the first booster)
#' @param course_period [character] The phase we're in for this course. ("first_day", "pre_flush", "flush_day", "flush", "post_flush")
#' @param flush_lag [integer] How many days to look back to determine how many people received the previous course and are ready for the next today.
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param longrange_avg [list(character:numeric)] List of averages for each brand in this location, used to estimate how many boosters will actually get administered.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param r_grp [character] The risk group we're administering for. ("hr", "lr") Will treat all risk groups other than high-risk as low-risk.
#' @param shots_day1 [Date] The first day of this period for this course.
#' @param period_last_day [Date] The last day of this period for this course.
#' @param catchup [integer] (May actually be numeric.) The number of days to divide the remaining dose by during the flush period.
#' @param first_day_shots [list(list(character:Date), list(character:numeric), character:integer)] To store the number of shots given on the first day if everyone who could get a shot up to the point did and we could use all the shots that had built up on the shelf, but then to spread that out over the first n days instead of having a first-day spike. first_day stores the date of the first day of each course; count stores the number of shots to distribute to the first day and n days after; DIST_PERIOD is the number of days to distribute the first-day spike over.
#' 
#' @return [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the passed arguments then updated before returning.
run_course_day <- function(
  loc_id, 
  scenario_dt,
  effective_rem,
  carry_hr,
  carry_lr,
  scen,
  day,
  course,
  course_period,
  flush_lag,
  Fraction_Agree_low,
  longrange_avg,
  brand_cols,
  r_grp,
  shots_day1,
  period_last_day,
  catchup,
  first_day_shots
) {
  
  # Updates and returns this to add rows and maintain running count of doses and the eligible.
  dose_list <- list(
    scenario_dt = scenario_dt,
    effective_rem = effective_rem,
    carry_hr = carry_hr,
    carry_lr = carry_lr,
    shots_day1 = shots_day1,
    first_day_shots = first_day_shots
  )

  # Get lagged row from previous course for this group/day.
  tmp_row <- scenario_dt[
    location_id == loc_id &
      date == day - flush_lag &
      risk_group == r_grp &
      vaccine_course == course - 1
  ]

  # Calculate doses administered in previous course lagged.
  shots <- NA
  if (course_period == "first_day") {
    shots <- sum(
      scenario_dt[
        location_id == loc_id &
          date <= day - flush_lag &
          vaccine_course == course - 1 &
          risk_group == r_grp,
        ..brand_cols
      ],
      na.rm = T
    )
  } else {
    shots <- sum(tmp_row[, ..brand_cols], na.rm = T)
  }

  # Remember day one of doses in this period to check if the period started with insufficient doses in the first place.
  if (
    course_period %in% c("pre_flush", "flush", "post_flush") &
      is.na(dose_list$shots_day1)
  ) {
    dose_list$shots_day1 <- shots
  }

  # Initialize new row. (Bring it back to today and this course.)
  tmp_row[, date := date + flush_lag]
  tmp_row[, vaccine_course := course]
  tmp_row[, c(brand_cols) := 0]

  # Specialized functions per period.
  course_per_funx <- list(
    first_day = .allocate_first_day,
    pre_flush = .allocate_pre_flush,
    flush_day = .allocate_flush_day,
    flush = .allocate_flush,
    post_flush = .allocate_post_flush
  )

  # Update records and counts by allocating shots for this scenario-location-period-day-course-group.
  
  dose_list <- course_per_funx[[course_period]](
    dose_list = dose_list,
    scen = scen,
    loc_id = loc_id,
    day = day,
    r_grp = r_grp,
    course = course,
    course_period = course_period,
    tmp_row = tmp_row,
    shots = shots,
    Fraction_Agree_low = Fraction_Agree_low,
    longrange_avg = longrange_avg,
    brand_cols = brand_cols,
    period_last_day = period_last_day,
    catchup = catchup
  )


  return(dose_list)
}


################################################################################


#' @description Allocates boosters on the first day of the course.
#' 
#' @param dose_list [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
#' @param scen [character] The scenario we're running. ("Reference_scenario", "Optim_scenario") Will treat all scenarios other than optimal scenario the same as reference scenario.
#' @param loc_id [integer]
#' @param day [Date] The day we're on.
#' @param r_grp [character] The risk group we're administering for. ("hr", "lr") Will treat all risk groups other than high-risk as low-risk.
#' @param course [integer] The course we're administering. (course 2 is the first booster)
#' @param course_period [character] The phase we're in for this course. ("first_day", "pre_flush", "flush_day", "flush", "post_flush")
#' @param tmp_row [data.table] The row n days before in the previous course so that today's row may be calculated.
#' @param shots [numeric] The total number of shots given across all brands n days before in the previous course, or for all days within the period n days before until first day if this is the first day.
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param longrange_avg [list(character:numeric)] List of averages for each brand in this location, used to estimate how many boosters will actually get administered.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param period_last_day [Date] The last day of this period for this course.
#' @param catchup [integer] (May actually be numeric.) The number of days to divide the remaining dose by during the flush period.
#' 
#' @return [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] (dose_list) Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
.allocate_first_day <- function(
  dose_list,
  scen,
  loc_id,
  day,
  r_grp,
  course,
  course_period,
  tmp_row,
  shots,
  Fraction_Agree_low,
  longrange_avg,
  brand_cols,
  period_last_day,
  catchup
) {
  
  
  if (r_grp == "hr") {
    dose_list$carry_hr <- (1 - Fraction_Agree_low) * shots
  } else { # All other scenarios will be treated as low-risk "lr".
  # } else if (r_grp == "lr") {
    if (Fraction_Agree_low * shots > dose_list$effective_rem) {
      tmp_shots <- dose_list$effective_rem / Fraction_Agree_low
      dose_list$carry_lr <- shots - tmp_shots
      shots <- tmp_shots
    }

    # Effective doses left?
    dose_list$carry_lr <- dose_list$carry_lr + (1 - Fraction_Agree_low) * shots
    # Effective doses left?
    dose_list$effective_rem <- dose_list$effective_rem - Fraction_Agree_low * shots
  }

  tmp_row[, risk_group := r_grp]

  tmp_row[
    , 
    names(tmp_row[, ..brand_cols]) :=
      as.list(longrange_avg * Fraction_Agree_low * shots)
  ]
  
  # To avoid a first-day spike due to built up supply and demand, spread out over n days.
  ## Save the date.
  dose_list$first_day_shots$date[[paste0("course_", course)]] = tmp_row[, date]
  ## Only use 1/nth of it on the first day.
  tmp_row[
    ,
    names(tmp_row[, ..brand_cols]) :=
      as.list(tmp_row[, ..brand_cols] / dose_list$first_day_shots$DIST_PERIOD)
  ]
  ## Store the daily distribution value.
  dose_list$first_day_shots$count[[r_grp]][[paste0("course_", course)]] =
    tmp_row[, ..brand_cols]

  dose_list$scenario_dt <- rbind(dose_list$scenario_dt, tmp_row)


  return(dose_list)
}


################################################################################


#' @description Allocates boosters after the first day of the course to the flush day of the course.
#' 
#' @param dose_list [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
#' @param scen [character] The scenario we're running. ("Reference_scenario", "Optim_scenario") Will treat all scenarios other than optimal scenario the same as reference scenario.
#' @param loc_id [integer]
#' @param day [Date] The day we're on.
#' @param r_grp [character] The risk group we're administering for. ("hr", "lr") Will treat all risk groups other than high-risk as low-risk.
#' @param course [integer] The course we're administering. (course 2 is the first booster)
#' @param course_period [character] The phase we're in for this course. ("first_day", "pre_flush", "flush_day", "flush", "post_flush")
#' @param tmp_row [data.table] The row n days before in the previous course so that today's row may be calculated.
#' @param shots [numeric] The total number of shots given across all brands n days before in the previous course, or for all days within the period n days before until first day if this is the first day.
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param longrange_avg [list(character:numeric)] List of averages for each brand in this location, used to estimate how many boosters will actually get administered.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param period_last_day [Date] The last day of this period for this course.
#' @param catchup [integer] (May actually be numeric.) The number of days to divide the remaining dose by during the flush period.
#' 
#' @return [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] (dose_list) Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
.allocate_pre_flush <- function(
  dose_list,
  scen,
  loc_id,
  day,
  r_grp,
  course,
  course_period,
  tmp_row,
  shots,
  Fraction_Agree_low,
  longrange_avg,
  brand_cols,
  period_last_day,
  catchup
) {
  
  if (r_grp == "hr") {
    for (col_name in brand_cols) { 
      tmp_row[
        ,
        c(col_name) :=
          longrange_avg[[col_name]] * Fraction_Agree_low * shots
      ]
    }
    # tmp_row[, ..brand_cols := test_longrange_avg * Fraction_Agree_low * shots]

    dose_list$carry_hr <- dose_list$carry_hr + (1 - Fraction_Agree_low) * sum(shots)
  } else { # All other scenarios will be treated as low-risk "lr".
    # } else if (r_grp == "lr") {
    lr_list <- .do_low_risk_reference(
      tmp_row = tmp_row,
      effective_rem = dose_list$effective_rem,
      carry_lr = dose_list$carry_lr,
      shots = shots,
      brand_cols = brand_cols,
      Fraction_Agree_low = Fraction_Agree_low,
      longrange_avg = longrange_avg,
      shots_day1 = dose_list$shots_day1,
      period_last_day = period_last_day
    )

    tmp_row <- lr_list$tmp_row
    dose_list$carry_lr <- lr_list$carry_lr
    dose_list$effective_rem <- lr_list$effective_rem
  }
  
  # Spread the first-day spike over n days.
  if (
    tmp_row$date <
    (
      dose_list$first_day_shots$date[[paste0("course_", course)]] +
      dose_list$first_day_shots$DIST_PERIOD
    )
  ) {
    tmp_row[
      ,
      names(tmp_row[, ..brand_cols]) :=
        as.list(
          tmp_row[, ..brand_cols] +
            dose_list$first_day_shots$count[[r_grp]][[paste0("course_", course)]]
        )
    ]
  }

  dose_list$scenario_dt <- rbind(dose_list$scenario_dt, tmp_row)


  return(dose_list)
}


################################################################################


#' @description Allocates boosters on the flush day of the course.
#' 
#' @param dose_list [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
#' @param scen [character] The scenario we're running. ("Reference_scenario", "Optim_scenario") Will treat all scenarios other than optimal scenario the same as reference scenario.
#' @param loc_id [integer]
#' @param day [Date] The day we're on.
#' @param r_grp [character] The risk group we're administering for. ("hr", "lr") Will treat all risk groups other than high-risk as low-risk.
#' @param course [integer] The course we're administering. (course 2 is the first booster)
#' @param course_period [character] The phase we're in for this course. ("first_day", "pre_flush", "flush_day", "flush", "post_flush")
#' @param tmp_row [data.table] The row n days before in the previous course so that today's row may be calculated.
#' @param shots [numeric] The total number of shots given across all brands n days before in the previous course, or for all days within the period n days before until first day if this is the first day.
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param longrange_avg [list(character:numeric)] List of averages for each brand in this location, used to estimate how many boosters will actually get administered.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param period_last_day [Date] The last day of this period for this course.
#' @param catchup [integer] (May actually be numeric.) The number of days to divide the remaining dose by during the flush period.
#' 
#' @return [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] (dose_list) Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
.allocate_flush_day <- function(
  dose_list,
  scen,
  loc_id,
  day,
  r_grp,
  course,
  course_period,
  tmp_row,
  shots,
  Fraction_Agree_low,
  longrange_avg,
  brand_cols,
  period_last_day,
  catchup
) {
  if (scen == "Optim_scenario") {
    group_carry <- NA
    if (r_grp == "hr") {
      group_carry <- dose_list$carry_hr
    } else { # All other scenarios will be treated as low-risk "lr".
      # } else if (r_grp == "lr") {
      group_carry <- dose_list$carry_lr
    }

    tmp_row[
      ,
      names(tmp_row[, ..brand_cols]) :=
        as.list(longrange_avg * (shots + group_carry / catchup))
    ]
  } else { # All other scenarios will be treated as reference scenario.
  # } else if (scen == "Reference_scenario") {
    if (r_grp == "hr") {
      tmp_row[
        ,
        names(tmp_row[, ..brand_cols]) :=
          as.list(longrange_avg * Fraction_Agree_low * shots)
      ]

      dose_list$carry_hr <- dose_list$carry_hr + (1 - Fraction_Agree_low) * shots
    } else { # All other scenarios will be treated as low-risk "lr".
      # } else if (r_grp == "lr") {
      shots <- Fraction_Agree_low * shots

      if (Fraction_Agree_low * shots > dose_list$effective_rem) {
        
        shots <- dose_list$effective_rem / Fraction_Agree_low
      }

      tmp_row[
        ,
        names(tmp_row[, ..brand_cols]) := as.list(longrange_avg * shots)
      ]

      dose_list$carry_lr <- dose_list$carry_lr + (1 - Fraction_Agree_low) * shots
      dose_list$effective_rem <- dose_list$effective_rem - Fraction_Agree_low * shots
    }
  }

  dose_list$scenario_dt <- rbind(dose_list$scenario_dt, tmp_row)


  return(dose_list)
}


################################################################################



#' 
#' @param dose_list [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
#' @param scen [character] The scenario we're running. ("Reference_scenario", "Optim_scenario") Will treat all scenarios other than optimal scenario the same as reference scenario.
#' @param loc_id [integer]
#' @param day [Date] The day we're on.
#' @param r_grp [character] The risk group we're administering for. ("hr", "lr") Will treat all risk groups other than high-risk as low-risk.
#' @param course [integer] The course we're administering. (course 2 is the first booster)
#' @param course_period [character] The phase we're in for this course. ("first_day", "pre_flush", "flush_day", "flush", "post_flush")
#' @param tmp_row [data.table] The row n days before in the previous course so that today's row may be calculated.
#' @param shots [numeric] The total number of shots given across all brands n days before in the previous course, or for all days within the period n days before until first day if this is the first day.
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param longrange_avg [list(character:numeric)] List of averages for each brand in this location, used to estimate how many boosters will actually get administered.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param period_last_day [Date] The last day of this period for this course.
#' @param catchup [integer] (May actually be numeric.) The number of days to divide the remaining dose by during the flush period.
#' 
#' @return [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] (dose_list) Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
.allocate_flush <- function(
  dose_list,
  scen,
  loc_id,
  day,
  r_grp,
  course,
  course_period,
  tmp_row,
  shots,
  Fraction_Agree_low,
  longrange_avg,
  brand_cols,
  period_last_day,
  catchup
) {
  if (scen == "Optim_scenario") {
    group_carry <- NA
    if (r_grp == "hr") {
      group_carry <- dose_list$carry_hr
    } else { # All other scenarios will be treated as low-risk "lr".
      # } else if (r_grp == "lr") {
      group_carry <- dose_list$carry_lr
    }

    for (col_name in brand_cols) {
      tmp_row[
        ,
        c(col_name) :=
          longrange_avg[[col_name]] * (shots + group_carry / catchup)
      ]
    }
  } else { # All other scenarios will be treated as reference scenario.
    # } else if (scen == "Reference_scenario") {
    if (r_grp == "hr") {
      for (col_name in brand_cols) {
        tmp_row[
          ,
          c(col_name) :=
            longrange_avg[[col_name]] * Fraction_Agree_low * shots
        ]
      }
    } else { # All other scenarios will be treated as low-risk "lr".
      # } else if (r_grp == "lr") {
      lr_list <- .do_low_risk_reference(
        tmp_row = tmp_row,
        effective_rem = dose_list$effective_rem,
        carry_lr = dose_list$carry_lr,
        shots = shots,
        brand_cols = brand_cols,
        Fraction_Agree_low = Fraction_Agree_low,
        longrange_avg = longrange_avg,
        shots_day1 = dose_list$shots_day1,
        period_last_day = period_last_day
      )

      tmp_row <- lr_list$tmp_row
      dose_list$carry_lr <- lr_list$carry_lr
      dose_list$effective_rem <- lr_list$effective_rem
    }
  }

  dose_list$scenario_dt <- rbind(dose_list$scenario_dt, tmp_row)


  return(dose_list)
}


################################################################################


#' @description Allocates boosters after the flush period of the course.
#' 
#' @param dose_list [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
#' @param scen [character] The scenario we're running. ("Reference_scenario", "Optim_scenario") Will treat all scenarios other than optimal scenario the same as reference scenario.
#' @param loc_id [integer]
#' @param day [Date] The day we're on.
#' @param r_grp [character] The risk group we're administering for. ("hr", "lr") Will treat all risk groups other than high-risk as low-risk.
#' @param course [integer] The course we're administering. (course 2 is the first booster)
#' @param course_period [character] The phase we're in for this course. ("first_day", "pre_flush", "flush_day", "flush", "post_flush")
#' @param tmp_row [data.table] The row n days before in the previous course so that today's row may be calculated.
#' @param shots [numeric] The total number of shots given across all brands n days before in the previous course, or for all days within the period n days before until first day if this is the first day.
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param longrange_avg [list(character:numeric)] List of averages for each brand in this location, used to estimate how many boosters will actually get administered.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param period_last_day [Date] The last day of this period for this course.
#' @param catchup [integer] (May actually be numeric.) The number of days to divide the remaining dose by during the flush period.
#' 
#' @return [list(data.table, numeric, numeric, numeric, Date, list(list(character:Date), list(character:numeric), character:integer))] (dose_list) Data table with boosters added and running variables that get updated on a daily or semi-daily basis. These get assigned from the arguments passed to run_course_day then updated before returning.
.allocate_post_flush <- function(
  dose_list,
  scen,
  loc_id,
  day,
  r_grp,
  course,
  course_period,
  tmp_row,
  shots,
  Fraction_Agree_low,
  longrange_avg,
  brand_cols,
  period_last_day,
  catchup
) {
  if (scen == "Optim_scenario") {
    for (col_name in brand_cols) {
      tmp_row[, c(col_name) := longrange_avg[[col_name]] * shots]
    }
  } else { # All other scenarios will be treated as reference scenario.
    # } else if (scen == "Reference_scenario") {
    if (r_grp == "hr") {
      for (col_name in brand_cols) {
        tmp_row[
          ,
          c(col_name) :=
            longrange_avg[[col_name]] * Fraction_Agree_low * shots
        ]
      }

      dose_list$carry_hr <- dose_list$carry_hr + (1 - Fraction_Agree_low) * sum(shots)
    } else { # All other scenarios will be treated as low-risk "lr".
      # } else if (r_grp == "lr") {
      lr_list <- .do_low_risk_reference(
        tmp_row = tmp_row,
        effective_rem = dose_list$effective_rem,
        carry_lr = dose_list$carry_lr,
        shots = shots,
        brand_cols = brand_cols,
        Fraction_Agree_low = Fraction_Agree_low,
        longrange_avg = longrange_avg,
        shots_day1 = dose_list$shots_day1,
        period_last_day = period_last_day
      )

      tmp_row <- lr_list$tmp_row
      dose_list$carry_lr <- lr_list$carry_lr
      dose_list$effective_rem <- lr_list$effective_rem
    }
  }

  dose_list$scenario_dt <- rbind(dose_list$scenario_dt, tmp_row)


  return(dose_list)
}


################################################################################


#' @description Allocates boosters and updates counts for low-risk group during multi-day periods.
#' 
#' @param tmp_row [data.table] The row n days before in the previous course so that today's row may be calculated.
#' @param effective_rem [numeric] Number of people who would get a shot.
#' @param carry_lr [numeric] Pool of low-risk people remaining who would but have not yet.
#' @param shots [numeric] The total number of shots given across all brands n days before in the previous course, or for all days within the period n days before until first day if this is the first day.
#' @param brand_cols [vector(character)] Names of columns for course 1 for each brand.
#' @param Fraction_Agree_low [numeric] The fraction of the willing and able population who will actually get a shot. Should be 0<=n<=1. (Currently set to 0.8.)
#' @param longrange_avg [list(character:numeric)] List of averages for each brand in this location, used to estimate how many boosters will actually get administered.
#' @param shots_day1 [Date] The first day of this period for this course.
#' @param period_last_day [Date] The last day of this period for this course.
#' 
#' @return [list(data.table, numeric, numeric)] The updated row and counts of remaining doses/people.
.do_low_risk_reference <- function(
  tmp_row,
  effective_rem,
  carry_lr,
  shots,
  brand_cols,
  Fraction_Agree_low,
  longrange_avg,
  shots_day1,
  period_last_day
) {
  
  lr_return_list <- list(
    tmp_row = copy(tmp_row),
    carry_lr = carry_lr,
    effective_rem = effective_rem
  )


  if (effective_rem == 0) {
    # No doses left. Nobody gets vaccinated.
    lr_return_list$carry_lr <- lr_return_list$carry_lr + sum(shots)

    lr_return_list$tmp_row[, c(brand_cols) := 0]
  } else if ((sum(shots) * Fraction_Agree_low) <= effective_rem) {
    # There are enough doses for those who want to be vaxxed.

    for (col_name in brand_cols) { 
      lr_return_list$tmp_row[
        ,
        c(col_name) := longrange_avg[[col_name]] * (Fraction_Agree_low * shots)
      ]
    }

    effective_rem <- effective_rem - (sum(shots) * Fraction_Agree_low)
  } else if (Fraction_Agree_low * shots_day1 > effective_rem) {
    # The estimated number of vaccinated on day one is already greater than the number who get vaccinated.
    # Just use the long-range average.
    lr_return_list$tmp_row[ 
      ,
      names(lr_return_list$tmp_row[, ..brand_cols]) :=
        as.list(longrange_avg * effective_rem)
    ]

    if (day == period_last_day) { # Last day of period. Don't leave any doses.
      effective_rem <- 0
    } else {
      effective_rem <- sum(lr_return_list$tmp_row)
    }
  } else {
    # There are not enough doses to cover the number who are willing and able to get vaccinated?
    # At some point the number of estimated vaccinated today exceeds the number of doses?
    # print("No more.")

    left <- effective_rem - Fraction_Agree_low * shots

    lr_return_list$tmp_row[
      ,
      names(lr_return_list$tmp_row[, ..brand_cols]) :=
        as.list(longrange_avg * left)
    ]

    lr_return_list$carry_lr <-
      lr_return_list$carry_lr + (sum(shots) - sum(lr_return_list$tmp_row[, ..brand_cols]))

    effective_rem <- 0
  }


  return(lr_return_list)
}


