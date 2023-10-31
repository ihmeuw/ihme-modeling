# This script should contain functions that do post-run checks on all vaccine model outputs that are used downstream
# Useful types of checks:
# Confirm that values are within bounds and are not missing/ have NAs
# Dates formatted
# Strive to collate checks in some readable format
# possibly auto-generate plots or quickly readable table in .output_path

#-------------------------------------------------------------------------------
# Systematic checks for observed data (prior to modeling)

.check_observed_vaccinations <- function(version_output_path) {

  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  observed <- vaccine_data$load_observed_vaccinations(version_output_path)

  message(paste('Max date of observed vaccination coverage data:', max(observed$date[observed$observed == 1], na.rm=T)))

  observed <- merge(observed[,location_name := NULL], hierarchy, by='location_id', all.y=T)
  sel <- which(observed$fully_vaccinated > observed$people_vaccinated)

  test <- observed[sel,]

  unique(test$location_name)
  hierarchy[location_id %in% unique(test$location_id),]
}


#-------------------------------------------------------------------------------
# Check vaccine hesitancy values within admissible range

.check_hesitancy_model_output <- function(version_output_path) {

  hesitancy_model_output <- vaccine_data$load_time_series_vaccine_hesitancy(version_output_path, scenario = 'default')
  suspects <- c("smooth_combined_yes", "smooth_survey_yes", "pct_vaccinated")

  date_seq <- seq.Date(as.Date(min(hesitancy_model_output$date, na.rm=T)),
                       as.Date(max(hesitancy_model_output$date, na.rm=T)),
                       by=1)

  all_dates <- do.call(
    rbind,
    lapply(split(hesitancy_model_output, by='location_id'), function(x) {

      tryCatch( {

        # Check for skipped dates
        all_good <- all(date_seq %in% x$date)
        if (!all_good) stop(paste('Dates missing in', x$location_name[1]))

      }, error=function(e){

        cat("Warning :", x$location_id[1], ":", conditionMessage(e), "\n")

      })

      return(all_good)
    })
  )

  dups <- duplicated(hesitancy_model_output, by=c("location_id","date"))
  hesitancy_model_output <- as.data.frame(hesitancy_model_output)

  out <- data.frame()
  for (i in suspects) {

    x <- hesitancy_model_output[,i]

    out <- rbind(out, data.frame(
      variable=i,
      above_0=all(x[!is.na(x)] >= 0),
      below_1=all(x[!is.na(x)] <= 1),
      no_NA=all(!any(is.na(x))),
      all_dates=all(all_dates),
      no_duplicates=ifelse(sum(dups) > 0, FALSE, TRUE)
    ))

  }

  tmp <- as.integer(out[out$variable == 'smooth_combined_yes',-1])
  if (!all(tmp)) stop('smooth_combined_yes is out of bounds or has missing values')

  return(out)
}


#-------------------------------------------------------------------------------
# Check vaccine projections (all_vaccinated, fully_vaccinated, effectively_vaccinated)
# Much more needed here but just a couple check for NAs now

.check_vaccine_projections <- function(version_output_path) {

  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  vaccine_model_output <- vaccine_data$load_scenario_forecast(version_output_path, scenario = 'slow')
  vaccine_model_output <- vaccine_model_output[vaccine_model_output$location_id %in% hierarchy$location_id,]

  suspects <- c("cumulative_all_vaccinated", "cumulative_all_fully_vaccinated", "cumulative_all_effective")

  tmp <- data.frame()
  for (i in suspects) {
    tmp <- rbind(tmp, data.frame(variable=i, missing=sum(is.na(vaccine_model_output[,..i]))))
  }

  message('Counting NAs in vaccine projections (COVID hierarchy only)')
  print(tmp)

}

.check_time_point_vax_willing_USA <- function(version_output_path,
                                              vaccine_model_output=NULL,
                                              hierarchy=NULL
                                              ) {

  if (is.null(hierarchy)) hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  if (is.null(vaccine_model_output)) vaccine_model_output <- vaccine_data$load_scenario_forecast(version_output_path, scenario = 'slow')

  hes <- vaccine_data$load_hestancy_model_ouput(version_output_path)

  cols <- c("cumulative_all_vaccinated", "cumulative_all_fully_vaccinated", "cumulative_all_effective")

  out <- data.frame()
  
  for (i in c(102, hierarchy[parent_id == 102]$location_id)) {

    tmp_hes <- hes[location_id == i,]
    tmp_hes <- tmp_hes[which.max(tmp_hes$date),]

    tmp <- vaccine_model_output[location_id == i]
    tmp <- tmp[tmp$date == Sys.Date(), ]
    tmp2 <- tmp[, ..cols]/tmp$adult_population

    tmp <- cbind(tmp[,.(location_id, location_name, date)], tmp2)

    tmp <- cbind(tmp, data.table(vax_and_willing = tmp_hes$smooth_combined_yes,
                                 max_date_vax_willing = tmp_hes$date))

    tmp$implausible_vax_willing <- tmp$vax_and_willing <= tmp$cumulative_all_effective

    out <- rbind(out, tmp)

  }

  fp <- file.path(version_output_path, "time_point_vax_hesitancy_USA.csv")
  write.csv(out, file = fp)
  message(glue("Latest time points of vaccinated and willing in USA saved to {fp}"))

}

