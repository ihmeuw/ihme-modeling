
#' Generic scatterplot function
#'
#' @param DATASET [data.table] dataset unique by location and 6-date subset, wide by ref & new measure columns
#' @param PERCENT_DIFF [numeric] what % difference to label points that differ from ref to new
#' @param VARIABLE [character] variable column name to plot
#' @param EXTRA_LABEL [character] user-controlled extra plot title label
#'
#' @return prints a ggplot
.scatter_plot_compare <- function(DATASET, PERCENT_DIFF, VARIABLE, EXTRA_LABEL = NULL) {
  
  library(stringr)
  
  NEW_VAR <- paste0(VARIABLE, "_new")
  if(any(grepl(pattern = "_ref$", names(DATASET)))){
    REF_VAR <- paste0(VARIABLE, "_ref")
  } else if (any(grepl(pattern = "_old$", names(DATASET)))) {
    REF_VAR <- paste0(VARIABLE, "_old")
  }
  
  if (all(DATASET[,..REF_VAR]==0)) {
    
    p <- DATASET %>% ggplot() +
      annotate("text", 
               x=1, 
               y=1,
               label=paste0("This column does not exist in reference data : ",
                            VARIABLE),
               color="red")
    
    print(p)
    
  } else {
    
    p <- DATASET %>% 
      ggplot(aes(x = get(REF_VAR), y = get(NEW_VAR))) + 
      geom_point() + 
      facet_wrap(~date) +
      geom_abline(intercept=0, slope=1, col="red") + 
      scale_x_log10("Reference model - Last Production", limits = c(1, NA)) +
      scale_y_log10("New model", limits = c(1, NA)) + 
      geom_text_repel(data = DATASET[abs((get(NEW_VAR) - get(REF_VAR))/get(REF_VAR)) > PERCENT_DIFF], 
                      aes(label = paste(location_id, ":", location_name)),
                      max.overlaps = 50,
                      force = 50) +
      
      ggtitle(paste0(str_extract(NEW_VAR, "^.*_"), " : Locations with > ", PERCENT_DIFF*100, "% change", "    ", EXTRA_LABEL),
              "Y-axis 0s are missing in reference data, X-axis 0s are missing in current data") +
      theme_bw()  
    
    print(p)
  }
}


#' Prepare processed observed data for vaccine scatterplots
#' 
#' Compares current processed observed data with the previous best version
#' Combines two data tables into one, replaces NAs with 0s, pivots wide for plotting
#'
#' @param vaccine_output_root versioned vaccine-coverage output folder path 
#'
#' @return [data.table] Unique by location and date, wide by measure and old/new data
.scatter_prep_observed <- function (vaccine_output_root) {
  
  # Setup 
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  previous_best_root <- model_parameters$previous_best_path
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  filename <- "observed_data.csv"
  
  # load data 
  observed_new <- fread(file.path(vaccine_output_root, filename))
  observed_old <- fread(file.path(previous_best_root, filename))
  # order data
  observed_new <- observed_new[with(observed_new, order(location_id, date)), ]
  observed_old <- observed_old[with(observed_old, order(location_id, date)), ]
  # add versions
  observed_new[, version := "new"]
  observed_old[, version := "ref"]

  # Six dates to check 
  dates <- 
    seq(
      min(observed_old$date, na.rm = T), 
      max(observed_old$date, na.rm=T),
      1
    )
  
  # Calculate 7 evenly spaced days, skip the first (too early)
  intervals <- round(length(dates)/7) * 1:7
  # Setting last scatter earlier to avoid messy labels - vaccine reporting delay can be long
  intervals[7] <- intervals[7] - 21 
  dates_plot <- as.Date(
    as.integer(dates)[1] + intervals[2:7]
  )
  
  observed_plot <- data.table(
    plyr::rbind.fill(
      observed_new,
      observed_old
    ))

  # Keep plot cols
  keep_cols <- c("date", "location_id", "reported_vaccinations", "fully_vaccinated", 
                 "people_vaccinated", "boosters_administered", "booster_1", "booster_2",
                 "version")
  plot_cols <- c("reported_vaccinations", "fully_vaccinated", 
                 "people_vaccinated", "boosters_administered", "booster_1", "booster_2")
  observed_plot <- observed_plot[ , ..keep_cols]
  observed_plot <- observed_plot[date %in% dates_plot]
  # get names
  observed_plot <- merge(
    observed_plot,
    hierarchy[,.(location_id, location_name)],
    by = "location_id",
    all.x = T
  )
  
  # pivot wide
  observed_plot <- observed_plot %>% 
    tidyr::pivot_wider(names_from = version, values_from = all_of(plot_cols)) %>% 
    as.data.table()
  
  message("Prepping observed vaccine scatters data: \n Setting all NA values = 0")
  fill_cols <- c(
    paste0(plot_cols, "_new"),
    paste0(plot_cols, "_ref")
    )
  observed_plot[, (fill_cols) := lapply(.SD, nafill, fill = 0), .SDcols = fill_cols]
  
  return(observed_plot)
  
}


#' Prepare booster point estimate for scatterplots
#'
#' @param vaccine_output_root [character] versioned vaccine-coverage output folder path
#'
#' @return [data.table] data long by location, wide by booster course and ref/new dataset
.scatter_prep_booster_point <- function (vaccine_output_root) {
  
  # Setup 
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  previous_best_root <- model_parameters$previous_best_path
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  filename <- "boosters_point_estimate.csv"
  
  # load data 
  new <- fread(file.path(vaccine_output_root, filename))
  ref <- fread(file.path(previous_best_root, filename))
  # order data
  new <- new[with(new, order(location_id, date)), ]
  ref <- ref[with(ref, order(location_id, date)), ]
  
  if(!"booster_course" %in% names(ref)) setnames(ref, "variable", "booster_course")
  
  # add versions
  new[, version := "new"]
  ref[, version := "ref"]
  
  # Keep & plot cols
  keep_cols <- c("location_id", "booster_course", "value", 
                 "version")
  plot_cols <- c("value")
  
  dt_plot <- data.table(
    plyr::rbind.fill(
      new,
      ref
    ))
  
  dt_plot <- dt_plot[ , ..keep_cols]
  # get names
  dt_plot <- merge(
    dt_plot,
    hierarchy[,.(location_id, location_name)],
    by = "location_id",
    all.x = T
  )
  
  # pivot wide - this needs date absent
  dt_plot <- dt_plot %>% 
    tidyr::pivot_wider(names_from = c("booster_course", "version"), 
                       values_from = all_of(plot_cols)) %>% 
    as.data.table()
  
  dt_plot[, date := Sys.Date()] # plotting function needs a date column
  
  message("Prepping booster point scatter data: \n Setting all NA values = 0")
  fill_cols <- grep("booster", names(dt_plot), value = T)
  dt_plot[, (fill_cols) := lapply(.SD, nafill, fill = 0), .SDcols = fill_cols]
  
  return(dt_plot)
  
}


#' Prepare last shots in arms data for scatter plots
#'
#' Calculates daily values by location and day, collapsing vaccine course and
#' risk groups for gross summary measures
#'
#' Calculates cumulative values by location, using same collapse scheme as daily
#' values
#'
#' @param vaccine_output_root [character] versioned vaccine-coverage output
#'   folder path
#'
#' @return [list] list of data tables, one for each scenario, long by location
#'   and date, wide by old/new data and summary measure
.scatter_prep_last_shots <- function(vaccine_output_root) {
  
  # Setup
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  previous_best_root <- model_parameters$previous_best_path
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  suffixes <- c("_reference.csv", "_optimal.csv")
  filenames <- paste0("last_shots_in_arm_by_brand_w_booster", suffixes)
  
  # load data
  new_ref <- fread(file.path(vaccine_output_root, grep("reference", filenames, value = T)))
  old_ref <- fread(file.path(previous_best_root, grep("reference", filenames, value = T)))
  new_opt <- fread(file.path(vaccine_output_root, grep("optimal", filenames, value = T)))
  old_opt <- fread(file.path(previous_best_root, grep("optimal", filenames, value = T)))
  
  # Six dates to check 
  last_prod_date <- stringr::str_extract(.previous_best_version, "\\d\\d\\d\\d_\\d\\d_\\d\\d")
  last_prod_date <- as.Date(stringr::str_replace_all(last_prod_date, "_", "-"))
  dates <- 
    seq(
      min(old_ref$date, na.rm = T), 
      last_prod_date,
      1
    )
  
  # Calculate 7 evenly spaced days, skip the first (too early)
  intervals <- round(length(dates)/7) * 1:7
  dates_plot <- as.Date(
    as.integer(dates)[1] + intervals[2:7]
  )
  
  dt_list <- list(new_ref = new_ref, 
                   old_ref = old_ref, 
                   new_opt = new_opt, 
                   old_opt = old_opt)
  
  for (dt in names(dt_list)) {
    
    tryCatch(
      {
        # remove index column
        if("V1" %in% names(dt_list[[dt]])) dt_list[[dt]][, V1 := NULL]
        # build total daily and cumulative shots in arms
        dt_list[[dt]] <- 
          rbindlist(
            lapply(
              split(dt_list[[dt]], f=list(dt_list[[dt]]$location_id, dt_list[[dt]]$vaccine_course)),
              
              # trycatch(
              tryCatch(
                {
                  
                  function(x) {
                    x$daily <- rowSums(x[,-c(1:4)])
                    x$cumul <- cumsum(x$daily)
                    return(x)
                  }
                  
                },
                error = function(e) {
                  cat(paste0("Last shots in arms location error", 
                             unique(x$location_id), " : ", 
                             conditionMessage(e), "\n"))
                }
              )
            )
          )
        
        # keep plotting columns
        dt_list[[dt]] <- dt_list[[dt]][, .(location_id, date, vaccine_course, risk_group, daily, cumul)]
        dt_list[[dt]][, version := stringr::str_extract(dt, "^\\w\\w\\w")]
        
        # Which most detailed have non-zero vaccines delivered & projected?
          # - This is already validated with .validate_last_shots() in utils.R
        
        # Select 6 dates to check
        dt_list[[dt]] <- dt_list[[dt]][date %in% dates_plot]
        
      },
      error = function(e) {
        cat(paste0("Error preparing scatters for last shots in arms : ", 
                   dt, " : ", 
                   conditionMessage(e), "\n"))
      }
    )
  }
  
  # Combine old/new into list of plotting data tables
  plot_list <- list(
    ref = rbindlist(dt_list[grep(names(dt_list), pattern = "ref")]),
    opt = rbindlist(dt_list[grep(names(dt_list), pattern = "opt")])
  )
  
  for (dt in names(plot_list)) {
    
    tryCatch(
      {
        
        # summarize
        plot_list[[dt]] <- 
          plot_list[[dt]][ , .(daily = sum(daily), cumul = sum(cumul)), by = .(location_id, date, version)] 
        # get names
        plot_list[[dt]] <- merge(plot_list[[dt]], 
                                 hierarchy[,.(location_id, location_name)], 
                                 by = "location_id", 
                                 all.x=T)
        
        plot_list[[dt]] <- 
          plot_list[[dt]] %>% 
          tidyr::pivot_wider(names_from = version, values_from = c("daily", "cumul")) %>% 
          as.data.table()
        
      },
      
      error = function(e) {
        cat(paste0("Error preparing scatters for last shots in arms : ", 
                   dt, " : ", 
                   conditionMessage(e), "\n"))
        
      }
    )
  }
  
  # Extra plotting labels
  plot_list$ref$scenario <- "Reference scenario"
  plot_list$opt$scenario <- "Optimal scenario"
  
  return(plot_list)
  
}
