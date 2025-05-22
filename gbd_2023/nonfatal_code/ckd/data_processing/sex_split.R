#' Title: Sex Splitting
#' Date: 03/22/2019, last modified May 2024
#'
#'
#' @param topic_name string, for output file names
#' @param output_dir string, filepath to directory to save outputs (including MR-BRT model, post sex split data, vetting plots, etc). This should end with a "/"
#' @param bundle_version_id only input if pulling bundle version to get sex-specific data for sex splitting model, and to get data that needs to be sex split (must have 'splitting' variable)
#' @param data_all_csv string, if pulling in all the data (sex-specific and both sex) in a csv format (must have 'splitting' variable)
#' @param data_to_split_csv string, if pulling data that needs to be sex split via csv
#' @param data_raw_sex_specific_csv string, if pulling sex-specific data for sex ratio model via csv
#' @param nids_to_drop numeric vector, list of NIDs which the user does want used to inform the sex split model.
#' @param cv_drop string, study-level covariates in sex-specific dataset that user does not want to use to find sex matches between observations. Covariates must be of the form "cv_xxx"
#' @param mrbrt_model string, identifies filepath of previously run MR-BRT sex ratio model. If null, a new model will be run
#' @param mrbrt_model_age_cv TRUE if user is running a new MR-BRT model with age covariate, default is FALSE
#' @param mrbrt_model_linear_age_cv TRUE if user is running a new MR-BRT model with age covariate and would like that age covariate to be linear, default is TRUE
#' @param mrbrt_model_age_spline_knots numeric vector specifying the location (and therefore number) of knots for the nonlinear MR-BRT model age covariate, default is c(0, 0.25, 0.5, 0.75, 1)
#' @param mrbrt_model_age_spline_degree numeric, specifying the degree for the MR-BRT model age covariate (1 = linear, 2 = quadratic, 3 = cubic), default is 3
#' @param mrbrt_model_age_spline_linear_tail_left TRUE if user would like the MR-BRT model age covariate to have a left linear tail, default is TRUE
#' @param mrbrt_model_age_spline_linear_tail_right TRUE if user would like the MR-BRT model age covariate to have a right linear tail, default is TRUE
#' @param release_id Release ID, for pulling in population data. No default
#' @param measure Character vector of measures that user wants to sex-split (prevalence, incidence, proportion, and/or continuous)
#' @param vetting_plots logical, Binary variable for if user wants vetting plots generated or not, default is true
#'
#' @return
#' @export
#'
#' @examples
sex_split <- function(topic_name, output_dir, bundle_version_id = NULL, data_all_csv = NULL, data_to_split_csv = NULL,
                      data_raw_sex_specific_csv = NULL, nids_to_drop = NULL, cv_drop = NULL, mrbrt_model = NULL,
                      mrbrt_model_age_cv = FALSE, mrbrt_model_linear_age_cv = TRUE, mrbrt_model_age_spline_knots = c(0, 0.25, 0.5, 0.75, 1),
                      mrbrt_model_age_spline_degree = 3, mrbrt_model_age_spline_linear_tail_left = TRUE, mrbrt_model_age_spline_linear_tail_right = TRUE,
                      release_id = NULL, measure = NULL, vetting_plots = TRUE) {

  # Initialize environment #####################################################
  message("Loading functions and metadata")
  date <- strtrim(gsub(":", "_", gsub(" ", "__", gsub("-", "_", Sys.time()))), 17)
  if (Sys.info()["sysname"] == "Linux") {
    ihme <- "FILEPATH"
  } else {
    stop("Please run this function on the cluster")
  }

  # Set Objects ################################################################
  ## Shared function objects
  functions_dir <- paste0(ihme, "FILEPATH")

  draws <- paste0("draw_", 0:999)
  draws2 <- paste0("mrbrt_draw_", 0:999)

  # Loading packages ###########################################################
  pacman::p_load(data.table, openxlsx, ggplot2, msm, Hmisc, dplyr, tidyverse)


  # Source shared functions ####################################################
  ## Standard shared functions
  shared_functs <- c("get_location_metadata",
                     "get_age_metadata",
                     "get_bundle_version",
                     "get_population",
                     "get_ids")
  invisible(lapply(shared_functs, function(x) source(paste0(functions_dir, x, ".R"))))
  ## 2020 MR-BRT functions:
  library(mrbrt002)

  source("FILEPATH/ihme-ggplot-themes.R")


  # User defined functions #####################################################
  find_sex_match <- function(dt, cv_drop){
    sex_dt <- copy(dt)
    sex_dt <- sex_dt[sex %in% c("Male", "Female")]
    match_vars <- c("nid", "age_start", "age_end", "location_id", "measure", "year_start", "year_end",
                    names(sex_dt)[grepl("^cv_", names(sex_dt)) & !names(sex_dt) %in% cv_drop])
    sex_dt[, match_n := .N, by = match_vars]
    sex_dt <- sex_dt[match_n == 2]
    keep_vars <- c(match_vars, "sex", "mean", "standard_error")
    sex_dt[, id := .GRP, by = match_vars]
    sex_dt <- dplyr::select(sex_dt, keep_vars)
    sex_dt <- data.table::dcast(sex_dt, ... ~ sex, value.var = c("mean", "standard_error"), fun.aggregate=mean)
    sex_dt <- sex_dt[!mean_Female == 0 & !mean_Male == 0]
    sex_dt[, id := .GRP, by = c("nid", "location_id")]
    return(sex_dt)
  }

  calc_sex_ratios <- function(dt){
    ratio_dt <- copy(dt)
    ratio_dt[, midage := (age_start + age_end)/2]
    ratio_dt[, `:=` (ratio = mean_Female/mean_Male,
                     ratio_se = sqrt((mean_Female^2 / mean_Male^2) * (standard_error_Female^2/mean_Female^2 + standard_error_Male^2/mean_Male^2)))]
    ratio_dt[, log_ratio := log(ratio)]
    ratio_dt$log_se <- sapply(1:nrow(ratio_dt), function(i) {
      mean_i <- ratio_dt[i, "ratio"]
      se_i <- ratio_dt[i, "ratio_se"]
      deltamethod(~log(x1), mean_i, se_i^2)
    })
    return(ratio_dt)
  }


  graph_predictions <- function(dt, plot_measure){
    for(i in plot_measure){
      graph_dt <- copy(dt[measure == i, .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
      graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
      graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
      graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
      graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
      setnames(graph_dt_error, "value", "error")
      graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
      graph_dt[, N := (mean*(1-mean)/error^2)]
      graph_dt <- subset(graph_dt, value > 0)
      wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
      graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
      graph_dt[, midage := (age_end + age_start)/2]
      ages <- c(60, 70, 80, 90)
      graph_dt[, age_group := cut2(midage, ages)]
      pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_graph_predictions_", i, ".pdf"))
      gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
        geom_point() +
        geom_errorbar(aes(ymin = lower, ymax = upper)) +
        labs(x = "Both Sex Mean", y = " Sex Split Means") +
        geom_abline() +
        ggtitle("Sex Split Means Compared to Both Sex Mean (With CI)") +
        scale_color_manual(name = "Sex", values = c("Male" = "#0095ff", "Female" = "#ff0004")) +
        theme_ihme_light()
      print(gg_sex)
      gg_sex_no_se <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
        geom_point() +
        labs(x = "Both Sex Mean",  y = " Sex Split Means") +
        geom_abline() +
        ggtitle("Sex Split Means Compared to Both Sex Mean (No CI)") +
        scale_color_manual(name = "Sex", values = c("Male" = "#0095ff", "Female" = "#ff0004")) +
        theme_ihme_light()
      print(gg_sex_no_se)
      dev.off()
    }
  }

  diff_plot_ratio <- function(data){
    data[, dm := standard_error/parent_standard_error]
    pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_parent_vs_child_standard_error_differences_ratio.pdf"))
    m <- ggplot(data = data[sex == "Male", ], aes(dm)) +
      geom_histogram(bins=30, linetype = 0) +
      labs(
        x = "Male child Standard Error over Both sex standard error",
        y = "Count",
        title = "Ratio of Male Child to Parent Observations Standard Errors"
      ) +
      theme_ihme_light()
    f <- ggplot(data = data[sex == "Female", ], aes(dm)) +
      geom_histogram(bins=30, linetype = 0) +
      labs(
        x = "Female child Standard Error over Both sex standard error",
        y = "Count",
        title = "Ratio of Female Child to Parent Observations Standard Errors"
      ) +
      theme_ihme_light()
    print(m)
    print(f)
    dev.off()
  }

  diff_plot_subtract <- function(data){
    data[, dm := standard_error-parent_standard_error]
    pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_parent_vs_child_standard_error_differences_subtract.pdf"))
    m <- ggplot(data = data[sex == "Male", ], aes(dm)) +
      geom_histogram(bins=30, linetype = 0) +
      labs(
        x = "Male child Standard Error minus Both sex standard error",
        y = "Count",
        title = "Difference between Male Child and Parent Observations Standard Errors"
      ) +
      theme_ihme_light()
    f <- ggplot(data = data[sex == "Female", ], aes(dm)) +
      geom_histogram(bins=30, linetype = 0) +
      labs(
        x = "Female child Standard Error minus Both sex standard error",
        y = "Count",
        title = "Difference between Female Child and Parent Observations Standard Errors"
      ) +
      theme_ihme_light()
    print(m)
    print(f)
    dev.off()
  }

  sex_specific_sex_split_plot <- function(data, measure, x_axis) {
    if (x_axis == "age") {
      for (i in measure) {
        data2 <- data[data$measure == i, ]
        plot_measure <- unique(data2$measure)
        pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_sex_specific_and_sex_split_over_age_", plot_measure, ".pdf"))
        for (x in unique(data2$region_name)) {
          temp <- data2[data2$region_name == x, ]

          if (nrow(temp) != 0) {
            region_name <- unique(temp$region_name)
            plot <- ggplot(data = temp, aes(group = description, color = description)) +
              geom_segment(aes(x = age_start, xend = age_end, y = mean, yend = mean)) +
              xlab("Age") +
              ylab(paste0("Mean ", sex_split_measure)) +
              ggtitle(paste0("Sex-Specific vs. Sex-Split ", sex_split_measure, " data: ", region_name)) +
              facet_grid(cols = vars(sex)) +
              theme(legend.title = element_blank()) +
              geom_pointrange(aes(x = midage, y = mean, ymin = mean - standard_error, ymax = mean + standard_error, fatten = 0.5)) +
              theme_ihme_light()
            print(plot)
          } else {
            next
          }
        }
        dev.off()
      }
    }
    if (x_axis == "year") {
      for (i in measure) {
        data2 <- data[data$measure == i, ]
        plot_measure <- unique(data2$measure)
        pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_sex_specific_and_sex_split_over_time_", plot_measure, ".pdf"))
        for (x in unique(data2$region_name)) {
          temp <- data2[data2$region_name == x, ]
          if (nrow(temp) != 0) {
            region_name <- unique(temp$region_name)
            plot <- ggplot(data = temp, aes(group = description, color = description)) +
              geom_segment(aes(x = year_start, xend = year_end, y = mean, yend = mean)) +
              xlab("Year") +
              ylab(paste0("Mean ", sex_split_measure)) +
              ggtitle(paste0("Sex-Specific vs. Sex-Split ", sex_split_measure, " data: ", region_name)) +
              facet_grid(cols = vars(sex)) +
              theme(legend.title = element_blank()) +
              geom_pointrange(aes(x = midyear, y = mean, ymin = mean - standard_error, ymax = mean + standard_error, fatten = 0.5)) +
              theme_ihme_light()
            print(plot)
          } else {
            next
          }
        }
        dev.off()
      }
    }
  }

  plot_both_sex_draws <- function(data) {
    pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_dist_both_sex_mean_draws.pdf"))
    for (i in data$seq) {
      data2 <- data[data$seq == i, ]
      plot_seq <- unique(data2$seq)
      plot_mean <- unique(data2$mean)
      data3 <- subset(data2, select = draws)
      data4 <- pivot_longer(data3, cols = draws)
      plot <- ggplot(data = data4, aes(x=value)) + geom_histogram(bins=30) +
        xlab("Both Sex Mean") + ylab("Count") + ggtitle(paste0("Seq: ", plot_seq)) +
        geom_vline(aes(xintercept=plot_mean), color="blue", linetype="dashed", size=1) +
        geom_vline(aes(xintercept=mean(value)), color="red", linetype="dashed", size=1) +
        labs(caption = "Where blue line is original both sex mean, red line is mean of the both sex mean draws") +
        theme_ihme_light()
      print(plot)
    }
    dev.off()
  }

  plot_input_sex_ratios <- function(data) {
    data[is.na(pred1), type := "Input"]
    data[!is.na(pred1), type := "Output"]
    ylim1 <- boxplot.stats(data[type == "Input"]$ratio)$stats[c(1, 5)]
    if (mrbrt_model_age_cv == FALSE) {
      pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_input_and_output_sex_ratios_boxplot.pdf"), height = 8, width = 12)
      plot <- ggplot(data = data, aes(x = factor(0), y=ratio)) + geom_boxplot(outlier.shape = NA) +
        ylab("Sex Ratio (Female/Male)") + ggtitle("MR-BRT Input & Output Sex Ratios") +
        coord_cartesian(ylim = ylim1*1.05) + labs(caption = "Input outliers not plotted") +
        facet_grid(cols = vars(type)) +
        theme(axis.title.x=element_blank(), axis.text.x=element_blank(), axis.ticks.x=element_blank()) + theme_ihme_light()
      print(plot)
      dev.off()
    }
    if (mrbrt_model_age_cv == TRUE) {
      data[, midage := round(midage/5)*5]
      pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_input_and_output_sex_ratios_boxplot.pdf"), height = 8, width = 12)
      plot <- ggplot(data = data, aes(x = midage, y=ratio, group = midage)) + geom_boxplot(outlier.shape = NA) +
        ylab("Sex Ratio (Female/Male)") + xlab("Mid-age") + ggtitle("MR-BRT Input & Output Sex Ratios by Mid-age") +
        facet_grid(cols = vars(type)) +
        coord_cartesian(ylim = ylim1*1.05) + labs(caption = "Input outliers not plotted, rounded to nearest mid-age of 5 years") + theme_ihme_light()
      print(plot)
      dev.off()
    }
  }


  plot_sex_ratios_age_superregion <- function(data) {
    ylim1 = boxplot.stats(data$ratio)$stats[c(1, 5)]
    data[, midage := round(midage/5)*5]
    pdf(paste0(output_dir2, "sex_split_", date, "_", topic_name, "_visualize_sex_ratios_age_superregion.pdf"))
    plot <- ggplot(data = data, aes(x = midage, y=ratio, group = midage)) + geom_boxplot(outlier.shape = NA) +
      ylab("Sex Ratio (Female/Male)") + xlab("Mid-age") + ggtitle("Sex Ratios by Mid-age and Super Region") +
      coord_cartesian(ylim = ylim1*1.05) + labs(caption = "Outliers not plotted, rounded to nearest mid-age of 5 years") +
      facet_wrap(~super_region_name) + theme_ihme_light() + theme(strip.text = element_text(size = 6))
    print(plot)
    dev.off()
  }

  # Get Metadata #################################################################
  ages <- get_ids("age_group")
  locations <- get_location_metadata(location_set_id = 35, release_id = release_id)
  locations <- subset(locations, select = c(location_id, region_name, super_region_name))

  # Running the Sex Split ########################################################

  message("Reading in datasets and subsetting to specified data")
  ## Pull in csvs OR bundle version OR full csv data and using splitting variables
  if (is.null(bundle_version_id) & is.null(data_all_csv) & !is.null(data_raw_sex_specific_csv) & !is.null(data_to_split_csv)) {
    sex_dt <- as.data.table(read.csv(data_raw_sex_specific_csv))
    split_dt <- as.data.table(read.csv(data_to_split_csv))
    unused_dt <- data.table()
  } else if((!is.null(bundle_version_id) & is.null(data_all_csv)) | (is.null(bundle_version_id) & !is.null(data_all_csv))) {
    if(!is.null(bundle_version_id) & is.null(data_all_csv)){
      bundle <- as.data.table(get_bundle_version(bundle_version_id, fetch="all", export=F))
    } else if(is.null(bundle_version_id) & !is.null(data_all_csv)){
      bundle <- as.data.table(read.csv(data_all_csv))
    }
    unused_dt <- data.table()
    sex_dt <- bundle[splitting %in% c(0, 1)] # Only data that doesn't need any splitting or needs only age-splitting
    split_dt <- bundle[splitting %like% 2, ] #Want to capture rows that are either '2' or '1,2' or '1, 2', etc.
    unused_dt2 <- bundle[!splitting %in% c("0", "1", "2", "1,2", "1, 2"), ] #Data that was not used to inform the sex split, and did not need to be sex split.
    unused_dt2[, drop := "Within study age-sex split data"]
    unused_dt <- rbind(unused_dt, unused_dt2)
    rm(bundle)
  } else { #If none of the correct data inputs are available
    print(paste("Please only select one of the data input methods. Define either data_raw_sex_specific_csv and data_to_split_csv OR bundle_version_id OR data_all_csv "))
  }
  # If the unused dataframe has no observations in it then we need to define it such that the columns are the same as the sex split columns.
  if(nrow(unused_dt) == 0){
    unused_dt <- data.table(data.frame(matrix(nrow = 0, ncol = length(names(sex_dt)))))
    colnames(unused_dt) <- names(sex_dt) #Making sure unused_dt has the same column names as the observations to be sex split
  }

  #Initializing the dataset to hold statements about the number of cases we are dropping.
  vet_dt <- data.table(Statement = character(), Number = integer())
  #tot_rows <- nrow(sex_dt) + nrow(split_dt) + nrow(unused_dt) #Used for validation checks so that no rows are dropped without being documented

  #A variable for the reason why an observation was not used in the sex split, either to inform the split or to be sex split
  unused_dt[, drop := NA]

  #Creating the output directory
  invisible(dir.create(paste0(output_dir, "sex_split_", date)))
  output_dir2 <- paste0(output_dir, "sex_split_", date, "/")

  # Dropping outlier and group_review observations
  if ("group_review" %in% colnames(sex_dt)) {
    sex_dt[is.na(group_review), group_review := 1]
    unused_dt2 <- sex_dt[group_review != 1, ]
    unused_dt2[, drop := "Observations to inform the split tagged as group review"]
    unused_dt <- rbind(unused_dt, unused_dt2)
    vet_dt <- rbind(vet_dt, data.table(Statement = "Observations to inform the split tagged as group review", Number = nrow(sex_dt[group_review != 1, ])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    sex_dt <- sex_dt[group_review == 1, ]
  }

  if ("group_review" %in% colnames(split_dt)) {
    split_dt[is.na(group_review), group_review := 1]
    unused_dt2 <- split_dt[group_review != 1, ]
    unused_dt2[, drop := "Observations to be split tagged as group review"]
    unused_dt <- rbind(unused_dt, unused_dt2, fill = TRUE)
    vet_dt <- rbind(vet_dt, data.table(Statement = "Observations to be split tagged as group review", Number = nrow(split_dt[group_review != 1, ])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    split_dt <- split_dt[group_review == 1, ]
  }

  if ("is_outlier" %in% colnames(sex_dt)) {
    sex_dt[is.na(is_outlier), is_outlier := 0]
    unused_dt2 <- sex_dt[is_outlier == 1,  ]
    unused_dt2[, drop := "Observations to inform the split tagged as outliers"]
    unused_dt <- rbind(unused_dt, unused_dt2)
    vet_dt <- rbind(vet_dt, data.table(Statement = "Observations to inform the split tagged as outliers", Number = nrow(sex_dt[is_outlier == 1, ])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    sex_dt <- sex_dt[is_outlier != 1, ]
  }

  if ("is_outlier" %in% colnames(split_dt)) {
    split_dt[is.na(is_outlier), is_outlier := 0]
    unused_dt2 <- split_dt[is_outlier == 1,  ]
    unused_dt2[, drop := "Observations to be split tagged as outliers"]
    unused_dt <- rbind(unused_dt, unused_dt2, fill =TRUE)
    vet_dt <- rbind(vet_dt, data.table(Statement = "Observations to be split tagged as outliers", Number = nrow(split_dt[is_outlier == 1, ])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    split_dt <- split_dt[is_outlier != 1, ]
  }

  #Selecting the measure that the modeler wants to sex split:
  sex_split_measure <- measure #Reassigning the user inputted measure variable.
  rm(measure) #Removing measure to ensure that data.table wont try to access the wrong thing and that errors will be thrown when measure is not a variable in the data set

  #Stop doing the sex split if there is no data to be sex split, or to inform the sex split.
  if(nrow(split_dt[measure %in% sex_split_measure, ]) == 0){stop(paste("There are no", paste(sex_split_measure, collapse = ", "), "observations in the data to be sex split"))}
  if(nrow(sex_dt[measure %in% sex_split_measure, ]) == 0){stop(paste("There are no", paste(sex_split_measure, collapse = ", "), "observations in the data to inform the sex split"))}

  # Dropping non-desired measures
  unused_dt2 <- sex_dt[!measure %in% sex_split_measure, ]
  unused_dt2[, drop := "Observations to inform the split of the wrong measure type"]
  unused_dt <- rbind(unused_dt, unused_dt2)
  vet_dt <- rbind(vet_dt, data.table(Statement = "Observations to inform the split of the wrong measure type", Number = nrow(sex_dt[!measure %in% sex_split_measure, ])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  sex_dt <- sex_dt[measure %in% sex_split_measure, ]

  #nrow(sex_dt) + nrow(split_dt) + nrow(unused_dt) == tot_rows  #Checking if we have lost any rows.

  unused_dt2 <- split_dt[!measure %in% sex_split_measure, ]
  unused_dt2[, drop := "Observations to be split of the wrong measure type"]
  unused_dt <- rbind(unused_dt, unused_dt2, fill =TRUE)
  vet_dt <- rbind(vet_dt, data.table(Statement = "Observations to be split of the wrong measure type", Number = nrow(split_dt[!measure %in% sex_split_measure, ])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  split_dt <- split_dt[measure %in% sex_split_measure, ]

  #nrow(sex_dt) + nrow(split_dt) + nrow(unused_dt) == tot_rows #Checking if we have lost any rows.

  #Dropping NIDs which we do not want to be used to inform the sex split.
  unused_dt2 <- sex_dt[nid %in% nids_to_drop, ]
  unused_dt2[, drop := "nids_to_drop specified observations dropped"]
  unused_dt <- rbind(unused_dt, unused_dt2)

  #Message for NIDs dropped using the nids_to_drop variable
  vet_dt <- rbind(vet_dt, data.table(Statement = "nids_to_drop specified NIDs dropped", Number = length(unique(nids_to_drop))))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  #Message for Observations dropped using the nids_to_drop variable
  vet_dt <- rbind(vet_dt, data.table(Statement = "nids_to_drop specified observations dropped", Number = sum(sex_dt[, nid %in% nids_to_drop])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  sex_dt <- sex_dt[!nid %in% nids_to_drop, ]

  ############## Calculating cases and sample size if missing for incidence/prevalence/proportion for data to inform sex split and data to be sex split
  message("Calculating missing mean/cases/sample size/standard error")
  if("incidence" %in% sex_split_measure | "prevalence" %in% sex_split_measure | "proportion" %in% sex_split_measure){
    sex_dt[is.na(sample_size) & is.na(effective_sample_size) & measure %in% c("prevalence", "incidence", "proportion"), imputed_sample_size := 1] #FiX: This technically doesnt work, doesnt cover all the situations mentioned below

    # Message for how many prevalence observations had Sample size calculated for them
    vet_dt <- rbind(vet_dt, data.table(Statement = "Prevalence Observations with calculated Sample Size (to inform split)", Number = sum(sex_dt[, measure == "prevalence" & is.na(sample_size)])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    # Message for how many incidence observations had Sample size calculated for them
    vet_dt <- rbind(vet_dt, data.table(Statement = "Incidence Observations with calculated Sample Size (to inform split)", Number = sum(sex_dt[, measure == "incidence" & is.na(sample_size)])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    # Message for how many proportion observations had Sample size calculated for them
    vet_dt <- rbind(vet_dt, data.table(Statement = "Proportion Observations with calculated Sample Size (to inform split)", Number = sum(sex_dt[, measure == "proportion" & is.na(sample_size)])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

    #calculating Sample Size for incidence, prevalence and proportion measures
    sex_dt[measure %in% c("incidence", "prevalence", "proportion") & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
    sex_dt[measure == "prevalence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean*(1-mean)/standard_error^2]
    sex_dt[measure == "proportion" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean*(1-mean)/standard_error^2]
    sex_dt[measure == "incidence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean/standard_error^2]

    # Calculating Cases
    vet_dt <- rbind(vet_dt, data.table(Statement = "Observations with calculated cases (to inform split)", Number = sum(sex_dt[, measure %in% c("prevalence", "incidence", "proportion") & is.na(cases)])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    sex_dt[is.na(cases), cases := sample_size * mean]

    # Calculating mean for those with only cases and sample size
    sex_dt[is.na(mean), mean := cases/sample_size]

    ####### Calculating standard error
    #Message for Prevalence SE calculations
    vet_dt <- rbind(vet_dt, data.table(Statement = "Prevalence Observations with calculated Standard Error (to inform split)", Number = sum(sex_dt[, is.na(standard_error) & measure == "prevalence"])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    #Message for Incidence SE calculations
    vet_dt <- rbind(vet_dt, data.table(Statement = "Incidence Observations with calculated Standard Error (to inform split)", Number = sum(sex_dt[, is.na(standard_error) & measure == "incidence"])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    #Message for Proportion SE calculations
    vet_dt <- rbind(vet_dt, data.table(Statement = "Proportion Observations with calculated Standard Error (to inform split)", Number = sum(sex_dt[, is.na(standard_error) & measure == "proportion"])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

    sex_dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    sex_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    sex_dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    sex_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    sex_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]

  ## Dropping observations where incidence or prevalence or proportion and mean or cases are 0
  #Message for Observations dropped to be split due to 0 cases or mean
  vet_dt <- rbind(vet_dt, data.table(Statement = "Prevalence Observations dropped from those to inform sex split due to 0 cases or 0 mean", Number = nrow(sex_dt[measure == "prevalence" & (mean == 0 | cases == 0), ])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  vet_dt <- rbind(vet_dt, data.table(Statement = "Incidence Observations dropped from those to inform sex split due to 0 cases or 0 mean", Number = nrow(sex_dt[measure == "incidence" & (mean == 0 | cases == 0), ])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  vet_dt <- rbind(vet_dt, data.table(Statement = "Proportion Observations dropped from those to inform sex split due to 0 cases or 0 mean", Number = nrow(sex_dt[measure == "proportion" & (mean == 0 | cases == 0), ])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  unused_dt[, imputed_sample_size := NA] #Adding this in so that the unused and sex dts have the same columns

  unused_dt2 <- sex_dt[measure == "prevalence" & (mean == 0 | cases == 0), ]
  unused_dt2[, drop := "Prevalence Observations dropped from those to inform sex split due to 0 cases or 0 mean"]
  unused_dt <- rbind(unused_dt, unused_dt2)

  unused_dt2 <- sex_dt[measure == "incidence" & (mean == 0 | cases == 0), ]
  unused_dt2[, drop := "Incidence Observations dropped from those to inform sex split due to 0 cases or 0 mean"]
  unused_dt <- rbind(unused_dt, unused_dt2)

  unused_dt2 <- sex_dt[measure == "proportion" & (mean == 0 | cases == 0), ]
  unused_dt2[, drop := "Proportion Observations dropped from those to inform sex split due to 0 cases or 0 mean"]
  unused_dt <- rbind(unused_dt, unused_dt2)

  sex_dt <- sex_dt[measure %in% c("incidence", "prevalence", "proportion") & (mean != 0 | cases != 0), ]

  #Same steps but for split_dt

  split_dt[is.na(sample_size) & is.na(effective_sample_size) & measure %in% c("prevalence", "incidence", "proportion"), imputed_sample_size := 1] #FiX: This technically doesnt work, doesnt cover all the situations mentioned below

  # Message for how many prevalence observations had Sample size calculated for them
  vet_dt <- rbind(vet_dt, data.table(Statement = "Prevalence Observations with calculated Sample Size (to split)", Number = sum(split_dt[, measure == "prevalence" & is.na(sample_size)])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  # Message for how many incidence observations had Sample size calculated for them
  vet_dt <- rbind(vet_dt, data.table(Statement = "Incidence Observations with calculated Sample Size (to split)", Number = sum(split_dt[, measure == "incidence" & is.na(sample_size)])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  # Message for how many proportion observations had Sample size calculated for them
  vet_dt <- rbind(vet_dt, data.table(Statement = "Proportion Observations with calculated Sample Size (to split)", Number = sum(split_dt[, measure == "proportion" & is.na(sample_size)])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  #calculating Sample Size for incidence, prevalence and proportion measures
  split_dt[measure %in% c("incidence", "prevalence", "proportion") & is.na(sample_size) & !is.na(effective_sample_size), sample_size := effective_sample_size]
  split_dt[measure == "prevalence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean*(1-mean)/standard_error^2]
  split_dt[measure == "proportion" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean*(1-mean)/standard_error^2]
  split_dt[measure == "incidence" & is.na(sample_size) & is.na(effective_sample_size), sample_size := mean/standard_error^2]

 ## Calculating Cases
    vet_dt <- rbind(vet_dt, data.table(Statement = "Observations with calculated cases (to split)", Number = sum(split_dt[, measure %in% c("prevalence", "incidence", "proportion") & is.na(cases)])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    split_dt[is.na(cases), cases := sample_size * mean]

    # Calculating mean for those with only cases and sample size
    split_dt[is.na(mean), mean := cases/sample_size]

    ####### Calculating standard error
    #Message for Prevalence SE calculations
    vet_dt <- rbind(vet_dt, data.table(Statement = "Prevalence Observations with calculated Standard Error (to split)", Number = sum(split_dt[, is.na(standard_error) & measure == "prevalence"])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    #Message for Incidence SE calculations
    vet_dt <- rbind(vet_dt, data.table(Statement = "Incidence Observations with calculated Standard Error (to split)", Number = sum(split_dt[, is.na(standard_error) & measure == "incidence"])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
    #Message for Proportion SE calculations
    vet_dt <- rbind(vet_dt, data.table(Statement = "Proportion Observations with calculated Standard Error (to split)", Number = sum(split_dt[, is.na(standard_error) & measure == "proportion"])))
    message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

    split_dt[, standard_error := as.numeric(standard_error)]
    split_dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
    z <- qnorm(0.975)
    split_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    split_dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
    split_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
    split_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  }

  ## Calculate sex matches - within study ratios
  message("Finding sex-specific matches and calculating sex ratios")
  sex_matches <- invisible(find_sex_match(sex_dt, cv_drop = cv_drop))
  #Message for number of sex matches found
  vet_dt <- rbind(vet_dt, data.table(Statement = "Sex matches found", Number =  nrow(sex_matches)))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  ## Remove NaNs and infs Standard errors
  #Message for dropping sex matches due to standard error issues
  vet_dt <- rbind(vet_dt, data.table(Statement = "Sex matches dropped as standard error is not greater than 0, is infinite, or is Not a Number ", Number =  sum(sex_matches[, standard_error_Male <= 0 | is.na(standard_error_Male)])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))
  sex_matches <- subset(sex_matches, standard_error_Male > 0)

  ## Checking there are no duplicates in sex_matches
  no_claim_dup <- within(sex_matches, x <- paste(nid, age_start, age_end, location_id, measure, year_start, year_end))
  no_claim_dup_check <- no_claim_dup[duplicated(no_claim_dup, by = "x")]
  vet_dt <- rbind(vet_dt, data.table(Statement = "Possible duplicate observations in the sex matches for NIDs:", Number = nrow(no_claim_dup_check)))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  # Message for how many NIDs need to be sex split
  vet_dt <- rbind(vet_dt, data.table(Statement = "NIDs to be sex split", Number = length(split_dt[, unique(nid)])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  # Message for how many observations need to be sex split
  vet_dt <- rbind(vet_dt, data.table(Statement = "Observations to be sex split", Number = nrow(split_dt)))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  ## Run MR-BRT
  message("Running MR-BRT")
  if (is.null(mrbrt_model) & mrbrt_model_age_cv == FALSE) {
    mrbrt_sex_dt <- calc_sex_ratios(sex_matches)
    model_name <- paste0(topic_name, "_mrbrt_sexsplit_", date, ".pkl")
    dat1 <- MRData()
    dat1$load_df(
      data = mrbrt_sex_dt,  col_obs = "log_ratio", col_obs_se = "log_se", col_study_id = "id")
    mod1 <- MRBRT(
      data = dat1,
      cov_models = list(
        LinearCovModel("intercept", use_re = TRUE)),
      inlier_pct = 0.9)

    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

    df_pred1 <- data.frame(intercept = 1)

    dat_pred1 <- MRData()

    dat_pred1$load_df(
      data = df_pred1, col_covs = list("intercept"))

    n_samples1 <- 1000L

    samples1 <- core$other_sampling$sample_simple_lme_beta(
      sample_size = n_samples1,
      model = mod1
    )

    draws1 <- mod1$create_draws(
      data = dat_pred1,
      beta_samples = samples1,
      gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
      random_study = FALSE )

    pred_draws <- as.data.table(draws1)

    py_save_object(object = mod1, filename = file.path(output_dir2, model_name), pickle = "dill")

  }

  if (is.null(mrbrt_model) & mrbrt_model_age_cv == TRUE) {
    mrbrt_sex_dt <- calc_sex_ratios(sex_matches)

    model_name <- paste0(topic_name, "_mrbrt_sexsplit_with_age_cv", date)

    dat1 <- MRData()
    dat1$load_df(
      data = mrbrt_sex_dt,  col_obs = "log_ratio", col_obs_se = "log_se", col_study_id = "id",
      col_covs = list("midage"))

    if (mrbrt_model_linear_age_cv == TRUE) {
      mod1 <- MRBRT(
        data = dat1,
        cov_models = list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel("midage")),
        inlier_pct = 0.9)

      mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

      df_pred1 <- data.frame(midage = seq(0.5, 100, by = 0.5))

      dat_pred1 <- MRData()

      dat_pred1$load_df(
        data = df_pred1, col_covs = list("midage"))

      n_samples1 <- 1000L

      samples1 <- core$other_sampling$sample_simple_lme_beta(
        sample_size = n_samples1,
        model = mod1
      )

      draws1 <- mod1$create_draws(
        data = dat_pred1,
        beta_samples = samples1,
        gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
        random_study = FALSE )

      pred_draws <- as.data.table(draws1)

      py_save_object(object = mod1, filename = file.path(output_dir2, model_name), pickle = "dill")

    } else {
      mod1 <- MRBRT(
        data = dat1,
        cov_models = list(
          LinearCovModel("intercept", use_re = TRUE),
          LinearCovModel(
            alt_cov = "midage",
            use_spline= TRUE,
            spline_knots = array(mrbrt_model_age_spline_knots),
            spline_degree = as.integer(mrbrt_model_age_spline_degree),
            spline_knots_type = 'domain',
            spline_r_linear = mrbrt_model_age_spline_linear_tail_right,
            spline_l_linear = mrbrt_model_age_spline_linear_tail_left)),
        inlier_pct = 0.9)

      mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

      df_pred1 <- data.frame(midage = seq(0.5, 100, by = 0.5))

      dat_pred1 <- MRData()

      dat_pred1$load_df(
        data = df_pred1, col_covs = list("midage"))

      n_samples1 <- 1000L

      samples1 <- core$other_sampling$sample_simple_lme_beta(
        sample_size = n_samples1,
        model = mod1
      )

      draws1 <- mod1$create_draws(
        data = dat_pred1,
        beta_samples = samples1,
        gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
        random_study = FALSE )

      pred_draws <- as.data.table(draws1)

      py_save_object(object = mod1, filename = file.path(output_dir2, model_name), pickle = "dill")
    }
  }

  if ((!is.null(mrbrt_model)) & mrbrt_model_age_cv == FALSE) {
    mod1 <- py_load_object(filename = file.path(mrbrt_model), pickle = "dill")

    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

    df_pred1 <- data.frame(intercept = 1)

    dat_pred1 <- MRData()

    dat_pred1$load_df(
      data = df_pred1, col_covs = list("intercept"))

    n_samples1 <- 1000L

    samples1 <- core$other_sampling$sample_simple_lme_beta(
      sample_size = n_samples1,
      model = mod1
    )

    draws1 <- mod1$create_draws(
      data = dat_pred1,
      beta_samples = samples1,
      gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
      random_study = FALSE )

    pred_draws <- as.data.table(draws1)

  }

  if ((!is.null(mrbrt_model)) & mrbrt_model_age_cv == TRUE) {
    mod1 <- py_load_object(filename = file.path(mrbrt_model), pickle = "dill")

    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

    df_pred1 <- data.frame(midage = seq(0.5, 100, by = 0.5))

    dat_pred1 <- MRData()

    dat_pred1$load_df(
      data = df_pred1, col_covs = list("midage"))

    n_samples1 <- 1000L

    samples1 <- core$other_sampling$sample_simple_lme_beta(
      sample_size = n_samples1,
      model = mod1
    )

    draws1 <- mod1$create_draws(
      data = dat_pred1,
      beta_samples = samples1,
      gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
      random_study = FALSE )

    pred_draws <- as.data.table(draws1)

  }

  colnames(pred_draws) <- draws2
  pred_draws[, (draws2) := lapply(.SD, exp), .SDcols = draws2]
  df_pred1$pred1 <- exp(mod1$predict(data = dat_pred1))
  df_pred1$pred1_lo <- exp(apply(draws1, 1, function(x) quantile(x, 0.025)))
  df_pred1$pred1_hi <- exp(apply(draws1, 1, function(x) quantile(x, 0.975)))
  ratio_mean <- mean(df_pred1$pred1)
  ratio_lo <- mean(df_pred1$pred1_lo)
  ratio_hi <- mean(df_pred1$pred1_hi)

  if (mrbrt_model_age_cv == TRUE) {
    pred_draws$midage <- seq(0.5, 100, by = 0.5)
  }


  ## Finding mid age and mid year
  split_dt[, midage := (age_start + age_end)/2]
  split_dt[, midage:=ifelse(midage ==1, 1.5, midage)] #recode midage 1 to 1.5 as "12- 23 months"
  split_dt[, midage:=ifelse(midage %in% c(0.5,1.5), midage, round(midage))] #keep mid age 0.5 and 1.5 as those have their own age groups and should not be rounded
  split_dt[, midyear := floor((year_start + year_end)/2)]
  tosplit_dt <- as.data.table(copy(split_dt))

  ## Pull out population data. We need age- sex- location-specific population.
  message("Pulling GBD population for sex split")
  pop <- get_population(age_group_id = "all", sex_id = "all", year_id = unique(floor(tosplit_dt$midyear)),
                        location_id=unique(tosplit_dt$location_id), single_year_age = T, release_id = release_id)

  #message out run id used
  run_id<-unique(pop$run_id)
  message(paste0("get_population run_id use:",run_id))
  vet_dt <- rbind(vet_dt, data.table(Statement = "Get population used run id:", Number = run_id))

  pop2 <- merge(pop, ages, by="age_group_id")
  pop2[age_group_name == "<1 year", age_group_name := 0.5]
  pop2[age_group_name == "95 plus", age_group_name := 97]
  pop2[age_group_name == "12 to 23 months", age_group_name := 1.5]
  pop2[, age_group_name := as.numeric(age_group_name)]

  pop <- copy(pop2)

  pop$age_group_id <- NULL

  ## Merge in populations for both-sex and each sex. Turning age bins into the midpoint - because the population ratios, not population itself, is what's important.
  if ("population" %in% colnames(tosplit_dt)) {
    tosplit_dt <- subset(tosplit_dt, select = -c(population))
  }
  tosplit_dt2 <- merge(tosplit_dt, pop[sex_id==3,.(location_id, year_id, population, age_group_name)],
                       by.x=c("midyear", "midage", "location_id"),
                       by.y=c("year_id", "age_group_name", "location_id"),
                       all.x=T)
  setnames(tosplit_dt2, "population", "population_both")
  tosplit_dt2 <- merge(tosplit_dt2, pop[sex_id==1,.(location_id, year_id, population, age_group_name)], by.x=c("midyear", "midage", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(tosplit_dt2, "population", "population_male")
  tosplit_dt2 <- merge(tosplit_dt2, pop[sex_id==2,.(location_id, year_id, population, age_group_name)], by.x=c("midyear","midage", "location_id"), by.y=c("year_id", "age_group_name", "location_id"), all.x=T)
  setnames(tosplit_dt2, "population", "population_female")

  vet_dt <- rbind(vet_dt, data.table(Statement = "Both sex observations unable to sex split because no population match", Number = sum(tosplit_dt2[, is.na(population_both)])))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  unused_dt2 <- tosplit_dt2[is.na(population_both), ]
  unused_dt2[, drop := "Both sex observations unable to sex split because no population match"]
  unused_dt <- rbind(unused_dt, unused_dt2, fill = T)

  tosplit_dt2 <- tosplit_dt2[!is.na(population_both), ] ## dropping data where no population match

  ## Calculating 1000 draws of both sex observations using SE
  message("Calculting 1000 draws of both sex observations")

  tosplit_dt2 <- tosplit_dt2[, standard_dev := standard_error*(sqrt(sample_size))]
  if("continuous" %in% sex_split_measure){
    tosplit_dt2 <- tosplit_dt2[, standard_dev := sqrt(variance)]
  }

  temp_dt <- data.table()

  if("prevalence" %in% sex_split_measure | "proportion" %in% sex_split_measure){
    temp_prev_prop <- tosplit_dt2[measure %in% c("prevalence", "proportion"), ]
    
    n <- 1000
    temp_prev_prop[, sample_size_int := as.integer(sample_size)]
    func1 <- function(x, y) rbinom(n, prob = x, size = y)
    list1 <- Map(func1, temp_prev_prop$mean, temp_prev_prop$sample_size_int)
    
    temp <- do.call(rbind.data.frame, list1)
    colnames(temp) <- draws
    tosplit_dt3 <- cbind(temp_prev_prop, temp)
    
    tosplit_dt4 <- tosplit_dt3[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) / sample_size_int)]
    
    temp_dt <- rbind(temp_dt, tosplit_dt4, fill = T)
  }

  if("continuous" %in% sex_split_measure | "incidence" %in% sex_split_measure){
    temp_cont_inc <- tosplit_dt2[measure %in% c("continuous", "incidence"), ]
    
    temp_cont_inc <- temp_cont_inc[, log_mean := log(mean)]
    n <- 1000
    func1 <- function(x, y) rnorm(n, mean = x, sd = y)
    list1 <- Map(func1, temp_cont_inc$log_mean, temp_cont_inc$standard_dev)
    
    temp <- do.call(rbind.data.frame, list1)
    colnames(temp) <- draws
    tosplit_dt3 <- cbind(temp_cont_inc, temp)
    
    tosplit_dt4 <- tosplit_dt3[ , (draws) := lapply(.SD, exp), .SDcols = draws]
    
    temp_dt <- rbind(temp_dt, tosplit_dt4, fill = T)
    
  }
  
  tosplit_dt4 <- temp_dt

  ## Merging both sex draws with MR-BRT sex ratio draws
  if (mrbrt_model_age_cv == FALSE) {
    tosplit_dt4[, merge := 1]
    pred_draws[, merge := 1]
    split_dt <- merge(tosplit_dt4, pred_draws, by = "merge", allow.cartesian = T)
  }

  if (mrbrt_model_age_cv == TRUE) {
    split_dt <- merge(tosplit_dt4, pred_draws, by = "midage", allow.cartesian = T) #Fix: Change to merge (base function)
  }


  ## Implementing sex split
  message("Implementing sex split")
  split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * (population_both/(population_male + (get(paste0("mrbrt_draw_", x)) * population_female))))]
  split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("mrbrt_draw_", x)) * get(paste0("male_", x)))]
  split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
  split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
  split_dt[, male_sample_size := sample_size*(population_male/population_both)]
  split_dt[, female_sample_size := sample_size-male_sample_size]
  split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999), draws2) := NULL]
  male_dt <- copy(split_dt)

  if (mrbrt_model_age_cv == FALSE) {
    male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA,
                    cases = NA, sample_size = male_sample_size, uncertainty_type_value = NA, sex = "Male", effective_sample_size = NA,
                    note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                          ratio_lo, " - ", ratio_hi, ")"),
                    split = "Sex")]
  }

  if (mrbrt_model_age_cv == TRUE) {
    midages <- unique(male_dt$midage)
    for (i in midages) {
      male_dt[midage == i, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA,
                                 cases = NA, sample_size = male_sample_size, uncertainty_type_value = NA, sex = "Male", effective_sample_size = NA,
                                 note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", round(df_pred1$pred1[df_pred1$midage == i], digits = 3), " (",
                                                       round(df_pred1$pred1_lo[df_pred1$midage == i], digits = 3), " - ", round(df_pred1$pred1_hi[df_pred1$midage == i], digits = 3), ")"),
                                 split = "Sex")]
    }
  }

  male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  female_dt <- copy(split_dt)

  if (mrbrt_model_age_cv == FALSE) {
    female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
                      cases = NA, sample_size = female_sample_size, uncertainty_type_value = NA, sex = "Female", effective_sample_size = NA,
                      note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                            ratio_lo, " - ", ratio_hi, ")"),
                      split = "Sex")]
  }

  if (mrbrt_model_age_cv == TRUE) {
    midages <- unique(female_dt$midage)
    for (i in midages) {
      female_dt[midage == i, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
                                   cases = NA, sample_size = female_sample_size, uncertainty_type_value = NA, sex = "Female", effective_sample_size = NA,
                                   note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", round(df_pred1$pred1[df_pred1$midage == i], digits = 3), " (",
                                                         round(df_pred1$pred1_lo[df_pred1$midage == i], digits = 3), " - ", round(df_pred1$pred1_hi[df_pred1$midage == i], digits = 3), ")"),
                                   split = "Sex")]
    }
  }

  female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
  total_dt <- rbindlist(list(female_dt, male_dt))
  total_dt <- unique(total_dt)

  # clearing sample size (calculated using GBD pop sex structure) for rows where mean is not 0 (epiuploader will calculate new sample size based on mean and SE)
  total_dt[!mean == 0, sample_size := NA]

  # clearing SE for rows where mean is 0 (epiuploader will calculate new SE based on mean and sample size)
  total_dt[mean == 0, standard_error := NA]

  #Message for how many sex split observations there are at the end
  vet_dt <- rbind(vet_dt, data.table(Statement = "Number of sex split observations", Number =  nrow(total_dt)))
  message(paste(vet_dt[nrow(vet_dt), ], collapse = ": "))

  #Vetting plots
  if(vetting_plots == T){
    message("Producing vetting plots")
    #Plots comparing post sex split data to sex specific data, over age and time
    data_for_plot <- rbind(total_dt, sex_dt, fill = T)
    if ("region_name" %in% colnames(data_for_plot)) {
      data_for_plot <- subset(data_for_plot, select = -c(region_name))
    }
    if ("super_region_name" %in% colnames(data_for_plot)) {
      data_for_plot <- subset(data_for_plot, select = -c(super_region_name))
    }
    data_for_plot <- data.table(data_for_plot)
    data_for_plot <- left_join(data_for_plot, locations, by = "location_id")
    data_for_plot <- data_for_plot[split == "Sex", description := "Sex-Split"]
    data_for_plot <- data_for_plot[is.na(split), description := "Sex-Specific"]
    data_for_plot <- data_for_plot[is.na(midage), midage := (age_start + age_end)/2]
    data_for_plot <- data_for_plot[, midyear := (year_start + year_end)/2]

    message("Plotting sex specific sex split plots")
    sex_specific_sex_split_plot(data = data_for_plot, measure = sex_split_measure, x_axis = "age")
    sex_specific_sex_split_plot(data = data_for_plot, measure = sex_split_measure, x_axis = "year")

    message("Plotting sex distribution of both sex-draws plots")
    # plotting distribution of both-sex draws
    plot_both_sex_draws(data = tosplit_dt4)

    #New means vs old mean
    message("Plotting graph predictions plots")
    data_for_plot <- copy(split_dt)
    graph_predictions(dt = data_for_plot, plot_measure = sex_split_measure)

    #Difference of confidence intervals
    message("Plotting difference in UI plots")
    data_for_plot <- copy(total_dt)
    parent_se <- split_dt[, c("seq", "standard_error")]
    setnames(parent_se, old = "standard_error", new = "parent_standard_error")
    data_for_plot <- merge(data_for_plot, parent_se, by = "seq")
    diff_plot_subtract(data_for_plot)
    diff_plot_ratio(data_for_plot)

    #Input and output sex ratios
    message("Plotting input and output sex ratio plots")

    df_pred1 <- as.data.table(df_pred1)
    data_for_plot <- df_pred1[, ratio := pred1]
    data_for_plot <- rbind(mrbrt_sex_dt, df_pred1, fill = TRUE)

    plot_input_sex_ratios(data = data_for_plot)

    #Visualize sex ratio over age and super region
    message("Plotting sex ratios over age by super region plots")

    data_for_plot <- merge(mrbrt_sex_dt, locations, by = "location_id", all.x=TRUE)
    plot_sex_ratios_age_superregion(data_for_plot)
  }


  # Prepping dataframes for output
  message("Outputting csvs")
  tosplit_dt5 <- subset(tosplit_dt2, select = -c(population_both, population_female, population_male))
  for_upload_data <- rbind(total_dt, unused_dt, sex_dt, fill = T)

  # Creating csv output storing arguments used
  args <- data.table(data.frame(matrix(nrow = 1, ncol = 18)))
  colnames(args) <- c("topic_name", "output_dir", "bundle_version_id", "data_all_csv", "data_to_split_csv", "data_raw_sex_specific_csv", "nids_to_drop", "cv_drop",
                      "mrbrt_model", "mrbrt_model_age_cv", "mrbrt_model_linear_age_cv", "mrbrt_model_age_spline_knots", "mrbrt_model_age_spline_degree",
                      "mrbrt_model_age_spline_linear_tail_left", "mrbrt_model_age_spline_linear_tail_right", "release_id", "measure", "vetting_plots")
  args$topic_name <- paste(topic_name, collapse = ", ")
  args$output_dir <- paste(output_dir, collapse = ", ")
  args$bundle_version_id <- paste(bundle_version_id, collapse = ", ")
  args$data_all_csv <- paste(data_all_csv, collapse = ", ")
  args$data_to_split_csv <- paste(data_to_split_csv, collapse = ", ")
  args$data_raw_sex_specific_csv <- paste(data_raw_sex_specific_csv, collapse = ", ")
  args$nids_to_drop <- paste(nids_to_drop, collapse = ", ")
  args$cv_drop <- paste(cv_drop, collapse = ", ")
  args$mrbrt_model <- paste(mrbrt_model, collapse = ", ")
  args$mrbrt_model_age_cv <- paste(mrbrt_model_age_cv, collapse = ", ")
  args$mrbrt_model_linear_age_cv <- paste(mrbrt_model_linear_age_cv, collapse = ", ")
  args$mrbrt_model_age_spline_knots <- paste(mrbrt_model_age_spline_knots, collapse = ", ")
  args$mrbrt_model_age_spline_degree <- paste(mrbrt_model_age_spline_degree, collapse = ", ")
  args$mrbrt_model_age_spline_linear_tail_left <- paste(mrbrt_model_age_spline_linear_tail_left, collapse = ", ")
  args$mrbrt_model_age_spline_linear_tail_right <- paste(mrbrt_model_age_spline_linear_tail_right, collapse = ", ")
  args$release_id <- paste(release_id, collapse = ", ")
  args$measure <- paste(sex_split_measure, collapse = ", ")
  args$vetting_plots <- paste(vetting_plots, collapse = ", ")

  ## Writing out csvs
  write.csv(vet_dt, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_vetting_data.csv"), row.names = F)
  write.csv(total_dt, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_post_sex_split_data.csv"), row.names = F)
  write.csv(unused_dt, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_unused_data.csv"), row.names = F)
  write.csv(sex_dt, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_sex_specific_data.csv"), row.names = F)
  write.csv(for_upload_data, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_for_upload_data.csv"), row.names = F)
  write.csv(mrbrt_sex_dt, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_mrbrt_sex_matched_data.csv"), row.names = F)
  write.csv(tosplit_dt5, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_pre_sex_split_data.csv"), row.names = F)
  write.csv(df_pred1, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_mrbrt_ratios.csv"), row.names = F)
  write.csv(args, paste0(output_dir2, "sex_split_", date, "_", topic_name, "_arguments.csv"), row.names = F)

}
