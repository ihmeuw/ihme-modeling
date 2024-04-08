
## Scatter old covariates against new covariates ##

library(data.table)
library(ggplot2)
library(ggrepel)

library(mortdb)

locs <- mortdb::get_locations(gbd_year = 2020)

dir_covid_plots <- ""

gbd <- T
options(scipen=999)

if (gbd) {

  old_date <- ""
  old_run_id <- ""
  per_diff_label_cutoff <- 5 # only label points with > _% difference

  new_date <- ""
  new_run_id <- ""

} else { # em paper

  old_date <- ""
  old_run_id <- ""
  per_diff_label_cutoff <- 5 # only label points with > _% difference

  new_date <- ""
  new_run_id <- ""

}

regression_locations <- fread("")

# CUSTOM #

if (gbd) {

  old <- fread(fs::path())
  new <- fread(fs::path())

} else {

  old <- fread(fs::path())
  new <- fread(fs::path())

}

old_long <- melt(setDT(old[, !c("location_id", "location_name", "age_name", "sex", "stars")]),
                 id.vars = c("ihme_loc_id"),
                 variable.name = "covariate")


new_long <- melt(setDT(new[, !c("location_id", "location_name", "stars")]),
                 id.vars = c("ihme_loc_id"),
                 variable.name = "covariate")

setnames(old_long, "value", "old")
setnames(new_long, "value", "new")

data <- merge(old_long,
              new_long,
              by = c("ihme_loc_id", "covariate"),
              all = TRUE)

data <- merge(data,
              locs[, c("ihme_loc_id", "super_region_name")],
              by = "ihme_loc_id",
              all.x = TRUE)

data[, abs_per_diff := abs(((new - old) / old) * 100)]

bins <- c(-1, 1, 2, 5, 10, 25, 100000)
bins_labels <- c("0 to 1","1 to 2","2 to 5","5 to 10","10 to 25", "25+")
data[, perc_diff_bin := cut(abs_per_diff, bins, bins_labels)]

all_covs <- unique(data$covariate)


if (gbd) {

  pdf(file = fs::path(), width = 14, height = 10)

} else {

  pdf(file = fs::path(), width = 14, height = 10)

}

for (cov in all_covs) {

  print(cov)

  temp <- data[covariate == cov & !is.na(old) & !is.na(new)]

  p <- ggplot(temp, aes(x = old, y = new, color = super_region_name, label = ihme_loc_id, shape = perc_diff_bin)) +
    geom_abline(slope = 1, intercept = 0, color = "gray") +
    geom_point() +
    geom_text_repel(data = temp[abs_per_diff > per_diff_label_cutoff], show.legend = FALSE, max.overlaps = 25) +
    theme_bw() +
    labs(
      x = "Old",
      y = "New",
      color = "Super Region",
      shape = "Percent Difference",
      title = paste("Input", cov),
      subtitle = paste0()
    )

  print(p)

}

dev.off()

# ALL #

if (gbd) {

  old <- fread(fs::path())
  new <- fread(fs::path())

} else {

  old <- fread(fs::path())
  new <- fread(fs::path())

}

old_long <- melt(setDT(old[, !c("location_id", "age_name", "sex", "stars")]),
                 id.vars = c("ihme_loc_id", "location_name"),
                 variable.name = "covariate")

new_long <- melt(setDT(new[, !c("location_id", "stars")]),
                 id.vars = c("ihme_loc_id", "location_name"),
                 variable.name = "covariate")

setnames(old_long, "value", "old")
setnames(new_long, "value", "new")

data_initial <- merge(old_long,
                      new_long,
                      by = c("ihme_loc_id", "location_name", "covariate"),
                      all = TRUE)

data_initial <- merge(data_initial,
                      locs[, c("ihme_loc_id", "super_region_name", "level")],
                      by = "ihme_loc_id",
                      all.x = TRUE)

data_initial <- as.data.table(data_initial)

covid_covs <- c("cumulative_infections", "cumulative_infections_lagged",
                "idr_reference", "idr_lagged",
                "mobility_reference", "mobility_lagged",
                "death_rate_covid", "death_rate_excess")

data_initial[, abs_per_diff := abs(((new - old) / old) * 100)]

bins <- c(-1, 1, 2, 5, 10, 25, 100000)
bins_labels <- c("0 to 1","1 to 2","2 to 5","5 to 10","10 to 25", "25+")
data_initial[, perc_diff_bin := cut(abs_per_diff, bins, bins_labels)]

for (type in c("both", "national", "subnational")) {
  
  message(type)
  
  if(type == "national"){
    data <- data_initial[level == 3]
  } else if(type == "subnational"){
    data <- data_initial[level > 3]
  } else {
    data <- copy(data_initial)
  }
  
  if (gbd) {
    
    pdf(file = fs::path(), width = 14, height = 10)
    
  } else {
    
    pdf(file = fs::path(), width = 14, height = 10)
    
  }
  
  for (cov in covid_covs) {
    
    print(cov)
    
    temp <- data[covariate == cov & !is.na(old) & !is.na(new)]
    
    temp$grp <- as.numeric(cut_number(temp$old, 3))
    
    p <- ggplot(temp, aes(x = old, y = new, color = super_region_name, label = location_name, shape = perc_diff_bin)) +
      geom_abline(slope = 1, intercept = 0, color = "gray") +
      geom_point() +
      geom_text_repel(data = temp[abs_per_diff > per_diff_label_cutoff], show.legend = FALSE, max.overlaps = 25) +
      theme_bw() +
      labs(
        x = "Old",
        y = "New",
        color = "Super Region",
        shape = "Percent Difference",
        title = paste("Prediction", cov),
        subtitle = paste0()
      )
    
    p_high <- ggplot(temp[grp == 3], aes(x = old, y = new, color = super_region_name, label = location_name, shape = perc_diff_bin)) +
      geom_abline(slope = 1, intercept = 0, color = "gray") +
      geom_point() +
      geom_text_repel(data = temp[grp == 3 & abs_per_diff > per_diff_label_cutoff], show.legend = FALSE, max.overlaps = 25) +
      theme_bw() +
      labs(
        x = "Old",
        y = "New",
        color = "Super Region",
        shape = "Percent Difference",
        title = paste("Top Third: Prediction", cov),
        subtitle = paste0()
      )
    
    p_mid <- ggplot(temp[grp == 2], aes(x = old, y = new, color = super_region_name, label = location_name, shape = perc_diff_bin)) +
      geom_abline(slope = 1, intercept = 0, color = "gray") +
      geom_point() +
      geom_text_repel(data = temp[grp == 2 & abs_per_diff > per_diff_label_cutoff], show.legend = FALSE, max.overlaps = 25) +
      theme_bw() +
      labs(
        x = "Old",
        y = "New",
        color = "Super Region",
        shape = "Percent Difference",
        title = paste("Mid Third: Prediction", cov),
        subtitle = paste0()
      )
    
    p_low <- ggplot(temp[grp == 1], aes(x = old, y = new, color = super_region_name, label = location_name, shape = perc_diff_bin)) +
      geom_abline(slope = 1, intercept = 0, color = "gray") +
      geom_point() +
      geom_text_repel(data = temp[grp == 1 & abs_per_diff > per_diff_label_cutoff], show.legend = FALSE, max.overlaps = 25) +
      theme_bw() +
      labs(
        x = "Old",
        y = "New",
        color = "Super Region",
        shape = "Percent Difference",
        title = paste("Bottom Third: Prediction", cov),
        subtitle = paste0()
      )
    
    print(p)
    print(p_high)
    print(p_mid)
    print(p_low)
    
  }
  
  dev.off()
}


# Run comparisons on year-specific files (only generated for GBD)

if (gbd) {

  # Only 2020 #

  old <- fread(fs::path())
  new <- fread(fs::path())

  old_long <- melt(setDT(old[, !c("location_id", "location_name", "age_name", "sex", "stars")]),
                   id.vars = c("ihme_loc_id"),
                   variable.name = "covariate")


  new_long <- melt(setDT(new[, !c("location_id", "location_name", "stars")]),
                   id.vars = c("ihme_loc_id"),
                   variable.name = "covariate")

  setnames(old_long, "value", "old")
  setnames(new_long, "value", "new")

  data <- merge(old_long,
                new_long,
                by = c("ihme_loc_id", "covariate"),
                all = TRUE)

  data <- merge(data,
                locs[, c("ihme_loc_id", "super_region_name")],
                by = "ihme_loc_id",
                all.x = TRUE)

  data[, abs_per_diff := abs(((new - old) / old) * 100)]
  
  bins <- c(-1, 1, 2, 5, 10, 25, 100000)
  bins_labels <- c("0 to 1","1 to 2","2 to 5","5 to 10","10 to 25", "25+")
  data[, perc_diff_bin := cut(abs_per_diff, bins, bins_labels)]

  pdf(file = fs::path(), width = 14, height = 10)

  for (cov in unique(data$covariate)) {

    print(cov)

    temp <- data[covariate == cov & !is.na(old) & !is.na(new)]

    p <- ggplot(temp, aes(x = old, y = new, color = super_region_name, label = ihme_loc_id, shape = perc_diff_bin)) +
      geom_abline(slope = 1, intercept = 0, color = "gray") +
      geom_point() +
      geom_text_repel(data = temp[abs_per_diff > per_diff_label_cutoff], show.legend = FALSE, max.overlaps = 25) +
      theme_bw() +
      labs(
        x = "Old",
        y = "New",
        color = "Super Region",
        shape = "Percent Difference",
        title = paste("Input", cov),
        subtitle = paste0()
      )

    print(p)

  }

  dev.off()


  # Only 2021 #

  old <- fread(fs::path())
  new <- fread(fs::path())

  old_long <- melt(setDT(old[, !c("location_id", "location_name", "age_name", "sex", "stars")]),
                   id.vars = c("ihme_loc_id"),
                   variable.name = "covariate")


  new_long <- melt(setDT(new[, !c("location_id", "location_name", "stars")]),
                   id.vars = c("ihme_loc_id"),
                   variable.name = "covariate")

  setnames(old_long, "value", "old")
  setnames(new_long, "value", "new")

  data <- merge(old_long,
                new_long,
                by = c("ihme_loc_id", "covariate"),
                all = TRUE)

  data <- merge(data,
                locs[, c("ihme_loc_id", "super_region_name")],
                by = "ihme_loc_id",
                all.x = TRUE)

  data[, abs_per_diff := abs(((new - old) / old) * 100)]
  
  bins <- c(-1, 1, 2, 5, 10, 25, 100000)
  bins_labels <- c("0 to 1","1 to 2","2 to 5","5 to 10","10 to 25", "25+")
  data[, perc_diff_bin := cut(abs_per_diff, bins, bins_labels)]

  pdf(file = fs::path(), width = 14, height = 10)

  for (cov in unique(data$covariate)) {

    print(cov)

    temp <- data[covariate == cov & !is.na(old) & !is.na(new)]

    p <- ggplot(temp, aes(x = old, y = new, color = super_region_name, label = ihme_loc_id, shape = perc_diff_bin)) +
      geom_abline(slope = 1, intercept = 0, color = "gray") +
      geom_point() +
      geom_text_repel(data = temp[abs_per_diff > per_diff_label_cutoff], show.legend = FALSE, max.overlaps = 25) +
      theme_bw() +
      labs(
        x = "Old",
        y = "New",
        color = "Super Region",
        shape = "Percent Difference",
        title = paste("Input", cov),
        subtitle = paste0()
      )

    print(p)

  }

  dev.off()

}


# Covariate histograms #

if (gbd) {
  
  old_fit <- fread(fs::path())
  new_fit <- fread(fs::path())
  new_predict <- fread(fs::path())
  new_2020 <- fread(fs::path())
  new_2021 <- fread(fs::path())
  
} else {
  
  new_fit <- fread(fs::path())
  new_predict <- fread(fs::path())
  
}

log_trans <- c("crude_death_rate", "cumulative_infections_lagged", "cvd_death_rate", 
               "death_rate_covid", "diabetes_death_rate", "hemog_thalass_death_rate",
               "hiv_death_rate", "resp_asthma_death_rate", "resp_copd_death_rate")

all_covs <- setdiff(names(new_predict), c("location_id","ihme_loc_id","location_name"))

pdf(file = fs::path(), 
    width = 14, height = 14)

for (cov in all_covs) {
  
  print(cov)
  
  temp_fit <- new_fit[, c("ihme_loc_id", get("cov"))]
  colnames(temp_fit)[2] <- "plot_cov"
  temp_fit[, dataset := "Fit"]
  temp_predict <- new_predict[, c("ihme_loc_id", get("cov"))]
  colnames(temp_predict)[2] <- "plot_cov"
  temp_predict[, dataset := "Predict"]
  temp_2020 <- new_2020[, c("ihme_loc_id", get("cov"))]
  colnames(temp_2020)[2] <- "plot_cov"
  temp_2020[, dataset := "Only2020"]
  temp_2021 <- new_2021[, c("ihme_loc_id", get("cov"))]
  colnames(temp_2021)[2] <- "plot_cov"
  temp_2021[, dataset := "Only2021"]
  
  temp <- rbind(temp_predict, temp_2020, temp_2021)
  
  temp <- merge(temp,
                locs[, c("ihme_loc_id", "super_region_name", "level")],
                by = "ihme_loc_id",
                all.x = TRUE)
  temp[is.na(super_region_name) & ihme_loc_id %like% c("USA|CAN|DEU|ESP|ITA"), super_region_name := "High-income"]
  temp[is.na(super_region_name) & ihme_loc_id %like% c("IND"), super_region_name := "South Asia"]
  temp[ihme_loc_id == "ITA_99999", level := 4]
  temp <- temp[!level < 3]
  assertable::assert_values(temp, "super_region_name", test = "not_na")
  
  temp[, dataset := factor(dataset, levels = c("Fit", "Predict", "Only2020", "Only2021"))]
  
  if(get("cov") %in% log_trans){
    temp[, plot_cov := log(plot_cov)]
  }
  
  htemp <- ggplot(temp, aes(x = plot_cov, fill = super_region_name)) +
    geom_histogram() + 
    theme_bw() +
    facet_wrap(~dataset, ncol = 1, scales = "free_y") +
    scale_fill_brewer(palette = "Dark2") +
    labs(
      x = ifelse(get("cov") %in% log_trans, paste0("log ", get("cov")), get("cov")),
      fill = "Super Region",
      title = ifelse(get("cov") %in% log_trans, paste0("log ", get("cov")), get("cov")),
      subtitle = paste0()
    ) + 
    theme(legend.position = "bottom")
  
  print(htemp)
  
}

dev.off()


# histogram of old vs new fit data 

pdf(file = fs::path(), 
    width = 14, height = 14)

for (cov in all_covs) {
  
  print(cov)
  
  temp_fit <- new_fit[, c("ihme_loc_id", get("cov"))]
  colnames(temp_fit)[2] <- "plot_cov"
  temp_fit[, dataset := "New Input"]
  old_temp_fit <- old_fit[, c("ihme_loc_id", get("cov"))]
  colnames(old_temp_fit)[2] <- "plot_cov"
  old_temp_fit[, dataset := "Old Input"]
  
  temp <- rbind(temp_fit, old_temp_fit)
  
  temp <- merge(temp,
                locs[, c("ihme_loc_id", "super_region_name", "level")],
                by = "ihme_loc_id",
                all.x = TRUE)
  temp[is.na(super_region_name) & ihme_loc_id %like% c("USA|CAN|DEU|ESP|ITA"), super_region_name := "High-income"]
  temp[is.na(super_region_name) & ihme_loc_id %like% c("IND"), super_region_name := "South Asia"]
  temp[ihme_loc_id == "ITA_99999", level := 4]
  temp <- temp[!level < 3]
  assertable::assert_values(temp, "super_region_name", test = "not_na")
  
  temp[, dataset := factor(dataset, levels = c("New Input", "Old Input"))]
  
  if(get("cov") %in% log_trans){
    temp[, plot_cov := log(plot_cov)]
  }
  
  htemp <- ggplot(temp, aes(x = plot_cov, fill = super_region_name)) +
    geom_histogram() + 
    theme_bw() +
    facet_wrap(~dataset, ncol = 1, scales = "free_y") +
    scale_fill_brewer(palette = "Dark2") +
    labs(
      x = ifelse(get("cov") %in% log_trans, paste0("log ", get("cov")), get("cov")),
      fill = "Super Region",
      title = ifelse(get("cov") %in% log_trans, paste0("log ", get("cov")), get("cov")),
      subtitle = paste0()
    ) + 
    theme(legend.position = "bottom")
  
  print(htemp)
  
}

dev.off()


# outside of range predictions 

new_fit_range <- copy(new_fit)

for (cov in log_trans){
  new_fit_range[,paste0("Log_",get("cov"))] = log(new_fit_range[,c(get("cov"))])
}

all_covs_w_log <- all_covs[!all_covs %in% log_trans]
all_covs_w_log <- c(levels(as.factor(all_covs_w_log)), paste0("Log_",log_trans))

out_range <- data.table()
for(type in c("2020", "2021", "predict")){
  if(type == "2020"){
    dt <- copy(new_2020)
  } else if (type == "2021"){
    dt <- copy(new_2021)
  } else {
    dt <- copy(new_predict)
  }
  
  for (cov in log_trans){
    dt[,paste0("Log_",get("cov"))] = log(dt[,c(get("cov"))])
  }
  
  for (cov in all_covs_w_log){
    min_cov <-  min(new_fit_range[, c(get("cov"))], na.rm = T)
    max_cov <- max(new_fit_range[, c(get("cov"))], na.rm = T)
    
    dt_max_out <- dt[dt[[get("cov")]] > max_cov]
    dt_min_out <- dt[dt[[get("cov")]] < min_cov]
    
    dt_max_out <- dt_max_out[, c("location_id", "ihme_loc_id", "location_name", get("cov"))]
    dt_max_out[, ':='(min_fit_value = min_cov, max_fit_value = max_cov)]
    dt_min_out <- dt_min_out[, c("location_id", "ihme_loc_id", "location_name", get("cov"))]
    dt_min_out[, ':='(min_fit_value = min_cov, max_fit_value = max_cov)]
    dt_out <- rbind(dt_min_out, dt_max_out)
    
    dt_out <- melt(
      dt_out, 
      id.vars = c("location_id", "ihme_loc_id", "location_name", "min_fit_value", "max_fit_value"),
      measure.vars = c(get("cov"))
    )
    
    dt_out[, time_period := type]
    
    out_range <- rbind(out_range, dt_out)
  }
}

out_range <- out_range[, c("location_id", "ihme_loc_id", "location_name",
                           "time_period", "variable", "value", "min_fit_value",
                           "max_fit_value")]

readr::write_csv(out_range, fs::path())
