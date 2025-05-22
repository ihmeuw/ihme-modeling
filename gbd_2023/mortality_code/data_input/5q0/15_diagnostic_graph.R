###############################################################################
## Description: Diagnostic graphs including
###############################################################################

## load the function
rm(list=ls())

Sys.unsetenv("PYTHONPATH")

library(foreign)
library(data.table)
library(mortdb)
library(mortcore)
library(scales)
library(readr)
library(argparse)
library(ggrepel)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(gridExtra)
source("FILEPATH")

# Get arguments
if(!interactive()) {
  parser <- ArgumentParser()
  parser$add_argument("--version_id", type="integer", required = TRUE,
                      help = "The version_id for this run of 5q0")
  parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                      help = "The current gbd year")
  parser$add_argument("--end_year", type = "integer", required = TRUE,
                      help = "The last year of estimation")
  parser$add_argument('--code_dir', type ="character", required=TRUE,
                      help="Code being run")
  args <- parser$parse_args()
  list2env(args, .GlobalEnv)
} else {

  version_id <-
  gbd_year <-
  end_year <-

}


gbd_round_id <- get_gbd_round(gbd_year = gbd_year)

yml_dir <- gsub("child-mortality", "", code_dir)
yml <- readLines(paste0("FILEPATH"))

prev_gbd_year <- yml[grepl("gbd_year_previous", yml)]
prev_gbd_year <- as.numeric(gsub("\\D", "", prev_gbd_year))

keep_cols <- c("location_id", "ihme_loc_id", "region_name", "year_id",
               "sex_id", "age_group_id")

old_id <- get_proc_version("5q0","estimate", gbd_year = prev_gbd_year, run_id = "best")
compare_id <- 531L
recent_completed <- get_proc_version("5q0", "estimate", run_id = "recent_completed")

loc_map <- get_locations(gbd_year = gbd_year)[, .(location_id, ihme_loc_id)]

############################1. comparison scatter plot #########################
year_list = c(seq(1950, 2010, by = 10), prev_gbd_year)
pc_to_label = 0.005

old5q0 = get_mort_outputs("5q0", "estimate",
                          estimate_stage_ids = 3, run_id = old_id,
                          year_ids = year_list, gbd_year = prev_gbd_year,
                          demographic_metadata = T)
old5q0 = old5q0[, c(..keep_cols, "mean")]
new5q0 = get_mort_outputs("5q0", "estimate",
                          run_id = version_id, estimate_stage_ids = 3,
                          year_ids = year_list, demographic_metadata = T)
new5q0 = new5q0[, c(..keep_cols, "mean")]
compare5q0 = get_mort_outputs("5q0", "estimate", estimate_stage_ids=3,
                              run_id = compare_id, year_ids = year_list,
                              demographic_metadata = T)
compare5q0 = compare5q0[, c(..keep_cols, "mean")]


combined = merge(old5q0, new5q0, by = keep_cols )
combined=merge(combined, compare5q0, by = keep_cols)
setnames(combined, c("mean.x", "mean.y", "mean"),
         c("mean_gbd2019", "mean_current", "mean_decomp1"))
combined_long <- melt(data = combined,
                      id.vars = c(keep_cols, "mean_current"),
                      measure.vars = c("mean_gbd2019", "mean_decomp1"),
                      value.name = "mean_compare")


combined_long[, diff := (mean_current / mean_compare) - 1]
combined_long[, p_difference := diff * 100]



threshold1 = 2
threshold2 = 5
threshold3 = 10

combined_long$category = NULL
combined_long[abs(p_difference) < threshold1,
              category:= paste0("<", threshold1, "%")]
combined_long[abs(p_difference) >= threshold1 & abs(p_difference) < threshold2,
              category:= paste0("<", threshold1, "% ~ <", threshold2,"%")]
combined_long[abs(p_difference) >= threshold2 & abs(p_difference) < threshold3,
              category:= paste0("<", threshold2, "% ~ <", threshold3,"%")]
combined_long[abs(p_difference) >= threshold3,
              category:= paste0(threshold3, "%+")]
combined_long$category <- factor(combined_long$category,
                                 levels = c(paste0(threshold3, "%+"),
                                            paste0("<", threshold2, "% ~ <", threshold3,"%"),
                                            paste0("<", threshold1, "% ~ <", threshold2, "%"),
                                            paste0("<", threshold1, "%")))

gen_plot <- function(subset, year, title){
  p_ctr = sort(abs(subset$diff), decreasing = T)[ceiling(pc_to_label * nrow(subset))]
  subset_plot = ggplot(data = subset, aes(x = mean_current, y = mean_compare)) +
    geom_point(size = 2, aes(shape = variable, color = category)) +
    geom_text_repel(data = subset[abs(diff) >= p_ctr,], aes(label = ihme_loc_id),
                    min.segment.length = 0, segment.size = 0.1, segment.alpha = 0.2) +
    xlab("current 5q0 ") + ylab("comparison 5q0") +
    scale_x_continuous(limits = c(0, max(subset$mean_current)), breaks = pretty_breaks(n = 10)) +
    scale_y_continuous(limits = c(0, max(subset$mean_compare)), breaks = pretty_breaks(n = 10))+
    geom_abline(intercept = 0, slope = 1) +
    theme_bw() + labs(color = "") +
    ggtitle(title) +
    theme(strip.text = element_text(size = 8),
          axis.text = element_text(size = 7),
          legend.position = c(0.95, 0.5),
          legend.justification = c("right", "top"),
          legend.box.just = "right",
          legend.margin = margin(6, 6, 6, 6),
          legend.text = element_text(size = 9),
          panel.grid.minor = element_blank())+
    guides(color = guide_legend(title.position = "top", order = 2))+
    guides(size = guide_legend(title.position= "top", order = 1))

  print(subset_plot)
}



pdf(paste0("FILEPATH"),
    width = 12, height = 8)
for(year in year_list){
  title = paste0("5q0 Estimate Comparison","  Year: " , year)
  subset = combined_long[year_id == year]
  gen_plot(subset, year, title)
}
dev.off()




############# 2. external covariates comparison hiv, ldi, eduction #############

# Helper function for pulling/merging covariates onto the previous round's version
merge_covariates <- function(cov_id) {

  data_path <- paste0("FILEPATH")

  # Pull current version
  if (cov_id == 57) {
    filepath <- paste0("FILEPATH")
    value_col <- "ldi"
  }
  else if (cov_id == 463) {
    filepath <- paste0("FILEPATH")
    value_col <- "maternal_edu"
  }
  else if (cov_id == 214) {
    filepath <- paste0("FILEPATH")
    value_col <- "hiv"
  } else{
    stop("Covariate ID must be one of 57,214,463")
  }

  current <- fread(filepath)
  recent <- fread(gsub(version_id, recent_completed, filepath))
  compare <- fread(gsub(version_id, compare_id, filepath))
  previous <- get_covariate_estimates(covariate_id = cov_id,
                                      release_id = 9,
                                      status = "best")

  # Log transform LDI
  if(cov_id == 57){
    current[, ldi := log(ldi)]
    recent[, ldi := log(ldi)]
    compare[, ldi := log(ldi)]
    previous[, mean_value := log(mean_value)]
  }

  # prevent NaN"s in pct_diff equation
  current[get(value_col) == 0, (value_col) := 1e-10]
  recent[get(value_col) == 0, (value_col) := 1e-10]
  compare[get(value_col) == 0, (value_col) := 1e-10]
  previous[mean_value == 0, mean_value := 1e-10]

  # Merge prep
  setnames(compare, c(paste0(value_col, "_model_version_id"), value_col),
           c(paste0(value_col, "_model_version_id_v", compare_id), paste0(value_col, "_v", compare_id)))

  # Merge together
  output <- merge(current,
                  previous[, .(location_id, location_name, year_id, prev_gbd_round = mean_value)],
                  by = c("location_id", "year_id"), all.x = T)
  output <- merge(output, recent,
                  by=c("location_id", "year_id"), suffixes = c(paste0("_v", recent_completed), "_current"))
  output <- merge(output, compare, by=c("location_id", "year_id"))

  # Find percentage difference and rank pct_diff, cap to 20
  output[, pct_diff_old := abs(get(paste0(value_col, "_current")) - prev_gbd_round) / prev_gbd_round]
  output[, diff_rank_old := frank(-pct_diff_old, ties.method = "first"), by = "year_id"]

  output[, pct_diff_comp := abs(get(paste0(value_col, "_current")) - get(paste0(value_col, "_v", compare_id))) / get(paste0(value_col, "_v", compare_id))]
  output[, diff_rank_comp := frank(-pct_diff_comp, ties.method = "first"), by ="year_id"]

  # Assign percentage different to bin 0-5, 5-10, ... 20+
  brks <- seq(0, 0.2, length = 5)
  output[, bin_old := findInterval(pct_diff_old, brks)]
  output[, bin_old := as.character(bin_old)]

  output[, bin_comp := findInterval(pct_diff_comp, brks)]
  output[, bin_comp := as.character(bin_comp)]

  for(var in c("bin_old", "bin_comp")){
    output[get(var) == 1, (var) := "0-5%"]
    output[get(var) == 2, (var) := "5-10%"]
    output[get(var) == 3, (var) := "10-15%"]
    output[get(var) == 4, (var) := "15-20%"]
    output[get(var) == 5, (var) := "20+%"]
  }

  return(output)

}


ldi <- merge_covariates(57)
maternal <- merge_covariates(463)
hiv <- merge_covariates(214)

# Save the datasets to the graphing folder
readr::write_csv(ldi, paste0("FILEPATH"))
readr::write_csv(maternal, paste0("FILEPATH"))
readr::write_csv(hiv, paste0("FILEPATH"))


# Change scatter
cov_change_scatter <- function(dataset, covariate_name, year, type) {

  # modification for log ldi
  if(covariate_name == "ldi"){
    cov_add <- "log_"
  }else{
    cov_add <- as.character()
  }

  # type arguments for plotting (takes values old or comp only)
  if(type == "old"){
    y_var <- "prev_gbd_round"
    diff_var <- "diff_rank_old"
    y_lab <- paste0(cov_add, covariate_name, " GBD ", prev_gbd_year)
    title_var <- paste0("GBD", prev_gbd_year)
    var_col <- "bin_old"
  }
  if(type == "comp"){
    y_var <- paste0(covariate_name, "_v", compare_id)
    diff_var <- "diff_rank_comp"
    y_lab <- paste0(cov_add, covariate_name, " model v",
                    dataset[1, get(paste0(covariate_name, "_model_version_id_v", compare_id))])
    title_var <- paste0("current best (v", compare_id, ")")
    var_col <- "bin_comp"
  }

  # merge ihme_loc_id and subset
  dataset <- merge(dataset, loc_map, by = "location_id")
  dataset <- dataset[year_id == year &
                       !is.na(get(paste0(covariate_name, "_current"))) &
                       !is.na(get(y_var))]

  p <- ggplot(data = dataset, aes(x = get(paste0(covariate_name, "_current")),
                                y = get(y_var))) +
    geom_point(aes(color = get(var_col)), size = 0.4) +
    ggtitle(paste0(covariate_name, ": Comparison to ", title_var, ", Year ", year)) +
    geom_abline(slope = 1, intercept = 0) +
    theme_bw() +
    xlab(paste0(cov_add, covariate_name, " model v", dataset[1, get(paste0(covariate_name, "_model_version_id_current"))])) +
    ylab(y_lab) +
    labs(color = "Abs % change") +

    geom_text_repel(data = dataset[year_id == year & get(diff_var) <= 20], aes(label = ihme_loc_id))

  return(p)
}

# Output plots of covariate comparisons
pdf(paste0("FILEPATH"))
for (cov_type in c("ldi", "maternal_edu", "hiv")) {

  if (cov_type == "ldi") {
    dataset <- ldi
  } else if(cov_type == "maternal_edu") {
    dataset <- maternal
  } else if(cov_type == "hiv") {
    dataset <- hiv
  }

  for (yr in c(seq(1970, 2010, 10), prev_gbd_year)) {
    plot_obj_old <- cov_change_scatter(dataset, cov_type, yr, "old")
    plot(plot_obj_old)
  }

  for (yr in c(seq(1970, 2010, 10), gbd_year:end_year)) {
    plot_obj_comp <- cov_change_scatter(dataset, cov_type, yr, "comp")
    plot(plot_obj_comp)
  }

}

dev.off()

#################################### 3. covariate coefficient comparsion   ########################################################################


# List directories
current_dir <- paste0("FILEPATH")
compare_dir <- paste0("FILEPATH")
old_dir <- paste0("FILEPATH")



# Pull first stage model from current version
load(paste0("FILEPATH"))
stage_1_current <- model

stage_1_current <- as.data.table(stage_1_current$coefficients$fixed, keep.rownames = T)
stage_1_current <- setnames(stage_1_current, c("V1", "V2"), c("variable_name", "coeff_value_current"))

# Pull first stage model from last run
load(paste0("FILEPATH"))
stage_1_compare <- model

stage_1_compare <- as.data.table(stage_1_compare$coefficients$fixed, keep.rownames = T)
stage_1_compare <- setnames(stage_1_compare, c("V1", "V2"), c("variable_name", "coeff_value_compare"))

# Pull first stage model from last gbd run
load(paste0("FILEPATH"))
stage_1_old <- model

stage_1_old <- as.data.table(stage_1_old$coefficients$fixed, keep.rownames = T)
stage_1_old <- setnames(stage_1_old, c("V1", "V2"), c("variable_name", "coeff_value_old"))

# Combine models and rename vairables
combined_stage_1 <- merge(stage_1_current, stage_1_compare, by="variable_name",
                          all.x = T, all.y = T)
combined_stage_1<-merge(combined_stage_1, stage_1_old, by="variable_name",
                        all.x = T, all.y = T)

combined_stage_1 <- combined_stage_1[variable_name == "beta1", variable_name := "log_ldi"]
combined_stage_1 <- combined_stage_1[variable_name == "beta2", variable_name := "maternal_edu"]
combined_stage_1 <- combined_stage_1[variable_name == "beta3", variable_name := "hiv"]

combined_stage_1[, percent_change_current_compare :=
                   (coeff_value_current - coeff_value_compare) / abs(coeff_value_compare) * 100]
combined_stage_1[, percent_change_current_old :=
                   (coeff_value_current-coeff_value_old) / abs(coeff_value_old) * 100]

# Output comparison file
fwrite(combined_stage_1, paste0("FILEPATH"))

