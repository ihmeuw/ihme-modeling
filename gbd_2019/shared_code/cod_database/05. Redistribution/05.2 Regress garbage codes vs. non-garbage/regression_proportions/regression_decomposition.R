library("ihme", lib.loc = "FILEPATH")

library(ggplot2)
library(data.table)



gbd_2017_rlsid <- 20
decomp_path <- c(20, 38, 42, 44, 67, 68, 69, 75, 81, 82) # stroke as of 3/7/19
#decomp_path <- c(19, 45, 57, 73) # diabetes as of 2/26/19
launch_set_base <- get_regression_launch_set(gbd_2017_rlsid)



# Read in/merge predictions -----------------------------------------------


fread_master_predictions <- function(launch_set) {
  dt <- fread(paste0(REGDIR, "ICD_cod/", launch_set$shared_package_id, "/",
                     launch_set$regression_launch_set_id, "/_master_predictions.csv"))
  dt <- dt[, c("model_group", "age", "age_full", "location_id", "location_name","region_name",
               "super_region_name", "year_id", "sex", "cause_id", "cause_name", "level", "prop_target_scaled")]
  setnames(dt, "prop_target_scaled", paste("prop_target_scaled", launch_set$regression_launch_set_id, sep = "_"))
}


create_percent_change_cols <- function(rlsid_path) {
  preds <- fread_master_predictions(get_regression_launch_set(rlsid_path[1]))
  
  for (rlsid in rlsid_path[-1]) {
    step_preds <- fread_master_predictions(get_regression_launch_set(rlsid))
    
    preds_rlsid <- gsub("prop_target_scaled_", "", names(preds)[grepl("prop_target", names(preds))])
    preds <- merge(preds, step_preds)
    
    preds[, paste("pct_change", preds_rlsid, rlsid, sep = "_") := (get(paste0("prop_target_scaled_", rlsid)) - get(paste0("prop_target_scaled_", preds_rlsid))) / get(paste0("prop_target_scaled_", preds_rlsid))]
    preds[, paste("abs_change", preds_rlsid, rlsid, sep = "_") := (get(paste0("prop_target_scaled_", rlsid)) - get(paste0("prop_target_scaled_", preds_rlsid)))]
    preds[, eval(paste0("prop_target_scaled_", preds_rlsid)) := NULL]
  }
  
  return(preds)
}

change_preds <- create_percent_change_cols(decomp_path)
change_preds[, abs_change_all := NA_real_]

for (col in grep("abs_change_[0-9]", names(change_preds), value = T)) {
  change_preds[ , abs_change_all := mapply(sum, abs_change_all, change_preds[[col]], na.rm = T)]
}


# Cast long and plot ------------------------------------------------------

change_preds <- melt(change_preds, id.vars = c("model_group", "age", "age_full", "location_id", "location_name","region_name",
                                               "super_region_name", "year_id", "sex", "cause_id", "cause_name", "level"), 
                     measure.vars = patterns("abs_change"),
                     variable.name = "version_step", value.name = "abs_change")

# change order so FIRST change shows up at top, last is bottom, etc
change_preds[, version_step := factor(version_step, levels = rev(levels(version_step)), ordered = TRUE)]

dir <- paste0(h, "FILEPATH", gsub(" ", "_", launch_set_base$shared_package_name), "FILEPATH")
dir.create(dir)

pdf(paste0(dir, "decomp_", Sys.Date(), ".pdf"), height = 10, width = 12, paper = "a4r")

for (group in unique(change_preds$model_group)) {
  cause <- unique(change_preds[model_group == group, cause_name])
  sex_name = unique(change_preds[model_group == group, sex])
  ages_allowed <- unique(change_preds$age_full)
  if (launch_set_base$shared_package_id == 2614) {
    # for diabetes bc we force <15 to all type 1, this is messing up
    # age color key in type 2
    ages_allowed <- ages_allowed[!ages_allowed %in% "0 - 14"]
  }
  
  g <- ggplot(change_preds[level == 3 & cause_name == cause & sex == sex_name & age_full %in% ages_allowed]) +
    geom_boxplot(aes(version_step, abs_change, fill = age_full), outlier.shape = NA) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) +
    geom_hline(yintercept = 0, linetype = "dashed") + 
    scale_y_continuous(limits = quantile(change_preds$abs_change, c(0.01, 0.99), na.rm = T)) +
    labs(title = paste0(launch_set_base$shared_package_name, " decomposition - ", cause, ", ", sex_name),
         y = "Absolute change in redistribution proportions compared to previous version") +
    theme_light() +
    coord_flip()
  
  print(g)
}

dev.off()

