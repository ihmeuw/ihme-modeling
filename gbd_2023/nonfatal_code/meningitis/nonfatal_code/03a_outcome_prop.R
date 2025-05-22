#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Description:	Setup draws for outcome fraction for all meningitis
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, openxlsx, data.table, dplyr, boot, meta, metafor, grid)
# Pull in crosswalk packages 
Sys.setenv("RETICULATE_PYTHON" = "FILEPATH") 
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

# Source shared functions-------------------------------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# User specific options --------------------------------------------------------
# pull demographics from RDS created in step 01
demographics <- readRDS(file.path("FILEPATH"))
years <- demographics$year_id
sexes <- demographics$sex_id

# Inputs -----------------------------------------------------------------------
# Load results from systematic review
if(etiology == "bacterial") review_name <- "Edmond"
if(etiology == "viral") review_name <- "Hudson"

# Helper function --------------------------------------------------------------
confidence_intervals <- function(dt) {
  dt[, lower := apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = (paste0("draw_", 0:999))]
  dt[, mean  := rowMeans(.SD), .SDcols = (paste0("draw_", 0:999))]
  dt[, upper := apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = (paste0("draw_", 0:999))]
  
  dt[, paste0("draw_",0:999) := NULL]
  return(dt)
}

# Run job ----------------------------------------------------------------------
if(etiology == "bacterial"){
  # load SR 
  health_dist <- read.xlsx(file.path("FILEPATH"), sheet="data")
  setDT(health_dist)
  
  # Run regression between GDP and outcome proportions
  gdp_cov <- get_covariate_estimates(covariate_id = 851, release_id = release)
  
  seq_path <- file.path("FILEPATH")
  seq_prop <- fread(seq_path)
  seq_prop <- merge(seq_prop, gdp_cov, by = c("location_id", "year_id"))
  setnames(seq_prop, c("mean_value", "proportionwith1majorsequela"), c("gdp", "prop_major"))
  seq_prop[, cases := n*prop_major]
  seq_prop[, lngdp := log(gdp)]
  
  # run regression --  this is a linear regression when in the form of binomial outputs proportion values in logit space so they're bounded between 0 and 1
  seq_prop.glm <- glm(prop_major ~ lngdp, data = seq_prop, family = "binomial", weights = n)
  
  # print the summary
  sink(paste0("/FILEPATH"))
  print(summary(seq_prop.glm))
  sink()  # returns output to the console

  # save file with GDP alongside values from Edmond
  fwrite(seq_prop, paste0("FILEPATH"))
  
  # create prediction variables -- want to predict these values still in logit space because that's where the linear model is
  prop_major <- gdp_cov[year_id %in% years]
  prop_major[, lngdp := log(mean_value)]
  prop_major <- prop_major[,.(location_id, lngdp, year_id)]
  predictions <- predict(seq_prop.glm, prop_major, se.fit = TRUE)
  prop_major$pred_mean <- predictions$fit
  prop_major$pred_se <- predictions$se.fit
  
  # create 1000 draws -- need to invlogit these because they've been calculated in logit space
  matrix_draws <- inv.logit(rnorm(n=1000 * length(prop_major$pred_mean), mean=prop_major$pred_mean, sd=prop_major$pred_se)) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
  names(matrix_draws) <- paste0("draw_", 0:999)
  prop_major <- cbind(prop_major, matrix_draws)
  
  # save!
  fwrite(prop_major, file.path("FILEPATH"))

  # Calculating proportions of >1 major outcome for each etiology
  rows.remove <- grep("Minor|IQR", health_dist$sequela)
  health_dist <- health_dist[!c(rows.remove)]
  
  # create normalized proportions: risk of outcome / all outcomes (not including clinical or minor impairments) for ALL meningitis
  major_temp <- health_dist[sequela == "At least one major sequela", meningitis_all_mean]
  clinical <- health_dist[sequela == "Major clinical impairments", meningitis_all_mean]
  major <- major_temp - clinical
  health_dist[!sequela %in% c("At least one major sequela", "At least one minor sequela"), 
              meningitis_all_mean := meningitis_all_mean / major]
  
  # Rename outcomes
  rows.remove <- grep("Major clinical impairments", health_dist$sequela)
  health_dist <- health_dist[!c(rows.remove)]
  major_cog_seq <- c("Major cognitive difficulties", "Major motor deficit", "Major multiple impairments")
  health_dist[sequela %in% major_cog_seq,              outcome := "major_mort1"]
  health_dist[sequela == "At least one minor sequela", outcome := "minor"]
  health_dist[sequela == "Major visual disturbance",   outcome := "major_mort0"]
  health_dist[sequela == "Major hearing loss",         outcome := "major_mort_"]
  health_dist[sequela == "Major seizure disorder",     outcome := "seizure"]
  health_dist[sequela == "At least one major sequela", outcome := "major"]
  
  # Proportions for major_mort0, major_mort1, major_mort_, and seizure
  se.sd.median.cols <- grep(".se|.sd|.median", colnames(health_dist))
  # Drops at least one major and at least one minor row
  # Also drops *se, *sd, and *median columns
  major_prop <- health_dist[2:7, !se.sd.median.cols, with = FALSE]
  major_prop <- major_prop[, lapply(.SD, sum), by=outcome, .SDcols = c("meningitis_all_mean")] # Summed across outcomes
  major_prop <- major_prop[order(outcome)]
  fwrite(major_prop, paste0(tmp_dir, "FILEPATH"))
  
  # Creating draws for major_all and minor_all
  major_minor_draws <- function(sev) { 
    mm <- health_dist[health_dist$outcome == sev, meningitis_all_mean]
    ss <- health_dist[health_dist$outcome == sev, meningitis_all_se] 
    # Setting beta distribution shape parameters (method of moments, for a non-traditional proportional distribution)
    # Constrained to give a proportion, can be over or under dispersed relative to other distributions defined by alpha and beta
    alpha <- (mm * (mm - mm^2 - ss^2)) / ss^2
    beta <- (alpha * (1 - mm)) / mm
    beta_draw <-  rbeta(n = 1000, shape1 = alpha, shape2 = beta)
    
    severity_tmp <- data.table(col = paste0("v_", 0:999),
                               draws = beta_draw)
    # Transpose data table
    severity_tmp <- dcast(melt(severity_tmp, id.vars = "col"), variable ~ col)
    severity_tmp[, variable := NULL]
    return(severity_tmp)
  }
  
  # Calculate ratio draws of minor against major 
  minor.dt <- major_minor_draws('minor')
  colnames(minor.dt) <- gsub("v", "w", colnames(minor.dt))
  major.dt <- major_minor_draws('major')
  minor.ratio.dt <- cbind(major.dt, minor.dt)
  # Replace minor with 1 if greater than major
  # Else replace minor with minor/major if less than major, normalizing proportion of minor to major
  minor.ratio.dt[, paste0("draw_",0:999) := lapply(0:999, function(i) { 
    ifelse( get(paste0("w_",i)) / get(paste0("v_",i)) > 1, 1, get(paste0("w_",i)) / get(paste0("v_",i)))
  })]
  minor.ratio.dt[, paste0("v_", 0:999) := NULL]
  
  # Make proportion draws for each etiology/outcome/location/year
  # Loop over all outcomes except major
  # This makes etiology and outcome specific files that have proportional draws for every location/year
  outcomes <- unique(health_dist[outcome != "major", outcome])
  for (out in outcomes) {
    prop_tmp <- melt(prop_major, id.vars = c("year_id", "location_id"))
    if (out == "minor") {
      minor.ratio.dt <- melt(minor.ratio.dt, measure.vars = paste0("draw_", 0:999), value.name = "minor_ratio")
      prop_tmp <- merge(prop_tmp, minor.ratio.dt, by = c("variable"))
      prop_tmp[, value := value * minor_ratio]
    } else {
      major_prop_vector <- major_prop[outcome == out, meningitis_all_mean]
      prop_tmp[, value := value * major_prop_vector]
    }
    prop_tmp <- dcast(prop_tmp, location_id + year_id ~ variable, value.var = "value")
    prop_tmp[, measure_id := 6] # measure id for incidence
    if (out == "minor") {
      group <- "long_mild"
    } else if (out == "major_mort1") {
      group <- "long_modsev"
    } else if (out == "seizure") {
      group <- "epilepsy"
    } else if (out =="major_mort_") {
      group <- "_hearing"
    } else if (out == "major_mort0") {
      group <- "_vision"
    }
    prop_tmp[, grouping := group]
    saveRDS(prop_tmp, file.path("FILEPATH"))
  }
} else if (etiology == "viral"){
  # Use one global meta-analyzed value
  seq_path <- file.path("FILEPATH")
  seq_prop <- as.data.table(read.xlsx(seq_path))
  # drop first row
  seq_prop <- seq_prop[2:nrow(seq_prop),]
  # delete blank spacer rows
  seq_prop <- seq_prop[!is.na(field_citation_value)]
  # formatting fixes
  seq_prop[, author := stringr::word(field_citation_value, 1)]
  seq_prop[,(c("cases", "sample_size", "discharge")) := lapply(.SD, as.numeric), .SDcols = c("cases", "sample_size", "discharge")]
  # include only POST-discharge results.
  seq_prop <- seq_prop[discharge == 0]
  # drop group_review == 0
  seq_prop <- seq_prop[is.na(group_review) | group_review == 1]
  # Remove clinical
  seq_prop <- seq_prop[impairment_category != "clinical"]
  # Do this for ALL proportions - 
  for (sev in c("major", "minor")){
    result_list <- list()
    squeezed_summary_list <- list()
    for (sequela in unique(seq_prop[severity == sev]$impairment_category)){
      # use meta package to run a meta-analysis, using logit transformation
      dat <- copy(seq_prop[impairment_category == sequela & severity == sev])
      meta_seq <- metaprop(event = cases, n = sample_size, studlab = author, sm = "PLOGIT", data = dat, title = paste(sev, sequela))
      # write out the results
      # print the summary
      sink(paste0(tmp_dir, "/FILEPATH"))
      print(summary(meta_seq))
      sink()  # returns output to the console
      pdf(paste0(tmp_dir, "FILEPATH"), width = 11, height = 8)
      forest(meta_seq)
      grid.text(paste(sev, sequela), .5, .8, gp=gpar(cex=2))
      dev.off()
      
      # if all inputs are 0, don't write out this sequela
      result <- cbind(unique(dat[,.(impairment_category, severity)]), 
                      mean = ifelse(all(unique(dat$cases) == 0), 0, inv.logit(meta_seq$TE.random)), 
                      lower = ifelse(all(unique(dat$cases) == 0), 0, inv.logit(meta_seq$lower.random)), 
                      upper = ifelse(all(unique(dat$cases) == 0), 0, inv.logit(meta_seq$upper.random)), 
                      mean_logit = meta_seq$TE.random,
                      se_logit = meta_seq$seTE.random) 
      result_list[[sequela]] <- result
    }
    result_all <- rbindlist(result_list)
    setorder(result_all, impairment_category)
    fwrite(result_all, paste0(tmp_dir, "/FILEPATH"))

    # for each of the minor and major envelopes:
    # create normalized proportions: risk of outcome / all outcomes (not including clinical impairments)
    all_outcomes <- result_list[["at_least_one"]]
    
    # combine motor & cognitive
    result_all[impairment_category %in% c("motor", "cognitive"), impairment_category := "motor_cog"]
    result_all <- result_all[, lapply(.SD, sum), by = c("impairment_category", "severity"), .SDcols = c("mean")] # Summed across outcomes
    
    # For the normalized %'s, take mean of the specific sequela / mean of all outcomes.
    normalized <- cbind(result_all[impairment_category != "at_least_one",.(impairment_category, severity, mean)], all_outcomes_mean = all_outcomes$mean)
    normalized[, normalizing_multiplier := mean / all_outcomes_mean]
    
    # Squeeze them to 1
    normalized[, normalizing_multiplier := normalizing_multiplier/sum(normalizing_multiplier)]
    normalized[, mean_normalized := normalizing_multiplier*all_outcomes_mean]
    # Write this out for comparison
    fwrite(normalized, paste0(tmp_dir, "FILEPATH"))
    
    # get draws for % any outcome
    # create 1000 draws -- need to get these back into linear because they've been calculated in logit space
    # use delta method to transform back to linear
    linear_vals <- cw$utils$logit_to_linear(mean = array(all_outcomes$mean_logit), sd = array(all_outcomes$se_logit))
    linear_vals <- as.data.table(linear_vals)
    
    # set names 
    setnames(linear_vals, old = c("V1", "V2"), new = c("mean_linear", "sd_linear"))
    
    # use beta distribution like for bacterial
    sigma <- linear_vals$sd_linear
    mu <- linear_vals$mean_linear
    # Setting beta distribution shape parameters (method of moments, for a non-traditional proportional distribution)
    # Constrained to give a proportion, can be over or under dispersed relative to other distributions defined by alpha and beta
    # alpha & beta formulas from exercise 1 in : https://www.statlect.com/probability-distributions/beta-distribution 
    alpha <- ((mu^2 - mu^3) / sigma^2) - mu
    beta <- (alpha * (1 - mu)) / mu
    all_outcomes_draws <- rbeta(n = 1000, shape1 = alpha, shape2 = beta) %>% matrix(., ncol=1000, byrow=FALSE) %>% as.data.table
    drawnames <- paste0("draw_", 0:999)
    names(all_outcomes_draws) <- drawnames
    all_outcomes_draws <- cbind(all_outcomes_mean = all_outcomes$mean, all_outcomes_draws)

    # Make proportion draws for each etiology/outcome/location/year
    # Loop over all outcomes 
    # This makes etiology and outcome specific files that have proportional draws for every location/year
    if(sev == "major") outcomes <- unique(normalized$impairment_category)
    if(sev == "minor") outcomes <- unique(normalized$impairment_category)[!unique(normalized$impairment_category) %in% c("hearing")] #ignore mild hearing, make mild vision vision_mono
    for (out in outcomes) {
      # multiply the normalized proportion * the overall draw
      prop_tmp <- cbind(normalized[impairment_category == out,.(impairment_category, normalizing_multiplier)], all_outcomes_draws)
      prop_tmp[, (drawnames) := lapply(.SD, "*", normalizing_multiplier), .SDcols = drawnames]
      prop_tmp$all_outcomes_mean <- NULL; prop_tmp$normalizing_multiplier <- NULL
      prop_tmp[, measure_id := 6] # measure id for incidence
      if (out == "motor_cog") {
        if(sev == "minor") group <- "long_mild" else if (sev == "major") group <- "long_modsev"
      } else if (out == "behavior") {
        group <- "behavior"
      } else if (out == "seizures") {
        group <- "epilepsy"
      } else if (out =="hearing") {
        group <- "_hearing"
      } else if (out == "vision") {
        if(sev == "minor") group <- "vision_mono" else if(sev == "major") group <- "_vision"
      }
      prop_tmp[, grouping := group]
      # Summarize prop_tmp and write it out
      prop_tmp_summary <- confidence_intervals(as.data.table(prop_tmp))
      squeezed_summary_list[[out]] <- prop_tmp_summary
      # expand.grid to all location-years
      loc_year_grid <- expand.grid(location_id = unique(demographics$location_id), year_id = years)
      prop_tmp <- cbind(loc_year_grid, prop_tmp)
      saveRDS(prop_tmp, file.path("FILEPATH")))
    }
    squeezed_summary <- rbindlist(squeezed_summary_list)
    fwrite(squeezed_summary, paste0(tmp_dir, "FILEPATH"))
  }

}

print(paste(step_num, step_name, "sequential runs completed"))

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
print(paste0(tmp_dir, "/FILEPATH"))
file.create(paste0(tmp_dir, "/FILEPATH"), overwrite=T)
# ------------------------------------------------------------------------------