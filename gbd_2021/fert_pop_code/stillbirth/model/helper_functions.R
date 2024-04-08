####################################################################################
##                                                                                ##
## Purpose: Load helper functions used in ST-GPR custom linear ensemble code      ##
##                                                                                ##
####################################################################################

library(data.table)
library(parallel)
library(plyr)
library(RMySQL)
library(stringr)

#######################
### TEST FOR BLANKS ###
#######################

is.blank <- function(x) {
  any(is.null(x)) || any(is.na(x)) || any(is.nan(x))
}

#########################
### OFFSET DATA 0s/1s ###
#########################

offset.data <- function(df, data_transform, offset) {
  ## Offset 0's if logit or log
  if ((nrow(df[data == 0 | data == 1]) > 0) & (data_transform %in% c("logit", "log"))) {
    df[data == 0, data := data + offset]
    ## Offset 1's if logit
    if (data_transform == "logit") {
      df[data == 1, data := data - offset]
    }
  } else{
    print("No data need offsetting!")
  }
  return(df)
}

#################################################
############ TRANSFORM_DATA FUNCTION ############
#################################################

transform_data <- function(var, space, reverse=F) {
  if (space == "logit" & reverse==F) {
    var <- logit(var)
  } else if (space == "logit" & reverse==T) {
    var <- inv.logit(var)
  } else if (space == "log" & reverse==F) {
    var <- log(var)
  } else if (space == "log" & reverse==T) {
    var <- exp(var)
  }

  return(var)

}

##################################
## RESTRICT VIOLATIONS FUNCTION ##
##################################

# Description: This function reviews the potential covariate combinations for models.
# Parameters:
#   1. rmses - data.frame where each row represent information about a potential model
#   2. covs - list of covariates
#   3. prior_sign - list of values (-1, 0, 1) indicating the expected direction of relationship between mean and covariate
#   4. p_value - threshold to test for significance

restrict_violations <- function(rmses, covs, prior_sign = NULL, p_value = .05) {

  ## Remove based on sign violation

  rmses[, sign_violation := 0]

  if (!is.null(prior_sign)) {

    message("Removing models that violate prior signs")
    signs <- data.table(sig = prior_sign, cov = covs)

    for(i in 1:nrow(signs)) {

      cov <- signs[i, cov]
      sign <- signs[i, sig]

      if (sign == -1) {

        message(cov, " must be negative")
        message("  Dropping ", nrow(rmses[get(paste0(cov, "_fixd")) > 0,])," model(s) where ", cov, " is greater than 0")

        rmses[get(paste0(cov, "_fixd")) > 0, sign_violation := 1]

      }

      if (sign == 1) {

        message(cov, " must be positive")
        message("  Dropping ", nrow(rmses[get(paste0(cov, "_fixd")) < 0,])," model(s) where ", cov, " is less than 0")

        rmses[get(paste0(cov, "_fixd")) < 0, sign_violation := 1]

      }

    }

  }

  ## Remove based on significance violation

  z <- qnorm(p_value/2, mean = 0, sd = 1, lower.tail = F)
  rmses[, sig_violation := 0]

  invisible(

    lapply(covs, function(x) {

      lowers <- rmses[, get(paste0(x, "_fixd")) - z * get(paste0(x, "_fixd_se"))]
      uppers <- rmses[, get(paste0(x, "_fixd")) + z * get(paste0(x, "_fixd_se"))]

      temp <- data.table(lower = lowers, upper = uppers)
      temp[, insig := ifelse(data.table::between(0, lower, upper), 1, 0)]
      temp[is.na(lower) & is.na(upper), insig := 0]

      message(nrow(temp[insig == 1, ]), " models have p>", p_value, " for ", x)

      rmses[temp$insig == 1, sig_violation := 1]
      rmses[temp$insig == 0, sig_violation := 0]

  })

  )

  ## Finalize Output

  rmses[sig_violation == 1 | sign_violation == 1, drop := 1]
  rmses[is.na(drop), drop := 0]

  return(rmses)

}

###############################
## CLUSTER JOB HOLD FUNCTION ##
###############################

# Description: This function holds jobs until specific jobs have finished.
# Parameters:
#   1. job_name - name of job
#   2. file_list - list of files to check for
#   3. obj - name of object in h5 files

job_hold <- function(job_name, file_list = NULL, obj = NULL, resub = 0) {

  ## Give it a sec to launch
  Sys.sleep(5)

  ## Start timer
  start.time <- proc.time()

  ## Wait for job to finish
  flag <- 0

  while (flag == 0) {

    ## Check if job is done
    if (system(paste0("squeue -r | grep ", job_name, "|wc -l"), intern = T) == 0) {

      ## If so, set flag to 1
      flag <- 1

    } else {

      Sys.sleep(5)

    }

  }

  ## End Timer
  job.runtime <- proc.time() - start.time
  job.runtime <- job.runtime[3]

  ## Give it another sec
  Sys.sleep(10)

  ## Check for the file list
  if (!is.null(file_list)) {

    missing_list <- NULL

    for (file in file_list) {

      ## Ensure that all files are there
      if (!file.exists(file)) {

        missing_list <- rbind(missing_list, file)

      } else {

        if (grepl(".h5", file_list[1])) {

          if (!(obj %in% h5ls(file_list)$name)) {

            missing_list <- rbind(missing_list, file)

          }

        }

      }

    }

    ## If missing_list > 0, break
    if (length(missing_list) > 0) {

      if (resub == 0) {

        stop(paste0("Job failed: ", job_name,
                    "\nTime elapsed: ", job.runtime,
                    "\nYou are missing the following files: ", toString(missing_list)))

      } else {

        return(1)

      }

    } else {

      return(0)

    }

  }

  ## Complete
  print(paste0("Job ", job_name, " has completed. Time elapsed: ", job.runtime))

}

##########################
## APPEND PDFS FUNCTION ##
##########################

# Description: This function combines multiple pdfs into one pdf.
# Parameters:
#   1. folder - location with files you want appended together
#   2. pattern - list of files to check for
#   3. output - location to save the pdf output
#   4. rm - where to remove input files or not

append_pdfs <- function(folder, pattern, output, rm = F) {

  files <- list.files(folder, full.names = T) ## getting all of the files
  inputs <- grep(pattern, files, value = T)   ## grepping for the pattern

  input <- gsub(",", "", toString(inputs))    ## the ghostscript needs all of the files to look like this

  ## the command itself is from Patty's append_pdf function, which is embedded in ST-GPR code
  cmd <- paste0("/usr/bin/ghostscript -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=", output, " ", input)
  system(cmd)

  if (rm) {

    invisible(lapply(inputs, unlink))
    # unlink(dirname(inputs[1]), recursive = T)

  }

}

###############################
## GET RECENT FILES FUNCTION ##
###############################

# Description: This function looks for the most recent version of a file in a folder.
# Parameters:
#   1. folder - location where file you want to read in is saved
#   2. pattern - string with part of file name
#   3. sheet - which sheet to read in (if xlsx file)
#   4. path - if T, returns filepath only; if F, reads in file

get_recent <- function(folder, pattern = NULL, sheet = NULL, path = F) {

  require(data.table)

  files <- list.files(folder, full.names = T, pattern = pattern)
  files <- files[!grepl("\\~\\$", files)]
  infoo <- file.info(files)

  most_recent_path <- row.names(infoo[infoo$mtime == max(infoo$mtime),])

  if (path == T) {

    message(paste("Most recent file: ", most_recent_path))
    return(most_recent_path)

  } else {

    ## get file type
    if (grepl(".csv", most_recent_path)) {

      recent <- fread(most_recent_path)

    }

    if (grepl(".rds", most_recent_path)) {

      recent <- readRDS(most_recent_path)

    }

    if (grepl(".xlsx", most_recent_path)) {

      require(openxlsx)

      if (length(sheet) == 0) {

        message(" Reading an xlsx file, but no sheet name given, reading first sheet")
        sheet <- 1

      }

      recent <- read.xlsx(most_recent_path, sheet = sheet)
      recent <- as.data.table(recent)

    }

    message(paste("Most recent file: ", most_recent_path))
    return(recent)

  }

}

##########################
## MAKE SQUARE FUNCTION ##
##########################

# Description: This function runs the get_demographics function and can merge on covariates.
# Parameters:
#   1. by_sex - 0 = both sexes combined; 1 = sex-specific
#   2. by_age - 0 = all ages; 1 = age-specific
#   3. custom_sex_id - uses custom sex_id instead of 1 & 2 or 3
#   4. custom_age_group_id - uses custom age_group_id instead of using 22 or pulling with get_ages
#   5. covariates - list of covariates
#   6. population - if T, returns filepath only; if F, reads in file

make_square <- function(location_set_id, gbd_round_id,
                        year_start, year_end,
                        by_sex = 1, by_age = 1,
                        custom_sex_id = NULL, custom_age_group_id = NULL,
                        covariates = NULL, population = FALSE) {

  ## Skeleton
  df <- get_demographics(location_set_id, gbd_round_id,
                         year_start, year_end,
                         by_sex, by_age,
                         custom_sex_id, custom_age_group_id)

  ## Covariates
  prediction_years <- seq(year_start, year_end)

  if (!is.null(covariates) & !is.na(covariates)) {

    covs <- get_covariates(list = covariates, location_set_id = location_set_id, gbd_round_id = gbd_round_id, prediction_years = prediction_years)
    df <- age_sex_merge(df, covs)

  }

  return(df)

}

###############################
## GET DEMOGRAPHICS FUNCTION ##
###############################

# Description: This function creates a data.frame of the wanted location-year-age-sex combinations.
# Parameters:
#   1. by_sex - 0 = both sexes combined; 1 = sex-specific
#   2. by_age - 0 = all ages; 1 = age-specific
#   3. custom_sex_id - uses custom sex_id instead of 1 & 2 or 3
#   4. custom_age_group_id - uses custom age_group_id instead of using 22 or pulling with get_ages

get_demographics <- function(location_set_id,
                             gbd_round_id,
                             year_start, year_end,
                             by_sex = 1, by_age = 1,
                             custom_sex_id = NULL, custom_age_group_id = NULL) {
  ## Locations
  locs <- get_location_metadata(location_set_id = location_set_id, gbd_round_id = gbd_round_id)
  locs <- locs[level >= 3]$location_id

  ## Years
  years <- seq(year_start, year_end, 1)

  ## Sexes
  if (is.blank(custom_sex_id)) {

    if (by_sex == 1) {

      sexes <- c(1, 2)

    } else if (by_sex == 0) {

      sexes <- 3

    }

  } else {

    sexes <- custom_sex_id

  }

  ## Ages
  if (is.blank(custom_age_group_id)) {

    if (by_age == 1) {

      ages <- get_ages()$age_group_id

    } else if (by_age == 0) {

      ages <- 22

    }

  } else {

    ages <- custom_age_group_id

  }

  ## Expand
  df <- data.table(expand.grid(location_id = locs, year_id = years, sex_id = sexes, age_group_id = ages))

  ## Force integer
  df <- df[, lapply(.SD, as.character)]
  df <- df[, lapply(.SD, as.integer)]

  return(df)

}
