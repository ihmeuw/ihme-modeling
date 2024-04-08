#########################################################################################################################################
##                                                                                                                                     ##
## Purpose: Create holdouts from a dataset                                                                                             ##
##                                                                                                                                     ##
## prep_ko()                                                                                                                           ##
##   - This function preps a dataset for the get_kos() function                                                                        ##
##   - It requires the make_square() function in the central ubcov repo                                                                ##
##   - Arguments:                                                                                                                      ##
##       - data: A data table that contains the following columns:                                                                     ##
##           - "data": The data of the modelable entity of interest                                                                    ##
##           - "location_id": IHME location_id                                                                                         ##
##           - "year_id': IHME year_id                                                                                                 ##
##   - Outputs:                                                                                                                        ##
##       - A list of four objects:                                                                                                     ##
##           1: A data.table containing a square dataset -> contains the original dataset as well as all missing locs-years-ages-sexes ##
##           2: A data.table of the original dataset where the "data" column has no missing values                                     ##
##           3: A vector of all location_ids you are modelling (as defined by location_version_id)                                     ##
##           4: A vector of all location_ids for which there is any data in the original 'data' data.table                             ##
##                                                                                                                                     ##
## get_kos()                                                                                                                           ##
##   - Creates knockouts from a dataset prepped by the pre_ko() function                                                               ##
##   - Currently holding out data by replicating patterns of missingness in the age-year matrix of one location in another             ##
##   - Arguments:                                                                                                                      ##
##       - REQUIRED                                                                                                                    ##
##           - square_data: A data.table containing a square dataset -> contains the original dataset as well as all                   ##
##                          missing locs-years-ages-sexes                                                                              ##
##           - no_miss_data: A data.table of the original dataset where the "data" column has no missing values                        ##
##           - all_locs: A vector of all location_ids you are modelling (as defined by location_version_id)                            ##
##           - data_locs: A vector of all location_ids for which there is any data in the original 'data' data.table                   ##
##       - OPTIONAL                                                                                                                    ##
##           - kos: Integer, number of knockouts to perform.  Default is 5                                                             ##
##           - prop_to_hold: Numeric, proportion of the total dataset to hold out for each knockout (default is 0.30)                  ##
##           - seed: Numeric, seed to set for holding out data                                                                         ##
##           - drop_nids: Binary, if TRUE, whole NIDs will be dropped instead of just single age-sex points                            ##
##   - Outputs:                                                                                                                        ##
##       - A single data.table that is the same as the original data as supplied to prep_ko(),                                         ##
##         but with the added columns "ko*", for ko in kos                                                                             ##
##           - Each ko* column contains a binary 0 or 1                                                                                ##
##           - If a row is marked as 1, then it has been designated to be held out for that knockout                                   ##
##                                                                                                                                     ##
#########################################################################################################################################

library(data.table)
library(ini)

central <- "FILEPATH"

source(paste0(central, "get_location_metadata.R"))

source(paste0(working_dir, "helper_functions.R"))

#############
## PREP_KO ##
#############

prep_ko <- function(data, location_set_id = 22, start_year = 1980, end_year = 2022, by_sex = 1, by_age = 1, gbd_round_id = 7) {

  if (F) {

    location_set_id = 22
    start_year = 1980
    end_year = 2019
    by_sex = 0
    by_age = 0
    remove_subnats = F

  }

  ## get level from location metadata if it isn't already there
  locs <- get_location_metadata(location_set_id = location_set_id, gbd_round_id = gbd_round_id)
  l_set_v_id <- unique(locs$location_set_version_id)
  message("Subsetting to GBD standard locations for beta estimation...")

  stnd_locs <- get_location_metadata(101, gbd_round_id = gbd_round_id)
  stnd_locs <- stnd_locs[, .(location_id, level)]

  data <- data[location_id %in% unique(stnd_locs$location_id),]
  message("Done")

  ## if there are multiple sexes in the data, by_sex should be 0 for now
  if (length(unique(data$sex_id)) > 1) {

    if ("temp_sex" %in% names(data)) stop(" 'temp_sex' is the name of a column in your data, this will cause problems")

    data[, temp_sex := sex_id]
    data[, sex_id := 3]

  }

  ## get square, the function in db_tools script ->
  # if 1 and 2 are in sex_id, by_sex needs to be 1
  # if unique sex_id is 3, then by_sex should be 0
  message("Creating square w/ STGPR function..")
  sqr <- make_square(location_set_id, gbd_round_id = gbd_round_id, start_year, end_year, covariates = NA, by_age = by_age, by_sex = by_sex)
  message("Done")

  ## no missing data
  no_na_data <- data[!is.na(data),] #.(location_id, year_id, age_group_id, data)]
  sqr[, sex_id := NULL]
  data.sqr <- merge(data, sqr, by = c("location_id", "year_id", "age_group_id"), all.y = T, all.x = T)

  ## whole square
  data.sqr <- data.sqr[, .(location_id, year_id, age_group_id, data)]

  ## get all locations
  locations1 <- unique(data.sqr$location_id)

  ## get locations w/ data
  locations2 <- unique(no_na_data$location_id)

  list(data.sqr, no_na_data, locations1, locations2)

}

#############
## GET_KOS ##
#############

get_kos <- function(square_data, no_miss_data, all_locs, data_locs, kos = 5, prop_to_hold = .30,
                    no_new_ages = T, only_data_locs = F, seed = 32, drop_nids = T) {

  if (F) {

    ko <- 1
    all_locs <- ko_items[[3]]
    data_locs <- ko_items[[4]]
    square_data <- ko_items[[1]]
    no_miss_data <- ko_items[[2]]
    prop_to_hold <- .250
    seed <- 32595
    kos <- 10

  }

  ## can overwrite all locations to include at least some data <- this avoids dropping 100% of the data in a location
  if (only_data_locs == T) {

    all_locs <- data_locs

  }

  no_na_data <- copy(no_miss_data)
  no_na_data[, data_id := seq_len(.N)]
  set.seed(seed)

  for(ko in 1:kos) {

    ## randomize locs
    all_locs <- sample(all_locs, length(all_locs))
    data_locs <- sample(data_locs, length(data_locs))

    ## create a loc1_loc2 var
    loc_link <- data.table(loc1 = all_locs[1:length(data_locs)], loc2 = data_locs)

    data_to_ko <- copy(no_na_data)
    # data_to_ko[, sex_id := NULL]

    data_to_ko <- merge(loc_link, data_to_ko, by.x = "loc2", by.y = "location_id", all.y = T)
    missing_data <- square_data[is.na(data), .(location_id, age_group_id, year_id)]

    var <- "test1"
    missing_data[, (var) := T] ## mark all age-years from these locs as possible to hold out

    ## bind together the missing data and the non-missing data.
    # where test1 == T, data was missing for that age-year in loc2 but not in loc1
    # where test1 is NA, data was present for both loc1 and loc2 for that age year
    data_to_ko <- merge(data_to_ko, missing_data, by.x = c("loc1", "age_group_id", "year_id"), by.y = c("location_id", "age_group_id", "year_id"), all.x = T)

    ## this while loop gets data_ids to drop until the specified proportion is hit
    loc_vect <- sample(unique(data_to_ko[test1 == T, loc2]), size = length(unique(data_to_ko[test1 == T, loc2])), replace = F)
    ids_to_drop <- c()
    i <- 0

    while(length(unique(ids_to_drop)) / nrow(no_na_data) < prop_to_hold) { i <- i + 1;

      if (i > length(loc_vect)) {

        message(paste0("Tried to knockout more locs than available, randomly dropping the remaining proportion"))
        message(paste0("  This shouldn't happen unless you have a dataset w/ repeated age-year patterns across all locs!"))

        leftovers <- unique(no_na_data$data_id)[!unique(no_na_data$data_id) %in% ids_to_drop]

        prop_remaining <- prop_to_hold - length(unique(ids_to_drop)) / nrow(no_na_data)
        randos <- data.table(ids = leftovers, drop = rbinom(length(leftovers), 1, prop_remaining))
        message(paste0("  Dropping ", sum(randos$drop), " random data points (",
                       round(sum(randos$drop) / nrow(no_na_data) * 100, digits = 3), "% of the whole dataset)"))

        ids_to_drop <- c(ids_to_drop, randos[drop == 1, ids])

        break

      } else {

        ## this is the KO process
        loc <- loc_vect[i]

        if (drop_nids == F) {

          ids_to_drop <- c(ids_to_drop, data_to_ko[loc2 == loc & test1 == T, data_id])

        } else {

          nids_to_drop <- data_to_ko[loc2 == loc & test1 == T, nid]
          ids_to_drop <- c(ids_to_drop, data_to_ko[nid %in% nids_to_drop, data_id])

        }

      }

    }

    ## create the ko column
    no_na_data[data_id %in% ids_to_drop, (paste0("ko", ko)) := 1]
    no_na_data[is.na(get(paste0("ko", ko))), (paste0("ko", ko)) := 0]
    message(" ", length(unique(ids_to_drop)), " (",
            round(sum(no_na_data[, get(paste0("ko", ko))]) / nrow(no_na_data) * 100, digits = 3), "%) data points dropped for ko ", ko)

    ## need to check if an entire age group id gets dropped
    # this is only a problem if factor(age_group_id) is in the model, it will break trying to predict new levels of a fixed effect
    if (no_new_ages == T) {

      train_ages <- unique(no_na_data[get(paste0("ko", ko)) == 0, age_group_id])
      test_ages <- unique(no_na_data[get(paste0("ko", ko)) == 1, age_group_id])

      all_held_ages <- test_ages[!test_ages %in% train_ages]

      if (length(all_held_ages > 1)) {

        message(paste0("  Ko ", ko, " held out all data points for age group(s) ", paste0(all_held_ages, collapse = ", "), ";"))
        message(paste0("   Adding these data points back to the training data to avoid trying to predict new levels"))

        ## put data points back into the training dataset
        no_na_data[age_group_id %in% all_held_ages, (paste0("ko", ko)) := 0]
        message("   New amount of heldout data is ", nrow(no_na_data[get(paste0("ko", ko)) == 1,]),
                " (", round(sum(no_na_data[, get(paste0("ko", ko))]) / nrow(no_na_data) * 100, digits = 3), "%) for ko ", ko)

      }

    }

  }

  if ("temp_sex" %in% names(no_na_data)) {

    no_na_data[, sex_id := temp_sex]
    no_na_data[, temp_sex := NULL]
  }

  return(no_na_data)

}
