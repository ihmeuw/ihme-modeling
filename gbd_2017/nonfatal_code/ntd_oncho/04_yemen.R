
#######################################
# Date: 3/5/18
# Purpose: Produce Estimates for Yemen
#######################################

######################################
# 1. SET-UP
######################################
# install packages
library(data.table)
library(stats)

# source central functions
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_draws.R")

######################################
# 2. PULL ESTIMATES
######################################

#######################################################################################
# MoH Yemen, 1991 reported 30,000 infected with Onchocerciasis
# SOURCE: http://apps.who.int/iris/bitstream/10665/37346/1/WHO_TRS_852.pdf

# WE KNOW STARTING IN 2000 THE AIM WAS TO TREAT APPROXIMATELY 300,000 PEOPLE 4 TIMES A YEAR
# SOURCE: https://www.cartercenter.org/resources/pdfs/news/health_publications/selendy-waterandsanitationrelateddiseases-chapt11.pdf

# CSSW ONCHO CONTROL PROGRAM IN YEMEM REPORTS AGE-ISH/SEX SPLIT DATA IN 2014
# MALE < 15 = 557 CASES
# MALE > 15 = 839 CASES
# FEMALE < 15 = 405 CASES
# FEMALE > 15 = 765 CASES
#######################################################################################


###########################################################
# BUILD GBD ESTIMATE SKELETON
###########################################################
# set-up GBD skeleton
demographics <- get_demographics(gbd_team = "ADDRESS")
gbdages <- demographics$age_group_id # gbd ages
gbdyears <- demographics$year_id # gbd years
gbdsexes <- demographics$sex_id # gbd sexes



###############################################################
# AGE/SEX SPLIT NATIONAL MF PREVALENCE CASES
###############################################################


datalist <- NULL # initialize list to store draws for each year

for(a in 1:length(gbdyears)) {
  print(a)

  year <- gbdyears[a]
  population <-get_population(age_group_id = 22, year_id = year, sex_id = 3, location_id = 157) # get national all-age population for year

  # set up variables for binomial proportion confidence interval
  cases <- 30000 # number of successes
  pop <- round(population$population) # number of trials
  prob <- cases/pop # hypothesized probability of success

  # sampling binomial distribution
  case_draws <- rbinom(1000, pop, prob)

  # get global prevalence by age/sex to inform splits
  global_prev_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 1494, measure_id = 5,
                                location_id = 1,  year_id = year,
                                sex_id = c(1,2), source = "ADDRESS", status="best")

  draws_temp <- global_prev_draws

  # get proportions now
  draw_names <- grep("draw", names(global_prev_draws), value = TRUE)

  # try taking mean of draws
  draw_means <- as.data.table(rowMeans(global_prev_draws[, ..draw_names]))

  draws_temp <- draws_temp[, (draw_names) := draw_means$V1]


  # split cases by proportions
  draws_temp <- as.data.table(mapply(`*`, draws_temp[, ..draw_names], case_draws))

  # for each draw col get scalar which is sum cases / sum (current est cases)
  scalars <- draws_temp[, lapply(.SD, sum, na.rm=TRUE), .SDcols = draw_names]
  scalars <- case_draws / scalars


  # scale now to correct case count
  draws_temp <- as.data.table(mapply(`*`, draws_temp[, ..draw_names], scalars))

  # convert to prevalence now by dividing cases by total population in that year
  draws_temp <- as.data.table(mapply(`/`, draws_temp[, ..draw_names], population$population))

  info <- data.table(modelable_entity_id = global_prev_draws$modelable_entity_id,
                     measure_id = global_prev_draws$measure_id,
                     age_group_id = global_prev_draws$age_group_id,
                     year_id = global_prev_draws$year_id,
                     location_id = 157,
                     sex_id = global_prev_draws$sex_id)

  # add back other column names needed
  draws_temp <- dplyr::bind_cols(info, draws_temp)

  # append to previous year....etc.
  datalist[[a]] <- draws_temp # add it to your list

}

nat_draws <- do.call(rbind, datalist)

# save file for national case counts
if (file.exists("FILEPATH/157.csv")) {
  file.remove("FILEPATH/157.csv")
}
fwrite(nat_draws, "FILEPATH/157.csv")

# Yemen has ROD most severe form with a lot of itching so for now mapping

nat_draws[, modelable_entity_id := 2620]

if (file.exists("FILEPATH/157.csv")) {
  file.remove("FILEPATH/157.csv")
}
fwrite(nat_draws, "FILEPATH/157.csv")



# stuff <- fread("FILEPATH/35.csv")
