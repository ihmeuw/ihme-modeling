#####################################INTRO#############################################
#' Author: 
#' 5/7/18
#' Purpose: Take mean, upper, and lower of the severity splits and convert into 1000 draws
#'          1) Get 1000 draws for gonorrhea, chlamydia for developed/developing
#'          2) Bind together
#'          3) Save 1 csv
#'
#' OUTPUTS: 1000 draws for the epididymo-orchitis severity splits in developing/develped
#'
#####################################INTRO#############################################

library("ihme")
setup()

library(dplyr)
library(magrittr)
library(data.table)


# Read in severity split summary ------------------------------------------

splits <- readr::read_csv(paste0("FILEPATH/epididymo_severity_splits.csv")) %>% 
  mutate(std_error = (upper_split - lower_split) / (2 * 1.96))




# Calculate 1000 draws of a binomial distribution -------------------------

# number of draws we want
size <- 1000 

# 1000 draws of a binomial distribution
#   back-calculate sample size from mean and standard error
binomial_draws<- function(size, std_error, mean) {
  n <- mean * (1 - mean) / std_error ^ 2
  rbinom(size, round(n), mean) / round(n)
}

for (i in seq_along(1:nrow(splits))) {
  assign(paste0("row_", i), binomial_draws(size, splits[i, ]$std_error, splits[i, ]$mean_split))
}

draws <- as.data.frame(rbind(row_1, row_2, row_3, row_4))
draws <- setNames(draws, paste0("draw_", 0:999))
draws <- as.data.frame(cbind(splits[, c("me_id", "me_name", "location_type")], draws))

draws <- melt(draws, 
              id.vars = c("me_id", "me_name", "location_type"), 
              measure.vars = paste0("draw_", 0:999),
              variable.name = "draw_num", value.name = "draw")


# Save as a csv -----------------------------------------------------------

readr::write_csv(draws, paste0(main_dir, "EX/severity_draws.csv"))

rm(draws)

