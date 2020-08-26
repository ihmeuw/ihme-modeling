rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "USERNAME"
}

library("stringr")
library("tidyverse")
#################



arg <- commandArgs(trailingOnly = TRUE)
loc <- arg[1]

years <- c(1980:2019)

#counter <- 1

#print(loc)

#loc <- 42

#test <- get_model_results('cod', model_version_id = 617867, decomp_step = 'step3')

#test_draws <- get_draws('cause_id', gbd_id = 615, source = 'codem', version_id = 617867, decomp_step = 'step3')

#dr_draws <- get_draws('cause_id', gbd_id = 618, source = 'codem', location_id = loc, version_id = 620900, decomp_step = 'step4')

#print(head(dr_draws))
print("Starting save")


#for (yr in years){
 # dr_draws_year <- dr_draws %>% filter(year_id == yr)
  #counter <- counter + 1
#}

#For Combining
for (year in years) {
  df <- read.csv(paste0("FILEPATH"))
  df <- df %>%
    select(-cause_id, -country_id, -envelope, -location_id, -measure_id, -pop, -region_id, -sex_id, -sex_name, -super_region_id, -year_id, -metric_id)
  write_csv(df, paste0("FILEPATH"))
  }

#print(counter)



