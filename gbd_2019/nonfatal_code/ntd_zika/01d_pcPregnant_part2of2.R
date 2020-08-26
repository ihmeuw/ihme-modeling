###  ESTIMATE REMAINING COMPONENTS OF PROPORTION PREGNANT 
###  continuing from part 1 of step 01d:   FILEPATH/ntd_zika\01d_pcPregnant_part1of2.do 

library(data.table)
library(readstata13)
library(ggplot2)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")


### pull demographics quantities

asfr <- get_covariate_estimates(covariate_id = ADDRESS, decomp_step = "step4", sex_id = 2, year_id = 1980:2019)
setnames(asfr, "mean_value", "asfr_mean")

qnn  <- get_covariate_estimates(covariate_id = ADDRESS, decomp_step = "step4", age_group_id = "all")
setnames(qnn, "mean_value", "qnn_mean")
qnn <- qnn[, .(location_id, year_id, qnn_mean)]

data <- merge(asfr, qnn, by = c("location_id", "year_id"))
data[, prPreg := 46/52 * asfr_mean * (1+qnn_mean)]

pop  <- get_population(age_group_id = "all", decomp_step = "step4", sex_id = 2, year_id = 1980:2019, location_id = "all")
pop <- pop[, .(location_id, year_id, population, age_group_id)]

data <- merge(pop, data, by = c("location_id", "year_id", "age_group_id"))
data[, nPreg := population * prPreg]


###  format based on old format

data <- data[, .(age_group_id, location_id, year_id, sex_id, population, asfr_mean, prPreg, nPreg)]
setnames(data, "asfr_mean", "asfr")

locs <- get_location_metadata(location_set_id = 35)
locs <- locs[, .(location_id, ihme_loc_id)]
data <- merge(data, locs, by = "location_id")


### save

fwrite(data, "FILEPATH")    ## prPreg  as  .csv
save.dta13(data, "FILEPATH")    ## prPreg  as  .dta
