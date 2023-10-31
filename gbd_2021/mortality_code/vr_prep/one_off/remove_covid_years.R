

library(data.table)
library(haven)

old_run_id <- 
new_run_id <- 

# final #
final_vrp <- fread(paste0())
final_vrp <- final_vrp[!year_id %in% c(2020, 2021)]
readr::write_csv(final_vrp, paste0())


# d00 #
d00 <- setDT(read_dta(paste0()))
d00 <- d00[!year %in% c(2020, 2021)]
haven::write_dta(d00, paste0())


# non-cod no shock #
nc_ns <- fread(paste0())
nc_ns <- nc_ns[!year_id %in% c(2020, 2021)]
readr::write_csv(nc_ns, paste0())


# cod no shock #
c_ns <- fread(paste0())
c_ns <- c_ns[!year_id %in% c(2020, 2021)]
readr::write_csv(c_ns, paste0())

