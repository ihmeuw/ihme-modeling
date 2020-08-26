## Generate plausible bounds for over-age-80 qx values to screen lifetables
## Using HMD lifetables, find the high-level percentiles that we consider as plausibility constraints

rm(list=ls())

if (Sys.info()[1]=="Windows") {
  root <- "FILEPATH/root"
  user <- Sys.getenv("USERNAME")
} else {
  root <- "FILEPATH/root"
  user <- Sys.getenv("USER")
}

source(paste0("FILEPATH/empirical-lt/functions/import_hmd_lts.R"))
main_input_folder <- paste0(root, "FILEPATH/empirical-lt/inputs")

hmd_results <- import_hmd_lts()
hmd_results <- hmd_results[age >= 80 & year >= 1950]
hmd_bounds <- hmd_results[, .(pct_95 = quantile(qx, c(.95), na.rm = T),
                           pct_975 = quantile(qx, .975, na.rm = T),
                           pct_99 = quantile(qx, .99, na.rm = T),
                           pct_1 = quantile(qx, .01, na.rm = T)), by = c("age", "sex")]
write_csv(hmd_bounds, paste0(main_input_folder, "/hmd_qx_bounds.csv"))
