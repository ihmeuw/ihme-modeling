library(readr)
source("empir_gen_funcs/R/import_hmd_lts.R")

hmd_results <- import_hmd_lts()
hmd_results <- hmd_results[age >= 80 & year >= 1950]
hmd_bounds <- hmd_results[, .(pct_95 = quantile(qx, c(.95), na.rm = T),
                           pct_975 = quantile(qx, .975, na.rm = T),
                           pct_99 = quantile(qx, .99, na.rm = T),
                           pct_1 = quantile(qx, .01, na.rm = T)), by = c("age", "sex")]
write_csv(hmd_bounds, "hmd_qx_bounds.csv")
