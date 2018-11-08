library(data.table)
library(readr)

rm(list=ls())

# Get settings ------------------------------------------------------------

main_dir <- commandArgs(trailingOnly=T)[1]
ihme_loc <- commandArgs(trailingOnly=T)[2]

source("settings.R")
get_settings(main_dir)

## Determine location specific hyperparameters

hyperparameters_data <- data.table(mu_n = -5, mu_n0 = -5, mu_s = -5, mu_g = -5, mu_f = -5, mu_srb = -5,
                                   sig_n = 3, sig_n0 = 3, sig_s = 3, sig_g = 3, sig_f = 3, sig_srb = 3,
                                   rho_g_t_mu = migration_t_mu, rho_g_t_sig = migration_t_sd,
                                   rho_g_a_mu = migration_a_mu, rho_g_a_sig = migration_a_sd)
write_csv(hyperparameters_data, path = paste0(temp_dir, "/inputs/hyperparameters.csv"))

