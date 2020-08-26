##########################################################################################################
# Purpose: As early and late syphilis stages have a differing transmission rate to the fetus,
#          our goal is to capture the difference in syphilis transmission between women with early
#          and late syphilis. This is dependent on being able to split the (sero)prevalence in women
#          into early and late. 

# OUTPUTS: untreated_death_rates_by_stage.csv
#           -> csv with the neonatal and fetal death rates for early/late untreated syphilis. At the draw level (1000) 
#          syphilis_stages_proportions.csv
#           -> csv with the proportions of pregnant women with early/late syphilis at the draw level (1000). 

##########################################################################################################

library(dplyr)

# Load data from Ingraham 1950 and meta-analysis --------------------------
message(paste(Sys.time(), "Load data from Ingraham 1950 and meta-analyses"))

# early/late syphilis deaths, sample size from Ingraham
early <- c(neonatal_deaths = 30, neonatal_n = 165, fetal_deaths = 55, fetal_n = 220)
late <- c(neonatal_deaths = 7, neonatal_n = 72, fetal_deaths = 10, fetal_n = 82)
both <- c(early["neonatal_deaths"] + late["neonatal_deaths"], 
          early["neonatal_n"] + late["neonatal_n"], 
          early["fetal_deaths"] + late["fetal_deaths"], 
          early["fetal_n"] + late["fetal_n"])

# combine together and calculate the proportion that died and the standard error,
# based on this formula: std error = sqrt(p * (1 - p) / n), where
# p is the mean and n is the sample size
ingraham <- rbind(early, late, both)
ingraham <- as.data.frame(ingraham) %>% 
  cbind(stage = c("early", "late", "both")) %>% 
  mutate(neonatal_prop_died = neonatal_deaths / neonatal_n,
         neonatal_std_error = sqrt(neonatal_prop_died * (1 - neonatal_prop_died) / neonatal_n),
         fetal_prop_died = fetal_deaths / fetal_n,
         fetal_std_error = sqrt(fetal_prop_died * (1 - fetal_prop_died) / fetal_n))

# Untreated neonatal deaths from meta-analysis
neonatal_mean <- 0.0653
neonatal_lower <- 0.0203
neonatal_upper <- 0.1102
neonatal_std_error <- (neonatal_upper - neonatal_lower) / (2 * 1.96)

# Inadequately treated neonatal deaths from meta-analysis
inadeq_neonatal_mean  <- 0.0467 
inadeq_neonatal_lower <- 0.0021 
inadeq_neonatal_upper <- 0.0913 
inadeq_neonatal_std_error <- (inadeq_neonatal_upper - inadeq_neonatal_lower) / (2 * 1.96)

# Untreated fetal deaths (stillbirths/miscarriages) from meta-analysis 
fetal_mean <- 0.1086 
fetal_lower <- 0.0627 
fetal_upper <- 0.1545 
fetal_std_error <- (fetal_upper - fetal_lower) / (2 * 1.96)

# Inadequately treated fetal deaths
inadeq_fetal_mean  <- 0.0910 
inadeq_fetal_lower <- 0.0473 
inadeq_fetal_upper <- 0.1347 
inadeq_fetal_std_error <- (inadeq_fetal_upper - inadeq_fetal_lower) / (2 * 1.96)


# Calculate 1000 draws for each binomial distribution --------------------------
message(paste(Sys.time(), "Calculate 1000 draws of each binomial distribution"))

# number of draws we want
size <- 1000                           

#   get 1000 draws for the proportions from the Ingraham 1950 study
binomial_draws <- function(size, n, mean, names) {
  df <- as.data.frame(mapply(function(n, mean) { rbinom(size, n, mean) / n }, 
                       n = n, mean = mean))
  df <- setNames(df, names)
}

#untreated
ingraham_nn_draws    <- binomial_draws(size = size, n = ingraham$neonatal_n, mean = ingraham$neonatal_prop_died, names = paste0("neonatal_", ingraham$stage))
ingraham_fetal_draws <- binomial_draws(size = size, n = ingraham$fetal_n,    mean = ingraham$fetal_prop_died,    names = paste0("fetal_",    ingraham$stage))

ingraham_inadeq_fetal_draws <- binomial_draws(size = size, n = ingraham$fetal_n[2],    mean = ingraham$fetal_prop_died[2],    # INADEQUATELY TREATED FETAL
                                              names = paste0("inadeq_fetal_", ingraham$stage[2]))
ingraham_inadeq_nn_draws    <- binomial_draws(size = size, n = ingraham$neonatal_n[2], mean = ingraham$neonatal_prop_died[2], # INADEQUATELY TREATED NEONATAL
                                              names = paste0("inadeq_neonatal_", ingraham$stage[2]))

# COMBINE
ingraham_draws <- cbind(ingraham_nn_draws, ingraham_fetal_draws, ingraham_inadeq_fetal_draws, ingraham_inadeq_nn_draws)

# 1000 draws for the untreated deaths from the meta-analysis 
#   back-calculate sample size from mean and standard error
#   for both neonatal deaths and fetal loss
binomial_draws_meta <- function(size, std_error, mean) {
  n <- mean * (1 - mean) / std_error ^ 2
  rbinom(size, round(n), mean) / round(n)
}

neonatal_meta_draws        <- binomial_draws_meta(size, neonatal_std_error,     neonatal_mean)
untreated_fetal_meta_draws <- binomial_draws_meta(size, fetal_std_error,        fetal_mean)
inadeq_fetal_meta_draws    <- binomial_draws_meta(size, inadeq_fetal_std_error, inadeq_fetal_mean)
inadeq_nn_meta_draws       <- binomial_draws_meta(size, inadeq_neonatal_std_error, inadeq_neonatal_mean)

# Combine both sets of draws together with a col for draw names
# NOTE: every col tagged with _meta comes from the meta-analyses we performed on congenital syph studies
draw_names <- paste0("draw_", 0:999)
transmission_draws <- cbind(draw_names, ingraham_draws, 
                            untreated_nn_meta    = neonatal_meta_draws, 
                            inadeq_nn_meta       = inadeq_nn_meta_draws,
                            untreated_fetal_meta = untreated_fetal_meta_draws,
                            inadeq_fetal_meta    = inadeq_fetal_meta_draws)


# Calculate early/late neonatal death rates for untreated syphilis -----------

message(paste(Sys.time(), "Calculate early/late neonatal death rates for untreated syphilis"))


# Here we take the value from the meta-analysis, scale it to the percentage
# difference between it and the value for BOTH early and late syphilis in
# Ingraham 1950 and then multiply by the value from the early/late proportion that died
# to get an estimate of the proportion of neonates the die from a mother with early
# syphilis vs. with late syphilis based on our meta-analysis
transmission_draws <- transmission_draws %>% 
  mutate(early_nn_emr              = untreated_nn_meta / neonatal_both * neonatal_early,
         untreated_late_nn_emr     = untreated_nn_meta / neonatal_both * neonatal_late,
         inadeq_late_nn_emr        = inadeq_nn_meta    / neonatal_both * neonatal_late,
         early_fetal_emr           = untreated_fetal_meta / fetal_both * fetal_early,
         untreated_late_fetal_emr  = untreated_fetal_meta / fetal_both * fetal_late,
         inadeq_late_fetal_emr     = inadeq_fetal_meta    / fetal_both * fetal_late)

trimmed_draws <- select(transmission_draws, draw_names, early_nn_emr, untreated_late_nn_emr, inadeq_late_nn_emr, 
                        early_fetal_emr, untreated_late_fetal_emr, inadeq_late_fetal_emr)


readr::write_csv(trimmed_draws, FILEPATH)














