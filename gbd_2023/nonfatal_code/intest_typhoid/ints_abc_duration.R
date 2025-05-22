# This code estimates the parameters of negative binomial distribution that best describe the duration of an iNTS episode in days.
# The variety of parameters given in the input data preclude a simple meta-analysis approach 
# (i.e. some studies report median + IQR, some report median + range, others report mean+SD, etc).
# We therefore use Approximate Bayesian Computation to assess the fit of range of simulated distributions to observed data
# and select the best parameters based on RMSE.
#
# Author: AUTHOR
# Created: 10 July 2018
# Last modified: 07 March 2024
#
# The code was updated in 3/2024 as part of in-cycle cause updates, to improve robustness, readability, and efficiency.
# I made no changes in the fundamental methodological approach, though I did add a second-stage finer resolution grid
# search that, due to increased precision, yields very slightly different parameters. This should have no meaningful
# effect on the point estimates as the mean duration only changed from 8.54 to 8.60 days, though the range is wider now
# so we may expect a bit wider uncertainty in the in the final estimates.
# Note, for posterity, the old parameters were size = 2.0, prob = 0.21.
#
# Libraries: data.table, readxl, and parallel; ggplot2 and Viridis for plotting only; 
#            dplyr loaded for convenience during development, but not used in formal code
# Input data: this reads in the extraction sheet from the systematic review of scientific lit with duration data
# Outputs: this saves a .csv file with the best fitting parameters of a negative binomial distribution for the duration of 
#          an iNTS episode, that read in and used to calculate prevalence from incidence in 'ints_sequela_split.py'


rm(list = ls())

library(data.table)
library(readxl)
library(parallel)
library(dplyr)
library(ggplot2)
library(viridis)

# Set working directory
ints_dir <- 'FILEPATH'
data_file <- file.path(ints_dir, 'FILENAME')

# Read in data -- non-data in row 2, so skip that (we'll read in col names below)
duration <- read_excel(data_file, sheet = 'extraction', col_names = FALSE, 
                       range = "A3:AJ99")

# Now read in again to get only column names
names(duration) <- as.character(read_excel(data_file, sheet = 'extraction', 
                                           n_max = 1, col_names = FALSE))

# Convert to a data table and drop all rows flagged for exclusion
setDT(duration)
duration <- duration[exclude == 0, ]

# Most data present median duration, but one study gives the mean duration 
# assuming negative binomial distribution, we'll estimate the median from the 
# mean and SD. See R rnbinom help file for details of parameterization used here
duration[, size := mean^2 / (standard_deviation^2 - mean)]
duration[!is.na(mean) & is.na(median), 
         median := median(rnbinom(1000000, size = size, mu = mean))]

# Some sources provide range and others IQR and the value of lower and upper depend on which
# Here we determine which sources provide IQR vs range and create percentile variables accordingly
# (i.e. if range: lower = 0, upper = 1; if IQR: lower = 0.25, upper = 0.75)
duration[, has_iqr := grepl("IQR", note_SR)]
duration[, lower_pctile := has_iqr * 0.25]
duration[, upper_pctile := 1 - lower_pctile]
duration[, median_pctile := 0.5]



# This function estimates deviations between a single row of input data  
# and corresponding data simulated given provided size & prob parameters, 
# and assuming that duration follows a negative binomial distribution

get_deviations <- function(i, size, prob, n_reps) {
  row_i <- duration[i, ]
  replicates <- replicate(n_reps, rnbinom(n = row_i$sample_size, size = size, 
                                          prob = prob) + 1) # add 1 to ensure no duration estimates less than a day
  dens_func <- ecdf(replicates)
  deviations <- with(row_i, rep(c(dens_func(lower) - lower_pctile, 
                                  dens_func(median) - median_pctile, 
                                  dens_func(upper) - upper_pctile), 
                                times = sample_size))
  return(deviations)
}



# This function runs get_deviations for each row of input data, 
# and calculates fit statistics based on the resulting deviations

test_parameters = function(size, prob, n_reps = 1000) {
  deviations <- lapply(1:nrow(duration), function(i) {
    get_deviations(i, size = size, prob = prob, n_reps = n_reps)})
  
  # Input data points without lower and upper will only fit on point estimate 
  # and return some NAs -- behavior is expected, but need to drop the NAs
  deviations <- na.omit(unlist(deviations)) 
  data.table(size = size, prob = prob, deviation = mean(deviations), 
             absDev = mean(abs(deviations)), rmse = sqrt(mean(deviations^2)))
}



# We're going to do a brute-force grid search of all possible combinations of size and prob parameters 
# for a negative binomial distribution to determine the parameter values that yield a distribution that 
# best fits the input data

size_seq <- seq(from = 0.5, to = 100, by = 0.5)
prob_seq <- seq(from = 0.01, to = 1, by = 0.01)

fit_stats <- rbindlist(mclapply(size_seq, function(size) rbindlist(lapply(prob_seq, function(prob) test_parameters(size = size, prob = prob))), mc.cores = 8))



# convert RMSE vector to matrix format needed for perspective plotting
rmse_mat <- as.matrix(reshape(fit_stats[, .(size, prob, rmse)], timevar = "size", idvar = "prob", direction = "wide")[, -1])
persp(prob_seq, size_seq, rmse_mat)




# We can use the results of the first grid search to guide a second grid search that is more focused 
# and with more repetitions (to reduce random error) 
# We take the range of parameters from top performers, and do finer resolution grid search within that range here

best_fits <- fit_stats[order(rmse), ][1:20, ] # take the 20 best performers
size_seq <- seq(from = min(best_fits$size), to = max(best_fits$size), by = 0.1)
prob_seq <- seq(from = min(best_fits$prob), to = max(best_fits$prob), by = 0.01)

fit_stats_best <- rbindlist(mclapply(size_seq, function(size) rbindlist(lapply(prob_seq, function(prob) test_parameters(size = size, prob = prob, n_reps = 10000))), mc.cores = 8))

rmse_mat <- as.matrix(reshape(fit_stats_best[, .(size, prob, rmse)], timevar = "size", idvar = "prob", direction = "wide")[, -1])
persp(prob_seq, size_seq, rmse_mat)



# The best fitting parameters will be those associated with the smallest RMSE
fit_stats_best <- fit_stats_best[order(rmse), ]
(best_fit <- fit_stats_best[1, ])



# MAKE A HEAT MAP OF RMSE #
fill_breaks <- c(0.1, 0.2, 0.4)

# Combine two passes to simplify faceting in a single plot
fit_stats_combined <- rbind(fit_stats[, pass := 'First pass: Coarse search of full range'], 
                            fit_stats_best[, pass := 'Second pass: Fine search of best fitting range'])

# geom_tile breaks with the different ranges in the two facets -- 
# fix this by providing tile height and width
fit_stats_combined[, c('height', 'width') := lapply(.SD, function(x) 
  (max(x) - min(x))/length(unique(x)[-1])), by = 'pass', .SDcols = c('prob', 'size')]

# Create the plot
ggplot(fit_stats_combined, aes(x = size, y = prob, fill = rmse)) + geom_tile(aes(height = height, width = width)) + 
  geom_point(data = best_fit, aes(x = size, y = prob), color = 'red') +
  facet_wrap(~as.factor(pass), scales = 'free') +
  scale_fill_viridis(option = "cividis", name = 'RMSE', trans = 'log', labels = fill_breaks, breaks = fill_breaks) +
  scale_x_continuous(expand = c(0.02,0)) + scale_y_continuous(expand = c(0.02,0)) +
  theme_minimal() + xlab('Size parameter') + ylab('Probability parameter') +
  ggtitle('Approximate Bayesian computation grid search',
          'RMSE of observed iNTS duration data vs data generated using a negative binomial distribution with varying parameterization')

# Save the plot
ggsave(file.path(ints_dir, 'abc_duration_rmse_heatmap.png'), width = 12, height = 6, bg = 'white')
ggsave(file.path(ints_dir, 'abc_duration_rmse_heatmap.pdf'), width = 12, height = 6)


# Save the final parameters to a csv to be used in prevalence estimation
write.csv(best_fit, file.path(ints_dir, 'abc_duration_parameters.csv'), row.names = F)

