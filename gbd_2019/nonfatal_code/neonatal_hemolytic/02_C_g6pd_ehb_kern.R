## ******************************************************************************
##
## Purpose: Calculate EHB and kernicterus from G6PD
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
}

pacman::p_load(data.table, dplyr, ggplot2, stats, boot, msm)
source("FILEPATH/get_covariate_estimates.R")

#import G6PD prevalence
g6pd <- fread(file = 'FILEPATH/g6pd_model_415010_prev.csv')

#calculate EHB prevalence by multiplying by scalar (with uncertainty)
#generate distribution of scalar
ehb_mean <- 0.0013
ehb_lower <- 0.00085
ehb_upper <- 0.002

ehb_se <- (ehb_upper - ehb_lower) / (2*1.96)
ehb_count <- (ehb_mean * (1-ehb_mean)) / (ehb_se^2)
ehb_alpha <- ehb_count * ehb_mean
ehb_beta <- ehb_count * (1-ehb_mean)

ehb_scalar <- rbeta(n = 1000, shape1 = ehb_alpha, shape2 = ehb_beta)

#multiply every g6pd draw by a draw of the ehb scalar
draw_cols <- paste0("draw_", 0:999)
ehb_scalar_table <- data.table(draw.id = draw_cols, scalar = ehb_scalar)

g6pd_long <- melt.data.table(g6pd, id.vars = names(g6pd)[!grepl("draw", names(g6pd))], 
                         measure.vars = patterns("draw"),
                         variable.name = 'draw.id')

g6pd_long <- merge(g6pd_long, ehb_scalar_table, by = 'draw.id')
g6pd_long[, value := value * scalar]


#add haqi to g6pd prev
haqi <- get_covariate_estimates(1099, decomp_step='step4')[,.(location_id,year_id,mean_value)]
setnames(haqi,c("mean_value", 'year_id'),c("haqi",'year'))

g6pd_long <- merge(g6pd_long, haqi, by = c('location_id','year'))

#predict the kernicterus scalar for every value of HAQI in the dataset
kern_scalars <- as.data.table(fread('FILEPATH/kern_prop_predicted_for_haqi_step4.csv'))
kern_scalars <- kern_scalars[, .(haqi, location_id, location_name, year_id, Y_mean, Y_mean_lo, Y_mean_hi)]
setnames(kern_scalars, 'year_id', 'year')

kern_scalars[, kern_mean := (exp(Y_mean)/(1 + exp(Y_mean))) + 0.000001]
kern_scalars[, kern_hi := (exp(Y_mean_hi)/(1 + exp(Y_mean_hi))) + 0.000001]
kern_scalars[, kern_lo := (exp(Y_mean_lo)/(1 + exp(Y_mean_lo))) + 0.000001]
kern_scalars[, kern_se := (kern_hi - kern_lo) / (2*1.96)]

for (x in 0:999) kern_scalars[, paste0("draw_",x) := mapply(rnorm,1,kern_mean,kern_se)]

kern_long <- melt.data.table(kern_scalars, id.vars = names(kern_scalars)[!grepl("draw", names(kern_scalars))], 
                             measure.vars = patterns("draw"),
                             variable.name = 'draw.id')

#merge on haqi and draw_id
prev_long <- merge(g6pd_long, kern_long, by = c('location_id','year','draw.id'), suffixes = c('.ehb','.kern'), all.x = TRUE)

prev_long[, kern_prev := value.ehb * value.kern]


#reshape wide
ehb_trim <- prev_long[, .(draw.id, location_id, location_name, year, sex, value.ehb)]
ehb_prev_wide <- dcast(ehb_trim, location_id + year + sex ~ draw.id, value.var = 'value.ehb')
write.csv(ehb_prev_wide, file = 'FILEPATH/g6pd_ehb_all_draws.csv',
          row.names = FALSE)

kern_trim <- prev_long[, .(draw.id, location_id, location_name, year, sex, kern_prev)]
kern_prev_wide <- dcast(kern_trim, location_id + year + sex ~ draw.id, value.var = 'kern_prev')
write.csv(kern_prev_wide, file = 'FILEPATH/g6pd_kernicterus_all_draws.csv',
          row.names = FALSE)

ehb_prev_wide$mean <- rowMeans(ehb_prev_wide[,draw_cols,with=FALSE], na.rm = T)
ehb_prev_wide$lower <- ehb_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
ehb_prev_wide$upper <- ehb_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
ehb_prev_wide <- ehb_prev_wide[,-draw_cols,with=FALSE]
write.csv(ehb_prev_wide, file = 'FILEPATH/g6pd_ehb_summary_stats.csv',
          row.names = FALSE)

kern_prev_wide$mean <- rowMeans(kern_prev_wide[,draw_cols,with=FALSE], na.rm = T)
kern_prev_wide$lower <- kern_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
kern_prev_wide$upper <- kern_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
kern_prev_wide <- kern_prev_wide[,-draw_cols,with=FALSE]
write.csv(kern_prev_wide, file = 'FILEPATH/g6pd_kernicterus_summary_stats.csv',
          row.names = FALSE)
