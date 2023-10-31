## ******************************************************************************
##
## Purpose: Calculate EHB and kernicterus from G6PD
## Input:   
## Output:
## Last Update: 9/9/19
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  h <- "PATHNAME"
}

pacman::p_load(data.table, dplyr, ggplot2, stats, boot, msm)
source("PATHNAME/get_covariate_estimates.R")

repo_dir <- "PATHNAME"
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source("PATHNAME/get_location_metadata.R")


#import G6PD prevalence
working_dir <- 'PATHNAME'
g6pd <- fread(file = paste0(working_dir, '/g6pd_model_505820_prev.csv'))

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
haqi <- get_covariate_estimates(1099, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
setnames(haqi,c("mean_value", 'year_id'),c("haqi",'year'))

g6pd_long <- merge(g6pd_long, haqi, by = c('location_id','year'))


#predict the kernicterus scalar for every value of HAQI in the dataset
model_fit <- readRDS(file="PATHNAME/fit1.RData")

#make predictions based on the model
df_pred <- expand.grid(haqi = unique(g6pd_long$haqi), TSB = 25)
kern_scalars <- predict_mr_brt(model_fit, newdata = df_pred)
check_for_preds(kern_scalars)

pred_object <- load_mr_brt_preds(kern_scalars)
preds <- data.table(pred_object$model_summaries)

kern_scalars <- copy(preds)
setnames(kern_scalars, 'X_haqi', 'haqi')
kern_scalars <- kern_scalars[, .(haqi, Y_mean, Y_mean_lo, Y_mean_hi)]

kern_scalars[, kern_mean := (exp(Y_mean)/(1 + exp(Y_mean))) + 0.000001]
kern_scalars[, kern_hi := (exp(Y_mean_hi)/(1 + exp(Y_mean_hi))) + 0.000001]
kern_scalars[, kern_lo := (exp(Y_mean_lo)/(1 + exp(Y_mean_lo))) + 0.000001]
kern_scalars[, kern_se := (kern_hi - kern_lo) / (2*1.96)]

for (x in 0:999) kern_scalars[, paste0("draw_",x) := mapply(rnorm,1,kern_mean,kern_se)]

kern_long <- melt.data.table(kern_scalars, id.vars = names(kern_scalars)[!grepl("draw", names(kern_scalars))], 
                             measure.vars = patterns("draw"),
                             variable.name = 'draw.id')


#merge on haqi and draw_id
prev_long <- merge(g6pd_long, kern_long, by = c('haqi','draw.id'), suffixes = c('.ehb','.kern'), all.x = TRUE)

prev_long[, kern_prev := value.ehb * value.kern]


#reshape wide
ehb_trim <- prev_long[, .(draw.id, location_id, year, sex, value.ehb)]
ehb_prev_wide <- dcast(ehb_trim, location_id + year + sex ~ draw.id, value.var = 'value.ehb')
write.csv(ehb_prev_wide, file = paste0(working_dir, '/g6pd_ehb_all_draws.csv'),
          row.names = FALSE)

kern_trim <- prev_long[, .(draw.id, location_id, year, sex, kern_prev)]
kern_prev_wide <- dcast(kern_trim, location_id + year + sex ~ draw.id, value.var = 'kern_prev')
write.csv(kern_prev_wide, file = paste0(working_dir, '/g6pd_kernicterus_all_draws.csv'),
          row.names = FALSE)

ehb_prev_wide$mean <- rowMeans(ehb_prev_wide[,draw_cols,with=FALSE], na.rm = T)
ehb_prev_wide$lower <- ehb_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
ehb_prev_wide$upper <- ehb_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
ehb_prev_wide <- ehb_prev_wide[,-draw_cols,with=FALSE]
write.csv(ehb_prev_wide, file = paste0(working_dir, '/g6pd_ehb_summary_stats.csv'),
          row.names = FALSE)

kern_prev_wide$mean <- rowMeans(kern_prev_wide[,draw_cols,with=FALSE], na.rm = T)
kern_prev_wide$lower <- kern_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
kern_prev_wide$upper <- kern_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
kern_prev_wide <- kern_prev_wide[,-draw_cols,with=FALSE]
write.csv(kern_prev_wide, file = paste0(working_dir, '/g6pd_kernicterus_summary_stats.csv'),
          row.names = FALSE)


# scatter current ehb and kernicterus proportions against previous run
locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7,
                              decomp_step = 'iterative')
prev_ehb <- fread(paste0(working_dir, '/g6pd_ehb_summary_stats_gbd19_kern_model.csv'))
prev_kern <- fread(paste0(working_dir, '/g6pd_kernicterus_summary_stats_gbd19_kern_model.csv'))
current_ehb <- fread(paste0(working_dir, '/g6pd_ehb_summary_stats.csv'))
current_kern <- fread(paste0(working_dir, '/g6pd_kernicterus_summary_stats.csv'))

compare <- merge(current_kern, prev_kern, by = c('location_id', 'year', 'sex'), all.x = TRUE)
setnames(compare, c('mean.x', 'mean.y'), c('current', 'previous'))
compare <- merge(compare, locs, by = 'location_id', all.x = TRUE)

compare_ehb <- merge(current_ehb, prev_ehb, by = c('location_id', 'year', 'sex'), all.x = TRUE)
setnames(compare_ehb, c('mean.x', 'mean.y'), c('current', 'previous'))
compare_ehb <- merge(compare_ehb, locs, by = 'location_id', all.x = TRUE)



pdf(file = paste0(working_dir, "/scatter_7_20_kern_model_vs_gbd19.pdf"),
    width = 12, height = 8)
gg1 <- ggplot() +
  geom_point(data = compare[year < 2020], 
             aes(x = previous, y = current, color = super_region_name),
             alpha = 0.5) +
  facet_wrap(~ year) +
  geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
  labs(title = paste0('Prevalence of Kernicterus due to G6PD'),
       x = 'GBD2019 Kernicterus Proportion Model',
       y = 'GBD2020 Kernicterus Proportion Model')
print(gg1)

gg2 <- ggplot() +
  geom_point(data = compare_ehb[year < 2020], 
             aes(x = previous, y = current, color = super_region_name),
             alpha = 0.5) +
  facet_wrap(~ year) +
  geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
  labs(title = paste0('Prevalence of EHB due to G6PD'),
       x = 'GBD2019 Kernicterus Proportion Model',
       y = 'GBD2020 Kernicterus Proportion Model')
print(gg2)
ggplotly(p = gg2, tooltip = 'text') 

dev.off()
