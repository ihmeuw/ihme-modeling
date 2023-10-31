## ******************************************************************************
##
## Purpose: Calculate EHB and kernicterus from preterm
## Input:   Filepath where preterm prevalence is saved
## Output:  Draw files of EHB and kernicterus
## Last Update: 7/29/20
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  h <- paste0("PATHNAME")
}

pacman::p_load(data.table, dplyr, ggplot2, stats, boot, msm)
source("PATHNAME/get_covariate_estimates.R")
repo_dir <- "PATHNAME"
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source("PATHNAME/get_location_metadata.R")

#launch full location set of plots
#Job specifications
username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=60G"
fthread <- "-l fthread=4"
runtime_flag <- "-l h_rt=01:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
shell_script <- "-cwd PATHNAME/r_shell_ide.sh"

### change to your own repo path if necessary
script <- paste0(h, "PATHNAME/03_A_pull_preterm_prev.R")

# NOTE: this script just has this:
# pacman::p_load(data.table)
# source("PATHNAME/get_draws.R")
# 
# out_dir = "PATHNAME"
# me_id = 15801
# 
# dt <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = me_id, source = 'epi', measure_id = 5,
#                 age_group_id = 164, status = 'best', gbd_round_id = 7, decomp_step = 'iterative')
# model_version_id <- unique(dt$model_version_id)
# 
# write.csv(dt, row.names = FALSE, file = paste0(out_dir, "/preterm_aggregate_birth_prevalence_vers",model_version_id,".csv"))

errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")

job_name <- paste0("-N", " preterm_draws")
job <- paste("qsub", m_mem_free, fthread, runtime_flag,
             jdrive_flag, queue_flag,
             job_name, "-P proj_neonatal", outputs_flag, errors_flag, shell_script, script)

system(job)


# ------------------------------------------------------------------------------------------------
# RUN AFTER JOB ABOVE COMPLETES
# ------------------------------------------------------------------------------------------------

#import preterm prevalence
out_dir <- 'PATHNAME' 
preterm <- fread(file = paste0(out_dir,'preterm_aggregate_birth_prevalence_vers496169.csv'))
setnames(preterm, c('year_id', 'sex_id'), c('year', 'sex'))

#calculate EHB prevalence by multiplying by scalar (with uncertainty)
#generate distribution of scalar
ehb_mean <- 0.00045
ehb_lower <- 0.00024
ehb_upper <- 0.0007

ehb_se <- (ehb_upper - ehb_lower) / (2*1.96)
ehb_count <- (ehb_mean * (1-ehb_mean)) / (ehb_se^2)
ehb_alpha <- ehb_count * ehb_mean
ehb_beta <- ehb_count * (1-ehb_mean)

ehb_scalar <- rbeta(n = 1000, shape1 = ehb_alpha, shape2 = ehb_beta)

#multiply every g6pd draw by a draw of the ehb scalar
draw_cols <- paste0("draw_", 0:999)
ehb_scalar_table <- data.table(draw.id = draw_cols, scalar = ehb_scalar)

preterm_long <- melt.data.table(preterm, id.vars = names(preterm)[!grepl("draw", names(preterm))], 
                             measure.vars = patterns("draw"),
                             variable.name = 'draw.id')

preterm_long <- merge(preterm_long, ehb_scalar_table, by = 'draw.id')
preterm_long[, value := value * scalar]


#add haqi to preterm prev
haqi <- get_covariate_estimates(1099, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
setnames(haqi,c("mean_value", 'year_id'),c("haqi",'year'))

preterm_long <- merge(preterm_long, haqi, by = c('location_id','year'))


#predict the kernicterus scalar for every value of HAQI in the dataset
model_fit <- readRDS(file="PATHNAME/fit1.RData")

#make predictions based on the model
df_pred <- expand.grid(haqi = unique(preterm_long$haqi), TSB = 25)
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
prev_long <- merge(preterm_long, kern_long, by = c('haqi','draw.id'), suffixes = c('.ehb','.kern'), all.x = TRUE)

prev_long[, kern_prev := value.ehb * value.kern]



#reshape wide
ehb_trim <- prev_long[, .(draw.id, location_id, year, sex, value.ehb)]
ehb_prev_wide <- dcast(ehb_trim, location_id + year + sex ~ draw.id, value.var = 'value.ehb')
write.csv(ehb_prev_wide, file = paste0(out_dir, 'preterm_ehb_all_draws.csv'),
          row.names = FALSE)

kern_trim <- prev_long[, .(draw.id, location_id, year, sex, kern_prev)]
kern_prev_wide <- dcast(kern_trim, location_id + year + sex ~ draw.id, value.var = 'kern_prev')
write.csv(kern_prev_wide, file = paste0(out_dir, 'preterm_kernicterus_all_draws.csv'),
          row.names = FALSE)

ehb_prev_wide <- as.data.table(ehb_prev_wide)
ehb_prev_wide$mean <- rowMeans(ehb_prev_wide[,draw_cols,with=FALSE], na.rm = T)
ehb_prev_wide$lower <- ehb_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
ehb_prev_wide$upper <- ehb_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
ehb_prev_wide <- ehb_prev_wide[,-draw_cols,with=FALSE]
write.csv(ehb_prev_wide, file = paste0(out_dir, 'preterm_ehb_summary_stats.csv'),
          row.names = FALSE)

kern_prev_wide <- as.data.table(kern_prev_wide)
kern_prev_wide$mean <- rowMeans(kern_prev_wide[,draw_cols,with=FALSE], na.rm = T)
kern_prev_wide$lower <- kern_prev_wide[, apply(.SD, 1, quantile, probs = .025, na.rm = T), .SDcols = draw_cols]
kern_prev_wide$upper <- kern_prev_wide[, apply(.SD, 1, quantile, probs = .975, na.rm = T), .SDcols = draw_cols]
kern_prev_wide <- kern_prev_wide[,-draw_cols,with=FALSE]
write.csv(kern_prev_wide, file = paste0(out_dir, 'preterm_kernicterus_summary_stats.csv'),
          row.names = FALSE)



# scatter current ehb and kernicterus proportions against previous run
prev_ehb <- fread('PATHNAME')
prev_kern <- fread('PATHNAME')

compare <- merge(kern_prev_wide, prev_kern, by = c('location_id', 'year', 'sex'), all.x = TRUE)
setnames(compare, c('mean.x', 'mean.y'), c('current', 'previous'))

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7,
                              decomp_step = 'iterative')
compare <- merge(compare, locs, by = 'location_id', all.x = TRUE)


pdf(file = paste0(out_dir, "/scatter_7_29_kern_model_vs_gbd19.pdf"),
    width = 12, height = 8)
gg1 <- ggplot() +
  geom_point(data = compare[year < 2020], 
             aes(x = previous, y = current, color = super_region_name),
             alpha = 0.5) +
  facet_wrap(~ year) +
  geom_abline(intercept = 0, slope = 1, alpha = 0.2) +
  labs(title = paste0('Prevalence of Kernicterus due to preterm'),
       x = 'GBD2019 Kernicterus Proportion Model',
       y = 'GBD2020 Kernicterus Proportion Model')
print(gg1)
dev.off()

