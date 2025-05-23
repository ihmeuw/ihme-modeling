##############################
## Purpose: Predict at the draw level results for young and old ASFR
## Details: 

##############################
sessionInfo()
rm(list=ls())

## libraries
library(argparse)
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(parallel)
library(magrittr)
library(MASS)
library(glue)
library(mvtnorm)
library(mortdb)
library(mortcore)
set.seed(173451)

if (interactive()){
  username <- Sys.getenv('USER')
  root <- 'FILEPATH'
  loc <- 'location input'
  version <- 'Run id'
  gbd_year <- 2020
  year_start <- 1950
  year_end <- 2022
} else {
  task_id <- as.integer(Sys.getenv('SGE_TASK_ID'))
  username <- Sys.getenv('USER')
  root <- '/home/j/'
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--loc', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  args <- parser$parse_args()
  version_id <- args$version_id
  loc <- args$loc
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end  
}

print(version_id)
print(class(year_start))

## a few directories 
base_dir <- "FILEPATH"
out_dir <- "FILEPATH"

## read in model object 
load(paste0(out_dir, 'young_old_asfr_workspace.RData'))

###########################
## Convenience Functions
###########################

## create 1000 draws by copying data 1000 times
copy_draws <- function(data){
  append_draws <- list()
  for(draws in 0:999){
    append_draws[[paste0(draws)]] <- copy(data)
    append_draws[[paste0(draws)]]$draw <- draws
  }
  append_draws <- rbindlist(append_draws)
  return(append_draws)
}

## extract draws from the random effects by first extracting the means, then simulating based on the standard deviations
## REs are iid
extract_re_draws <- function(model, re_level, return_draws = F){
  data <- data.table(ranef(model)[[paste0(re_level)]], keep.rownames = T)
  se <-  as.numeric(paste0(VarCorr(model)[[paste0(re_level)]]))
  draws <- list()
  mean_re <- list()
  for(units in unique(data[,rn])){
    draws[[paste0(units)]] <- data.table(draw = c(0:999))
    draws[[paste0(units)]][,paste0(re_level) := rep(units, 1000)]
    draws[[paste0(units)]][,paste0(re_level, '_re') := rnorm(n = 1000, mean = unlist(data[rn == units][,'(Intercept)']), sd = se)]
    
    mean_re[[paste0(units)]] <- data.table(hack = 'fake')
    mean_re[[paste0(units)]][,paste0(re_level):= units]
    mean_re[[paste0(units)]][,paste0(re_level, '_re') :=  unlist(data[rn == units][,'(Intercept)'])]
    mean_re[[paste0(units)]][,hack := NULL]
    
  }
  draws <- rbindlist(draws)
  mean_re <- rbindlist(mean_re)
  if(return_draws == T) return(draws) else if(return_draws == F) return(mean_re)
}

###########################
## ASFR 10-14 Prediction
###########################

## get a square data set
pred_young <- data.table(expand.grid(year_id = year_start:year_end, ihme_loc_id = loc))

## merge on covariates: asfr15-19, region, superregion
asfr_ycov <- asfr_cov[age == 15][,.(ihme_loc_id, year_id = floor(year), log_asfr_15 = log(mean))]
pred_young <- merge(pred_young, asfr_ycov, by = c('ihme_loc_id', 'year_id'), all.x = T)
pred_young <- merge(pred_young, reg_map, by = 'ihme_loc_id', all.x = T)
pred_young[,age := 10]

pred_young_draws <- copy_draws(pred_young)

### calculate draws of case counts - 1000 draws for uncertainty
# coefficient matrix
cols <- c('constant', 'log_asfr_15')
coeff <- data.frame(fixef(mod))
coeff$val <- rownames(coeff)
coeff <- data.table(coeff)
coeff <- dcast.data.table(coeff, ...~val , value.var = 'fixef.mod.')
coeff[,`.` := NULL]
coefmat <- matrix(unlist(coeff), ncol=length(cols), byrow=TRUE, dimnames=list(c('coef'), c(as.vector(cols))))

# covariance matrix
vcovmat <- vcov(mod)
vcovlist <- NULL
for (ii in 1:length(cols)) {
  vcovlist_a <- c(vcovmat[ii, 1], vcovmat[ii, 2])
  vcovlist <- c(vcovlist, vcovlist_a)
}
vcovmat2 <- matrix(vcovlist, ncol=length(cols), byrow=TRUE)

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- data.table(rmvnorm(n=1000, mean=coefmat, sigma=vcovmat2))
setnames(betadraws, c('V1', 'V2'), c('intercept', 'coef_log_asfr_15'))
betadraws[,draw := .I - 1]

## create draws of random effects.
super_reg_re <- extract_re_draws(model = mod, re_level = 'super_region_name')
reg_re <- extract_re_draws(model = mod, re_level = 'region_name')
loc_re <- extract_re_draws(model = mod, re_level = 'ihme_loc_id')

## merging random effects together to prediction
pred_young_draws <- merge(pred_young_draws, loc_re, by = c('ihme_loc_id'), all.x = T)
pred_young_draws <- merge(pred_young_draws, reg_re, by = c('region_name'), all.x = T)
pred_young_draws <- merge(pred_young_draws, super_reg_re, by = c('super_region_name'), all.x =T)

## addressing locations, regions, and super-regions that might be missing data
pred_young_draws[is.na(ihme_loc_id_re), ihme_loc_id_re := 0]
pred_young_draws[is.na(region_name_re), region_name_re := 0]
pred_young_draws[is.na(super_region_name_re), super_region_name_re := 0]

pred_young_draws[,nested_loc_re := ihme_loc_id_re + region_name_re + super_region_name_re]

## merge the beta draws
pred_young_draws <- merge(pred_young_draws, betadraws, by = 'draw', all = T)

## predict
pred_young_draws[,pred_log_ratio := intercept + nested_loc_re + (log_asfr_15 * coef_log_asfr_15)]
pred_young_draws[,asfr := exp(pred_log_ratio) * exp(log_asfr_15)]

#########################
## ASFR 50-54 Prediction
#########################

## get a square data set
pred_old <- data.table(expand.grid(year_id = year_start:year_end, ihme_loc_id = loc))
## merge on covariate: asfr15-19, region, superregion
asfr_ocov <- asfr_cov[age == 45][,.(ihme_loc_id, year_id = floor(year), asfr_45 = mean)]
pred_old <- merge(pred_old, asfr_ocov, by = c('ihme_loc_id', 'year_id'), all.x = T)
pred_old <- merge(pred_old, reg_map, by = 'ihme_loc_id', all.x = T)
pred_old[,age := 50]

## get draws
int <- data.table(mvrnorm(1000, mu = mod2$coef, Sigma = vcov(mod2)))
int[,draw := .I -1]
setnames(int, c('(Intercept)'), c('ratio'))

## 
pred_old_draws <- copy_draws(pred_old)

pred_old_draws <- merge(pred_old_draws, int, by = 'draw')
pred_old_draws[,pred_asfr_50 := asfr_45 * exp(ratio)]

###############################################
## Standardize Format, collapse to means, save
###############################################

pred_young_draws <- pred_young_draws[, year := year_id + 0.5][,.(ihme_loc_id, sim = draw, year, val = asfr)]
pred_old_draws <- pred_old_draws[, year := year_id + 0.5][,.(ihme_loc_id, sim = draw, year, val = pred_asfr_50)]

## summarize
pred_young_sum <- copy(pred_young_draws)
pred_young_sum <- pred_young_sum[,.(mean = mean(val), lower = quantile(val, probs = 0.025), upper = quantile(val, probs = 0.975)), by = c('ihme_loc_id', 'year')]

pred_old_sum <- copy(pred_old_draws)
pred_old_sum <- pred_old_sum[,.(mean = mean(val), lower = quantile(val, probs = 0.025), upper = quantile(val, probs = 0.975)), by = c('ihme_loc_id', 'year')]

year_start <- as.numeric(year_start)
year_end <- as.numeric(year_end)
## assertion checks
draw_id_vars <- list(ihme_loc_id = loc, year = (year_start +0.5):(year_end+0.5), sim = 0:999)

assertable::assert_ids(pred_young_draws, id_vars = draw_id_vars)
assertable::assert_values(pred_young_draws, colnames='val', test='gte', test_val=0)

assertable::assert_ids(pred_old_draws, id_vars = draw_id_vars)
assertable::assert_values(pred_old_draws, colnames='val', test='gte', test_val=0)

mean_id_vars <- list(ihme_loc_id = loc, year = (year_start +0.5):(year_end+0.5))

assertable::assert_ids(pred_young_sum, id_vars = mean_id_vars)
assertable::assert_values(pred_young_sum, colnames='val', test='gte', test_val=0)

assertable::assert_ids(pred_old_sum, id_vars = mean_id_vars)
assertable::assert_values(pred_old_sum, colnames='val', test='gte', test_val=0)

setnames(pred_old_draws, 'val', 'fert')
setnames(pred_young_draws, 'val', 'fert')

pred_old_draws[, age := 50]
pred_old_sum[, age := 50]
pred_young_draws[, age := 10]
pred_young_sum[, age := 10]


if(loc %in% parents_and_childs){
  write.csv(pred_old_draws, paste0(out_dir, '/FILEPATH/gpr_', loc, '_50_sim.csv'), row.names = F)
  write.csv(pred_old_sum, paste0(out_dir, '/FILEPATH/gpr_', loc, '_50.csv'), row.names = F)
  
  write.csv(pred_young_draws, paste0(out_dir, '/FILEPATH/gpr_', loc, '_10_sim.csv'), row.names = F)
  write.csv(pred_young_sum, paste0(out_dir, '/FILEPATH/gpr_', loc, '_10.csv'), row.names = F)
  
} else {
  write.csv(pred_old_draws, paste0(out_dir, '/FILEPATH/gpr_', loc, '_50_sim.csv'), row.names = F)
  write.csv(pred_old_sum, paste0(out_dir, '/FILEPATH/gpr_', loc, '_50.csv'), row.names = F)
  
  write.csv(pred_young_draws, paste0(out_dir, '/FILEPATH/gpr_', loc, '_10_sim.csv'), row.names = F)
  write.csv(pred_young_sum, paste0(out_dir, '/FILEPATH/gpr_', loc, '_10.csv'), row.names = F)
  
}

