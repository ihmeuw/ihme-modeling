##################################################
## Project: CVPDs
## Script purpose: Child script to create draws of each location_month's prediction from cascade spline model using sample_simple
## Date: May 13, 2021
## Author: USERNAME
##################################################
rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2)
username <- Sys.info()[["user"]]
date <- gsub("-", "_", Sys.Date())
gbd_round_id <- 7
decomp_step <- "step3"

# SOURCE SHARED FUNCTIONS ------------------------------
shared_functions <- "/FILEPATH/"
functions <- c("get_location_metadata.R")
for (func in functions) {
  source(paste0(shared_functions, func))
}

# load packages, install if missing

packages <- c("data.table","magrittr","matrixStats", "MASS")
for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    library(p, character.only = T, lib.loc = "/FILEPATH")
  } else {
    library(p, character.only = T)
  }
}

library(mrbrt001, lib.loc = "/FILEPATH/")


args <- commandArgs(trailingOnly = TRUE)


# arguments
n_draws <- args[[1]] %>% as.integer() # how many draws
loc_id <- args[[2]] %>% as.integer() # what location are you getting draws for
super_region <- args[[3]] %>% as.logical() # are you sampling for SR model (ie if loc_id is location without data in input data frame)
my_work_dir <- args[[4]] # where to read input data etc from (ie cause specific work dir from main 02 script)
my_cause <- args[[5]] # measles or flu?
region_weight <- args[[6]] %>% as.numeric #for locs w/o data in input data, how much more likely should we be to sample from locs in same SR than diff SR?
final_n_draws <- args[[7]] # how many draws to downsample to (should be 1000)

ratio <- paste0("cause_", my_cause)

draw_cols <- paste0("draw_", 0:(as.integer(final_n_draws)-1))

final_pred_dt <- fread(file.path(my_work_dir, "pred_frame.csv"))
dt <- fread(file.path(my_work_dir, "input_data.csv"))

loc_sample_1 <- matrix(nrow = n_draws, ncol = 7) # empty matrix to hold samples of the betas (1 col per spline coeff + 1 on mask use)

if(super_region){

  tmp_pred <- final_pred_dt[!(location_id %in% dt[cause == my_cause]$location_id) & super_region_id == loc_id & month %in% c(1:12)] #subset to just the countries that don't have input data and are in this SR

  # sample from country-specific regional and global results (locations with data in input data) -- region_weight is how many more times locations in same SR as the loc of interest will be in the set to sample from than the locations in rest of world
  # locations in same SR are region_weight more times more likely to be sampled
  # better than sampling just from SR model because that model has more data so would give narrower uncert for locs without any input data -- not possible
  sample_locs <- sample(c(unique(dt[cause == my_cause, location_id]), rep(unique(dt[super_region_id == loc_id & cause == my_cause, location_id]), region_weight-1)), size = n_draws, replace = T)

  i <- 1

  for(loc in unique(sample_locs)){

    n_samples <- sum(sample_locs == loc)

    model1 <- py_load_object(file.path(my_work_dir, "stage1/pickles", paste0("loc_cause__", loc, "_", my_cause,".pkl")), pickle = "dill")

    loc_sample_1[i:(i+n_samples-1),] <- mrbrt001::core$other_sampling$sample_simple_lme_beta(sample_size = as.integer(n_samples), model = model1)

    i <- i+n_samples # make next locs samples will append on to loc_sample_1 below prev loc's samples
  }


}else{

  tmp_pred <- final_pred_dt[location_id == loc_id & month %in% c(1:12)]

  # sample directly from location-specific results
  model1 <- py_load_object(file.path(my_work_dir, "stage1/pickles", paste0("loc_cause__", loc_id, "_", my_cause,".pkl")), pickle = "dill")

  loc_sample_1 <- mrbrt001::core$other_sampling$sample_simple_lme_beta(sample_size = as.integer(n_draws), model = model1)

}

dat_pred <- MRData()

dat_pred$load_df(
  data = tmp_pred,
  col_covs=list("mob_avg_month", "mask_avg_month")
)


# create draws of preds from draws of betas and move out of log space
loc_draws <- model1$create_draws(dat_pred,
                                 beta_samples = loc_sample_1,
                                 gamma_samples = matrix(nrow = n_draws, ncol = 6, data = 0), # number of columns should be same as number of spline segments
                                 random_study = TRUE, # this can be true or false because draws of gamma are 0 (b/c no REs in model) so will no affect preds. If gamma nonzero, setting to T will include draws from gamma.
                                 sort_by_data_id = TRUE) %>% exp()

# take 1000 quantiles of the actual pred draws -- subset from larger number to 1000 draws to use to adjust the 1000 draws of GBD estimates
loc_draws <- rowQuantiles(loc_draws, probs = seq(0,1,length.out = as.integer(final_n_draws)))

tmp_pred[, c(paste0("ratio_", draw_cols)) := as.data.table(loc_draws)]
write.csv(tmp_pred, file.path(my_work_dir, "draws", paste0(loc_id, "_draws.csv")), row.names = F)
