
# 0. Settings ---------------------------------------------------------------------------
### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "malaria"
run_version <- "ADDRESS"

library(dplyr)
library(data.table)
library(ggplot2)

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_epi.R")

# Change the options in the block below as needed
gbd_year <- ADDRESS
release_id <- ADDRESS
gbd_label <- "ADDRESS"
date <- Sys.Date()
map_version <- "ADDRESS"
model_description <- "ADDRESS"

################################################################################
bundle_cw <- ADDRESS 
mv_a <- ADDRESS 
mv_b <- ADDRESS 
mv_c <- ADDRESS 
save_results <- FALSE
path <- paste0(FILEPATH)
outpath <- paste0(FILEPATH)

# 1. Set up some functions for later ---------------------------------------------------------------------------
#pull and format draws
get_the_draws <- function(modelable_entity_id, location_id, release_id, mv_id) {

  df <- get_draws('ADDRESS', modelable_entity_id, 'ADDRESS',
                  location_id = location_id, measure_id=ADDRESS, metric_id=ADDRESS,
                  release_id = release_id, version_id = mv_id,
                  sex_id = ADDRESS, age_group_id = ADDRESS,
                  year_id = c(1990,1995,2000,2005,2010,2015,2020,2022,2023,2024,2025))

  df <- as.data.table(df)
  df[, c('measure_id', 'metric_id', 'modelable_entity_id',
         'model_version_id'):=NULL]
  df <- as.data.frame(df)

  df <- setorder(df, cols = 'location_id', 'year_id', 'age_group_id', 'sex_id')
  return(df)
}

make_sum_parasitemia <- function(df1, df2){

  df1 <- as.data.table(df1)
  df1 <- setcolorder(df1, c(1,1002:1004, 2:1001))
  df2 <- as.data.table(df2)
  df2 <- setcolorder(df2, c(1,1002:1004, 2:1001))

  comb_draws<-df1[,5:1004]+df2[,5:1004]
  front_matter<-df1[,1:4]

  total<-cbind(front_matter, comb_draws)
  total <- as.data.frame(total)
  return(total)
}

#creating parasitemia proportions
make_prop <- function(numerator, denominator){
  numerator <- as.data.table(numerator)
  numerator <- setcolorder(numerator, c(1,1002:1004, 2:1001))
  denominator <- as.data.table(denominator)

  comb_draws<-numerator[,5:1004]/denominator[,5:1004]
  front_matter<-numerator[,1:4]

  df<-cbind(front_matter, comb_draws)

  df <- as.data.frame(df)
  df[is.na(df)] <- 0
  return(df)
}

#subtract parasitemia from clinical and write out draw
make_parasitemia_noclin <- function(parasitemia, clindf, prop, out_me, lid,
                                    out_dir){

  parasitemia <- as.data.table(parasitemia)
  parasitemia <- setcolorder(parasitemia, c(1,1002:1004, 2:1001))
  clindf <- as.data.table(clindf)
  clindf <- setcolorder(clindf, c(1,1002:1004, 2:1001))
  prop <- as.data.table(prop)

  propclin <- clindf[,5:1004] * prop[,5:1004]
  comb_draws <- parasitemia[,5:1004] - propclin

  front_matter<-prop[,1:4]

  df<-cbind(front_matter, comb_draws)
  df <- pmax(df, 0)

  df <- as.data.table(df)
  df[is.na(df)] <- 0

  df <- as.data.frame(df)

  temp_out <- paste0(out_dir, out_me, "/")
  dir.create(temp_out, showWarnings = F)
  write.csv(df, paste0(temp_out, lid, ".csv"), row.names = FALSE)

}

# 2. Process data ---------------------------------------------------------------------------
pfpr <- get_the_draws(ADDRESS, location_id, release_id, mv_b)
message("\nDraws for ADDRESS have been pulled")
pvpr <- get_the_draws(ADDRESS, location_id, release_id, mv_c)
message("\nDraws for ADDRESS have been pulled")
clinical_malaria <- get_the_draws(ADDRESS, location_id, release_id, mv_a)
message("\nDraws for ADDRESS have been pulled")
sum_parasitemia <- make_sum_parasitemia(pfpr, pvpr)
message("\nParasitemia has been summed")
pf_prop <- make_prop(pfpr, sum_parasitemia)
message("\nParasitemia proportions for PfPR have been made")
pv_prop <- make_prop(pvpr, sum_parasitemia)
message("\nParasitemia proportions for PvPR have been made")
make_parasitemia_noclin(pfpr, clinical_malaria, pf_prop, ADDRESS, location_id, outpath)
message("\nPfPR non clinical has been saved")
make_parasitemia_noclin(pvpr, clinical_malaria, pv_prop, ADDRESS, location_id, outpath)
message("\nPvPR non clinical has been saved")


# 5. Upload results ---------------------------------------------------------------------

if (save_results == TRUE) {
  mark_as_best <- TRUE
  username <- Sys.info()["user"]
  mes <- c(ADDRESS)
  for (me in mes){

    short_term_path <- paste0(FILEPATH)
    message("Uploading results")

    index_df  <- save_results_epi(input_dir=short_term_path,
                                  input_file_pattern="{measure_id}_{location_id}.csv",
                                  modelable_entity_id=me,
                                  description=paste0(username, " - ", model_description),
                                  measure_id = ADDRESS,
                                  release_id = release_id,
                                  mark_best=mark_as_best,
                                  bundle_id = ADDRESS, 
                                  crosswalk_version_id = bundle_cw)

  }
}
