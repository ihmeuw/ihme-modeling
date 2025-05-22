

# create regression for birth weight & gestational age, then predict

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_location_metadata.R"  )
source("FILEPATH/get_demographics.R"  )
source("FILEPATH/get_draws.R"  )
source("FILEPATH/find_non_draw_cols.R")

# reticulate::use_python('FILEPATH')
# library(argparse)
# parser <- ArgumentParser(description='Mean Modelling task for LBWSG, parallelized by location')
# parser$add_argument("--location", "-loc", required=TRUE, help='Location for which to calculate')
# parser$add_argument("--type", required=TRUE, help='Type being ga or bw')
# parsed_args <- parser$parse_args() 
# location <- parsed_args$location
# type <- parsed_args$type
# rm(parser)
# rm(parsed_args)
# 
# constants <- reticulate::import('lbwsg_parameters')
# input_me_id <- if (type=='bw') constants$ME_MAP$stgpr[['lbw']] else constants$ME_MAP$stgpr[['ga_37']]
# output_me_id <- if (type=='bw') constants$ME_MAP$mean_modeling[['lbw']] else constants$ME_MAP$mean_modeling[['ga']]
# decomp_step <- constants$DECOMP_STEP
# gbd_round_id <- constants$GBD_ROUND_ID
# model <- readRDS(paste0(constants$MeanModeling$RESULTS_PATH, type, '_model.RDS'))


run_interactively = 0

if(run_interactively == 1){
  
  location = 7
  type = "bw"#"ga"
  input_me_id = 24450#24449
  output_me_id = 15803#15802
  decomp_step = "iterative"
  gbd_round_id = 7
  
} else {
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  type <- param_map[task_id, type]
  location <- param_map[task_id, location]
  input_me_id <- param_map[task_id, input_me_id]
  output_me_id <- param_map[task_id, output_me_id]
  decomp_step <- param_map[task_id, decomp_step]
  gbd_round_id <- param_map[task_id, gbd_round_id]
  
}

dir = paste0("FILEPATH/")
model <- readRDS(paste0(dir, "microdata_model/", type, "_model.rds"))

#############

estimation_years <- get_demographics("epi", gbd_round_id = gbd_round_id)$year_id


#to_fit <- fread(file.path("FILEPATH", input_me_id, "draws_temp_0", paste0(location, ".csv")))

#TODO: get clarity as to whether this is not going to be done again anymore and delete
to_fit <- get_draws(source = "epi",
                    gbd_id_type = "modelable_entity_id",
                    gbd_id = input_me_id,
                    location_id = location,
                    age_group_id = list(2),
                    year_id = estimation_years,
                    gbd_round_id = gbd_round_id,
                    decomp = decomp_step,
                    status = "best",
                    num_workers = 1)


to_fit[, age_group_id := 164]
to_fit[, measure_id := 19]
to_fit <- to_fit[, -c("metric_id")]

non_draw_cols <- find_non_draw_cols(to_fit)

to_fit <- melt(to_fit, id.vars = non_draw_cols, variable.factor = F) 

setnames(to_fit, "value", "prev")

predicted = data.table(predicted = predict(model, to_fit, re.form = NA))
predicted <- cbind(to_fit, predicted)

predicted_draws <- dcast(predicted, formula = paste0(paste(non_draw_cols, collapse = " + "), " ~ variable"), value.var = "predicted")

draw_dir <- file.path(dir, output_me_id)

if(isFALSE(dir.exists(draw_dir))){
  dir.create(draw_dir, recursive=TRUE)
}

predicted_draws <- predicted_draws[, -c("modelable_entity_id")]

predicted_draws_agid2 <- copy(predicted_draws)
predicted_draws_agid2[, age_group_id := 2]
predicted_draws <- rbind(predicted_draws, predicted_draws_agid2)

write.csv(predicted_draws, file.path(draw_dir, paste0(location, ".csv")), row.names = F, na = "")