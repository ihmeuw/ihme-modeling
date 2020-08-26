# Compare scaling for current approach vs. old approach

rm(list=ls())
library(data.table); library(parallel); library(argparse)
library(assertable)
library(mortcore, lib = "FILEPATH") # scale_results, agg_results

# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run')
parser$add_argument('--ihme_loc_id', type="character", required=TRUE,
                    help='Country to run')
parser$add_argument('--hiv_uncert', type="integer", required=TRUE,
                    help='HIV uncertainty toggle')
parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                    help='GBD round id')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help="GBD year")

args <- parser$parse_args()
gbd_year <- args$gbd_year
version_id <- args$version_id
parent_ihme <- args$ihme_loc_id
hiv.uncert <- as.logical(args$hiv_uncert)
gbd_round_id <- args$gbd_round_id

version_dir <- "FILEPATH"
output_dir <- paste0(version_dir, "/draws")

if (hiv.uncert) {
  input_dir <- paste0(version_dir, "/compiled_gpr")
} else {
  input_dir <- paste0(version_dir, "/gpr")
}

data_ids <- c("year", "sex", "sim", "location_id")

## loading in data, location lists
est_locs <- fread(paste0(version_dir, "/data/locations.csv"))
parent_location <- est_locs[ihme_loc_id == parent_ihme, location_id]

## Find all children with the parent in the path-to-top-parent
child_locations <- unique(est_locs[grepl(paste0(",", parent_location, ","), path_to_top_parent) & location_id != parent_location, location_id])
if(parent_ihme == "USA") child_locations <- child_locations[child_locations != 385]
run_location_ids <- c(parent_location, child_locations)
run_ihme_loc_ids <- est_locs[location_id %in% run_location_ids, ihme_loc_id]

## Import 45q15 draws
input_draws <- assertable::import_files(filenames = c(paste0("gpr_", run_ihme_loc_ids, "_male_sim_not_scaled.csv"),
                                                      paste0("gpr_", run_ihme_loc_ids, "_female_sim_not_scaled.csv")),
                                        folder = input_dir,
                                        multicore = T, mc.cores = 5,
                                        use.names = T)
# get results from other stages
model_results <- fread(paste0(version_dir,"/stage_2/prediction_model_results_all_stages.csv"))
model_results <- unique(model_results[, list(pred.1.wRE, pred.1.noRE, pred.2.final, sex, year, ihme_loc_id)])

input_draws <- merge(input_draws, model_results, by = c("sex", "year", "ihme_loc_id"), all.x = T)

# Save results from first two stages to merge on later
pre_gpr <- input_draws[,list(year, sex, sim, ihme_loc_id, pred.1.noRE, pred.2.final, pred.1.wRE)]
pre_gpr <- merge(pre_gpr, est_locs[, list(ihme_loc_id, location_id)], by = "ihme_loc_id")

input_draws <- input_draws[, list(year, sex, sim, ihme_loc_id, mort)]
input_draws <- merge(input_draws, est_locs[, list(ihme_loc_id, location_id)], by = "ihme_loc_id")
input_draws[, ihme_loc_id := NULL]


################################
## Step 1: Prep population and input draws
################################
population <- fread(paste0(version_dir, "/data/population.csv"))
pop <- population[location_id %in% c(parent_location, child_locations)]
pop[,process_version_map_id := NULL]

#creating old andhra pradesh
if(parent_ihme == "IND") {
  pop_weight <- population[location_id %in% c("4841", "4871")]
  setkey(pop_weight, sex_id, year_id)
  pop_weight <- pop_weight[,list(population = sum(population), location_id = 44849), by = key(pop_weight)]
  
  pop  <- rbind(pop, pop_weight, use.names = T, fill=T)
}

setkey(pop, location_id, year_id, sex_id)
pop <- pop[,list(scale_var = sum(population)), by=key(pop)]
pop[sex_id == 1, sex := "male"]
pop[sex_id == 2, sex := "female"]
pop <- pop[, sex_id := NULL]
setnames(pop, "year_id", "year")


## Convert from qx to mx and then to death space
input_draws[, mort := log(1-mort)/-45]
input_draws  <- merge(input_draws, pop, by = c("location_id", "year", "sex"))
input_draws[, mort := mort * scale_var]
input_draws[, scale_var := NULL]
input_draws[, sim := sample(0:999), by = c("location_id", "year", "sex")]


################################
## Step 2: Scale everything if not South Africa
################################
## Scale results with GBR 1981 exception
agg_locations <- c("ZAF", "IRN", "BRA")

if(!(parent_ihme %in% agg_locations)) {
  scaled_data <- scale_results(input_draws, id_vars = data_ids,
                               value_var = "mort", location_set_id = 82,
                               exception_gbr_1981 = T,
                               exclude_parent = "CHN",
                               gbd_year = gbd_year)
}

################################
## Step 3: Aggregate children if South Africa
################################
# Aggregate ZAF, rather than scaling
if(parent_ihme %in% agg_locations) {
  scaled_data <- agg_results(input_draws[location_id != parent_location],
                             id_vars = data_ids,
                             value_var = "mort",
                             end_agg_level = 3,
                             loc_scalars = F,
                             tree_only = parent_ihme,
                             gbd_year = gbd_year)
}

################################
## Step 4: Convert to qx space and compare output
################################
scaled_data <- merge(scaled_data, pop, by = c("location_id", "year", "sex"))
scaled_data[, mort := mort / scale_var]
scaled_data[, mort := 1 - exp(-45 * mort)]

scaled_data[, year := year + 0.5]
scaled_data <- merge(scaled_data, pre_gpr, by = c("location_id", "year", "sex", "sim"))
scaled_data[,c("scale_var") := NULL]

setkey(scaled_data, ihme_loc_id, location_id, year, sex)
sum_scaled <- scaled_data[,list(mort_med = mean(mort),mort_lower = quantile(mort,probs=.025),
                                mort_upper = quantile(mort,probs=.975),
                                med_stage1=quantile(pred.1.noRE,.5),
                                med_stage2 =quantile(pred.2.final,.5)),
                          by=key(scaled_data)]



################################
## Step 5: Output files
################################
for(loc_id in unique(scaled_data$location_id)){
  cc <- est_locs[location_id == loc_id, ihme_loc_id]
  for(ss in unique(scaled_data$sex)){
    scaled_data_ss <- scaled_data[sex == ss & location_id == loc_id,]
    sum_scaled_ss <- sum_scaled[sex == ss & location_id == loc_id,]
    fwrite(scaled_data_ss, paste0(output_dir,  "/gpr_", cc,"_", ss, "_sim.csv"))
    fwrite(sum_scaled_ss, paste0(output_dir,  "/gpr_", cc,"_", ss, ".csv"))
    
  }
}