rm(list=ls())

library(readr); library(foreign); library(plyr); library(rhdf5); library(data.table); library(assertable); library(assertthat)


## Setup filepaths
if (Sys.info()[1]=="Windows") {
  root <- "J:" 
  user <- Sys.getenv("USERNAME")
  
  
} else {
  root <- "/home/j"
  user <- Sys.getenv("USER")
  windows <- FALSE
}

args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  country <- commandArgs(trailingOnly = T)[1]
  group <- paste0(commandArgs(trailingOnly = T)[2]) # Enforce it as string
  spec_name <- commandArgs(trailingOnly = T)[3]
  new_upload_version <- commandArgs(trailingOnly = T)[4]
  mlt_lt_version <- commandArgs(trailingOnly = T)[5]
  start_year <- as.integer(commandArgs(trailingOnly = T)[6])
  gbd_year <- as.integer(commandArgs(trailingOnly = T)[7])
} else {
  country <- 'AGO'
  group <- "1A"
  spec_name 
  new_upload_version 
  start_year <- 1950
  gbd_year <- 2022
  mlt_lt_version 
}

stgpr_dir <- "FILEPATH"

if(country %in% c("STP","MRT","COM")){
  group <- "1A"
}

master_dir <- paste0("FILEPATH",new_upload_version)

## Inputs
input_dir <- paste0(master_dir, "/inputs")
spec_sim_dir <- paste0("FILEPATH", spec_name)
mlt_dir <- paste0("FILEPATH", mlt_lt_version)
input_hiv_free_dir <- paste0(mlt_dir, "/lt_hiv_free/scaled")
input_with_hiv_dir <- paste0(mlt_dir, "/lt_with_hiv/scaled")
input_env_dir <- paste0(mlt_dir, "/env_with_hiv/scaled")



## Outputs
whiv_dir <- paste0(master_dir,"/envelope_whiv")
hiv_free_dir <- paste0(master_dir,"/envelope_hivdel")
out_dir_hiv <- paste0(master_dir,"/hiv_output")

## Grab functions etc.
home.dir <- paste0(ifelse(windows, "H:/", "/homes/"))


years <- c(start_year:gbd_year)
gbd_round <-  7

locations <- fread(paste0(input_dir,"/locations_mort.csv"))
loc_id <- locations[ihme_loc_id==country, location_id]
loc_name <- locations[ihme_loc_id==country, location_name]
assert_that(!is.null(loc_id) && !is.na(loc_id))

age_map <- fread(paste0(input_dir,"/age_map_01.csv"))[, list(age_group_id, age_group_name_short)]
setnames(age_map,"age_group_name_short","age_group_name")
age_map[age_group_name == "95+", age_group_name := "95"]
age_map = unique(age_map)
ages <- unique(age_map$age_group_id)
age_map_old <- get_age_map(gbd_year = 2019)
age_map_old[,age_group_name := NULL]
setnames(age_map_old,"age_group_name_short","age_group_name")
age_map_old[age_group_name == "95+", age_group_name := "95"]
age_map_old = unique(age_map_old)
age_map_old <- age_map_old[age_group_id != 33,]
age_map_old <- age_map_old[age_group_id != 21,]

ages_old <- unique(age_map_old$age_group_id)
## Use draw maps to scramble draws so that they are not correlated over time
## This is because Spectrum output is semi-ranked due to Ranked Draws into EPP
## Then, it propogates into here which would screw up downstream processes
## This is done here as opposed to raw Spectrum output because we want to preserve the draw-to-draw matching of Spectrum and lifetables, then scramble them after they've been merged together
draw_map <- fread(paste0(input_dir, "/draw_map.csv"))
# draw_map <- draw_map[location_id==loc_id,list(old_draw,new_draw)]
# setnames(draw_map,"old_draw","sim")

compute_sims <- c(0:999)
lt_ages <- c(0, 1, seq(5, 95, 5))
age_map_old <- age_map_old[age_group_name %in% lt_ages]
lt_id_vars <- list(year = years, sim = compute_sims, sex_id = c(1:2), age = lt_ages)

###############################################################################################################
## Bring in required input files

## Population
pop <- fread(paste0(input_dir, "/pop.csv"))
pop <- pop[location_id==loc_id & sex_id %in% c(1:2) & age_group_id %in% ages, ]
pop <- pop[, list(year_id, sex_id, age_group_id, population)]
pop[, year_id := as.integer(year_id)]
setnames(pop, "population", "pop_gbd")
setnames(pop, "year_id", "year")

## HIV-Free and With-HIV Lifetables
import_lts <- function(location_id, lt_type) {
  file_prefix <- paste0(lt_type, "_lt")
  print(paste0("input_", lt_type, "_dir"))
  
  data <- assertable::import_files(filenames = paste0(file_prefix, "_", years, ".h5"),
                                   folder = get(paste0("input_", lt_type, "_dir")),
                                   FUN = load_hdf,
                                   by_val = location_id)
  assert_values(data, colnames(data), test= "not_na")
  ##TODO: This was supposed to check for sex = "both", but that wasn't backfilled in ditzels and was throwing an error, so needed to remove.
  ##This doesn't make a difference since format_lts removes "both".
  data <- data[! sex == 'both']
  id_vars <- list(year = years, sim = compute_sims, sex = c("male", "female"), age = c(0, 1, seq(5, 110, 5)))
  assert_ids(data, id_vars = id_vars)
  
  return(data.table(data))
}

format_lts <- function(data, lt_type) {
  data <- copy(data)
  
  setnames(data, "sim", "draw")
  
  data[sex=="male", sex_id := 1]
  data[sex=="female", sex_id := 2]
  data[sex=="both", sex_id := 3]
  data[, sex := NULL]
  
  data <- data[sex_id !=3 & year >= start_year, list(sex_id, year, age, draw, mx, ax, qx)]
  
  setkeyv(data, c("sex_id", "year", "draw", "age"))
  
  # Approximate 95+ mx by taking lx/tx
  gen_age_length(data)
  qx_to_lx(data)
  lx_to_dx(data)
  gen_nLx(data)
  gen_Tx(data, id_vars = c("sex_id", "year", "draw", "age"))
  
  data[age==95, mx:=lx/Tx] 
  
  data <- data[age <= 95, list(sex_id, year, age, draw, mx)]
  
  assert_values(data, colnames(data), "not_na")
  assert_values(data, c("mx"), "gte", 0)
  
  setnames(data,c("draw", "mx"),c("sim", paste0("mx_env_",lt_type)))
  assert_ids(data, id_vars = lt_id_vars)
  return(data)
}

lt_hiv_free <- import_lts(loc_id, "hiv_free")
lt_hiv_free <- format_lts(lt_hiv_free,"hiv_free")

lt_whiv <- import_lts(loc_id, "with_hiv")
lt_whiv <- format_lts(lt_whiv,"whiv")

lt_combined <- merge(lt_hiv_free, lt_whiv, by=c("sex_id","year","age","sim"))
assert_ids(lt_combined, id_vars = lt_id_vars)

# If a decimal point issue, enforce 0
lt_combined[(mx_env_whiv - mx_env_hiv_free) < 0 & (mx_env_whiv - mx_env_hiv_free) > -.0000000001, mx_env_whiv := mx_env_hiv_free]
lt_combined[, mx_env_hiv := mx_env_whiv - mx_env_hiv_free]

# dump_diagnostic_file(data=lt_combined[mx_env_hiv < 0, ], reckoning_version=new_upload_version, folder="01_run_ensemble", filename=paste0(country, "_mx_env_hiv_lt0.csv"))

lt_combined[mx_env_hiv < 0 & age == 95, mx_env_hiv := 0] # For now, do it if there's any magnitude of difference at all
assert_values(lt_combined, "mx_env_hiv", "gte", 0)
lt_combined[, age := as.character(age)]
lt_combined <- merge(lt_combined, age_map_old[,.(age_group_id,age_group_name)], by.x = "age", by.y = "age_group_name")
lt_combined[, age := as.numeric(age)]

assert_ids(lt_combined, id_vars = lt_id_vars)

lt_u1 <- lt_combined[age == 0, list(sex_id, year,sim, mx_env_hiv)]
lt_1to4 <- lt_combined[age %in% c(1), list(sex_id,year, sim, mx_env_whiv, mx_env_hiv_free)]
lt_5to14 <- lt_combined[age %in% c(5, 10), list(age_group_id, sex_id,year, sim, mx_env_whiv, mx_env_hiv_free)]
lt_95plus <- lt_combined[age == 95, list(sex_id, year, sim, mx_env_whiv, mx_env_hiv_free)] 


# With-HIV Envelope (to get NN breakdowns of envelope HIV and 1-4 breakdowns of envelope HIV, and to replace with-HIV over-95 with envelope over-95)
env <- assertable::import_files(filenames = paste0("with_hiv_env_", years, ".h5"),
                                folder = input_env_dir,
                                FUN = load_hdf,
                                by_val = loc_id)


assert_values(env[year_id >= 1950], colnames = colnames(env), test="not_na")
env[age_group_id == 49, age_group_id := 238]

env <- env[year_id >= start_year & !sex_id == 3 & (age_group_id == 235 | age_group_id %in% c(2:5)),]
env[, location_id := NULL]
setnames(env, c("year_id"), c("year"))


env <- merge(env, pop, by=c("sex_id","year","age_group_id")) 
env[, deaths := deaths / pop_gbd]
env[, pop_gbd := NULL]
setnames(env, c("deaths"), c("mx_env_whiv"))

assert_values(env[year >= 1950], colnames(env), "not_na")
assert_values(env, "mx_env_whiv","gte", 0, na.rm = T)
id_vars <- list(year = years, sex_id = c(1, 2), age_group_id = c(2:5,235), sim = compute_sims)
assert_ids(env, id_vars)

env_95plus <- env[age_group_id == 235,]
env_1to4 <- env[age_group_id %in% c(5),]
env_nn <- env[age_group_id %in% c(2:4),]


## Rescale LT over-95 approximated mx with envelope real 95+ mx
## This is because we always want the 95+ envelope to be consistent, so we basically rescale hiv-free envelope and implied HIV to make it all mix
setnames(env_95plus,"mx_env_whiv","env_mx")
lt_95plus <- merge(lt_95plus, env_95plus, by=c("sex_id", "year", "sim"))
#If pop = 0, env_mx will be na because of the format_env function
lt_95plus[is.na(env_mx), env_mx:= mx_env_whiv]
lt_95plus[,scalar := env_mx / mx_env_whiv]
lt_95plus[,mx_env_whiv := scalar * mx_env_whiv]
lt_95plus[,mx_env_hiv_free := scalar * mx_env_hiv_free]
lt_95plus[,mx_env_hiv := mx_env_whiv - mx_env_hiv_free]

# dump_diagnostic_file(data=lt_95plus[mx_env_hiv < 0,], reckoning_version=new_upload_version, folder="01_run_ensemble", paste0(country, "_mx_env_hiv_lt0_95p.csv"))

lt_95plus[mx_env_hiv < 0, mx_env_hiv := 0] # For now, do it if there's any magnitude of difference at all

lt_95plus[, c("scalar","env_mx") := NULL]
lt_95plus[, age := "95"]

lt_combined <- lt_combined[age_group_id != 235,]
lt_combined <- rbindlist(list(lt_combined,lt_95plus),use.names=T)


## Spectrum Results (if Group 1 [GEN], Group 2B [CON incomplete VR], or Group 2C [CON no data])
## Note that non-HIV deaths are only used for Group 1 and aren't accurate for ENN/LNN/PNN (they represent under-1 deaths, need to be split by envelope)

##Using this also for PHL locations that do not have COD data
no.cod.locs <- c("PHL_53598", "PHL_53609", "PHL_53610", "PHL_53611", "PHL_53612", "PHL_53613","IDN_4740","IDN_4741","IDN_4739",locations[grepl("CHN",ihme_loc_id),ihme_loc_id])

no_cod_location_ids  <- locations[ihme_loc_id %in% no.cod.locs,location_id]
china <- locations[grepl("CHN",ihme_loc_id),location_id]

## GROUP 1 COUNTRIES: Pull out under-15 values from LTs
convert_num <- function(conv_data, convert_vars) {
  ## Convert from rate to number space before collapsing
  conv_data <- merge(conv_data, pop, by = c("sex_id","year","age_group_id"))
  mult_pop <- function(x) return(x * conv_data[['pop_gbd']])
  conv_data[, (convert_vars) := lapply(.SD, mult_pop), .SDcols = convert_vars] 
  conv_data[,pop_gbd:=NULL]
  return(conv_data)
}

convert_rate <- function(conv_data, convert_vars) {
  ## Convert from rate to number space before collapsing
  conv_data <- merge(conv_data, pop, by = c("sex_id","year","age_group_id"))
  
  mult_pop <- function(x) return(x / conv_data[['pop_gbd']])
  conv_data[, (convert_vars) := lapply(.SD, mult_pop), .SDcols = convert_vars] 
  conv_data[, pop_gbd := NULL]
  return(conv_data)
}

if(group %in% c("1A","1B","2C") | loc_id %in% no_cod_location_ids) {
  spec_draws <- data.table(assertable::import_files(paste0(country, "_ART_deaths.csv"), folder=spec_sim_dir))
  setnames(spec_draws,c("year_id", "run_num", "hiv_deaths"), c("year", "sim", "mx_spec_hiv"))
  spec_draws[, sim := sim - 1] # Format sims to be in the same number-space
  spec_draws[mx_spec_hiv == 0 & non_hiv_deaths == 0, non_hiv_deaths_prop := 1]
  save_once = spec_draws
  
  spec_draws_count = convert_num(spec_draws[age_group_id %in% c(34,238,388,389)],c("mx_spec_hiv","non_hiv_deaths"))
  spec_draws_count[age_group_id %in% c(34,238),age_group_id := 5]
  spec_draws_count[age_group_id %in% c(388,389),age_group_id := 4]
  spec_draws_count <- spec_draws_count[,list(mx_spec_hiv = sum(mx_spec_hiv), non_hiv_deaths = sum(non_hiv_deaths), non_hiv_deaths_prop = mean(non_hiv_deaths_prop)), by = c("age_group_id","sex_id","year","sim")]
  
  spec_draws_old_ages = convert_rate(unique(spec_draws_count),c("mx_spec_hiv","non_hiv_deaths"))     
  spec_draws = spec_draws[!age_group_id %in% c(34,238,388,389)]
  spec_draws = rbind(spec_draws, spec_draws_old_ages, use.names=TRUE)
  
  min_spec_year <- min(spec_draws[, year])
  if (min_spec_year > start_year) {
    age_group_ids <- unique(spec_draws[, age_group_id])
    sex_ids <- unique(spec_draws[, sex_id])
    sims <- unique(spec_draws[, sim])
    new_years <- start_year : min_spec_year-1
    
    new_rows <- expand.grid(sex_id = sex_ids, year = new_years, age_group_id = age_group_ids, sim = sims, mx_spec_hiv = 0, non_hiv_deaths = 2, non_hiv_deaths_prop = 1)
    spec_draws <- rbindlist(list(spec_draws, new_rows), use.names = T)
  }
}

## ST-GPR Results (if Group 2A [CON complete VR])
## For these, we trust their VR systems so we take straight GPR results
if(group %in% c("2A", '2B') && !(loc_id %in% no_cod_location_ids)) {
  
  #################################################
  spec_draws <- import_files("gpr_results.csv", folder=stgpr_dir)
  spec_draws <- spec_draws[location_id == loc_id & !is.na(gpr_mean), list(year_id, age_group_id, sex_id, gpr_mean, gpr_var)]
  
  assert_that(nrow(spec_draws) > 0)
  assert_values(spec_draws, colnames(spec_draws), "not_na")
  ## Generate 1000 draws by location/year/age/sex
  ## Need to use Delta Method to transform into real space before making draws
  spec_draws[gpr_mean == 0,zero := 1]
  spec_draws[gpr_mean != 0,gpr_var := ((1/gpr_mean)^2)*gpr_var]
  spec_draws[gpr_mean != 0,gpr_sd := sqrt(gpr_var)]
  spec_draws[gpr_var == 0, gpr_sd := 0]
  spec_draws[gpr_mean != 0,gpr_mean := log(gpr_mean)]
  
  ## Create 1000 normal sims around the logged mean/sd
  sims <- spec_draws[,list(gpr_mean,gpr_sd)]
  setnames(sims,c("mean","sd"))
  sims <- data.table(mdply(sims,rnorm,n=1000))
  
  ## Combine and reshape the results, then back-transform
  spec_draws <- cbind(spec_draws,sims)
  spec_draws[,c("mean","sd","gpr_mean","gpr_var","gpr_sd"):=NULL]
  spec_draws <- melt(spec_draws,id.vars=c("year_id","age_group_id","sex_id","zero"),variable.name="sim")
  spec_draws[,sim:=as.numeric(gsub("V","",sim))-1]
  spec_draws[,mx_spec_hiv:=exp(value)/100] # Convert to real numbers then divide by 100 since the death rate is in rate per capita * 100
  spec_draws[zero==1,mx_spec_hiv:=0]
  
  spec_draws_copy <- copy(spec_draws[age_group_id==6])
  spec_draws_copy[,age_group_id := 2]
  spec_draws_copy[,c('mx_spec_hiv') := 0]
  spec_draws <- rbind(spec_draws, spec_draws_copy)
  
  spec_draws_copy <- copy(spec_draws[age_group_id==6])
  spec_draws_copy[,age_group_id := 3]
  spec_draws_copy[,c('mx_spec_hiv') := 0]
  spec_draws <- rbind(spec_draws, spec_draws_copy)
  
  
  
  spec_draws[mx_spec_hiv < 0, mx_spec_hiv := 0]
  save_once = spec_draws
  
  spec_draws <- spec_draws[,list(year_id,age_group_id,sex_id,sim,mx_spec_hiv)]
  setnames(spec_draws,"year_id","year")
  
  
  spec_draws_count = convert_num(spec_draws[age_group_id %in% c(34,238,388,389)],c("mx_spec_hiv"))
  spec_draws_count[age_group_id %in% c(34,238),age_group_id := 5]
  spec_draws_count[age_group_id %in% c(388,389),age_group_id := 4]
  spec_draws_count <- spec_draws_count[,list(mx_spec_hiv = sum(mx_spec_hiv)), by = c("age_group_id","sex_id","year","sim")]
  
  #but then need to revert to rates so that it can run through the rest of the code. mx stands for the mean of the draws 
  spec_draws_old_ages = convert_rate(unique(spec_draws_count),c("mx_spec_hiv"))     
  spec_draws = spec_draws[!age_group_id %in% c(34,238,388,389)]
  spec_draws = rbind(spec_draws, spec_draws_old_ages, use.names=TRUE)
  
}
#Create old age groups to facilitate merge
assert_values(spec_draws, colnames(spec_draws), "not_na")
assert_values(spec_draws, "mx_spec_hiv", "gte", 0)

id_vars <- list(year = unique(spec_draws$year), sim = compute_sims, age_group_id = ages_old[!ages_old %in% c(22,28,42,48)], sex_id = c(1:2))
assert_ids(spec_draws, id_vars)


###############################################################################################################
## Split NN and under-1 envelope/LT into with-HIV and HIV-free proportional to the populations in those age groups 
env_nn <- merge(env_nn,lt_u1,by=c("sex_id","year","sim"))
env_nn[age_group_id %in% c(2,3),mx_env_hiv:=0] # We assume no HIV deaths to ENN and LNN age groups

# Because the PNN envelope with HIV is not necessarily higher than all HIV under-1 deaths due to different estimation processes,
# We need to constrain under-1 HIV to 90% of the PNN hiv-deleted envelope
# dump_diagnostic_file(data=env_nn[mx_env_hiv > (.9 * mx_env_whiv),], reckoning_version=new_upload_version,  folder="01_run_ensemble", filename=paste0(country, "_env_nn_mx_env_hiv_capped.csv"))
env_nn[mx_env_hiv > (.9 * mx_env_whiv), mx_env_hiv:= .9 * mx_env_whiv]
env_nn[,mx_env_hiv_free := mx_env_whiv - mx_env_hiv]


###############################################################################################################
## Use the all-cause 1-4 envelope as the standard, because LT mx doesn't equal envelope/pop 
## We will take the ratio of HIV-free to with-HIV from the LT results, then use all-cause mortality from envelope times the ratio to get HIV-free mx
setnames(env_1to4, "mx_env_whiv", "all_cause_envelope")
env_1to4 <- merge(env_1to4, lt_1to4,by=c("sex_id","year","sim"))
env_1to4[, scalar := mx_env_hiv_free / mx_env_whiv]
env_1to4[, mx_env_hiv_free := all_cause_envelope * scalar]

env_1to4[,c("mx_env_whiv","scalar"):=NULL]
setnames(env_1to4,"all_cause_envelope","mx_env_whiv")

env_1to4 = unique(env_1to4)
id_vars <- list(year = years, sex_id = c(1:2), age_group_id = c(5), sim = compute_sims)
assert_ids(env_1to4, id_vars)

env_1to4[, mx_env_hiv := mx_env_whiv - mx_env_hiv_free]

###############################################################################################################
## Group 1 countries: Use the non-HIV death to all-cause ratio from Spectrum, and multiply it by all-cause envelope to get the HIV-free envelope

## GROUP 1 COUNTRIES: Pull out under-15 values from LTs
convert_num <- function(conv_data, convert_vars) {
  ## Convert from rate to number space before collapsing
  conv_data <- merge(conv_data, pop, by = c("sex_id","year","age_group_id"))
  mult_pop <- function(x) return(x * conv_data[['pop_gbd']])
  conv_data[, (convert_vars) := lapply(.SD, mult_pop), .SDcols = convert_vars] 
  conv_data[,pop_gbd:=NULL]
  return(conv_data)
}

convert_rate <- function(conv_data, convert_vars) {
  ## Convert from rate to number space before collapsing
  conv_data <- merge(conv_data, pop, by = c("sex_id","year","age_group_id"))
  
  mult_pop <- function(x) return(x / conv_data[['pop_gbd']])
  conv_data[, (convert_vars) := lapply(.SD, mult_pop), .SDcols = convert_vars] 
  conv_data[, pop_gbd := NULL]
  return(conv_data)
}

get_u15_hiv <- function(data) {
  data <- data[age_group_id <= 7,]
  data <- convert_num(data,c("mx_spec_hiv", "non_hiv_deaths"))
  
  # dump_diagnostic_file(data=data[non_hiv_deaths == 0], reckoning_version=new_upload_version, folder="01_run_ensemble", filename=paste0(country, "_non_hiv_deaths_zero.csv"))
  data[non_hiv_deaths == 0 ,non_hiv_deaths := 1] # For gap-filled results from start_year-198?
  
  ## Collapse NN granular to under-1 
  data[age_group_id <=4, age_group_id := 28]
  data <- data[, lapply(.SD,sum),.SDcols=c("mx_spec_hiv","non_hiv_deaths"),
               by=c("age_group_id","sex_id","year","sim")] 
  
  ## Get ratio of HIV-free to with-HIV (doesn't matter that it's in number vs. rate space since denominator is the same)
  data[, hiv_free_ratio := non_hiv_deaths / (mx_spec_hiv + non_hiv_deaths)]
  data <- data[, list(age_group_id, sex_id, year, sim, hiv_free_ratio)]
  return(data)
}

get_u1_env <- function(data) {
  data <- convert_num(data,c("mx_env_whiv"))
  data <- data.table(data)[,lapply(.SD,sum),.SDcols=c("mx_env_whiv"),
                           by=c("sex_id","year","sim")] 
  return(data)
}


create_u15_mx_group1 <- function() {
  ## We want to use the ratio of Spectrum HIV-deleted to with-HIV to convert lifetable with-HIV to lifetable HIV-free
  ## data is with-HIV dataset from get_u15_lt, already under-1 non-granular
  ## We want to take in the spectrum NN results, collapse to under-1, merge on with the u5 HIV, calculate the 
  
  env_convert_vars <- c("mx_env_whiv","mx_env_hiv","mx_env_hiv_free")
  
  hiv_u15 <- get_u15_hiv(spec_draws)
  
  ## Calculate post-neonatal deaths
  env_u1 <- get_u1_env(env_nn)
  pnn_calc <- merge(env_u1, hiv_u15[age_group_id == 28,], by=c("sex_id","year","sim"))
  pnn_calc[,mx_avg_hiv:= mx_env_whiv - (mx_env_whiv * hiv_free_ratio)]
  pnn_calc[,age_group_id:=4]
  pnn_calc <- pnn_calc[,list(age_group_id,sex_id,year,sim,mx_avg_hiv)]
  
  ## Merge back on PNN deaths
  final_nn <- convert_num(env_nn, env_convert_vars)
  final_nn <- merge(final_nn, pnn_calc, all.x=T, by=c("age_group_id","sex_id","year","sim"))
  final_nn[age_group_id %in% c(2,3), mx_avg_hiv := 0] # Still no deaths in enn/lnn
  
  # First, constrain HIV to at most be 90% of the all-cause total (some draws in ZAF violate this in PNN due to the ratio being applied at U-1 level, not PNN)
  # dump_diagnostic_file(data=final_nn[mx_avg_hiv > (.9 * mx_env_whiv)], reckoning_version=new_upload_version,  folder="01_run_ensemble", filename=paste0(country, "_final_nn_mx_avg_capped.csv"))
  final_nn[mx_avg_hiv > (.9 * mx_env_whiv), mx_avg_hiv := (.9 * mx_env_whiv)]
  
  final_nn[, mx_hiv_free := mx_env_whiv - mx_avg_hiv]
  final_nn <- convert_rate(final_nn, c(env_convert_vars, "mx_avg_hiv","mx_hiv_free"))
  
  lt_5to14[, mx_env_hiv := mx_env_whiv - mx_env_hiv_free]
  env_1to14 <- rbindlist(list(env_1to4, lt_5to14), use.names = T)
  final_1to14 <- merge(env_1to14, hiv_u15[age_group_id != 28,], by=c("sex_id","year","sim","age_group_id"))
  
  final_1to14[,mx_hiv_free := mx_env_whiv * hiv_free_ratio]
  final_1to14[,mx_avg_hiv := mx_env_whiv - mx_hiv_free]
  final_1to14[,hiv_free_ratio := NULL]
  
  env_u15 <- rbindlist(list(final_nn, final_1to14), use.names = T)
  
  env_u15 <- merge(env_u15, spec_draws, by=c("sex_id","year","age_group_id","sim")) # Bring in mx_spec_hiv
  env_u15[, non_hiv_deaths := NULL]
  
  return(env_u15)
} 


###############################################################################################################
## Merge results together, apply ensemble processes
lt_combined <- lt_combined[age != "0" & age != "1"]
lt_combined[,age:=NULL]
lt_combined <- rbind(lt_combined,env_nn,env_1to4)

lt_combined <- merge(lt_combined,pop,by=c("year","sex_id","age_group_id"))



if((group == "2A" | group == "2B") & !(loc_id %in% no_cod_location_ids )) {
  ## We only have ST-GPR draws from 1981 onwards, so we assume 0 HIV deaths beforehand
  lt_combined <- merge(lt_combined,spec_draws,by=c("sex_id","year","sim","age_group_id"),all.x=T)
  lt_combined[,non_hiv_deaths:=NULL] # If it exists, no need to use after the 
  lt_combined[is.na(mx_spec_hiv) & year < 1981,mx_spec_hiv:=0]
} else {
  lt_combined <- merge(lt_combined,spec_draws,by=c("sex_id","year","sim","age_group_id"), all.x = T)
}

lt_combined[,mx_hiv_free:=mx_env_hiv_free]

## If Group 1, average the HIV from Envelope and Spectrum
## For under-15 and over-5, use spectrum HIV instead of averaged
## For under-5 ages in Group 1, use envelope with-HIV scaled using Spectrum ratios to get HIV-free and rescaled Spectrum HIV
if(group %in% c("1A","1B")) {
  lt_combined[age_group_id > 7, mx_avg_hiv := (mx_spec_hiv + mx_env_hiv) / 2]
  # lt_combined[age_group_id > 5 & age_group_id <= 7, mx_avg_hiv := mx_spec_hiv]
  
  env_u15 <- create_u15_mx_group1()
  env_u15 <- merge(env_u15,pop,by=c("year","sex_id","age_group_id"))
  lt_combined[,non_hiv_deaths:=NULL]
  
  lt_combined <- rbindlist(list(lt_combined[age_group_id > 7], env_u15), use.names = T)
}

## Otherwise, consider HIV straight from Spectrum or ST-GPR
if(group %in% c("2A","2B","2C") | loc_id %in% no_cod_location_ids) lt_combined[, mx_avg_hiv := mx_spec_hiv]

## For all groups except Group 1A, we believe the all-cause and want to preserve it instead of HIV-free
## So we recalculate HIV-deleted based on the averaged HIV and the with-HIV
if(group %in% c("1B","2A","2B","2C") | loc_id %in% no_cod_location_ids) {
  # First, constrain HIV to at most be 90% of the all-cause total (some draws in USA_555 and BRB/BMU violate this)
  
  # dump_diagnostic_file(data=lt_combined[mx_avg_hiv > (.9 * mx_env_whiv), list(sex_id, year, sim, age_group_id, mx_avg_hiv, mx_env_whiv)], reckoning_version=new_upload_version, folder="01_run_ensemble", filename=paste0(country, "_mx_avg_hiv_capped_to_90.csv"))
  
  lt_combined[mx_avg_hiv > (.9 * mx_env_whiv), mx_avg_hiv := (.9 * mx_env_whiv)]
  lt_combined[,mx_hiv_free:=mx_env_whiv - mx_avg_hiv]
}

lt_combined[,mx_avg_whiv := mx_hiv_free + mx_avg_hiv]

###############################################################################################################
## Output all results
## Create convert_mx function to go from rate to number space
convert_mx <- function(x) return(x*lt_combined[['pop_gbd']])
c_vars <- colnames(lt_combined)[grepl("mx",colnames(lt_combined))]
lt_combined[,(c_vars) := lapply(.SD,convert_mx),.SDcols=c_vars] 

##Pull back split age groups deaths
u5_dt <-  assertable::import_files(filenames = paste0("with_hiv_env_", years, ".h5"),
                                   folder = input_env_dir,
                                   FUN = load_hdf,
                                   by_val = loc_id)
u5_dt = u5_dt[age_group_id==49, age_group_id := 238]
u5_dt <- u5_dt[year_id >= start_year & !sex_id == 3 & (age_group_id  %in% c(4,5,238,388,389,34))]

## Split to new age groups 
get_new_ages = function(old_ages,new_ages){
  
  limit = lt_combined[age_group_id %in% old_ages]
  
  all = u5_dt[age_group_id %in% old_ages]
  split = u5_dt[age_group_id %in% new_ages]
  split = merge(split ,all , by = c("location_id","year_id","sex_id","sim"))
  split[,prop := deaths.x/deaths.y]
  setnames(split,c("year_id","age_group_id.y"),c("year","age_group_id"))
  
  limit = merge(limit,split[,.(year,sex_id,age_group_id.x,prop,sim)],by=c("year","sex_id","sim"), allow.cartesian = TRUE)
  convert_back <- function(x) return(x*limit[['prop']])
  c_vars <- colnames(lt_combined)[grepl("mx",colnames(lt_combined))]
  
  limit[,(c_vars) := lapply(.SD,convert_back),.SDcols=c_vars] 
  limit[,age_group_id := age_group_id.x]
  limit[,c('age_group_id.x','pop_gbd','prop') := NULL]
  limit = merge(limit,pop,by=c("sex_id","year","age_group_id"))
  limit = unique(limit)
  
  return(limit)
}

u5 = get_new_ages(c(5),c(34,238))
u1 = get_new_ages(c(4),c(388,389) )


lt_combined <- lt_combined[!age_group_id %in% c(4,5)]
lt_combined <- rbind(lt_combined,u1,u5,use.names=TRUE)

## Add on loc_id
lt_combined[,location_id:=loc_id]
setnames(lt_combined,c("year","pop_gbd"),c("year_id","pop"))


## Check that no draws are under 0 or too high
lt_combined[mx_avg_whiv == 0, mx_avg_whiv := 0.01]
lt_combined[mx_hiv_free == 0, mx_hiv_free := 0.01]

assert_values(lt_combined, colnames(lt_combined)[grepl("mx", colnames(lt_combined))], "gte", 0)
assert_values(lt_combined, colnames(lt_combined), "not_na")

id_vars <- list(year_id = years, age_group_id = ages[!ages %in% c(28,4,5)], sex_id = c(1:2), sim  = compute_sims)
assert_ids(lt_combined, id_vars)

## Create age- and draw-specific scalars from HIV-free to with_HIV envelope
output <- lt_combined
output[, scalar_del_to_all := mx_avg_whiv / mx_hiv_free]
output <- output[, list(year_id, sex_id, age_group_id, sim, scalar_del_to_all)]

output[is.na(scalar_del_to_all), scalar_del_to_all := 1]
assert_values(output, colnames(output), "not_na")

## Output HIV-specific deaths
output <- lt_combined[,list(location_id,year_id,sex_id,age_group_id,sim,mx_avg_whiv,mx_hiv_free)]
output[,hiv_deaths:=mx_avg_whiv-mx_hiv_free]
output[,c("mx_avg_whiv","mx_hiv_free"):=NULL]
write_csv(output,paste0(out_dir_hiv,"/hiv_death_",country,".csv"))


## Output total envelope deaths pre-Reckoning (Envelope and Spectrum), and total averaged deaths
output <- lt_combined
output[,mx_avg_hiv:=mx_avg_whiv-mx_hiv_free]
# collapse_vars <- c("mx_avg_whiv","mx_env_whiv","mx_hiv_free","mx_env_whiv_free","mx_avg_hiv","mx_env_hiv","mx_spec_hiv")
collapse_vars <- c("mx_avg_whiv","mx_env_whiv","mx_avg_hiv","mx_spec_hiv","mx_env_hiv")
## Collapse to aggregate age groups for repoßrting
output[age_group_id %in% c(2,3,238,388,389,34), age_group_id:= 1] # Under-5
output[age_group_id %in% c(6:7), age_group_id:= 23] # 5-14
output[age_group_id <= 14 & age_group_id > 7, age_group_id:= 24] # 15-49
output[age_group_id %in% c(13:20, 30:32, 235), age_group_id:= 40] # 50+

output <- data.table(output)[,lapply(.SD,sum),.SDcols=collapse_vars,
                             by=c("sex_id","year_id","location_id","age_group_id","sim")]
setnames(output,c("mx_avg_whiv","mx_env_whiv","mx_env_hiv","mx_spec_hiv","mx_avg_hiv"),c("env_post","env_pre","hiv_pre_env","hiv_pre_oth","hiv_post"))
output <- melt(output,id.vars=c("sex_id","year_id","location_id","age_group_id","sim"),variable.name="measure_type",value.name="value")
write_csv(output,paste0(out_dir_hiv,"/reckon_reporting_",country,".csv"))