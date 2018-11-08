## Purpose: Create new Lifetables based off of post-ensemble envelope results


###############################################################################################################
## Set up settings
rm(list=ls())

library(readr); library(foreign); library(plyr); library(rhdf5); library(data.table); library(assertable); library(assertthat)

root <- "ROOT_FILEPATH"
user <- "USERNAME"
  
country <- commandArgs(trailingOnly = T)[1]
group <- paste0(commandArgs(trailingOnly = T)[2]) # Enforce it as string
spec_name <- commandArgs(trailingOnly = T)[3]
new_upload_version <- commandArgs(trailingOnly = T)[4]
mlt_lt_version <- commandArgs(trailingOnly = T)[5]
start_year <- as.integer(commandArgs(trailingOnly = T)[6])
gbd_year <- as.integer(commandArgs(trailingOnly = T)[7])


master_dir <- paste0("/FILEPATH/hiv_adjust/",new_upload_version)


## Inputs
input_dir <- paste0(master_dir, "/inputs")
spec_sim_dir <- paste0("/FILEPATH/spectrum_prepped/death_draws/", spec_name)
mlt_dir <- paste0("/FILEPATH/model_life_tables/", mlt_lt_version)
input_hiv_free_dir <- paste0(mlt_dir, "/lt_hiv_free/scaled")
input_with_hiv_dir <- paste0(mlt_dir, "/lt_with_hiv/scaled")
input_env_dir <- paste0(mlt_dir, "/env_with_hiv/scaled")
stgpr_dir <- "/FILEPATH/st_gpr"

## Outputs
whiv_dir <- paste0(master_dir,"/envelope_whiv")
hiv_free_dir <- paste0(master_dir,"/envelope_hivdel")
out_dir_hiv <- paste0(master_dir,"/hiv_output")

## Grab functions etc.
source(paste0("/FILEPATH/", user, "/hiv_adjust/reckoning_diagnostics.R"))
library(mortdb)
library(mortcore)
library(ltcore)

years <- c(start_year:gbd_year)
gbd_round <- gbd_year - 2012

locations <- fread(paste0(input_dir,"/locations_mort.csv"))
loc_id <- locations[ihme_loc_id==country, location_id]
loc_name <- locations[ihme_loc_id==country, location_name]
assert_that(!is.null(loc_id) && !is.na(loc_id))

age_map <- fread(paste0(input_dir,"/age_map_01.csv"))[, list(age_group_id, age_group_name_short)]
ages <- unique(age_map$age_group_id)
setnames(age_map,"age_group_name_short","age_group_name")
age_map[age_group_name == "95+", age_group_name := "95"]

## Use draw maps to scramble draws so that they are not correlated over time
## This is because Spectrum output is semi-ranked due to Ranked Draws into EPP
## Then, it propogates into here which would screw up downstream processes
## This is done here as opposed to raw Spectrum output because we want to preserve the draw-to-draw matching of Spectrum and lifetables, then scramble them after they've been merged together
draw_map <- fread(paste0(input_dir, "/draw_map.csv"))
draw_map <- draw_map[location_id==loc_id,list(old_draw,new_draw)]
setnames(draw_map,"old_draw","sim")

compute_sims <- c(0:999)
lt_ages <- c(0, 1, seq(5, 95, 5))
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

  data <- assertable::import_files(filenames = paste0(file_prefix, "_", years, ".h5"),
                                   folder = get(paste0("input_", lt_type, "_dir")),
                                   FUN = load_hdf,
                                   by_val = location_id)
  
  assert_values(data, colnames(data), test= "not_na")
  id_vars <- list(year = years, sim = compute_sims, sex = c("male", "female", "both"), age = c(0, 1, seq(5, 110, 5)))
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

dump_diagnostic_file(data=lt_combined[mx_env_hiv < 0, ], reckoning_version=new_upload_version, folder="01_run_ensemble", filename=paste0(country, "_mx_env_hiv_lt0.csv"))

lt_combined[mx_env_hiv < 0 & age == 95, mx_env_hiv := 0] # For now, do it if there's any magnitude of difference at all
assert_values(lt_combined, "mx_env_hiv", "gte", 0)
lt_combined[, age := as.character(age)]
lt_combined <- merge(lt_combined, age_map, by.x = "age", by.y = "age_group_name")
lt_combined[, age := as.numeric(age)]

assert_ids(lt_combined, id_vars = lt_id_vars)

lt_u1 <- lt_combined[age==0, list(sex_id, year,sim, mx_env_hiv)]
lt_1to4 <- lt_combined[age==1, list(sex_id,year, sim, mx_env_whiv, mx_env_hiv_free)]
lt_95plus <- lt_combined[age==95, list(sex_id, year, sim, mx_env_whiv, mx_env_hiv_free)] 


# With-HIV Envelope (to get NN breakdowns of envelope HIV and 1-4 breakdowns of envelope HIV, and to replace with-HIV over-95 with envelope over-95)
env <- assertable::import_files(filenames = paste0("with_hiv_env_", years, ".h5"),
                                folder = input_env_dir,
                                FUN = load_hdf,
                                by_val = loc_id)


assert_values(env[year_id >= 1950], colnames = colnames(env), test="not_na")

env <- env[year_id >= start_year & !sex_id == 3 & (age_group_id == 235 | age_group_id <=5),]
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
env_1to4 <- env[age_group_id == 5,]
env_nn <- env[age_group_id %in% c(2,3,4),]


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

dump_diagnostic_file(data=lt_95plus[mx_env_hiv < 0,], reckoning_version=new_upload_version, folder="01_run_ensemble", paste0(country, "_mx_env_hiv_lt0_95p.csv"))

lt_95plus[mx_env_hiv < 0, mx_env_hiv := 0] # For now, do it if there's any magnitude of difference at all

lt_95plus[, c("scalar","env_mx") := NULL]
lt_95plus[, age := "95"]

lt_combined <- lt_combined[age_group_id != 235,]
lt_combined <- rbindlist(list(lt_combined,lt_95plus),use.names=T)


## Spectrum Results (if Group 1 [GEN], Group 2B [CON incomplete VR], or Group 2C [CON no data])
## Note that non-HIV deaths are only used for Group 1 and aren't accurate for ENN/LNN/PNN (they represent under-1 deaths, need to be split by envelope)
if(group %in% c("1A","1B","2B","2C")) {
  spec_draws <- data.table(assertable::import_files(paste0(country, "_ART_deaths.csv"), folder=spec_sim_dir))
  setnames(spec_draws,c("year_id", "run_num", "hiv_deaths"), c("year", "sim", "mx_spec_hiv"))
  spec_draws[, sim := sim - 1] # Format sims to be in the same number-space
  spec_draws[mx_spec_hiv == 0 & non_hiv_deaths == 0, non_hiv_deaths_prop := 1]
  
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
if(group=="2A") {
  spec_draws <- import_files("gpr_results.csv", folder=stgpr_dir)
  spec_placeholder <- import_files("gpr_results_2017_placeholder.csv", folder=stgpr_dir)
  
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
  
  spec_draws <- spec_draws[,list(year_id,age_group_id,sex_id,sim,mx_spec_hiv)]
  setnames(spec_draws,"year_id","year")
}

assert_values(spec_draws, colnames(spec_draws), "not_na")
assert_values(spec_draws, "mx_spec_hiv", "gte", 0)

id_vars <- list(year = unique(spec_draws$year), sim = compute_sims, age_group_id = ages[ages != 28], sex_id = c(1:2))
assert_ids(spec_draws, id_vars)

###############################################################################################################
## Split NN and under-1 envelope/LT into with-HIV and HIV-free proportional to the populations in those age groups 
env_nn <- merge(env_nn,lt_u1,by=c("sex_id","year","sim"))
env_nn[age_group_id %in% c(2,3),mx_env_hiv:=0] # We assume no HIV deaths to ENN and LNN age groups

# Because the PNN envelope with HIV is not necessarily higher than all HIV under-1 deaths due to different estimation processes,
# We need to constrain under-1 HIV to 90% of the PNN hiv-deleted envelope
dump_diagnostic_file(data=env_nn[mx_env_hiv > (.9 * mx_env_whiv),], reckoning_version=new_upload_version,  folder="01_run_ensemble", filename=paste0(country, "_env_nn_mx_env_hiv_capped.csv"))
env_nn[mx_env_hiv > (.9 * mx_env_whiv), mx_env_hiv:= .9 * mx_env_whiv]
env_nn[,mx_env_hiv_free := mx_env_whiv - mx_env_hiv]


###############################################################################################################
## Use the all-cause 1-4 envelope as the standard, because LT mx doesn't equal envelope/pop (something to do with u5 and with LT recalculation based on qx)
## We will take the ratio of HIV-free to with-HIV from the LT results, then use all-cause mortality from envelope times the ratio to get HIV-free mx
setnames(env_1to4,"mx_env_whiv","all_cause_envelope")
env_1to4 <- merge(env_1to4,lt_1to4,by=c("sex_id","year","sim"))
env_1to4[,scalar:=mx_env_hiv_free/mx_env_whiv]
env_1to4[,mx_env_hiv_free:=all_cause_envelope * scalar]

env_1to4[,c("mx_env_whiv","scalar"):=NULL]
setnames(env_1to4,"all_cause_envelope","mx_env_whiv")

id_vars <- list(year = years, sex_id = c(1:2), age_group_id = 5, sim = compute_sims)
assert_ids(env_1to4, id_vars)

env_1to4[,mx_env_hiv:=mx_env_whiv - mx_env_hiv_free]

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

get_u5_hiv <- function(data) {
  data <- data[age_group_id <= 5,]
  data <- convert_num(data,c("mx_spec_hiv", "non_hiv_deaths"))
  
  dump_diagnostic_file(data=data[non_hiv_deaths == 0], reckoning_version=new_upload_version, folder="01_run_ensemble", filename=paste0(country, "_non_hiv_deaths_zero.csv"))
  data[non_hiv_deaths == 0 ,non_hiv_deaths:=1] # For gap-filled results from start_year-198?
  
  ## Collapse NN granular to under-1 
  data[age_group_id <=4, age_group_id := 28]
  data <- data.table(data)[,lapply(.SD,sum),.SDcols=c("mx_spec_hiv","non_hiv_deaths"),
                           by=c("age_group_id","sex_id","year","sim")] 
  
  ## Get ratio of HIV-free to with-HIV (doesn't matter that it's in number vs. rate space since denominator is the same)
  data[,hiv_free_ratio := non_hiv_deaths/(mx_spec_hiv + non_hiv_deaths)]
  data <- data[,list(age_group_id,sex_id,year,sim,hiv_free_ratio)]
  return(data)
}

get_u1_env <- function(data) {
  data <- convert_num(data,c("mx_env_whiv"))
  data <- data.table(data)[,lapply(.SD,sum),.SDcols=c("mx_env_whiv"),
                           by=c("sex_id","year","sim")] 
  return(data)
}

create_u5_mx_group1 <- function() {
  ## We want to use the ratio of Spectrum HIV-deleted to with-HIV to convert lifetable with-HIV to lifetable HIV-free
  ## data is with-HIV dataset from get_u15_lt, already under-1 non-granular
  ## We want to take in the spectrum NN results, collapse to under-1, merge on with the u5 HIV, calculate the 
  
  env_convert_vars <- c("mx_env_whiv","mx_env_hiv","mx_env_hiv_free")
  
  hiv_u5 <- get_u5_hiv(spec_draws)
  
  ## Calculate post-neonatal deaths
  env_u1 <- get_u1_env(env_nn)
  pnn_calc <- merge(env_u1,hiv_u5[age_group_id==28,],by=c("sex_id","year","sim"))
  pnn_calc[,mx_avg_hiv:= mx_env_whiv - (mx_env_whiv * hiv_free_ratio)]
  pnn_calc[,age_group_id:=4]
  pnn_calc <- pnn_calc[,list(age_group_id,sex_id,year,sim,mx_avg_hiv)]
  
  ## Merge back on PNN deaths
  final_nn <- convert_num(env_nn, env_convert_vars)
  final_nn <- merge(final_nn,pnn_calc,all.x=T,by=c("age_group_id","sex_id","year","sim"))
  final_nn[age_group_id %in% c(2,3),mx_avg_hiv := 0] # Still no deaths in enn/lnn
  
  # First, constrain HIV to at most be 90% of the all-cause total (some draws in ZAF violate this in PNN due to the ratio being applied at U-1 level, not PNN)
  dump_diagnostic_file(data=final_nn[mx_avg_hiv > (.9 * mx_env_whiv)], reckoning_version=new_upload_version,  folder="01_run_ensemble", filename=paste0(country, "_final_nn_mx_avg_capped.csv"))
  final_nn[mx_avg_hiv > (.9 * mx_env_whiv), mx_avg_hiv := (.9 * mx_env_whiv)]
  
  final_nn[,mx_hiv_free:=mx_env_whiv - mx_avg_hiv]
  final_nn <- convert_rate(final_nn, c(env_convert_vars, "mx_avg_hiv","mx_hiv_free"))
  
  final_1to4 <- merge(env_1to4,hiv_u5[age_group_id != 28,],by=c("sex_id","year","sim","age_group_id"))
  
  final_1to4[,mx_hiv_free := mx_env_whiv * hiv_free_ratio]
  final_1to4[,mx_avg_hiv := mx_env_whiv - mx_hiv_free]
  final_1to4[,hiv_free_ratio := NULL]
  
  env_u5 <- rbind(final_nn,final_1to4)
  env_u5 <- merge(env_u5,spec_draws,by=c("sex_id","year","age_group_id","sim")) # Bring in mx_spec_hiv
  env_u5[,non_hiv_deaths := NULL]
  
  return(env_u5)
} 


###############################################################################################################
## Merge results together, apply ensemble processes
lt_combined <- lt_combined[age != "0" & age != "1"]
lt_combined[,age:=NULL]
lt_combined <- rbind(lt_combined,env_nn,env_1to4)

lt_combined <- merge(lt_combined,pop,by=c("year","sex_id","age_group_id"))

if(group == "2A") {
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
  lt_combined[age_group_id > 5 & age_group_id <= 7, mx_avg_hiv := mx_spec_hiv]
  
  env_u5 <- create_u5_mx_group1()
  env_u5 <- merge(env_u5,pop,by=c("year","sex_id","age_group_id"))
  lt_combined[,non_hiv_deaths:=NULL]
  
  lt_combined <- rbind(lt_combined[age_group_id > 5],env_u5)
}

## Otherwise, consider HIV straight from Spectrum or ST-GPR
if(group %in% c("2A","2B","2C")) lt_combined[,mx_avg_hiv:=mx_spec_hiv]

## For all groups except Group 1A, we believe the all-cause and want to preserve it instead of HIV-free
## So we recalculate HIV-deleted based on the averaged HIV and the with-HIV
if(group %in% c("1B","2A","2B","2C")) {
  # First, constrain HIV to at most be 90% of the all-cause total (some draws in USA_555 and BRB/BMU violate this)
  
  dump_diagnostic_file(data=lt_combined[mx_avg_hiv > (.9 * mx_env_whiv), list(sex_id, year, sim, age_group_id, mx_avg_hiv, mx_env_whiv)], reckoning_version=new_upload_version, folder="01_run_ensemble", filename=paste0(country, "_mx_avg_hiv_capped_to_90.csv"))
  
  lt_combined[mx_avg_hiv > (.9 * mx_env_whiv), mx_avg_hiv := (.9 * mx_env_whiv)]
  lt_combined[,mx_hiv_free:=mx_env_whiv - mx_avg_hiv]
}

## Now, the with-HIV envelope is the sum of the HIV-free envelope and HIV (either averaged or direct from Spectrum/ST-GPR)
## Or it is with-HIV envelope minus HIV -- because we modify the mx_hiv_free variable in those cases above, it should all compute appropriately.
lt_combined[,mx_avg_whiv := mx_hiv_free + mx_avg_hiv]

###############################################################################################################
## Output all results
## Create convert_mx function to go from rate to number space
convert_mx <- function(x) return(x*lt_combined[['pop_gbd']])
c_vars <- colnames(lt_combined)[grepl("mx",colnames(lt_combined))]
lt_combined[,(c_vars) := lapply(.SD,convert_mx),.SDcols=c_vars] 

## Add on loc_id
lt_combined[,location_id:=loc_id]
setnames(lt_combined,c("year","pop_gbd"),c("year_id","pop"))

## Rescramble draws
lt_combined <- merge(lt_combined,draw_map,by=c("sim"))
lt_combined[,sim:=new_draw]
lt_combined[,new_draw:=NULL]

## Check that no draws are under 0 or too high
lt_combined[mx_avg_whiv == 0, mx_avg_whiv := 0.01]
lt_combined[mx_hiv_free == 0, mx_hiv_free := 0.01]

assert_values(lt_combined[year_id >= 1954], colnames(lt_combined)[grepl("mx", colnames(lt_combined))], "gte", 0)
assert_values(lt_combined[year_id >= 1954], colnames(lt_combined), "not_na")
id_vars <- list(year_id = years, age_group_id = ages[ages != 28], sex_id = c(1:2), sim  = compute_sims)
assert_ids(lt_combined, id_vars)

## Create age- and draw-specific scalars from HIV-free to with_HIV envelope
output <- lt_combined
output[, scalar_del_to_all := mx_avg_whiv / mx_hiv_free]
output <- output[, list(year_id, sex_id, age_group_id, sim, scalar_del_to_all)]

dump_diagnostic_file(data=output[is.na(scalar_del_to_all), ], reckoning_version=new_upload_version, folder="01_run_ensemble",filename=paste0(country, "_NA_scalar_del_to_all.csv"))

output[is.na(scalar_del_to_all), scalar_del_to_all := 1]
assert_values(output, colnames(output), "not_na")

write.dta(output,paste0(whiv_dir,"/scalars_",country,".dta"))

## Save envelopes
save_envelope <- function(data, hiv_type) {
  # hiv_type: "whiv" or "hiv_free"
  if(hiv_type == "whiv") env_varname <- "mx_avg_whiv"
  if(hiv_type == "hiv_free") env_varname <- "mx_hiv_free"
  
  data <- data[, .SD, .SDcols = c("location_id","year_id","sex_id","age_group_id","sim","pop",env_varname)]
  
  filepath <- paste0(get(paste0(hiv_type,"_dir")), 
                     "/env_", country, ".h5")
  file.remove(filepath)
  h5createFile(filepath)
  
  # Save HDF file grouped by year
  lapply(years, save_hdf, data=data, filepath=filepath,by_var="year_id")
  H5close()
}

save_envelope(lt_combined, "whiv")
save_envelope(lt_combined, "hiv_free")

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
## Collapse to aggregate age groups for reporting
output[age_group_id <= 5, age_group_id:= 1] # Under-5
output[age_group_id <= 7 & age_group_id > 5, age_group_id:= 23] # 5-14
output[age_group_id <= 14 & age_group_id > 7, age_group_id:= 24] # 15-49
output[age_group_id %in% c(13:20, 30:32, 235), age_group_id:= 40] # 50+

output <- data.table(output)[,lapply(.SD,sum),.SDcols=collapse_vars,
                             by=c("sex_id","year_id","location_id","age_group_id","sim")]
setnames(output,c("mx_avg_whiv","mx_env_whiv","mx_env_hiv","mx_spec_hiv","mx_avg_hiv"),c("env_post","env_pre","hiv_pre_env","hiv_pre_oth","hiv_post"))
output <- melt(output,id.vars=c("sex_id","year_id","location_id","age_group_id","sim"),variable.name="measure_type",value.name="value")
write_csv(output,paste0(out_dir_hiv,"/reckon_reporting_",country,".csv"))
