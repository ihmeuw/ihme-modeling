###############################################################################################################
## Create rescaled mx values for all stages of the mortality pipeline (at lowest location level)
###############################################################################################################
## Set up settings

  rm(list=ls())
  options(digits = 13)
  user <- Sys.getenv("USER")

## Source libraries and functions
  library(pacman)
  pacman::p_load(readr, data.table, assertable, haven, tidyr, parallel, rhdf5, argparse)
  library(mortdb, lib.loc = FILEPATH)
  library(mortcore, lib.loc = FILEPATH)
  library(ltcore, lib.loc = FILEPATH)

  parser <- ArgumentParser()
  parser$add_argument('--shock_death_number_estimate_version', type="integer", required=TRUE,
                      help='The with shock death number estimate version for this run')
  parser$add_argument('--no_shock_death_number_estimate_version', type="integer", required=TRUE,
                      help='The no shock death number estimate version for this run')
  parser$add_argument('--mlt_life_table_version', type="integer", required=TRUE,
                      help='MLT life table estimate version')
  parser$add_argument('--gbd_year', type="integer", required=TRUE,
                      help='GBD Year')
  parser$add_argument('--loc', type="character", required=TRUE,
                      help='Location to run')
  parser$add_argument('--u5_envelope_version', type='character', required=FALSE, default = 1,
                      help = "For terminator finalizer runs, specify the U5 version")
  parser$add_argument('--age_sex_version', type='integer', required=TRUE)

  args <- parser$parse_args()
  shocks_addition_run_id <- args$shock_death_number_estimate_version
  reckoning_run_id <- args$no_shock_death_number_estimate_version
  mlt_run_id <- args$mlt_life_table_version
  gbd_year <- args$gbd_year
  current_ihme <- args$loc
  u5_version <- args$u5_envelope_version
  age_sex_version <- args$age_sex_version


## Look up parallelization metadata
  loc_map <- fread(FILEPATH)
  current_location_id <- loc_map[ihme_loc_id == current_ihme, location_id]
  print(current_ihme)

## Check if current_ihme is a lowest location or not. This informs where we read in
## lt draws and HIV adjustment from.

  lowest_map <- fread(FILEPATH)
  is_lowest <- (current_location_id %in% lowest_map$location_id)

## Set general ID variables
  lt_ids <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")
  draw_ids <- c("location_id", "year_id", "sex_id", "age", "draw")
  lt_func_id_vars <- c(draw_ids[draw_ids != "age"], "age")

## Age file
  age_map <- fread(FILEPATH)
  age_map <- age_map[, list(age_group_id, age_group_years_start)]
  setnames(age_map, "age_group_years_start", "age")

###############################################################################################################
## Import map files, assign ID values

## Population file
  population <- fread(FILEPATH)

## Create ID combinations to use with assert_ids
  sexes <- c("male", "female")
  years <- c(1950:gbd_year)
  sims <- c(0:999)
  ages <- unique(age_map$age)

  env_ages <- c(1:20, 30:32, 235)

  set.seed(current_location_id)
  shuffled_years <- sample(years, replace = FALSE)
  if(!identical(sort(shuffled_years), sort(years))) stop("Year resample failed")

## Child and adult sims
  sim_ids <- list(ihme_loc_id = current_ihme,
                  year = c(1950:gbd_year),
                  sex = c("male", "female"),
                  simulation = c(0:999))

  lt_sim_ids <- list(ihme_loc_id = current_ihme,
                     year = c(1950:gbd_year),
                     sex = c("male", "female"),
                     sim = c(0:999),
                     age = c(0, 1, seq(5, 110, 5)))


###############################################################################################################
## Import reckoning and shocks addition results, and generate mx scalar values

if (is_lowest) { # for lowest locations, read these files in from the FLT directory.
  lt_with_shock <- fread(FILEPATH)
  lt_no_shock <- fread(FILEPATH)
  lt_no_hiv <- fread(FILEPATH)
  hiv_adjust <- fread(FILEPATH)
} else { # otherwise, the aggregated hiv and collapsed abridged lts are read in from the finalizer directory
  flt_dir <-FILEPATH
  lt_with_shock <- fread(FILEPATH)
  lt_no_shock <- fread(FILEPATH)
  lt_no_hiv <- fread(FILEPATH)
  hiv_adjust <- fread(FILEPATH)
}
if ((nrow(lt_with_shock[mx==Inf|qx>1])+nrow(lt_no_shock[mx==Inf|qx>1])+nrow(lt_no_hiv[mx==Inf|qx>1])) > 0) {
  stop("Found rows where mx is Inf or qx > 1.")
}

setnames(lt_with_shock, c("mx", "ax"), c("mx_with_shock", "ax_with_shock"))
setnames(lt_no_shock, c("mx", "ax"), c("mx_no_shock", "ax_no_shock"))
setnames(lt_no_hiv, c("mx", "ax"), c("mx_no_hiv", "ax_no_hiv"))

final_abridged_lts <- merge(lt_with_shock[, .SD, .SDcols = c(draw_ids, "mx_with_shock", "ax_with_shock")],
                 lt_no_shock[, .SD, .SDcols = c(draw_ids, "mx_no_shock", "ax_no_shock")],
                 by = draw_ids)

final_abridged_lts <- merge(final_abridged_lts,
                            lt_no_hiv[, .SD, .SDcols = c(draw_ids, "mx_no_hiv", "ax_no_hiv")],
                            by = draw_ids)

final_abridged_lts <- merge(final_abridged_lts, age_map[, .(age, age_group_id)], by = "age")
final_abridged_lts[, age := NULL]


final_abridged_lts[mx_no_shock < mx_no_hiv, mx_no_hiv := mx_no_shock]

if (is_lowest) {
  shock_deaths <- fread(FILEPATH)
} else {
  shock_deaths <- fread(FILEPATH)
}
setnames(shock_deaths, "deaths", "shock_specific_deaths")

###############################################################################################################
## Directly import and use U-5 Envelope values directly from number-space calculations

format_lt_mx_95_plus <- function(lt, id_vars) {
  lt <- copy(lt)
  setnames(lt, colnames(lt)[grepl("ax", colnames(lt))], "ax")
  setnames(lt, colnames(lt)[grepl("mx", colnames(lt))], "mx")
  setkeyv(lt, id_vars)
  qx_to_lx(lt)
  lx_to_dx(lt)
  gen_nLx(lt)
  gen_Tx(lt, id_vars = id_vars)

  ## 95+ mx is approximately lx/Tx at the 95-99 age group
  lt[, mx := lx / Tx]
  return(lt[age == 95, .SD, .SDcols = c(id_vars, "mx")])
}

## Generate 95+ results for HIV so that we can generate HIV-specific deaths in the 95+ age group and merge it onto the u5_over_95 dt
abridged_95_plus_no_shock <- format_lt_mx_95_plus(lt_no_shock, lt_func_id_vars)
abridged_95_plus_no_hiv <- format_lt_mx_95_plus(lt_no_hiv, lt_func_id_vars)
abridged_95_plus <- merge(abridged_95_plus_no_shock, abridged_95_plus_no_hiv, by = draw_ids)
abridged_95_plus[, age_group_id := 235]
abridged_95_plus[, age := NULL]
setnames(abridged_95_plus, c("mx.x", "mx.y"), c("mx_no_shock", "mx_no_hiv"))
abridged_95_plus[mx_no_hiv > mx_no_shock, mx_no_hiv := mx_no_shock]

abridged_shock <- final_abridged_lts[age_group_id %in% c(env_ages, 28), .SD, .SDcols = c(lt_ids, "mx_no_shock", "mx_no_hiv")]
abridged_shock <- rbindlist(list(abridged_shock, abridged_95_plus), use.names = T)
abridged_shock[, mx_hiv_specific := mx_no_shock - mx_no_hiv]
abridged_shock[mx_hiv_specific < 0, mx_hiv_specific := 0]
abridged_shock[, hiv_free_ratio := (mx_no_hiv / mx_no_shock)]
abridged_shock <- merge(abridged_shock, population, by = lt_ids[lt_ids != "draw"])
abridged_shock[, deaths_no_shock := mx_no_shock * population]
abridged_shock[, deaths_no_hiv := mx_no_hiv * population]
abridged_shock[, deaths_hiv_specific := mx_hiv_specific * population]
abridged_shock <- merge(abridged_shock, shock_deaths, by = lt_ids, all.x = T)
abridged_shock[, deaths_with_shock := shock_specific_deaths + deaths_no_shock]
abridged_shock[, shock_specific_deaths := NULL]


if(u5_version != "1") {
  u5_dt <- fread(FILEPATH)

  # Reshape

  # Drop person years cols
  pys <- c(grep("pys", names(u5_dt), value=T), 'deathspna', 'deathspnb')
  u5_dt[, c(pys) := NULL]

  # Reshape long
  id_cols <- c('ihme_loc_id', 'year', 'sim', 'sex')
  u5_dt <- melt(u5_dt, id.vars=id_cols, variable.name='age_group_name', value.name='deaths')
  u5_dt[, age_group_name := gsub("deaths", "", age_group_name)]

  # Aggregate 1,2,3,4 to 1-4
  deaths_1to4 <- u5_dt[age_group_name %in% c("1", "2", "3", "4"), .(deaths=sum(deaths), age_group_id = 5), by=id_cols]

  # Manually attach age group ids
  u5_dt <- u5_dt[age_group_name %in% c("enn", "lnn", "pnn"), ]
  u5_dt[age_group_name == "enn", age_group_id := 2]
  u5_dt[age_group_name == "lnn", age_group_id := 3]
  u5_dt[age_group_name == "pnn", age_group_id := 4]
  u5_dt[, age_group_name := NULL]

  # Bind 1-4 and <1 dts
  u5_dt <- rbind(u5_dt, deaths_1to4, use.names=T)

  # Map on location id
  u5_dt[, location_id := current_location_id]
  u5_dt[, ihme_loc_id := NULL]

  # Map sex ids
  u5_dt[, sex_id := ifelse(sex=='male', 1, ifelse(sex=='female', 2, 3))]
  u5_dt <- u5_dt[sex_id != 3]
  u5_dt[, sex := NULL]

  # set names
  setnames(u5_dt, c("sim", "deaths", "year"), c("draw", "deaths_no_shock", "year_id"))

  #  Rescale subnational draws to national draws using scalar map
  if ((nchar(current_ihme) > 3) & (current_ihme != "CHN_44533")) {

    HF_loc_map <- fread(FILEPATH)

    parent_location_id <- HF_loc_map[ihme_loc_id == substring(current_ihme, 1, 3), location_id]
    if (parent_location_id == 6) {
      parent_location_id <- 44533
    }

    scalar_map_dir <- FILEPATH
    nat_scalar_map_file <- FILEPATH

    nat_scalar_map <- fread(paste0(scalar_map_dir, nat_scalar_map_file))

    HF_merge_cols <- c("year_id", "draw", "age_group_id", "sex_id")
    old_u5_dt_names <- names(u5_dt)
    u5_dt <- merge(u5_dt, nat_scalar_map[, -c("location_id")], by = HF_merge_cols, all.x = TRUE)

    u5_dt[, deaths_no_shock := deaths_no_shock * scalar]
    u5_dt[, scalar := NULL]

    if (all(names(u5_dt) != old_u5_dt_names)) {
      stop("Not all marged columns were removed from u5_dt after applying scalars.")
    }

    rm(HF_loc_map, nat_scalar_map, scalar_map_dir, nat_scalar_map_file, HF_merge_cols, old_u5_dt_names, parent_location_id)

  }

  # Rescale the U5 envelope here for usa
  if (current_ihme %like% "USA") {
    usa_scalars <- fread(FILEPATH)
    u5_pre18 <- u5_dt[year_id < 2018]
    u5_dt <- u5_dt[year_id >= 2018]
    u5_dt <- merge(u5_dt, usa_scalars, by=c('year_id', 'sex_id', 'age_group_id'))
    u5_dt[, deaths := deaths_no_shock*scaling_factor]
    u5_dt <- u5_dt[, .SD, .SDcols=names(u5_pre18)]
    u5_dt <- rbind(u5_dt, u5_pre18, use.names=T)
  }


  # Bind back into main dataframe
  u5_over_95 <- copy(u5_dt)

  } else {
  env_filenames <- FILEPATH
  u5_over_95 <- assertable::import_files(env_filenames,
                                          folder = FILEPATH,
                                          FUN = load_hdf,
                                          by_val = current_location_id)

  setnames(u5_over_95, c("sim", "deaths"), c("draw", "deaths_no_shock"))
  u5_over_95 <- u5_over_95[age_group_id %in% c(2:5, 235) & sex_id != 3]

}



# Bring in age sex results as final nqx, use those for calculating nmx and ax ----------------------------------------------------

age_sex <- fread(FILEPATH)

# formatting
age_sex[, year_id := as.integer(floor(year))]
age_sex[, location_id := current_location_id]
age_sex <- age_sex[sex != 'both']
age_sex[, sex_id := ifelse(sex=='male', 1, 2)]
age_sex <- age_sex[, .(location_id, year_id, sex_id, draw=simulation, enn=q_enn, lnn=q_lnn, pnn=q_pnn, ch = q_ch)]

# make long
age_sex <- melt(age_sex, id.vars = c('location_id', 'year_id', 'sex_id', 'draw'), variable.name = 'age_group', value.name = 'qx_no_shock')

as_agemap <- data.table(
  age_group = c('enn', 'lnn', 'pnn', 'ch'),
  age_group_id = 2:5,
  age_length = c(7/365, 21/365, 337/365, 4)
)
age_sex <- merge(age_sex, as_agemap)


# find nmx no shock
age_sex[, mx_no_shock := qx_to_mx(q=qx_no_shock, t = age_length)]

# find shock specific mx for u5
ss_mx <- shock_deaths[age_group_id %in% 2:5]
ss_mx <- merge(ss_mx, population, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
ss_mx[, shock_specific_mx := shock_specific_deaths / population]

# find hiv specific mx
u5_hiv_mx <- merge(abridged_shock[age_group_id %in% c(5,28), .SD, .SDcols = c(lt_ids, "deaths_hiv_specific", "hiv_free_ratio")],
                   population,
                   by=setdiff(lt_ids, 'draw'))

u5_hiv_mx[age_group_id==28, age_group_id := 4]
u5_hiv_mx[, hiv_specific_mx := deaths_hiv_specific/population]


# combine all together
u5_mx <- merge(age_sex, ss_mx[, .SD, .SDcols=c(lt_ids, 'shock_specific_mx')],
               by=lt_ids)
u5_mx <- merge(u5_mx, u5_hiv_mx[, .SD, .SDcols=c(lt_ids, 'hiv_specific_mx', 'hiv_free_ratio')],
               by=lt_ids, all.x=T)
u5_mx[is.na(hiv_specific_mx), hiv_specific_mx := 0] # assuming no hiv for enn/lnn

# find shock/hiv-specific mx and ax
u5_mx[, mx_with_shock := mx_no_shock + shock_specific_mx]
u5_mx[mx_no_shock < hiv_specific_mx, hiv_specific_mx := mx_no_shock] # cap hiv specific mx
u5_mx[age_group_id != 5, mx_no_hiv := mx_no_shock - hiv_specific_mx] # same logic as regular u5 - use ratio for age gorup 5
u5_mx[age_group_id==5, mx_no_hiv := mx_no_shock * hiv_free_ratio]
assertable::assert_values(u5_mx, 'mx_no_hiv', test='gte', test_val = 0) # ensure no negative mx from subtraction


# calculate ax values
u5_mx[, ax_no_shock := mx_to_ax(mx_no_shock, age_length)]
u5_mx[, ax_with_shock := mx_to_ax(mx_with_shock, age_length)]
u5_mx[, ax_no_hiv := mx_to_ax(mx_no_hiv, age_length)]

# trim cols
u5_mx <- u5_mx[, .SD, .SDcols=c(lt_ids, grep("^(mx|ax)", names(u5_mx), value=T))]

# U5_mx here should be our final mx/ax for the under 5 age groups. -----------------------------------------------------------------------------------------


u5_over_95[age_group_id == 4, age_group_id := 28]
u5_over_95 <- merge(u5_over_95, abridged_shock[, .SD, .SDcols = c(lt_ids, "deaths_hiv_specific", "hiv_free_ratio")], by = lt_ids, all.x = T)
u5_over_95[age_group_id == 28, age_group_id := 4]
u5_over_95[age_group_id %in% c(2:3), deaths_hiv_specific := 0]
u5_over_95[age_group_id != 5, deaths_no_hiv := deaths_no_shock - deaths_hiv_specific]

under0 <- u5_over_95[age_group_id != 5 & deaths_no_hiv < 0]
if(nrow(under0) > 0){
  u5_over_95[age_group_id != 5, deaths_hiv_specific := 0.1 * deaths_no_shock]
  u5_over_95[age_group_id != 5, deaths_no_hiv := deaths_no_shock - deaths_hiv_specific]
}
u5_over_95[age_group_id == 5, deaths_no_hiv := deaths_no_shock * hiv_free_ratio]
assert_values(u5_over_95, "deaths_no_hiv", "gte", 0)
u5_over_95[, (c("deaths_hiv_specific", "hiv_free_ratio")) := NULL]

env_merged <- merge(u5_over_95, shock_deaths, by = lt_ids)
env_merged[, deaths_with_shock := shock_specific_deaths + deaths_no_shock]
env_merged[, shock_specific_deaths := NULL]

# bring in u5_mx here
final_abridged_lts <- rbindlist(list(final_abridged_lts[age_group_id != 5], u5_mx), use.names = T, fill = T)

## Combine the abridged + shock results along with the U5 env shock results and the 95+ shock results
if(u5_version != "1") {
  final_env <- rbindlist(list(abridged_shock[!age_group_id %in% c(2:5, 28), .SD, .SDcols = c(lt_ids, "deaths_no_hiv", "deaths_no_shock", "deaths_with_shock")],
                            env_merged[, .SD, .SDcols = c(lt_ids, "deaths_no_hiv", "deaths_no_shock", "deaths_with_shock")]),
                            use.names = T)
} else {
  final_env <- rbindlist(list(abridged_shock[!age_group_id %in% c(2:5, 28, 235), .SD, .SDcols = c(lt_ids, "deaths_no_hiv", "deaths_no_shock", "deaths_with_shock")],
                              env_merged[, .SD, .SDcols = c(lt_ids, "deaths_no_hiv", "deaths_no_shock", "deaths_with_shock")]),
                         use.names = T)
}


###############################################################################################################
## Run assertions

final_abridged_lts[, rel_diff := (mx_no_shock - mx_no_hiv) / mx_no_hiv]
final_abridged_lts[abs(rel_diff) < .000000001, mx_no_shock := mx_no_hiv + .000000000001]
final_abridged_lts[, rel_diff := NULL]
final_abridged_lts[abs(mx_with_shock - mx_no_shock) < .000000001, mx_with_shock := mx_no_shock + .00000000001]


assert_values(final_abridged_lts, "mx_with_shock", "gte", final_abridged_lts$mx_no_shock, warn_only = T)
assert_values(final_abridged_lts, "mx_no_shock", "gte", final_abridged_lts$mx_no_hiv)
assert_values(final_abridged_lts, c("mx_with_shock", "mx_no_shock", "mx_no_hiv"), "gte", 0)
if(nrow(final_abridged_lts[mx_with_shock==Inf|mx_no_shock==Inf|mx_no_hiv==Inf]) > 0) stop("Found rows where mx is Inf.")

###############################################################################################################
## Output files

## Save final with-shock envelope results for envelope substitution

# Check for duplicates
dups <- final_env[, if (.N > 1) .SD, by=lt_ids]
if (nrow(dups) > 0) stop("Duplicates in final envelope by age, location, year, sex, draw")
rm(dups)
filepath <- FILEPATH
if(file.exists(filepath)) file.remove(filepath)
rhdf5::h5createFile(filepath)
invisible(lapply(years, save_hdf, data= final_env, filepath=filepath, by_var="year_id"))
rhdf5::H5close()

## Save mx and ax for all ages
dups <- final_abridged_lts[, if (.N > 1) .SD, by=lt_ids]
if (nrow(dups) > 0) stop("Duplicates in final abridged life table by age, location, year, sex, draw")
rm(dups)
filepath <-FILEPATH
if(file.exists(filepath)) file.remove(filepath)
rhdf5::h5createFile(filepath)
invisible(lapply(years, save_hdf, data= final_abridged_lts, filepath=filepath, by_var="year_id", level=2))
rhdf5::H5close()

## Save HIV adjustment metrics
final_env_collapsed <- copy(final_env)
over_95_deaths <- final_env[age_group_id == 235]
over_95_deaths[, age_group_id := 40]
collapse_vars <- c("deaths_no_hiv", "deaths_no_shock")

## Collapse to aggregate age groups for reporting
final_env_collapsed[age_group_id <= 5, age_group_id := 1] # Under-5
final_env_collapsed[age_group_id <= 7 & age_group_id > 5, age_group_id:= 23] # 5-14
final_env_collapsed[age_group_id <= 14 & age_group_id > 7, age_group_id:= 24] # 15-49
final_env_collapsed[age_group_id %in% c(13:20, 30:32, 235), age_group_id:= 40] # 50+

final_env_collapsed <- final_env_collapsed[, lapply(.SD, sum), .SDcols = collapse_vars,
                             by = lt_ids]

## Merge over-95 deaths on to HIV adjustment because we don't keep 95+ from the LT process, but want to add the overall totals here
hiv_adjust <- merge(hiv_adjust, over_95_deaths, by = lt_ids, all.x = T)
hiv_adjust[age_group_id == 40, with_hiv_mx := with_hiv_mx + deaths_no_shock]
hiv_adjust[age_group_id == 40, no_hiv_mx := no_hiv_mx + deaths_no_hiv]
hiv_adjust[age_group_id == 40, implied_lt_hiv := with_hiv_mx - no_hiv_mx]
hiv_adjust[, (c("no_hiv_mx", "deaths_no_hiv", "deaths_no_shock")) := NULL]

hiv_adjust <- merge(hiv_adjust, final_env_collapsed, by = lt_ids)
hiv_adjust[, deaths_with_shock := NULL]

setnames(hiv_adjust,
         c("implied_lt_hiv", "with_hiv_mx", "mx_spec_hiv", "deaths_no_hiv", "deaths_no_shock"),
         c("input_lt_hiv", "input_with_hiv_env", "input_spec_hiv", "output_no_hiv_env", "output_with_hiv_env"))


hiv_adjust[age_group_id == 1, input_with_hiv_env := output_with_hiv_env]
hiv_adjust[, output_hiv_specific := output_with_hiv_env - output_no_hiv_env]

## Save mx and ax for all ages
dups <- hiv_adjust[, if (.N > 1) .SD, by=lt_ids]
if (nrow(dups) > 0) stop("Duplicates in hiv adjustment by age, location, year, sex, draw")
rm(dups)
filepath <- FILEPATH
if(file.exists(filepath)) file.remove(filepath)
rhdf5::h5createFile(filepath)
invisible(lapply(years, save_hdf, data = hiv_adjust, filepath = filepath, by_var = "year_id", level = 2))
rhdf5::H5close()


