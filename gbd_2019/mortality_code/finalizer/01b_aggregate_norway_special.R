

library(pacman)
pacman::p_load(data.table, argparse, assertable, readr)
library(mortcore, lib=FILEPATH)


parser <- ArgumentParser()
parser$add_argument('--shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The with shock death number estimate version for this run')
parser$add_argument('--parent_nor_loc', type='integer', required=TRUE,
                    help = "Parent location ID for the Norway loc to aggregate up for")


args <- parser$parse_args()
shocks_addition_run_id <- args$shock_death_number_estimate_version
parent_nor_loc <- args$parent_nor_loc

years <- 1950:2019

# set ID variables
mean_ids = c('location_id', 'sex_id', 'year_id', 'age_group_id')
draw_ids = c(mean_ids, 'draw')

nor_locs <- fread(FILEPATH)[parent_id == parent_nor_loc]
nor_locs <- nor_locs[!duplicated(nor_locs)]

lowest_nor <- unique(nor_locs$ihme_loc_id)

# bring in mx/ax for lowest locations

load_hdf_draws <- function(fpath, by_vals) {

  # load hdf file by draw
  draws <- lapply(by_vals, function(i) {
    mortcore::load_hdf(filepath = fpath, by_val = i)
  })
  draws <- rbindlist(draws)
  return(draws)
}

mx_ax_filenames <- FILEPATH
mx_ax <- lapply(mx_ax_filenames, load_hdf_draws, by_vals=years)
mx_ax <- rbindlist(mx_ax)

# Bring in envelope
env_filenames <- FILEPATH
env <- lapply(env_filenames, load_hdf_draws, by_vals=years)
env <- rbindlist(env)



# Bring in population
pop_dt <- fread(paste0(input_dir, "/population.csv"))

# fill old ages
fill_ages <- function(new_age_group_id) {
  dt <- pop_dt[age_group_id == 235]
  dt[, age_group_id := new_age_group_id]
  return(dt)
}

old_pops <- rbindlist(lapply(c(33, 44, 45, 148), fill_ages))

pop_dt <- rbindlist(list(pop_dt, old_pops))

# Merge on pop to mx/ax - aggregate in count space
agg_mx_ax <- merge(mx_ax, pop_dt, by=mean_ids, all.x=T)

# check pop merge worked
if (any(is.na(agg_mx_ax$population))) stop("Population merge didn't work, double check")


# Get mx/ax in count space
agg_mx_ax[, `:=`(
  deaths_with_shock = mx_with_shock*population,
  deaths_no_shock = mx_no_shock*population,
  deaths_no_hiv = mx_no_hiv*population
)]

agg_mx_ax[, `:=`(
  ax_with_shock = ax_with_shock*deaths_with_shock,
  ax_no_shock = ax_no_shock*deaths_no_shock,
  ax_no_hiv = ax_no_hiv * deaths_no_hiv
)]

# Aggregate
agg_mx_ax <- agg_mx_ax[, lapply(.SD, sum), by=c('sex_id', 'year_id', 'age_group_id', 'draw'), .SDcols=setdiff(names(agg_mx_ax), draw_ids)]

# turn back into count space
agg_mx_ax[, `:=`(
  ax_with_shock = ax_with_shock / deaths_with_shock,
  ax_no_shock = ax_no_shock / deaths_no_shock,
  ax_no_hiv = ax_no_hiv / deaths_no_hiv
)]

agg_mx_ax[, `:=`(
  mx_with_shock = deaths_with_shock / population,
  mx_no_shock = deaths_no_shock / population,
  mx_no_hiv = deaths_no_hiv / population
)]


# set final cols
agg_mx_ax[, c('population', 'deaths_with_shock', 'deaths_no_shock', 'deaths_no_hiv') := NULL]
agg_mx_ax[, location_id := parent_nor_loc]


# Aggregate envelope
agg_env <- env[, lapply(.SD, sum), by=setdiff(draw_ids, 'location_id'), .SDcols=setdiff(names(env), draw_ids)]
agg_env[, location_id := parent_nor_loc]


# Check some assertions - ids, na, dups ------------------------------------------------------------
env_ids <- list(
  location_id = parent_nor_loc,
  age_group_id = unique(env$age_group_id),
  year_id = years,
  sex_id = 1:2,
  draw = 0:999
)

mx_ids <- list(
  location_id = parent_nor_loc,
  age_group_id = unique(mx_ax$age_group_id),
  year_id = years,
  sex_id = 1:2,
  draw = 0:999
)

assertable::assert_ids(agg_env, env_ids)
assertable::assert_ids(agg_mx_ax, mx_ids)

assertable::assert_values(agg_mx_ax, colnames=names(agg_mx_ax))
assertable::assert_values(agg_env, colnames=names(agg_env))

if (nrow(agg_mx_ax[, if(.N > 1) .SD, by=draw_ids]) > 0) stop("Duplicates in mx/ax")
if (nrow(agg_env[, if(.N > 1) .SD, by=draw_ids]) > 0) stop("Duplicates in envelope")

# Write out  ------------------------------------------------------------------


env_filepath <- paste0(lowest_dir, "final_env_NOR_", parent_nor_loc, ".h5")
if(file.exists(env_filepath)) file.remove(env_filepath)
rhdf5::h5createFile(env_filepath)
invisible(lapply(years, save_hdf, data= agg_env, filepath=env_filepath, by_var="year_id"))
rhdf5::H5close()


mx_filepath <- paste0(lowest_dir, "mx_ax_NOR_", parent_nor_loc, ".h5")
if(file.exists(mx_filepath)) file.remove(mx_filepath)
rhdf5::h5createFile(mx_filepath)
invisible(lapply(years, save_hdf, data= agg_mx_ax, filepath=mx_filepath, by_var="year_id"))
rhdf5::H5close()
