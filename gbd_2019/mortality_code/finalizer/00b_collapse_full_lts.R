#### Create non-lowest location level abridged life tables


library(pacman)
pacman::p_load(assertable, data.table, argparse, readr)
library(ltcore, lib=FILEPATH)


parser <- argparse::ArgumentParser()
parser$add_argument('--shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The with shock death number estimate version for this run')
parser$add_argument('--no_shock_death_number_estimate_version', type="integer", required=TRUE,
                    help='The no shock death number estimate version for this run')
parser$add_argument('--lt_type', type='character', required=TRUE,
                    help = "Type of LT to run; with_shock, with_hiv, no_hiv")
parser$add_argument('--location_id', type="integer", required=TRUE,
                    help='National level location ID to run this job for')

args <- parser$parse_args()
shock_death_number_estimate_version <- args$shock_death_number_estimate_version
no_shock_version <- args$no_shock_death_number_estimate_version
lt_type <- args$lt_type
loc_id <- args$location_id

# Set id variables
id_vars <- c('location_id', 'year_id', 'sex_id', 'age_group_id', 'draw')

# Read in the full life table draw

flt_dir <- FILEPATH
input_file <- FILEPATH
print(input_file)

lt <- fread(input_file)

# Read in full age map
finalizer_dir <- FILEPATH
full_ages <- fread(FILEPATH)


# Merge on age start to lt
lt <- merge(lt, full_ages[, .(age_group_id, age=age_group_years_start)], by='age_group_id')

setkeyv(lt, c('sex_id', 'year_id', 'draw', 'age'))

# Generate lx and dx
ltcore::qx_to_lx(lt)
ltcore::lx_to_dx(lt)


# Generate abridged LT
lt[, c('mx', 'lx') := NULL]
abridged_lt <- ltcore::gen_abridged_lt(lt, id_vars = setdiff(id_vars, 'age_group_id'))

# Generate age_length: do this manually, only 4 lengths to consider.
abridged_lt[age==0, age_length := 1]
abridged_lt[age==1, age_length := 4]
abridged_lt[age==110, age_length := 15]
abridged_lt[is.na(age_length), age_length := 5]

assertable::assert_values(abridged_lt, 'age_length')

# Find mx
abridged_lt[age != 110, mx := qx_ax_to_mx(qx, ax, age_length)]
abridged_lt[age == 110, mx := 1/ax]

# ensure no missingness
assertable::assert_values(abridged_lt, names(abridged_lt))

# Output
output_filepath <- FILEPATH
readr::write_csv(abridged_lt, output_filepath)
