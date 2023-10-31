
# Meta --------------------------------------------------------------------

# Prepare non-pandemic years (i.e. 1980-2019 and 2022) upload files for
# `save_results_cod()`. Basically, we need to fill these files with zeros. Since
# these results will not change between runs, these files are saved in a
# "constant outputs" directory, and the files with actual data (2020-2021) can
# Be symlinked into this directory from their versioned run.
#
# Since cause ID and sex ID can be specified in the function arguments, they
# don't need to be present in the file itself. Therefore, only one permutation
# of cause-sex combination needs to be real files; the other combinations can be
# symlinks of that first canonical set.
#
# The file structure will follow the template:
# ```
# "FILEPATH"v
# ```


# Load packages -----------------------------------------------------------

library(data.table)


# Set parameters ----------------------------------------------------------

dir_out <- "FILEPATH"
if (!fs::dir_exists(dir_out)) fs::dir_create(dir_out, mode = "775")

years_nonpandemic <- c(1980:2019, 2022)


# Load maps ---------------------------------------------------------------

map_locs_lowest <- demInternal::get_locations(gbd_year = 2021, level = "lowest")
map_locs_lowest <- map_locs_lowest[level >= 3]

map_ages <- demInternal::get_age_map(gbd_year = 2021, type = "gbd")

dt_cause_sex <- CJ(
  cause_id = c(1048, 1058),
  sex_id = 1:2
)


# Make data ---------------------------------------------------------------

dt_const <- CJ(
  location_id = map_locs_lowest$location_id,
  age_group_id = map_ages$age_group_id
)

dt_const[, paste0("draw_", 0:999) := 0]


# Save --------------------------------------------------------------------

## clear/create sub-directories ----

subdir_out <- with(dt_cause_sex, fs::path(dir_out, cause_id, sex_id))
fs::dir_delete(subdir_out[fs::dir_exists(subdir_out)])
fs::dir_create(subdir_out, mode = "775")

subdir_out_canon <- subdir_out[1]
subdir_out_sym <- setdiff(subdir_out, subdir_out_canon)

paths_canon <- fs::path(subdir_out_canon, years_nonpandemic, ext = "csv")

## Save "canonical" files ----

purrr::walk(paths_canon, \(fp) readr::write_csv(dt_const, fp))

## Symlink to other  sub-directories ----

purrr::walk(subdir_out_sym, \(fp) fs::link_create(
  path = paths_canon,
  new_path = fs::path(fp, fs::path_file(paths_canon))
))
