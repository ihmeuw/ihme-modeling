#' Download and format empirical life tables from the database
#'
#' Format empirical life tables appropriately for processing in main lifetable process.
#' Throw it wide by lx* and empir variables, and carry out assertions on values.
#'
#' @param master_lts data.table with the variables ihme_loc_id, year, sex, source_type_id,
#'   life_table_category_id, lx* (0, 1, 5 [5] 110), empir_5q0, empir_45q15, empir_logit_5q0, empir_logit_45q15
#' @param lt_type character value representing the LT type to pull results for. Values:
#'  * universal: Countries without country-specific lifetables, matching to universal lifetable database.
#'      Will use all lifetables in the universal lifetable database.
#'  * location-specific: Countries with some country-specific non-generalizable lifetables. Will use all
#'      country-specific lifetables from the location-specific database, and then all in universal database.
#'  * USA: United States subnational locations (but not USA national). Will only use USA subnational-specific lifetables.
#'  * ZAF: South Africa and all subnational locations. Will only use ZAF-specific lifetables, currently coming
#'      from location-specific life_table_category_id = 2.
#' @param hiv_cdr data.table containing adult HIV crude death rate, to drop lifetables with high HIV results. Only used if lt_type == "large"
#'  variables: ihme_loc_id, sex, year, adult_hiv_cdr
#' @param location character, ihme_loc_id for the country to pull results for
#' @param loc_map data.table with variables ihme_loc_id, region_id, super_region_id, parent_ihme
#'
#' @return data.table with column names:
#' * ihme_loc_id, year, sex, source_type
#' * lx(0, 1, 5 (5) 110)
#' * empir_5q0, empir_45q15, empir_logit_5q0, empir_logit_45q15
#' * region_id, super_region_id, parent_ihme
#' @export
#'
#' @examples
#'
#' @import data.table
#' @import assertable

download_lt_empirical <- function(run_id = "best") {
  age_map <- setDT(get_age_map("lifetable"))[, .(age_group_id, age_group_years_start)]

  empir_lts <- get_mort_outputs("life table empirical", "data", life_table_parameter_id = c(3, 4), run_id = run_id)
  empir_lts <- empir_lts[outlier_type_id == 1] ## Not outliered data

  summary_5q0_45q15 <- get_mort_outputs("life table empirical", "data", life_table_parameter_id = 3, age_group_ids = c(1, 199), run_id = run_id)
  summary_5q0_45q15 <- summary_5q0_45q15[outlier_type_id == 1] ## Not outliered data

  ## Format empirical LTs
  empir_lts <- merge(empir_lts, age_map, by = "age_group_id")
  empir_lts[life_table_parameter_id == 3, age := paste0("qx", age_group_years_start)]
  empir_lts[life_table_parameter_id == 4, age := paste0("lx", age_group_years_start)]

  id_vars <- c("year_id", "ihme_loc_id", "sex_id", "life_table_category_id", "source_type_id")
  if(nrow(empir_lts[duplicated(empir_lts, by = c(id_vars, "age", "life_table_category_id"))]) > 0) stop("Duplicates found in empir_lts by id_vars and age")

  empir_lts <- empir_lts[, .SD, .SDcols = c(id_vars, "age", "mean")]
  empir_lts <- dcast(empir_lts, as.formula(paste0(paste(id_vars, collapse = "+"), " ~ age")), value.var = "mean")

  # Calculate lx110, then drop qx columns. In the future, we might want to use qx directly instead of going through lx.
  # lx = lx_previous * (1-qx_previous)
  empir_lts[, lx110 := lx105 * (1 - qx105)]
  lx_colnames_all <- paste0("lx", c(0, 1, seq(5, 110, 5)))
  empir_lts <- empir_lts[, .SD, .SDcols = c(id_vars, lx_colnames_all)]

  ## Format and merge on summary metrics
  summary_5q0_45q15[age_group_id == 1, age := "empir_5q0"]
  summary_5q0_45q15[age_group_id == 199, age := "empir_45q15"]

  id_vars <- c("year_id", "ihme_loc_id", "sex_id", "life_table_category_id", "source_type_id")
  if(nrow(summary_5q0_45q15[duplicated(summary_5q0_45q15, by = c(id_vars, "age"))]) > 0) stop("Duplicates found in summary_5q0_45q15 by id_vars and age")

  summary_5q0_45q15 <- summary_5q0_45q15[, .SD, .SDcols = c(id_vars, "age", "mean")]
  summary_5q0_45q15 <- dcast(summary_5q0_45q15, as.formula(paste0(paste(id_vars, collapse = "+"), " ~ age")), value.var = "mean")
  summary_5q0_45q15[, empir_logit_5q0 := logit_qx(empir_5q0)]
  summary_5q0_45q15[, empir_logit_45q15 := logit_qx(empir_45q15)]

  orig_rows <- nrow(empir_lts)
  empir_lts <- merge(empir_lts, summary_5q0_45q15, by = id_vars)
  if(nrow(empir_lts) != orig_rows) stop("Inconsistent number of summary 5q0/45q15 compared to empirical lifetables")

  ## Format and output empirical lifetables
  empir_lts[sex_id == 1, sex := "male"]
  empir_lts[sex_id == 2, sex := "female"]
  setnames(empir_lts, "year_id", "year")

  lx_colnames <- paste0("lx", c(0, 1, seq(5, 85, 5)))
  lx_na_colnames <- paste0("lx", seq(90, 110, 5)) # These colnames do not exist for ZAF and USA-specific LTs

  for(col in lx_na_colnames) {
    if(!col %in% colnames(empir_lts)) empir_lts[, (col) := as.numeric(NA)]
  }

  lt_colnames <- c("ihme_loc_id", "year", "sex", "source_type_id", "life_table_category_id", lx_colnames,
                  "empir_5q0", "empir_45q15", "empir_logit_5q0", "empir_logit_45q15")
  empir_lts <- empir_lts[, .SD, .SDcols= c(lt_colnames, lx_na_colnames)]

  ## For USA -- lx90 and lx95 are NAN for certain places. Ok as long as lx85 is there (qx gets extrapolated) in gen_hiv_free_lt using regression coefficients
  assert_values(empir_lts, lx_colnames, "not_na", quiet=T)
  assert_values(empir_lts, c(lx_colnames, "empir_5q0", "empir_45q15"), "lte", 1, na.rm=T, quiet=T)
  assert_values(empir_lts, c(lx_colnames, "empir_5q0", "empir_45q15"), "gte", 0, na.rm=T, quiet=T)

  return(empir_lts)
}
