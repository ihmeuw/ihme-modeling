#' Download and format empirical life tables from the database
#'
#' @export
#'
#' @examples
#' 
#' @import data.table
#' @import assertable

download_lt_empirical <- function(run_id = "best") {
  age_map <- setDT(get_age_map("lifetable"))[, .(age_group_id, age_group_years_start)]

  empir_lts <- get_mort_outputs("life table empirical", "data", life_table_parameter_id = 4, run_id = run_id)
  summary_5q0_45q15 <- get_mort_outputs("life table empirical", "data", life_table_parameter_id = 3, age_group_ids = c(1, 199), run_id = run_id)

  ## Format empirical LTs
  empir_lts <- merge(empir_lts, age_map, by = "age_group_id")
  empir_lts[, age := paste0("lx", age_group_years_start)]

  id_vars <- c("year_id", "ihme_loc_id", "sex_id", "life_table_category_id", "source_type_id")
  if(nrow(empir_lts[duplicated(empir_lts, by = c(id_vars, "age"))]) > 0) stop("Duplicates found in empir_lts by id_vars and age")

  empir_lts <- empir_lts[, .SD, .SDcols = c(id_vars, "age", "mean")]
  empir_lts <- dcast(empir_lts, as.formula(paste0(paste(id_vars, collapse = "+"), " ~ age")), value.var = "mean")

  ## Format and merge on summary metrics
  summary_5q0_45q15[age_group_id == 1, age := "empir_5q0"]
  summary_5q0_45q15[age_group_id == 199, age := "empir_45q15"]

  id_vars <- c("year_id", "ihme_loc_id", "sex_id", "life_table_category_id", "source_type_id")
  if(nrow(summary_5q0_45q15[duplicated(summary_5q0_45q15, by = c(id_vars, "age"))]) > 0) stop("Duplicates found in empir_lts by id_vars and age")

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
  lx_na_colnames <- paste0("lx", seq(90, 110, 5)) 
  
  for(col in lx_na_colnames) {
    if(!col %in% colnames(empir_lts)) empir_lts[, (col) := as.numeric(NA)]
  }

  lt_colnames <- c("ihme_loc_id", "year", "sex", "source_type_id", "life_table_category_id", lx_colnames, 
                  "empir_5q0", "empir_45q15", "empir_logit_5q0", "empir_logit_45q15")
  empir_lts <- empir_lts[, .SD, .SDcols= c(lt_colnames, lx_na_colnames)]

  assert_values(empir_lts, lx_colnames, "not_na", quiet=T) 
  assert_values(empir_lts, c(lx_colnames, "empir_5q0", "empir_45q15"), "lte", 1, na.rm=T, quiet=T)
  assert_values(empir_lts, c(lx_colnames, "empir_5q0", "empir_45q15"), "gte", 0, na.rm=T, quiet=T)

  return(empir_lts)
}
