#' Import Empirical Lifetable values based on the lifetable database category for a given country
#'
#' Import and format empirical lifetables based on the subset of lifetable entries which you want to import.
#'
#' @param lt_type character value representing the LT type to pull results for. Values:
#'  * universal: Countries without country-specific lifetables, matching to universal lifetable database. Will use all lifetables in the universal lifetable database.
#'  * location-specific: Countries with some country-specific non-generalizable lifetables. Will use all country-specific lifetables from the location-specific database, and then all in universal database.
#'  * USA: United States and all subnational locations. Will only use USA-specific lifetable.
#'  * ZAF: South Africa and all subnational locations. Will only use ZAF-specific lifetable.
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
#' @import haven
#' @import assertable


import_lt_empirical <- function(lt_type = "", hiv_cdr, location, loc_map) {
  input_dir <- "FILEPATH"
  if(!(lt_type) %in% c("universal", "location-specific", "USA", "ZAF")) stop("lt_type must be one of the following options: large, small, USA, ZAF")

  if(lt_type == "universal") filepath <- paste0(input_dir,"/ltbase.dta")
  if(lt_type == "location-specific") filepath <- paste0(input_dir,"/ltbase_small.dta")
  if(lt_type == "USA") filepath <- paste0(input_dir, "/usa_life_tables.dta")
  if(lt_type == "ZAF") filepath <- paste0(input_dir, "/ltbase_small.dta")

  empir_lts <- data.table(read_dta(filepath))

  if(lt_type != "USA") {
    if(lt_type != "universal") {
      univ_lts <- data.table(read_dta(paste0(input_dir,"/ltbase.dta")))
    } else {
      univ_lts <- empir_lts
    }
    univ_lts <- merge(univ_lts,
                      hiv_cdr[, .SD, .SDcols = c("ihme_loc_id", "year", "sex", "adult_hiv_cdr")],
                      by = c("ihme_loc_id", "year", "sex"), all.x=T)
    univ_lts <- univ_lts[adult_hiv_cdr < .001 | is.na(adult_hiv_cdr)]
    univ_lts[, adult_hiv_cdr := NULL]
  }

  ## For location-specific LTs, use the location-specific observation only along with all of the universal observations
  if(lt_type %in% c("location-specific", "ZAF")) {
    if(lt_type == "location-specific") empir_lts <- empir_lts[ihme_loc_id == location]
    if(lt_type == "ZAF") empir_lts <- empir_lts[grepl("ZAF", ihme_loc_id)]

    ## Rename source type slightly to avoid duplicates (e.g. MEX_4654 has observations in the location-specific db and in the universal db)
    empir_lts[, source_type := paste0("location_specific_", source_type)]

    ## Drop universal life tables that conflict with a location-sex-specific observation for that same year
    ## since all location-specific results should have priority from the location-specific db
    for(drop_sex in c("male", "female")) {
      loc_specific_years <- unique(empir_lts[sex == drop_sex, year])
      add_lts <- univ_lts[sex == drop_sex & (ihme_loc_id != location | !(year %in% loc_specific_years))]
      empir_lts <- rbindlist(list(empir_lts, add_lts), use.names=T, fill=T)
    }
  }

  if(lt_type == "universal") {
    empir_lts <- univ_lts
  }

  setnames(empir_lts,
          c("v5q0", "v45q15", "logit5q0", "logit45q15"),
          c("empir_5q0", "empir_45q15", "empir_logit_5q0", "empir_logit_45q15"))

  lx_colnames <- paste0("lx", c(0,1,seq(5,95,5)))
  lx_na_colnames <- paste0("lx", seq(100, 110, 5))
  lt_colnames <- c("ihme_loc_id", "year", "sex", "source_type", lx_colnames,
                  "empir_5q0", "empir_45q15", "empir_logit_5q0", "empir_logit_45q15")

  for(var in lx_na_colnames) {
    if(!var %in% colnames(empir_lts)) empir_lts[, (var) := NA]
  }

  if(lt_type == "USA") setnames(empir_lts, "source", "source_type")

  empir_lts <- empir_lts[, .SD, .SDcols= c(lt_colnames, lx_na_colnames)]

  assert_colnames <- lt_colnames[!lt_colnames %in% c("lx90", "lx95")]
  assert_values(empir_lts, assert_colnames, "not_na", quiet=T)
  assert_values(empir_lts, c(lx_colnames, "empir_5q0", "empir_45q15"), "lte", 1, na.rm=T, quiet=T)
  assert_values(empir_lts, c(lx_colnames, "empir_5q0", "empir_45q15"), "gte", 0, na.rm=T, quiet=T)

  empir_lts <- merge(empir_lts, loc_map[, list(ihme_loc_id, region_id, super_region_id, parent_ihme)],
                     by = "ihme_loc_id")

  return(empir_lts)
}
