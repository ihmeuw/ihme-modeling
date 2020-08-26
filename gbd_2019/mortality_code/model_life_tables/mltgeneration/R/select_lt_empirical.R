#' Select Empirical Lifetable values based on the lifetable database category for a given country
#'
#' Select and format empirical lifetables based on the subset of lifetable entries which you want to import.
#' 
#' @param master_lts data.table with the variables ihme_loc_id, year, sex, source_type_id, life_table_category_id,
#'    lx* (0, 1, 5 [5] 110), empir_5q0, empir_45q15, empir_logit_5q0, empir_logit_45q15
#' @param lt_type character value representing the LT type to pull results for. Values:
#'  * universal: Countries without country-specific lifetables, matching to universal lifetable database. Will use
#'      all lifetables in the universal lifetable database.
#'  * location-specific: Countries with some country-specific non-generalizable lifetables. Will use all
#'      country-specific lifetables from the location-specific database, and then all in universal database.
#'  * USA: United States subnational locations (but not USA national). Will only use USA subnational-specific lifetables.
#'  * ZAF: South Africa and all subnational locations. Will only use ZAF-specific lifetables, currently coming from
#'      location-specific life_table_category_id = 2.
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


select_lt_empirical <- function(master_lts, lt_type = "", hiv_cdr, location, loc_map) {
  if(!(lt_type) %in% c("universal", "location-specific", "USA", "ZAF")) stop("lt_type must be one of the following options: large, small, USA, ZAF")

  if(lt_type == "universal") empir_lts <- master_lts[life_table_category_id == 1]
  if(lt_type == "location-specific") empir_lts <- master_lts[life_table_category_id == 2 & ihme_loc_id == location]
  if(lt_type == "ZAF") empir_lts <- master_lts[life_table_category_id == 2 & grepl("ZAF", ihme_loc_id)]
  if(lt_type == "USA") empir_lts <- master_lts[life_table_category_id == 4]

  master_lts <- merge(master_lts,
                    hiv_cdr[, .SD, .SDcols = c("ihme_loc_id", "year", "sex", "adult_hiv_cdr")],
                    by = c("ihme_loc_id", "year", "sex"), all.x=T)

  ## Drop universal lifetables with more than .001 HIV CDR
  master_lts <- master_lts[adult_hiv_cdr < .001 | is.na(adult_hiv_cdr) | life_table_category_id != 1]

  ## Drop location-specific lifetables with more than .001 HIV CDR
  if(lt_type != "ZAF") master_lts <- master_lts[adult_hiv_cdr < .001 | is.na(adult_hiv_cdr) | life_table_category_id != 2]

  master_lts[, adult_hiv_cdr := NULL]

  ## For location-specific LTs, use the location-specific observation only along with all of the universal observations
  if(lt_type %in% c("location-specific", "ZAF")) {
    univ_lts <- master_lts[life_table_category_id == 1]

  ## Drop universal life tables that conflict with a location-sex-specific observation for that same year
  ## since all location-specific results should have priority from the location-specific db
    for(drop_sex in c("male", "female")) {
      loc_specific_years <- unique(empir_lts[sex == drop_sex, year])
      add_lts <- univ_lts[sex == drop_sex & (ihme_loc_id != location | !(year %in% loc_specific_years))]
      empir_lts <- rbindlist(list(empir_lts, add_lts), use.names=T, fill=T)
    }
  }

  lx_colnames <- paste0("lx", c(0, 1, seq(5, 85, 5)))
  lx_na_colnames <- paste0("lx", seq(90, 110, 5))
  lt_colnames <- c("ihme_loc_id", "year", "sex", "source_type_id", "life_table_category_id", lx_colnames, 
                  "empir_5q0", "empir_45q15", "empir_logit_5q0", "empir_logit_45q15")
  empir_lts <- empir_lts[, .SD, .SDcols= c(lt_colnames, lx_na_colnames)]

  empir_lts <- merge(empir_lts, loc_map[, list(ihme_loc_id, region_id, super_region_id, parent_ihme)],
                     by = "ihme_loc_id")

  return(empir_lts)
}
