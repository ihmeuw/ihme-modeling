#' Calculate thresholds for which to include lifetables in the universal or location-specific pool
#' After segmenting lifetables, keep each country/year/sex observation with the least smoothing, by lifetable category
#'
#' @param empir_lt data.table with variables ihme_loc_id, sex, source_type, deaths_nid, deaths_underlying_nid, smooth_width, age, qx
#' @param id_vars character vector of variables that, in addition to age and smooth_width, uniquely identify all observations
#' @param hiv_cdr data.table of adult HIV CDR, coming from best 45q15 estimate run. ihme_loc_id, year, sex
#' @param group_1_list character vector of ihme_loc_ids that are Group 1A or Group 1B
#' @param pop_age_pools data.table with ihme_loc_id and pop_pool_age, from population old-age pooling methods
#' @param country_pop data.table with ihme_loc_id, year_id, mean_pop (representing population). Used for conditional population-based outliering/cutoffs
#' @param loc_spec_comp data.table with ihme_loc_id, sex, year, source_type, comp_under_85. Countries that violate minimum completeness bounds and need to be forced to location-specific life-tables
#' @param loc_spec_manual data.table with ihme_loc_id, sex, year, source_type, smooth_width, manual_loc_specific. Where manual_loc_specific=1 we'll set to location-specific.
#' @param outliers_5q0_45q15 data.table with ihme_loc_id, sex, year, source_type, child_adult. Where child_adult=1 we'll set to outlier all smooth widths.
#' @param outliers_comp data.table with ihme_loc_id, sex, year, source_type, comp_under_50. Where comp_under_50=1 we'll outlier all smooth widths.
#' @param outliers_manual data.table with ihme_loc_id, sex, year, source_type, smooth_width, manual_outlier. Where manual_outlier=1 we'll outlier.
#'
#' @return returns empir_lt with the same variables
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable
#' @import stringr

cascade_select_lts <- function(empir_lt, id_vars, hiv_cdr, group_1_list, pop_age_pools, country_pop,
                               loc_spec_comp, loc_spec_manual, outliers_5q0_45q15, outliers_comp, outliers_manual) {

  empir_lt <- copy(empir_lt)

  ## Double check that we don't have duplicates by id_vars
  id_list <- list()
  for(var in c(id_vars, "smooth_width", "age")) {
    id_list[[var]] <- unique(empir_lt[, get(var)])
  }
  assert_ids(empir_lt, id_list, assert_combos = F, assert_dups = T)

  ## Calculate standard lifetable tests/exclusion criteria for categorization
  ## Test 1: Number of times over age 50 where qx goes down by age -- we expect qx to increase in older ages as age also increases
  ## Test 2: Whether there are any small-numbers drop-downs, contextualized by having a drop of more than 25% immediately followed by an increase of more than 50%
  ## Test 3: Whether there are jumps (qx doubles and then gets halved) in qx in ages 20 and older -- probably indicative of an age-specific extraction error
  ## Test 4: Whether qx is less than .5 at age 90-94 (males) or .4 (females), than .35 at age 85-89 (males) or .25 (females), or less than .25 at age 80-84 (males), or .13 (females). Based on lowest qx in Japan
  ## Test 5: Whether population ages are pooled into 80+ then re-estimated, or broader age groups such as 70+. If so, categorize as location-specific since we don't trust the age-patterns as much for universal use
  
  # setup, calculate changes age to age
  setorderv(empir_lt, c(id_vars, "smooth_width", "age"))
  empir_lt[, qx_diff := qx - shift(qx, type = "lag"), by = c(id_vars, "smooth_width")]
  empir_lt[, qx_log_diff := log(qx) - shift(log(qx), type = "lag"), by = c(id_vars, "smooth_width")]
  empir_lt[, pct_qx_diff := qx_diff / shift(qx, type = "lag"), by = c(id_vars, "smooth_width")]

  # Test 4: old age qx cutoffs, based on Japan
  old_qx_cutoff_dt <- data.table(sex = c(rep("male", 3), rep("female", 3)), 
                                 age = rep(seq(80, 90, 5), 2),
                                 old_qx_min = c(.25, .35, .5, .13, .25, .4))
  empir_lt <- merge(empir_lt, old_qx_cutoff_dt, by = c("sex", "age"), all.x = T)

  # Create table of drop info, using tests above
  empir_lt_drops <- empir_lt[, .(num_down = length(qx_diff[qx_diff < 0 & age >= 50]),  # Test 1
                                 small_num_drops = length(qx_diff[qx_diff <= (-.25 * qx) & shift(qx_diff, type = "lead") > (.5 * qx) & age >= 20]),  # Test 2
                                 qx_jumps = length(qx_diff[qx_diff >= shift(qx, type = "lag") & shift(qx_diff, type = "lead") <= -.5 * qx & age >= 20]),  # Test 3
                                 old_qx_cutoff = length(qx[qx < old_qx_min & age >= 80 & age <= 90]), # Test 4
                                 min_5q5 = qx[age == 5],
                                 min_5q10 = qx[age == 10]),
                             by = c(id_vars, "smooth_width")]
  empir_lt[, c("old_qx_min") := NULL]
  
  ## Merge on 5q0/45q15, manual, and completeness outliers
  empir_lt_drops <- merge(empir_lt_drops, outliers_5q0_45q15, by = id_vars, all.x = T)
  empir_lt_drops <- merge(empir_lt_drops, outliers_manual, by = c(id_vars, "smooth_width"), all.x = T)
  empir_lt_drops <- merge(empir_lt_drops, outliers_comp, by = id_vars, all.x = T)
  
  ## Merge manual location specific designations
  empir_lt_drops <- merge(empir_lt_drops, loc_spec_manual, by = c(id_vars, "smooth_width"), all.x = T)

  ## Completeness restrictions to location-specific
  empir_lt_drops <- merge(empir_lt_drops, loc_spec_comp, by = id_vars, all.x = T)
  empir_lt_drops[is.na(comp_under_85), comp_under_85 := 0]
  
  ## Pop old-age pooling restrictions to location-specific
  empir_lt_drops <- merge(empir_lt_drops, pop_age_pools, by = "ihme_loc_id", all.x = T)

  ## Initialize as outlier, roll-down and recategorize from least-restrictive to most-restrictive
  empir_lt_drops[, triggered_outliers := ""]
  empir_lt_drops[num_down >= 2, triggered_outliers := paste(triggered_outliers, 7, sep = ",")]
  empir_lt_drops[small_num_drops > 0 | qx_jumps > 0, triggered_outliers := paste(triggered_outliers, 8, sep = ",")]
  empir_lt_drops[old_qx_cutoff > 0, triggered_outliers := paste(triggered_outliers, 11, sep = ",")]
  empir_lt_drops[manual_outlier == 1, triggered_outliers := paste(triggered_outliers, 13, sep = ",")]
  empir_lt_drops[child_adult == 1, triggered_outliers := paste(triggered_outliers, 18, sep = ",")]
  empir_lt_drops[comp_under_50 == 1, triggered_outliers := paste(triggered_outliers, 19, sep = ",")]
  empir_lt_drops[triggered_outliers != "", life_table_category := "outlier"]
  
  ## Location specific designations, completeness & population age pooling
  empir_lt_drops[triggered_outliers == "", life_table_category := "universal"]
  empir_lt_drops[(life_table_category != "outlier") &
                   (comp_under_85 == 1 |
                    pop_pool_age < 85 |
                    manual_loc_specific == 1), life_table_category := "location specific"]

  ## Recategorize universal life tables for South Africa to location specific
  ## Because they have specific age patterns (HIV)
  empir_lt_drops[(grepl("ZAF", ihme_loc_id)) & life_table_category != "outlier", life_table_category := "location specific"]

  ## Similarly, recategorize Iran subnationals to location specific if universal, since poulation etc. is in flux 
  empir_lt_drops[(grepl("IRN_", ihme_loc_id)) & life_table_category != "outlier", life_table_category := "location specific"]

  ## Move all Group 1 and 2 countries with HIV CDR over .0001 (based on USA 1997 lifetables as a threshold) to location specific
  ## Note: Not a huge issue that they are there in general because ST-GPR subtracts HIV directly, so fine if our HIV estimation is flawed
  ## And in Group 1, countries between .0001 and .001 usually have high enough adult mortality where it's not as dramatic of an effect on overall mortality
  ## But a big issue if it were in universal because Group 1 countries could potentially get influenced with an incorrect with-HIV age pattern
  empir_lt_drops <- merge(empir_lt_drops, hiv_cdr, by = c("ihme_loc_id", "sex", "year"), all.x = T)
  empir_lt_drops[is.na(adult_hiv_cdr), adult_hiv_cdr := 0]
  empir_lt_drops[adult_hiv_cdr >= .0001 & life_table_category != "outlier", life_table_category := "location specific"]

  ## For group 1, remove all LTs above a higher threshold of HIV that really affects age patterns
  ## Exclude ZAF because we want the with-HIV lifetables for them
  empir_lt_drops[ihme_loc_id %in% group_1_list & adult_hiv_cdr >= .0005 &
                   life_table_category != "outlier" &
                   !(grepl("ZAF", ihme_loc_id)),
                 triggered_outliers := paste(triggered_outliers, 14, sep = ",")]

  ## Population-based recategorizations, to avoid such significant jumpiness in low-population countries
  ## For countries with under 100,000 population, restrict from being used as universal
  empir_lt_drops <- merge(empir_lt_drops, country_pop, by = c("ihme_loc_id", "year"))
  empir_lt_drops[mean_pop <= 100000 & life_table_category == "universal", life_table_category := "location specific"]

  ## Remove places where 5-9 qx is less than .0001 and 10-14 qx is less than .0002. Current GBD country-level lowest are .0001463 and .000238, respectively
  ## Mostly affects places where the population is low and mortality is relatively low in that age group, causing deaths to be between 0 and 2 most times in those age groups
  empir_lt_drops[min_5q5 < .0001 | min_5q10 < .0002, triggered_outliers := paste(triggered_outliers, 16, sep = ",")]

  ## Second outlier trigger to grab all of the exclusions from 14 onwards that have not been captured in the LT category list yet
  empir_lt_drops[triggered_outliers != "", life_table_category := "outlier"]

  ## Keep only the least-smoothed across IDs and life table categories
  ## If the least-smoothed is a universal, do not let a location-specific LT be generated -- use the least-smoothed LT (universal) for the location
  empir_lt_drops[, min_smooth := min(smooth_width), by = c(id_vars, "life_table_category")]
  empir_lt_drops[life_table_category != "outlier", overall_min_smooth := as.double(min(min_smooth)), by = c(id_vars)]
  empir_lt_drops[min_smooth != smooth_width & life_table_category != "outlier",
                 `:=` (triggered_outliers = "17", life_table_category = "outlier")] # keep lowest by category
  empir_lt_drops[triggered_outliers != "17" & life_table_category == "location specific" & min_smooth > overall_min_smooth,
                 `:=` (triggered_outliers = "17", life_table_category = "outlier")] # drop location specific if greater smoothing than universal
  empir_lt_drops[, min_smooth := NULL]
    
  ## Keep only the maximum outlier_type_id value from the list, for upload
  empir_lt_drops[, max_outlier_type_id := apply((str_split_fixed(triggered_outliers, ",", Inf)), MARGIN = 1, FUN = function(x) max(as.numeric(x), na.rm = T))]
  empir_lt_drops[is.na(max_outlier_type_id) | is.infinite(max_outlier_type_id), max_outlier_type_id := 1] # Not outliered
  empir_lt_drops[, outlier_type_id := max_outlier_type_id]
  empir_lt_drops[, max_outlier_type_id := NULL]
  
  empir_lt <- merge(empir_lt, empir_lt_drops, by = c(id_vars, "smooth_width"), all.x = T)
  
  ## Recategorize life tables such that ones with sex crossover are only used as location-specific
  ## This needs to go last because it depends on which smoothing levels of ELTs have been included
  crossover <- empir_lt[age >= 60 & life_table_category == "universal" & outlier_type_id == 1]
  crossover <- dcast(crossover, ihme_loc_id + year + source_type + age ~ sex, value.var = "qx")
  crossover <- crossover[!is.na(female) & !is.na(male)] # if only male or only female is universal, drop them
  crossover <- crossover[age >= 60 & female > male]
  crossover <- crossover[, .SD, .SDcols = c("ihme_loc_id", "year", "source_type")]
  crossover <- unique(crossover)
  crossover[, crossover := 1]
  crossover <- crossover[, c("ihme_loc_id", "year", "source_type", "crossover")]
  
  empir_lt <- merge(empir_lt, crossover, by = c("ihme_loc_id", "year", "source_type"), all.x = T)
  empir_lt[crossover == 1 & life_table_category == "universal", life_table_category := "location specific"]
  
  ## Remove higher smooth width location-specific if two loc-specific are now included
  empir_lt[life_table_category == "location specific", n_loc_specific := .N, by = c(id_vars)]
  empir_lt[n_loc_specific > 1, min_smooth := min(smooth_width), by = c(id_vars)]
  empir_lt[smooth_width > min_smooth, `:=` (life_table_category = "outlier",
                                            outlier_type_id = 17)]
  
  ## Double check that all outliers have life_table_category = "outlier"
  empir_lt[outlier_type_id != 1, life_table_category := "outlier"]

  ## Format and output
  return(empir_lt)
}
