#' Calculate thresholds for which to include lifetables in the universal or location-specific pool
#' After segmenting lifetables, keep each country/year/sex observation with the least smoothing, by lifetable category
#'
#' @param empir_lts list of data.tables with variables ihme_loc_id, sex, source_type, deaths_nid, deaths_underlying_nid, smooth_width, age, qx
#' @param id_vars character vector of variables that, in addition to age and smooth_width, uniquely identify all observations
#' @param hiv_cdr data.table of adult HIV CDR, coming from best 45q15 estimate run. ihme_loc_id, year, sex
#' @param group_1_list character vector of ihme_loc_ids that are Group 1A or Group 1B
#' @param loc_spec_comp_restrictions data.table with ihme_loc_id, sex, year, source_type, comp_under_85. Countries that violate minimum completeness bounds and need to be forced to location-specific life-tables
#' @param hmd_qx_bounds data.table with age, sex, pct_95, pct_975, and pct_99. Upper bounds on qx values.
#' @param hmd_qx_diff_regression data.table with age, sex, intercept, and qx_coef Used to calculate predicted qx percent change at age 85 based on actual qx value, compared to the life table itself
#' @param pop_age_pools data.table with ihme_loc_id, pop_pool_age. Where pop_pool_age refers to the minimum age to start pooling ages for pop estimation (e.g. v75 = 75+ gets pooled together)
#' @param country_pop data.table with ihme_loc_id, year_id, mean_pop (representing population). Used for conditional population-based outliering/cutoffs
#' @param keep_metadata logical, whether to keep metadata on the variables used to identify classification criteria
#' @param keep_drops logical, whether to keep return all rows regardless of selection (vetting only)
#'
#' @return returns empir_lt with the same variables, but with many reduced rows after applying selection criteria (unless keep_drops == T)
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable
#' @import stringr

cascade_select_lts <- function(empir_lt, id_vars, hiv_cdr, group_1_list, loc_spec_comp_restrictions, hmd_qx_bounds, hmd_qx_diff_regression, pop_age_pools, country_pop, keep_metadata, keep_drops) {

  empir_lt <- rbindlist(empir_lt, use.names = T)

  id_list <- list()
  for(var in c(id_vars, "smooth_width", "age")) {
    id_list[[var]] <- unique(empir_lt[, get(var)])
  }

  assert_ids(empir_lt, id_list, assert_combos = F, assert_dups = T)

  ## Test that age 90-84, 85-89, and 90-94 qx values are below the ____th percentile of HMD qx values
  hmd_qx_drops <- merge(empir_lt[age >= 80 & age <= 90], hmd_qx_bounds, by = c("age", "sex"))
  hmd_qx_drops[qx > pct_99, hmd_qx_flag := 1]
  hmd_qx_drops[is.na(hmd_qx_flag), hmd_qx_flag := 0]
  hmd_qx_drops <- hmd_qx_drops[, .(hmd_qx_flag = max(hmd_qx_flag)), by = c(id_vars, "smooth_width")]

  ## Test that age 85-89 relationship between the percent change in qx from 80-84 to 85-89 and the qx value at age 85 is within .3 of the predicted based on HMD relationships
  empir_lt <- merge(empir_lt, hmd_qx_diff_regression, by = c("age", "sex"), all.x = T)
  empir_lt[, pred_qx_diff := intercept + qx_coef * qx]

  ## Calculate standard lifetable tests/exclusion criteria for categorization
  ## Test 1: Variance in age-to-age log qx values over the age of 60 (use age 65 as starting since that will first evaluate the change from 60-64 to 65-69).
  ##         Limit at age 90-94 as most data beyond there is extrapolated and suppresses the variance
  ## Test 2: Number of times over age 50 where qx goes down by age -- we expect qx to increase in older ages as age also increases
  ##    Test 3: Whether 1q0 is less than 4q1 -- except in very extreme cases, 1q0 should almost always exceed 4q1 and indicates an age-incompleteness/misreporting issue
  ## Test 4: Whether there are any small-numbers drop-downs, contextualized by having a drop of more than 25% immediately followed by an increase of more than 50%
  ## Test 5: Whether there are any old-age (between age 70 and 95) age-to-age movements where qx does not increase by at least 5% from one age to the next -- indicative of old-age numerator or denominator issues
  ## Test 6: Whether there are jumps (qx doubles and then gets halved) in qx in ages 20 and older
  ## Test 7: Whether qx in age 80-84, 85-89, or 90-94 exceeds the Nth percentile of qx in HMD lifetables
  ## Test 8: Whether population ages are pooled into 80+ then re-estimated, or broader age groups such as 70+. If so, categorize as country-specific.
  ## Test 9: Whether qx is less than .5 at age 90-94 (males) or .4 (females), than .35 at age 85-89 (males) or .25 (females), or less than .25 at age 80-84 (males), or .13 (females). Based on lowest qx in Japan
  ## Test 10: Using regression coefficients for qx vs. pct_qx_diff, determine predicted pct_qx_diff.
  ##          For age 80-84 (comparing diff b/t 75-79 and 80-84) :If difference between predicted pct_qx_diff and actual pct_qx_diff is greater than .2 for male and .3 for female, outlier. Must be within .15 for male or .2 for female for universal
  ##          For age 85-89: If difference between predicted pct_qx_diff and actual pct_qx_diff is greater than .3, outlier. Must be within .15 qx for universal
  setorderv(empir_lt, c(id_vars, "smooth_width", "age"))
  empir_lt[, qx_diff := qx - shift(qx, type = "lag"), by = c(id_vars, "smooth_width")]
  empir_lt[, qx_log_diff := log(qx) - shift(log(qx), type = "lag"), by = c(id_vars, "smooth_width")]
  empir_lt[, pct_qx_diff := qx_diff / shift(qx, type = "lag"), by = c(id_vars, "smooth_width")]

  old_qx_cutoff_dt <- data.table(sex = c(rep("male", 3), rep("female", 3)),
                                 age = rep(seq(80, 90, 5), 2),
                                 old_qx_min = c(.25, .35, .5, .13, .25, .4))

  empir_lt <- merge(empir_lt, old_qx_cutoff_dt, by = c("sex", "age"), all.x = T)

  empir_lt_drops <- empir_lt[, .(var.log.qx = var(qx_log_diff[age >= 65 & age <= 90], na.rm = T),
                             num_down = length(qx_diff[qx_diff < 0 & age >= 50]),
                             qx_1_violated = length(qx_diff[qx_diff > 0 & age == 1]),
                             small_num_drops = length(qx_diff[qx_diff <= (-.25 * qx) & shift(qx_diff, type = "lead") > (.5 * qx) & age >= 20]),
                             old_age_drops = length(qx_diff[qx_diff <= (.05 * qx) & age >= 70 & age <= 95]),
                             qx_jumps = length(qx_diff[qx_diff >= shift(qx, type = "lag") & shift(qx_diff, type = "lead") <= -.5 * qx & age >= 20]),
                             old_qx_cutoff = length(qx[qx < old_qx_min & age >= 80 & age <= 90]),
                             qx_diff_80_residual = abs(pct_qx_diff[age == 80] - pred_qx_diff[age == 80]),
                             qx_diff_85_residual = abs(pct_qx_diff[age == 85] - pred_qx_diff[age == 85]),
                             min_5q5 = qx[age == 5],
                             min_5q10 = qx[age == 10]),
                         by = c(id_vars, "smooth_width")]

  empir_lt[, c("old_qx_min", "intercept", "qx_coef") := NULL]

  empir_lt_drops <- merge(empir_lt_drops, loc_spec_comp_restrictions, by = c("ihme_loc_id", "sex", "year", "source_type"), all.x = T)
  empir_lt_drops[is.na(comp_under_85), comp_under_85 := 0]
  empir_lt_drops <- merge(empir_lt_drops, hmd_qx_drops, by = c(id_vars, "smooth_width"))
  empir_lt_drops <- merge(empir_lt_drops, pop_age_pools, by = "ihme_loc_id", all.x = T)
  empir_lt_drops[is.na(pop_pool_age), pop_pool_age := 95]

  ## Initialize as outlier, roll-down and recategorize from least-restrictive to most-restrictive
  empir_lt_drops[, triggered_outliers := ""]
  empir_lt_drops[var.log.qx >= .02, triggered_outliers := paste(triggered_outliers, 6, sep = ",")]
  empir_lt_drops[num_down >= 2, triggered_outliers := paste(triggered_outliers, 7, sep = ",")]
  empir_lt_drops[small_num_drops > 0 | qx_jumps > 0, triggered_outliers := paste(triggered_outliers, 8, sep = ",")]
  empir_lt_drops[old_age_drops > 0, triggered_outliers := paste(triggered_outliers, 9, sep = ",")]
  empir_lt_drops[hmd_qx_flag > 0, triggered_outliers := paste(triggered_outliers, 10, sep = ",")]
  empir_lt_drops[old_qx_cutoff > 0, triggered_outliers := paste(triggered_outliers, 11, sep = ",")]
  empir_lt_drops[(qx_diff_85_residual > .3 | (qx_diff_80_residual > .2 & sex == "male") | (qx_diff_80_residual > .3 & sex == "female")), triggered_outliers := paste(triggered_outliers, 12, sep = ",")]

  empir_lt_drops[triggered_outliers != "", life_table_category := "outlier"]
  empir_lt_drops[is.na(life_table_category), life_table_category := "location specific"]
  empir_lt_drops[life_table_category == "location specific" & var.log.qx < .01 & num_down == 0 & comp_under_85 == 0 & pop_pool_age >= 85 & qx_diff_85_residual <= .15 & ((qx_diff_80_residual <= .15 & sex == "male") | (qx_diff_80_residual <= .2 & sex == "female")), life_table_category := "universal"]

  ## Recategorize universal life tables for South Africa to location specific
  ## Because they have specific age patterns (HIV)
  empir_lt_drops[(grepl("ZAF", ihme_loc_id)) & life_table_category != "outlier", life_table_category := "location specific"]

  ## Similarly, recategorize Iran subnationals to location specific if universal
  empir_lt_drops[(grepl("IRN_", ihme_loc_id)) & life_table_category != "outlier", life_table_category := "location specific"]

  ## Move all Group 1 and 2 countries with HIV CDR over .0001 (based on USA 1997 lifetables as a threshold) to location specific
  empir_lt_drops <- merge(empir_lt_drops, hiv_cdr, by = c("ihme_loc_id", "sex", "year"), all.x = T)
  empir_lt_drops[is.na(adult_hiv_cdr), adult_hiv_cdr := 0]
  empir_lt_drops[adult_hiv_cdr >= .0001 & life_table_category != "outlier", life_table_category := "location specific"]

  ## For group 1, remove all LTs above a higher threshold of HIV that really affects age patterns
  ## Exclude ZAF because we want the with-HIV lifetables for them
  empir_lt_drops[ihme_loc_id %in% group_1_list & adult_hiv_cdr >= .0005 & life_table_category != "outlier" & !(grepl("ZAF", ihme_loc_id)), triggered_outliers := paste(triggered_outliers, 14, sep = ",")]

  ## Population-based recategorizations, to avoid such significant jumpiness in low-population countries
  ## For countries with under 100,000 population, always outlier single-year and three-year-smoothed lifetables
  ## For countries with under 1 million population, always outlier single-year lifetables, and restrict from being used as universal
  empir_lt_drops <- merge(empir_lt_drops, country_pop, by = c("ihme_loc_id", "year"))
  empir_lt_drops[mean_pop <= 1000000 & life_table_category == "universal", life_table_category := "location specific"]
  empir_lt_drops[mean_pop <= 1000000 & smooth_width == 1, triggered_outliers := paste(triggered_outliers, 15, sep = ",")]
  empir_lt_drops[mean_pop <= 100000 & smooth_width %in% c(1,3), triggered_outliers := paste(triggered_outliers, 15, sep = ",")]

  ## Remove places where 5-9 qx is less than .0001 and 10-14 qx is less than .0002.
  empir_lt_drops[min_5q5 < .0001 | min_5q10 < .0002, triggered_outliers := paste(triggered_outliers, 16, sep = ",")]

  empir_lt_drops[triggered_outliers != "", life_table_category := "outlier"]

  ## Keep only the least-smoothed across IDs and life table categories
  ## If the least-smoothed is a universal, do not let a location-specific LT be generated -- use the least-smoothed LT (universal) for the location
  empir_lt_drops[, min_smooth := min(smooth_width), by = c(id_vars, "life_table_category")]
  empir_lt_drops[life_table_category != "outlier", overall_min_smooth := as.double(min(min_smooth)), by = c(id_vars)]
  empir_lt_drops[min_smooth != smooth_width & life_table_category != "outlier", triggered_outliers := "17"]
  empir_lt_drops[triggered_outliers != "17" & life_table_category == "location specific" & min_smooth > overall_min_smooth, triggered_outliers := "17"]

  ## Keep only the maximum outlier_type_id value from the list, for upload
  empir_lt_drops[, max_outlier_type_id := apply((str_split_fixed(triggered_outliers, ",", Inf)), MARGIN = 1, FUN = function(x) max(as.numeric(x), na.rm = T))]
  empir_lt_drops[is.na(max_outlier_type_id) | is.infinite(max_outlier_type_id), max_outlier_type_id := 1] # Not outliered

  ## Format and output
  return(list(empir_lt, empir_lt_drops))
}
