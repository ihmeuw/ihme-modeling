#' Calculate Standard Life Tables
#'
#' Calculate a standard lifetable matched to a set of observed 5q0/45q15 observations (chunked by location/year/sex, parallel by draw)
#' based off of the number of closest matches you want to keep in relation to each observation.
#' Uses weights based on distance in geographic hierarchy from the empirical LT to the observation to weight the empirical collapse to the standard
#' weight and collapse all empirical LT results into a single age-specific standard lifetable.
#'
#' @param lt_draw numeric, refers to the sim number of hiv_free_qx to use (0-999).
##			\strong{NOTE:} requires hiv_free_qx dataset in global environment (not passed into arguments to conserve memory while doing mclapply)
#' @param empir_lt data.table with variables: ihme_loc_id, year, sex, source_type_id, life_table_category_id, parent_ihme, super_region_id, region_id, empir_logit_45q15, empir_logit_5q0, lx*
#'                 Empirical lifetable database
#' @param weights data.table with variables: sex, age, region_cat (other/super region/region/country), weights
#'				   Weights to determine relative weight of each matched database entry towards standard life table qx value
#' @param loc_map data.table with variables: ihme_loc_id, region_name, super_region_name, parent_ihme
#'
#' @return list containing two data.tables:
#' stan_lt: weighted standard qx values matching the input draws (the standard lifetable)
#'			  variables: ihme_loc_id, year, sex, age, sim, qx, sq5, sq45
#' lt_list: each matched entry of lifetable results, with the normalized weight assigned to it
#'			  variables: ihme_loc_id, year, sex, sim, stan_weight
#'
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable
#'

calc_stan_lt <- function(lt_draw, empir_lt, weights, loc_map) {

		lt_draw <- hiv_free_qx[sim == lt_draw, ]

	## Run assertions
		assert_values(lt_draw, c("ihme_loc_id", "year", "sex", "sim", "logit_calc_45q15", "logit_calc_5q0"), "not_na", quiet=T)

	## Initialize by-vars for rankings
	## We expect the input data to have one row per ihme_loc_id, year, sex, and sim combination
	## After matching empirical LTs, we'll do ranking etc. on those id_vars
		est_id_vars <- c("est_ihme_loc_id", "est_year", "est_sim", "sex")

	## Pull out metadata associated with the given LT observation
		lt_country <- unique(lt_draw[, ihme_loc_id])

		lt_region <- loc_map[ihme_loc_id == lt_country, region_id]
		lt_super <- loc_map[ihme_loc_id == lt_country, super_region_id]
		lt_parent <- loc_map[ihme_loc_id == lt_country, substr(parent_ihme, 1, 3)]

	## Calculate mahalanobis distance (basically, standardized distance between 5q0 and 45q15 values of database entries and target 5q0/45q15
	## Note: distances calculated for sex-specific matches only
		empir_lt <- rbindlist(lapply(1:nrow(lt_draw), calc_mahalanobis, lt_draw, empir_lt))

	## Perform first subset: Keep n_match observations, as well as all location- or location-parent-specific observations
		setkeyv(empir_lt, est_id_vars)
		empir_lt[, dist_rank := frank(.SD, distance), by = est_id_vars]
		empir_lt <- empir_lt[dist_rank <= n_match | ihme_loc_id == lt_country | substr(parent_ihme, 1, 3) == lt_parent]
		empir_lt[, dist_rank := NULL]

	## Exclusion: For South Africa subnationals and nationals, retain only empirical lifetables from the location itself during the HIV epidemic
	## Also, enforce certain year cutoffs to ensure that with-HIV ZAF lifetables are not matched to HIV-free ZAF years
	## Use 1993 as the cutpoint since that's the first year we have with-HIV lifetables from ZAF
	## Current HIV-free ZAF national years: 1980, 1981, 1982
		if(lt_parent == "ZAF" | lt_country == "ZAF") {
			empir_lt <- empir_lt[ihme_loc_id == lt_country | (ihme_loc_id == "ZAF" & year <= 1992)]
	}


	## Exclude locations where the absolute difference in time is 30+ years (unless it's an early-year case in which you allow it to go up to 2000)
	##				 AND it's either a country or parent match
		empir_lt[, abs_lag := abs(year - est_year)]
		empir_lt <- empir_lt[(est_year <= 1969 & year <= 2000) | abs_lag <= 30 | (ihme_loc_id != lt_country & substr(parent_ihme, 1, 3) != lt_parent)]

	## Sort and exclude matches, first keeping country LTs, then parent country LTs, then all others.
	## Within each group, observations are sorted by distance in years and then mahalanobis distance
		empir_lt[, geo_dist := 2]
		empir_lt[substr(parent_ihme, 1, 3) == lt_parent, geo_dist := 1]
		empir_lt[ihme_loc_id == lt_country, geo_dist := 0]

		empir_lt[, dist_rank := frank(.SD, geo_dist, abs_lag, distance), by = est_id_vars]
    	empir_lt <- empir_lt[dist_rank <= n_match]
    	empir_lt[, dist_rank := NULL]
    	if(nrow(empir_lt) == 0) stop("Empirical lifetables reduced to 0 observations after ranked drops")

	## Merge on qx weights according to country/region/super-region proximity and difference in years from current observation
		empir_lt[, c("distance", "abs_lag") := NULL]
		empir_lt[, region_cat := "other super region"]
		empir_lt[super_region_id == lt_super, region_cat := "super region"]
		empir_lt[region_id == lt_region, region_cat := "region"]
		empir_lt[substr(parent_ihme, 1, 3) == lt_parent, region_cat := "parent_subnational"]
		empir_lt[ihme_loc_id == lt_country, region_cat := "country"]

		empir_lt[, lag := est_year - year]
		empir_lt <- merge(empir_lt, weights, by=c("sex", "lag", "region_cat"), all.x=T)
		assert_values(empir_lt, "weights", "not_na", quiet=T)

	## Reduce weights for country-umbrella empirical LT values
		empir_lt[geo_dist == 1, weights := weights/2]

	## Collapse observation weights by age/sex/country etc. (re-scaled to 1)
		setkeyv(empir_lt, est_id_vars)
		empir_lt[, stan_weight := weights / sum(weights), by = est_id_vars]

		lt_list <- empir_lt[, list(ihme_loc_id, source_type_id, life_table_category_id, year, sex, stan_weight,
								   est_ihme_loc_id, est_year, est_sim)]

	## Convert lx to qx values
    	empir_lt <- lx_to_qx_wide(empir_lt, keep_lx = F, assert_na = F)

	## Collapse over standard weights to create standard LT, then reshape
		ages <- c(0, 1, seq(5, 110, 5))
		qx_vars <- paste0("qx", ages)
		empir_lt[, (qx_vars) := lapply(qx_vars, function(x) get(x) * stan_weight)]
		empir_lt <- melt(empir_lt[, .SD, .SDcols=c(est_id_vars, qx_vars)],
						id.vars = est_id_vars,
						variable.name = "age",
						value.name = "qx")
		empir_lt[, age := as.numeric(gsub("qx", "", age))]
		empir_lt <- empir_lt[, list(stan_qx = sum(qx)), by=c(est_id_vars, "age")]

	## Create 5q0 and 45q15 for the standard lifetable -- add on as a general variable
		setkeyv(empir_lt, est_id_vars)
		empir_lt[, px := 1-stan_qx]
		sq5 <- empir_lt[age <= 1, list(sq5 = 1 - prod(px)), by = est_id_vars]
		sq45 <- empir_lt[age >= 15 & age <= 55, list(sq45 = 1 - prod(px)), by = est_id_vars]
		empir_lt <- merge(empir_lt, sq5, by = est_id_vars)
		empir_lt <- merge(empir_lt, sq45, by = est_id_vars)

	## Add on ID variables before returning dataset
		setnames(empir_lt, c("est_ihme_loc_id", "est_year", "est_sim"), c("ihme_loc_id", "year", "sim"))

	## In the life table map estimate, ref_ihme refers to the value of the output standard lifetable (target)
	## While the regular (e.g. ihme_loc_id) value is the value of the empirical lifetable itself
		setnames(lt_list,
				 c("est_ihme_loc_id", "est_year", "est_sim"),
				 c("ref_ihme", "ref_year", "sim"))

	## Final assertions
		assert_values(empir_lt, c("sq5", "sq45"), "not_na", quiet=T)
		assert_values(empir_lt[age <= 80], "stan_qx", "not_na", quiet=T)
		assert_values(lt_list, colnames(lt_list), "not_na", quiet=T)

	return(list(stan_lt = empir_lt, lt_list = lt_list))
}
