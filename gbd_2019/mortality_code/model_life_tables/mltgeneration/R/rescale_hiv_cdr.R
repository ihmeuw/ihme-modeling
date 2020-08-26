#' Rescale HIV Crude Death Rates
#'
#' Use country/sex and sex-specific scalars to scale child and adult HIV CDR
#'
#' @param hiv_draws data.table with variables: ihme_loc_id, year, sex, sim, adult_hiv_cdr, child_hiv_cdr
#' @param country_scalar data.table with variables: ihme_loc_id, sex, ctry_adult_scalar, ctry_child_scalar
#' @param sex_scalar data.table with variables: sex, adult_scalar, child_scalar
#' @param hiv_ctry_exceptions data.table with variables: ihme_loc_id, sex
#' @param hiv_ctry_groups data.table with variables: ihme_loc_id, sex, group (character)
#'
#' @return data.table with variables: ihme_loc_id, year, sex, sim, adult_hiv_cdr, child_hiv_cdr
#' @export
#'
#' @import data.table
#' @import assertable

rescale_hiv_cdr <- function(hiv_draws, country_scalar, sex_scalar, hiv_ctry_exceptions, hiv_ctry_groups) {
	
	## Bring all datasets together
		hiv_draws <- merge(hiv_draws, country_scalar, by=c("ihme_loc_id", "sex"), all.x=T)
		hiv_draws <- merge(hiv_draws, sex_scalar, by = "sex")
		hiv_draws <- merge(hiv_draws, hiv_ctry_groups, by=c("ihme_loc_id"), all.X=T)

		assert_values(hiv_draws, "group", "not_na", quiet=T)

	## Set sex-specific scalars to 0 for non-group-1A countries
		hiv_draws[group != "1A", adult_scalar := 1]
		hiv_draws[group != "1A", child_scalar := 1]
		hiv_draws[, group := NULL]

	## Apply country-specific and sex-specific scalars
		hiv_draws[, adult_hiv_cdr := adult_hiv_cdr * ctry_adult_scalar * adult_scalar]
		hiv_draws[, child_hiv_cdr := child_hiv_cdr * ctry_child_scalar * child_scalar]
		hiv_draws[, c("ctry_adult_scalar", "ctry_child_scalar", "adult_scalar", "child_scalar") := NULL]

	## Enforce HIV to 0 for a subset of countries where with-HIV VR is used and should be fit directly
		hiv_draws <- merge(hiv_draws, hiv_ctry_exceptions, by=c("ihme_loc_id"), all.x=T)
		hiv_draws[exception == 1 & !is.na(exception), adult_hiv_cdr := 0]
		hiv_draws[exception == 1 & !is.na(exception), child_hiv_cdr := 0]
		hiv_draws[, (c("exception", "group")) := NULL]

	assert_values(hiv_draws, c("adult_hiv_cdr", "child_hiv_cdr"), "not_na", quiet=T)
	return(hiv_draws)
}
