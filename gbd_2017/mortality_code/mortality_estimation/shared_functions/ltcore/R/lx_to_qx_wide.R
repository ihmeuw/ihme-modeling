#' Convert a dataset of person-years lived (lx) to probabilities of death (qx)
#'
#' Given a dataset with columns named lx# where # is a numeric age (e.g. lx0, lx1, etc.), convert to qx. 
#' Modifies the existing dataset, adding variables qx#.
#' qx_current_age = 1 - (lx_next_age / lx_current_age), e.g. qx5 = lx10 / lx5
#'
#' @param dt data.table including: variables lx## covering all ages specified in lx_ages. Assumes 5-year jumps in age groups except ages 0 and 1.
#' @param keep_lx logical for whether to preserve lx variables. Default: F.
#' @param lx_ages numeric vector of ages to generate qx for. Default: 0, 1, 5, 10, 15, 20...110
#' @param assert_na logical, whether to check for NA values in the qx variables. Default: T.
#'
#' @return None. Performs conversions in-place -- will modify original dataset fed into it
#' @export
#'
#' @examples
#' data <- data.table(test=1, test2=2, lx0=1, lx1=.1, lx5=.05, lx10=.025, lx15=.02)
#' lx_to_qx_wide(data, keep_lx=T, lx_ages = c(0,1,5,10,15))
#' 
#' @import data.table
#' @import assertable

lx_to_qx_wide <- function(dt, keep_lx = F, lx_ages = c(0, 1, seq(5, 110, 5)), assert_na = T) {
	for(age in lx_ages) {		
		if(!paste0("lx",age) %in% colnames(dt)) stop(paste0("Need column lx", age, " in dataset -- set lx_ages if you are non-standard"))
		
		# If it's the final age and lx is non-NA for that group, then assume qx=1; otherwise, take lx of current age group - lx of next age group
		if(age == max(lx_ages)) {
			dt[!is.na(get(paste0("lx",age))), (paste0("qx",age)) := 1]
			if(!paste0("qx", age) %in% colnames(dt)) dt[, (paste0("qx",age)) := NA]
		} else {
			# print(age)
			next_age <- lx_ages[match(age, lx_ages) + 1]
			dt[, (paste0("qx",age)) := 1 - (get(paste0("lx", next_age)) / get(paste0("lx", age)))]
		}
	}

	lx_vars <- paste0("lx", lx_ages)
	if(keep_lx == F) dt[, (lx_vars) := NULL]
  
	# qx = 1 at terminal age group, as long as lx for the age group is not NA
	if(assert_na == T) assertable::assert_values(dt, paste0("qx", lx_ages), "not_na", quiet=T)

	return(dt)
}
