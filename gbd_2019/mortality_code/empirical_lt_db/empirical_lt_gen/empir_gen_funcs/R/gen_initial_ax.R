#' Generate Starting ax Values for all ages
#'
#' Initiate ax at midpoint for most ages, but for under-5 do a custom ax calculation based on _______
#'
#' @param dt data.table with variables: ihme_loc_id, sex, year, age, mx
#'
#' @return None. Modifies given data.table in-place
#' @export
#'
#' @examples
#' @import data.table

gen_initial_ax <- function(dt, id_vars) {
    dt <- copy(dt)

    setorderv(dt, id_vars)

    mx_age0 <- dt[age == 0]
    mx_age0[mx < .107, mx_low_correction := 1]
    mx_age0[is.na(mx_low_correction), mx_low_correction := 0]
    setnames(mx_age0, "mx", "mx_0")
    mx_age0[, c("age") := NULL]

    dt <- merge(dt, mx_age0, by = id_vars[id_vars != "age"])
    dt[, ax := 2.5]

    dt[age == 0 & mx_low_correction == 0 & sex == "male", ax := .33]
    dt[age == 0 & mx_low_correction == 0 & sex == "female", ax := .35]
    dt[age == 0 & mx_low_correction == 1 & sex == "male", ax := .045 + 2.684 * mx_0]
    dt[age == 0 & mx_low_correction == 1 & sex == "female", ax := .053 + 2.8 * mx_0]
    dt[age == 1 & mx_low_correction == 0 & sex == "male", ax := 1.352]
    dt[age == 1 & mx_low_correction == 0 & sex == "female", ax := 1.361]
    dt[age == 1 & mx_low_correction == 1 & sex == "male", ax := 1.651 - 2.816 * mx_0]
    dt[age == 1 & mx_low_correction == 1 & sex == "female", ax := 1.522 - 1.518 * mx_0]

    dt[, c("mx_low_correction", "mx_0") := NULL]
    return(dt)
}
