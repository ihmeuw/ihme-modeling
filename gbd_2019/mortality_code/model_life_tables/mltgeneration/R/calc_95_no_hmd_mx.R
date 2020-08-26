#' Replace 95+ death rate for envelope, in non-HMD countries, with fitted relationship between 90-94 and 95+ derived from HMD
#'
#' Given a data.table with mx for age 90 and HMD regression parameters, generate mx for age 95.
#'
#' @param env_mx data.table with variables: ihme_loc_id, sex, age, year, sim, mx
#' @param mx_params data.table with variables: sex, parmx. Regression parameters based on HMD.
#' @param id_vars character vector of id variables (last one must be age). Must uniquely identify observations.
#'
#' @return data.table with all existing variables from env_mx. Modified mx value.
#' @export
#'
#' @import data.table
#' @import assertable

calc_95_no_hmd_mx <- function(env_mx, mx_params, id_vars) {
    if(tail(id_vars, 1) != "age") stop("numeric variable age must be the last var specified in id_vars")
    setkeyv(env_mx, id_vars)
    env_mx <- merge(env_mx, mx_params, by = "sex")
    
    env_mx[age == 95, mx := env_mx[age == 90, mx] * parmx]
    env_mx[, parmx := NULL]

    assert_values(env_mx[age==95], "mx", "not_na", quiet=T)

    return(env_mx)
}
