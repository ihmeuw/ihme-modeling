# Common functions used for validations

source("utils/db.R")

# Determine whether an rei is a custom paf handled outside this code base
is_custom_paf <- function(rei) {
    return(rei %in% c("abuse_ipv_hiv", "bullying", "drugs_alcohol", "drugs_illicit_direct",
                      "occ_injury", "smoking_direct") |
        rei %like% "^eti_|^temperature" |
        (rei %like% "^air_" & rei != "air_pmhap"))
}

# Check that a risk is continuous and centrally calculated
valid_mediator_risk <- function(rei_id, gbd_round_id) {
    is_valid <- TRUE
    rei_meta <- get_rei_meta(rei_id, gbd_round_id)
    if (rei_meta$rei_calculation_type != 2) {
        message("Risk ", rei_id, " is not continuous")
        is_valid <- FALSE
    }
    if (is_custom_paf(rei_meta$rei) |
        (rei_meta$rei %in% c("nutrition_lbw", "nutrition_preterm"))) {
        message("Risk ", rei_id, " is not centrally calculated")
        is_valid <- FALSE
    }
    return(is_valid)
}
