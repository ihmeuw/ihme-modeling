#' Reconcile HIV estimates
#'
#' Reconcile implied HIV mortality from MLT outputs and HIV team's spectrum
#' draws.
#'
#' @param dt Table of a single locations with/without HIV mortality estimates.
#' @param hiv_group HIV group category of input location.
#' @param allow_neonatal_hiv Should non-zero reconciled HIV-specific mortality
#'   in neonatal age groups be allowed? If not (the default) set reconciled
#'   HIV mortality in those age groups to 0.
#'
#' @return Invisibly return `dt` with updated `mx_with_hiv` and `mx_hiv_free`
#'   columns.
#'
#' @export
reconcile_hiv <- function(dt, hiv_group, allow_neonatal_hiv = FALSE) {

  cols_hiv <- c("mx_hiv_free", "mx_with_hiv", "implied_lt_hiv", "mx_spec_hiv", "hiv_free_ratio")
  stopifnot(all(cols_hiv %in% names(dt)))
  stopifnot(length(unique(dt$location_id)) == 1)

  # Reconciled HIV mx ----

  if (hiv_group %in% c("1A", "1B")) {

    dt[age_start >= 0 & age_start < 1, reconciled_hiv := implied_lt_hiv]
    dt[age_start >= 1 & age_start < 15, reconciled_hiv := mx_with_hiv * (1 - hiv_free_ratio)]
    dt[age_start >= 15, reconciled_hiv := (mx_spec_hiv + implied_lt_hiv) / 2]

  } else {

    dt[, reconciled_hiv := mx_spec_hiv]

  }

  if (!allow_neonatal_hiv) {

    dt[age_start < 28/365, reconciled_hiv := 0]

  }

  stopifnot(dt[is.na(reconciled_hiv), .N] == 0)

  # Reconciled with-HIV and no-HIV mx ----

  if (hiv_group == "1A") {

    dt[age_start >= 15, mx_with_hiv := mx_hiv_free + reconciled_hiv]

    dt[
      age_start < 15 & reconciled_hiv > (.9 * mx_with_hiv),
      reconciled_hiv := .9 * mx_with_hiv
    ]
    dt[age_start < 15, mx_hiv_free := mx_with_hiv - reconciled_hiv]

  } else {

    dt[
      reconciled_hiv > (.9 * mx_with_hiv),
      reconciled_hiv := .9 * mx_with_hiv
    ]
    dt[, mx_hiv_free := mx_with_hiv - reconciled_hiv]

  }

  # Check ----

  stopifnot(
    dt[is.na(mx_hiv_free) | is.na(mx_with_hiv), .N] == 0,
    dt[mx_hiv_free > mx_with_hiv, .N] == 0,
    dt[mx_hiv_free < 0, .N] == 0
  )

  if (!allow_neonatal_hiv) {
    stopifnot(
      "Neonatal ages have non-zero reconciled hiv-specific mortality" = all(
        dt[age_start < 28/365, mx_hiv_free == mx_with_hiv]
      )
    )
  }

  dt[, reconciled_hiv := NULL]
  return(invisible(dt))

}
