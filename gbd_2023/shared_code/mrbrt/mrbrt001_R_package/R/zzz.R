#
# zzz.R
#
# May 2020
#

mrtool <- NULL
xwalk <- NULL

.onLoad <- function(libname, pkgname) {
  use_condaenv(condaenv="mrtool_0.0.1", conda="FILEPATH", required = TRUE)
  mrtool <<- import("mrtool")
  for (nm in names(mrtool)) assign(nm, mrtool[[nm]], parent.env(environment()))

  xwalk <<- import("crosswalk")
  for (nm2 in c("linear_to_log", "linear_to_logit", "log_to_linear", "logit_to_linear")) {
    assign(nm2, xwalk[["utils"]][[nm2]], parent.env(environment()))
  }
}

