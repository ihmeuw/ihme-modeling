#
# zzz.R
#
# January 2020
#
#

.onLoad <- function(libname, pkgname) {

  # use_condaenv(condaenv="crosswalk",
  use_condaenv(condaenv="mrtool_0.0.1",
  # use_condaenv(condaenv="mrtool_dev",
               conda="FILEPATH", required = TRUE)

  cmds1 <- c(
    "import numpy as np",
    "import pandas as pd",
    "from pprint import pprint"
  )

  for (cmd in cmds1) {
    reticulate::py_run_string(cmd)
  }

  xwalk <<- import("crosswalk")
  xsp <<- import("xspline")
}

# crosswalk <- NULL
#
# .onLoad <- function(libname, pkgname) {
#   use_condaenv(condaenv="mrtool_0.0.1", conda="FILEPATH", required = TRUE)
#   crosswalk <<- import("crosswalk")
#   for (nm in names(crosswalk)) assign(nm, crosswalk[[nm]], parent.env(environment()))
# }



