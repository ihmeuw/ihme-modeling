## Set up settings

  if (Sys.info()[1]=="Windows") {
    user <- Sys.getenv("USERNAME")
  } else {
    user <- Sys.getenv("USER")
  }

## Set code_dir
  code_dir <- "FILEPATH"
  setwd(code_dir)

## Source ltcore files
library(ltcore, lib = "FILEPATH/r-pkg")
library(compiler)

## Source mltgeneration files
mltgen_funcs <- c("agg_u5_env", "calc_95_no_hmd_mx", "calc_mahalanobis", "calc_stan_lt",
                  "gen_110_mx", "gen_80plus_ax", "gen_stage_1_life_table", "gen_starting_ax",
                  "gen_whiv_env", "gen_stage_2_life_table", "import_lt_empirical", "qx_iterations",
                  "recalc_85plus_qx", "recalc_u10_nlx_mx_ax", "rescale_hiv_cdr", "subtract_hiv_mx", "select_scalars", "iter_adjustment_functions",
                  "adjust_stan_qx", "smooth_stan_lts", "select_lt_empirical")
for(func in mltgen_funcs) {
  source(paste0(code_dir, "/mltgeneration/R/", func, ".R"))

  ## Byte-compile each function
  if(func != "iter_adjustment_functions") {
    eval(parse(text = paste0(func, " <- cmpfun(", func, ")")))
  } else {
    for(subfunc in c("stage_1_5q0_adjustment", "stage_1_45q15_adjustment", "with_hiv_5q0_adjustment", "with_hiv_45q15_adjustment", "hiv_free_ZAF_5q0_adjustment", "hiv_free_ZAF_45q15_adjustment")) {
      eval(parse(text = paste0(subfunc, " <- cmpfun(", subfunc, ")")))
    }
  }
}

