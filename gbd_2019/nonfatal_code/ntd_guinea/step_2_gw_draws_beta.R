###########################################################################
# Description: Generate draws of GW incidence for each location year for  #
#              which we have case data.                                   #
#                                                                         #
###########################################################################

### ======================= BOILERPLATE ======================= ###

rm(list=ls())
code_root <- FILEPATH
data_root <- FILEPATH
cause <- "ntd_guinea"

## Define paths 
# Toggle btwn production arg parsing vs interactive development
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse)
    print(commandArgs())
    parser <- ArgumentParser()
    parser$add_argument("--params_dir", type = "character")
    parser$add_argument("--draws_dir", type = "character")
    parser$add_argument("--interms_dir", type = "character")
    parser$add_argument("--logs_dir", type = "character")
    args <- parser$parse_args()
    print(args)
    list2env(args, environment()); rm(args)
    sessionInfo()
} else {
    params_dir  <- FILEPATH
    draws_dir   <- FILEPATH
    interms_dir <- FILEPATH
    logs_dir    <- FILEPATH
}

##	Source relevant libraries
library(stringr)


### ======================= MAIN EXECUTION ======================= ###

create_beta_draws <- function() {
    gw_b_dt <- fread(str_c(interms_dir, FILEPATH))

    # Generate draws from beta distribution
    draws.required <- 1000
    draw.cols <- paste0("draw_", 0:999)

    gw_b_dt[, beta:=sample_size-cases]
    gw_b_dt[, id := .I]
    gw_b_dt[, (draw.cols) := as.list(rbeta(draws.required, cases, beta)), by=id]

    # Output draws for step 2
    step_draws_dir <- str_c(draws_dir, FILEPATH)
    if (!(dir.exists(step_draws_dir))){
        dir.create(step_draws_dir)
    }

    write.csv("FILEPATH", na="")
}


create_beta_draws()
