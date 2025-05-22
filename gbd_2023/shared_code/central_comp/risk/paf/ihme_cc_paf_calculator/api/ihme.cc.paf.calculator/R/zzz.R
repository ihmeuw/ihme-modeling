# Package side-effects 

# Upon package load, reticulate python code, importing ihme_cc_paf_calculator to
# a global variable 'paf_calculator'
.onLoad <- function(libname, pkgname) {
    #' Load module via reticulate
    #'
    #' Checks environment variables for optional override of conda env
    load_paf_calculator <- function() {
        CENTRAL_CONDA_ENV = 
        USE_CURRENT_ENV <- Sys.getenv(
            "PAF_USE_CURRENT_ENV",
            unset = "0"
        )
        CONDA_PREFIX <- Sys.getenv(
            "CONDA_PREFIX",
            unset = CENTRAL_CONDA_ENV
        )
        CONDA_ENV = ifelse(USE_CURRENT_ENV != "0", CONDA_PREFIX, CENTRAL_CONDA_ENV)

        # Set CONDA_PREFIX before reticulating so the environment variable is propagated.
        # Used for sbatch command within launch
        Sys.setenv("CONDA_PREFIX" = CONDA_ENV)

        packageStartupMessage(
            paste("Loading PAF Calculator public functions from", CONDA_ENV, "via reticulate")
        )
        reticulate::use_condaenv(CONDA_ENV, required = TRUE)
        return(reticulate::import("ihme_cc_paf_calculator"))
    }

    # Load reticulated PAF Calculator upon package load
    paf_calculator <<- load_paf_calculator()

    invisible()
}

# When package is unloaded, remove the global variable
.onUnload <- function(libpath) {
    rm(paf_calculator, pos = ".GlobalEnv")

    invisible()
}
