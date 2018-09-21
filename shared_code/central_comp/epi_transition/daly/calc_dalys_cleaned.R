##########################################################
# Calculates Expected DALYs
##########################################################
## DRIVE MACROS
rm(list=ls())
if (Sys.info()[1] == "Linux"){
    j <- "FILEPATH"
    h <- "FILEPATH"
} else if (Sys.info()[1] == "Darwin"){
    j <- "FILEPATH"
    h <- "FILEPATH"
}

##########################################################
## DEFINE FUNCTIONS
currentDir <- function() {
    # Identify program directory
    if (!interactive()) {
        cmdArgs <- commandArgs(trailingOnly = FALSE)
        match <- grep("--file=", cmdArgs)
        if (length(match) > 0) fil <- normalizePath(gsub("--file=", "", cmdArgs[match]))
        else fil <- normalizePath(sys.frames()[[1]]$ofile)
        dir <- dirname(fil)
    } else dir <- "FILEPATH"
    return(dir)
}
source(paste0(currentDir(), "/primer.R"))

##########################################################
##  Define arguments (dUSERts are set for manual run)
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    dUSERt = "FILEPATH", type = "character")
parser$add_argument("--etmvid", help = "DALYs/HALE Version ID",
                    dUSERt = 2, type = "integer")
parser$add_argument("--etmvid_yll", help="Expected YLL version", 
                    dUSERt = 23, type = "integer")
parser$add_argument("--etmvid_yld", help = "Expected YLD version",
                    dUSERt = 12, type = "integer")
parser$add_argument("--agid", help = "Age Group ID",
                    dUSERt = 10, type = "integer")
parser$add_argument("--sid", help = "Sex ID",
                    dUSERt = 1, type = "integer")
parser$add_argument("--gbdid", help = "Cause ID",
                    dUSERt = 294, type = "integer")
args <- parser$parse_args()

list2env(args, environment()); rm(args)

##########################################################

main <- function() {
    
    ##  Read in DALY inputs and combine (NEED TO ACCOUNT FOR YLD ONLY AND YLL ONLY CAUSES)
    ylldf <- NULL
    ylddf <- NULL
    
    if(file.exists("FILEPATH")) ylldf <- readRDS("FILEPATH")
    if(file.exists("FILEPATH")) ylddf <- readRDS("FILEPATH")
    
    if(gbdid == 294) ylldf <- ylldf[, .(pred = mean(pred), draw = 1, sim = 1), by = eval(names(ylldf)[!names(ylldf) %in% c("draw", "sim", "pred")])]
    if(gbdid == 294) ylldf[, cause_id := gbdid]
    
    dalydf <- rbindlist(list(ylldf, ylddf), use.names = T, fill = T)
    
    ## Calculate DALYs and save
    dalydf <- dalydf[, list(pred = sum(pred)), by = eval(names(dalydf)[!names(dalydf) %in% c("measure_id", "etmodel_type_id", "etmodel_version_id", "pred")])][, measure_id := 2]
    dalydf[, ':='(et_yll_model_version_id = etmvid_yll, et_yld_model_version_id = etmvid_yld)]
    saveRDS(dalydf,
            file = "FILEPATH")

    
}

##########################################################
## EXECUTE

main()

##########################################################
quit("no")


