##########################################################
## Calculates Expected HALE with Epi Transition Outputs
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
parser$add_argument("--etmvid_lt", help = "Expected Lifetable version",
                    dUSERt = 24, type = "integer")
parser$add_argument("--etmvid_yld", help = "Expected YLD version",
                    dUSERt = 12, type = "integer")
parser$add_argument("--agids", help = "Age group IDs",
                    dUSERt = c(28, 5:20, 30:32, 235), type = "integer")
parser$add_argument("--sids", help = "Sex IDs",
                    dUSERt = 1:3, type = "integer")
args <- parser$parse_args()

list2env(args, environment()); rm(args)

main <- function() {
    
    ylddf <- et.getProduct(etmtid = 6, etmvid = etmvid_yld,
                           data_dir,
                           agids, sids, gbdids = 294,
                           process_dir = "raked")
    setnames(ylddf, "pred", "pred_yld")
    
    ltdf <- fread("FILEPATH")
    setnames(ltdf, "pred", "pred_ex")
    
    agesdf <- et.getAgeMetadata(agids = agids)
    
    haledf <- merge(ylddf[,  c("sdi","age_group_id","sex_id","pred_yld"), with=FALSE],
                    ltdf[, c("sdi","age_group_id","sex_id","nLx","lx_mx","pred_ex"), with=FALSE],
                    by=c("sdi","age_group_id","sex_id"))
    
    haledf <- merge(haledf, agesdf[, c("age_group_id", "age_group_years_start")], by = "age_group_id")
    
    ## Calculate HALE and save
    calcHALE <- function(agys, haledf) {
        agehaledf <- haledf[age_group_years_start >= agys, pred_hale := sum(nLx * (1 - pred_yld)) / lx_mx, by=c("sdi","sex_id")]
        agehaledf <- rename(agehaledf, c("pred_hale" = "pred"))
        agehaledf <- agehaledf[, measure_id := 28][, metric_id := 5][age_group_years_start == agys, c("sdi","age_group_id","sex_id","measure_id","metric_id","pred"), with=FALSE]
        return(agehaledf)
    }
    
    haledf <- rbindlist(lapply(as.list(unique(haledf$age_group_years_start)), calcHALE, haledf))
    
    write.csv(haledf, 
              file="FILEPATH", 
              row.names=FALSE)

}

## EXECUTE
main()

## EXIT
q("no")