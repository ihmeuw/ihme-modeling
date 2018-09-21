##########################################################
## Epi transition summarizer
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
  } else dir <- paste0(h, "/epi_transition/") 
  return(dir)
}
source(paste0(currentDir(), "/primer.R"))

getProductIFEXIST <- function(etmtid, etmvid,
                              data_dir,
                              agids, sids, gbdids,
                              process_dir) {
  # Read output of a given epi transition model (right now all files for the specified ages and sexes)
  argsdf <- expand.grid(age_group_id = agids, sex_id = sids, gbd_id = gbdids)
  df <- rbindlist(mapply(agid = argsdf$age_group_id,
                         sid = argsdf$sex_id,
                         gbdid = argsdf$gbd_id,
                         function(agid, sid, gbdid, data_dir, etmtid, etmvid, process_dir) {
                                                                                             if (file.exists("FILEPATH")) {
                                                                                               fildf <- readRDS("FILEPATH")
                                                                                               fildf$ax_mx <- NULL
                                                                                               return(fildf)
                                                                                             }
                                                                                             else {
                                                                                               print(paste0("File does not exist: ", "FILEPATH"))
                                                                                               return(data.table())
                                                                                             }
                                                                                           },
                         MoreArgs = list(data_dir, etmtid, etmvid, process_dir),
                         SIMPLIFY = FALSE),
                  fill = TRUE)
  return(df)
}

main <- function() {
  # Parse arguments
  parser <- ArgumentParser()
  parser$add_argument("--data_dir", help = "Site where data will be stored",
                      dUSERt = "FILEPATH", type = "character")
  parser$add_argument("--etmtid", help = "Model type ID",
                      dUSERt = 7, type = "integer")
  parser$add_argument("--etmvid", help = "Model version ID",
                      dUSERt = 2, type = "integer")
  parser$add_argument("--summ_steps", help = "Which parent step should be read in to make YLLs",
                      dUSERt = c("raked"), nargs = "+", type = "character")
  parser$add_argument("--agids", help = "Age group IDs",
                      dUSERt = c(2:20, 30:32, 235, 22, 27, 28), nargs = "+", type = "integer")
  parser$add_argument("--sids", help = "Sex IDs",
                      dUSERt = 1:3, nargs = "+", type = "integer")
  parser$add_argument("--gbdid", help = "GBD ID (cause/risk/etc)",
                      dUSERt = 294, type = "integer")
  parser$add_argument("--ystart", help = "Year IDs",
                      dUSERt = 1980, type = "integer")
  args <- parser$parse_args()
  list2env(args, environment()); rm(args)
  
  # Load data and summarize
  if (etmtid == 5 & gbdid == 294) {
    summ_steps <- c("wHIV", "YLLs")
    etmtids <- c(1, etmtid)
    etmvids <- c(23, etmvid)
  } else {
    etmtids <- c(etmtid, etmtid)
    etmvids <- c(etmvid, etmvid)
  }
  df <- rbindlist(mapply(process_dir = as.list(summ_steps),
                         etmtid = as.list(etmtids),
                         etmvid = as.list(etmvids),
                         getProductIFEXIST,
                         MoreArgs = list(data_dir = data_dir, agids = agids, sids = sids, gbdids = gbdid),
                         SIMPLIFY = FALSE),
                  fill = TRUE)
  df <- df[, .(mean = mean(pred, na.rm = TRUE), lower = quantile(pred, 0.025, na.rm = TRUE), upper = quantile(pred, 0.975, na.rm = TRUE)), by = eval(names(df)[!names(df) %in% c("draw", "sim", "pred", "ax_mx")])]
  
  if (etmtid == 5 & gbdid == 294) df[, cause_id := gbdid] ## ERRONEOUSLY CODED AS CAUSE ID 0 
  
  # Save
  write.csv(df,
            file = paste0(data_dir, "/t", etmtid, "/v", etmvid, "/summaries/gbdid_", gbdid, ".csv"),
            row.names = FALSE)
  
  # Create location year summaries with only age-standardized and all-age results
  sdilydf <- readRDS(file = "FILEPATH")
  locsdf <- readRDS(file = "FILEPATH")
  
  newround <- function(x, base) base*round(x/base)
  
  sdilydf[, sdi := newround(sdi, .005)]
  
  sdilydf[, sdi := round(sdi, 3)] # ensures proper merge
  df[, sdi := round(sdi, 3)]
  
  
  df <- merge(df[age_group_id %in% c(22,27)], sdilydf[year_id >= 1980, list(location_id, year_id, sdi)], by = "sdi", allow.cartesian = T)
  
  # Save
  write.csv(df,
            file = paste0(data_dir, "/t", etmtid, "/v", etmvid, "/summaries_ly/gbdid_", gbdid, "_ly.csv"),
            row.names = F)
}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################
