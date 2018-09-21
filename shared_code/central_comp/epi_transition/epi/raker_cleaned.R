##########################################################
## Epi transition raking job - Epi, scales from all-cause, sequentially down the hierarchy
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

loadNRake <- function(data_dir, etmtid, etmvid, lvls, agids, sids, pid, cids, rake_step) {
  # Load parent data frame
  if (etmtid == 5 & pid == 294) pdf <- et.getProduct(etmtid = 1, etmvid = 23, 
                                                     data_dir,
                                                     agids, sids, gbdids = 294,
                                                     process_dir = "wHIV")
  else if (etmtid != 5 & pid == 294 & lvls == 0) pdf <- et.getProduct(etmtid, etmvid,
                                                          data_dir,
                                                          agids, sids, gbdids = pid,
                                                          process_dir = "fits", mean = T)
  else pdf <- et.getProduct(etmtid, etmvid,
                            data_dir,
                            agids, sids, gbdids = pid,
                            process_dir = "raked")
  
  # Take mean of parent
  pdf <- pdf[, .(pred = mean(pred), draw = 1, sim = 1), by = eval(names(pdf)[!names(pdf) %in% c("pred", "draw", "sim")])]
  
  
  if ((nrow(pdf[!is.na(pred) & !is.infinite(pred)]) > 0) & lvls > 0) {
    # Format parent
    if(etmtid == 5 & pid == 294) pdf[, cause_id := 294] 
      
    pdf <- pdf[, c("etmodel_type_id", "etmodel_version_id") := .(NULL, NULL)]
    for (idcol in names(pdf)[!names(pdf) %in% c("age_group_id", "sex_id")]) {  
      if (all(unique(pdf[[idcol]]) == pid)) pdf[[idcol]] <- NULL
    }
    pdf <- rename(pdf, c("pred" = "pred_env"))
    
    # Load child data frames
    cdf <- et.getProduct(etmtid, etmvid,
                         data_dir,
                         agids, sids, gbdids = cids,
                         mean = T,
                         process_dir = rake_step)
    
    # Format children
    
    ## Setting floor on predictions by cause
    cdfs <- split(cdf, by = "cause_id")
    
    cdf <- rbindlist(lapply(cdfs, function(ddf) {
        
        ddf[, pred := round(exp(pred) - 1e-8, 16)]
        floor <- ddf[pred >= 0, .1*min(pred)]
        ddf[pred < 0, pred := floor]
        
        print(floor)
        
        return(ddf)
    }))
    

    ## Formatting variables
    cdf[, c("measure_id", "lmpred", "etmodel_type", "measure"):= NULL][, c("draw", "sim") := 1][, c("etmodel_type_id", "etmodel_version_id") := .(etmtid, etmvid)]
    if (etmtid == 5 & rake_step == "fits") cdf[, measure_id := 1]
    if (etmtid == 5 & rake_step == "YLLs") cdf[, measure_id := 4]
    if (etmtid == 6) cdf[, measure_id := 3]
    
    # Combine and scale
    df <- merge(cdf,
                pdf,
                by = names(pdf)[names(pdf) != "pred_env"])
    df <- df[pred > 0, pred := pred * (pred_env / sum(pred)), by = eval(names(pdf)[names(pdf) != "pred_env"])]
    df$pred_env <- NULL
    df$et_model_type <- NULL
    
    
    if(all(pdf$pred == 0)) df[, pred := 0] 
    
  } else df <- data.table(pdf[, .(sdi, age_group_id, sex_id, measure_id = measure, metric_id, draw, sim, etmodel_type_id = etmtid, etmodel_version_id = etmvid, cause_id, pred = exp(pred) - 1e-8)])
  
  return(df)
}

splitSave <- function(df, cids, data_dir, etmtid, etmvid, agid, sid) {

  # MAKE SUMMARIES
    summdf <- df[, .(pred = mean(pred)), by = eval(names(df)[!names(df) %in% c("draw", "sim", "pred")])]
    
  # SAVE DRAWS AND SUMMARIES
  for (cid in cids) {
      
    savesummdf <- data.table(summdf[cause_id == cid,])
    saveRDS(savesummdf,
            file = "FILEPATH")
      
    savedf <- data.table(df[cause_id == cid,])
    saveRDS(savedf,
            file = "FILEPATH")
  }
}

main <- function() {
  # Parse arguments
  parser <- ArgumentParser()
  parser$add_argument("--data_dir", help = "Site where data will be stored",
                      dUSERt = "FILEPATH", type = "character")
  parser$add_argument("--etmtid", help = "Model type ID",
                      dUSERt = 6, type = "integer")
  parser$add_argument("--etmvid", help = "Model version ID",
                      dUSERt = 11, type = "integer")
  parser$add_argument("--rake_step", help = "Which parent step should be read in to rake",
                      dUSERt = "fits", type = "character")
  parser$add_argument("--lvl", help = "Level of Cause Hierarchy",
                      dUSERt = 3, type = "integer")
  parser$add_argument("--pid", help = "Parent ID",
                      dUSERt = 301, type = "integer")
  parser$add_argument("--agid", help = "Age group ID",
                      dUSERt = 15, type = "integer")
  parser$add_argument("--sid", help = "Sex ID",
                      dUSERt = 1, type = "integer")

  args <- parser$parse_args()
  list2env(args, environment()); rm(args)
  
  
  # Load prerequisite data 
  causesdf <- readRDS("FILEPATH")
  causesdf <- causesdf[parent_id == pid & level == lvl]#cause_id != pid,]
  cids <- causesdf$cause_id
  
  # Load/rake data
  rakedf <- loadNRake(data_dir, etmtid, etmvid, lvls = lvl, agids = agid, sids = sid, pid, cids, rake_step)
  
  # Split and save
  splitSave(rakedf, cids, data_dir, etmtid, etmvid, agid, sid)
}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################
