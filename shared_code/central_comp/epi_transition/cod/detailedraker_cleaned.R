##########################################################
## Epi transition raking job for CoD- Uses only means, rakes most detailed to all-cause, then aggregates up 
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

loadNRakeDetailed <- function(data_dir, etmtid, etmvid, agids, sids, pids, rakeids, rake_step, causesdf) {

    # Load parent data frame - all cause
    if (etmtid == 5 & any(pids %in% 294)) pdf <- et.getProduct(etmtid = 1, etmvid = 23, 
                                                       data_dir,
                                                       agids, sids, gbdids = pids,
                                                       process_dir = "wHIV")
    else if (etmtid != 5) pdf <- et.getProduct(etmtid, etmvid,
                                                                        data_dir,
                                                                        agids, sids, gbdids = pids,
                                                                        mean = T, 
                                               process_dir = "fits")
    else pdf <- et.getProduct(etmtid, etmvid,
                              data_dir,
                              agids, sids, gbdids = pids,
                              process_dir = "raked")

    if (nrow(pdf[!is.na(pred) & !is.infinite(pred)]) > 0) {
        
        # Take mean of parent 
        if (length(pids) == 1) pdf <- pdf[, .(pred = mean(pred), draw = 1, sim = 1), by = eval(names(pdf)[!names(pdf) %in% c("pred", "draw", "sim")])]
        
        # Format parent
        if(etmtid == 5 & length(pids) == 1) pdf[, cause_id := 294]
        if(etmtid %in% c(5,6) & length(pids) > 1) pdf[, parent_id := cause_id]
        if(etmtid == 6) pdf$lmpred <- NULL
        if(etmtid == 6) setnames(pdf, "et_model_type", "etmodel_type_id")
        if(etmtid == 6) setnames(pdf, "measure", "measure_id")
        if(etmtid == 6) pdf[, pred := exp(pred) - 1e-8]
            
        
        pdf <- pdf[, c("etmodel_type_id", "etmodel_version_id", "cause_id", "level") := NULL]
        pdf <- rename(pdf, c("pred" = "pred_env"))
        
        # Load child data frames
        cdf <- et.getProduct(etmtid, etmvid,
                             data_dir,
                             agids, sids, gbdids = rakeids,
                             mean = T,
                             process_dir = rake_step)
        
        # Format children 
        
        ## Setting floor on predictions
        cdf[, pred := exp(pred) - 1e-8]
        floor <- cdf[pred >= 0, .1*min(pred)]
        cdf[pred < 0, pred := floor] 
        
        ## Formatting variables
        cdf[, c("measure_id", "lmpred", "etmodel_type"):= NULL][, c("draw", "sim") := 1][, c("etmodel_type_id", "etmodel_version_id") := .(etmtid, etmvid)]
        setnames(cdf, "measure", "measure_id")
        if (etmtid == 5) cdf[, measure_id := 1]
        if(etmtid == 6) cdf[, measure_id := 3]
        
        if (etmtid %in% c(5,6) & length(pids) > 1) cdf <- merge(cdf, causesdf[, .(cause_id, parent_id)], by = "cause_id")
        
        # Combine and scale
        df <- merge(cdf,
                    pdf,
                    by = names(pdf)[names(pdf) != "pred_env"])
        df <- df[, pred := pred * (pred_env / sum(pred)), by = eval(names(pdf)[names(pdf) != "pred_env"])]
        
        
        for (pid in unique(pdf$parent_id)) {
            
            if(all(pdf[parent_id == pid]$pred == 0)) df[parent_id == pid, pred := 0] ## IF PARENT IS ZERO ALL CHILDREN MUST NECESSARILY BE ZERO
            
        }
        
        df[, c("pred_env", "et_model_type", "parent_id") := NULL]
        
    } else df <- data.table(pdf)
    return(df)
}

aggregate <- function(df, lvl, causesdf) {
    
    cdf <- df[level == lvl]
    pdf <- cdf[, .(pred = sum(pred)), by = eval(names(cdf)[!names(cdf) %in% c("pred", "cause_id", "level")])]
    setnames(pdf, "parent_id", "cause_id")
    pdf <- merge(pdf, causesdf[, .(cause_id, parent_id, level)], by = "cause_id")
    
    df <- rbindlist(list(df, pdf), use.names = T, fill = T)
   
    return(df)
}


aggFromDetailed <- function(rakedf, causesdf) {
    
    ## ATTACH CAUSE METADATA
    aggdf <- merge(rakedf, causesdf[, .(cause_id, level, parent_id)], by = "cause_id")
    
    for (lvl in max(causesdf$level):min(causesdf$level)) {
        
        aggdf <- aggregate(aggdf, lvl, causesdf)
        
    }
    
    aggdf[, c("level", "parent_id") := NULL]

    
    return(aggdf)
    
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
                        dUSERt = 7, type = "integer")
    parser$add_argument("--rake_step", help = "Which parent step should be read in to rake",
                        dUSERt = "fits", type = "character")
    parser$add_argument("--agid", help = "Age group ID",
                        dUSERt = 9, type = "integer")
    parser$add_argument("--sid", help = "Sex ID",
                        dUSERt = 1, type = "integer")
    
    args <- parser$parse_args()
    list2env(args, environment()); rm(args)
    
    
    # Load prerequisite data 
    causesdf <- readRDS("FILEPATH")
    rakeids <- causesdf[most_detailed == T]$cause_id
    
    # Load/rake most-detailed to all-cause
    rakedf <- loadNRakeDetailed(data_dir, etmtid, etmvid, agids = agid, sids = sid, pids = 294, rakeids, rake_step, causesdf)
    
    # Aggregate up hierarchy
    aggdf <- aggFromDetailed(rakedf, causesdf)
    
    # Save
    splitSave(aggdf, unique(aggdf$cause_id), data_dir, etmtid, etmvid, agid, sid)
    

}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################
