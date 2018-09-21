##########################################################
## Functions for data retrieval
##########################################################
# Dependencies

et.envelopeGrab <- function(lids, yids, agids, sids, mvid) {
  # Load envelope
  df <- rbindlist(mclapply(as.list(lids),
                           function(lid, yids, agids, sids) {
                             ldf <- fread("FILEPATH")
                             ldf <- ldf[, location_id := lid]
                             ldf <- ldf[year_id %in% yids & age_group_id %in% agids & sex_id %in% sids,]
                             return(ldf)
                           },
                           yids, agids, sids,
                           mc.cores = et.coreNum()))
  df <- df[, c("measure_id", "metric_id") := .(1, 1)]
  df <- melt(df,
             id.vars = c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id"),
             measure.vars = paste0("draw_", 0:999),
             variable.name = "draw",
             value.name = "val")
  df <- df[, draw := as.numeric(gsub("draw_", "", draw))]
  df <- df[, draw := round_any(draw, 10, f = floor) / 10]
  df <- df[, .(val = mean(val)), by = .(location_id, year_id, age_group_id, sex_id, measure_id, metric_id, draw)]
  return(df[order(location_id, year_id, age_group_id, sex_id, draw), c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id", "draw", "val"), with = FALSE])
}

et.BurdenGrab <- function(lsid, lids, yids, agids, sids, gbdids, etmtid, mvid, gbdrid) {
  # Load cause model
  if (etmtid == 5) {
    s <- "codcorrect"
    measid <- 1
    metid <- 3
    f <- "cause_id"
  }
  if (etmtid == 6) {
    s <- "como"
    measid <- 3
    metid <- 3
    f <- "cause_id"
  }
    

    if (etmtid == 5) yids <- "full"
        
        df <- get_outputs("cause",
                        location_set_id = lsid,
                        location_id = lids,
                        year_id = yids,
                        sex_id = sids,
                        age_group_id = agids,
                        compare_version = mvid,
                        measure_id = measid,
                        metric_id = metid,
                        cause_id = gbdids,
                        gbd_round_id = gbdrid
        )
        

    return(df[order(location_id, year_id, age_group_id, sex_id), c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id", "cause_id", "val", "upper", "lower")])
    
    
    


}

et.popStructure <- function(data_dir, etmtid, etmvid, agids, sids) {
  df <- readRDS("FILEPATH")
  if (etmtid == 3) df <- df[, val := population / sum(population), by = c("location_id", "year_id", "sex_id")]
  if (etmtid == 4) df <- df[, val := population / sum(population), by = c("location_id", "year_id", "age_group_id")]
  df <- df[, c("draw", "measure_id", "metric_id", "population") := .(0, 18, 2, NULL)]
  df <- df[age_group_id %in% agids & sex_id %in% sids]
  return(df)
}



et.SummaryGrab <- function(lids, yids, agids, sids, gbdids, etmtid, gbdrid) {

  df <- rbindlist(lapply(as.list(yids), 
                         function(yid, field, table, measid, metid, lids, sids, agids, gbdids) {
                           yeardf <- et.dbQuery("SQL QUERY")
                           return(yeardf)
  }, field, table, measid, metid, lids, sids, agids, gbdids))
  df$draw <- 0
  return(df)
}

et.getDraws <- function(data_dir,
                        etmtid, etmvid, mvid, lsid,
                        lids, yids, agids, sids,
                        gbdids,
                        gbdrid, 
                        popsdf,
                        ...) {
  # Make wrapper for get_draws
  if (etmtid == 1) {
    df <- et.envelopeGrab(lids, yids, agids, sids, mvid)

  }

  if (etmtid %in% c(3, 4)) df <- et.popStructure(data_dir, etmtid, etmvid, agids, sids)

  # Alter counts to become rates
  if (nrow(df) > 0) {
    if (unique(df$metric_id) == 1) {
      df <- merge(df,
                  popsdf[, c("location_id", "year_id", "age_group_id", "sex_id", "population"), with = FALSE],
                  by = c("location_id", "year_id", "age_group_id", "sex_id"))
      df <- df[, c("metric_id", "val") := .(3, val / population)]
      df <- df[population < 1, val := NA][, population := NULL] 
    }
  }
  
  dfs <- split(df, df[, !names(df) %in% c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id", "val"), with = FALSE])
  return(dfs)
}

et.getOutputs <- function(data_dir, 
                         etmtid, etmvid, mvid, lsid,
                         lids, yids, agids, sids,
                         gbdids, 
                         gbdrid,
                         popsdf,
                         ...) {
    
    if (etmtid %in% c(3,4)) df <- et.popStructure(data_dir, etmtid, etmvid, agids, sids)

        

    
    if (etmtid %in% c(5,6)) df <- et.BurdenGrab(lsid, lids, yids, agids, sids, gbdids, etmtid, mvid, gbdrid)
            
    
    return(df)
}


et.getProduct <- function(etmtid, etmvid,
                          data_dir,
                          agids, sids, gbdids,
                          mean = F, process_dir,
                          ...) {
  
    # Read output of a given epi transition model (right now all files for the specified ages and sexes)
  argsdf <- expand.grid(age_group_id = agids, sex_id = sids, gbd_id = gbdids)
  df <- rbindlist(mapply(agid = argsdf$age_group_id,
                         sid = argsdf$sex_id,
                         gbdid = argsdf$gbd_id,
                         function(agid, sid, gbdid, data_dir, etmtid, etmvid, process_dir) {
                                                                                             if(mean == T) fildf <- readRDS("FILEPATH")
                                                                                             else fildf <- readRDS("FILEPATH")
                                                                                             
                                                                                             return(fildf)
                                                                                           },
                         MoreArgs = list(data_dir, etmtid, etmvid, process_dir),
                         SIMPLIFY = FALSE),
                  fill = TRUE)
  return(df)
}
