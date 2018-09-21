##########################################################
## Aggregate ages/sexes
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

agggregateAges <- function(aggid, startid, endid, preddf, agepropdf, sexpropdf, gbdrid, agids) {
  # Get scaled age weights for group
  if (aggid != 27) {
    agepropdf <- agepropdf[age_group_id >= startid & age_group_id <= endid][, ageprop := ageprop / sum(ageprop), by = c("sdi", "sex_id", "sim")]
    agedf <- merge(preddf,
                   agepropdf,
                   by = c("sdi", "age_group_id", "sex_id", "sim", "draw"))
    agesexpropdf <- merge(agepropdf,
                          sexpropdf,
                          by = c("sdi", "age_group_id", "sex_id", "sim", "draw"))
  } else {
    agepropdf <- et.getAgeWeights(gbdrid, agids)
    agepropdf <- rename(agepropdf, c("age_group_weight_value" = "ageprop"))
    agedf <- merge(preddf,
                   agepropdf,
                   by = "age_group_id")
    agesexpropdf <- merge(agepropdf,
                          sexpropdf,
                          by = "age_group_id")
  } 
  
  # Combine aggregate
  agedf <- agedf[, age_group_id := aggid][, .(pred = sum(pred * ageprop, na.rm = TRUE)), by = eval(names(agedf)[!names(agedf) %in% c("pred", "ageprop")])]
  
  # Get age-sex weight
  agesexpropdf <- agesexpropdf[, agesexprop := (ageprop * sexprop) / sum(ageprop * sexprop), by = c("sdi", "sim")][, c("ageprop", "sexprop") := .(NULL, NULL)]
  agesexdf <- merge(preddf,
                    agesexpropdf,
                    by = c("sdi", "age_group_id", "sex_id", "sim", "draw"))
  agesexdf <- agesexdf[, c("age_group_id", "sex_id") := .(aggid, 3)][, .(pred = sum(pred * agesexprop, na.rm = TRUE)), by = eval(names(agesexdf)[!names(agesexdf) %in% c("pred", "agesexprop")])]
  
  # Combine and return
  df <- rbind(agedf, agesexdf)
  return(df[, names(preddf), with = FALSE])
}

main <- function() {
  # Parse arguments
  parser <- ArgumentParser()
  parser$add_argument("--data_dir", help = "Site where data will be stored",
                      dUSERt = "FILEPATH", type = "character")
  parser$add_argument("--agg_step", help = "Which step should be read in to aggregate",
                      dUSERt = "wHIV", type = "character")
  parser$add_argument("--etmtid", help = "Model type ID",
                      dUSERt = 1, type = "integer")
  parser$add_argument("--etmvid", help = "Model version ID",
                      dUSERt = 24, type = "integer")
  parser$add_argument("--etmvid_age", help = "Model version ID for population (age)",
                      dUSERt = 7, type = "integer")
  parser$add_argument("--etmvid_sex", help = "Model version ID for population (sex)",
                      dUSERt = 7, type = "integer")
  parser$add_argument("--agids", help = "Age group IDs",
                      dUSERt = c(2:20, 30:32, 235), nargs = "+", type = "integer")
  parser$add_argument("--sids", help = "Sex IDs",
                      dUSERt = 1:2, nargs = "+", type = "integer")
  parser$add_argument("--gbdrid", help = "GBD round ID",
                      dUSERt = 4, type = "integer")
  parser$add_argument("--gbdid", help = "GBD ID (cause/risk/etc)",
                      dUSERt = 294, type = "integer")
  args <- parser$parse_args()
  list2env(args, environment()); rm(args)

  # Load each data frame (rescale age and sex proportions)
  preddf <- et.getProduct(etmtid, etmvid,
                          data_dir,
                          agids, sids, gbdids = gbdid,
                          process_dir = agg_step)
  preddf$ax_mx <- NULL
  preddf$lmpred <- NULL
  
  dfcols <- names(preddf) 
  agepropdf <- et.getProduct(etmtid = 3, etmvid = etmvid_age,
                             data_dir,
                             agids, sids, gbdids = 0,
                             process_dir = "fits")
  agepropdf <- agepropdf[, ageprop := pred / sum(pred), by = .(sdi, sex_id, sim, draw)][, c("sdi", "age_group_id", "sex_id", "draw", "sim", "ageprop"), with = FALSE]
  agepropdf <- agepropdf[, .(ageprop = mean(ageprop), draw = 1, sim = 1), by = eval(grep("ageprop|draw|sim", names(agepropdf), invert = T, value = T))]
  
  
  sexpropdf <- et.getProduct(etmtid = 4, etmvid = etmvid_sex,
                             data_dir,
                             agids, sids, gbdids = 0,
                             process_dir = "fits")
  sexpropdf <- sexpropdf[, sexprop := pred / sum(pred), by = .(sdi, age_group_id, draw, sim)][, c("sdi", "age_group_id", "sex_id", "draw", "sim", "sexprop"), with = FALSE]
  sexpropdf <- sexpropdf[, .(sexprop = mean(sexprop), draw = 1, sim = 1), by = eval(grep("sexprop|draw|sim", names(sexpropdf), invert = T, value = T))]
  
  # Aggregate sexes by age

  if (etmtid == 1) preddf <- preddf[, .(pred = mean(pred), draw = 1, sim = 1), by = .(sdi, age_group_id, sex_id)]
  if (etmtid == 1) preddf[, c("etmodel_type_id", "etmodel_version_id", "measure_id", "metric_id", "cause_id") := .(1, 23, 1, 3, 0)]
  
  bsdf <- merge(preddf,
                sexpropdf,
                by = c("sdi", "age_group_id", "sex_id", "sim", "draw"), all.x = T)
  
  bsdf <- bsdf[, sex_id := 3][, .(pred = sum(pred * sexprop, na.rm = TRUE)), by = eval(names(bsdf)[!names(bsdf) %in% c("pred", "sexprop")])]
  
  # Aggregate ages (and age-sex multivariate aggregate)
  aggmapdf <- fread("FILEPATH")
  aggagedf <- rbindlist(mapply(aggid = aggmapdf$agg_age_group_id,
                               startid = aggmapdf$age_group_id_start,
                               endid = aggmapdf$age_group_id_end,
                               agggregateAges,
                               MoreArgs = list(preddf, 
                                               agepropdf, 
                                               sexpropdf,
                                               gbdrid,
                                               agids),
                               SIMPLIFY = FALSE))
  

  # Store each age/sex combination that has been created
  df <- rbind(bsdf,
              aggagedf)
  df <- df[, dfcols, with = FALSE]
  dfs <- split(df, paste0(df$age_group_id, df$sex_id))
  rm(df)
  mclapply(dfs,
         function(df, data_dir, etmtid, etmvid, agg_step, gbdid) {
           df <- data.table(df)
           agid <- unique(df$age_group_id)
           sid <- unique(df$sex_id)
           saveRDS(df,
                   file = "FILEPATH")
         },
         data_dir, etmtid, etmvid, agg_step, gbdid, mc.cores = et.coreNum())
}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################
