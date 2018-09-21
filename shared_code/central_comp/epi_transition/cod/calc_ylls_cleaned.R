
##########################################################
## Calculate YLLs
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

loadDeaths <- function(data_dir, etmtid, etmvid, agid, sid, gbdid, etmvid_lt) {
  # Load deaths
  if (etmtid == 5 & gbdid == 294) df <- readRDS("FILEPATH") 
  
  else df <- readRDS("FILEPATH")
  
  # Load ax
  if (!agid %in% c(2, 3, 4)) axdf <- readRDS("FILEPATH")
  else {
    axdf <- readRDS("FILEPATH")
    axdf <- axdf[, age_group_id := agid]
    axdf <- axdf[age_group_id == 2, ax_mx := ax_mx * (6 / 365)][age_group_id == 3, ax_mx := ax_mx * (21 / 365)][age_group_id == 4, ax_mx := ax_mx * (337 / 365)]
  }
  axdf <- axdf[, .(ax_mx = mean(ax_mx)), by = .(sdi, age_group_id, sex_id)]
  
  # Attach ax
  df <- merge(df,
              axdf,
              by = c("sdi", "age_group_id", "sex_id"))
  
  # Attach years to get age at death
  amdf <- et.getAgeMetadata(agids = agid)
  df <- merge(df,
              amdf[, c("age_group_id", "age_group_years_start"), with = FALSE],
              by = "age_group_id")
  df <- df[, age := round(age_group_years_start + ax_mx, 2)]
  df <- df[, age_group_years_start := NULL][, ax_mx := NULL]
  return(df)
}

main <- function() {
  # Parse arguments
  parser <- ArgumentParser()
  parser$add_argument("--data_dir", help = "Site where data will be stored",
                      dUSERt = "FILEPATH", type = "character")
  parser$add_argument("--etmtid", help = "Model type ID",
                      dUSERt = 5, type = "integer")
  parser$add_argument("--etmvid", help = "Model version ID",
                      dUSERt = 23, type = "integer")
  parser$add_argument("--etmvid_lt", help = "Life table model version ID",
                      dUSERt = 23, type = "integer")
  parser$add_argument("--yll_step", help = "Which parent step should be read in to make YLLs",
                      dUSERt = "raked", type = "character")
  parser$add_argument("--agids", help = "Age group IDs",
                      dUSERt = c(2:20, 30:32, 235), nargs = "+", type = "integer")
  parser$add_argument("--sids", help = "Sex IDs",
                      dUSERt = 1:2, nargs = "+", type = "integer")
  parser$add_argument("--gbdid", help = "GBD ID (cause/risk/etc)",
                      dUSERt = 495, type = "integer")
  args <- parser$parse_args()
  list2env(args, environment()); rm(args)
  
  # Read in theoretical minimum risk life table
  tmrltdf <- fread("FILEPATH")
  
  # Loop through ages and sexes
  for (agid in agids) {
    for (sid in sids) {
      # Read in death data
      print(paste0("Age group ", agid, ", sex ", sid))
      deathsdf <- loadDeaths(data_dir, etmtid, etmvid, agid, sid, gbdid, etmvid_lt)
      
      # Make YLLs
      ylldf <- merge(deathsdf,
                     tmrltdf,
                     by = "age")
      ylldf <- ylldf[, pred := pred * Pred_ex][, Pred_ex := NULL][, age := NULL]
      ylldf <- ylldf[, measure_id := 4]
      
      # Save
      ylldf <- ylldf[order(sdi, draw, sim)]
      saveRDS(ylldf,
              file = paste0("FILEPATH"))
      rm(sid)
    }
    rm(agid)
  }
}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################
