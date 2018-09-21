##########################################################
## Prepare files for life expectancy calculation using location-year specific population structure, then run that script
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

loadPopsSDI <- function(lsid, agids, sids, gbdrid) {
  # Fetch population and SDI data
  popsdf <- get_population(location_id = -1, location_set_id = lsid, year_id = c(1970:2016), age_group_id = agids, sex_id = sids, gbd_round_id = gbdrid)
  popsdf <- rename(popsdf, c("population" = "pop"))
  
  # Since no population draws, repeat 100 times to preserve 100 sim output
  setnames(popsdf, "pop", "pop_1")
  popsdf[, paste0("pop_", seq(2,100)) := pop_1]
  
  sdidf <- get_covariate_estimates(covariate_name_short = "sdi")
  sdidf <- rename(sdidf, c("mean_value" = "sdi"))
  
  # Combine data frames
  df <- merge(popsdf[, c("location_id", "year_id", "age_group_id", "sex_id", grep("pop", names(popsdf), value = T)), with = FALSE],
              sdidf[, c("location_id", "year_id", "sdi"), with = FALSE],
              by = c("location_id", "year_id"))
  
  df <- df[, sdi := round(0.005 * round(sdi / 0.005), 3)]
  return(df[, c("location_id", "year_id", "sdi", "age_group_id", "sex_id", grep("pop", names(df), value = T)), with = FALSE])
}

pairSDI <- function(idf, data_dir, etmvid, amdf, popsdf) {
  # Combine with pops, aggregate to both sexes
  popsdf <- popsdf[sdi < min(idf$sdi), sdi := min(idf$sdi)][sdi > max(idf$sdi), sdi := max(idf$sdi)]
  df <- merge(popsdf[, c("location_id", "year_id", "sdi", "age_group_id", "sex_id", "pop_1"), with = F],
              idf,
              by = c("sdi", "age_group_id", "sex_id"),
              allow.cartesian = TRUE)
  bsdf <- data.table(df)
  
  # No pops "draws" so just use first, stored in singular saved popsdf
  bsdf <- bsdf[, sex_id := 3][, .(pred = sum(pred * pop_1) / sum(pop_1)), by = .(location_id, year_id, age_group_id, sex_id, draw, sim)]
  df <- rbind(df[, c("location_id", "year_id", "age_group_id", "sex_id", "draw", "sim", "pred"), with = FALSE],
              bsdf[, c("location_id", "year_id", "age_group_id", "sex_id", "draw", "sim", "pred"), with = FALSE])
  rm(bsdf)
  
  # Reshape wide
  df <- dcast(df,
              location_id + year_id + age_group_id + sex_id + draw ~ paste0("mx_", sim),
              value.var = "pred")
  
  # Add age start/end
  df <- merge(df,
              amdf[, c("age_group_id", "age_group_years_start", "age_group_years_end"), with = FALSE],
              by = "age_group_id")
  
  df <- df[, c("location_id", "year_id", "age_group_id", "age_group_years_start", "age_group_years_end", "sex_id", "draw", paste0("mx_", 1:max(idf$sim))), with = FALSE]
  dnum <- unique(df$draw)
  df <- df[, draw := NULL]
  write.csv(df,
            file = "FILEPATH",
            row.names = FALSE)
}

splitSave <- function(df, popsdf, data_dir, etmvid, agids) {
  # Split by draw for parallel life table computation
  df <- df[order(draw, sdi, age_group_id, sex_id, sim)]
  dfs <- split(df, df$draw)
  amdf <- et.getAgeMetadata(agids = agids)
  mclapply(dfs, 
           pairSDI, 
           data_dir, etmvid, amdf, popsdf,
           mc.cores = et.coreNum(3))
}

runLTCalc <- function(dnums, etmvid, data_dir) {
  # Call python script to create
  for (dnum in dnums) {
    qsub(jobname = paste0("epi_trans_ltly_etmvid_", etmvid, "_draw_", dnum), 
         shell = "FILEPATH",
         code = paste0(currentDir(), "/life_expectancy_ly.py"), 
         project = "epitrans",
         args = list("--in_path", "FILEPATH", 
                     "--o", "FILEPATH",
                     "--pop_path", "FILEPATH",
                     "--pop_prefix", "pop"),
         slots = 10)
  }
  
  # Wait for them to complete
  for (dnum in dnums) {
    while (!file.exists("FILEPATH")) {
      print(paste0("Waiting for draw ", dnum, "... ", Sys.time()))
      Sys.sleep(15)
    }
  }
  
  # Compile draws
    
    df <- rbindlist(mclapply(as.list(dnums), function(dnum, data_dir, etmvid) {
        
        ddf <- fread("FILEPATH")
        ddf <- ddf[age_group_years_start == 0 & age_group_years_end == 1, age_group_id := 28]
        ddf <- ddf[, draw := dnum]
        for (sim in 1:max(mxdf$sim)) ddf[age_group_years_start == max(ddf$age_group_years_start), paste0("ax_mx_", sim):= get(paste0("ex_mx_", sim))]
        ddf <- ddf[, c("measure_id", "metric_id") := .(26, 5)]
        return(ddf[, c("location_id", "year_id", "age_group_id", "sex_id", "draw", "measure_id", "metric_id", grep("lx_mx|ax_mx|ex_mx", names(ddf), value = T)), with = FALSE])
    },
    data_dir, etmvid,
    mc.cores = et.coreNum(3), mc.preschedule = F))
    
    # Collapse Sims to Produce Original Summary File
    df[, ax_mx := apply(.SD, 1, mean), .SDcols = grep("ax_mx_", names(df))]
    df[, lx_mx := apply(.SD, 1, mean), .SDcols = grep("lx_mx_", names(df))]
    df[, ex_mx := apply(.SD, 1, mean), .SDcols = grep("ex_mx_", names(df))]
    
    df <- df[, c("location_id", "year_id", "age_group_id", "sex_id", "draw", "measure_id", "metric_id", "lx_mx", "ax_mx", "ex_mx"), with = FALSE]
    
    
  
  # Take mean of draws and return
  df <- df[, lapply(.SD, mean), by = .(location_id, year_id, age_group_id, sex_id, measure_id, metric_id), .SDcols = c("lx_mx", "ax_mx", "ex_mx")]
  return(df[, c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id", "lx_mx", "ax_mx", "ex_mx"), with = FALSE])
}

main <- function() {
  # Parse arguments
  parser <- ArgumentParser()
  parser$add_argument("--data_dir", help = "Site where data will be stored",
                      dUSERt = "FILEPATH", type = "character")
  parser$add_argument("--etmvid", help = "Envelope model version ID",
                      dUSERt = 24, type = "integer")
  parser$add_argument("--agids", help = "Age groups",
                      dUSERt = c(28, 5:20, 30:32, 235), nargs = "+", type = "integer")
  parser$add_argument("--sids", help = "Base sexes",
                      dUSERt = c(1, 2), nargs = "+", type = "integer")
  parser$add_argument("--lsid", help = "Location set ID",
                      dUSERt = 35, type = "integer")
  parser$add_argument("--gbdrid", help = "GBD round ID",
                      dUSERt = 4, type = "integer")
  args <- parser$parse_args()
  list2env(args, environment()); rm(args)
  
  # Load pop dataset
  popsdf <- loadPopsSDI(lsid, agids, sids, gbdrid)
  
  popsdf <- popsdf[, .(location_id, year_id, sdi, age_group_id, sex_id, pop_1)]
  
  # Load combined dataset (wide)
  mxdf <- et.getProduct(etmtid = 1, etmvid,
                        data_dir,
                        agids, sids, gbdids = 294,
                        process_dir = "wHIV")
  
  mxdf <- mxdf[, .(pred = mean(pred), draw = 1, sim = 1), by = eval(names(mxdf)[!names(mxdf) %in% c("draw", "sim", "pred")])]
  
  # Save these files
  dir.create("FILEPATH", recursive = TRUE)
  dir.create("FILEPATH")
  splitSave(mxdf, popsdf, data_dir, etmvid, agids)
  write.csv(popsdf[, c("location_id", "year_id", "age_group_id", "sex_id", grep("pop",names(popsdf), value = T)), with = FALSE],
            file = "FILEPATH",
            row.names = FALSE)
  
  # Run life table simulation
  ltdf <- runLTCalc(dnums = unique(mxdf$draw), etmvid, data_dir)
  for (var in c("location_id", "year_id", "age_group_id", "sex_id", "measure_id", "metric_id")) {
    ltdf[[var]] <- as.integer(ltdf[[var]])
  }
  
  # Save means
  write.csv(ltdf,
            file = "FILEPATH",
            row.names = FALSE)
}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################
