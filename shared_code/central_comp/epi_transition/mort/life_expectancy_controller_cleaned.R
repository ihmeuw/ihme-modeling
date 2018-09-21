##########################################################
## Prepare files for life expectancy calculation, then run that script
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
  } else "FILEPATH"
  return(dir)
}
source(paste0(currentDir(), "/primer.R"))

loadAllCauseMort <- function(data_dir, etmvid, gbdrid) {
  # Load draws
  sids <- c(1, 2, 3)
  if (gbdrid == 4) agids <- c(28, 5:20, 30:32, 235)
  df <- et.getProduct(etmtid = 1, etmvid,
                      data_dir,
                      agids, sids, gbdids = 294,
                      process_dir = "wHIV")
  
  df <- df[, .(pred = mean(pred), draw = 1, sim = 1), by = eval(names(df)[!names(df) %in% c("draw", "sim", "pred")])]
  
  # Reshape wide
  df <- dcast(df,
              sdi + age_group_id + sex_id + draw ~ paste0("mx_", sim),
              value.var = "pred")
  
  # Add age start/end
  amdf <- et.getAgeMetadata(agids = agids)
  df <- merge(df,
              amdf[, c("age_group_id", "age_group_years_start", "age_group_years_end"), with = FALSE],
              by = "age_group_id")
  
  # Return formatted dataset
  return(df[, c("sdi", "age_group_id", "age_group_years_start", "age_group_years_end", "sex_id", "draw", paste0("mx_", 1)), with = FALSE]) 
}

loadPops <- function(data_dir, etmvid_age, etmvid_sex, gbdrid) {
  # Load data and scale to 1
  sids <- c(1, 2)
  if (gbdrid == 4) agids <- c(2:20, 30:32, 235)
  adf <- et.getProduct(etmtid = 3, etmvid = etmvid_age,
                       data_dir, agids, sids, gbdids = 0, process_dir = "fits")
  adf[, apred := pred / sum(pred), by = c("sdi", "sex_id", "draw", "sim")] 
  adf <- adf[, .(apred = mean(apred), draw = 1, sim = 1), by = eval(names(adf)[!names(adf) %in% c("apred", "draw", "sim", "pred", "lmpred")])]
  
  
  sdf <- et.getProduct(etmtid = 4, etmvid = etmvid_sex,
                       data_dir, agids, sids, gbdids = 0, process_dir = "fits")
  sdf[, spred := pred / sum(pred), by = c("sdi", "age_group_id", "draw", "sim")]
  sdf <- sdf[, .(spred = mean(spred), draw = 1, sim = 1), by = eval(names(sdf)[!names(sdf) %in% c("spred", "draw", "sim", "pred", "lmpred")])]
  
  # Get age-specific pops
  df <- merge(adf[, c("sdi", "age_group_id", "sex_id", "sim", "apred"), with = FALSE],
              sdf[, c("sdi", "age_group_id", "sex_id", "sim", "spred"), with = FALSE],
              by = c("sdi", "age_group_id", "sex_id", "sim"))
  df <- df[, pop := apred * spred * 1e8]
  
  # Make both sexes
  bsdf <- data.table(df)
  bsdf <- bsdf[, sex_id := 3][, .(pop = sum(pop)), by = .(sdi, age_group_id, sex_id, sim)]
  df <- rbind(df[, c("sdi", "age_group_id", "sex_id", "sim", "pop"), with = FALSE],
              bsdf[, c("sdi", "age_group_id", "sex_id", "sim", "pop"), with = FALSE])
  
  # Aggregate under 1
  df <- df[age_group_id %in% c(2, 3, 4), age_group_id := 28][, .(pop = sum(pop)), by = .(sdi, age_group_id, sex_id, sim)]
  
  # Reshape wide
  df <- dcast(df,
              sdi + age_group_id + sex_id ~ paste0("pop_", sim),
              value.var = "pop")
  
  # Assign pop variable
  return(df[, c("sdi", "age_group_id", "sex_id", paste0("pop_", 1)), with = FALSE]) # CHANGE WHEN HAVE MORE DRAWS AGAIN
}

splitSave <- function(df, data_dir, etmvid) {
  # Split by draw for parallel life table computation
  dfs <- split(df, df$draw)
  lapply(dfs, function(idf, data_dir, etmvid) {
                                                ddf <- data.table(idf)
                                                dnum <- unique(ddf$draw)
                                                ddf <- ddf[, draw := NULL]
                                                write.csv(ddf,
                                                          file = "FILEPATH",
                                                          row.names = FALSE)
                                              },
         data_dir, etmvid)
}

runLTCalc <- function(dnums, etmvid, data_dir) {
  # Call python script to create
  for (dnum in dnums) {
    qsub(jobname = paste0("epi_trans_lt_etmvid_", etmvid, "_draw_", dnum), 
         shell = "FILEPATH",
         code = paste0(currentDir(), "/life_expectancy.py"), 
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
                                                                              ddf <- ddf[, ax_mx := apply(.SD, 1, mean), .SDcols = grep("ax_mx_", names(ddf))]
                                                                              ddf <- ddf[, lx_mx := apply(.SD, 1, mean), .SDcols = grep("lx_mx_", names(ddf))]
                                                                              ddf <- ddf[, mean_ex := apply(.SD, 1, mean), .SDcols = grep("ex_mx_", names(ddf))]
                                                                              ddf <- ddf[age_group_years_start == max(ddf$age_group_years_start), ax_mx := mean_ex]
                                                                              return(ddf[, c("sdi", "age_group_id", "sex_id", "draw", "nLx", "lx_mx", "ax_mx", grep("ex_mx_", names(ddf), value = TRUE)), with = FALSE])
                                                                            },
                         data_dir, etmvid,
                         mc.cores = et.coreNum(3)))
  
  # Reshape long and return
  df <- melt(df,
             id.vars = c("sdi", "age_group_id", "sex_id", "draw", "nLx", "lx_mx", "ax_mx"),
             measure.vars = grep("ex_mx_", names(df)),
             variable.name = "sim",
             value.name = "pred")
  df <- df[, sim := as.numeric(gsub("ex_mx_", "", sim))]
  df <- df[, c("measure_id", "metric_id") := .(26, 5)]
  return(df[, c("sdi", "age_group_id", "sex_id", "draw", "sim", "measure_id", "metric_id", "nLx", "lx_mx", "ax_mx", "pred"), with = FALSE])
}

main <- function() {
  # Parse arguments
  parser <- ArgumentParser()
  parser$add_argument("--data_dir", help = "Site where data will be stored",
                      dUSERt = "FILEPATH", type = "character")
  parser$add_argument("--etmvid", help = "Envelope model version ID",
                      dUSERt = 24
                      , type = "integer")
  parser$add_argument("--etmvid_age", help = "Pop model (age) version ID",
                      dUSERt = 7, type = "integer")
  parser$add_argument("--etmvid_sex", help = "Pop model (sex) version ID",
                      dUSERt = 7, type = "integer")
  parser$add_argument("--gbdrid", help = "GBD round ID",
                      dUSERt = 4, type = "integer")
  args <- parser$parse_args()
  list2env(args, environment()); rm(args)
  
  print(etmvid)
  # Load combined dataset (wide)
  mxdf <- loadAllCauseMort(data_dir, etmvid, gbdrid)
  
  # Load pop dataset
  popsdf <- loadPops(data_dir, etmvid_age, etmvid_sex, gbdrid)
  
  # Save these files
  dir.create("FILEPATH", recursive = TRUE)
  dir.create("FILEPATH")
  splitSave(mxdf, data_dir, etmvid)
  write.csv(popsdf,
            file = "FILEPATH",
            row.names = FALSE)
  
  # Run life table simulation
  ltdf <- runLTCalc(dnums = unique(mxdf$draw), etmvid, data_dir)
  for (var in c("age_group_id", "sex_id", "draw", "sim", "measure_id", "metric_id")) {
    ltdf[[var]] <- as.integer(ltdf[[var]])
  }
  
  # Save
  ltdfs <- split(ltdf, paste0(ltdf$age_group_id, ltdf$sex_id))
  rm(ltdf)
  lapply(ltdfs,
         function(df, data_dir, etmvid) {
           df <- data.table(df)
           agid <- unique(df$age_group_id)
           sid <- unique(df$sex_id)
           saveRDS(df,
                   file = "FILEPATH")
         }, data_dir, etmvid)
  
  # Save summary
  ltdfs <- rbindlist(lapply(ltdfs,
                            function(df) {
                                df <- data.table(df)
                                df <- df[, lapply(.SD, mean), by = .(sdi, age_group_id, sex_id, measure_id, metric_id),
                                         .SDcols = c("nLx", "lx_mx", "ax_mx", "pred")]
                                return(df)
                            }))
  
  write.csv(ltdfs,
            file = "FILEPATH")
}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################
