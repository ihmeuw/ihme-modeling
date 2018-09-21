##########################################################
## Epi transition controller 
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

main <- function() {
  # Parse arguments
  library(argparse)
  parser <- ArgumentParser()
  parser$add_argument("--data_dir", help = "Site where data will be stored",
                      dUSERt = "FILEPATH", type = "character")
  parser$add_argument("--etmtid", help = "Model type ID",
                      dUSERt = 7, type = "integer")
  parser$add_argument("--etmvid", help = "Model version ID",
                      dUSERt = 2, type = "integer")
  parser$add_argument("--etftid", help = "fit type ID: 1-Splines, 2-GPR",
                      dUSERt = 2, type = "integer")
  parser$add_argument("--basemethid", help = "Prior for GPR", 
                      dUSERt = 2, type = "integer")
  parser$add_argument("--nbasis", help = "Number of internal mesh points for GPR", 
                      dUSERt = 2, type = "integer")
  args <- parser$parse_args()
  list2env(args, environment()); rm(args)
  
  ## KEY FOR TYPES:
  # 1) Envelope
  # 2) HIV - no longer relevant for GBD 2016
  # 3) Pop age
  # 4) Pop sex
  # 5) CoD
  # 6) YLDs
  # 7) DALYs/HALE (only calculated outputs)


  # For now, manually define demographics, etc (will come from SQLite tables)
  gbdrid <- 4
  lsid <- 35
  
  if (etmtid == 5) csid <- 3 
  if (etmtid == 6) csid <- 9
  
  if (gbdrid == 4) {
    if (etmtid %in% c(1, 3, 4)) yids <- 1970:2016
    if (etmtid == 2) yids <- 1980:2016
    if (etmtid %in% c(5, 7)) yids <- 1980:2016
  }
  if (etmtid == 6) yids <- c(seq(1990, 2010, 5), 2016)
  
  if (gbdrid == 3) yids <- seq(1990, 2015, 5)
  if (gbdrid == 4) agids <- c(2:20, 30:32, 235)
  if (gbdrid == 3) agids <- 2:21
  sids <- c(1, 2)
  if (etmtid == 7) sids <- 1:3
  
  # Make root version directory
  dir.create(paste0(data_dir, "/t", etmtid, "/v", etmvid))
  
  # Get model versions (will come from SQLite tables)
  if (gbdrid == 4) sdimvid <- get_best_model_versions(entity = "covariate", ids = 881, gbd_round_id = gbdrid)$model_version_id
  if (gbdrid == 3) sdimvid <- 4814
  if (etmtid == 1) mvid <- as.numeric(et.dbQuery("SQL QUERY"))
  if (etmtid == 2) mvdf <- get_best_model_versions(entity = "cause", ids = 298, gbd_round_id = gbdrid)
  if (etmtid %in% c(3, 4)) mvid <- as.numeric(et.dbQuery("SQL QUERY"))

  if (etmtid == 5) mvid <- 194
  if (etmtid == 6) mvid <- 200
  
  # Store location metadata for parallel use
  locsdf <- get_location_metadata(location_set_id = lsid, gbd_round_id = gbdrid)
  saveRDS(locsdf,
          file = "FILEPATH")
  
  # Store cause metadata for parallel use
  if (etmtid %in% c(1, 3, 4)) gbdids <- 0
  if (etmtid == 2) gbdids <- 298
  if (etmtid %in% c(5, 6, 7)) {
    causesdf <- get_cause_metadata(cause_set_id = csid, gbd_round_id = gbdrid)
    causesdf <- causesdf[cause_id != 740]
    if (etmtid == 5) causesdf <- causesdf[!cause_id %in% c(294) & is.na(yld_only)]
    else if (etmtid == 6) causesdf <- causesdf[is.na(yll_only),]
    saveRDS(causesdf,
            file = "FILEPATH")
    gbdids <- causesdf$cause_id
  }

  
  # Store population for parallel use
  popsdf <- get_population(year_id = yids, sex_id = sids, age_group_id = agids, gbd_round_id = gbdrid, location_id = -1, location_set_id = lsid)
  
  # Pops location specificity needs to match the locs - India, Bra, China States only
  popsdf <- popsdf[location_id %in% c(locsdf[ihme_loc_id %like% "BRA|CHN" & most_detailed == 1, location_id],
                                      locsdf[ihme_loc_id %like% "IND" & level == 4, location_id],
                                      locsdf[!ihme_loc_id %like% "BRA|CHN|IND" & level == 3, location_id]),
                   c("location_id", "year_id", "age_group_id", "sex_id", "population"), with = FALSE]
  

  saveRDS(popsdf,
          file = paste0(data_dir, "/t", etmtid, "/v", etmvid, "/popsdf.RDs"))
  
  # Store SDI for parallel use
  if (gbdrid == 4) sdidf <- get_covariate_estimates(covariate_name_short = "sdi")
  if (gbdrid == 3) {
    sdidf <- et.dbQuery("SQL QUERY")
  }

  sdidf <- sdidf[location_id %in% c(locsdf[ihme_loc_id %like% "BRA|CHN|USA" & most_detailed == 1, location_id],
                                    locsdf[ihme_loc_id %like% "IND" & level == 4, location_id],
                                    locsdf[!ihme_loc_id %like% "BRA|CHN|IND" & level == 3, location_id]) &
                     year_id %in% yids, 
                c("model_version_id", "location_id", "year_id", "mean_value"), with = FALSE]
  

  sdidf <- rename(sdidf, c("mean_value" = "sdi"))
  saveRDS(sdidf,
          file = "FILEPATH")
  
  # Fit models
  dir.create("FILEPATH")
  
  if (etmtid %in% c(3, 4)) {
    sl <- 12
    m <- 6
  }  else if (etmtid %in% c(5, 6)) {
    sl <- 10
    m <- 6
  } else if (etmtid %in% c(1,2)) {
    sl <- 6
    m <- 12
  } else {
    sl <- 15
    m <- 30
  }
 
  if (!etmtid %in% c(7)) {
    if (etftid == 1) cd <- paste0(currentDir(), "/fit.R")
    if (etftid == 2) cd <- paste0(currentDir(), "/fit_inla_gpr.r")
    priormetadf <- fread("FILEPATH")
    usint <- 0 
    usnat <- 1
    if (etftid == 1) write(paste0("SPLINE FIT USING GBD 2015 DUMMY COVARIATES (US, C ASIA, E EUROPE, OCEANIA)"),
                           file = "FILEPATH")
    
    meshrange <- c(.3, .7)
    
    if (etftid == 2) write(paste0("GPR FIT IN INLA USING ", priormetadf$prior_type[priormetadf$methid == basemethid], " PRIOR\n
                 US DUMMY COVARIATE: ", as.logical(usint),
                 "US NATIONAL INSTEAD OF STATES: ", as.logical(usnat),
                 " NINTERNALMESH: ", nbasis,
                 " MESH TERMINI: ", meshrange),
          file = "FILEPATH")
    if (etftid == 2 & etmtid %in% c(5, 6)) {
        
        gprparamdf <- fread("FILEPATH")
        gprparamdf[manual == 0, methid := basemethid]
        
    } 
    
    for (agid in agids) {
    for (sid in sids) {
      for (gbdid in gbdids) {
        if (etmtid == 2) mvid <- mvdf$model_version_id[mvdf$sex_id == sid & mvdf$age_start <= agid & mvdf$age_end >= agid]
        if (etmtid %in% c(5,6)) {
            
            agsxcsparamdf <- gprparamdf[age_group_id_start <= agid &
                                     age_group_id_end >= agid &
                                     sex_id_start <= sid &
                                     sex_id_end >= sid &
                                     cause_id == gbdid]
            
            methid <- agsxcsparamdf$methid
            prioronly <- agsxcsparamdf$prioronly
            
        } else {
            
            methid <- basemethid
            prioronly <- 0
        }
        
       if(!file.exists("FILEPATH")) {
            
                qsub(jobname = paste0("epi_trans_fit_inla_etmtid_", etmtid, "_etmvid_", etmvid, "_agid_", agid, "_sid_", sid, "_gbdid_", gbdid),
                     shell = paste0(currentDir(), "/r_shell.sh"),
                     code = paste0(currentDir(), "/fit_inla_gpr.r"),
                     project = "epitrans",
                     args = list("--data_dir", data_dir,
                                 "--etmtid", etmtid,
                                 "--etmvid", etmvid,
                                 "--mvid", mvid,
                                 "--gbdrid", gbdrid,
                                 "--lsid", lsid,
                                 "--yids", paste0(yids, collapse = " "),
                                 "--agid", agid,
                                 "--sid", sid,
                                 "--gbdid", gbdid,
                                 "--usintercept", usint,
                                 "--usnat", usnat,
                                 "--meth", methid,
                                 "--nbasis", nbasis,
                                 "--meshrange", paste0(meshrange, collapse = " "),
                                 "--prioronly", prioronly),
                     slots = sl,
                     mem = m)
        }
        print(paste0(agid, sid, gbdid, sep = " "))
      }
      if (length(gbdids) > 10) Sys.sleep(5)
    }
  }
  print(agids)
  for (agid in agids) {
    for (sid in sids) {
      for (gbdid in gbdids) {
        while (!file.exists("FILEPATH")) {
          print(paste0("Waiting for FILEPATH"))
          Sys.sleep(30)
        }
      }
    }
  }
}
  # CoD functions
  if (etmtid %in% c(5,6)) {
    # Rake
    dir.create("FILEPATH")

      ## Rake - Epi
      
      if (etmtid == 6) {    
    for (lvl in min(causesdf$level):max(causesdf$level)) {
      pids <- unique(causesdf$parent_id[causesdf$level == lvl])
      for (pid in pids) {
        for (agid in agids) {
          for (sid in sids) {
            if (any(!file.exists("FILEPATH"))) {
              qsub(jobname = paste0("epi_trans_rake_etmtid_", etmtid, "_etmvid_", etmvid, "_agid_", agid, "_sid_", sid, "_pid_", pid, "_lvl_", lvl),
                   shell = paste0(currentDir(), "/r_shell.sh"),
                   code = paste0(currentDir(), "/raker.R"),
                   project = "epitrans",
                   args = list("--data_dir", data_dir,
                               "--etmtid", etmtid,
                               "--etmvid", etmvid,
                               "--rake_step", "fits",
                               "--lvl", lvl,
                               "--pid", pid,
                               "--agid", agid,
                               "--sid", sid),
                   slots = 8, 
                   mem = 16) 
            }
          }
        }
      }
      for (gbdid in causesdf$cause_id[causesdf$parent_id %in% pids & causesdf$level == lvl]) {
        for (agid in agids) {
          for (sid in sids) {
            while (!file.exists("FILEPATH")) {
              print(paste0("Waiting for FILEPATH"))
              Sys.sleep(30)
            }
          }
        }
      }
    }
}
      
      ## Rake - CoD
      
      if (etmtid == 5) {      
          for (agid in agids) {
              for (sid in sids) {
                  if (any(!file.exists("FILEPATH"))) {
                      qsub(jobname = paste0("epi_trans_rake_etmtid_", etmtid, "_etmvid_", etmvid, "_agid_", agid, "_sid_", sid),
                           shell = paste0(currentDir(), "/r_shell.sh"),
                           code = paste0(currentDir(), "/detailedraker.R"),
                           project = "epitrans",
                           args = list("--data_dir", data_dir,
                                       "--etmtid", etmtid,
                                       "--etmvid", etmvid,
                                       "--rake_step", "fits",
                                       "--agid", agid,
                                       "--sid", sid),
                           slots = 8, 
                           mem = 16) 
                  }
              }
          }
          
          for (agid in agids) {
              for (sid in sids) {
                  for (gbdid in gbdids) {
                      while (!file.exists("FILEPATH")) {
                          print(paste0("Waiting for FILEPATH"))
                          Sys.sleep(30)
                      }
                  }
              }
          }
      }
      
if (etmtid == 5) {    # Make YLLs
    dir.create("FILEPATH")
    for (gbdid in unique(c(294, gbdids))) { 
      if (any(!file.exists("FILEPATH"))) {
        qsub(jobname = paste0("epi_trans_YLL_etmtid_", etmtid, "_etmvid_", etmvid, "_gbdid_", gbdid),
             shell = paste0(currentDir(), "/r_shell.sh"),
             code = paste0(currentDir(), "/calc_ylls.R"),
             project = "epitrans",
             args = list("--data_dir", data_dir,
                         "--etmtid", etmtid,
                         "--etmvid", etmvid,
                         "--etmvid_lt", 23,
                         "--yll_step", "raked",
                         "--agids", paste0(agids, collapse = " "),
                         "--sids", paste0(sids, collapse = " "),
                         "--gbdid", gbdid),
             slots = 5, 
             mem = 10) 
      }
    }
    for (agid in agids) {
      for (sid in sids) {
        for (gbdid in gbdids) {
          while (!file.exists("FILEPATH")) {
            print(paste0("Waiting for FILEPATH"))
            Sys.sleep(30)
          }
        }
      }
    }
    }
  }

  # Aggregate ages and sexes 
  if (etmtid %in% c(1, 2, 5, 6)) {
    aggkeydf <- fread("FILEPATH")
    if (etmtid %in% c(1, 2)) agg_steps <- "fits"
    if (etmtid == 5) agg_steps <- c("YLLs", "raked")
    if (etmtid == 6) agg_steps <- "raked"
    if (etmtid == 5) gbdids <- gbdids  
    for (agg_step in agg_steps) {
        if(etmtid == 5 & agg_step == "YLLs") gbdids <- c(294, gbdids)
      for (gbdid in gbdids) {
        if (any(!file.exists("FILEPATH"))) {
          qsub(jobname = paste0("epi_trans_age_sex_agg_etmtid_", etmtid, "_etmvid_", etmvid, "_gbdid_", gbdid, "_", agg_step),
               shell = paste0(currentDir(), "/r_shell.sh"),
               code = paste0(currentDir(), "/age_sex_aggregation.R"),
               project = "epitrans",
               args = list("--data_dir", data_dir,
                           "--etmtid", etmtid,
                           "--etmvid", etmvid,
                           "--agg_step", agg_step,
                           "--etmvid_age", 7,  
                           "--etmvid_sex", 7, 
                           "--agids", paste0(agids, collapse = " "),
                           "--sids", paste0(sids, collapse = " "),
                           "--gbdrid", gbdrid,
                           "--gbdid", gbdid),
               slots = 15,
               mem = 30)
        }
      }
    }
    for (agg_step in agg_steps) {
      for (gbdid in gbdids) {
          for (agid in c(agids, 22, 27, 28)) {
        while (any(!file.exists(paste0(data_dir, "/t", etmtid, "/v", etmvid, "/", agg_step, "/agid_", agid, "_sid_3_gbdid_", gbdid, ".RDs")))) {
          print(paste0("Waiting for ", data_dir, "/t", etmtid, "/v", etmvid, "/", agg_step, "/agid_", agid,"_sid_3_gbdid_", gbdid, ".RDs -- ", Sys.time()))
          Sys.sleep(30)}
        }
      }
    }
  }

  if (etmtid == 1) {
    
      dir.create("FILEPATH") 
      
      hagids <- c(2:20, 22, 27, 28, 30:32, 235)
      hsids <- 1:3
      
      
      for (agid in hagids) {
          for (sid in hsids) {
              
              df <- readRDS("FILEPATH")
              saveRDS(df, "FILEPATH")
              
              print(paste0("AGID ", agid, " SID ", sid, " DONE."))
          }
      }

    
    for (agid in agids) {
        for (sid in sids) {
            
            while (!file.exists("FILEPATH")) {
                print(paste0("Waiting for FILEPATH"))
                Sys.sleep(30)
            }
        }
    }
  }
  
  # Calc DALYs and HALE if type 7
  if (etmtid == 7) {
      dir.create(paste0(data_dir, "/t7/v", etmvid, "/raked"))
      
      ## DALYs
      
      for (agid in c(agids, 22, 27, 28)) {
          for (sid in c(sids, 3)) {
              for (gbdid in gbdids) {
                  if (!file.exists("FILEPATH")) {                  
                  qsub(jobname = paste0("epi_trans_calc_dalys_etmtid_7_etmvid_", etmvid, "_agid_", agid, "_sid_", sid, "_gbdid_", gbdid),
                       shell = paste0(currentDir(), "/r_shell.sh"),
                       code = paste0(currentDir(), "/calc_dalys.R"),
                       project = "epitrans",
                       args = list("--data_dir", data_dir,
                                   "--etmvid", etmvid,
                                   "--etmvid_yll", 23,
                                   "--etmvid_yld", 12, 
                                   "--agid", agid,
                                   "--sid", sid,
                                   "--gbdid", gbdid),
                       slots = 4,
                       mem = 8)
    }
                  
              }
          }
      }
      
      for (agid in c(agids, 22, 27, 28)) {
          for (sid in c(sids,3)) {
              for (gbdid in gbdids) {
                  while (!file.exists("FILEPATH")) {
                      print(paste0("Waiting for FILEPATH"))
                      Sys.sleep(30)
                  }
              }
          }
      }
      
      ## HALE
      
      qsub(jobname = paste0("epi_trans_calc_hale_etmtid_7_etmvid_", etmvid),
           shell = paste0(currentDir(), "/r_shell.sh"),
           code = paste0(currentDir(), "/calc_hale.R"),
           project = "epitrans",
           args = list("--data_dir", data_dir,
                       "--etmvid", etmvid,
                       "--etmvid_lt", 24,
                       "--etmvid_yld", 12,
                       "--agids", paste0(c(28, 5:20, 30:32, 235), collapse = " "),
                       "--sids", paste0(c(1, 2, 3), collapse = " ")),
           slots = 4,
           mem = 8)
      
      while(!file.exists("FILEPATH")) {
          
          print(paste0("Waiting for FILEPATH"))
          
          Sys.sleep(30)
      }
      
      
  }
  
  # Make summaries
  if (etmtid %in% c(1, 2, 5, 6, 7)) {
    aggkeydf <- fread("FILEPATH")
    dir.create("FILEPATH")
    dir.create("FILEPATH")
    if (etmtid == 1) gbdids <- 294 
    if (etmtid == 1) summ_steps <- "wHIV"
    if (etmtid == 2) summ_steps <- "fits"
    if (etmtid == 5) summ_steps <- c("YLLs", "raked")
    if (etmtid %in% c(6,7)) summ_steps <- "raked"
    for (gbdid in gbdids) {
      if (!file.exists("FILEPATH")) {
        qsub(jobname ="FILEPATH",
             shell = paste0(currentDir(), "/r_shell.sh"),
             code = paste0(currentDir(), "/summarizer.R"),
             project = "epitrans",
             args = list("--data_dir", data_dir,
                         "--etmtid", etmtid,
                         "--etmvid", etmvid,
                         "--summ_steps", paste0(summ_steps, collapse = " "),
                         "--agids", paste0(c(agids, aggkeydf$agg_age_group_id), collapse = " "),
                         "--sids", paste0(c(1, 2, 3), collapse = " "),
                         "--gbdid", gbdid),
             slots = 15,
             mem = 30)
      }
    }
    
    
    for (gbdid in gbdids) {
      while (!file.exists("FILEPATH")) {
        print(paste0("Waiting for FILEPATH"))
        Sys.sleep(30)
      }
    }
  }
  
}

##########################################################
## EXECUTE
main()

##########################################################
## END SESSION
quit("no")

##########################################################