
#-------------------------------------------------------------#
## Purpose: Launch Spectrum, Ensemble and CIBA
## Date created: 
## Date modified:
## Author: Austin Carter, aucarter@uw.edu; updated to GBD 2017 by Tahvi Frank; updated to GBD 2019 by Deepa J
## Run instructions: 
## Notes:
#-------------------------------------------------------------#

#' @import data.table, ggplot2,library(mortdb, lib = "/mnt/team/mortality/pub/shared/r")
#' @param run.name.grid, string, folder where all possible ART and incidence inputs are stored. 
#' This must contain the word 'grid' in it for arguments to work (example: gbd20_grid)
#' @param run.name, string, official run.name where results will output and others will pull from (example: 200713_yuka)
#' @param base_art, string, folder where UNAIDS ART data is extracted to and incidence base for 2Cs + India -  (example: "2001316_windchime")


# Setup ---- 
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
home.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/")


# Packages ---- 
library(data.table)

# Arguments ---- 
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  
} else {
  
  # Process toggles
  prep.inputs <- T
  run_spectrum <- F
  model_select <- F
  plot.spec <- F
  
  # Run arguments
  run.name <- "gbd20_grid"  # Update for each new run
  base_art <- "200316_windchime"  #Wherever UNAIDS ART data is extracted to
  cluster.project <- "proj_hiv"
  n.jobs <- 1
  n.runs <- 1
  stage.1.inc.adj <- 1
  no_age_split <- FALSE
}

# Functions ----
library(mortdb, lib = "FILEPATH")
source(paste0(root,"/FILEPATH/check_loc_results.r"))
loc.table <- data.table(get_locations(hiv_metadata = T, level = 'all'))

# Paths ----
shell.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/")
code.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/03_spectrum2019/")
ciba.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/03a_ciba2019/")

config.dir <- paste0("FILEPATH/", run.name)
input.folders.path <- paste0(config.dir, "/input_folders.csv")
stage1.out.dir <- paste0("FILEPATH/stage_1/")
stage2.out.dir <- paste0("FILEPATH/stage_2/")
adj.inc.dir <- paste0("/FILEPATH/", run.name, "_adj")
adj.ratios.dir <- paste0("/FILEPATH/", run.name, "_ratios")
plot.dir <- paste0("/FILEPATH/", run.name)

eppasm.locs <- sort(loc.table[epp == 1 & grepl('1', group), ihme_loc_id])
spec.locs <- sort(loc.table[spectrum == 1, ihme_loc_id])
loc.list <- spec.locs[!spec.locs %in% eppasm.locs]


del_files <- function(dir, prefix = "", postfix = "") {
  system(paste0("perl -e 'unlink <", dir, "/", prefix,  "*", postfix, ">' "))
}


grid = 0
if(grepl("grid",run.name)){
  grid <- 1
}

stage1.only = c("PHL_53613", "PHL_53598", 
                "PHL_53611", "PHL_53610", "PHL_53609", 
                "PHL_53612", "IDN_4739", "IDN_4740", "IDN_4741",
                "MMR","KHM","GBR_433","GBR_434", "GBR_4636")
loc.list = loc.list[!loc.list %in% loc.table[group=="2C",ihme_loc_id]]
loc.list = loc.list[!loc.list %in% stage1.only]
loc.list <- loc.list[!grepl("IND",loc.list)] ##India has its own pipeline

## Create grid of inputs 
if(prep.inputs){
  
  
  for(loc in loc.list){

    selection_grid <- paste0("qsub -l m_mem_free=10.0G -l fthread=2 -l h_rt=1:00:00 -l archive=True -q all.q ",
                       "-cwd -P ",cluster.project," ",
                       "-e /share/temp/sgeoutput/",user,"/errors ",
                       "-o /share/temp/sgeoutput/", user, "/output ",
                       "-N ", loc, "_", "prep_grid ",
                       shell.dir, "singR_shell.sh ",
                       home.dir,"/01_prep/04_launch_ART_inc_selection.R ", 
                       loc," 2 ", base_art," ",run.name," ",2022)
    print(selection_grid)
    system(selection_grid)
    
  }
  
  check_loc_results(loc.list,
                    paste0("FILEPATH"), 
                    postfix = ".csv")
  
}

## Get locations
if(run_spectrum){

for(loc in loc.list) {
  
  index = fread(paste0("FILEPATH",loc,".csv"))
  index[incidence == base_art, incidence := "UNAIDS"] 


  for(j in 1:nrow(index)){
    
      art_folder = index[j,art_denom]
      inc_folder = index[j,incidence]
      out_folder = index[j,index]
  
        # Launch Spectrum that reads in CIBA results
        loc.group <- loc.table[ihme_loc_id == loc, group]
        onARTmort.adj <- ifelse(loc.table[ihme_loc_id == loc, super_region_name] == "High-income", 'T', 'F')
        i = 1

        if(inc_folder %in% "UNAIDS"){
          for(i in 1:n.jobs){

            spectrum <- paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=24:00:00 -l archive=True -q long.q ",
                               "-cwd -P ",cluster.project," ",
                               "-e /share/temp/sgeoutput/",user,"/errors ",
                               "-o /share/temp/sgeoutput/", user, "/output ",
                               "-N ", loc, "_", gsub("/", "_", run.name),"_",out_folder, "_spectrum_", i, " ",
                               "-hold_jid " , loc, "_", "prep_grid ",
                               shell.dir, "python_shell.sh ",
                               code.dir, "cohort_spectrum_nosplit.py ",
                               loc, " ", run.name, " ", i, " ", n.runs, " ", stage.1.inc.adj, " stage_1 ",
                               loc.group,' ', onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid)
            print(spectrum)
            system(spectrum)

            Sys.sleep(0.2) ##This prevents jobs from submitting too fast, which overwhelms PYthon spectrum
          }

        } else {

            for(i in 1:n.jobs){

              spectrum <- paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=24:00:00 -l archive=True -q long.q ",
                                        "-cwd -P ",cluster.project," ",
                                        "-e /share/temp/sgeoutput/",user,"/errors ",
                                        "-o /share/temp/sgeoutput/", user, "/output ",
                                        "-hold_jid " , loc, "_", "prep_grid ",
                                        "-N ", loc, "_", gsub("/", "_", run.name),"_", out_folder,"_spectrum_", i, " ",
                                        shell.dir, "python_shell.sh ",
                                        code.dir, "cohort_spectrum_age_split.py ",
                                        loc, " ", run.name, " ", i, " ", n.runs, " ", stage.1.inc.adj, " stage_1 ", loc.group,' ',
                                        onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid )

              print(spectrum)
              system(spectrum)

              Sys.sleep(0.2)
            }

        }

      
  
        # Compile results
        compile <- paste0("qsub -l m_mem_free=30.0G -l fthread=1 -l h_rt=00:30:00 -l archive=True -q  long.q ",
                          "-cwd -P ",cluster.project," ",
                          "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name),"_",out_folder, "_spectrum_", 1:n.jobs), collapse=","), " ",
                          "-N ", loc, "_", gsub("/", "_", run.name), "_compile_", out_folder," ",
                          "-e /share/temp/sgeoutput/", user, "/errors ",
                          "-o /share/temp/sgeoutput/", user, "/output ",
                          shell.dir, "singR_shell.sh ",
                          code.dir, "compile_results.R ",
                          loc, " stage_1 five_year ", run.name, ' ', n.jobs, ' F', ' F ', 0," " ,out_folder)
        print(compile)
        system(compile)
  
  
        # Compile cohort
        compile.cohort <- paste0("qsub -l m_mem_free=20.0G -l fthread=2 -l h_rt=01:00:00 -l archive=True -q  long.q ",
                                 "-cwd -P ",cluster.project," ",
                                 "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name),"_",out_folder, "_spectrum_", 1:n.jobs), collapse=","), " ",
                                 "-e /share/temp/sgeoutput/", user, "/errors ",
                                 "-o /share/temp/sgeoutput/", user, "/output ",
                                 "-N ", loc, "_", gsub("/", "_", run.name), "_compile_cohort ",
                                 shell.dir, "singR_shell.sh ",
                                 code.dir, "compile_cohort.R ",
                                 loc, " ", run.name, " 2 ", 0," " , out_folder)
        print(compile.cohort)
        system(compile.cohort)
    
    }

  }
  
}
  
  
## Get Spec:VR bias
if(model_select){
  
  for(loc in loc.list){
      
      index = fread(paste0("FILEPATH",loc,".csv"))

      number_combos = nrow(index)
      
      print(loc)
      model_selection <- paste0("qsub -l m_mem_free=10.0G -l fthread=2 -l h_rt=01:00:00 -l archive=True -q long.q ",
                                   "-cwd -P ",cluster.project," ",
                                   "-e /share/temp/sgeoutput/", user, "/errors ",
                                   "-o /share/temp/sgeoutput/", user, "/output ",
                                   "-hold_jid ", loc, "_", gsub("/", "_", run.name), "_compile_", out_folder," ",
                                   "-N ", loc, "_", gsub("/", "_", run.name), "_model_selection ",
                                   shell.dir, "singR_shell.sh ",
                                   ciba.dir, "launch_model_selection.R ",
                                   loc, " ", run.name, " ", number_combos," stage_1 ", 2, " ", 0)
      print(model_selection)
      system(model_selection)
  
  }
  
}



## Summarize and plot the best results
##Choose best models and summarize/plot these combos
setwd("~/")

if(plot.spec){
  k=0
    for(loc in loc.list){
  
          if(file.exists(paste0("/FILEPATH/",loc,"_all_models.csv"))){
            
            ind <- fread(paste0("/FILEPATH/",loc,"_all_models.csv"))
            
           } else {
             
              next
             
           }
      
            if(!("RMSE" %in% colnames(ind))){
              
              next
            }
                
          lowest_RMSE =  ind[RMSE == min(RMSE,na.rm=TRUE), index]
          lowest_RMSE_pre =  ind[rmse_spec_pre == min(rmse_spec_pre,na.rm=TRUE), index]
          lowest_RMSE_post =  ind[rmse_spec_post == min(rmse_spec_post,na.rm=TRUE), index]
          lowest_trend_post = ind[trend_fit_post == max(trend_fit_post,na.rm=TRUE), index]
          lowest_trend_fit = ind[trend_fit == max(trend_fit,na.rm=TRUE), index]

          best_models =ind[index %in% c(lowest_RMSE,
                                        lowest_RMSE_pre,
                                        lowest_RMSE_post, 
                                        lowest_trend_fit,
                                        lowest_trend_post)]
          
        for(j in 1:nrow(best_models)){
          
          combo = best_models[j,index]
          print(combo) 
          # # 
          hold1 = paste0(loc,"_",run.name,"_model_selection")
          summarize.spectrum <- paste0("qsub -l m_mem_free=3.0G -l fthread=1 -l h_rt=01:00:00 -l archive=True -q all.q ",
                                       "-cwd -P ",cluster.project," ",
                                       "-e /share/temp/sgeoutput/", user, "/errors ",
                                       "-o /share/temp/sgeoutput/", user, "/output ",
                                       "-hold_jid ", hold1," ",
                                       "-N ", loc, "_", gsub("/", "_", run.name), "_summarize_",combo," ",
                                       shell.dir, "singR_shell.sh ",
                                       code.dir, "summarize_spec_output.R ",
                                       run.name, " ", loc, " ",k, " ",combo)
          print(summarize.spectrum)
          system(summarize.spectrum)


          hold2 = paste0(loc, "_", gsub("/", "_", run.name), "_summarize_",combo, collapse=",")
          plot.spectrum <- paste0("qsub -l m_mem_free=3.0G -l fthread=1 -l h_rt=01:00:00 -l archive=True -q all.q ",
                                       "-cwd -P ",cluster.project," ",
                                       "-e /share/temp/sgeoutput/", user, "/errors ",
                                       "-o /share/temp/sgeoutput/", user, "/output ",
                                       "-hold_jid ",hold2," ",
                                       "-N ", loc, "_", gsub("/", "_", run.name), "_plot ",
                                       shell.dir, "singR_shell.sh ",
                                       code.dir, "/plotting/plot_age_sex_spec_mortality.R ",
                                       run.name, " ", loc, " ", combo)
          print( plot.spectrum)
          system( plot.spectrum )
          
 
      }
      
  }    
      
} 




