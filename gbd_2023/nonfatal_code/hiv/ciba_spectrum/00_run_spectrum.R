
################################################################################
## Purpose: Launch Spectrum, Ensemble and CIBA
## Date created: 
## Date modified:
## Author: 
## Run instructions: 
## Notes:
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- "FILEPATH"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
home.dir <- paste0("FILEPATH/hiv_gbd2019/")

## Packages
library(data.table)

### Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  
} else {
  
  # Process toggles
  stage1 <- T
  ciba <- T
  stage2 <- T
  plot.spec <- T
  
  # Run arguments

  run.name <- "200316_windchime"  # Update for each new run - this becomes the base run
  cluster.project <- "proj_hiv"
  n.jobs <- 200
  n.runs <- 5
  stage.1.inc.adj <- 1
  test <- F 
  no_age_split <- FALSE
}

### Functions
library(mortdb, lib = "FILEPATH")
source(paste0(root,"/FILEPATH/check_loc_results.r"))

del_files <- function(dir, prefix = "", postfix = "") {
  system(paste0("perl -e 'unlink <", dir, "/", prefix,  "*", postfix, ">' "))
}


### Paths
shell.dir <- paste0("FILEPATH/hiv_gbd2019/")
code.dir <- paste0("FILEPATH/hiv_gbd2019/03_spectrum2019/")
ciba.dir <- paste0("FILEPATH/hiv_gbd2019/03a_ciba2019/")

config.dir <- paste0("FILEPATH", run.name)
input.folders.path <- paste0(config.dir, "/input_folders.csv")
stage1.out.dir <- paste0("FILEPATH", run.name, "/compiled/stage_1/")
stage2.out.dir <- paste0("FILEPATH", run.name, "/compiled/stage_2/")
adj.inc.dir <- paste0("FILEPATH", run.name, "_adj")
adj.ratios.dir <- paste0("FILEPATH", run.name, "_ratios")
plot.dir <- paste0("FILEPATH", run.name)

## Create output directories
for(path in c(stage1.out.dir, stage2.out.dir, adj.inc.dir, adj.ratios.dir, paste0(config.dir, c("/results/stage_1", "/results/stage_2")))) {
  dir.create(path, recursive = T, showWarnings = F)
}

## Get locations
loc.table <- data.table(get_locations(hiv_metadata = T, level = 'all'))
eppasm.locs <- sort(loc.table[epp == 1 & grepl('1', group), ihme_loc_id])
spec.locs <- sort(loc.table[spectrum == 1, ihme_loc_id])
loc.list <- spec.locs[!spec.locs %in% eppasm.locs]
stage1.list <- intersect(loc.table[spectrum == 1 & !(grepl('GBR', ihme_loc_id) & level == 6), ihme_loc_id], loc.list)
## We run group 2A, 2B, and India through CIBA. Include UTLAs but not GBR regions
ciba.locs <- intersect(loc.table[(group %in% c("2A", "2B") & spectrum == 1 & !(grepl('GBR', ihme_loc_id) & level < 6)) | (grepl('IND', ihme_loc_id) & level == 5), ihme_loc_id], loc.list)
ciba.ratio.locs <- intersect(loc.table[group %in% c("2C") & spectrum == 1, ihme_loc_id], loc.list)
stage2.list <- intersect(c(ciba.locs, ciba.ratio.locs), loc.list)


##grid-specific arguments all NULL for the 1st run
grid = 999 ##This can be anything but 1 or 0, 1 is for the grid run, while 0 is for the final run which still pulls from grid folders
art_folder = run.name
inc_folder = run.name
out_folder = run.name


#Spectrum Stage 1 is run using the new UNAIDS data

## First Stage Spectrum
if(stage1) {
  # Setup testing configuration
  if(test) {
    n.jobs <- 1
    n.runs <- 1
  }

  for(loc in stage1.list) {
    no_age_split <- TRUE ##Only change if running on age-split data

    # Launch first stage Spectrum
    loc.group <- loc.table[ihme_loc_id == loc, group]
    onARTmort.adj <- ifelse(loc.table[ihme_loc_id == loc, super_region_name] == "High-income", 'T', 'F')
    i = 1
  
       for(i in 1:n.jobs) {
      print(no_age_split)
      if(no_age_split){
        first.spectrum = paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=10:00:00 -l archive=True -q all.q ",
                                "-cwd -P ",cluster.project," ",
                                "-e /FILEPATH/", user, "/errors ",
                                "-o /FILEPATH/", user, "/output ",
                                "-N ", loc, "_", gsub("/", "_", run.name), "_spectrum_", i, " ",
                                shell.dir, "python_shell.sh ",
                                code.dir, "cohort_spectrum_nosplit.py ",
                                loc, " ", run.name, " ", i, " ", n.runs, " ", 
                                stage.1.inc.adj, " stage_1 ", loc.group,' ', 
                                onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid)
        
        print(first.spectrum)
        system(first.spectrum)
        
      } else {
        
        first.spectrum = paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=03:00:00 -l archive=True -q all.q ",
                                "-cwd -P ",cluster.project," ",
                                "-e /FILEPATH/", user, "/errors ",
                                "-o /FILEPATH/", user, "/output ",
                                "-N ", loc, "_", gsub("/", "_", run.name), "_spectrum_", i, " ",
                                shell.dir, "python_shell.sh ",
                                code.dir, "cohort_spectrum_age_split.py ",
                                loc, " ", run.name, " ", i, " ", n.runs, " ", stage.1.inc.adj, " stage_1 ", loc.group,' ', onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid)
        
        print(first.spectrum)
        system(first.spectrum)
      }
      Sys.sleep(0.2)
    }
    
    # Compile results
    
    compile <- paste0("qsub -l m_mem_free=30.0G -l fthread=1 -l h_rt=00:30:00 -l archive=True -q  all.q ",
                      "-cwd -P ",cluster.project," ",
                      "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name), "_spectrum_", 1:n.jobs), collapse=","), " ",
                      "-N ", loc, "_", gsub("/", "_", run.name), "_compile ",
                      "-e /FILEPATH/", user, "/errors ",
                      "-o /FILEPATH/", user, "/output ",
                      shell.dir, "singR_shell.sh ",
                      code.dir, "compile_results.R ",
                      loc, " stage_1 five_year ", run.name, ' ', n.jobs, ' F', ' F')
    print(compile)
    system(compile)
    
    
    # Compile cohort
    if(loc %in% ciba.locs| grepl('GBR', loc)){
      compile.cohort <- paste0("qsub -l m_mem_free=20.0G -l fthread=2 -l h_rt=01:00:00 -l archive=True -q  all.q ",
                               "-cwd -P ",cluster.project," ",
                               "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name), "_spectrum_", 1:n.jobs), collapse=","), " ", 
                               "-e /FILEPATH/", user, "/errors ",
                               "-o /FILEPATH/", user, "/output ",
                               "-N ", loc, "_", gsub("/", "_", run.name), "_compile_cohort ", 
                               shell.dir, "singR_shell.sh ", 
                               code.dir, "compile_cohort.R ", 
                               loc, " ", run.name, " 2")
      print(compile.cohort)
      system(compile.cohort)
    }
  }
  
}



##These locations dont have VR output
ciba.locs <- ciba.locs[!ciba.locs %in% c("PHL_53613", "PHL_53598", "PHL_53611", "PHL_53610", "PHL_53609", "PHL_53612", "IDN_4739", "IDN_4740", "IDN_4741")]

if(ciba) {
  for(loc in  ciba.locs){
    if(! loc %in% loc.table[grepl('GBR', ihme_loc_id) & level == 6, ihme_loc_id]){
      ciba.hold <- paste(paste0(loc, "_", gsub("/", "_", run.name), c("_compile", "_compile_cohort")), collapse=",")
    } else{
      loc.parent <- loc.table[ihme_loc_id == loc, parent_id]
      ciba.hold <- paste(paste0('GBR_',loc.parent, "_", gsub("/", "_", run.name), c("_compile", "_compile_cohort")), collapse=",")
    }
    
    ciba.string <- paste0("qsub -l m_mem_free=20.0G -l fthread=6 -l h_rt=01:00:00 -l archive=True -q all.q ",
                          "-cwd -P ",cluster.project," ",
                          "-hold_jid ", ciba.hold, " ",
                          "-N ", loc, "_", gsub("/", "_", run.name), "_ciba ",
                          "-e /FILEPATH/", user, "/errors ",
                          "-o /FILEPATH/", user, "/output ",
                          shell.dir, "singR_shell.sh ",
                          ciba.dir, "Group_2AB_ciba.r ",
                          loc, " ", run.name, " 2A")
    print(ciba.string)
    system(ciba.string)
    Sys.sleep(0.2) 
  }
  
  #Launch CIBA for locations without high quality vital registration data
  # # 
  for(loc in ciba.ratio.locs) {
    ciba.hold <- paste(paste0(ciba.locs, "_", gsub("/", "_", run.name), "_ciba"), collapse=",")
    ciba.string <-  paste0("qsub -l m_mem_free=20.0G -l fthread=1 -l h_rt=01:00:00 -l archive=True -q all.q ",
                           "-cwd -P ",cluster.project," ",
                           #"-hold_jid ", ciba.hold, " ",
                           "-N ", loc, "_", gsub("/", "_", run.name), "_ciba ",
                           "-e /FILEPATH/", user, "/errors ",
                           "-o /FILEPATH/", user, "/output ",
                           shell.dir, "singR_shell.sh ",
                           ciba.dir, "Group_2C_ciba.R ",
                           loc, " ", run.name, " 2C ", n.jobs*n.runs)
    print(ciba.string)
    system(ciba.string)
  }
}



## Second Stage Spectrum
##Exclude locs with no VR data which fail during CIBA -Stage 1 becomes final
stage2.list <- stage2.list[!stage2.list %in% c("PHL_53613", "PHL_53598", "PHL_53611", "PHL_53610", "PHL_53609", "PHL_53612", "IDN_4739", "IDN_4740", "IDN_4741")]

if(stage2) {

  for(loc in stage2.list) {
    # Launch second stage Spectrum
    loc.group <- loc.table[ihme_loc_id == loc, group]
    onARTmort.adj <- ifelse(loc.table[ihme_loc_id == loc, super_region_name] == "High-income", 'T', 'F')
    i = 1
    for(i in 1:n.jobs){
      second.spectrum <- paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=24:00:00 -l archive=True -q all.q ",
                                "-cwd -P ",cluster.project," ",
                                "-e /FILEPATH/", user, "/errors ",
                                "-o /FILEPATH/", user, "/output ",
                                "-hold_jid ", loc, "_", gsub("/", "_", run.name), "_ciba ",
                                "-N ", loc, "_", gsub("/", "_", run.name), "_spectrum2_", i, " ",
                                shell.dir, "python_shell.sh ",
                                code.dir, "cohort_spectrum_age_split.py ", 
                                loc, " ", run.name, " ", i, " ", n.runs, " 0 stage_2 ", loc.group, ' ', onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid)
      print(second.spectrum)
      system(second.spectrum)
      
      Sys.sleep(0.2)
    }
    
    # Compile results
    compile <- paste0("qsub -l m_mem_free=30.0G -l fthread=1 -l h_rt=00:30:00 -l archive=True -q long.q ",
                      "-cwd -P ",cluster.project," ",
                      "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name), "_spectrum2_", 1:n.jobs), collapse=","), " ", 
                      "-N ", loc, "_", gsub("/", "_", run.name), "_compile2 ", 
                      "-e /FILEPATH/", user, "/errors ",
                      "-o /FILEPATH/", user, "/output ",
                      shell.dir, "singR_shell.sh ", 
                      code.dir, "compile_results.R ", 
                      loc, " stage_2 five_year ", run.name, ' ', n.jobs, ' F', ' T')
    print(compile)
    system(compile)
    
    
  }
}


## Summarize and plot results
###These plots are for vetting purposes post Stage 1 & 2 Spectrum
if(plot.spec){
  for(loc in loc.list){
    
    summarize.spectrum <- paste0("qsub -l m_mem_free=3.0G -l fthread=1 -l h_rt=01:00:00 -l archive=True -q all.q ",
                                 "-cwd -P ",cluster.project," ", 
                                 "-e /FILEPATH/", run.name, "/errors ",
                                 "-o /FILEPATH/", run.name, "/output ",
                                 "-hold_jid ", loc, "_", gsub("/", "_", run.name), "_compile2 ",
                                 "-N ", loc, "_", gsub("/", "_", run.name), "_summarize ", 
                                 shell.dir, "singR_shell.sh ", 
                                 code.dir, "summarize_spec_output.R ", 
                                 run.name, " ", loc)
    print(summarize.spectrum)
    system(summarize.spectrum)
    
    plot.sex <- paste0("qsub -l m_mem_free=2.0G -l fthread=1 -l h_rt=01:00:00 -l archive=True -q all.q ",
                       "-cwd -P ",cluster.project," ",
                       "-e /FILEPATH/", user, "/errors ",
                       "-o /FILEPATH/", user, "/output ",
                       "-hold_jid ", loc, "_", gsub("/", "_", run.name), "_summarize ",
                       "-N ", loc, "_", gsub("/", "_", run.name), "_plot_age_sex ",
                       shell.dir, "singR_shell.sh ",
                       code.dir, "/plotting/plot_age_sex_spec.R ",
                       run.name, " ", loc)
    print(plot.sex)
    system(plot.sex)
    
  }
}			
