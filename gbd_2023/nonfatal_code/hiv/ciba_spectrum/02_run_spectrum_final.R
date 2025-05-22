#' @import data.table, ggplot2,library(mortdb, lib = "/mnt/team/mortality/pub/shared/r")
#' @param run.name.grid, string, folder where all possible ART and incidence inputs are stored. 
#' This must contain the word 'grid' in it for arguments to work (example: gbd20_grid)
#' @param run.name, string, official run.name where results will output and others will pull from (example: 200713_yuka)
#' @param base_art, string, folder where UNAIDS ART data is extracted to and incidence base for 2Cs + India -  (example: "2001316_windchime")


# Setup ---- 
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- "FILEPATH"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
home.dir <- paste0("FILEPATH/hiv_gbd2019/")


# Packages ---- 
library(data.table)

# Arguments ---- 
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
  
} else {
  
  # Process toggles
  prep.inputs <- F
  run_spectrum <- T
  ciba <- F
  group_2C_IND <- F
  plot.spec <- F
  
  
  # Run arguments
  run.name.grid <- 'gbd20_grid'
  run.name <- "200713_yuka"  # Update for each new run
  base_art <- "200316_windchime" #Wherever UNAIDS ART data is extracted to and incidence base for 2Cs + India 
  cluster.project <- "proj_hiv"
  n.jobs <- 200
  n.runs <- 5
  stage.1.inc.adj <- 1
  no_age_split <- FALSE
}

#Is this a grid run?
grid = 0
if(grepl("grid",run.name)){
  grid <- 1
}

# Functions ----
library(mortdb, lib = "/FILEPATH/r")
source(paste0(root,"FILEPATH/check_loc_results.r"))
loc.table <- data.table(get_locations(hiv_metadata = T, level = 'all'))

# Paths ----
shell.dir <- paste0("FILEPATH/hiv_gbd2019/")
code.dir <- paste0("FILEPATH/hiv_gbd2019/03_spectrum2019/")
ciba.dir <- paste0("FILEPATH/hiv_gbd2019/03a_ciba2019/")

config.dir <- paste0(root, "FILEPATH", run.name)
input.folders.path <- paste0(config.dir, "/input_folders.csv")
stage1.out.dir <- paste0("FILEPATH", run.name, "/compiled/stage_1/")
plot.dir <- paste0("FILEPATH", run.name)
adj.inc.dir <- paste0("FILEPATH", run.name, "_adj")
adj.ratios.dir <- paste0("FILEPATH", run.name, "_ratios")



eppasm.locs <- sort(loc.table[epp == 1 & grepl('1', group), ihme_loc_id])
spec.locs <- sort(loc.table[spectrum == 1 & most_detailed, ihme_loc_id])
loc.list <- spec.locs[!spec.locs %in% eppasm.locs]

stage1.only <- c("PHL_53613", "PHL_53598", 
                 "PHL_53611", "PHL_53610", "PHL_53609", 
                 "PHL_53612", "IDN_4739", "IDN_4740", "IDN_4741",
                 "MMR","KHM","GBR_433","GBR_434", "GBR_4636")


#Though India is not Group 2C, it follows the same workflow (Spectrum I, CIBA, Spectrum II)
group2C_ciba <- c(loc.list[grepl("IND",loc.list)],loc.table[group == "2C" & most_detailed,ihme_loc_id])


## Create output directories
for(path in c(stage1.out.dir,  adj.inc.dir, adj.ratios.dir, paste0(config.dir, c("/results/stage_1", "/results/stage_2")))) {
  dir.create(path, recursive = T, showWarnings = F)
}


## Get locations
if(run_spectrum){

  for(loc in loc.list) {
    
    print(loc)
    loc.group = loc.table[ihme_loc_id==loc,group]
    out_folder = run.name
    
    final_combo = fread("FILEPATH/final_GBD20_index.csv")[ihme_loc_id == loc] 
    final_combo = final_combo[1,]
    final_combo[incidence == base_art, incidence := "UNAIDS"]
    

    if(loc %in% group2C_ciba){ ##Seed with UNAIDS-files based incidence for 2C, or EPP Incidence for India
      
      inc_folder = "UNAIDS"
      art_folder = "UNAIDS_count"
      
    } else {
  
      art_folder = final_combo[,art_denom]
      inc_folder = final_combo[,incidence]
  
    }
    
    if(inc_folder == "UNAIDS") next

  
          # Launch Spectrum that reads in CIBA results
  
          onARTmort.adj <- ifelse(loc.table[ihme_loc_id == loc, super_region_name] == "High-income", 'T', 'F')
          i = 1


          if(inc_folder %in% "UNAIDS"){
            
          
            
            for(i in 1:n.jobs){

              spectrum <- paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=24:00:00 -l archive=True -q long.q ",
                                 "-cwd -P ",cluster.project," ",
                                 "-e FILEPATH/",user,"/errors ",
                                 "-o FILEPATH/", user, "/output ",
                                 "-N ", loc, "_", gsub("/", "_", run.name),"_",out_folder, "_spectrum_", i, " ",
                                 shell.dir, "python_shell.sh ",
                                 code.dir, "cohort_spectrum_nosplit.py ",
                                 loc, " ", run.name, " ", i, " ", n.runs, " ", stage.1.inc.adj, " stage_1 ", loc.group,' ', onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid)
              print(spectrum)
              system(spectrum)

              Sys.sleep(0.2)
            }

          } else {

              for(i in 1:n.jobs){

                spectrum <- paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=24:00:00 -l archive=True -q long.q ",
                                          "-cwd -P ",cluster.project," ",
                                          "-e FILEPATH/",user,"/errors ",
                                          "-o FILEPATH/", user, "/output ",
                                          "-N ", loc, "_", gsub("/", "_", run.name),"_", out_folder,"_spectrum_", i, " ",
                                          shell.dir, "python_shell.sh ",
                                          code.dir, "cohort_spectrum_age_split.py ",
                                          loc, " ", run.name, " ", i, " ", n.runs, " ", stage.1.inc.adj, " stage_1 ", loc.group,' ', onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid)

                print(spectrum)
                system(spectrum)

                Sys.sleep(0.2)
              }

          }

          # Compile results
          compile <- paste0("qsub -l m_mem_free=30.0G -l fthread=1 -l h_rt=00:30:00 -l archive=True -q  all.q ",
                            "-cwd -P ",cluster.project," ",
                            "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name),"_",out_folder)), "* ",
                            "-N ", loc, "_", gsub("/", "_", run.name), "_compile_", out_folder," ",
                            "-e FILEPATH/", user, "/errors ",
                            "-o FILEPATH/", user, "/output ",
                            shell.dir, "singR_shell.sh ",
                            code.dir, "compile_results.R ",
                            loc, " stage_1 five_year ", run.name, ' ', n.jobs, ' F', ' F ', 0," " ,out_folder)
          print(compile)
          system(compile)
          
          compile.cohort <- paste0("qsub -l m_mem_free=20.0G -l fthread=2 -l h_rt=01:00:00 -l archive=True -q  all.q ",
                                    "-cwd -P ",cluster.project," ",
                                    "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name),"_",out_folder)), "* ", 
                                    "-e FILEPATH/", user, "/errors ",
                                    "-o FILEPATH/", user, "/output ",
                                    "-N ", loc, "_", gsub("/", "_", run.name), "_compile_cohort ",
                                   shell.dir, "singR_shell.sh ",
                                   code.dir, "compile_cohort.R ",
                                   loc, " ", run.name, " 2 ", "stage_1"," 0" , out_folder)
    
      
  
  }
  
}


if(group_2C) {
  for(loc in group2C_ciba){
        onARTmort.adj <- ifelse(loc.table[ihme_loc_id == loc, super_region_name] == "High-income", 'T', 'F')
        i = 1
        if(loc == "PAK") next ##Not a 2C anymore, done at subnationals. should update model strategy spreadsheet
        
        if(grepl("IND",loc)){
          
        ##In India we use SRS data unlike 2C where we use ratios from 2AB (different script)
        ciba.string <-  paste0("qsub -l m_mem_free=20.0G -l fthread=1 -l h_rt=01:00:00 -l archive=True -q all.q ",
                                 "-cwd -P ",cluster.project," ",
                                 "-hold_jid ciba* ",
                                 "-N ",  paste0(loc, "_", gsub("/", "_", run.name), "_ratio "),
                                 "-e FILEPATH/", user, "/errors ",
                                 "-o FILEPATH/", user, "/output ",
                                 shell.dir, "singR_shell.sh ",
                                 ciba.dir, "Group_2AB_ciba.R ",
                                 loc, " ", run.name, " 2A ", n.jobs*n.runs," ", base_art)
        print(ciba.string)
        system(ciba.string)
        
        } else {
        ciba.string <-  paste0("qsub -l m_mem_free=20.0G -l fthread=1 -l h_rt=01:00:00 -l archive=True -q all.q ",
                               "-cwd -P ",cluster.project," ",
                               "-hold_jid ciba* ",
                               "-N ",  paste0(loc, "_", gsub("/", "_", run.name), "_ratio "),
                               "-e FILEPATH/", user, "/errors ",
                               "-o FILEPATH/", user, "/output ",
                               shell.dir, "singR_shell.sh ",
                               ciba.dir, "Group_2C_ciba.R ",
                               loc, " ", run.name, " 2C ", n.jobs*n.runs," ", base_art)
        print(ciba.string)
        system(ciba.string)
        }
        
        
   
        out_folder = run.name
        loc.group = loc.table[ihme_loc_id==loc,group]
        art_folder = "UNAIDS_count"
        inc_folder = "no_need" ##Pulls in CIBA incidence
        
        if(file.exists( paste0("FILEPATH",art_folder,"/",loc,".csv"))){
          
          file.copy(from = paste0("FILEPATH",art_folder,"/",loc,".csv"),
                    to = paste0("FILEPATH",loc,".csv"),
                    overwrite = T)
          
        }
   
        for(i in 1:n.jobs){
  
          spectrum <- paste0("qsub -l m_mem_free=7.0G -l fthread=1 -l h_rt=24:00:00 -l archive=True -q all.q ",
                             "-cwd -P ",cluster.project," ",
                             "-hold_jid ", paste0(loc, "_", gsub("/", "_", run.name), "_ratio "),
                             "-e FILEPATH/",user,"/errors ",
                             "-o FILEPATH/", user, "/output ",
                             "-N ", loc, "_", gsub("/", "_", run.name),"_", out_folder,"_spectrum_", i, " ",
                             shell.dir, "python_shell.sh ",
                             code.dir, "cohort_spectrum_age_split.py ",
                             loc, " ", run.name, " ", i, " ", n.runs, " ", stage.1.inc.adj, " stage_2 ", loc.group,' ', onARTmort.adj,' ',art_folder, ' ', inc_folder, ' ', out_folder,' ',grid)
  
          print(spectrum)
          system(spectrum)
  
          Sys.sleep(0.2)
        }
  
      # Compile results
      compile <- paste0("qsub -l m_mem_free=30.0G -l fthread=1 -l h_rt=00:30:00 -l archive=True -q  all.q ",
                        "-cwd -P ",cluster.project," ",
                        "-hold_jid ", paste(paste0(loc, "_", gsub("/", "_", run.name),"_",out_folder)), "* ",
                        "-N ", loc, "_", gsub("/", "_", run.name), "_compile_", out_folder," ",
                        "-e FILEPATH/", user, "/errors ",
                        "-o FILEPATH/", user, "/output ",
                        shell.dir, "singR_shell.sh ",
                        code.dir, "compile_results.R ",
                        loc, " stage_2 five_year ", run.name, ' ', n.jobs, ' F', ' F ', 0," " ,out_folder)
      print(compile)
      system(compile)
      
  }

}


