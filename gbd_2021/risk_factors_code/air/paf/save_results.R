# clear memory
rm(list=ls())
user <- "USERNAME"

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}

  arg <- commandArgs(trailingOnly=T)


# load packages, install if missing
pacman::p_load(data.table, magrittr, ini)

#define args:
risk <- arg[1]
me <- arg[2]
rei <- arg[3] %>% as.numeric
paf.version <- arg[4]
decomp <- arg[5]
years <- c(1990,2010,2020)

#draw directories
home.dir <- file.path("FILEPATH")
paf.dir.new <- file.path(home.dir,risk,"FILEPATH",paf.version)


#output directories
paf.scatter <- file.path(home.dir,risk,"FILEPATH",paf.version)
dir.create(paf.scatter, showWarnings=F, recursive=T)

#----------------------------Functions---------------------------------------------------


#Save Results Risk (RR, PAF)
source(file.path(central_lib,"FILEPATH/save_results_risk.R"))


#PAF scatters
source("FILEPATH/paf_scatter.R")



#--------------------SCRIPT-----------------------------------------------------------------


save_results_risk(input_dir = paf.dir.new,
                  input_file_pattern = "{location_id}.csv",
                  modelable_entity_id = me,
                  year_id = years,
                  description = paste0("PAFs version: ",paf.version," estimation years"),
                  risk_type = "paf",
                  mark_best=T,
                  decomp_step=decomp,
                  gbd_round_id=7)

#PAF Scatters
 
paf_scatter(rei_id= rei,
               measure_id= 4,
               file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yll_1990.pdf")),
               year_id= 1990,
               decomp_step=decomp)
 
paf_scatter(rei_id= rei,
               measure_id= 3,
               file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_1990.pdf")),
               year_id= 1990,
             decomp_step=decomp)

paf_scatter(rei_id= rei,
             measure_id= 4,
             file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yll_2019.pdf")),
             year_id= 2010,
             decomp_step=decomp)
 
paf_scatter(rei_id= rei,
             measure_id= 3,
             file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2019.pdf")),
             year_id= 2010,
             decomp_step=decomp)

paf_scatter(rei_id= rei,
            measure_id= 4,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yll_2020.pdf")),
            year_id= 2020,
            decomp_step=decomp)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2020.pdf")),
            year_id= 2020,
            decomp_step=decomp)

