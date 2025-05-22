
#-------------------Header------------------------------------------------
# Author: 
# Date: 
# Purpose: Interpolate PAFs for NO2 for all years

#***************************************************************************

#------------------Set-up--------------------------------------------------

# clear memory
rm(list=ls())

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

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}



#------------------Arguments----------------------------------------------------
arg <- commandArgs(trailingOnly=T)

#define args:
risk <- arg[1]
me <- arg[2]
rei <- arg[3] %>% as.numeric
paf.version <- arg[4]
# decomp <- arg[5]
location <- arg[5] %>% as.numeric
#years <- c(1990,1995,2000,2005,2010,2015,2017,2019)

#------------------Directories and shared functions-----------------------------

# load shared functions
source("FILEPATH/get_draws.R")
source("FILEPATH/interpolate.R")
source("FILEPATH/paf_scatter.R")

#draw directories
home.dir <- file.path("FILEPATH")
paf.dir.new <- file.path(home.dir,risk, "paf/save",paf.version)
dir.create(paf.dir.new, recursive = T)

#output directories

paf.scatter <- file.path("FILEPATH")
dir.create(paf.scatter,showWarnings=F, recursive=T)

#------------------Interpolate PAFs: ambient, HAP, air_pmhap----------------------

# only ylds for asthma
df_ylds = interpolate(
  gbd_id_type='rei_id', gbd_id=rei, source='paf',
  measure_id=3, 
  location_id=location, 
  sex_id=c(1,2),
  release_id=16,
  # gbd_round_id=7,
  #decomp_step=decomp, 
  reporting_year_start=1990,
  reporting_year_end=2024)

write.csv(df_ylds, paste0(paf.dir.new, "/", location, "_3.csv"), row.names = FALSE)


#------------------PAF Scatters: YLLs, YLDs---------------------------------------

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_1990.pdf")),
            year_id= 1990)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_1995.pdf")),
            year_id= 1995)

paf_scatter(rei_id= rei,
             measure_id= 3,
             file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2000.pdf")),
             year_id= 2000)

paf_scatter(rei_id= rei,
             measure_id= 3,
             file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2005.pdf")),
             year_id= 2005)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2010.pdf")),
            year_id= 2010)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2015.pdf")),
            year_id= 2015)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2020.pdf")),
            year_id= 2020)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2021.pdf")),
            year_id= 2021)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2022.pdf")),
            year_id= 2022)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2023.pdf")),
            year_id= 2023)

paf_scatter(rei_id= rei,
            measure_id= 3,
            file_path= file.path(paste0(paf.scatter,"/",risk,"_version_",paf.version,"_scatter_yld_2024.pdf")),
            year_id= 2024)