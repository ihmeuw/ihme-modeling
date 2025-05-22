
#-------------------Header------------------------------------------------
# Purpose: Prep ozone RR data for MR-BRT 
#          
#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/"
  h_root <- "~/"
  } else {
  j_root <- "J:"
  h_root <- "H:"
  }



# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# load the functions
repo_dir <- "FILEPATH"
source(file.path(repo_dir,"mr_brt_functions.R"))

version <- 1 # starting over for GBD2019
version <- 2 # first run GBD2020
version <- 3 # weighted SEs
version <- 4 # new scorelator
version <- 5 # calculating draws with Fisher's Information boost

#------------------DIRECTORIES--------------------------------------------------
home_dir <- file.path("/FILEPATH")
out_dir <- file.path(home_dir,"model",version)
dir.create(out_dir,showWarnings = F)

results_dir <- file.path(home_dir,"results",version)
dir.create(results_dir,showWarnings = F)

#------------------Load and scale RR data---------------------------------------
data <- read.xlsx(file.path("FILEPATH/ozone_mr_brt_input.xlsx")) %>% as.data.table
data <- data[exclude==0]

# convert all to ppb
# using US EPA conversion factor of 0.51 * micrograms/m^3 = ppb
data[unit=="microg/m^3",number:=number*.51]

# converting every RR to per 10 ppb
data[,rr_shift:=effect_size^(10/number)]
data[,rr_lower_shift:=lower^(10/number)]
data[,rr_upper_shift:=upper^(10/number)]

# if we have the upper and lower, we can just log those and convert to log SE
data[, log_effect_size_se := (log(rr_upper_shift)-log(rr_lower_shift))/(qnorm(0.975)*2)]

# take the log of the risk
data[,log_effect_size:=log(rr_shift)]

data[,.(cv_exposure_study,cv_confounding_uncontrolled,log_effect_size,log_effect_size_se)]

#------------------Prep covariates ---------------------------------------------
# Format covariates according to protocol given by MSCM for the new MR-BRT

# create dummy covariates for cv_confounding_uncontrolled because it has 3 levels
data[,cv_confounding_uncontrolled_1:=ifelse(cv_confounding_uncontrolled==1,1,0)]
data[,cv_confounding_uncontrolled_2:=ifelse(cv_confounding_uncontrolled==2,1,0)]
data[,cv_confounding_uncontrolled:=NULL] # remove unnecessary variable

# weight the sample sizes in accordance with the guidance for the new MRBRT
# for studies with >1 observations, we want to weight the SEs to represent smaller sample sizes
# this is so each individual study is weighted less
for (n in data$nid){
  if (nrow(data[nid==n])>1){
    n_observations <- nrow(data[nid==n])
    data[nid==n,log_se_weighted:=(log_effect_size_se*sqrt(n_observations))]
  }
}

data[is.na(log_se_weighted),log_se_weighted:=log_effect_size_se]

# don't need to code these as effect modifiers because the exposure information is the same for all (10)

write.csv(data,paste0(out_dir,"/ozone_input_prepped.csv"),row.names = F)

