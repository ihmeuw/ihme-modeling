
# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  } else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
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


#------------------DIRECTORIES--------------------------------------------------
home_dir <- file.path("FILEPATH")
out_dir <- file.path(home_dir,"model",version)
dir.create(out_dir,showWarnings = F)

results_dir <- file.path(home_dir,"results",version)
dir.create(results_dir,showWarnings = F)

#------------------Load and scale RR data---------------------------------------
data <- read.xlsx(file.path("FILEPATH.xlsx")) %>% as.data.table
data <- data[exclude==0]

# convert all to ppb
# using US EPA conversion factor of 0.51 * micrograms/m^3 = ppb
data[unit=="microg/m^3",number:=number*.51]

# converting every RR to per 10 ppb
data[,rr_shift:=effect_size^(10/number)]
data[,rr_lower_shift:=lower^(10/number)]
data[,rr_upper_shift:=upper^(10/number)]

# if we have the upper and lower, we can convert to log SE
data[, log_effect_size_se := (log(rr_upper_shift)-log(rr_lower_shift))/(qnorm(0.975)*2)]

# take the log of the risk
data[,log_effect_size:=log(rr_shift)]

data[,.(cv_exposure_study,cv_confounding_uncontrolled,log_effect_size,log_effect_size_se)]

#------------------Prep covariates ---------------------------------------------
# Format covariates

data[,cv_confounding_uncontrolled_1:=ifelse(cv_confounding_uncontrolled==1,1,0)]
data[,cv_confounding_uncontrolled_2:=ifelse(cv_confounding_uncontrolled==2,1,0)]
data[,cv_confounding_uncontrolled:=NULL] # remove unnecessary variable

# weight the sample sizes
for (n in data$nid){
  if (nrow(data[nid==n])>1){
    n_observations <- nrow(data[nid==n])
    data[nid==n,log_se_weighted:=(log_effect_size_se*sqrt(n_observations))]
  }
}

data[is.na(log_se_weighted),log_se_weighted:=log_effect_size_se]

write.csv(data,paste0(out_dir,"/ozone_input_prepped.csv"),row.names = F)

