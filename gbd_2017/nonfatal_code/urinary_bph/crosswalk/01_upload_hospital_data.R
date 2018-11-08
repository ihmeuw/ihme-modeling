# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
  require(readxl)
  require(openxlsx)
  require(pacman)
  p_load(data.table,RMySQL,magrittr)
} else { 
  stop("Must be run on the cluster")
}

function_dir<-paste0(h_root,"FILEPATH")
invisible(lapply(paste0(function_dir,list.files(function_dir,pattern = "^\\D*_function.R")),source))

# load config 
config<-fread(paste0(h_root,"FILEPATH/config.csv"))

args<-commandArgs(trailingOnly = TRUE)
print(args)
i<-as.numeric(args[1])

for (n_col in 1:ncol(config)){
  col_name<-names(config)[n_col]
  assign(names(config)[n_col],config[i,get(col_name)])
}

if (outlier_analysis==1){
  outlier_hospital(acause,bundle,method,margin,haqi_scalar,compare_claims_hosp,crosswalk,delete_prev_data,output_bundle)
}
