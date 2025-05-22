
################################################################################
## DESCRIPTION ##  FAO Data Shifts
## INPUTS ##
## OUTPUTS ##
## AUTHOR ##   
## DATE ## 
## UPDATES ## 
## DATE MODIFIED 
## UPDATE SUMMARY: 
################################################################################

rm(list = ls())


## Config ----------------------------------------------------------------------#

# System info & drives
os <- Sys.info()[1]
user <- Sys.info()[7]
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""

data_dir <- "FILEPATH"
output_dir <- "FILEPATH"

# load libraries
library(ggplot2)
library(data.table)
library(msm)
library(plyr)
library(dplyr)
library(readr)

#Read in csv with rei-specific values
# path 
gbd_round <- "gbd2022"
path <- paste0("FILEPATH")
diet_info <- fread(paste0(path, "diet_ids_for_ap_fao_stgpr.csv"))



#loop over REIs of interest
for (i in unique(diet_info$rei_id)){


## Values to update
rei <- i
print(rei)
version <- "_v2"
print_plots <- TRUE
save <- FALSE


## Set some values
risk_info <- diet_info[rei_id == rei]
risk <- risk_info$risk
unit <- risk_info$unit
file_name <- paste0("FILEPATH/08_output/", risk, "_", unit ,"_unadj.csv")



## Read in data  ----------------------------------------#

data <- fread(file_name)


## Apply ratio shift approach by location  ----------------------------------------#

#define list
list_df_tmp <- list()

#define locations
locations <- unique(data$location_id)

for (i in locations){
  loc <- i
  data_loc <- data[location_id == loc]
  
  data_model <- data_loc[year_id >= 2010]
  
  data_model$year_id <- as.factor(data_model$year_id)
  
  if (length(unique(data_model$year_id)) > 1) {
  
  model <- lm(data ~ year_id, data_model)
  
  levels(data_model$year_id)
  
  names(coef(model))
  
  a <- coef(model)['year_id2011']
  shifter <- coef(model)['(Intercept)']
  
  data_loc$val2 <- ifelse(data_loc$year_id < 2010, data_loc$data - a, data_loc$data)
  }else {
    print(paste0("Only 1 year of data for ", loc))
  }
  
  
  if (((2007 %in% data_loc$year_id) || (2008 %in% data_loc$year_id) || (2009 %in% data_loc$year_id)) & 
  ((2010 %in% data_loc$year_id) || (2011 %in% data_loc$year_id) || (2012 %in% data_loc$year_id))) {
    
  recent_vals <- mean(data_loc$data[data_loc$year_id >= 2010 & data_loc$year_id <= 2012])
  old_vals <- mean(data_loc$data[data_loc$year_id >= 2007 & data_loc$year_id <= 2009])
  ratio <- recent_vals/old_vals
  
  data_loc$val <- ifelse(data_loc$year_id < 2010, data_loc$data * ratio, data_loc$data)
  
  }else{
    print(paste0("Missing 2009 or 2010 data for ", loc))
}

  list_df_tmp<- append(list_df_tmp, list(data_loc))

}

df_shifted <- bind_rows(list_df_tmp)


## Print plots of shifts ----------------------------------------#
if(print_plots){

  pdf(file = paste0(output_dir, "vetting_plots/", risk, version, "_fao_shifts.pdf"), width = 8, height = 6)
  
  for (i in locations){
    loc <- i
    data_loc_shifted <- df_shifted[location_id == loc]
    iso3 <- unique(data_loc_shifted$ihme_loc_id)
    
    title_p1 = paste0("Fig 1. ", risk, " for ", iso3, "-", loc, " : Original FAO data (black) with ratio shift (red)")
    p1 <- ggplot(data_loc_shifted) +
      geom_point(aes(x = year_id, y = data, color = "Original values"), size = 3, color = "black") + # Plotting val
      geom_point(aes(x = year_id, y = val, color = "Ratio shift applied"), size = 3, shape = 1, color = "red") +
      geom_vline(xintercept = 2010) +
      labs(x = "Year", y = paste0("Values ", unit), title = title_p1) +
      theme_minimal() 
    print(p1)
    
  }
  
  dev.off()
}

             
## Save shifted data files----------------------------------------#

if (save){
  df_shifted_write <- df_shifted
  df_shifted_write$data <- ifelse(!is.na(df_shifted_write$val), df_shifted_write$val, df_shifted_write$data)
  df_shifted_write[, c("val", "val2"):=NULL]
  
  fwrite(df_shifted_write, paste0(output_dir, risk, version, "_fao_shifted.csv" ))
}

}

