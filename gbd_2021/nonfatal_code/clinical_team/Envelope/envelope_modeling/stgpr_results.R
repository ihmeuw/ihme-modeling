####################################################################################################
## STGPR_RESULTS.R
## Saving and plotting ST-GPR results for inpatient utilization envelope - scatters and plot_gpr
####################################################################################################

rm(list=ls())

library(ggplot2)
library(data.table)

## ---------------------------------------------------------------------------------------------------------------------
## USE SAVE RESULTS ST-GPR - UPLOADS TO EPIVIZ
## Need to have run with 1000 draws for this
source("FILEPATH/save_results_stgpr.R")
save_results_stgpr(stgpr_version_id = 164348,
                   description = "",
                   metric_id = 3,
                   db_env = "prod",
                   mark_best = FALSE)

## ---------------------------------------------------------------------------------------------------------------------
## GET HELPFUL INFO FOR PLOTTING 
## Get ST-GPR utilities
central_root <- 'FILEPATH'
setwd(central_root)
source('FILEPATH')

## Get age and location metadata
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_ids.R")
loc_data <- get_location_metadata(location_set_id = 22, gbd_round_id = 7, decomp_step = "iterative")
loc_data <- loc_data[,c("location_id","super_region_id","region_id","super_region_name","region_name","location_name")]
ages <- get_ids("age_group")

## ---------------------------------------------------------------------------------------------------------------------
## LOAD MODEL INFORMATION 
## Get results (use model version id)
data_151676 <- as.data.table(model_load(151676, "raked"))
data_163934 <- as.data.table(model_load(163934, "raked"))

## Rename means for merge
data_151676$mean_151676 <- data_151676$gpr_mean
data_163934$mean_163934 <- data_163934$gpr_mean

## ---------------------------------------------------------------------------------------------------------------------
## MERGE ALL DATA FOR PLOTTING
## Merge model results 
merged_to_plot <- merge(data_151676, data_163934, by=c("sex_id","location_id","age_group_id","year_id"))

## Merge on loc data
merged_to_plot <- merge(merged_to_plot, loc_data, by=c("location_id"))

## Merge on age data
merged_to_plot <- merge(merged_to_plot, ages, by=c("age_group_id"))


## ---------------------------------------------------------------------------------------------------------------------
## CREATE PLOTTING FUNCTION TO CREATE SCATTERS TO COMPARE MODEL VERSIONS
## Define function
plot_ages <- function(data, xlabel, ylabel, title, models){ 
  data <- merged_to_plot
  data$sex_id <- factor(data$sex_id, labels=c("Male","Female"))
  plot_list <- list()
  ## Reorganize age_group_ids in order by actual age
  ids <- c(2,3,388,389,238,34,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)
  for(i in 1:length(ids)){
    age_data <- subset(data, age_group_id == ids[[i]])
    ## Make sure to change variables here
    age_plot <- ggplot(age_data, aes(x=mean_151676, y=mean_163934, color=factor(super_region_name))) +
      theme_bw() + 
      geom_point(size = 3, alpha = 0.5) +
      geom_abline(slope = 1, intercept = 0, size = 0.2) + 
      ggtitle(paste0(age_data$age_group_name[1], ", ", title)) +
      labs(color="super region") + 
      facet_wrap(~sex_id) +
      xlab(xlabel) +
      ylab(ylabel) + 
      scale_color_viridis_d()
    plot_list[[i]] <- age_plot
  }
  pdf(paste0("FILEPATH"), width = 15, height = 6)
  for(i in 1:length(plot_list)){
    print(plot_list[[i]])
  }
  dev.off()
}

plot_ages(merged_to_plot, 
          "", 
          "", 
          "Comparing model versions", 
          "")



## ---------------------------------------------------------------------------------------------------------------------
## RUN PLOT_GPR CODE
## Source my modified script
source("FILEPATH")
run_id_to_plot <- 163934
plot_gpr(run.id = run_id_to_plot, output.path = paste0("FILEPATH",run_id_to_plot,".pdf"),
         add.regions = FALSE, add.outliers = TRUE, cluster.project = "proj_hospital", add.gbd.past = FALSE, y.axis = TRUE)







