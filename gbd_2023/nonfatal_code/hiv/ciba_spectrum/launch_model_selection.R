################################################################################
## Purpose: Launch model selection for Spectrum/ART grid runs
## Date created: 
## Date modified:
## Author: A
## Run instructions: 
## Notes:
################################################################################

rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- "FILEPATH"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
home.dir <- paste0("FILEPATH/hiv_gbd2019/")

library(ggplot2)
library(parallel)
library(Metrics, lib = "FILEPATH")
library(MASS)
library(data.table)
library(mortdb, lib = "FILEPATH")
source("FILEPATH/get_population.R")
source(paste0(home.dir,"/03a_ciba2019/model_selection.R"))

args <- commandArgs(trailingOnly = TRUE)

if(length(args) > 0) {
  
  loc <- args[1]
  run.name <- args[2]
  number_combos <- args[3]
  stage <- args[4]
  cores <- args[5]
  create_example <- args[6]
 
} else {

  loc <- 'SAU'
  run.name <- "gbd20_grid"
  number_combos <- 5
  stage <- 'stage_1'
  cores <- 2
  create_example <- 0
 
}

index = fread(paste0("FILEPATH",loc,".csv"))

number_combos = nrow(index)

out_dir <- "FILEPATH"
out_dir_compiled <-  "FILEPATH"
plot_dir <-  "FILEPATH"
  
  
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir_compiled, recursive = TRUE, showWarnings = FALSE)
dir.create(plot_dir, recursive = TRUE, showWarnings = FALSE)

loc.table  = get_locations(hiv_metadata = T)
options = index$index

if(!grepl("grid",run.name)){
  index[,index := "combo_0"]
  out_folder <- "combo_0"
  path = "FILEPATH"
  xx <- assess_fit(run.name,stage,loc,out_folder=NULL)
  index[index == out_folder, c("rmse_spec_pre", 
                               "rmse_spec_post",
                               "trend_fit",
                               "n_preART",
                                "n_postART") := .(xx$rmse_spec_pre,
                                                 xx$rmse_spec_post,
                                                 xx$trend_fit,
                                                 xx$pre_art_yrs, 53-xx$pre_art_yrs)]
                                                  
  index[index == out_folder, RMSE := rmse_spec_post + rmse_spec_pre]
  saveRDS(xx,file = paste0(out_dir,out_folder, ".RDS"))
  fwrite(index,file = paste0(out_dir_compiled,loc, "_all_models.csv"))
  
}

if(grepl("grid",run.name)){
  
  missing = unlist(lapply(options, function(out_folder) {
    
                  
                  if(file.exists( "FILEPATH")){
                    
                        xx <- assess_fit(run.name,stage,loc,out_folder)
                        index[index == out_folder, c("rmse_spec_pre", 
                                                     "rmse_spec_post",
                                                     "trend_fit_pre",
                                                     "trend_fit_post", 
                                                     "trend_fit",
                                                     "n_preART",
                                                     "n_postART") := .(xx$rmse_spec_pre,
                                                                      xx$rmse_spec_post,
                                                                      xx$trend_fit_pre,
                                                                      xx$trend_fit_post,
                                                                      xx$trend_fit, 
                                                                      xx$pre_art_yrs, 53-xx$pre_art_yrs)]
                        
                        mean_RMSE = weighted.mean(c(index[index == out_folder,rmse_spec_pre],
                                                    index[index == out_folder,rmse_spec_post]),
                                                  c(index[index == out_folder,n_preART],
                                                    index[index == out_folder,n_postART]))
                        
                        mean_trend = weighted.mean(c(index[index == out_folder,trend_fit_pre],
                                                    index[index == out_folder,trend_fit_post]),
                                                  c(index[index == out_folder,n_preART],
                                                    index[index == out_folder,n_postART]))
                        
                        
                        index[index == out_folder, RMSE := mean_RMSE]
                        index[index == out_folder, trend_fit := mean_trend]
                        
                        saveRDS(xx, file = paste0(out_dir,out_folder, ".RDS"))
                       
              
                  } else {
                    
                        print(paste0("missing: ", out_folder))
                        return(out_folder)
                  }
                    
              }
                  
          )
                
      )
  
  if("RMSE" %in% colnames(index)){
    fwrite(index,file = paste0(out_dir_compiled,loc, "_all_models.csv"))
  }
  
  if(length(missing) > 1){
    
    table = data.table(loc = loc, combo = options, miss = 0)
    table[combo %in% missing, miss := 1]
    table = dcast(table, loc ~ combo, value.var = "miss")
    
    ##Write files with missing draws
    missing.out.dir <-  "FILEPATH"
    dir.create(missing.out.dir, recursive = T)
    write.csv(missing, paste0(missing.out.dir, loc, '.csv'), row.names = F)
  
  } 
  
  
  if(create_example == 1){
    
      index = fread(paste0(out_dir_compiled,loc, "_all_models.csv"))
      index_melt <- melt(index[,.(index,
                                  rmse_spec_pre,
                                  rmse_spec_post,
                                  trend_fit_pre,
                                  trend_fit_post
                                  )], id.vars = c("index"))
      
    
      
      lowest_RMSE =  index[RMSE == min(RMSE,na.rm=TRUE), index]
      lowest_trend = index[trend_fit == max(trend_fit,na.rm=TRUE), index]
      
      lowest = c(lowest_RMSE,
                 lowest_trend)
    
      
      lowest_runs  =rbindlist(lapply(unique(lowest), function(low){
       
        best <- readRDS(paste0(out_dir,low, ".RDS"))[['dat']]
        best[,combo := low]
        
        return(best)
        }))
     
    
      lowest_runs  <-  lowest_runs[,list(deaths = sum(deaths), 
                                         spec_deaths = sum(spec_deaths)), by = c("year","combo")]
      l.melt = melt(lowest_runs[,.(year,deaths,spec_deaths,combo)], id.vars= c("year","combo"))
      
      plot_name = loc.table[ihme_loc_id==loc,plot_name]
      gg1 <- ggplot(l.melt[variable == "deaths"] ,aes(year, value, color = combo)) + geom_point() + 
             geom_line(data = l.melt[variable == "spec_deaths"] ,aes(year, value, color = combo)) + theme_bw() +
             ggtitle(paste0(plot_name," ","Best models"))
      gg2 = ggplot(index_melt,aes(index, value, color = variable, group=variable)) + geom_point() + theme_bw() +
             ggtitle(paste0(plot_name," ","Model selection"))
      
      pdf(paste0(plot_dir,"/",loc,".pdf"), height = 6, width = 10)
      
        print(gg2)
        print(gg1)
        
      dev.off()
  
    
    
  }


}







