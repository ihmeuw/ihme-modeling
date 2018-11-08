###################################################################################################################
## Author: 
## Description: save results of asymptomatic gallbladder draws
###################################################################################################################


##working environment
os <- .Platform$OS.type
if (os=="windows") {
  j<- "J:/"
  h<-"H:/"
} else {
  j<- "/home/j/"
  h<-"/homes/USERNAME/"
}

#set object and source shared function
functions_dir <- paste0(j, FILEPATH)
source(paste0(functions_dir, "save_results_epi.R"))

#run save results
save_results_epi(input_dir=FILEPATH, input_file_pattern = "{measure_id}_{location_id}.csv", modelable_entity_id =9535, description = 'GBD2017 custom upload of asymptomatic', mark_best='TRUE', measure_id = 5)  