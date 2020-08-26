#########################################################
###### Purpose: save script for hemog-splits ############
####### in order to submit parallel so i can be away#####
#########################################################
#########################################################

rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "USERNAME"
}

arg <- commandArgs(trailingOnly = TRUE)
me <- arg[1]

# me <- 2584

#############################################

source(paste0("FILEPATH"))

#############################################

upload_map <- read.csv(paste0("FILEPATH"))

if(me == 3083){
  draw_me <- 2336
} else if(me == 3084){
  draw_me <- 2335
} else{
  draw_me <- me
}

comment <- paste0(upload_map$message[upload_map$me_id == me])

print("Starting save")

save_results_epi(modelable_entity_id = me,
                 input_dir = paste0("FILEPATH"),
                 input_file_pattern = "FILEPATH",
                 description = comment,
                 gbd_round_id = 6,
		 decomp_step = 'step4',
		 mark_best=TRUE)
                     



