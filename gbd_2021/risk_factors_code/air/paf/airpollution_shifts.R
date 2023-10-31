Sys.umask(mode = 002)

os <- .Platform$OS.type
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_demographics.R"  )

run_interactively = 0

if(run_interactively == 1){
  
  jointdistr_fp = "FILEPATH.rds"
  airpol_shift_version = 47
  make_diagnostic = 1
  results_dir = "FILEPATH"
  
} else{
  
  ## Move to parallel script
  ## Getting normal QSub arguments
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  jointdistr_fp <- param_map[task_id, jointdistr_fp]
  airpol_shift_version <- param_map[task_id, airpol_shift_version]
  make_diagnostic <- param_map[task_id, make_diagnostic]
  results_dir <- param_map[task_id, results_dir]
  
}


estimation_years <- get_demographics(gbd_team = "epi", gbd_round_id = 7)$year_id

jointdistr_dir = file.path(results_dir, "FILEPATH")

## Likely need to edit this filepath
shift_dir = paste0("FILEPATH", airpol_shift_version, sep = "/")

shifted_jointdistr_dir = file.path(results_dir, "FILEPATH")

diagnos_dir = file.path(results_dir, "vetting", "airpol_diagnostics")
warnings_dir = file.path(results_dir, "vetting", "warnings")

pdf_filepath = file.path(diagnos_dir, gsub(x=jointdistr_fp, pattern = ".rds", replacement = ".pdf"))

# create if necessary
dir.create(shifted_jointdistr_dir, showWarnings = F)
dir.create(diagnos_dir, showWarnings = F)
dir.create(warnings_dir, showWarnings = F)

# Read in copula

jointdist <- readRDS(file.path(jointdistr_dir, jointdistr_fp))

location_id = unlist(strsplit(jointdistr_fp, split = "_"))[1]
location_id = as.integer(location_id)

e_year = unlist(strsplit(jointdistr_fp, split = "_"))[4]
e_year = gsub(e_year, pattern = ".rds", replacement = "")
e_year = as.integer(e_year)

shift_fp = paste0(location_id, "_", e_year, ".csv")
shift <- fread(file.path(shift_dir, shift_fp ))
shift <- shift[draw %in% 1:100, ]
shift[draw == 100, draw := 0]
shift[, draw := paste0('draw_', draw)]
setnames(shift, c("bw", "ga"), c("bw_shift", "ga_shift"))

message("Number of shifts creating smaller babies or shorter gestational ages: ", shift[bw_shift > 0 | ga_shift > 0, .N] ) 

if(jointdist[bw < 0 | ga < 0, .N ] > 0){
  
  dir.create(file.path(warnings_dir, "FILEPATH"))
  saveRDS(jointdist[bw < 0 | ga < 0], file.path(warnings_dir, "FILEPATH", jointdistr_fp))
  
}


# do the shifting
jointdist <- merge(jointdist, shift)

jointdist[, bw_orig := bw]
jointdist[, ga_orig := ga]

jointdist[, bw := bw_orig - bw_shift]
jointdist[, ga := ga_orig - ga_shift]

message("Number of birthweights shifted to be less than 0: ", jointdist[bw < 0, .N] ) 
message("Number of gestational ages shifted to be less than 0: ", jointdist[ga < 0, .N] ) 

if(shift[bw_shift > 0 | ga_shift > 0, .N] > 0){
  
  dir.create(file.path(warnings_dir, "shifted_opposite_direction"))
  saveRDS(shift[bw_shift > 0 | ga_shift > 0], file.path(warnings_dir, "shifted_opposite_direction", copula_fp))
  
}

saveRDS(jointdist[, -c("bw_orig", "ga_orig", "ga_shift", "bw_shift")], 
        file.path(shifted_jointdistr_dir, jointdistr_fp))

message("Expecting 100 draws")
print(jointdist[,.N,draw])
message("Expecting 1 location")
print(jointdist[,.N,location_id])
message("Expecting 1 age_group_id (EN, 2)")
print(jointdist[,.N,age_group_id])
message("Expecting 10,000 rows per loc-year-age-sex-draw")
print(jointdist[,.N,.(location_id,year_id,sex_id,age_group_id,draw)])



if(make_diagnostic == 1){
  
  pdf(pdf_filepath)
  
  bw_plot <-  ggplot(jointdist) + 
    geom_freqpoly(aes(x = bw, y = ..density.., group = draw), color = "red", alpha = 0.2, bins = 50) + 
    geom_freqpoly(aes(x = old_bw, y = ..density.., group = draw), color = "blue", alpha = 0.2, bins = 50) + 
    theme_bw() + xlim(0,6000) + 
    ggtitle(paste0("Air pollution shifts on birth weight"), 
            subtitle = "Blue = original distribution, Red = shifted")
  
  print(bw_plot)
  
  ga_plot <-  ggplot(jointdist) + 
    geom_freqpoly(aes(x = ga, group = draw), color = "red", alpha = 0.2, bins = 40) + 
    geom_freqpoly(aes(x = old_ga, group = draw), color = "blue", alpha = 0.2, bins = 40) + 
    theme_bw() +
    xlim(20,60) +
    ggtitle(paste0("Air pollution shifts on gestational age"), 
            subtitle = "Blue = original distribution, Red = shifted")
  
  print(ga_plot)
  
  dev.off()
  
}

