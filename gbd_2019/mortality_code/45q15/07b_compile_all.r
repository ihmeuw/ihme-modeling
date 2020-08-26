################################################################################
## Description: Compile GPR results after getting output at the country-level
################################################################################

rm(list=ls())
library(foreign); library(data.table); library(dplyr); library(argparse)

# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='Version id of 45q15 model run')
parser$add_argument('--hiv_uncert', type="integer", required=TRUE,
                    help='HIV uncertainty toggle')

args <- parser$parse_args()
version_id <- args$version_id
hivuncert <- as.logical(args$hiv_uncert)

output_dir <- "FILEPATH"
data_dir <- paste0(output_dir, "/draws")

## Get locations
codes <- fread(paste0(output_dir, "/data/locations.csv"))
parents <- codes[codes$level_1==1 & codes$level_2==0,]
nonparents <- codes[!(codes$ihme_loc_id %in% parents$ihme_loc_id),]
nonparents <- nonparents[nonparents$level == 3 | nonparents$ihme_loc_id %in% c("CHN_354", "CHN_361"),]

data=NULL
unscaled = NULL
file_errors <- 0
i <- 0
j <- 0


## Import results
for (cc in unique(codes$ihme_loc_id)) {
  for (ss in c("male", "female")) {
    print(paste0("Importing ",cc," ",ss))
    file <- paste(data_dir, ifelse(hivuncert==T,"/compiled",""),"/gpr_",cc,"_",ss,".csv",sep="")
    file_sim <- paste(data_dir, ifelse(hivuncert==T,"/compiled",""),"/gpr_",cc,"_",ss,"_sim.csv",sep="")
    
    # if we performed raking, read in unscaled files
    if (! cc %in% nonparents$ihme_loc_id){
      file2 <- paste(data_dir, ifelse(hivuncert==T,"/compiled",""),"/gpr_",cc,"_",ss,"_not_scaled.csv",sep="")
      file_sim2 <- paste(data_dir, ifelse(hivuncert==T,"/compiled",""),"/gpr_",cc,"_",ss,"_sim_not_scaled.csv",sep="")
    }
    if (file.exists(file) ) {
      i <- i + 1
      data[[i]] <- fread(file)
      if (! cc %in% nonparents$ihme_loc_id){
        j <- j + 1
        unscaled[[j]] <- fread(file2)
      }
    } else {
      cat(paste("Does not exist:", file,"\n")); flush.console()
      file_errors <- file_errors + 1
    }
    if(! file.exists(file_sim) ) {
      cat(paste("Does not exist:", file_sim,"\n")); flush.console()
      file_errors <- file_errors + 1
    }
  }
}


data <- as.data.frame(rbindlist(data, use.names = T, fill = T))

unscaled <- as.data.frame(rbindlist(unscaled, use.names = T, fill = T))
unscaled <- select(unscaled, ihme_loc_id, sex, year, unscaled_mort = mort_med)
data <- merge(data,unscaled, by = c("ihme_loc_id","sex","year"), all.x = T)
data$unscaled_mort[is.na(data$unscaled_mort)] <- data$mort_med[is.na(data$unscaled_mort)]


## Check whether we have a full dataset
years <- unique(data$year)
sexes <- unique(data$sex)
countries <- unique(codes$ihme_loc_id)

counter = length(years) * length(sexes) * length(countries)
length_dataset <- nrow(data[!is.na(data$mort_med),])

if(length_dataset == counter & file_errors == 0) {
  print("All observations are accounted for")
} else {
  stop(paste0("We are expecting ",counter," observations but get ", length_dataset, " instead, and ", file_errors, " files are missing"))
  print(unique(data$ihme_loc_id[is.na(data$mort_med)]))
  print(unique(data$year[is.na(data$mort_med)]))
}

## save final file
data <- data[order(data$ihme_loc_id, data$sex, data$year),]

without_covariate_file <- paste0(output_dir, "/draws/estimated_45q15_noshocks.csv")
with_covariate_file <- paste0(output_dir, "/draws/estimated_45q15_noshocks_wcovariate.csv")
fwrite(data[,c("ihme_loc_id","sex","year","mort_med","mort_lower","mort_upper","unscaled_mort")], file=without_covariate_file)
fwrite(data[,c("ihme_loc_id","sex","year","mort_med","mort_lower","mort_upper","unscaled_mort","med_stage1","med_stage2")], file=with_covariate_file)