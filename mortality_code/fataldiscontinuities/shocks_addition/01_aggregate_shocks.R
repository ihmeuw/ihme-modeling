## Author:NAME 
## Date:  March 8, 2016
## Purpose: Aggregate the shock deaths post CoD-Correct so that we have with-shock results at region/super-region/global/country(for subnats) level
##          These files will feed into the add_shocks
## Parallelized by year, 5/1/2017 by NAME 
##


if (Sys.info()[1] == 'Windows') {
  username <- "USER"
  root <- ""
  source("PATH")
  source("PATH")
  source("PATH")
} else {
  username <- Sys.getenv("USER")
  root <- ""
  source("PATH")
  source("PATH")
  source("PATH")
  output_version_id <- commandArgs()[3]
}
# for interactive runs 
# output_version_id <- 25

library(data.table)
code_dir <- paste("PATH", username, "PATH", sep="")  
r_shell <- "r_shell.sh"
errout <- T
errout_paths <- paste0("-o PATH",username,
                       "/output -e PATH",username,"/errors ")

locs <- get_locations(level= "lowest")
loc_id <- locs[, colnames(locs) %in%  c("location_id")] 
##########  4/23/17 Code should use above locations and variables, not those below...
# locations <- get_locations(level="all")
# locations2 <- get_locations(level="all", gbd_type = "sdi") ## Ready for second round,  2/23/17 
# locations <- rbind(locations,locations2[!locations2$location_id %in% unique(locations$location_id),])


## Compiling shocks numbers files

missing_files <- c()
compiled_shock_numbers <- list()
for (loc in loc_id){
  file <- paste0("PATH", output_version_id ,"PATH", loc, ".csv")
  if(file.exists(file)){ 
    shocks <- fread(file)
    shocks <- shocks[cause_id==294]
    shocks <- shocks[,cause_id:=NULL]
    compiled_shock_numbers[[paste0(file)]] <- shocks
    cat(paste0(file, "\n")); flush.console()
  } else {
    missing_files <- c(missing_files, file)
  }
}

if(length(missing_files)>0) stop("Files are missing.")


## Creating one shocks file with all lowest level locations
compiled_shock_numbers <- rbindlist(compiled_shock_numbers)
row.names(compiled_shock_numbers) <- 1:nrow(compiled_shock_numbers)

draws <- grep("draw", names(compiled_shock_numbers), value=T)
################################### parallel by year ##########################
years <- c(1970:2016)


for (year in (years)) {
    print(paste0("writing year ", year))
    one_year = compiled_shock_numbers[compiled_shock_numbers$year_id == year,]
    write.csv(one_year, paste0("PATH", year, ".csv"))
}
## Using aggregation function to create file with all aggregated locations -- by year

## submit calls to agg_results.R by year
setwd(code_dir)
jlist1 <- c()
mycores <- 6
proj <- "-P proj_shocks" 
for (year in (years)) {
        jname <- paste("aggregate_shock_locations_year_", year, sep="")
        sys.sub <- paste0("qsub ",ifelse(errout,errout_paths,""),"-cwd ", proj, " -N ", jname, " ", "-pe multi_slot ", mycores, " ", "-l mem_free=", 2 * mycores, "G ")
        script <- "01.5_agg_locations_by_year.R"
        args <- paste(year, sep=" ")
        system(paste(sys.sub, r_shell, script, args))
        jlist1 <- c(jlist1, jname)
}

# compile the files that have been aggregated by year

missing_files <- c()
compiled_shock_numbers <- list()
for (year in (years)) {
    file <- paste0("PATH", year, "_aggregates.csv")
    if(file.exists(file)){ 
        shocks <- fread(file)
        compiled_shock_numbers[[paste0(file)]] <- shocks
        cat(paste0(file, "\n")); flush.console()
    } else {
        missing_files <- c(missing_files, file)
    }
}

if(length(missing_files)>0) {
    print(missing_files)
    stop("Files are missing.")
}
## Creating one shocks file with all lowest level locations
compiled_shock_numbers <- rbindlist(compiled_shock_numbers)
compiled_shock_numbers[, V1:= NULL]
#####################################################################

## Subsetting locations to only aggregate level locations
## And writing those files
compiled_shock_numbers <- compiled_shock_numbers[!location_id %in% loc_id]


for(locs in unique(compiled_shock_numbers$location_id)){
  write.csv(compiled_shock_numbers[location_id==locs], paste0("PATH" , output_version_id, "/shocks_", locs, ".csv"), row.names=F)
}




