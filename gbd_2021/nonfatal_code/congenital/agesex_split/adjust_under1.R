################################################################################
## Description: Applies under-1 cohort adjustment to mean inpatient clinical data
##
## REQUIRED INPUT:
##    df           ->  dataframe containing bundle data (including inpatient clinical data)
##    bundle_id    ->  bundle ID
################################################################################

adjust_under1 <- function(df, bundle_id, diagnostics=FALSE){
  
  # Source functions and set up
  user <- Sys.info()[["user"]]
  source("FILEPATH/get_population.R")
  source("FILEPATH/get_covariate_estimates.R")
  source("FILEPATH/get_age_metadata.R")
  
  # Format data 
  df <- data.table(df)
  df[,index := 1:.N] # Create index reference variable in full data set
  data <- df[clinical_data_type=="inpatient" & age_end < 1, 
             .(age_start,age_end,sex,location_id,year_start,year_end,nid,mean,index)] # Subset to <1 inpatient data for adjustment
  
  if (nrow(data)==0) print("There is no under-1 inpatient data in your data set!")
  if (nrow(data)==0) next
  
  # Merge on age and population
  data[, year_id := floor((year_start + year_end)/2)] # create a reference year_id for population
  data[, sex_id := ifelse(sex=="Male",1,2)] # create sex_id for easier use
  
  ages <- get_age_metadata(age_group_set_id = 19)[,.(age_group_id,age_group_years_start)]
  pops <- get_population(age_group_id=c(2,3,388,389,164), location_id=unique(data$location_id), 
                         year_id=unique(data$year_id), sex_id=c(1,2), gbd_round_id=7, decomp_step="iterative")[,-c("run_id")]

  data <- merge(data,ages,by.x="age_start",by.y="age_group_years_start",all.x=TRUE)
  data[age_start==0 & age_end==0, age_group_id:=164] 
  data <- merge(data,pops,by=c("location_id","age_group_id","year_id","sex_id"),all.x=TRUE)
  
  data <- data[,`:=` (age_start=NULL, age_end=NULL, sex=NULL, year_start=NULL, year_end=NULL)]
  
  # Transform data to count space and save index info
  data[, mean:=mean*population]
  ref <- data[,.(location_id,age_group_id,sex_id,year_id,nid,index,population)]
  
  # Cast wide by age
  data <- dcast(data, location_id + sex_id + year_id + nid ~ age_group_id, value.var="mean")
  
  # Check for no missing ages
  if (nrow(data[is.na(`2`) | is.na(`3`) | is.na(`388`) | is.na(`389`)]) > 0){
    print(paste0("Error: Dataset is not square by under-1 age groups! Check for rows missing some of the under-1 age groups 
          at FILEPATH/",bundle_id,".csv"))
    write.csv(data,paste0("FILEPATH/",bundle_id,".csv"),row.names=F)
    next
  }
  
  # Make the under-1 cohort adjustment
  data[, `:=` (`2` = `2` + ((1/3)*`3`) + ((1/(5/12*52))*`388`) + ((1/(6/12*52))*`389`),
               `3` = `3` + ((3/(5/12*52))*`388`) + ((3/(6/12*52))*`389`),
               `388` = `388` + ((5/6)*`389`))]
  data[, `164` := `2`*52]
  
  # Melt down by age
  data <- melt(data, id.vars=c("location_id","sex_id","year_id","nid"),
               variable.name="age_group_id",value.name="mean")
  
  # Merge index and pop back on
  data[,age_group_id := as.numeric(levels(age_group_id))[age_group_id]] # revert from factor
  data <- merge(data,ref,by=c("location_id","age_group_id","sex_id","year_id","nid"),all.x=TRUE)

  # Revert to rate space
  data[, mean := mean/population]
  
  # Merge on to original data
  data <- data[,.(index,mean)]
  setnames(data,"mean","mean_u1_adjust")
  df <- merge(df,data,by="index",all.x=TRUE)
  
  # Generate diagnostics if specified
  if (diagnostics){
    print(paste0("Launching diagnostics for bundle ",bundle_id))
    
    # Prep data and write-out
    plot_df <- df[!is.na(mean_u1_adjust), .(location_id,age_start,age_end,sex,mean,mean_u1_adjust)]
    write.csv(plot_df, paste0("FILEPATH/",bundle_id,".csv"),row.names=F)
    
    # Launch job
    command <- paste0("qsub -q all.q -l h_rt=01:30:00 -l m_mem_free=20G -l fthread=5 -l archive=TRUE -P PROJECT -N ", 
                      paste0("clinical_diagnostics_bundle_", bundle_id)," -o FILEPATH ", 
                      "-e FILEPATH", 
                      " FILEPATH/health_fin_forecasting_shell_singularity.sh ", 
                      "FILEPATH/adjust_u1_diagnostics.R ",bundle_id)
    system(command)
  }
  
  # Replace with new vals and clean up columns
  df[!is.na(mean_u1_adjust), mean := mean_u1_adjust]
  df[, `:=` (index=NULL,
             mean_u1_adjust=NULL)]
  
  # Write-out
  print(paste0("Under-1 adjustment successfully applied! Writing out final data set to FILEPATH.csv"))
  output_dir <- paste0("FILEPATH/",bundle_id)
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive=TRUE)
  write.csv(df,paste0(output_dir,"/FILEPATH.csv"),row.names=F)
  
  return(df)
}

## END