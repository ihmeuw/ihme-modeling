#####################################################################################
### Project: Anemia 
### Purpose: Master data prep script for mean hemoglobin and anemia prevalence models
#####################################################################################

##################
##### SET-UP #####
##################
rm(list=ls())
pacman::p_load(ggplot2, data.table, magrittr, dplyr, msm, openxlsx)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
  k <- "FILEPATH"
}

## Source Central Functions
setwd(paste0(k,"FILEPATH"))
funcs <- c("get_bundle_data","upload_bundle_data","save_bundle_version","get_bundle_version","save_crosswalk_version","get_age_metadata","get_population","get_ids",
           "get_location_metadata")
for (i in funcs) source(paste0(i,".R"))

## Source General/Custom Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/custom_db.R")
source("FILEPATH/xw_pregnant_mrbrt.R")

## Define Functions

prep_age_sex_split <- function(df, bundle_id){
  message(paste0("Prepping data for age and sex splitting for bundle ",bundle_id))
  ages <- get_age_metadata(19)[,.(age_group_id,age_group_years_start,age_group_years_end)]
  startyears <- sort(ages$age_group_years_start)
  endyears <- sort(ages$age_group_years_end)
  
  data <- copy(df)
  data[, age_start := orig_age_start]
  data[, age_end := orig_age_end]
  data[, age_group_id := NULL]
  df_dups <- data[,c("nid","location_id","sex","age_start","age_end","year_start","year_end","val")]
  data <- data[!duplicated(df_dups)]
  
  grab_floor <- function(num){
    for (i in rev(startyears)){
      if (i <= num){
        return(i)
      }
    }
  }
  grab_ceil <- function(num){
    for (i in endyears){
      if (i >= num){
        return(i)
      }
    }
  }
  
  data[, age_floor := as.numeric(lapply(data$age_start,grab_floor))]
  data[, age_ceil := as.numeric(lapply(data$age_end,grab_ceil))]
  
  data[age_start==1 & age_end==1, age_ceil := 2]
  data[age_end <= 5 & age_end >=2, age_ceil := 5]
  
  data <- merge(data,ages[,.(age_group_id,age_group_years_start)],by.x="age_floor",by.y="age_group_years_start",all.x=T)
  setnames(data,"age_group_id","age_group_id_floor")
  data <- merge(data,ages[,.(age_group_id,age_group_years_end)],by.x="age_ceil",by.y="age_group_years_end",all.x=T)
  setnames(data,"age_group_id","age_group_id_ceil")
  data$age_group_id_floor <- mapvalues(data$age_group_id_floor, from=c(30,31,32,235,34,238,388,389,2,3), to=c(21,22,23,24,5,4,2,3,0,1))
  data$age_group_id_ceil <- mapvalues(data$age_group_id_ceil, from=c(30,31,32,235,34,238,388,389,2,3), to=c(21,22,23,24,5,4,2,3,0,1))
  
  data[, n_age := age_group_id_ceil - age_group_id_floor + 1] 
  data[, good_range := ifelse(age_group_id_floor==age_group_id_ceil,1,0)]
  data[, good_sex := ifelse(sex=="Both",0,1)]
  data[, good_cols := good_range + good_sex]
  data[, to_train := ifelse(good_cols==2,1,0)]
  data[, to_split := ifelse(good_cols < 2,1,0)]
  data[to_train==1 & is_outlier==1, to_train := 0]
  data[, sex_id := ifelse(sex=="Both",3,ifelse(sex=="Male",1,2))]
  data[, crosswalk_parent_seq := seq]
  
  message(paste0("Data prepped for age and sex splitting! Writing out to FILEPATH.csv"))
  write.csv(data,paste0("FILEPATH.csv"), row.names=F)
  return(data)
}

age_sex_split <- function(df, bundle_id, decomp_step){
  
  ## Get population estimates and format
  message("Getting populations")
  pops <- get_population(year_id=unique(df$year_id), sex_id=c(1, 2, 3), location_id=unique(df$location_id), age_group_id='all',location_set_id=22, status="best", gbd_round_id=7, decomp_step=decomp_step)
  pops[, run_id:=NULL]
  
  ## Link age_group_id with age start/end
  age_groups <- get_age_metadata(19)[,.(age_group_id,age_group_years_start,age_group_years_end)]
  setnames(age_groups,c("age_group_years_start","age_group_years_end"),c("age_start","age_end"))
  pops <- merge(pops, age_groups, by="age_group_id")
  pops <- subset(pops, age_group_id %in% c(2,3, 6:20, 30:32, 34, 235, 238, 388, 389))

  ## Set-Up Data
  # Generate unique ID for easy merging
  df[, split_id := 1:.N]  
  
  # Save original values
  orig <- c("age_start", "age_end", "sex_id", "val", "sample_size", "age_floor", "age_ceil", "age_group_id_floor", "age_group_id_ceil", "n_age")
  orig.cols <- paste0("orig.", orig)
  df[, (orig.cols) := lapply(.SD, function(x) x), .SDcols=orig]  
  
  # Separate metadata from required variables
  cols <- c("location_id", "year_id", "age_start", "age_end", "sex_id", "val", "sample_size", "age_floor", "age_ceil", "age_group_id_floor", "age_group_id_ceil", "n_age") 
  meta.cols <- setdiff(names(df), cols)  
  metadata <- df[, meta.cols, with=F] 
  data <- df[, c("split_id", "to_train", "to_split", cols), with=F] 
  
  ## Set up Train and Test Data
  unsplit <- data[to_train==0&to_split==0]  ## are neither test nor train
  training<-data[to_train==1]
  split <- data[to_split==1]  
  
  ## Create Age-Sex Pattern
  asp <- data.table(aggregate(training[["val"]],by=lapply(training[,c("age_floor", "sex_id"), with=F],function(x)x),FUN=mean,na.rm=TRUE))
  asp[sex_id==1, sex:="Male"]
  asp[sex_id==2, sex:="Female"]
  names(asp)[3] <- "rel_est"
  
  asp <- dcast(asp, formula(paste0("age_floor"," ~  sex")), value.var="rel_est")  
  setnames(asp, c("Male", "Female"), c("1", "2"), skip_absent = TRUE)
  
  # Fill NAs with values from adjacent age/sex groups
  asp[is.na(asp[[1]]), 1] <- asp[is.na(asp[[1]]),2]    
  asp[is.na(asp[[2]]), 2] <- asp[is.na(asp[[2]]),1]
  asp <- melt(asp,id.var=c("age_floor"), variable.name="sex_id", value.name="rel_est")  
  asp$sex_id <- as.integer(as.character(asp$sex_id))  
  
  ## Graph Age-Sex Pattern
  asp.t <- copy(asp)
  asp.t[, sex := ifelse(sex_id==1,"Male","Female")]
  
  pdf(paste0("FILEPATH.pdf"))
  p <- ggplot(data=asp.t, aes(x=age_floor, y=rel_est)) + geom_point(color=I(rgb(0.4,0.65,0.25))) + 
    facet_wrap(~sex) + labs(title=paste0("Global Age-Sex Pattern, Bundle ",bundle_id)) + xlab("Age") + 
    ylab("Val") + theme_minimal()
  print(p)
  dev.off()

  setnames(asp, old=c("age_floor"), new=c("age_start"))
  asp <- merge(asp, age_groups, by=c("age_start"))  
  asp[, age_end := NULL]
  asp[, age_start := NULL]
  
  ## Set up rows for splitting
  message("Age/Sex Splitting Data")
  
  if (nrow(split) > 0){
    split[, n.sex := ifelse(sex_id==3, 2, 1)]      
    ## Expand for age 
    split[, age_start_floor := "age_floor"]          
    expanded <- rep(split$split_id, split$n_age) %>% data.table("split_id" = .)  # create a column called split_id that will connect rows that came initially from same row
    split <- merge(expanded, split, by="split_id", all=T)   # merge 'expanded' with 'split' on 'split_id' so that the right amount of new rows can be made from the orignal row in 'split'
    split[, age.rep := 1:.N - 1, by=.(split_id)]            # create 'age.rep' col, describes the iteration number of a split by age from the original row
    split[, agid_split:= age_group_id_floor + age.rep]         # makes new appropriate age_starts for the new rows

    ## Expand for sex
    split[, sex_split_id := paste0(split_id, "_", agid_split)]              # creates a col with a unique value for each row, describes the split_id (maps to the original row) and age_start
    expanded <- rep(split$sex_split_id, split$n.sex) %>% data.table("sex_split_id" = .)  # create 'expanded' again, this time with column 'sex_split_id', with repititions=split$n.sex
    split <- merge(expanded, split, by="sex_split_id", all=T) # again, merge 'expanded' onto split, this time creating a row for each unique sex_split_id
    split[,sex_id := as.integer(sex_id)][sex_id==3, sex_id := 1:.N, by=sex_split_id]   # replaces any sex_id==3 with 1 or 2, depending on if it is the 1st or 2nd new row for the unique sex_split_id

    split[, age_start := NULL]
    split[, age_end := NULL]
    setnames(split, old=c("agid_split"), new=c("age_group_id"))
    split$age_group_id <- mapvalues(split$age_group_id,from=c(21,22,23,24,5,4,2,3,0,1),to=c(30,31,32,235,34,238,388,389,2,3))
    split[, age_floor := NULL]
    split[, age_ceil := NULL]
    split[, age_group_id_floor := NULL]
    split[, age_group_id_ceil := NULL]
  
    ## Split by Age and Sex
  
    # Merge on population and the asp, aggregate pops by split_id
    split <- merge(split, pops, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)  
    split <- merge(split, asp, by=c("sex_id", "age_group_id"))                                     
    split[, pop_group := sum(population), by="split_id"]                                         

    ## Calculate R, the single-group age/sex estimate in population space using the age pattern from asp  
    split[, R := rel_est * population]
    split <- split[!is.na(R)]
  
    ## Calculate R_group, the grouped age/sex estimate in population space
    split[, R_group := sum(R), by="split_id"] 
  
    ## Split
    split[, val := val * (pop_group/population) * (R/R_group) ]  
  
    ## Split the sample size
    split[, sample_size := sample_size * population/pop_group]       
    ## Mark as split
    split[, cv_split := 1]
  }
  
  ## Append training
  setnames(training,old=c("age_group_id_floor"), new=c("age_group_id"))
  training$age_group_id <- mapvalues(training$age_group_id,from=c(21,22,23,24,5,4,2,3,0,1),to=c(30,31,32,235,34,238,388,389,2,3))
  
  setnames(unsplit,old=c("age_group_id_floor"), new=c("age_group_id"))
  unsplit$age_group_id <- mapvalues(unsplit$age_group_id,from=c(21,22,23,24,5,4,2,3,0,1),to=c(30,31,32,235,34,238,388,389,2,3))
  
  out <- rbind(split, training, unsplit, fill=T)
  if (!("cv_split" %in% names(out))) out[, cv_split:=0]
  out <- out[is.na(cv_split), cv_split := 0]  # mark anything that was from the training df 
  
  ## Append on metadata
  out <- merge(out, metadata, by="split_id", all.x=T)
  names(out)[names(out)=='to_train.x']<- 'to_train'
  names(out)[names(out)=='to_split.x']<- 'to_split'
  ## Clean
  cols <- cols[!cols=="age_group_id_floor"]
  out <- out[, c(meta.cols, cols, "age_group_id", "cv_split"), with=F]
  out[, sex := ifelse(sex_id==1,"Male",ifelse(sex_id==2,"Female","Both"))]
  out[, split_id := NULL]
  message("Done! Data Age-Sex Split")
  
  split <- out[, !grep("orig.", names(out), value=T), with=F]
  split[is.na(cv_pregnant), cv_pregnant := 0]
  
  if ("group_review" %in% names(split)) split <- split[group_review == 1 | is.na(group_review)]
  
  ## Save
  message(paste0("Data age- and sex-split! Writing out to FILEPATH.csv"))
  write.csv(split,paste0("FILEPATH.csv"), row.names=F)
  return(split)
}

xwalk_pregnant <- function(df,me,folder){
  data <- copy(df)
  data <- prep_data(data)
  data <- match_data(data)
  save_matches(data,me)
  data <- prep_mrbrt(data)
  fit_mrbrt(data,me,folder)
  df <- apply_mrbrt(df,me,folder)
  return(df)
}

upload_xwalk_version <- function(df, bundle_id, bundle_version, description){
  # Edit seq and necessary xwalk columns
  df[, seq := ""]
  df[!is.na(upper), uncertainty_type_value := 95]
  df[, unit_value_as_published := 1]
  df[, unit_value_as_published := as.integer(unit_value_as_published)]
  # write out to xlsx
  FOLDER <- "imp_anemia"
  BUNDLE <- bundle_id
  path <- paste0("FILEPATH.xlsx")
  write.xlsx(df,path,sheetName="extraction")
  # upload
  cvid <- save_crosswalk_version(bundle_version,path,description)
  return(cvid$crosswalk_version_id)
}


###############
##### RUN #####
###############

prep_hemoglobin <- function(me, decomp_step=NA, crosswalk_description=NA) {
  
  ref <- fread("FILEPATH/paths.csv")
  if (me=="anemia") bundle_ids <- c(4754,8108,8120,8117)
  if (me!="anemia"){
    bundle_ids <- ref[me_names==me,bundle_id]
  }
  
  for (bid in bundle_ids){
    message(paste0("Pulling bundle version data for bundle ",bid))
    bvid <- save_bundle_version(bundle_id=bid, decomp_step=decomp_step, gbd_round_id=7)
    df <- get_bundle_version(bvid$bundle_version_id, fetch="all")
    df <- prep_age_sex_split(df, bundle_id=bid)
    df <- age_sex_split(df, bid, decomp_step)
    df[val==0,val:=.001]
    df <- xwalk_pregnant(df,bun_id,folder="hemoglobin_5yr")
    cvid <- upload_xwalk_version(df,bundle_id=bid,bundle_version=bvid$bundle_version,description=crosswalk_description)
    ref[bundle_id==bid,crosswalk_version_id:=cvid]
  }
  write.csv(ref,"FILEPATH/paths.csv",row.names=F)
}