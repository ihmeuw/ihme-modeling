######################
##Purpose: quick function for expanding rows that cover many age groups by age group
##  source("FILEPATH/utility/expand_pops.R")
########################


expand_pops<-function(df, expand_sex = F){
  df<-copy(df)

  ################### SCRIPTS #########################################
  #################################################################

  require(dplyr)
  central<-paste0(j, "FILEPATH")

  source(paste0(central, "get_population.R"))
  source(paste0(central, "get_ids.R"))


  ################### CHECK FOR NECESSARY COLUMNS #########################################
  #################################################################
  nec_cols<-c("age_start", "age_end", "sex_id", "location_id")

  #if(!all(nec_cols %in% names(df))){stop("Missing at least one necessary column")}

  for(col in nec_cols){
    if(!col %in% names(df)){stop("Your df doesn't contain a column named", col, "!")}
  }

  df[, sex_id:=as.integer(sex_id)]

  ## make sure new cols don't already exist
  create_cols<-c("pops_split_id")
  for(col in create_cols){
    if(col %in% names(df)){stop("Your df contains a column named ", col, ", please rename and re-run as this column needs to be created")}
  }

  ################### SETUP AND CLEAN COLS #########################################
  #################################################################

  df[, pops_split_id:=seq_len(nrow(df))]
  meta_cols<-setdiff(names(df), c("age_start", "age_end", "sex_id", "location_id", "year_id"))
  meta<-df[, c(meta_cols), with=F]

  df.exp<-copy(df)

  df.exp<-df.exp[, .(pops_split_id, age_start, age_end, sex_id, location_id, year_id)]
  ## clean age_start and age_end
  df.exp[, age_start:=age_start-age_start %% 5]
  df.exp[, age_end:=age_end-age_end %% 5+4]

  ## expand df for each necessary row--this is from the age sex splitting function
  df.exp[, n.age := (age_end + 1 - age_start)/5]  ## calculates the number of 5 year age groups that a row holds and assigns it to the n.age col


  ################### EXPAND BY AGE #########################################
  #################################################################
  ## Expand for age
  #df.exp[, age_start_floor := age_start]
  expanded <- rep(df.exp$pops_split_id, df.exp$n.age) %>% data.table("pops_split_id" = .)  ## create a column called split_id that will connect rows that came initially from same row
  ##for the rep() function, read 'replicate the value in split$split_id the number of times equal to the value in split$n.age'
  df.exp <- merge(expanded, df.exp, by="pops_split_id", all=T)   ## merge 'expanded' with 'split' on 'pops_split_id' so that the right amount of new rows can be made from the orignal row in 'split'
  df.exp[, age.rep := 1:.N - 1, by=.(pops_split_id)]            ## create 'age.rep' col, describes the iteration number of a split by age from the original row
  df.exp[, age_start := age_start + age.rep * 5 ]         ## makes new appropriate age_starts for the new rows
  df.exp[, age_end :=  age_start + 4 ]                  ## makes new appropriate age_ends for the new rows
  df.exp[, c("n.age", "age.rep") := NULL]


  ################### EXPAND BY SEX #########################################
  #################################################################
  if(expand_sex==T){
    ## Expand for sex
    df.exp[, sex_split_id := paste0(pops_split_id, "_", age_start)]              ## creates a col with a unique value for each row, describes the split_id (maps to the original row) and age_start
    #split<-split[!is.na(sex_id)]  
    df.exp[, n.sex := ifelse(sex_id==3, 2, 1)]      ## if sex_id is "both", assign it a 2, if it is 2 or 1, assign 1.  So tells number of sexes described.

    expanded <- rep(df.exp$sex_split_id, df.exp$n.sex) %>% data.table("sex_split_id" = .)  ## create 'expanded' again, this time with column 'sex_split_id', with repititions=split$n.sex
    df.exp <- merge(expanded, df.exp, by="sex_split_id", all=T)                           ## again, merge 'expanded' onto split, this time creating a row for each unique sex_split_id
    df.exp <- df.exp[sex_id==3, sex_id := 1:.N, by=sex_split_id]   ## replaces any sex_id==3 with 1 or 2, depending on if it is the 1st or 2nd new row for the unique sex_split_id
    ## remember that here, sex="sex_id".  also there is one sex_split_id per original age-country-year row (age was split above)
    df.exp[, c("n.sex", "sex_split_id"):=NULL]
  }

  ################### MERGE ON AGE GROUP IDS AND FORMAT #########################################
  #################################################################

  message("  Getting age_group_ids..")
  ## get age_group_ids and reformat
  invisible(age_ids<-get_ids(table="age_group"))
  message(" Done")
  suppressWarnings(age_ids[, age_start:=as.numeric(unlist(lapply(strsplit(age_ids$age_group_name, "to"), "[", 1)))])
  suppressWarnings(age_ids[, age_end:=as.numeric(unlist(lapply(strsplit(age_ids$age_group_name, "to"), "[", 2)))])
  ## merge
  df.exp<-merge(df.exp, age_ids[!is.na(age_start) & age_end-age_start==4, .(age_group_id, age_start, age_end)], by=c("age_start", "age_end"), all.y=F, all.x=T)

  ## change age group for pops
  df.exp[age_group_id==236, age_group_id:=1]
  df.exp[age_group_id %in% c(44, 33, 45, 46) , age_group_id:=235]
  df.exp[age_start>100 , age_group_id:=235]


  ################### MERGE ON POPULATIONS AND METADATA #########################################
  #################################################################
  message("  Getting populations..")
  pops<-get_population(age_group_id=-1, year_id=unique(df$year_id), location_id=unique(df$location_id), sex_id=c(1,2,3))
  message(" Done")
  df.exp<-merge(df.exp, pops, by=c("age_group_id", "year_id", "location_id", "sex_id"), all.x=T)
  df.exp<-merge(df.exp, meta, by="pops_split_id")
  df.exp[, total_pop:=sum(population), by=pops_split_id]

  df.exp[, pops_split_id:=NULL]

  return(df.exp)
}
