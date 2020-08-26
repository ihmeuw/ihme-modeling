########################################################################################################################
## Date: 05/21/2018 | Updated: 06/26/2019
## Project: Child Covariates
## Purpose: Generate Child Care Indicators for Collapsing
########################################################################################################################
## REQUIRED INPUT:
##    me           ->  ME group for which you wish to generate indicators ("diarrhea"/"lri")
##    extract.path ->  file path to where extracted data resides
##    output.path  ->  file path to where indicator-generated data should be stored pre-collapse                     
########################################################################################################################
## Child Care Indicators: 
## DIARRHEA
## 1) ORS - proportion of children with diarrhea treated with oral rehydration solution
## 2) Zinc - proportion of children with diarrhea treated with zinc
## 3) Antibiotics - proportion of children with diarrhea treated with antibiotics
## LRI
## 1) Antibiotics - proportion of children with suspected LRI treated with antibiotics
########################################################################################################################


collapse_prep <- function(me,extract.path,output.path){
  
  ###################
  ### Setting up ####
  ###################
  message("Setting up environment...")
  pacman::p_load(data.table, dplyr, parallel)
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "ADDRESS"
    h <- "ADDRESS"
  } else {
    j <- "ADDRESS"
    user <- Sys.info()[["user"]]
    h <- paste0("ADDRESS", user)
  }

  ## Indicators lists
  indicators <- list()
  indicators[['diarrhea']] <- c("ors", "zinc", "antibiotics")
  indicators[['lri']] <- c("lri1", "lri2", "lri3", "lri4")
  
  ## LRI reference
  lri.symp <- c("had_fever", "had_cough", "diff_breathing", "chest_symptoms")
  ## Four definitions of LRI
  ## 1. Cough, difficulty breathing, chest, fever
  lri1 <- c('had_cough', 'diff_breathing', 'chest_symptoms', 'had_fever')
  ## 2. Cough, difficulty breathing, chest
  lri2 <- c('had_cough', 'diff_breathing', 'chest_symptoms')
  ## 3. Cough, difficulty breathing, fever
  lri3 <- c('had_cough', 'diff_breathing', 'had_fever')
  ## 4. Cough, difficulty breathing
  lri4 <- c('had_cough', 'diff_breathing')

  ## Other reference
  error <- data.frame()
  count <- 0

  #################################
  ########DEFINE FUNCTIONS#########
  #################################
  
  ## Prep for indicator generation
  collapse.prep <- function(df) {
    ## If child_age_year and child_age_sex, replace
    if ("child_age_year" %in% names(df)) df <- df[, age_year := child_age_year]
    if ("child_sex_id" %in% names(df)) df <- df[, sex_id := child_sex_id]
    return(df)
  }
  
  ## Check for errors before generating indicators for collapse
  collapse.check <- function(df, me) {
    continue <- FALSE
    error <- ""
    ## TOPIC SPECIFIC CHECKS
    if (me %in% "diarrhea") {
      source <- "diarrhea_tx_type_mapped"
      indicators <- indicators[[me]]
      check1<- ifelse(source %in% names(df), 1, 0) ## If no source variable
      if (check1 == 0) error <- paste0(error, ";No source variable available")
      check2 <- ifelse(any(!is.na(df[[source]])), 1, 0) ## If source variable completely missing
      if (check2 == 0) error <- paste0(error, ";Source variable completely missing")
      check3 <- ifelse(any(grepl(paste0(indicators, collapse="|"), df[[source]])), 1, 0) ## If no zinc, ors, anti
      if (check3 == 0) error <- paste0(error, ";No indiators of interest in source variable")
      ## Store checklist
      checklist <- c(check1, check2, check3)
    }
    if (me %in% "lri") {
      source <- "lri_tx_type_mapped"
      indicators <- indicators[[me]]
      check1 <- ifelse(all(lri4 %in% names(df)), 1, 0) ## Dataset has to have at least cough and difficulty breathing
      if (check1 == 0) error <- paste0(error, ";Needs cough and difficulty breathing")
      check2 <- ifelse(source %in% names(df), 1, 0) ## If no source variable
      if (check2 == 0) error <- paste0(error, ";No source variable available")
      check3 <- ifelse(any(!is.na(df[[source]])), 1, 0) ## If source variable completely missing
      if (check3 == 0) error <- paste0(error, ";Source variable completely missing")
      check4 <- ifelse(lapply(lri4, function(x) !all(is.na(df[[x]]))) %>% unlist %>% all, 1, 0) ## Cough and difficulty breathing not entirely missing
      if (check4==0) error <- paste0(error, ";Cough and difficulty entirely missing")
      ## Store checklist
      checklist <- c(check1, check2, check3, check4)
    }
    ## GENERAL CHECKS
    check1 <- ifelse("age_year" %in% names(df), 1, 0) ## No age year variable
    if (check1 == 0) error <- paste0(error, ";No age_year var")
    if ("age_year" %in% names(df)) check2 <- ifelse(nrow(df[age_year>0 & age_year <= 5])>0, 1, 0) else check2 <- 1
    if (check2 == 0) error <- paste0(error, ";No under 5")
    checklist <- c(checklist, check1, check2)
    ## IF ANY ERROR, PASS COLLAPSE
    continue <- all(checklist==1)
    if (error != "") print(error)
    return(list(continue, error))
  }
  
  ## Generate indicators for collapse
  collapse.gen <- function(df, me) {
    if (me == "diarrhea") {
      source <- "diarrhea_tx_type_mapped"
      indics <- indicators[[me]]
      for (i in indics) {
        if (any(grepl(i, df[[source]]))){
          if ("had_diarrhea" %in% names(df)) {
            df <- df[had_diarrhea==1, (i) := ifelse(grepl(i, get(source)), 1, 0)]
          } else if ("diarrhea_tx" %in% names(df)) {
            df <- df[diarrhea_tx==1, (i) := ifelse(grepl(i, get(source)), 1, 0)]
            df <- df[diarrhea_tx==0, (i) := 0]
          }
        }
      }
    }
    if (me == "lri") {
      source <- "lri_tx_type_mapped"
      indics <- indicators[[me]]
      for (i in indics) {
        ## Check if symptom vars present required for specific definition
        check <- all(get(i) %in% names(df))
        if (check) {
          cond <- paste0(get(i), "==1", collapse=" & ") ## Conditional statement for if all of the symptoms == 1
          df <- df[eval(parse(text=cond)), (i) := ifelse(grepl("antibiotics",get(source)), 1, 0)]
        }
      }
    }
    ## Other custom processing
    ## Round age_year and restrict ages to < 5
    df <- df[, age_year := floor(age_year)]
    if (me != "maternal") df <- df[age_year >=0 & age_year < 5]
    ## For Diarrhea and LRI, group all <5
    if (me %in% c("diarrhea", "lri")) df <- df[, age_year := 5]
    return(df)
  }
  #################################
  #######GENERATE INDICATORS#######
  #################################
  setwd(extract.path)
  files <- list.files()
  check <- list.files(output.path)
  files <- files[!(files %in% check)]
  
  for (file in files){ 
    message(paste0("Loading input dataset ",file))
    data <- fread(file)
    message(paste0("Input data ",file," starts with ",nrow(data)," rows."))
    message("Prepping for Collapse")
    data <- data %>% collapse.prep
    check <- collapse.check(data,me)
    continue <- check[[1]]
    errors <- check[[2]]
    if (continue) {
      data <- collapse.gen(data,me)
      message(paste0("Indicators generated for ",file,"!"))
      message("Writing prepped file to ",output.path)
      write.csv(data,file=paste0(output.path,"/",file),row.names=F)
    } else {
      message(paste0("Error: could not prep ",file))
      out <- data.frame(file,errors)
      error <- rbind(error,out)
      count <- count+1
    } 
  }  
  
  if (count > 0) {
    message("Writing list of files with errors to ",output.path)
    write.csv(error,paste0(output.path,"/errors.csv"),row.names=F)
  } 
  message("Success! Indicator generation complete")
}
