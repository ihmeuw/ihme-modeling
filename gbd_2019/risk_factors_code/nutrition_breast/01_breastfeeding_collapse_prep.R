########################################################################################################################
## Project: RF: Suboptimal Breastfeeding
## Purpose: Generate Breastfeeding Indicators for Collapsing
########################################################################################################################
## Breastfeeding Indicators: 
## 1) abf - Proportion of children receiving any breastmilk currently
## 2) ebf - Proportion of children under 6 months who are exclusively breastfed (only breastmilk + medicines/ORS)
## 3) predbf - Proportion of children under 6 months who are predominantly breastfed (breastmilk + other liquids)
## 4) partbf - Proportion of children under 6 months who are partially breastfed (breastmilk + other foods/liquids)
########################################################################################################################


## Setup, load & clean data -------------------------------------------------------------------------
message("Setting up environment...")
rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
}

# set arguments 
input_dir <- file.path(paste0("FILEPATH"))      
output_dir <- file.path(paste0("FILEPATH"))    
if (!dir.exists(output_dir)) dir.create(output_dir, recursive=TRUE)


# load libraries & functions 
library(data.table)
missing <- data.frame()

# load data 

files <- list.files(input_dir)
files <- files[files %like% "MACRO_DHS" | files %like% "MIS"]

for (file in files) {
  message(paste0("Loading input dataset ",file))
  data <- fread(file.path(paste0(input_dir,file)))
  if ((data$survey_name %like% "DHS" | data$survey_name %like% "MIS") & "b9" %in% names(data)){
    data <- data[b9=="respondent"|b9=="Respondent"|b9=="yes"|b9=="Yes"]
  } else if ((data$survey_name %like% "DHS" | data$survey_name %like% "MIS") & "qb21" %in% names(data)){
    data <- data[qb21=="respondent"|qb21=="Respondent"|qb21=="yes"|qb21=="Yes"]  
  } else if ((data$survey_name %like% "DHS" | data$survey_name %like% "MIS") & "q202" %in% names(data)){
    data <- data[q218=="respondent"|q218=="Respondent"|q218=="yes"|q218=="Yes"]  
  }
  if (!("caseid" %in% names(data))) message(paste0("caseid not present in file ", file))
  if (!("caseid" %in% names(data))) next
  data <- data[!duplicated(caseid)]
  message(paste0("Input data ",file," starts with ", nrow(data), " rows."))

  if (("age_month" %in% names(data))) {
    # clean data #
    message("Cleaning up dataset...")
    if (!("age_year" %in% names(data))) data$age_year <- NA
  
    # drop rows missing age, or with age over 5 #
    message(paste0("Dropping ", nrow(data[is.na(age_year) & is.na(age_month),]), " rows with no age information"))
    data <- data[!is.na(age_year) | !is.na(age_month),]
    message(paste0("Dropping ", nrow(data[age_year >= 5 & (age_month >= 60 | is.na(age_month)),]), " rows with neither age_year/age_month under 5y/60mo."))
    data <- data[age_year < 5 | age_month < 60,]
    message(paste0("Dropping ", nrow(data[age_year >= 5 & age_month < 60,]), " rows with age_month under 60 but age_year over 5."))
    data <- data[!(age_year >=5 & age_month < 60),]
    message(paste0("Dropping ", nrow(data[age_month >= 60 & age_year < 5,]), " rows with age_year under 5 but age_month over 60."))
    data <- data[(age_month < 60 | is.na(age_month)) & age_year < 5,]
    
    
    ## Generate all indicator variables -----------------------------------------------------------------
    # clean up recoded BF duration variable from DHS surveys 
    message("Reassigning BF duration variables from DHS surveys...")  
    if (!"bf_dur" %in% names(data)) data$bf_dur <- NA
    if (!"bf_still" %in% names(data)) data$bf_still <- NA
    data[, dhs_recode := ifelse(min(bf_dur, na.rm=T) < 0, 1, 0), by=c("nid")]
      #still breastfeeding (bf_dur = -1)
      data[bf_dur == -1 & dhs_recode == 1 & is.na(bf_still), bf_still := 1]
      data[bf_dur == -1 & dhs_recode == 1, bf_dur := NA]
      data[bf_dur != 0 & dhs_recode == 1 & is.na(bf_still), bf_still := 0]
      #never breastfed (bf_dur = -2)
      data[bf_dur == -2 & dhs_recode == 1, bf_dur := 0]
      #breastfed until died (bf_dur = -3)
      data[bf_dur == -3 & dhs_recode == 1, bf_dur := NA] # leaving todeaths values as NA for now
      data$bf_still <- as.numeric(data$bf_still)
    # clean up food and liquid variables 
    message("Assigning binary Y/N values to food & liquid variables...")
    if ("food_24h" %in% names(data) & "liq_24h" %in% names(data)) {
      
      # list all indicators we want to collapse in the 'indics' vector below 
      indics <- c("ebf", "predbf", "partbf", "bf") 
      message(paste(c("Generating indicators:", indics), collapse=" "))
        # ebf 
        if("ebf" %in% indics) {
          #keep only rows with age_month under 6 months 
          ebf <- data[age_month < 6 ] 
          ebf[, hasvars := ifelse(length(na.omit(bf_still)) + length(na.omit(food_24h)) + length(na.omit(liq_24h)) > 0, 1, 0), by=c("nid")] #mark surveys with necessary variables
          ebf[hasvars == 1 & !is.na(bf_still) & !is.na(food_24h) & !is.na(liq_24h), value := 0] #generate indicator
          ebf[hasvars == 1 & bf_still == 1 & food_24h == 0 & liq_24h == 0, value := 1]
          if ("bf_still" %in% names(ebf)) ebf <- ebf[bf_still==1]
          ebf[hasvars == 0, value := bf_excl] #use bf_excl as a 2nd option (mostly LSMS surveys)
          ## --> bf_still & food/drink variable combination is 1st option, using bf_excl if those aren't present
        }
        # predbf 
        if("predbf" %in% indics) {
          #keep only rows with age_month under 6 months 
          predbf <- data[age_month < 6 ] 
          predbf[, hasvars := ifelse(length(na.omit(bf_still)) + length(na.omit(food_24h)) + length(na.omit(liq_24h)) > 0, 1, 0), by=c("nid")] #mark surveys with necessary variables
          predbf[hasvars == 1, value := 0] #generate indicator
          predbf[hasvars == 1 & bf_still == 1 & food_24h == 0 & liq_24h == 1, value := 1]
          if ("bf_still" %in% names(predbf)) predbf <- predbf[bf_still==1]
          ## --> this represents the proportion of kids 'predominantly' breastfeeding: breastmilk + liquids, no food
        }
        # partbf #
        if("partbf" %in% indics) {
          #keep only rows with age_month under 6 months 
          partbf <- data[age_month < 6 ] 
          partbf[, hasvars := ifelse(length(na.omit(bf_still)) + length(na.omit(food_24h)) + length(na.omit(liq_24h)) > 0, 1, 0), by=c("nid")] #mark surveys with necessary variables
          partbf[hasvars == 1, value := 0] #generate indicator
          partbf[hasvars == 1 & bf_still == 1 & food_24h == 1, value := 1]
          if ("bf_still" %in% names(partbf)) partbf <- partbf[bf_still==1]
          ## --> this represents the proportion of kids 'partially' breastfeeding: breastmilk + food and/or liquids
        }
        # bf #
        if("bf" %in% indics) {
          bf <- data[age_month < 24 ]
          bf[, askstill := ifelse(length(na.omit(bf_still)) > 0, 1, 0), by = c("nid")] #only keep surveys that asked the still_bf question
          bf <- bf[askstill == 1,]
          bf[, value := bf_still]
          ## generate any breastfeeding splits needed for modeling
          abf0_5 <- bf[age_month < 6,]                               #any breastfeeding in first six months
          abf6_11 <- bf[age_month >= 6 & age_month < 12,]            #any breastfeeding in second six months
          abf12_23 <-  bf[age_month >= 12 & age_month < 24,]         #any breastfeeding in second year
        }
    }
    else if ("bf_excl" %in% names(data)) {
      indics <- c("ebf","bf")
      message(paste(c("Generating indicators:",indics),collapse=" "))
      if("ebf" %in% indics) {
        #keep only rows with age_month under 6 months 
        ebf <- data[age_month < 6 ] 
        if ("bf_still" %in% names(ebf)) ebf <- ebf[bf_still==1]
        ebf[,value := bf_excl] #use bf_excl as a 2nd option (mostly LSMS surveys)
        ## --> bf_still & food/drink variable combination is 1st option, using bf_excl if those aren't present
      }
      if("bf" %in% indics) {
        bf <- data[age_month < 24 ]
        bf[, askstill := ifelse(length(na.omit(bf_still)) > 0, 1, 0), by = c("nid")] #only keep *surveys* that asked the still_bf question
        bf <- bf[askstill == 1,]
        bf[, value := bf_still]
        ## generate any breastfeeding splits needed for modeling
        abf0_5 <- bf[age_month < 6,]
        abf6_11 <- bf[age_month >= 6 & age_month < 12,]              #any breastfeeding in second six months
        abf12_23 <-  bf[age_month >= 12 & age_month < 24,]           #any breastfeeding in second year
      }
    }
    else {
      indics <- c("bf")
      message(paste(c("Generating indicators:", indics), collapse=" "))
      if("bf" %in% indics) {
        bf <- data[age_month < 24 ]
        bf[, askstill := ifelse(length(na.omit(bf_still)) > 0, 1, 0), by = c("nid")] #only keep *surveys* that asked the still_bf question
        bf <- bf[askstill == 1,]
        bf[, value := bf_still]
        ## generate any breastfeeding splits needed for modeling
        abf0_5 <- bf[age_month < 6,]
        abf6_11 <- bf[age_month >= 6 & age_month < 12,]              #any breastfeeding in second six months
        abf12_23 <-  bf[age_month >= 12 & age_month < 24,]           #any breastfeeding in second year
        }
      }
    
    ## Format data & save --------------------------------------------------------------------------------
    message("Final formatting & saving data...")
    
    if ("food_24h" %in% names(data) & "liq_24h" %in% names(data)) {
    
      message("Saving abfrate0to5")
      data <- abf0_5
      i <- "abfrate0to5"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      
      message("Saving abfrate6to11")
      data <- abf6_11
      i <- "abfrate6to11"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
  
      
      message("Saving abfrate12to23")
      data <- abf12_23
      i <- "abfrate12to23"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
  
      
      message("Saving ebfrate0to5")
      data <- ebf
      i <- "ebfrate0to5"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
  
      
      message("Saving predbfrate0to5")
      data <- predbf
      i <- "predbfrate0to5"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
  
      
      message("Saving partbfrate0to5")
      data <- partbf
      i <- "partbfrate0to5"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
    }
    else if ("bf_excl" %in% names(data)) {
      
      message("Saving abfrate0to5")
      data <- abf0_5
      i <- "abfrate0to5"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      
      message("Saving abfrate6to11")
      data <- abf6_11
      i <- "abfrate6to11"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      
      
      message("Saving abfrate12to23")
      data <- abf12_23
      i <- "abfrate12to23"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      
      message("Saving ebfrate0to5")
      data <- ebf
      i <- "ebfrate0to5"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      
    }
    else {
      message("Saving abfrate0to5")
      data <- abf0_5
      i <- "abfrate0to5"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      
      message("Saving abfrate6to11")
      data <- abf6_11
      i <- "abfrate6to11"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      
      message("Saving abfrate12to23")
      data <- abf12_23
      i <- "abfrate12to23"
      ifelse(!dir.exists(paste0(output_dir,i)), dir.create(paste0(output_dir,i)), FALSE)
      write.csv(data, file = paste0(output_dir,i,"/",file), row.names=F)
      }

    
    message(paste0("Indicators successfully generated for ",file))
  }
  
  else {
    message(paste0("File ",file," is missing age in months."))
    miss <- data.frame(file)
    missing <- rbind(missing, miss)
  }

}

message("Writing list of files without age_month to ",output_dir)
missing <- unique(missing)
write.csv(missing, paste0(output_dir,"/missing_age_month.csv"))
message("Success! Indicator generation complete.")