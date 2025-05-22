################################################################################################
### Purpose: Prep Microdata for Hearing Loss
#################################################################################################
#rm(list= ls())  
#setup
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())
j <- "FILEPATH"
j_hear <- paste0(j,"FILEPATH", user, "FILEPATH")

j_hear <- paste0("FILEPATH")
install.packages("msm")
library(data.table)
library(openxlsx)
library(plyr)
library(dplyr)
library(boot)
library(msm) 
library(ggplot2)
source("FILEPATH")
source("FILEPATH")

#MAKE ALTERNATE & REFERENCE CATEGORY DT's

map_year <- 2017
prep_alt <- function(map_year){
  alt <- read.xlsx(paste0(j_hear,"FILEPATH",map_year,"FILEPATH")) #standardize map location
  alts <- data.table(alt_low=alt$thresh_lower, alt_high=alt$thresh_upper, ref_low = alt$gbd_lower, ref_high = alt$gbd_upper, meid = alt$modelable_entity_id, ref = alt$ref)
  alts <- alts[!is.na(ref_low)]
  alts[ , `:=` (name_alt = paste0("prev_",alt_low, "_", alt_high), name_ref = paste0("prev_", ref_low, "_", ref_high))]  
  alts[ ,combo_id := seq(1:nrow(alts))]
  return(alts)
}
alts_2017 <- prep_alt(map_year)

refs <- data.table(ref_low= c(0, 20, 35, 50, 65, 80),
                   ref_high = c(19, 34, 49, 64, 79, 94))  

#USING NHANES DATA, CALCULATE PREVALENCE OF EACH SEVERITY CATEGORY BY AGE & SEX
calc_meanby <- c("gbd_age1", "sex") 
nhanes_cleaned <- function(map) {
  micro <- data.table(read.xlsx(paste0(j_hear,"FILEPATH"))) ##reading the dt
  micro <- micro[ ,1:9]                                                         
  micro$year_start <- as.numeric(micro$year_start)                              
  micro <- micro[!is.na(db_loss),]                                              
  micro[ ,gbd_age2 := gbd_age] #collapse for less uncertainty                   
                         

  micro[ , ref_0_19 := ifelse(db_loss <= 19, 1, 0)]                           
  micro[ , "prev_0_19" := mean(ref_0_19), by= c("sex", "gbd_age2")] 

  for (i in 2:nrow(refs)) {
    ref_name <- paste0("ref_",refs$ref_low[i],"_",refs$ref_high[i])
    prev_name <- paste0("prev_",refs$ref_low[i],"_",refs$ref_high[i])
    micro[ , paste0(ref_name) := ifelse(db_loss %inrange% refs[i], 1, 0)]
    micro[ , paste0(prev_name) := mean(get(ref_name)), by=c("sex", "gbd_age2")] #by year_start
  }

  for (i in c(35,95)) {
    ref_name <- paste0("ref_",i,"+")
    prev_name <- paste0("prev_",i,"+")
    micro[ , paste0(ref_name) := ifelse(db_loss >= i, 1, 0)]
    micro[ , paste0(prev_name) := mean(get(ref_name)), by=c("sex", "gbd_age2")] #by year_start
  }

  for (i in 1:nrow(map)) {
    alt_name <- paste0("alt_",map$alt_low[i],"_",map$alt_high[i])
    altprev_name <- paste0("prev_",map$alt_low[i],"_",map$alt_high[i])
    micro[ , paste0(alt_name) := ifelse(db_loss %inrange% map[i, c("alt_low", "alt_high")], 1, 0)]
    micro[ , paste0(altprev_name) := mean(get(alt_name)), by=c("sex", "gbd_age2")] #by year_start
  }
  micro[ ,note_modeler := paste0("prevs grouped by ", paste0(calc_meanby, collapse = ", "))]
  return(micro)
}
micro <- nhanes_cleaned(map = alts_2017)
#WRITE GROUPED NHANES PREVS TO FILE (you should end up w/ 128 cols)
write.xlsx(x=micro, file = paste0(xwalk_temp, "FILEPATH",paste0(calc_meanby, collapse = ""), "_", map_year,".FILEPATH"))

######CREATING DT FOR MRBRT PRE-INPUT (NHANES PREVALENCE ONLY BY AGE, SEX, & THRESHOLD)
dt_prep <-copy(micro)
dt_bert <- data.table(dt_prep[, grep("prev", names(dt_prep)), with = FALSE])
dt_bert <- dt_bert[ , `:=` (age = dt_prep$age, age2 = dt_prep$gbd_age2, sex = dt_prep$sex )] #year = dt_prep$year_start #age2 is for the 50+ collaps
setnames(dt_bert, old = c("prev_35+", "prev_95+"), new = c("prev_35_200", "prev_95_200"))

write.xlsx(dt_bert, file = paste0(xwalk_temp, "FILEPATH", paste0(calc_meanby, collapse = ""), "_", map_year, "FILEPATH"))

#CREATE MASTER DT OF HEARING DATA TO BE YOUR "RAW" FOR DECOMP2 
#   -----------------------------------------------------------------------

#PREP RAW, EXTRACTED DATA & CROSSWALK MAP DATA FOR MERGE 
#merging the two so that every raw data point will be duplicated to each possible severity category/bundle

merge_dt <- function(map) {
  extract <- data.table(read.xlsx(paste0(j, "FILEPATH"))) #get raw data
  alts <-map 

  
  alts[ , thresh := paste0(alt_low, "_", alt_high)]
  alts[ , group := paste0(ref_low, "_", ref_high)]
  alts[ , c("alt_low", "alt_high", "ref_low", "ref_high", "meid") := NULL]
  setnames(alts, "group", "sev")

  test <- merge(x = extract, y = alts, by = c("thresh"), allow.cartesian = TRUE, sort = FALSE)
  test[ , merge_seq := 1:nrow(test)]

  #clean up merged data
  test[is.na(mean),mean:=cases/sample_size]
  test <- test[!(is.na(nid)),]

  #CALCULATE GBD AGES TO MERGE NHANES DATA ON
  test[ , age_midpoint := (age_start+age_end)/2]

  gbd_ages <- data.table(age_min = seq(from = 0, to = 80, by = 5),
                         age_max = c(seq(from = 4.99, to = 80,by = 5), 120),
                         age = c(1, seq(5,80,5)))

  test$gbd_age <- as.numeric(NA)
  age_var <- function(x) {
    for (i in 1:nrow(gbd_ages)) {
      range <- gbd_ages[i, 1:2]
      val <- gbd_ages[i, age]
      x[is.na(gbd_age), gbd_age := ifelse(age_midpoint %inrange% range, val, NA)]
    }
  }
  sex_var <- function(x){
    test[sex == "Male" ,sex_id := 1]
    test[sex == "Female" ,sex_id := 2]
    test[sex == "Both" ,sex_id := 3]
  }
  age_var(test)
  sex_var(test)
  return(test)
}

merged_dt <- merge_dt(map = alts_2017)
write.xlsx(x=merged_dt, file = paste0(hxwalk_temp, "FILEPATH")) #use this to sex split


#SPLIT INTO BUNDLES BY MEID
sep <- split(x = test, f = merged_dt$meid)

for (i in 1:length(sep)) {
   dt <- sep[i]
   write.xlsx(x = dt, file = paste0(j_hear,"FILEPATH",names(dt), "FILEPATH"))
  }

#FREQUENCY DISTRIBUTION OF DB LOSS VALUES
db_freq <- ggplot(micro, aes(x = db_loss, color = gbd_age)) +
  geom_histogram(binwidth = 0.2, alpha = 0.5) +
  facet_wrap(~gbd_age)
print(db_freq)
