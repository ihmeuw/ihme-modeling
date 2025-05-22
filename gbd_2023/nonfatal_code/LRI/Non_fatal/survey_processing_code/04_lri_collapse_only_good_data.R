
######################################################################################
## Follow general pattern of LRI survey prep but only keep surveys with best definition ##
######################################################################################

#Toggle for whether we drop 0-case rows for which SE cannot be calculated, & keep SE 
dropping_zeros = FALSE
invisible(sapply(list.files("FILEPATH", full.names = T), source))
### PREP ###
#Set filepaths
root <- ifelse(Sys.info()[1]=="Windows", "FILEPATH/", "/FILEPATH/")
currentdata <- "FILEPATH"
files       <- list.files(currentdata, full.names = T)
surveys     <- lapply(files, read_dta)
surveys     <- rbindlist(surveys, fill=T)



#Read in supplementary information
locs <- get_location_metadata(location_set_id = 35, release_id = 16)
locs <- locs[,c("location_name","location_id","super_region_name","super_region_id","region_name","region_id","parent_id", "ihme_loc_id")]

#Read in dataset, rename columns
lri <- as.data.table(surveys)
setnames(lri, old = c('survey_name', 'year_start', 'year_end'), new = c('survey_series', 'start_year', 'end_year'))

#Merge locs & seasonality scalars onto dataset
lri <- join(lri, locs, by = "ihme_loc_id")

months <- fread(paste0(root, "FILEPATH")) #seasonality scalars
months$scalar <- months$gamscalar

scalar <- months[,c("scalar","month","region_name")]
setnames(lri, old = 'int_month', new = 'month')
lri <- join(lri, scalar, by=c("region_name","month"))


### CLEAN UP DATA ###
#Change NA's to 9's in symptom columns
lri$had_fever[is.na(lri$had_fever)] = 9
lri$had_cough[is.na(lri$had_cough)] = 9
lri$diff_breathing[is.na(lri$diff_breathing)] = 9
lri$chest_symptoms[is.na(lri$chest_symptoms)] = 9

## Keep only surveys with best definition ##
lri$chest <- ave(lri$chest_symptoms, lri$nid, FUN=function(x) min(x, na.rm=T))
lri$fever <- ave(lri$had_fever, lri$nid, FUN=function(x) min(x, na.rm=T))

lri$keep <- ifelse(lri$chest==0 & lri$fever==0, 1, 0)


best <- subset(lri, keep==1)

#Drop any NIDs completely missing difficulty breathing responses
best$tabulate <- ave(best$diff_breathing, best$nid, FUN= function(x) min(x))
best <- subset(best, tabulate != 9)

#Drop rows w/o sex, round ages, drop over 5s
setnames(best, old = 'sex_id', new = 'child_sex')
best$child_sex[is.na(best$child_sex)] = 3
best$age_year <- floor(best$age_year)
best <- subset(best, age_year <= 4)

#Reassign pweight = 1 if missing or 0
#Include pwt_adj indicator if reassigned to 1
best <- best[, pwt_adj := 0]
best <- best[is.na(pweight) | pweight == 0, pwt_adj := 1]
best <- best[is.na(pweight), pweight := 1]
best <- best[pweight == 0, pweight := 1]

best <- best[, start_n := .N, by = c("nid", "ihme_loc_id", "start_year")]


#Drop if missing PSU 
best <- subset(best, !is.na(psu))


best <- best[,psudrop_n := .N, by = c("nid", "ihme_loc_id", "start_year")]

### COLLAPSE TO COUNTRY-YEAR-AGE-SEX ###
#Data frame for outputs
df <- data.frame()

#Loop over surveys to 1) Assign case definitions, 2) Incorporate seasonality, 3) Apply surveydesign & collapse to country-year-age-sex
nid.new <- unique(best$nid[!is.na(best$nid)])
num <- 1
for(n in nid.new) {
  print(paste0("On survey number ",num," of ",length(nid.new)))
  temp <- subset(best, nid == n)
  
  temp$diff_breathing <- ifelse(temp$diff_breathing==1,1,0)
  temp$chest_symptoms <- ifelse(temp$chest_symptoms==1,1,0)
  temp$diff_fever <- ifelse(temp$had_fever==1 & temp$diff_breathing==1,1,0)
  temp$chest_fever <- ifelse(temp$had_fever==1 & temp$chest_symptoms==1,1,0)
  temp$obs <- 1
  
  #Pull in recall period (if bsswlank, assume it's 2 weeks)
  temp$recall_period <- ifelse(is.na(temp$recall_period_weeks), 2, temp$recall_period_weeks)
  recall_period <- max(temp$recall_period, na.rm=T)

  #Apply seasonality scalars
  scalar.dummy <- max(temp$scalar)
  if(is.na(scalar.dummy)) {
    temp$scalar <- 1
  } 
  temp$diff_breathing <- temp$diff_breathing * temp$scalar
  temp$diff_fever <- temp$diff_fever * temp$scalar
  temp$chest_symptoms <- temp$chest_symptoms * temp$scalar
  temp$chest_fever <- temp$chest_fever * temp$scalar
  
  #Apply survey design & collapse
    if(length(unique(temp$psu))>1){
      dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
      prev <- svyby(~diff_breathing, ~child_sex + age_year, dclus, svymean, na.rm=T)
      prev$diff_fever <- svyby(~diff_fever, ~child_sex + age_year, dclus, svymean, na.rm=T)$diff_fever
      prev$chest_symptoms <- svyby(~chest_symptoms, ~child_sex + age_year, dclus, svymean, na.rm=T)$chest_symptoms
      prev$chest_fever <- svyby(~chest_fever, ~child_sex + age_year, dclus, svymean, na.rm=T)$chest_fever
      prev$sample_size <- svyby(~diff_breathing, ~child_sex + age_year, dclus, unwtd.count, na.rm=T)$count
      prev$ihme_loc_id <- unique(substr(temp$ihme_loc_id,1,3))
      prev$location <- unique(temp$location_name)
      prev$start_year <- unique(temp$start_year)
      prev$end_year <- unique(temp$end_year)
      prev$nid <- unique(temp$nid)
      prev$sex <- ifelse(prev$child_sex==2,"Female",ifelse(prev$child_sex==1,"Male","Both"))
      prev$age_start <- prev$age_year
      prev$age_end <- prev$age_year + 1
      prev$recall_period <- recall_period
      
      df <- rbind.data.frame(df, prev)
    } 
  num <- num + 1
}

#Export for further prep #
write.csv(df, paste0(root, "FILEPATH"), row.names=F)

# rbind with previous tabulated survey best def
previous <- fread(paste0(root, "FILEPATH"))

all <- rbind(previous, df, fill=TRUE)
write.csv(all, paste0(root, "FILEPATH"), row.names=F)
