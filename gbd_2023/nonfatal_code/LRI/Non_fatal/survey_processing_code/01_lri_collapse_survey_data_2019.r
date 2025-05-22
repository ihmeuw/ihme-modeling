######################################################################################
#### Synchronize LRI data processing between GBD and Geospatial ####
######################################################################################

#Toggle for whether we drop 0-case rows for which SE cannot be calculated, & keep SE
dropping_zeros = FALSE

### PREP ###
library(data.table)
#Set filepaths
root        <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
currentdata <- "/FILEPATH/"
files       <- list.files(currentdata, full.names = T)
surveys     <- lapply(files, read_dta)
surveys     <- rbindlist(surveys, fill=T)


#Read in supplementary information
locs <- get_location_metadata(location_set_id = 35, release_id = 16)
locs <- locs[,c("location_name","location_id","super_region_name","super_region_id","region_name","region_id","parent_id", "ihme_loc_id","exists_subnational")]

#Read in dataset, rename columns
lri <- as.data.table(surveys)

setnames(lri, old = c('survey_name', 'year_start', 'year_end'), new = c('survey_series', 'start_year', 'end_year'))

#Merge locs & seasonality scalars onto dataset
lri <- join(lri, locs, by = "ihme_loc_id")

### CLEAN UP DATA ###
#Change NA's to 9's in symptom columns
lri$had_fever[is.na(lri$had_fever)] = 9
lri$had_cough[is.na(lri$had_cough)] = 9
lri$diff_breathing[is.na(lri$diff_breathing)] = 9
lri$chest_symptoms[is.na(lri$chest_symptoms)] = 9

## For seasonality
lri$any_lri <- ifelse(lri$chest_symptoms==1,1,ifelse(lri$diff_breathing==1 & lri$chest_symptoms!=0,1,0))

#Drop any NIDs completely missing difficulty breathing responses
lri$tabulate <- ave(lri$diff_breathing, lri$nid, FUN= function(x) min(x))
lri <- subset(lri, tabulate == 0)

#Drop rows w/o sex, round ages, drop over 5s
setnames(lri, old = 'sex_id', new = 'child_sex')
lri$child_sex[is.na(lri$child_sex)] = 3
lri$age_year <- floor(lri$age_year)
lri <- subset(lri, age_year <= 5)

#Reassign pweight = 1 if missing or 0
#Include pwt_adj indicator if reassigned to 1
lri <- lri[, pwt_adj := 0]
lri <- lri[is.na(pweight) | pweight == 0, pwt_adj := 1]
lri <- lri[is.na(pweight), pweight := 1]
lri <- lri[pweight == 0, pweight := 1]

# Calculate seasonality #

lri <- lri[, start_n := .N, by = c("nid", "ihme_loc_id", "start_year")]

months <- fread(paste0(root, "FILEPATH")) #seasonality scalars
# Use GAM scalar! #
months$scalar <- months$gamscalar

scalar <- months[,c("scalar","month","region_name")]
setnames(lri, old = 'int_month', new = 'month')
lri <- join(lri, scalar, by=c("region_name","month"))

#Drop if missing PSU 
lri <- subset(lri, !is.na(psu))
lri <- lri[,psudrop_n := .N, by = c("nid", "ihme_loc_id", "start_year")]

## add information to map to GBD subnationals ##
firstup <- function(x) {
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  x
}

## Create a subnational map ##
lookup <- lri[,c("location_name","ihme_loc_id","admin_1")]
lookup <- join(lookup, locs, by="ihme_loc_id")
lookup <- subset(lookup, exists_subnational==1)
lookup <- unique(lookup)
lookup$location <- lookup$admin_1

subnat_map <- read.csv("FILEPATH")
lookup <- join(lookup, subnat_map, by="location")
lookup$name_lower <- tolower(lookup$admin_1)
locs$name_lower <- tolower(locs$location_name)
lookup <- join(lookup, locs, by="name_lower")

lookup$correct_country <- ifelse(lookup$ihme_loc_id == lookup$parent_id, 1, 0)

# Use that file to identify subnational units from the survey, map them to GBD locations ##
## then save it as "gbd_subnat_map.csv"
# 'subnat' in this CSV means that there is a matched subnational location in GBD 
subnat_map <- read.csv("/FILEPATH")
setnames(subnat_map, c("parent_id", "location"), c("iso3","admin_1"))

lri$iso3 <- substr(lri$ihme_loc_id,1,3)
lri <- join(lri, subnat_map, by=c("admin_1","iso3"))
lri$subnat[is.na(lri$subnat)] <- 0

## Identify known subnationals ##
lri$subname <- ifelse(lri$subnat==1, as.character(lri$ihme_loc_id_mapped), lri$iso3)
lri$nid.new <- ifelse(lri$subnat==1, paste0(lri$nid,"_",lri$subname), lri$nid)



### COLLAPSE TO COUNTRY-YEAR-AGE-SEX ###
#Data frame for outputs
df <- data.frame()

#Loop over surveys to 1) Assign case definitions, 2) Incorporate seasonality, 3) Apply surveydesign & collapse to country-year-age-sex
num <- 1
for(n in unique(lri$nid.new)) {
  print(paste0("On survey number ",num," of ",length(unique(lri$nid.new))))
  temp <- subset(lri, nid.new == n)
  
  temp$obs <- 1
  #Set indicator = 1 if survey has chest/fever, set 0 if not
  exist_chest <- ifelse(min(temp$chest_symptoms, na.rm=T) == 0, 1, 0)
  exist_fever <- ifelse(min(temp$had_fever, na.rm=T) == 0, 1, 0)  
  
  #Pull in recall period (if bsswlank, assume it's 2 weeks)
  temp$recall_period <- ifelse(is.na(temp$recall_period_weeks), 2, temp$recall_period_weeks)
  recall_period <- max(temp$recall_period, na.rm=T)
  
  #Assign case definitions
  #We want prevalence w/o fever for crosswalk
  temp$good_no_fever <- 0
  temp$poor_no_fever <- 0
  if(exist_fever==1 & exist_chest==1) {
    temp$overall_lri = ifelse(temp$had_fever==1 & temp$chest_symptoms==1, 1, 0)
    temp$good_no_fever = ifelse(temp$chest_symptoms==1,1,0)
  } else if (exist_fever==1 & exist_chest==0) {
    temp$overall_lri = ifelse(temp$had_fever==1 & temp$diff_breathing==1, 1, 0)
    temp$poor_no_fever = ifelse(temp$diff_breathing==1,1,0)
  } else if (exist_fever==0 & exist_chest==1) {
    temp$overall_lri = ifelse(temp$chest_symptoms==1, 1, 0)
    temp$good_no_fever = 0
  } else if (exist_fever==0 & exist_chest==0) {
    temp$overall_lri = ifelse(temp$diff_breathing==1, 1, 0)
    temp$poor_no_fever = 0
  }

  #Apply seasonality scalars
  scalar.dummy <- max(temp$scalar)
  if(is.na(scalar.dummy)) {
    temp$scalar_lri <- temp$overall_lri
  } else {
    temp$scalar_lri <- temp$overall_lri*temp$scalar
    temp$good_no_fever <- temp$good_no_fever*temp$scalar
    temp$poor_no_fever <- temp$poor_no_fever*temp$scalar
  }
  
  #Apply survey design & collapse
    if(length(unique(temp$psu))>1){
      dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
      prev <- svyby(~scalar_lri, ~child_sex + age_year, dclus, svymean, na.rm=T)
      prev$base_lri <- svyby(~overall_lri, ~child_sex + age_year, dclus, svymean, na.rm=T)$overall_lri
      prev$good_no_fever <- svyby(~good_no_fever, ~child_sex + age_year, dclus, svymean, na.rm=T)$good_no_fever
      prev$poor_no_fever <- svyby(~poor_no_fever, ~child_sex + age_year, dclus, svymean, na.rm=T)$poor_no_fever
      prev$sample_size <- svyby(~scalar_lri, ~child_sex + age_year, dclus, unwtd.count, na.rm=T)$count
    } else {
      prev <- aggregate(cbind(scalar_lri, good_no_fever, poor_no_fever) ~ child_sex + age_year, FUN=mean, na.rm=T, data=temp)
      prev$base_lri <- aggregate(overall_lri ~ child_sex + age_year, FUN=mean, na.rm=T, data=temp)$overall_lri
      prev$se <- aggregate(scalar_lri ~ child_sex + age_year, FUN=sd, na.rm=T, data=temp)$scalar_lri
      prev$sample_size <- aggregate(obs ~ child_sex + age_year, FUN=sum, na.rm=T, data=temp)$obs
    }
  prev$cases <- prev$scalar_lri * prev$sample_size
  prev$ihme_loc_id <- unique(temp$subname)
  prev$location <- unique(temp$location_name)
  prev$start_year <- unique(temp$start_year)
  prev$end_year <- unique(temp$end_year)
  prev$cv_diag_valid_good <- exist_chest
  prev$cv_had_fever <- exist_fever
  prev$nid <- unique(temp$nid)
  prev$sex <- ifelse(prev$child_sex==2,"Female",ifelse(prev$child_sex==1,"Male","Both"))
  prev$age_start <- prev$age_year
  prev$age_end <- prev$age_year + 1
  prev$recall_period <- recall_period
  prev$notes <- paste0("LRI prevalence adjusted for seasonality. The original value was ", round(prev$base_lri,4))
  
  df <- rbind.data.frame(df, prev)
  num <- num + 1
}


uniqlri = unique(lri[, c("nid", "ihme_loc_id", "start_year", "start_n", "psudrop_n")])
dt_df = as.data.table(df)
dt_df = dt_df[, collapsed_n := sum(sample_size), by = c("nid", "ihme_loc_id", "start_year")]
dt_df = merge(dt_df, uniqlri, by = c("nid", "ihme_loc_id", "start_year"))


#Export for further prep #
write.csv(df, paste0(root, "FILEPATH"), row.names=F)

