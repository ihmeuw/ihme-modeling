#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Prep vaccination for ST-GPR (reference data)
#***********************************************************************************************************************


#----SETUP--------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### load packages
source("FILEPATH/load_packages.R")
load_packages(c("data.table", "dplyr", "parallel", "readxl", "ggplot2", "boot", "lme4", "pscl", "purrr", "splines", "stringr"))

### set os flexibility
if (.Platform$OS.type=="windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
}

### path locals
setwd(paste0(j, "FILEPATH"))
code.root  <- paste0(unlist(strsplit(getwd(), "hsa"))[1], "FILEPATH")
paths.file <- paste0(code.root, "/paths.csv"); paths <- fread(paths.file)
source(paths[obj=="ubcov_tools", 2, with=F] %>% gsub("J:/", j, .) %>% unlist)
path_loader(paths.file)

### source functions
source(db_tools)
"FILEPATH/read_excel.R" %>% source
file.path(j, "FILEPATH/get_location_metadata.R") %>% source
locations <- get_location_metadata(location_set_id=22)
locs      <- locations[level >= 3, ] #get_location_hierarchy(location_set_version_id)[level>=3]
#***********************************************************************************************************************


#----PREP---------------------------------------------------------------------------------------------------------------
### set paths for vaccine introduction reference data and admin coverage reports
vacc.intro          <- paste0(data_root, "FILEPATH/year_vaccine_introduction.xls")
vacc.intro.supp     <- paste0(data_root, "FILEPATH/vacc_intro_supplement.csv")
vacc.outro.supp     <- paste0(data_root, "FILEPATH/vacc_outro_supplement.csv")
vacc.admin          <- paste0(data_root, "FILEPATH/coverage_series.xls")
vacc.admin.subnat   <- paste0(j, "FILEPATH/Data_request_20171201.xlsx")
vacc.whosurvey      <- paste0(data_root, "FILEPATH/Coverage_survey_data.xls")
vacc.whosurveyclean <- paste0(data_root, "FILEPATH/who_survey/who_coverage_survey_cleaned_with_NIDs.xlsx")
vacc.schedule       <- paste0(data_root, "FILEPATH/schedule_data.xls")

### prep modelable entity db for reference
me.db <- paste0(code_root, "FILEPATH/me_db.csv") %>% fread

### set objects
year.est.start <- year_start
year.est.end   <- year_end
#***********************************************************************************************************************


########################################################################################################################
# SECTION 1: Prep WHO Survey Data
########################################################################################################################


#----FUNCTION-----------------------------------------------------------------------------------------------------------
prep.who_survey_clean <- function() {
  df <- read_excel(vacc.whosurveyclean) %>% data.table
  old <- c("ISO3", "cohortYear", "vaccine", "coverage", "surveyNameProduction", "NID", "Sample_Size")
  new <- c("ihme_loc_id", "year_id", "who_name", "data", "survey_name", "nid", "sample_size")
  setnames(df, old, new)
  ## Add one year to cohort year, as it's currently assigned as birth year not year of survey when 12-23 mos
  df[, year_id := year_id + 1]
  ## Subset to card or history
  df <- df[evidence=="Card or History"]
  ## Subset to crude
  df <- df[validity=="crude"]
  ## Keep only surveys we don't already have microdata for, as well as surveys we don't have NIDs for (i.e. aren't catalogued)
  df[is.na(ignore), ignore := 2]
  df <- df[(ignore==0 | (Ownership=="Report only" & ignore==2)) & !is.na(nid)]
  ## Clean up who_name (to be merged to me_name)
  ## Compare against names from me.db
  setdiff(unique(df$who_name), me.db$who_name)
  ## Clean
  df[who_name == "PcV1", who_name := "PCV1"]
  df[who_name == "PcV3", who_name := "PCV3"]
  #df[who_name == "RotaC", who_name := "rotac"]
  df[who_name == "HepBB", who_name := "HepB3"]
  df[who_name %in% c("TT2+", "TT2"), who_name := "TT2plus"]
  ## Check again
  setdiff(unique(df$who_name), me.db$who_name)
  ## Merge on to me.db
  df <- merge(df, me.db[, .(me_name, who_name)], by='who_name', all.x=TRUE)
  ## Drop superfulous me's
  extra.mes <- df[is.na(me_name)][['who_name']] %>% unique
  print(paste0("Vaccines dropped: ", paste(extra.mes, collapse=", ")))
  df <- df[!is.na(me_name)]
  ## Adjust data
  df[, data := data / 100]
  df[, variance := data * (1 - data) / sample_size]
  ## Keep
   df <- df[, c("nid", "survey_name", "ihme_loc_id", "year_id", "data", "variance", "sample_size", "me_name"),  with=FALSE]
  ## Clean
  df[, cv_whosurvey := 1]
  ## Drop unmapped locations
  drop.locs <- df[!(ihme_loc_id %in% locs$ihme_loc_id)]$ihme_loc_id %>% unique
  if (length(drop.locs)>0 ) {
    print(paste0("UNMAPPED LOCATIONS (DROPPING): ", toString(drop.locs)))
    df <- df[!(ihme_loc_id %in% drop.locs)]
  }
  return(df)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 2: Prep WHO admin data
########################################################################################################################


#----FUNCTION-----------------------------------------------------------------------------------------------------------
prep.who_admin <- function(me) {
  who <- me.db[me_name==me]$who_name
  df <- read_excel(vacc.admin, sheet=who) %>% data.table
  drop <- c("WHO_REGION", "Cname", "Vaccine")
  df <- df[, (drop) := NULL]
  df <- melt(df, id="ISO_code", variable.name="year_id", value.name="data")
  df <- df[, me_name := me]
  df <- df[, data := data/100]
  df <- df[!is.na(data)]
  df <- df[, cv_admin := 1]
  setnames(df, "ISO_code", "ihme_loc_id")
  df <- df[, nid := 203321]
  df <- df[, survey_name := "WHO/UNICEF Admin Data"]
  ## Duplicate Demark for Greenland
  df.grl <- df[ihme_loc_id =="DNK"] %>% copy
  df.grl <- df.grl[, ihme_loc_id := "GRL"]
  df <- rbind(df, df.grl)
  ## Drop unmapped locations
  drop.locs <- df[!(ihme_loc_id %in% locs$ihme_loc_id)]$ihme_loc_id %>% unique
  if (length(drop.locs)>0 ) {
    print(paste0("UNMAPPED LOCATIONS (DROPPING): ", toString(drop.locs)))
    df <- df[!(ihme_loc_id %in% drop.locs)]
  }
  return(df)
}

prep.who.admin.subnat <- function(me) {
  ### read in dataset
  data <- read_excel(vacc.admin.subnat, sheet="Data_request") %>% data.table %>%
    setnames(., "Vaccine Type", "who_name")
  ### grab GBD me_names for modeling
  data <- merge(data, me.db[, .(who_name, me_name)], by="who_name", all.x=TRUE)
  data[is.na(me_name) & who_name %in% c("RotaC", "rotac"), me_name := "vacc_rotac"]
  ### prep
  setnames(data, c("Year", "Iso Code", "Country Name", "Coverage"), c("year_id", "ihme_loc_id", "location_name", "data"))
  ### just keep rows with either both numerator/denominator or coverage
  data <- data[!is.na(data) | !is.na(Numerator & Denominator), ]
  data[is.na(data), data := Numerator / Denominator]
  ### keep only countries where we estimate subnationals
  parents <- locations[location_id %in% locations[level > 3, parent_id] & level==3, ihme_loc_id]
  data <- data[ihme_loc_id %in% parents]
  ### since 
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 3: Prep vaccine intro years
########################################################################################################################


#----FUNCTION-----------------------------------------------------------------------------------------------------------
## For recently introduced vaccinations, set year of introduction.
## Currently assumes subnational introduced same as national
make.intro_frame <- function(me) {
  ## Ratios
  ## Get WHO ME
  if (grepl("ratio", me))  {
    temp <- unlist(strsplit(me, "_"))[1:2] %>% paste0(., collapse="_")
    who_me <- me.db[me_name==temp]$who_name_agg
  } else {
    who_me <- me.db[me_name==me]$who_name_agg
  }
  print(me); print(who_me)
  ## Load intro
  df <- read_excel(vacc.intro, sheet=who_me) %>% data.table
  ## Subset intro years
  df <- df[, c(1, 6, 7), with=F]
  setnames(df, names(df), c("ihme_loc_id", "intro", "intro_partial"))
  df <- df[, intro_partial := gsub("prior to ", "", intro_partial) %>% as.character %>% as.numeric]
  df <- df[, cv_intro := gsub("prior to ", "", intro) %>% as.character %>% as.numeric]
  df <- df[!is.na(ihme_loc_id)]
  ## Replace intro if partial intro in country
  df <- df[!is.na(intro_partial), cv_intro := intro_partial]
  df <- df[, .(ihme_loc_id, cv_intro)]
  ## Append using vacc intro supplement
  intro.supp <- fread(vacc.intro.supp)[who_name_agg==who_me] #[me_name==me]
  ## Drop locations in main if added in supplement
  drop.locs <- intersect(unique(intro.supp$ihme_loc_id), unique(df$ihme_loc_id))
  df <- df[!(ihme_loc_id %in% drop.locs)]
  if (nrow(intro.supp) > 0) {
    intro.supp <- intro.supp[, .(ihme_loc_id, cv_intro)]
    df <- rbind(df, intro.supp)
  }
  ## Merge onto hierarchy
  df <- merge(locs[, .(ihme_loc_id, location_id, parent_id, level)], df, by='ihme_loc_id', all.x=TRUE)
  ## For each level >3, see if parent has a intro date, and replace with that if not present
  for (lvl in unique(df[level>3]$level) %>% sort) {
  intro.parent <- df[, .(location_id, cv_intro)]
  setnames(intro.parent, c('cv_intro', 'location_id'), c('intro_parent', 'parent_id'))
  df <- merge(df, intro.parent, by='parent_id', all.x=TRUE)
  df <- df[level==lvl & is.na(cv_intro), cv_intro := intro_parent]
  df <- df[, intro_parent := NULL]
  }
  df <- df[, c("parent_id", "level") := NULL]
  ## Set intro year to 9999 if no data
  df <- df[is.na(cv_intro), cv_intro := 9999]
  ## Create square frame
  square <- expand.grid(ihme_loc_id=locs$ihme_loc_id, year_id=as.numeric(year.est.start:year.est.end)) %>% data.table
  df <- merge(square, df, by='ihme_loc_id', all.x=TRUE)
  df <- df[, me_name := me]
  df <- df[order(ihme_loc_id, year_id)]
  ## Set years since introduction (where year of introduction counts as 1st year)
  df <- df[, cv_intro_years := ifelse((year_id - (cv_intro - 1)) >= 0, year_id - (cv_intro - 1), 0)]
  return(df)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 4: Prep vaccine schedule
########################################################################################################################


#----FUNCTION-----------------------------------------------------------------------------------------------------------
## Preps vaccination schedule to get the number of doses in the schedule (mostly for rota rn)
prep.schedule <- function(me) {
  ## Get WHO ME
  who_me <- me.db[me_name==me]$who_name_agg
  ## Load schedule
  df <- read_excel(vacc.schedule, sheet="schedule") %>% data.table
  ## Subset
  df <- df[, c(2, 4, 6)]
  setnames(df, names(df), c("ihme_loc_id", "vacc", "schedule"))
  ## Keep
  df <- df[vacc == who_me]
  df <- df[, doses := (gsub(";", ",", schedule) %>% gsub(".$", "", .) %>% str_count(., ",")) + 1]
  df <- df[is.na(doses), doses := 0]
  ## Push into locs
  df <- merge(locs[, .(ihme_loc_id, location_id, parent_id, level)], df, by="ihme_loc_id", all.x=TRUE)
  ## For each level>3, set schedule based on parent
  for (lvl in unique(df[level>3]$level) %>% sort){
  parent <- df[, .(location_id, doses)]
  setnames(parent, c("location_id", "doses"), c("parent_id", "parent_doses"))
  df <- merge(df, parent, by='parent_id', all.x=TRUE)
  df <- df[level==lvl & is.na(doses), doses := parent_doses]
  df <- df[, parent_doses := NULL]
  }
  df <- df[, c("parent_id", "level", "vacc", "schedule") := NULL]
  df <- df[, me_name := me]
  df <- df[!is.na(doses)]
  return(df)
}
#***********************************************************************************************************************


########################################################################################################################
# SECTION 5: Prep vaccine removal (BCG)
########################################################################################################################


#----FUNCTION-----------------------------------------------------------------------------------------------------------
## Preps vaccination frame for when vaccines were removed from schedule (mostly BCG in high income)
make.outro_frame <- function(me) {
  ## Load intro
  df <- fread(vacc.outro.supp)[me_name==me]
  ## Merge onto hierarchy
  df <- merge(locs[, .(ihme_loc_id, location_id, parent_id, level)], df, by='ihme_loc_id', all.x=TRUE)
  ## For each level >3, see if parent has a outro date, and replace with that if not present
  for (lvl in unique(df[level>3]$level) %>% sort) {
    outro.parent <- df[, .(location_id, cv_outro)]
    setnames(outro.parent, c('cv_outro', 'location_id'), c('outro_parent', 'parent_id'))
    df <- merge(df, outro.parent, by='parent_id', all.x=TRUE)
    df <- df[level==lvl & is.na(cv_outro), cv_outro := outro_parent]
    df <- df[, outro_parent := NULL]
  }
  df <- df[, c("parent_id", "level") := NULL]
  ## Set outro year to 9999 if no data
  df <- df[is.na(cv_outro), cv_outro := 9999]
  ## Create square frame
  square <- expand.grid(ihme_loc_id=locs$ihme_loc_id, year_id=as.numeric(year.est.start:year.est.end)) %>% data.table
  df <- merge(square, df, by='ihme_loc_id', all.x=TRUE)
  df <- df[, me_name := me]
  df <- df[order(ihme_loc_id, year_id)]
  ## Set years since introoduction. Assume 1980 is start date, and set to 0 after outro
  df <- df[, cv_intro_years := ifelse((cv_outro - year_id)>0, year_id - 1980 + 1, 0)]
  df <- df[, .(ihme_loc_id, year_id, location_id, me_name, cv_outro, cv_intro_years)]
  return(df)
}
#***********************************************************************************************************************


#----RUN----------------------------------------------------------------------------------------------------------------
## Prep who survey data
who.survey <- prep.who_survey_clean()
saveRDS(who.survey, paste0(data_root, "FILEPATH/who_survey.rds"))

## Prep WHO admin data
admin.vacc <- c("vacc_bcg", "vacc_polio3", "vacc_dpt1", "vacc_dpt3", "vacc_hepb3", "vacc_hib3", 
                "vacc_mcv1", "vacc_mcv2", "vacc_pcv1",  "vacc_pcv3", "vacc_rotac", "vacc_yfv", "vacc_rcv1")
who.admin <- lapply(admin.vacc, prep.who_admin) %>% rbindlist
saveRDS(who.admin, paste0(data_root, "FILEPATH/who_admin.rds"))

## Make Intro frame
intro.vacc <- c(paste0("vacc_hepb", 1:3),
                paste0("vacc_hib", 1:3),
                paste0("vacc_pcv", 1:3),
                paste0("vacc_rota", 1:3),
                "vacc_rotac",
                "vacc_mcv2",
                "vacc_rcv1",
                "vacc_yfv",
                "vacc_hib3_dpt3_ratio", 
                "vacc_hepb3_dpt3_ratio",
                "vacc_pcv3_dpt3_ratio", 
                "vacc_rotac_dpt3_ratio",
                "vacc_mcv2_mcv1_ratio",
                "vacc_rcv1_mcv1_ratio"
                )
intro.frame <- lapply(intro.vacc, make.intro_frame) %>% rbindlist
outro.frame <- make.outro_frame("vacc_bcg")
intro.frame <- rbind(intro.frame, outro.frame, fill=TRUE, use.names=TRUE)
saveRDS(intro.frame, paste0(data_root, "FILEPATH/vaccine_intro.rds"))

## Make doses frame
schedule.vacc <- lapply("vacc_rotac", prep.schedule) %>% rbindlist
saveRDS(schedule.vacc, paste0(data_root, "FILEPATH/vaccine_schedule.rds"))
#***********************************************************************************************************************