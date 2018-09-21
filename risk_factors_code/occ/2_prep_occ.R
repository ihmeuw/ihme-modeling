# ---HEADER-------------------------------------------------------------------------------------------------------------

# Project: OCC - OCC
# Purpose: Prep the Occupational Major Groups for modelling by creating square dataset and filling in custom covs

#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# set toggles


# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  # necessary to set this option in order to read in a non-english character shapefile on a linux system (cluster)
  Sys.setlocale(category = "LC_ALL", locale = "C")
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, readxl)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#set values for project
location_set_version_id <- 149
year_start <- 1970
year_end <- 2016
by_sex <- 1
by_age <- 0

##in##
data.dir <- file.path(home.dir, "FILEPATH")
microdata.dir <- file.path(home.dir, "FILEPATH")
tabs <- file.path(microdata.dir, 'collapse_06_26_appended.csv') %>% fread
cw.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, 'FILEPATH')
isco.map <- file.path(doc.dir, 'ISCO_MAJOR_SUB_MAJOR.xlsx') %>% read_xlsx(sheet=2) %>% as.data.table

##out##

#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source

inv.logit <- function(x) {
  return(exp(x)/(exp(x)+1))
}

logit_offset <- function(x, offset) {
  
  x_len = length(x)
  
  value <- vector(mode="numeric", length=x_len)
  
  for (i in 1:x_len) {
    
    if (x[i]==1) {
      value[i] <- x[i] - offset
    } else if (x[i]==0)  {
      value[i] <- x[i] + offset
    } else value[i] <- x[i]
    
  }
  
  return(log(value/(1-value)))
}
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator
#***********************************************************************************************************************

# ---PREP---------------------------------------------------------------------------------------------------------------
#read in and prep data
ilo.data <- file.path(data.dir, "prepped_occ_major.csv") %>% fread

#cleanp
ilo.data <- ilo.data[!is.na(data), list(ihme_loc_id, location_id, year_id, age_group_id, sex_id,
                                        source, cv_isco_version, isco_code, sourcelabel,
                                        occ_label, nid, cv_isco68, cv_isco08, cv_iscouk,
                                        data, variance, sample_size, cv_subgeo)]

## Get levels
locs <- get_location_hierarchy(location_set_version_id)
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

# Merge dt to the location hierarchy
ilo.data <- merge(ilo.data, levels, by=c('location_id', 'ihme_loc_id'))

#add in the microdata
tabs <- tabs[!(var %like% 'hrh')] #remove hrh vars
tabs <- tabs[var %like% 'occ'] #remove industry vars
tabs <- tabs[!(ihme_loc_id %in% c('QCH', 'TAP', 'KSV', 'QAR', 'QES'))] #remove random iso3s from PISA
tabs[ihme_loc_id == "MAC", ihme_loc_id := "CHN_361"]
tabs[, year_id := floor((year_start + year_end)/2)]
setnames(tabs, 'cv_occupational_code_type', 'cv_isco_version')

#drop duplicates 
#setkeyv(tabs, c('ihme_loc_id', 'year_id', 'sex_id', 'var', 'file_path', 'survey_module'))
#tabs <- unique(tabs, by=key(tabs))

#fix the major groups
tabs[, isco_code := substr(var, start=11, stop=13)]

#calculate variance
tabs[is.na(standard_error), standard_error := standard_deviation / sqrt(sample_size)]
tabs[, variance := standard_error^2]
tabs[, data := mean]

#outliers
tabs <- tabs[data < .7]
tabs <- tabs[!(ihme_loc_id %like% 'GBR' & year_id < 1991)] 
tabs <- tabs[!(file_path %like% 'UNICEF')] 
tabs <- tabs[!(file_path %like% 'IDN_CENSUS_HHM_IDN_1971_1971')] 

#Gen variables for crosswalking between different versions.
tabs[cv_isco_version == "ISCO68", cv_isco68 := 1]
tabs[cv_isco_version == "ISCO08", cv_isco08 := 1]
tabs[, cv_subgeo := 0] #all of these should be nationally rep
tabs[, age_group_id := 22]

#cleanup
setnames(tabs, 'survey_name', 'source')
tabs <- tabs[, list(file_path, nid, source, ihme_loc_id, year_id, sex_id, sample_size, cv_isco_version,
                    isco_code, data, variance, cv_subgeo)]
# Merge dt to the location hierarchy
tabs <- merge(tabs, levels, by=c('ihme_loc_id'))

#merge
dt <- list(ilo.data, tabs) %>% rbindlist(use.names=T, fill=T)

# Save your raw data/variance for reference
dt[, "raw_data" := copy(data)]
dt[, "raw_variance" := copy(variance)]

##crosswalk isic versions using splits defined by the major groups correspondance tables
#we will be crosswalking everything to ISCO88 (SOC2010 is already crosswalked)
#Armed forces also doesnt need to be split
ref <- dt[cv_isco_version %in% c("ISCO88", 'SOC2010') | isco_code %in% c("AF", "0") | occ_label %like% 'Armed']

#crosswalk ISCO 08 to ISCO 88
split.new <- dt[cv_isco_version=="ISCO08"]
split.new[, og_isco_code := as.character(isco_code)] #save code for comparison
split.new[, major_4 := as.character(isco_code)]
#calculate the count of categories to inform uncertainty around splits
split.new[, count := unique(major_4) %>% length, by=list(ihme_loc_id, year_id, sex_id, source)]
isco3.map <- file.path(cw.dir, "ISCO_08_TO_ISCO_88_LVL_1_CW.csv") %>% fread
isco3.map[, major_4 := substr(major_4, start = 3, stop = 3)]
isco3.map[, major_3 := substr(major_3, start = 3, stop = 3)]
split.new <- merge(split.new, isco3.map[, list(major_3, major_4, prop)],
                   by="major_4", allow.cartesian = T)

#now use the proportion to xwalk each ISCO08 category to ISCO88, then collapse
split.new[, split_data := lapply(.SD, function(x, w) sum(x*w), w=prop), .SDcols='data',
          by=c('major_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id')]
split.new <- unique(split.new, by=c('major_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id'))
split.new[, isco_code := major_3]
split.new <- split.new[abs(split_data-raw_data) < .25] #remove unstable points
split.new[count<7, cv_subgeo := 1] #increase uncertainty of the lower count points

#crosswalk ISIC 2 to ISIC 3
split.old <- dt[cv_isco_version=="ISCO68"]
split.old[, og_isco_code := as.character(isco_code)] #save code for comparison
split.old[, major_2 := as.character(isco_code)]
#calculate the count of categories to inform uncertainty around splits
split.old[, count := unique(major_2) %>% length, by=list(ihme_loc_id, year_id, sex_id, source)]
isco3.map <- file.path(cw.dir, "ISCO_68_TO_ISCO_88_LVL_1_CW.csv") %>% fread
isco3.map[, major_2 := substr(major_2, start = 3, stop = 5)]
isco3.map[, major_2 := str_replace_all(major_2, "_", "-")]
isco3.map[, major_3 := substr(major_3, start = 3, stop = 3)]
split.old <- merge(split.old, isco3.map[, list(major_2, major_3, prop)],
                   by="major_2", allow.cartesian = T)

#now use the proportion to xwalk each ISIC2 category to ISIC3, then collapse
split.old[, split_data := lapply(.SD, function(x, w) sum(x*w), w=prop), .SDcols='data',
          by=c('major_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id')]
split.old <- unique(split.old, by=c('major_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id'))
split.old[, isco_code := major_3]
split.old <- split.old[abs(split_data-raw_data) < .25] #remove unstable points
split.old <- split.old[!(count<5 & prop <.75)] #if low counts, only keep the most informed categories
split.old[count<5, cv_subgeo := 1] #increase uncertainty of the lower count points

dt <- rbindlist(list(ref,
                     split.old[, -c('major_2', 'major_3', 'prop'), with = F],
                     split.new[, -c('major_4', 'major_3', 'prop'), with = F]),
                use.names=T,
                fill=T)

#create a graph of the split
ggplot(data=dt[!(cv_isco_version %like% "88")],
       aes(x=data, y=split_data, color=level_1 %>% as.factor)) +
  geom_text(aes(label=ihme_loc_id)) +
  facet_wrap(~isco_code) +
  theme_bw()

dt[!is.na(split_data), data := split_data]
dt[isco_code == "AF", isco_code := "0"] #recode armed forces from isco68
dt[, cv_isco_version := "ISCO88"] #replace with 88

#create country year indicator to display which you need to split
dt[, id := paste(ihme_loc_id, year_id, sep="_")]

#impute sample size with conservative estimate of 10th pctlie
dt[, p10_sample_size := quantile(sample_size, probs=.1, na.rm=T) ]
dt[is.na(sample_size), sample_size := p10_sample_size]

#outliers
dt <- dt[!(ihme_loc_id == "WSM" & year_id == 1976)] #all data == 1 (also its somehow isco88..time travel?)

#set the short category names by merging on the isco 88 map
isco.map[, isco_code := occ_major %>% as.character]
dt <- merge(dt, isco.map[, list(isco_code, occ_major_label_short)] %>% unique, by="isco_code")

# Now loop over the different levels, merge the square, fill in study level covs, and save a csv
saveData <- function(cat, dt) {
  
  message("saving -> ", cat)
  
  cat.dt <- dt[occ_major_label_short==cat]
  
  # Transform data
  cat.dt[, data := logit_offset(data, .001)]
  
  # Run model
  mod <- lmer(data ~ cv_isco08 + cv_isco68 + cv_iscouk + (1|level_1) + (1|level_2) + (1|level_3),
              data=cat.dt)
  
  # summarize model
  summary(mod)
  
  ## Crosswalk your non-gold standard datapoints
  # First store the relevant coefficients
  coefficients <- as.data.table(coef(summary(mod)), keep.rownames = T)
  
  if (nrow(coefficients[rn %like% 'isco08'])>0) {
    
    # First crosswalk isco08
    coeff_08 <- as.numeric(coefficients[rn == "cv_isco08", 'Estimate', with=F])
    cat.dt[, "data" := data - (cv_isco08 * coeff_08)]
    
    # Now adjust the variance for points crosswalked from isc08
    se_08 <- as.numeric(coefficients[rn == "cv_isco08", 'Std. Error', with=F])
    cat.dt[, variance := variance + (cv_isco08^2 * se_08^2)]
    
  }
  
  if (nrow(coefficients[rn %like% 'isco68'])>0) {
    
    # repeat for isco68 and iscouk
    coeff_68 <- as.numeric(coefficients[rn == "cv_isco68", 'Estimate', with=F])
    cat.dt[, "data" := data - (cv_isco68 * coeff_68)]
    se_68 <- as.numeric(coefficients[rn == "cv_isco68", 'Std. Error', with=F])
    cat.dt[, variance := variance + (cv_isco68^2 * se_68^2)]
    
  }
  
  if (nrow(coefficients[rn %like% 'iscouk'])>0) {
    
    coeff_uk <- as.numeric(coefficients[rn == "cv_iscouk", 'Estimate', with=F])
    cat.dt[, "data" := data - (cv_iscouk * coeff_uk)]
    se_uk <- as.numeric(coefficients[rn == "cv_iscouk", 'Std. Error', with=F])
    cat.dt[, variance := variance + (cv_iscouk^2 * se_uk^2)]
    
  }
  
  # Now reset all study level covariates to predict as the gold standard
  # Also save the originals for comparison
  cat.dt[, cv_isco68_og := cv_isco08]
  cat.dt[, cv_isco68 := 0]
  cat.dt[, cv_isco08_og := cv_isco08]
  cat.dt[, cv_isco08 := 0]
  cat.dt[, cv_iscouk_og := cv_iscouk]
  cat.dt[, cv_iscouk := 0]
  
  #transform data back
  cat.dt[, data := inv.logit(data)]
  
  # set the group #
  this.group <- cat.dt[!is.na(isco_code), unique(isco_code)]
  this.entity <- paste("occ_occ_major", this.group, cat, sep = "_")
  
  cat.dt[, me_name := this.entity]
  
  write.csv(cat.dt, file=file.path(data.dir, paste0(this.entity, ".csv")))
  
  return(cat.dt)
  
}

list <- lapply(dt[, occ_major_label_short] %>% unique, saveData, dt=dt)

all <- rbindlist(list)
write.csv(all, file=file.path(data.dir, "occ_occ_major_all.csv"))
#***********************************************************************************************************************

# ---TEST REG-----------------------------------------------------------------------------------------------------------
#test mgmt
test <- list[[10]]

# get the covariates to test
cov.list <- c('sdi', 'prop_urban')
covs <- get_covariates(cov.list)
test <- merge(test, covs, by=c('location_id', 'year_id'))

#transform
test[, lt_data := logit_offset(data, .001)]

# do some test regressions to provide coeffs
mod <- lmer(data=test, lt_data~coastal_prop+prop_urban+(1|level_1) + (1|level_2) + (1|level_3))

#***********************************************************************************************************************