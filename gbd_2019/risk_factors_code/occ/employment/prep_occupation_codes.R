# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Prep the Occupational Major Groups for modelling
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, openxlsx)

source("FUNCTION")
source("FUNCTION")
source("FUNCTION")
source("FUNCTION")
source("FUNCTION")

# set working directories
home.dir <- "FILEPATH"
setwd(home.dir)

#set values for project
collapse_version <- 1
decomp_step <- "step4"
bundles_toggle <- T
version_toggle <- F
xwalk_description <- "Step 4, added new microdata"

##in##
in.dir <- "FILEPATH"
out.dir <- "FILEPATH"
if(!dir.exists(out.dir)) dir.create(out.dir, recursive = TRUE)
out.bundle <- "FILEPATH"
microdata.dir <- "FILEPATH"
  tabs <- rbindlist(lapply(file.path(microdata.dir, paste0(c('occ_','occ_census_'),collapse_version,'.csv')),fread),fill=T)
cw.dir <- "FILEPATH"
doc.dir <- "FILEPATH"
  isco.map <- file.path(doc.dir, 'ISCO_MAJOR_SUB_MAJOR.xlsx') %>% read.xlsx(sheet=2) %>% as.data.table

bundles <- fread("FILEPATH")[grepl("occ_occ_",me_name)]

#***********************************************************************************************************************

## Outliering
do_outliering <- function() {
  # all occs
  dt[nid %in% c(20499,74393,151805,218592,56151,19787,129770,313084,313103,313158,313190,313413,19950,19963),is_outlier := 1]

  # armed forces
  dt[isco_code == 0 & nid %in% c(46682,43158),is_outlier := 1]

  # management
  dt[isco_code == 1 & nid == 294537 & location_id %in% c(4849,43880,43916,4856,43887,43923,46682,39455),is_outlier := 1]
  dt[isco_code == 1 & nid == 144370 & location_id == 198 & year_id %in% c(2011,2014),is_outlier := 1]

  # professionals
  dt[isco_code == 2 & nid %in% c(322360,322384,322498,39455),is_outlier := 1]
  dt[isco_code == 2 & sex_id == 2 & nid %in% c(322361),is_outlier := 1]
  dt[isco_code == 2 & sex_id == 1 & nid %in% c(313767),is_outlier := 1]
  dt[isco_code == 2 & nid == 144370 & location_id == 67 & year_id %in% seq(2009,2015),is_outlier := 1]
  dt[isco_code == 2 & nid == 144370 & location_id == 128 & year_id == 2011,is_outlier := 1]
  dt[isco_code == 2 & nid == 144370 & location_id == 153 & year_id == 2010,is_outlier := 1]

  # clerks
  dt[isco_code == 4 & nid %in% c(131297,131333),is_outlier := 1]

  # sales
  dt[isco_code == 5 & nid %in% c(142790,313452,322393,322483),is_outlier := 1]

  # agriculture
  dt[isco_code == 6 & nid %in% c(210592,313421,322494,20404,134371,76706),is_outlier := 1]
  dt[isco_code == 6 & nid %in% c(144370,313320) & location_id == 6 & year_id %in% c(2005,2008),is_outlier := 1]
  dt[isco_code == 6 & nid == 144370 & location_id == 194 & year_id == 1999,is_outlier := 1]

  # machine
  dt[isco_code == 8 & nid == 144370 & location_id == 67 & year_id %in% seq(2009,2015),is_outlier := 1]
  dt[isco_code == 8 & nid == 294537 & location_id %in% c(4850,43881,43917),is_outlier := 1]
  dt[isco_code == 8 & nid == 227131 & grepl("_",ihme_loc_id),is_outlier := 1]

  # elementary work
  dt[isco_code == 9 & nid %in% c(313767,322361),is_outlier := 1]
  dt[isco_code == 9 & nid == 144370 & location_id == 109 & year_id %in% c(2009),is_outlier := 1]
}

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source

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
ilo.data <- file.path(in.dir, "prepped_occ_major.csv") %>% fread
setnames(ilo.data,"isco_version","cv_isco_version")

#cleanup
ilo.data <- ilo.data[!is.na(data) & isco_code != "X" & data != 1, list(ihme_loc_id, location_id, year_id, age_group_id, sex_id,
                            source, cv_isco_version, isco_code, sourcelabel,
                            occ_label, nid, cv_isco68, cv_isco08, cv_iscouk,
                            data, variance, sample_size, cv_subgeo)]

## rescale occupation codes to sum to one after dropping missingness (only if remaining codes are exhaustive)
x <- ilo.data[,paste(unique(isco_code),collapse = ", "),by=list(ihme_loc_id,year_id,sex_id,cv_isco_version,source)] # list out all isic codes in a survey
x[cv_isco_version == "ISCO68" & grepl("0-1",V1) & grepl("2",V1) & grepl("3",V1) &
    grepl("4",V1) & grepl("5",V1) & grepl("6",V1) & grepl("7-9",V1), cv_rescale := 1] # mark those that are exhaustive
x[cv_isco_version == "ISCO88" & grepl(0,V1) & grepl(1,V1) & grepl(2,V1) & grepl(3,V1) &
    grepl(4,V1) & grepl(5,V1) & grepl(6,V1) & grepl(7,V1) & grepl(8,V1) & grepl(9,V1), cv_rescale := 1]
x[cv_isco_version == "ISCO08" & grepl(0,V1) & grepl(1,V1) & grepl(2,V1) & grepl(3,V1) &
    grepl(4,V1) & grepl(5,V1) & grepl(6,V1) & grepl(7,V1) & grepl(8,V1) & grepl(9,V1), cv_rescale := 1]
ilo.data <- merge(ilo.data,x[,-c("V1"),with = F],by=c("ihme_loc_id","year_id","sex_id","cv_isco_version","source"),all.x=T)
ilo.data[cv_rescale == 1, scale_factor := sum(data), by=list(ihme_loc_id,year_id,sex_id,cv_isco_version,source)]
ilo.data[cv_rescale == 1, data := data/scale_factor]
ilo.data[cv_rescale == 1, variance := variance/(scale_factor**2)]

## Get levels
source("FUNCTION")
locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
levels <- locs[,grep("region_id|location_id|ihme_loc_id", names(locs)), with=F]

# Merge dt to the location hierarchy
ilo.data <- merge(ilo.data, levels, by=c('location_id', 'ihme_loc_id'))

#add in the microdata
tabs <- tabs[!(ihme_loc_id %in% c('QCH', 'TAP', 'KSV', 'QAR', 'QES'))] #remove random iso3s from PISA
tabs[ihme_loc_id == "MAC", ihme_loc_id := "CHN_361"]
tabs[, year_id := floor((year_start + year_end)/2)]
setnames(tabs, 'occupational_code_type', 'cv_isco_version')
tabs[,cv_isco_version := gsub(" ","",cv_isco_version)]

#fix the major groups
tabs[grepl("0-1",var),isco_code := "0-1"]
tabs[grepl("2",var),isco_code := "2"]
tabs[grepl("3",var),isco_code := "3"]
tabs[grepl("4",var),isco_code := "4"]
tabs[grepl("5",var),isco_code := "5"]
tabs[grepl("6",var),isco_code := "6"]
tabs[grepl("7-9",var),isco_code := "7-9"]
tabs[is.na(isco_code), isco_code := substr(var, start=11, stop=13)]

#calculate variance
tabs[is.na(standard_error), standard_error := standard_deviation / sqrt(sample_size)]
tabs[, variance := standard_error^2]
tabs[, data := mean]

#outliers
tabs <- tabs[!(ihme_loc_id %like% 'GBR' & year_id < 1991)] #mapping here needs further investigation
tabs <- tabs[!(file_path %like% 'UNICEF')] # something wrong with the mapping here
tabs <- tabs[!(file_path %like% 'IDN_CENSUS_HHM_IDN_1971_1971')] # something wrong with the mapping here

#Gen variables for crosswalking between different versions.
tabs[cv_isco_version == "ISCO68", cv_isco68 := 1]
tabs[is.na(cv_isco68), cv_isco68 := 0]
tabs[cv_isco_version == "ISCO08", cv_isco08 := 1]
tabs[is.na(cv_isco08), cv_isco08 := 0]
tabs[, cv_iscouk := 0]
tabs[, cv_subgeo := 0] #all of these should be nationally rep
tabs[, age_group_id := 22]

#cleanup
setnames(tabs, 'survey_name', 'source')
tabs <- tabs[, list(file_path, nid, source, ihme_loc_id, year_id, sex_id, age_group_id, sample_size, cv_isco_version,
                    isco_code, data, variance, cv_subgeo, cv_isco68, cv_isco08, cv_iscouk)]
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
split.old[count<5, cv_subgeo := 1] #increase uncertainty of the lower count points

dt <- rbindlist(list(ref,
                     split.old[, -c('major_2', 'major_3', 'prop'), with = F],
                     split.new[, -c('major_4', 'major_3', 'prop'), with = F]),
                use.names=T,
                fill=T)

dt[!is.na(split_data), data := split_data]
dt[isco_code == "AF", isco_code := "0"] #recode armed forces from isco68
dt[, cv_isco_version := "ISCO88"] #replace with 88

#create country year indicator to display which you need to split
dt[, id := paste(ihme_loc_id, year_id, sep="_")]

#impute sample size with conservative estimate of 10th pctlie
dt[, p10_sample_size := quantile(sample_size, probs=.1, na.rm=T) ]
dt[is.na(sample_size), sample_size := p10_sample_size]

#set the short category names by merging on the isco 88 map
isco.map[, isco_code := occ_major %>% as.character]
dt <- merge(dt, isco.map[, list(isco_code, occ_major_label_short)] %>% unique, by="isco_code", all.x = T)

dt[,country_id := substr(ihme_loc_id,1,3)]

## impute variance
dt[,impute_variance := data*(1-data)/sample_size]
dt[variance < impute_variance,variance := impute_variance]
dt[,impute_variance := NULL]

# outliers
dt[,is_outlier := 0]
if (decomp_step %in% c("step4","iterative")) {
  dt[ihme_loc_id == "WSM" & year_id == 1976,is_outlier := 1]
  dt[grepl("_",ihme_loc_id) & grepl("ISSP",file_path),is_outlier := 1]
} else {
  dt <- dt[!(ihme_loc_id == "WSM" & year_id == 1976)]
  dt <- dt[!(grepl("_",ihme_loc_id) & grepl("ISSP",file_path))]
}

## mark outliers
do_outliering()

# Now loop over the different levels, merge the square, fill in study level covs, and save a csv
saveData <- function(cat, dt) {

  message("saving -> ", cat)

  cat.dt <- dt[occ_major_label_short==cat]

  # Transform data
  cat.dt[, data := logit_offset(data, .001)]

  # Run model
  mod <- lmer(data ~ cv_isco08 + cv_isco68 + cv_iscouk + (1|super_region_id) + (1|region_id) + (1|country_id),
            data=cat.dt[is_outlier == 0])

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

  ## remove "cv_" from colnames to appease bundle restrictions
  setnames(cat.dt,names(cat.dt)[grepl("cv_",names(cat.dt))],gsub("cv_","",names(cat.dt)[grepl("cv_",names(cat.dt))]))

  ## write outputs
  if (decomp_step %in% c("step4","iterative")) {
    write.csv(cat.dt, file=file.path(out.dir, paste0(this.entity, ".csv")), row.names = F)
  } else {
    bid <- bundles[me_name == this.entity,bundle_id]
    bvid <- get_bvid_documentation(bid)[,min(bundle_version_id)]
    if (this.entity == "occ_occ_major_7_craft") bvid <- 4484 ## bundles whose first uploads were unsuccessful
    if (this.entity == "occ_occ_major_9_elem") bvid <- 4487
    step1 <- get_bundle_version(bvid)

    cat.dt <- cat.dt[nid %in% step1$nid]
    if (decomp_step == "step2") cat.dt[,step2_location_year := "reextracted"]

    write.csv(cat.dt, file=file.path(out.dir, paste0(this.entity, ".csv")), row.names = F)
  }

  return(cat.dt)

}

list <- lapply(dt[!is.na(occ_major_label_short), occ_major_label_short] %>% unique, saveData, dt=dt)

all <- rbindlist(list)
write.csv(all, file=file.path(out.dir, "occ_occ_major_all.csv"), row.names = F)
#***********************************************************************************************************************


# Bundle Uploads for Decomp -----------------------------------------------
if (bundles_toggle) {
  for (me in bundles[me_name %like% "pro", me_name]) {
    logs <- fread("FILEPATH")

    print(paste0("UPLOADING ",me))
    bid <- bundles[me_name == me,bundle_id]

    ## clear out bundle
    print("CLEARING BUNDLE")
    wipe_db(bid,decomp_step)

    ## upload new data
    print("UPLOADING BUNDLE")
    upload_db(bid,decomp_step,file.path(out.dir,paste0(me,".csv")),"proportion")

    ## save bundle version
    print("SAVING BUNDLE VERSION")
    bversion <- save_bundle_version(bid,decomp_step)
    bversion <- bversion[,.(me_name = me,bid,dstep = decomp_step,bundle_version_id)]
    print(bversion)

    ## create crosswalk version for bundle version (no additional data step required)
    print("GETTING BUNDLE VERSION")
    df <- get_bundle_version(bversion$bundle_version_id)
    df[,crosswalk_parent_seq := seq]
    df[,unit_value_as_published := 1]
    df[,`unit_value_as_published` := as.double(`unit_value_as_published`)]
    path <- paste0(out.bundle,me,"_xwalked_",decomp_step,".xlsx")
    write.xlsx(df,path,sheetName="extraction")
    print("SAVING XWALK VERSION")
    cversion <- save_crosswalk_version(bversion$bundle_version_id,path,description = xwalk_description)
    cversion <- cversion[,.(bid,crosswalk_version_id)]
    print(cversion)

    ## save log of IDs
    log <- merge(bversion,cversion,by="bid")
    log[, c("timestamp","notes"):= .(as.character(Sys.time()),xwalk_description)]
    setcolorder(log,names(logs))
    logs <- rbind(log,logs)
    write.csv(logs, "FILEPATH")
  }
} else if (version_toggle) { ## Update bundle version with new outliers, so no need for new bundle version
  logs <- fread(file.path(out.bundle,"occ_bid_logs.csv"))
  bvids <- logs[,max(bundle_version_id),by=me_name]$V1

  for (bvid in bvids) {
    logs <- fread(file.path(out.bundle,"occ_bid_logs.csv"))
    log <- unique(logs[bundle_version_id == bvid,.(me_name,dstep,bid,bundle_version_id)])
    print("GETTING BUNDLE VERSION")
    dt <- get_bundle_version(bvid)
    dt[,sex_id := ifelse(sex == "Male",1,2)]
    do_outliering()

    ## prep for xwalk versioning
    dt[,crosswalk_parent_seq := seq]
    dt[,unit_value_as_published := 0]
    dt[,`unit_value_as_published` := as.double(`unit_value_as_published`)]
    path <- paste0(out.bundle,log$me_name,"_xwalked_",decomp_step,".xlsx")
    write.xlsx(dt,path,sheetName="extraction")
    print("SAVING XWALK VERSION")
    cversion <- save_crosswalk_version(bvid,path,description = xwalk_description)
    cversion <- cversion[,.(bid = log$bid,crosswalk_version_id)]

    ## save log of IDs
    log <- merge(log,cversion,by="bid")
    log[, c("timestamp","notes"):= .(as.character(Sys.time()),xwalk_description)]
    setcolorder(log,names(logs))
    logs <- rbind(log,logs)
    write.csv(logs,file.path(out.bundle,"occ_bid_logs.csv"),row.names=F)
  }
}

