#####################################################
## Purpose: Prep data for graph age-sex model results
#####################################################

rm(list=ls())

library(RMySQL)
library(foreign)
library(data.table)
library(readstata13)
library(plyr)
library(haven)
library(argparse)

## load central functions
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")
source("FILEPATH")

username <- Sys.getenv("USER")

## get args =============================================================================

if(interactive()){
  version_id <-
  gbd_year <-

}else{

  parser <- ArgumentParser()
  parser$add_argument("--version_id", type="integer", required=TRUE,
                      help="Age sex estimate version id for this run")
  parser$add_argument("--gbd_year", type="integer", required=TRUE,
                      help="GBD Year")

  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
}

## more set-up  =========================================================================

# get previous gbd_year and pull version_id for comparator
gbd_year_table <- get_ids("gbd_round")
gbd_round_id_current <- gbd_year_table[gbd_round==gbd_year, gbd_round_id]
last_cycle <- gbd_year_table[gbd_round_id==(gbd_round_id_current - 2), gbd_round]
last2_cycle <- gbd_year_table[gbd_round_id==(gbd_round_id_current - 3), gbd_round]
previous_version_id <- get_best_versions("Age sex estimate", gbd_year = last_cycle)$`Age sex estimate`
previous2_version_id <- get_best_versions("Age sex estimate", gbd_year = last2_cycle)$`Age sex estimate`

# optional additional comparator
comp_version_id <- previous2_version_id

# set directories
output_dir <- paste0("FILEPATH")
previous_output_dir <- paste0("FILEPATH")
dir.create(paste0("FILEPATH"), showWarnings = F)
dir.create(paste0("FILEPATH"), showWarnings = F)

## load in data =========================================================================

## loading in location lists
regs <- data.table(get_locations(level="all"))
regs <- regs[,.(ihme_loc_id, region_name)]
loc_names <- data.table(get_locations(level="all"))[,.(ihme_loc_id, location_name, location_id)]
for_ordering <- get_locations(gbd_year = gbd_year)[, .(ihme_loc_id, region_name, super_region_name)]

## load in scaled estimates
load_scaled <- function(version){
  summary_files <- list.files(paste0("FILEPATH"), full.names = T)
  gpr <- data.table()
  for(file in summary_files){
    temp <- as.data.table(read.csv(file))
    change_cols <- names(temp)[names(temp) %like% "change"]
    keep_cols <- setdiff(names(temp),change_cols)
    temp <- temp[,c(keep_cols),with=F]
    gpr <- rbind(gpr,temp)
  }
  gpr <- merge(gpr, loc_names, by="ihme_loc_id")
  return(gpr)
}
gpr <- load_scaled(version_id)
gpr_comp <- load_scaled(previous_version_id)
if(!is.na(comp_version_id)) gpr_comp2 <- load_scaled(comp_version_id)

## load in input data
d <- fread(paste0("FILEPATH"))
if(last_cycle <= 2019){
  d_old <- data.table(read.dta(paste0("FILEPATH")))
}else{
  d_old <- fread(paste0("FILEPATH"))

}

# only compare current gbd_round locations
d_old <- merge(d_old, regs[, .(ihme_loc_id)], by = "ihme_loc_id")

## load stage 1 and stage 2 males and females
s2_f <- fread(paste0("FILEPATH"))
s2_m <-  fread(paste0("FILEPATH"))

## load in parameters
params <- list()
for(age_names in c("enn", "lnn", "pnn", "pna", "pnb", "ch", "cha", "chb", "inf")){
  for(sex_names in c("male", "female")){
    params[[paste(sex_names, age_names, sep="_")]] <-
      fread(paste0("FILEPATH"))
    params[[paste(sex_names, age_names, sep="_")]]$sex <- sex_names
    params[[paste(sex_names, age_names, sep="_")]]$age <- age_names
  }
}
params <- rbindlist(params)

penn <- params[age == "enn"]
plnn <- params[age == "lnn"]
ppnn <- params[age == "pnn"]
ppna <- params[age == "pna"]
ppnb <- params[age == "pnb"]
pinf <- params[age == "inf"]
pch <- params[age == "ch"]
pcha <- params[age == "cha"]
pchb <- params[age == "chb"]

## more parameters
amp2 <- fread(paste0("FILEPATH"))
amp1 <- fread(paste0("FILEPATH"))

amp <- rbind(amp1, amp2)
amp <- amp[,.(ihme_loc_id, year_id, age_group_name, sex, mse)]
amp <- unique(amp)
amp[,amplitude := sqrt(mse)]

ampenn <- amp[age_group_name == "enn"]
amplnn <- amp[age_group_name == "lnn"]
amppnn <- amp[age_group_name == "pnn"]
amppna <- amp[age_group_name == "pna"]
amppnb <- amp[age_group_name == "pnb"]
ampinf <- amp[age_group_name == "inf"]
ampch <- amp[age_group_name == "ch"]
ampcha <- amp[age_group_name == "cha"]
ampchb <- amp[age_group_name == "chb"]

## prep/format ======================================================================

## (1) FINAL RESULTS

gpr[, location_id := NULL]
gpr <- merge(gpr, regs, by="ihme_loc_id", all.x=T)
gpr <- merge(gpr, loc_names, by="ihme_loc_id", all.x=T)
gpr[,category := "current model"]

gpr_comp[, location_id := NULL]
gpr_comp <- merge(gpr_comp, regs, by="ihme_loc_id", all.x=T)
gpr_comp <- merge(gpr_comp, loc_names, by="ihme_loc_id", all.x=T)
gpr_comp[, category := paste0("GBD ", last_cycle)]

if(!is.na(comp_version_id)){
  gpr_comp2[, location_id := NULL]
  gpr_comp2 <- merge(gpr_comp2, regs, by="ihme_loc_id", all.x=T)
  gpr_comp2 <- merge(gpr_comp2, loc_names, by="ihme_loc_id", all.x=T)
  gpr_comp2[, category := paste0("GBD ", last2_cycle)]
}

final_results <- rbind(gpr, gpr_comp, fill=T)
if(!is.na(comp_version_id)) final_results <- rbind(final_results, gpr_comp2, fill=T)
final_results <- final_results[,c("q_nn_upper", "q_nn_lower", "q_nn_med", "location_id"):=NULL]

## reshaping to get long by age and wide by med, lower, upper
if("location_name.y" %in% names(final_results)){
  setnames(final_results,"location_name.y","location_name")
  final_results[,location_name.x:=NULL]
}
ids <- c("region_name", "location_name", "ihme_loc_id", "sex", "year", "category")
measures <- names(final_results)[!names(final_results) %in% ids]
final_results <- melt.data.table(final_results, id.vars= ids, measure.vars= measures)
final_results[,variable := as.character(variable)]

final_results[,age_group_name:=tstrsplit(variable, "_", keep=2)]
final_results[,type:=tstrsplit(variable, "_", keep=3)]

final_results[,variable:=NULL]
final_results <- dcast.data.table(final_results, region_name + location_name + ihme_loc_id + sex + year + category + age_group_name ~type, value.var="value")
final_results[,year_id:=floor(year)]

## (2) STAGE 1 & 2

s2_f[,sex:="female"]
s2_m[,sex:="male"]
s2 <- rbind(s2_f, s2_m, use.names=T)
setnames(s2, "year", "year_id")
s2 <- s2[,.(ihme_loc_id, year_id, age_group_name, pred_log_qx_s1, pred_log_qx_s2, sex)]
s2 <- unique(s2)
setnames(s2, c("pred_log_qx_s1", "pred_log_qx_s2"), c("stage1", "stage2"))

s2 <- melt.data.table(s2, id.vars=c("ihme_loc_id", "sex", "year_id", "age_group_name"), measure.vars = c("stage1", "stage2"), value.name="med", variable.name="category")
s2[,med := exp(med)]
s2 <- merge(s2, regs, by="ihme_loc_id", all.x=T)

## (3) GPR PRE-SCALING

gpr_prescale <- data.table(age=NA, sex=NA, ihme_loc_id=NA, year=NA, prescale_qx=NA)
for(aa in c("enn","lnn","pnn", "pna", "pnb", "inf","ch", "cha", "chb")){
  print(aa)
  for(ss in c("male","female")){
    print(ss)
    prescale_files <- list.files(paste0("FILEPATH"), pattern=paste0(aa,".txt"), full.names=T)
    for(f in prescale_files){
      ps <- read.csv(f)
      ps <- as.data.table(ps)
      ps[,age_group_name:=aa]
      ps[,sex:=ss]
      ps[,med:=exp(med)]
      ps[,c("lower","upper"):=NULL]
      gpr_prescale <- rbind(gpr_prescale,ps,fill=T)
    }
  }
}
gpr_prescale <- gpr_prescale[!is.na(age)]
gpr_prescale[,category:="GPR prescale"]
gpr_prescale[,year_id:=year-0.5]

## (4) INPUT DATA

# current input data
keep1 <- names(d)[grepl("exclude_", names(d))]
keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource",
          "q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch", "q_cha", "q_chb", "q_u5", keep1)
d <- d[,names(d) %in% keep, with=F]
temp <- copy(d)
d <- melt.data.table(d, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", keep1),
                     measure.vars=c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_ch", "q_cha", "q_chb", "q_inf"), variable.name = "age_group_name" ,value.name="qx_data")
d[,age_group_name := gsub("q_", "", age_group_name)]

temp <- temp[,c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_ch", "q_cha", "q_chb", "q_inf") := NULL]
temp <- melt.data.table(temp, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource"), measure.vars = keep1, variable.name="age_group_name", value.name="exclude")
temp[,age_group_name := gsub("exclude_", "", age_group_name)]

d <- d[,!names(d) %in% keep1, with=F]
d <- d[!is.na(qx_data)]
d <- merge(d, temp, by=c("region_name", "ihme_loc_id", "year", "sex", "age_group_name", "source", "broadsource"), all.x =T)
d <- unique(d, by=c())

setnames(d, "qx_data", "med")
d[,source:=NULL]
d[,category:="current data"]
d[exclude!=0, category:="current outliered_data"]
d[,year_id := round(year)]

##classifying sources
d[,source_type:=broadsource]
d[,source_type:=tolower(source_type)]
d[grepl("dhs", source_type), source_type:="dhs"]
d[grepl("survey", source_type), source_type:="survey_other"]
d[grepl("lsms", source_type), source_type:="lsms"]
d[grepl("census", source_type), source_type:="vr"]
d[!source_type %in% c("dhs","survey_other","lsms", "cdc rhs", "dsp","srs", "vr","wfs"), source_type:="survey_other"]

# previous input data
keep1 <- names(d_old)[grepl("exclude_", names(d_old))]
keep2 <- names(d_old)[grepl("q_", names(d_old))]
keep2 <- setdiff(keep2, "q_u5")
keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", "q_u5", keep2, keep1)
d_old <- d_old[,names(d_old) %in% keep, with=F]
temp <- copy(d_old)
d_old <- melt.data.table(d_old, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", keep1),
                         measure.vars=c(keep2), variable.name = "age_group_name" ,value.name="qx_data")
d_old[,age_group_name := gsub("q_", "", age_group_name)]

temp <- temp[,c(keep2) := NULL]
temp <- melt.data.table(temp, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource"), measure.vars = keep1, variable.name="age_group_name", value.name="exclude")
temp[,age_group_name := gsub("exclude_", "", age_group_name)]

d_old <- d_old[,!names(d_old) %in% keep1, with=F]
d_old <- d_old[!is.na(qx_data)]
d_old <- merge(d_old, temp, by=c("region_name", "ihme_loc_id", "year", "sex", "age_group_name", "source", "broadsource"), all.x =T)
d_old <- unique(d_old, by=c())

setnames(d_old, "qx_data", "med")
d_old[,source:=NULL]
d_old[,category:="previous data"]
d_old[exclude!=0, category:="previous outliered_data"]
d_old[,year_id := round(year)]

##classifying sources
d_old[,source_type:=broadsource]
d_old[,source_type:=tolower(source_type)]
d_old[grepl("dhs", source_type), source_type:="dhs"]
d_old[grepl("survey", source_type), source_type:="survey_other"]
d_old[grepl("lsms", source_type), source_type:="lsms"]
d_old[grepl("census", source_type), source_type:="vr"]
d_old[!source_type %in% c("dhs","survey_other","lsms", "cdc rhs", "dsp","srs", "vr","wfs"), source_type:="survey_other"]

## append all =========================================================================

temp <- list(d, d_old, final_results, s2, gpr_prescale)
d <- rbindlist(temp, use.names=T, fill=T)

setnames(params, "age" ,"age_group_name")
d <- merge(d, params, all.x=T, by=c("ihme_loc_id", "sex", "age_group_name"))

d[,sex:=as.factor(sex)]
d[,category := as.factor(category)]

d[,age_group_name:=as.factor(age_group_name)]
d <- d[year_id>=1949]
d <- d[,location_name:=NULL]

d <- merge(d, loc_names, by="ihme_loc_id", all.x=T)
d[,exclude:=as.character(exclude)]
d[exclude=="0" ,exclude:="keep"]
d[exclude=="2" ,exclude:="excluded from gpr "]
d[exclude=="3" ,exclude:="incomplete "]
d[exclude=="11" ,exclude:="CBH before range "]
d[exclude=="12" ,exclude:="under 0.0001 "]

d <- d[age_group_name %in% c("enn","lnn","pnn", "pna", "pnb", "inf", "ch", "cha", "chb","u5")]

# output prepped data file
readr::write_csv(d, paste0("FILEPATH"))
