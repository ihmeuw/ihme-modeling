#####################################################
## Purpose: Graph age-sex model results
#####################################################

rm(list=ls())

library(RMySQL)
library(foreign)
library(data.table)
library(readstata13)
library(plyr)
library(RColorBrewer)
library(ggplot2)
library(haven)
library(argparse)

## load central functions
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")
source("FILEPATH/get_ids.R")

username <- Sys.getenv("USER")

## get args =============================================================================

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='Age sex estimate version id for this run')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='GBD Year')

args <- parser$parse_args()
version_id <- args$version_id
gbd_year <- args$gbd_year

## more set-up  =========================================================================

gbd_year_table <- get_ids("gbd_round")
gbd_round_id_current <- gbd_year_table[gbd_round==gbd_year, gbd_round_id]
last_cycle <- gbd_year_table[gbd_round_id==(gbd_round_id_current - 1), gbd_round]
previous_version_id <- get_best_versions("Age sex estimate", gbd_year = last_cycle)$`Age sex estimate`

# set directories
output_dir <- "FILEPATH"
previous_output_dir <- "FILEPATH"
dir.create(paste0(output_dir, "/graphs"), showWarnings = F)
dir.create(paste0(output_dir, "/graphs/location_specific"), showWarnings = F)

## load in data =========================================================================

## loading in location lists
regs <- data.table(get_locations(level="all"))
regs <- regs[,.(ihme_loc_id, region_name)]
loc_names <- data.table(get_locations(level="all"))[,.(ihme_loc_id, location_name, location_id)]

## load in scaled estimates
load_scaled <- function(version){
  summary_files <- list.files(paste0("FILEPATH"), full.names = T)
  gpr <- data.table()
  for(file in summary_files){
    temp <- as.data.table(read.csv(file))
    change_cols <- names(temp)[names(temp) %like% 'change']
    keep_cols <- setdiff(names(temp),change_cols)
    temp <- temp[,c(keep_cols),with=F]
    gpr <- rbind(gpr,temp)
  }
  gpr <- merge(gpr, loc_names, by='ihme_loc_id')
  return(gpr)
}
gpr <- load_scaled(version_id)
gpr_comp <- load_scaled(previous_version_id)
if(exists("comp_version_id")) gpr_comp2 <- load_scaled(comp_version_id)

## load in input data
d <- data.table(read.dta(paste0(output_dir, "/input_data_original.dta")))

## load stage 1 and stage 2 males and females
s2_f <- fread(paste0(output_dir, "/age_model/stage_1/stage_1_age_pred_female.csv"))
s2_m <-  fread(paste0(output_dir, "/age_model/stage_1/stage_1_age_pred_male.csv"))

## load in parameters
params <- list()
for(age_names in c("enn", "lnn", "pnn", "ch", "inf")){
  for(sex_names in c("male", "female")){
    params[[paste(sex_names, age_names, sep="_")]] <-
      fread(paste0(output_dir, "/age_model/stage_2/", sex_names, "_", age_names, "_model_params.txt"))
    params[[paste(sex_names, age_names, sep="_")]]$sex <- sex_names
    params[[paste(sex_names, age_names, sep="_")]]$age <- age_names
  }
}
params <- rbindlist(params)

penn <- params[age == "enn"]
plnn <- params[age == "lnn"]
ppnn <- params[age == "pnn"]
pinf <- params[age == "inf"]
pch <- params[age == "ch"]

## more parameters
amp2 <- fread(paste0(output_dir, "/age_model/stage_2/gpr_input_file_female.csv"))
amp1 <- fread(paste0(output_dir, "/age_model/stage_2/gpr_input_file_male.csv"))

amp <- rbind(amp1, amp2)
amp <- amp[,.(ihme_loc_id, year_id, age_group_name, sex, mse)]
amp <- unique(amp)
amp[,amplitude := sqrt(mse)]

ampenn <- amp[age_group_name == "enn"]
amplnn <- amp[age_group_name == "lnn"]
amppnn <- amp[age_group_name == "pnn"]
ampinf <- amp[age_group_name == "inf"]
ampch <- amp[age_group_name == "ch"]

## prep/format ======================================================================

## (1) FINAL RESULTS

gpr[, location_id := NULL]
gpr <- merge(gpr, regs, by='ihme_loc_id', all.x=T)
gpr <- merge(gpr, loc_names, by='ihme_loc_id', all.x=T)
gpr[,category := "current model"]

gpr_comp[, location_id := NULL]
gpr_comp <- merge(gpr_comp, regs, by='ihme_loc_id', all.x=T)
gpr_comp <- merge(gpr_comp, loc_names, by='ihme_loc_id', all.x=T)
gpr_comp[, category := paste0("GBD ", last_cycle)]

if(exists("comp_version_id")){
  gpr_comp2[, location_id := NULL]
  gpr_comp2 <- merge(gpr_comp2, regs, by='ihme_loc_id', all.x=T)
  gpr_comp2 <- merge(gpr_comp2, loc_names, by='ihme_loc_id', all.x=T)
  gpr_comp2[, category := "previous model"]
}

final_results <- rbind(gpr, gpr_comp, fill=T)
if(exists("comp_version_id")) final_results <- rbind(final_results, gpr_comp2, fill=T)
final_results <- final_results[,c("q_nn_upper", "q_nn_lower", "q_nn_med", "location_id"):=NULL]

if('location_name.y' %in% names(final_results)){
  setnames(final_results,'location_name.y','location_name')
  final_results[,location_name.x:=NULL]
}
ids <- c("region_name", "location_name", "ihme_loc_id", "sex", "year", "category")
measures <- names(final_results)[!names(final_results) %in% ids]
final_results <- melt.data.table(final_results, id.vars= ids, measure.vars= measures)
final_results[,variable := as.character(variable)]

final_results[,age_group_name:=tstrsplit(variable, "_", keep=2)]
final_results[,type:=tstrsplit(variable, "_", keep=3)]

final_results[,variable:=NULL]
final_results <- dcast.data.table(final_results, region_name + location_name + ihme_loc_id + sex +
                                  year + category + age_group_name ~ type, value.var="value")
final_results[,year_id:=floor(year)]

## (2) STAGE 1 & 2

s2_f[,sex:="female"]
s2_m[,sex:="male"]
s2 <- rbind(s2_f, s2_m, use.names=T)
setnames(s2, "year", "year_id")
s2 <- s2[,.(ihme_loc_id, year_id, age_group_name, pred_log_qx_s1, pred_log_qx_s2, sex)]
s2 <- unique(s2)
setnames(s2, c("pred_log_qx_s1", "pred_log_qx_s2"), c("stage1", "stage2"))

s2 <- melt.data.table(s2, id.vars=c("ihme_loc_id", "sex", "year_id", "age_group_name"),
                      measure.vars = c("stage1", "stage2"), value.name="med", variable.name="category")
s2[,med := exp(med)]
s2 <- merge(s2, regs, by="ihme_loc_id", all.x=T)

## (3) GPR PRE-SCALING

gpr_prescale <- data.table(age=NA, sex=NA, ihme_loc_id=NA, year=NA, prescale_qx=NA)
for(aa in c('enn','lnn','pnn','inf','ch')){
  print(aa)
  for(ss in c('male','female')){
    print(ss)
    prescale_files <- list.files(paste0(output_dir,'/age_model/gpr/',ss,'/',aa,'/'), pattern=paste0(aa,'.txt'), full.names=T)
    for(f in prescale_files){
      ps <- read.csv(f)
      ps <- as.data.table(ps)
      ps[,age_group_name:=aa]
      ps[,sex:=ss]
      ps[,med:=exp(med)]
      ps[,c('lower','upper'):=NULL]
      gpr_prescale <- rbind(gpr_prescale,ps,fill=T)
    }
  }
}
gpr_prescale <- gpr_prescale[!is.na(age)]
gpr_prescale[,category:='GPR prescale']
gpr_prescale[,year_id:=year-0.5]

## (4) INPUT DATA

keep1 <- names(d)[grepl("exclude_", names(d))]
keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", "q_enn", "q_lnn", "q_pnn", "q_inf", "q_ch", "q_u5", keep1)
d <- d[,names(d) %in% keep, with=F]
temp <- copy(d)
d <- melt.data.table(d, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", keep1),
                     measure.vars=c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf"), variable.name = "age_group_name" ,value.name="qx_data")
d[,age_group_name := gsub("q_", "", age_group_name)]

temp <- temp[,c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf") := NULL]
temp <- melt.data.table(temp, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource"), measure.vars = keep1,
                        variable.name="age_group_name", value.name="exclude")
temp[,age_group_name := gsub("exclude_", "", age_group_name)]

d <- d[,!names(d) %in% keep1, with=F]
d <- d[!is.na(qx_data)]
d <- merge(d, temp, by=c("region_name", "ihme_loc_id", "year", "sex", "age_group_name", "source", "broadsource"), all.x =T)
d <- unique(d, by=c())

setnames(d, "qx_data", "med")
d[,source:=NULL]
d[,category:="data"]
d[exclude!=0, category:="outliered_data"]
d[,year_id := round(year)]

d[,source_type:=broadsource]
d[,source_type:=tolower(source_type)]
d[grepl("dhs", source_type), source_type:="dhs"]
d[grepl("survey", source_type), source_type:="survey_other"]
d[grepl("lsms", source_type), source_type:="lsms"]
d[grepl("census", source_type), source_type:="vr"]
d[!source_type %in% c("dhs","survey_other","lsms", "cdc rhs", "dsp","srs", "vr","wfs"), source_type:="survey_other"]

## append all =========================================================================

temp <- list(d, final_results, s2, gpr_prescale)
d <- rbindlist(temp, use.names=T, fill=T)

setnames(params, "age" ,"age_group_name")
d <- merge(d, params, all.x=T, by=c("ihme_loc_id", "sex", "age_group_name"))

## graphing ============================================================================

d[,sex:=as.factor(sex)]
d[,category := as.factor(category)]

colors<-brewer.pal(9, "Set1")[c(1:4,7,5)]
colnames<-c( "previous model", paste0("GBD ",last_cycle),"current model", "stage2", "stage1", "GPR prescale")
names(colors)<-colnames
colScale <- scale_color_manual(name = "", values = colors)

shs <- c(24,21)
shsnames <- c("outliered_data","data")
names(shs) <- shsnames
shapescale <- scale_shape_manual(name = "", values = shs)

gg <- brewer.pal(8,"Paired")
cols <- c(gg)
colsnames <- c("cdc rhs","dhs", "dsp", "lsms", "srs", "survey_other","vr","wfs")
names(cols) <- colsnames
colfScale <- scale_fill_manual(name = "", values = cols)

d <- d[age_group_name!="nn"]
d[,age_group_name:=as.factor(age_group_name)]
d <- d[year_id>=1949]
d <- d[,location_name:=NULL]

d <- merge(d, loc_names, by="ihme_loc_id", all.x=T)
d <- d[ihme_loc_id!="GBR_4749"]
d[,exclude:=as.character(exclude)]
d[exclude=="0" ,exclude:="keep"]
d[exclude=="2" ,exclude:="excluded from gpr "]
d[exclude=="3" ,exclude:="incomplete "]
d[exclude=="11" ,exclude:="CBH before range "]
d[exclude=="12" ,exclude:="under 0.0001 "]

d <- d[age_group_name %in% c("enn","lnn","pnn","inf","ch","u5")]

plot_age_sex_one_loc <- function(loc, sexes){
  temp <- d[ihme_loc_id==loc & sex==sexes]
  temp$age_group_name <- factor(temp$age_group_name, levels=c("enn", "lnn", "pnn", "inf", "ch", "u5"))
  cat(paste0(loc,"\n")); flush.console()
  loc_name <- unique(temp$location_name[temp$ihme_loc_id==loc])
  outlier_reasons <- paste(unique(temp$exclude[!is.na(temp$exclude) & temp$exclude!="keep"]), collapse="")
  title_string = "Age-sex split of under-5 mortality"
  subtitle_string =  paste0(loc, " Child Mort ", sexes," \n ENN lambda: ", penn$lambda[penn$ihme_loc_id==loc & penn$sex==sexes],
                            ", zeta: ", penn$zeta[penn$ihme_loc_id==loc & penn$sex==sexes],
                            ", amp: ", round(unique(ampenn$amplitude[ampenn$ihme_loc_id==loc & ampenn$sex==sexes]),3),
                            ", scale: ", penn$scale[penn$ihme_loc_id==loc & penn$sex==sexes],
                            "\n LNN lambda: " ,plnn$lambda[plnn$ihme_loc_id==loc & plnn$sex==sexes],
                            ", zeta: ", plnn$zeta[plnn$ihme_loc_id==loc & plnn$sex==sexes],
                            ", amp: ", round(unique(amplnn$amplitude[amplnn$ihme_loc_id==loc & amplnn$sex==sexes]),3),
                            ", scale: ", plnn$scale[plnn$ihme_loc_id==loc & plnn$sex==sexes],
                            "\n PNN lambda: ",ppnn$lambda[ppnn$ihme_loc_id==loc & ppnn$sex==sexes],
                            ", zeta: ", ppnn$zeta[ppnn$ihme_loc_id==loc & ppnn$sex==sexes],
                            ", amp: ", round(unique(amppnn$amplitude[amppnn$ihme_loc_id==loc & amppnn$sex==sexes]),3),
                            ", scale: ", ppnn$scale[ppnn$ihme_loc_id==loc & ppnn$sex==sexes],
                            "\n INF lambda: ", pinf$lambda[pinf$ihme_loc_id==loc & pinf$sex==sexes],
                            ", zeta: ", pinf$zeta[pinf$ihme_loc_id==loc & pinf$sex==sexes],
                            ", amp: ", round(unique(ampinf$amplitude[ampinf$ihme_loc_id==loc & ampinf$sex==sexes]),3),
                            ", scale: ", pinf$scale[pinf$ihme_loc_id==loc & pinf$sex==sexes],
                            "\n CH lambda: ", pch$lambda[pch$ihme_loc_id==loc & pch$sex==sexes],
                            ", zeta: ", pch$zeta[pch$ihme_loc_id==loc & pch$sex==sexes],
                            ", amp: ", round(unique(ampch$amplitude[ampch$ihme_loc_id==loc & ampch$sex==sexes]),3),
                            ", scale: ", pch$scale[pch$ihme_loc_id==loc & pch$sex==sexes]," \n reasons for outliering include ", outlier_reasons)
  p <- ggplot() +
    geom_ribbon(data=temp[category == paste0("GBD ",last_cycle)], aes(x=year_id, ymax=upper, ymin=lower), fill="#377EB8",alpha=.3)
  if(nrow(temp[temp$category=="previous model"])>0){
    p <- p + geom_ribbon(data=temp[category == "previous model"], aes(x=year_id, ymax=upper, ymin=lower), fill="#E41A1C",alpha=.3)
  }
  p <- p +
    geom_ribbon(data=temp[category == "current model"], aes(x=year_id, ymax=upper, ymin=lower), fill="#4DAF4A",alpha=.3) +
    geom_line(data = temp[category == "stage1" | category=="stage2" |  category=="previous model"| category==paste0("GBD ",last_cycle) | category=="current model"], aes(x=year_id, y=med ,colour=category)) +
    geom_point(data = temp[category == "data",], aes(x=year, y=med, fill=source_type), size=2, shape=21) +
    geom_point(data = temp[category == "outliered_data",], aes(x=year, y=med), size=2, shape=21) +
    colScale +
    colfScale +
    shapescale +
    facet_wrap(~age_group_name, scales="free") +
    labs(title = title_string,
         subtitle = subtitle_string,
         x= "year",
         y= "qx") +
    theme(plot.title = element_text(face="bold"))
  return(p)
}

# plot by location
for(loc in sort(unique(d$ihme_loc_id))) {
  print(paste0(output_dir, "/graphs/location_specific/", loc, ".pdf"))
  pdf(paste0(output_dir, "/graphs/location_specific/", loc, ".pdf"), width=15, height=10)
  p_males <- plot_age_sex_one_loc(loc, "male")
  print(p_males)
  p_females <- plot_age_sex_one_loc(loc, "female")
  print(p_females)
  dev.off()
}


# plot all locations
pdf(paste0(output_dir, "/graphs/all_locations_version_", version_id, ".pdf"), width=15, height=10)
for(loc in sort(unique(d$ihme_loc_id))){
  p <- plot_age_sex_one_loc(loc, "male")
  print(p)
  p <- plot_age_sex_one_loc(loc, "female")
  print(p)
}
dev.off()
