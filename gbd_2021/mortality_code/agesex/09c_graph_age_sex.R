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

if(interactive()){
  version_id <- x
  gbd_year <- x
  loc <- ""
  
}else{
  
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  job_map <- fread("FILEPATH")
  
  version_id <- job_map[task_id, version_id]
  gbd_year <- job_map[task_id, gbd_year]
  loc <- job_map[task_id, ihme_loc_id]
}

## more set-up  =========================================================================

# get previous gbd_year and pull version_id for comparator
gbd_year_table <- get_ids("gbd_round")
gbd_round_id_current <- gbd_year_table[gbd_round==gbd_year, gbd_round_id]
last_cycle <- gbd_year_table[gbd_round_id==(gbd_round_id_current - 1), gbd_round]
last2_cycle <- gbd_year_table[gbd_round_id==(gbd_round_id_current - 2), gbd_round]
previous_version_id <- get_best_versions("Age sex estimate", gbd_year = last_cycle)$`Age sex estimate`
previous2_version_id <- get_best_versions("Age sex estimate", gbd_year = last2_cycle)$`Age sex estimate`

# optional additional comparator
comp_version_id <- previous2_version_id

# set directories
output_dir <- paste0("FILEPATH")
previous_output_dir <- paste0("FILEPATH")

## load in data =========================================================================

## loading in location lists
regs <- data.table(get_locations(level="all"))
regs <- regs[,.(ihme_loc_id, region_name)]
loc_names <- data.table(get_locations(level="all"))[,.(ihme_loc_id, location_name, location_id)]

# load graphing data and subset to graphing loc
d <- fread(paste0("FILEPATH"))
d <- d[ihme_loc_id == loc]

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

## graphing ============================================================================

# Set legend settings
d[,sex:=as.factor(sex)]
d[,category := as.factor(category)]

colors<-brewer.pal(9, "Set1")[c(1:4,7,5)]
colnames<-c( paste0("GBD ", last2_cycle), paste0("GBD ",last_cycle),"current model", "stage2", "stage1", "GPR prescale")
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

d[,age_group_name:=as.factor(age_group_name)]

# create plotting function
plot_age_sex_one_loc <- function(loc, sexes){
  temp <- d[ihme_loc_id==loc & sex==sexes]
  temp$age_group_name <- factor(temp$age_group_name, levels=c("enn", "lnn", "pnn", "pna", "pnb", "inf", "ch", "cha", "chb", "u5"))
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
                            "\n PNA lambda: ",ppna$lambda[ppna$ihme_loc_id==loc & ppna$sex==sexes],
                            ", zeta: ", ppna$zeta[ppna$ihme_loc_id==loc & ppna$sex==sexes],
                            ", amp: ", round(unique(amppna$amplitude[amppna$ihme_loc_id==loc & amppna$sex==sexes]),3),
                            ", scale: ", ppna$scale[ppna$ihme_loc_id==loc & ppna$sex==sexes],
                            "\n PNB lambda: ",ppnb$lambda[ppnb$ihme_loc_id==loc & ppnb$sex==sexes],
                            ", zeta: ", ppnb$zeta[ppnb$ihme_loc_id==loc & ppnb$sex==sexes],
                            ", amp: ", round(unique(amppnb$amplitude[amppnb$ihme_loc_id==loc & amppnb$sex==sexes]),3),
                            ", scale: ", ppnb$scale[ppnb$ihme_loc_id==loc & ppnb$sex==sexes],
                            "\n INF lambda: ", pinf$lambda[pinf$ihme_loc_id==loc & pinf$sex==sexes],
                            ", zeta: ", pinf$zeta[pinf$ihme_loc_id==loc & pinf$sex==sexes],
                            ", amp: ", round(unique(ampinf$amplitude[ampinf$ihme_loc_id==loc & ampinf$sex==sexes]),3),
                            ", scale: ", pinf$scale[pinf$ihme_loc_id==loc & pinf$sex==sexes],
                            "\n CH lambda: ", pch$lambda[pch$ihme_loc_id==loc & pch$sex==sexes],
                            ", zeta: ", pch$zeta[pch$ihme_loc_id==loc & pch$sex==sexes],
                            ", amp: ", round(unique(ampch$amplitude[ampch$ihme_loc_id==loc & ampch$sex==sexes]),3),
                            ", scale: ", pch$scale[pch$ihme_loc_id==loc & pch$sex==sexes],
                            "\n CHA lambda: ", pcha$lambda[pcha$ihme_loc_id==loc & pcha$sex==sexes],
                            ", zeta: ", pcha$zeta[pcha$ihme_loc_id==loc & pcha$sex==sexes],
                            ", amp: ", round(unique(ampcha$amplitude[ampcha$ihme_loc_id==loc & ampcha$sex==sexes]),3),
                            ", scale: ", pcha$scale[pcha$ihme_loc_id==loc & pcha$sex==sexes],
                            "\n CHB lambda: ", pchb$lambda[pchb$ihme_loc_id==loc & pchb$sex==sexes],
                            ", zeta: ", pchb$zeta[pchb$ihme_loc_id==loc & pchb$sex==sexes],
                            ", amp: ", round(unique(ampchb$amplitude[ampchb$ihme_loc_id==loc & ampchb$sex==sexes]),3),
                            ", scale: ", pchb$scale[pchb$ihme_loc_id==loc & pchb$sex==sexes]," \n reasons for outliering include ", outlier_reasons)
  p <- ggplot() +
    geom_ribbon(data=temp[category == paste0("GBD ",last_cycle)], aes(x=year_id, ymax=upper, ymin=lower), fill="#377EB8",alpha=.3)
  p <- p +
    geom_ribbon(data = temp[category == "current model"], aes(x=year_id, ymax=upper, ymin=lower), fill="#4DAF4A",alpha=.3) +
    geom_line(data = temp[category == "stage1" | category=="stage2" |  category==paste0("GBD ", last2_cycle) | category==paste0("GBD ",last_cycle) | category=="current model"], aes(x=year_id, y=med ,colour=category)) +
    geom_point(data = temp[category == "previous data",], aes(x=year, y=med, fill=source_type), size=2, shape=21, alpha=0.2) +
    geom_point(data = temp[category == "previous outliered_data",], aes(x=year, y=med), size=2, shape=21, alpha=0.2) +
    geom_point(data = temp[category == "current data",], aes(x=year, y=med, fill=source_type), size=2, shape=21) +
    geom_point(data = temp[category == "current outliered_data",], aes(x=year, y=med), size=2, shape=21) +
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

## plot by location
print(paste0("FILEPATH"))
pdf(paste0("FILEPATH"), width=15, height=10)
p_males <- plot_age_sex_one_loc(loc, "male")
print(p_males)
p_females <- plot_age_sex_one_loc(loc, "female")
print(p_females)
dev.off()