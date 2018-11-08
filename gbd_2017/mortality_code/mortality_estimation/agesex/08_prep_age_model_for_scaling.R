###############################
## Date: 10/24/16
## Purpose: format gpr output for scaling
###################################

## Initializing R, libraries
rm(list=ls())

library(RMySQL)
library(data.table)
library(foreign)
library(plyr)
library(haven)
library(argparse)


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of age-sex splitting')
parser$add_argument('--ihme_loc_id', type="character", required=TRUE,
                    help='The ihme_loc_id for this run of age-sex splitting')
parser$add_argument('--location_id', type="integer", required=TRUE,
                    help='The location_id for this run of age-sex splitting')
parser$add_argument('--version_5q0_id', type="integer", required=TRUE,
                    help='The version_5q0_id for this run of age-sex splitting')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
loc <- args$ihme_loc_id
location_id <- args$location_id
version_5q0_id <- args$version_5q0_id


output_dir <- "FILEPATH"
output_5q0_dir <- "FILEPATH"

# Get location data
location_data <- fread("FILEPATH")


## reading in GPR
sex_input <- c("male", "female")
age_input <- c("enn", "lnn", "pnn", "inf", "ch")

s3 <- list()
missing_files <- c()
for (sex in sex_input){
  for(age in age_input){
    cat(paste(loc, sex, age, sep="_")); flush.console()
    file_dir = "FILEPATH"
    file <- "FILEPATH"
    if(file.exists(file)){
      s3[[paste0(file)]] <- fread(file)
      s3[[paste0(file)]]$age_group_name <- age
      s3[[paste0(file)]]$sex_name <- sex
    } else {
      missing_files <- c(missing_files, file)
    }
  }
}

if(length(missing_files)>0) stop("Files are missing.")

s3 <- rbindlist(s3)
s3[,mort:=exp(mort)]
s3 <- dcast.data.table(s3, ihme_loc_id+year+sim+sex_name~age_group_name, value.var="mort")
setnames(s3, c("sex_name", "sim", "enn", "lnn", "pnn", "inf", "ch"),
         c("sex", "simulation", "q_enn_", "q_lnn_", "q_pnn_", "q_inf_", "q_ch_"))


## reading in birth sex ratio
births <- fread("FILEPATH")
births <- merge(births, location_data[, c('location_id', 'ihme_loc_id')],
                all.x=T, by=c('location_id'))
births <- births[,c("location_name", "sex_id", "location_id"):=NULL]
births <- births[ihme_loc_id==loc]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var="births")
births[,birth_sexratio:=male/female]
births <- births[,c("both", "female", "male"):=NULL]
births[,year:=year+0.5]

## reading in both sex 5q0
ch <- fread("FILEPATH")
ch <- ch[ihme_loc_id==loc]
setnames(ch, c("sim", "mort"), c("simulation", "q_u5_both"))
ch[,simulation:=as.numeric(simulation)]
setkey(ch, NULL)
ch <- unique(ch)

## reading in sex model results, calculating sex specific 5q0
ch2 <- fread("FILEPATH")
ch2[,mort := exp(mort)/(1+exp(mort))]
ch2[,mort := (mort * 0.7) + 0.8]

setnames(ch2, "sim", "simulation")
ch2 <- merge(ch2, births, all=T, by=c("ihme_loc_id", "year"))
ch2 <- merge(ch2, ch, by=c("ihme_loc_id", "year", "simulation"))

ch2[,q_u5_female:= (q_u5_both*(1+birth_sexratio))/(1+(mort * birth_sexratio))]
ch2[,q_u5_male:= q_u5_female * mort]

ch2 <- melt.data.table(ch2, id.vars=c("ihme_loc_id", "year", "simulation", "birth_sexratio"),
                       measure.vars=c("q_u5_both", "q_u5_female", "q_u5_male"), value.name="q_u5_", variable.name="sex")

ch2[sex=="q_u5_both", sex:="both"]
ch2[sex=="q_u5_male", sex:="male"]
ch2[sex=="q_u5_female", sex:="female"]


## mering in birthsex ratio and 5q0
s3 <- merge(s3, ch2, by=c("ihme_loc_id", "year", "simulation", "sex"), all=T)

write.csv(s3, "FILEPATH", row.names=F)
