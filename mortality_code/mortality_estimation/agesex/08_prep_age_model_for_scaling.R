

rm(list=ls())

library(RMySQL)
library(data.table)
library(foreign)
library(plyr)
library(haven)

if (Sys.info()[1] == 'Windows') {
  username <- "USER"
  root <- "FILEPATH"
  dir <- paste0("FILEPATH")
  loc <- "USA"
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  dir <- paste0("FILEPATH")
  loc <- commandArgs()[3]
  print(loc)
}


## reading in GPR

sex_input <- c("male", "female")
age_input <- c("enn", "lnn", "pnn", "inf", "ch")

s3 <- list()
missing_files <- c()
for (sex in sex_input){
  for(age in age_input){
    cat(paste(loc, sex, age, sep="_")); flush.console()
    file <- paste0("FILEPATH/" , sex, "_", age, "/gpr_", loc, "_", sex,"_", age, "_sim.txt")
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
births <- data.table(read_dta(paste0(root, "FILEPATH/births_gbd2016.dta")))
births <- births[,c("location_name", "sex_id", "source", "location_id"):=NULL]
births <- births[ihme_loc_id==loc]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var="births")
births[,birth_sexratio:=male/female]
births <- births[,c("both", "female", "male"):=NULL]
births[,year:=year+0.5]

## reading in both sex 5q0
ch <- fread(paste0("FILEPATH/gpr_", loc, "_sim.txt"))
ch <- ch[ihme_loc_id==loc]
setnames(ch, c("sim", "mort"), c("simulation", "q_u5_both"))
ch[,simulation:=as.numeric(simulation)]
setkey(ch, NULL)
ch <- unique(ch) 

ch2 <- fread(paste0("FILEPATH/gpr_", loc, "_sim.txt"))
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

write.csv(s3, paste0("FILEPATH/scaling_input_", loc, ".csv"), row.names=F)

