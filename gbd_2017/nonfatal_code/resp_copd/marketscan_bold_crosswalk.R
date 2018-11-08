## CROSSWALK MARKETSCAN WITH JUST BOLD

library(data.table)
library(ggplot2)
library(readxl)
library(openxlsx)
library(dplyr)
source("FILEPATH/get_epi_data.R")
source("FILEPATH/upload_epi_data.R")

date <- Sys.Date()
date <- gsub("-", "_", date)

# get BOLD in Kentucky
bold <- get_epi_data(122)
bold <- bold[location_name=="Kentucky" & extractor!="USER" & (is.na(group_review) | group_review==1),]
bold[,type:="BOLD"]
setnames(bold,"standard_error","bold_standard_error")

# get raw Marketscan from Kentucky
ky_ms <- get_epi_data(123)
ky_ms <- ky_ms[location_name=="Kentucky" & extractor=="USER",]
ky_ms <- ky_ms[year_start!=2000]
ky_ms[age_start==95,age_start:=99]
ky_ms[,type:="Marketscan"]
setnames(ky_ms,"standard_error","ms_standard_error")

# combine
dt <- rbind(bold,ky_ms,fill=T) %>% as.data.table
dt[,age_mid:=(age_start+age_end)/2]

# collapse over sex (using mean)
dt[,mean:=mean(mean),by=c('nid','age_start','age_end','type')]
dt[,bold_standard_error:=mean(bold_standard_error),by=c('nid','age_start','age_end','type')]
dt[,ms_standard_error:=mean(ms_standard_error),by=c('nid','age_start','age_end','type')]
dt <- unique(dt,by=c('nid','age_start','age_end','type'))
dt[,sex:="Both"]

# create an empty data table with 5 yr age groups
ratios <- data.table(age_start=seq(0,95,5),age_end=seq(4,99,5))
ratios[,age_mid:=(age_start+age_end)/2]

ratios$bold <- predict(loess(mean~age_mid,data=dt[type=="BOLD"]), newdata=ratios)
ratios$ms <- predict(loess(mean~age_mid,data=dt[type=="Marketscan"]), newdata=ratios)

ratios[,ratio:=bold/ms]

# carry forward and backward to low ages and high ages
max_age <- max(ratios[!is.na(ratio),age_start])
min_age <- min(ratios[!is.na(ratio),age_start]) 
ratio_min_age <- unique(ratios[age_start==min_age,ratio])
ratio_max_age <- unique(ratios[age_start==max_age,ratio])
ratios[age_start<min_age,ratio:=ratio_min_age]
ratios[age_start>max_age,ratio:=ratio_max_age]

# se
se <- data.table(age_start=seq(0,95,5),age_end=seq(4,99,5))
se[,age_mid:=(age_start+age_end)/2]

bold[,age_mid:=(age_start+age_end)/2]
se$se_bold <- predict(loess(bold_standard_error~age_mid,data=bold), newdata=se)

ratios <- merge(ratios,se_bold_all,by=c("age_start","age_end","age_mid"))
se_min_age <- unique(ratios[age_start==min_age,se_bold])
se_max_age <- unique(ratios[age_start==max_age,se_bold])
ratios[age_start<min_age,se_bold:=se_min_age]
ratios[age_start>max_age,se_bold:=se_max_age]

# change age_start
ratios[age_start==0,age_start:=1]

# clear out marketscan and taiwan claims in prepped bundle
old <- as.data.table(get_epi_data(122))
old <- old[extractor=="USER",c("seq")]
write.xlsx(old, paste0("FILEPATH/",122,"_remove_claims.xlsx"), sheetName = "extraction")
upload_epi_data(122, filepath = paste0("FILEPATH/",122,"_remove_claims.xlsx"))

# read in raw marketscan data
ms <- get_epi_data(123)
ms <- ms[extractor=="USER",]

# merge on final ratios
ms[age_start==95,age_end:=99]
new <- merge(ms,ratios,by=c("age_start","age_end"))

# calculate new means
new[,mean:=mean*ratio]
new[,standard_error:=se_bold]
new[standard_error>1,standard_error:=0.9]

# drop lower and upper
new[,lower:=NA]
new[,upper:=NA]
new[,uncertainty_type_value:=NA]

# add note_modeler
new[,note_modeler:=paste0(note_modeler,"| adjusted with ratios from BOLD", date)]

# remove seq values
new[,seq:=NA]

# save and upload
write.xlsx(new, paste0("FILEPATH/",date,"_marketscan_crosswalk.xlsx"), sheetName="extraction")
upload_epi_data(bundle_id=122, paste0("FILEPATH/",date,"_marketscan_crosswalk.xlsx"))
