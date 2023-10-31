
rm(list=ls())
library(foreign)
library(data.table)
library(readxl)

for(MEPS_panel in c(4:18)){
  print(paste("CURRENTLY LOOPING THROUGH PANEL", MEPS_panel))
  MEPS <- read.dta((paste0("FILEPATH",MEPS_panel,"/HH_CONSOLIDATED.dta")))
  names(MEPS)<-tolower(names(MEPS))
  MEPS <- data.table(MEPS)

  ## Standardise education variable ##
  if(MEPS_panel %in% c(4:8)){
    MEPS[educyear>=0, educ_years:=as.numeric(educyear)]
  } else if(MEPS_panel %in% c(9:14)){
    MEPS[educyr>=0, educ_years:=as.numeric(educyr)]
  } else if(MEPS_panel %in% c(15, 16)){
    isced_2011 <- data.table(education=levels(MEPS$eduyrdeg), years=c(NA, NA, NA, NA, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12, 12, 14, 14, 14, 16, 18, 19.5, 21, 0))
    for(i in isced_2011$education){MEPS[educyr==-1 & eduyrdeg==i, educ_years:=isced_2011[education==i, years]]}
    MEPS[educyr>=0, educ_years:=as.numeric(educyr)]
  } else if(MEPS_panel==17){
    isced_2011_1 <- data.table(education=levels(MEPS$eduyrdeg), years=c(NA, NA, NA, NA, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12, 12, 14, 14, 14, 16, 18, 19.5, 21, 0))
    isced_2011_2 <- data.table(education=levels(MEPS$eduyrdg), years=c(NA, NA, NA, NA, 4, 10.5,12, 12, 14, 14, 14, 16, 19.5, 0))
    for(i in isced_2011_2$education){MEPS[eduyrdg==i, educ_years:=isced_2011_2[education==i, years]]}
    for(i in isced_2011_1$education){MEPS[eduyrdeg==i, educ_years:=isced_2011_1[education==i, years]]}
  } else if(MEPS_panel==18){
    isced_2011 <- data.table(education=levels(MEPS$eduyrdg), years=c(NA, NA, NA, NA, 4, 10.5,12, 12, 14, 14, 14, 16, 19.5, 0))
    for(i in isced_2011$education){MEPS[(educyr==-1 | is.na(educyr)) & eduyrdg==i, educ_years:=isced_2011[education==i, years]]}
    MEPS[educyr>=0, educ_years:=as.numeric(educyr)]
  }

  ## Isolate variables needed ##
  income_variables <- names(MEPS)[grep("ttlp", names(MEPS))]
  MEPS <- MEPS[, c("dupersid", "mcs42", "pcs42", "datayear", "panel", "age31x", "age42x", "sex", "region31", "region42", "region53", "educ_years", income_variables), with=F]

  ## label rounds ##
  MEPS[,round := ifelse(datayear==min(datayear), 2, 4)]

  ## SF-12 introduced in Panel 4, year 2000 so exclude data prior to this ##
  MEPS <- MEPS[datayear!=1999,]

  ## Standardise round-specific estimates ##
  MEPS[age42x==-1, age42x:= ifelse(age31x == -1, NA, age31x)]
  MEPS[age31x==-1, age31x:= ifelse(age42x == -1, NA, age42x)]
  MEPS[,`:=` (age = round((age31x + age42x) / 2), age31x=NULL, age42x=NULL)]
  MEPS[,`:=` (personal_income = apply(.SD, 1, mean, na.rm=T)), .SDcols = income_variables]

  # Education reported twice in some panels, once in others. Correct here

  ## Fill missing regions ##
  MEPS[, region := region42]
  MEPS[(region42=="-1 INAPPLICABLE" | region42==-1) & round==2, region := ifelse(region31!="-1 INAPPLICABLE" & region31!=-1, region31, region53)]
  MEPS[(region42=="-1 INAPPLICABLE" | region42==-1) & round==4, region := ifelse(region53!="-1 INAPPLICABLE" & region53!=-1, region31, region31)]

  ## standardise variables ##
  MEPS <- MEPS[, .(dupersid, datayear, panel, age, sex, round, region, education=educ_years, personal_income, mcs=mcs42, pcs=pcs42)]
  if(typeof(MEPS$panel)!="double"){
    MEPS[,`:=` (panel=as.numeric(substring(panel, 1, 2)), sex=as.numeric(substring(sex, 1, 2)), region=as.numeric(substring(region, 1, 2)))]
  }

  ## Pull ICD9 codes for each person ##
  MEPS_medical <- read.dta((paste0("FILEPATH",MEPS_panel,"/IND_MEDICAL_CONDITIONS.dta")))
  names(MEPS_medical)<-tolower(names(MEPS_medical))
  MEPS_medical <- data.table(MEPS_medical)[condrn!="5 ROUND 5" & condrn!=5, .(dupersid, panel, condrn, crnd1, crnd2, crnd3, crnd4, crnd5, icd=icd9codx)] # conditions from round 5 occured AFTER SF-12 was taken

  ## Pull ICD9 to GBD conditgion map ##
  Map <- as.data.table(read_excel("FILEPATH.xlsx"))[,.(icd = `ICD-9`, cond_name = paste0("t", `non-fatal cause`), Acute)]
  Map <- Map[!grepl("N", icd),]
  Map <- Map[!grepl("V", icd),]

  ## Merge with ICD codes for each person ##
  MEPS_medical <- merge(MEPS_medical, Map, by="icd")

  ## Map conditions to rounds ##
  if(typeof(MEPS_medical$crnd1)!="double"){
    MEPS_medical[,`:=` (condrn=as.numeric(substring(condrn, 1, 2)), crnd1=as.numeric(substring(crnd1, 1, 2)), crnd2=as.numeric(substring(crnd2, 1, 2)), crnd3=as.numeric(substring(crnd3, 1, 2)), crnd4=as.numeric(substring(crnd4, 1, 2)))]
  }
  MEPS_medical_round2 <- MEPS_medical[((crnd1 == 1 | condrn == 1) & Acute=="N") | (crnd2 == 1 | condrn == 2), ]
  MEPS_medical_round4 <- MEPS_medical[((crnd3 == 1 | condrn == 3) & Acute=="N") | (crnd4 == 1 | condrn == 4), ]
  MEPS_medical_round2[,round:=2]
  MEPS_medical_round4[,round:=4]
  MEPS_medical <- rbind(MEPS_medical_round2, MEPS_medical_round4) # Duplicates conditions experienced in both rounds
  MEPS_medical <- unique(MEPS_medical[,.(dupersid, cond_name, round)])

  ## Merge medical conditions onto individuals and SF-12 scores ##
  MEPS <- merge(MEPS, MEPS_medical, by=c("dupersid", "round")) # Observations not merged are observations of SF12 for a round where no conditions where present
  MEPS[, t:=1] # create dichotomous condition present yes/no value to be used when casting the dataset

  ## Cast across conditions ##
  MEPS <- dcast.data.table(MEPS, dupersid + round + panel + age + sex + region + datayear + education + personal_income + mcs + pcs ~ cond_name, value.var=c("t"), fill=0)

  ## Minor data mods ##
  MEPS[mcs<0, mcs:=NA]
  MEPS[pcs<0, pcs:=NA]
  MEPS[age<0, age:= NA]
  MEPS[, `:=` (id = paste0(dupersid, MEPS_panel), panel=MEPS_panel)]

  assign(paste0("Panel", MEPS_panel), MEPS)
  print(paste("PANEL", MEPS_panel, "OF 18 FINISHED"))
}

## Append all the panels. Change the count to be the minimum panel and update the c()
Appended_MEPS <- rbind(Panel4, Panel5, fill=T)
for(p in c(6:18)){
  Appended_MEPS <- rbind(Appended_MEPS, get(paste0("Panel", p)), fill=T)
}

## data from panel 4, 5, and the first half of panel 6 are sf-12v1, MEPS documentation suggests the following adjustment:
## source: http://meps.ahrq.gov/mepsweb/data_stats/download_data/pufs/h79/h79doc.pdf (page C-63)

Appended_MEPS[panel==4 | panel==5 | (panel==6 & round ==2), pcs:= as.numeric(pcs) + 1.07897]
Appended_MEPS[panel==4 | panel==5 | (panel==6 & round ==2), mcs:= as.numeric(mcs) - 0.16934]
Appended_MEPS[age<15, pcs:=NA] # participants under 15 should not have these scores and are data errors
Appended_MEPS[age<15, mcs:=NA] # participants under 15 should not have these scores and are data errors

## Recode missing conditions to 0 ##
Appended_MEPS[is.na(age), age:=-999] # so it isn't set to 0 below
Appended_MEPS[is.na(mcs), mcs:=-999] # so it isn't set to 0 below
Appended_MEPS[is.na(pcs), pcs:=-999] # so it isn't set to 0 below
Appended_MEPS[is.na(region), region:=-999] # so it isn't set to 0 below
Appended_MEPS[is.na(education), education:=-999] # so it isn't set to 0 below
Appended_MEPS[is.na(personal_income), personal_income:=-999] # so it isn't set to 0 below
Appended_MEPS[is.na(sex), sex:=-999] # so it isn't set to 0 below

Appended_MEPS[is.na(Appended_MEPS)] <- 0
Appended_MEPS[age<0, age:=NA] # switch back to NA
Appended_MEPS[mcs<0, mcs:=NA] # switch back to NA
Appended_MEPS[pcs<0, pcs:=NA] # switch back to NA
Appended_MEPS[region<0, region:=NA] # switch back to NA
Appended_MEPS[education<0, education:=NA] # switch back to NA
Appended_MEPS[personal_income<0, personal_income:=NA] # switch back to NA
Appended_MEPS[sex<0, sex:=NA] # switch back to NA

## Calculate SF-12 total score ##
Appended_MEPS[, `:=` (sf= pcs + mcs, predict = pcs + mcs)]

## Add respective disability weights calculated in step 1 for each SF 12 score ##
DW_mapping <- fread("FILEPATH.csv")

Appended_MEPS <- rbind(Appended_MEPS,DW_mapping, fill=T)
Appended_MEPS <- Appended_MEPS[order(id, dupersid, panel, round, age, sex, region, education, personal_income, pcs, mcs, sf, predict, dw),]
Appended_MEPS[, key:=seq_len(.N)]

### Fix cause names not corrected in map file
setnames(Appended_MEPS, "tAngina", "tcvd_ihd")
setnames(Appended_MEPS, "tOther gynecological", "tgyne_other")
setnames(Appended_MEPS, "tsense_other_chron", "tsense_other")
setnames(Appended_MEPS, "tUpper respiratory infections", "tinf_uri")
setnames(Appended_MEPS, "tUterine fibroids", "tgyne_fibroids")

### Write files
write.csv(Appended_MEPS, file=(paste0("FILEPATH.csv")), row.names=FALSE)
key_only <- Appended_MEPS[, .(sf, dw, key, predict)]
write.csv(key_only, file=(paste0("FILEPATH.csv")), row.names=FALSE)



