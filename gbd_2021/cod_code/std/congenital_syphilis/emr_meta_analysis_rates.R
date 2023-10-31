################################################################################################
### Purpose: Calculate pooled excess fetal & neonatal mortality proportion from meta-analysis.
#################################################################################################
#setup
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
date <- gsub("-","_",Sys.Date())

j_work <- "FILEPATH"
xwalk_temp <- "FILEPATH"
risk_file <- "FILEPATH"

library(metafor)
library(openxlsx)
library(data.table)

#'Untreated Fetal Loss
#'PREP DATASET
untr_fetal <- data.table(read.xlsx(xlsxFile = risk_file, sheet = "untreated_fetal"))
untr_fetal[ ,prop := excess_deaths/sample_size]
untr_fetal[ ,se := sqrt((prop*(1-prop))/sample_size)]
untr_fetal[prop < 0, se := sqrt((-1*prop*(1--1*prop))/sample_size)] #this allows you to calculate SE even for "negative" AKA protective proportions
untr_fetal <- setorder(untr_fetal, by = year)

#RUN REGRESSION AND PLOT IN BASE R 
untr_fetal_rma <- rma(yi = prop, sei = se , measure = "GEN", weighted = TRUE, data = untr_fetal)
forest.rma(x = untr_fetal_rma, showweights = FALSE, slab = paste(untr_fetal$author, untr_fetal$year, untr_fetal$location, sep = " | "), 
           mlab = "Pooled Effect", header = T, top = 2, refline = untr_fetal_rma$beta)
forest.rma(x = untr_fetal_rma, showweights = FALSE, slab = paste0(untr_fetal$author, untr_fetal$year, untr_fetal$location), 
           mlab = "Pooled Effect", title("Excess Risk of Stillbirth among Pregnancies Untreated for Syphilis"))


#'Inadequately Treated Fetal Loss
inadq_fetal <- data.table(read.xlsx(xlsxFile = risk_file, sheet = "inadequate_fetal"))
inadq_fetal <- inadq_fetal[!is.na(sample_size)]
inadq_fetal[ ,prop := excess_deaths/sample_size]
inadq_fetal[ ,se := sqrt((prop*(1-prop))/sample_size)]
inadq_fetal[prop < 0, se := sqrt((-1*prop*(1--1*prop))/sample_size)] #this allows you to calculate SE even for "negative" proportions
inadq_fetal <- setorder(inadq_fetal, by = year)

inadq_fetal_rma <- rma(yi = prop, sei = se , measure = "GEN", weighted = TRUE, data = inadq_fetal)

forest.rma(x = inadq_fetal_rma, showweights = FALSE, slab = paste(inadq_fetal$author, inadq_fetal$year, inadq_fetal$location, sep = " | "), 
           mlab = "Pooled Effect", header = T, top = 2, refline = inadq_fetal_rma$beta)
forest.rma(x = inadq_fetal_rma, showweights = FALSE, slab = paste(inadq_fetal$author, inadq_fetal$year, inadq_fetal$location, sep = " | "), 
           mlab = "Pooled Effect", title("Excess Risk of Stillbirth among Pregnancies Inadequately Treated for Syphilis"))

#'Untreated Neonatal Deaths
untr_neo <- data.table(read.xlsx(xlsxFile = risk_file, sheet = "untreated_neo"))
untr_neo <- untr_neo[!is.na(sample_size)]
untr_neo[ ,prop := excess_deaths/sample_size]
untr_neo[ ,se := sqrt((prop*(1-prop))/sample_size)]
untr_neo[prop < 0, se := sqrt((-1*prop*(1--1*prop))/sample_size)] #this allows you to calculate SE even for "negative" proportions
untr_neo <- setorder(untr_neo, by = year)

untr_neo_rma <- rma(yi = prop, sei = se , measure = "GEN", weighted = TRUE, data = untr_neo)

forest.rma(x = untr_neo_rma, showweights = FALSE, slab = paste(untr_neo$author, untr_neo$year, untr_neo$location, sep = " | "), 
           mlab = "Pooled Effect", header = T, top = 2, refline = untr_neo_rma$beta)
forest.rma(x = untr_neo_rma, showweights = TRUE, slab = untr_neo$study_name, 
           mlab = "Pooled Effect", title("Excess Risk of Neonatal Death among Pregnancies Untreated for Syphilis"))

#'Inadequately Treated Neonatal Deaths 
inadq_neo <- data.table(read.xlsx(xlsxFile = risk_file, sheet = "inadequate_neo"))
inadq_neo <- inadq_neo[!is.na(sample_size)]
inadq_neo[ ,prop := excess_deaths/sample_size]
inadq_neo[ ,se := sqrt((prop*(1-prop))/sample_size)]
inadq_neo[prop < 0, se := sqrt((-1*prop*(1--1*prop))/sample_size)] #this allows you to calculate SE even for "negative" proportions
inadq_neo <- setorder(inadq_neo, by = year)

inadq_neo_rma <- rma(yi = prop, sei = se , measure = "GEN", weighted = TRUE, data = inadq_neo)

forest.rma(x = inadq_neo_rma, showweights = FALSE, slab = paste(inadq_neo$author, inadq_neo$year, inadq_neo$location, sep = " | "), 
           mlab = "Pooled Effect", header = T, top = 2, refline = inadq_neo_rma$beta)
forest.rma(x = inadq_neo_rma, showweights = FALSE, slab = paste(inadq_neo$author, inadq_neo$year, inadq_neo$location, " | "), 
           mlab = "Pooled Effect", title("Excess Risk of Neonatal Death among Pregnancies Inadequately Treated for Syphilis"))
