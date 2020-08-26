rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "USERNAME"
}

library('openxlsx')
library("stringr")
library("tidyverse")
#################

source("FILEPATH")



perio_data <- read.xlsx("FILEPATH")

perio_data$uncertainty_type[!is.na(perio_data$lower)] <- "Confidence interval"
perio_data$uncertainty_type_value[!is.na(perio_data$lower)] <- 95

perio_data$case_definition[nchar(perio_data$case_definition) >= 1800] <- substr(perio_data$case_definition, 1, 1600)
colnames(perio_data)[which(names(perio_data) == "note_SR")] <- "note_sr"



write.xlsx(perio_data, "FILEPATH", sheetName = "extraction")

perio_step4 <- get_bundle_data(262, 'step4')

perio_step4 <- perio_step4 %>%
  select(seq)

result <- upload_bundle_data(262, 'step4', "FILEPATH")



edent_data <- read.xlsx("FILEPATH")

edent_data$uncertainty_type[!is.na(edent_data$lower)] <- "Confidence interval"
edent_data$uncertainty_type_value[!is.na(edent_data$lower)] <- 95

edent_data$case_definition[is.na(edent_data$case_definition)] <- ''
edent_data$case_definition[nchar(edent_data$case_definition) >= 1800] <- substr(edent_data$case_definition, 1, 1600)
colnames(edent_data)[which(names(edent_data) == "note_SR")] <- "note_sr"



write.xlsx(edent_data, "FILEPATH", sheetName = "extraction")

edent_step4 <- get_bundle_data(263, 'step4')

edent_step4 <- edent_step4 %>%
  select(seq)

result <- upload_bundle_data(263, 'step4', "FILEPATH")


perm_data <- read.xlsx("FILEPATH")

#perm_data$mean <- as.numeric(perm_data$mean)
colnames(perm_data)[which(names(perm_data) == "cv_dmf_score")] <- "dmf_score"
colnames(perm_data)[which(names(perm_data) == "cv_dmf_score_decay")] <- "dmf_score_decay"
colnames(perm_data)[which(names(perm_data) == "cv_dmf_score_miss")] <- "dmf_score_score"
colnames(perm_data)[which(names(perm_data) == "cv_dmf_score_fill")] <- "dmf_score_fill"

perm_data$nid[perm_data$nid == 418664] <- 415864
perm_data$urbanicity_type[is.na(perm_data$urbanicity_type)] <- "Mixed/both"
colnames(perm_data)[which(names(perm_data) == "note_SR")] <- "note_sr"



write.xlsx(perm_data, "FILEPATH", sheetName = "extraction")

perm_step4 <- get_bundle_data(261, 'step4')

perm_step4 <- perm_step4 %>%
  select(seq)

result <- upload_bundle_data(261, 'step4', "FILEPATH")


decid_data <- read.xlsx("FILEPATH")

decid_data$mean <- as.numeric(decid_data$mean)
colnames(decid_data)[which(names(decid_data) == "cv_dmf_score")] <- "dmf_score"
colnames(decid_data)[which(names(decid_data) == "cv_dmf_score_decay")] <- "dmf_score_decay"
colnames(decid_data)[which(names(decid_data) == "cv_dmf_score_miss")] <- "dmf_score_miss"
colnames(decid_data)[which(names(decid_data) == "cv_dmf_score_fill")] <- "dmf_score_fill"




decid_data$urbanicity_type[is.na(decid_data$urbanicity_type)] <- "Mixed/both"
decid_data$age_end[decid_data$age_end < decid_data$age_start] <- 1.000
decid_data$group_review[decid_data$nid == 415859] <- NA
decid_data$group_review[decid_data$nid == 415855] <- NA

decid_data$recall_type[decid_data$recall_type == "lifetime"] <- "Lifetime"
colnames(decid_data)[which(names(decid_data) == "note_SR")] <- "note_sr"
#decid_data$note_sr[is.na(decid_data$note_sr)] <- ''



write.xlsx(decid_data, "FILEPATH", sheetName = "extraction")

decid_step4 <- get_bundle_data(260, 'step4')

decid_step4 <- decid_step4 %>%
  select(seq)

result <- upload_bundle_data(260, 'step4', "FILEPATH")

print(nchar(edent_data$case_definition))
print(nchar(perio_data$case_diagnostics))


decid_version_step2 <- save_bundle_version(260, 'step2')

decid_version_full <- get_bundle_version(12554)

decid_version_step4 <- save_bundle_version(260, 'step4')

perm_version_step4 <- save_bundle_version(261, 'step4')

perio_version_step4 <- save_bundle_version(262, 'step4')

edent_version_step4 <- save_bundle_version(263, 'step4')

decid_bundle_step2 <- get_bundle_data(260, 'step2', export = TRUE)

decid_bundle_step4 <- get_bundle_data(260, 'step4', TRUE)

print(setdiff(colnames(decid_version_full), colnames(perm_bundle_step2)))
print(setdiff(colnames(perm_bundle_step4), colnames(perm_bundle_step2)))

perm_bundle_step4$file_path <- ''
colnames(perm_bundle_step2)[which(names(perm_bundle_step2) == "note_sr")] <- "note_SR"
colnames(perm_bundle_step4)[which(names(perm_bundle_step4) == "dmf_score_score")] <- "dmf_score_miss"
perm_bundle_step4$note_modeler <- ''
perm_bundle_step4$seq_parent <- NA
perm_bundle_step4$cv_d_conversion <- 0
perm_bundle_step4$cv_diag_other <- 0
perm_bundle_step4$cv_dmf_incidence <- 0
perm_bundle_step4$dmf_score_missing <- NA

full_perm_bundle <- rbind(perm_bundle_step2, perm_bundle_step4)

write.xlsx(full_perm_bundle, "FILEPATH", sheetName = "extraction")

result <- upload_bundle_data(260, 'step4', "FILEPATH")

perm_version <- get_bundle_version(12590, TRUE)




