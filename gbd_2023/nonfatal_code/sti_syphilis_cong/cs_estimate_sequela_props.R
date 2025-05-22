################################################################################################
### Estimate CS Sequela
#################################################################################################

#SETUP
date <- gsub("-","_",Sys.Date())

#SOURCE THINGS
library(data.table)
library(openxlsx)
library(metafor)
library(arm) #package with logit
library(crosswalk, lib.loc = "FILEPATH")

#ARGS & DIRS
sequela_fpath <- "FILEPATH"
sequela_fpath
sequela_file <- data.table(read.xlsx(xlsxFile = sequela_fpath, sheet = "early_sequela"))

names(sequela_file)
unique(sequela_file$Final.Determination)

#CALCULATE STANDARD ERROR
early_sequela <- c("Moderate Infection", "Slight Disfigurement") #"Motor & Cognitive Impairment"
sequela_file <- sequela_file[Final.Determination %in% early_sequela & Mean > 0]
sequela_file[ ,standard_error := sqrt((Mean*(1-Mean))/(Sample.Size))]

#LOGIT TRANSFORM MEAN & STANDARD ERROR
logit_data <- data.table(delta_transform(mean = sequela_file$Mean, sd = sequela_file$standard_error, transformation = "linear_to_logit"))
head(logit_data)
dim(logit_data)

full_dt <- cbind(sequela_file, logit_data)
head(full_dt)

#GET DATA FOR EACH SEQUELA
mod_inf_dt <- full_dt[Final.Determination == "Moderate Infection" & Mean > 0]
slight_dis_dt <- full_dt[Final.Determination == "Slight Disfigurement" & Mean > 0]
#motor_cog_dt <- full_dt[Final.Determination == "Motor & Cognitive Impairment" & Mean > 0]

#EARLY SEQUELA-----------------------------------------------------------------------------------------------------------
#MODERATE INFECTION
mod_inf_dt[ ,dat_num := 1:nrow(mod_inf_dt)]
mod_inf_dt[ ,study_name := paste0(Author, "_", Year)]

mod_inf_rma <- rma(yi = Mean, sei = standard_error, measure = "GEN", weighted = TRUE, data = mod_inf_dt[Final.Analysis == "Selected"])

#SLIGHT DISFIGUREMENT
slight_dis_dt[ ,dat_num := 1:nrow(slight_dis_dt)]
slight_dis_dt[ ,study_name := paste0(Author, "_", Year)]
slight_dis_dt <- sort(slight_dis_dt, by = "Year")

slight_dis_rma <- rma(yi = Mean, sei = standard_error, measure = "GEN", weighted = TRUE, data = slight_dis_dt[Final.Analysis == "Selected"])


#PLOT EARLY
pdf(file = "FILEPATH")

forest.rma(x = mod_inf_rma, showweights = TRUE, slab = mod_inf_dt[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect")
forest.rma(x = mod_inf_rma, showweights = TRUE, slab = mod_inf_dt[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect", title("Moderate Infection in Infants with Early Congenital Syphilis"))

forest.rma(x = slight_dis_rma, showweights = TRUE, slab = slight_dis_dt[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect")
forest.rma(x = slight_dis_rma, showweights = TRUE, slab = slight_dis_dt[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect", title("Slight Disfigurement in Infants with Early Congenital Syphilis"))

dev.off()


#LATE SEQUELA-------------------------------------------------------------------------------------------------------------------
late_sequela_file <- data.table(read.xlsx(xlsxFile = sequela_fpath, sheet = "late_sequela"))
late_sequela_file[, standard_error := sqrt((Mean*(1-Mean))/(Sample.Size))]

names(late_sequela_file)
unique(late_sequela_file$Final.Determination)

#LOGIT TRANSFORM MEAN & STANDARD ERROR
logit_late <- data.table(delta_transform(mean = late_sequela_file$Mean, sd = late_sequela_file$standard_error, transformation = "linear_to_logit"))
head(logit_data)
dim(logit_data)

late_full <- cbind(late_sequela_file, logit_late)
head(late_full)


motor_cog_late <- late_full[Final.Determination %in% c("Motor & Cognitive Impairment") & Mean > 0]
slight_dis_late <- late_full[Final.Determination == "Slight Disfigurement " & Mean > 0]
uni_hl_late <- late_full[Final.Determination == "Unilateral Hearing Loss" & Mean > 0]
mc_vision <- late_full[Final.Determination ==  c("Vision Loss") & Mean > 0]

#LATE MOTOR COG
motor_cog_late[ ,dat_num := 1:nrow(motor_cog_late)]
motor_cog_late[ ,study_name := paste0(Author, "_", Year)]

motor_cog_late_rma <- rma(yi = Mean, sei = standard_error, measure = "GEN", weighted = TRUE, data = motor_cog_late[Final.Analysis == "Selected"])

#LATE SLIGHT DISFIGUREMENT
slight_dis_late[ ,dat_num := 1:nrow(slight_dis_late)]
slight_dis_late[ ,study_name := paste0(Author, "_", Year)]

slight_dis_late_rma <- rma(yi = Mean, sei = standard_error, measure = "GEN", weighted = TRUE, data = slight_dis_late[Final.Analysis == "Selected"])

#LATE UNILATERAL HL
uni_hl_late[ ,dat_num := 1:nrow(uni_hl_late)]
uni_hl_late[ ,study_name := paste0(Author, "_", Year)]

uni_hl_late_rma <- rma(yi = Mean, sei = standard_error, measure = "GEN", weighted = TRUE, data = uni_hl_late[Final.Analysis == "Selected"])

#VISION
mc_vision[ ,dat_num := 1:nrow(mc_vision)]
mc_vision[ ,study_name := paste0(Author, "_", Year)]

mc_vision_rma <- rma(yi = Mean, sei = standard_error, measure = "GEN", weighted = TRUE, data = mc_vision[Final.Analysis == "Selected"])

#vision

#PLOT LATE SEQUELA
pdf(file = "FILEPATH")

#MC impairment
forest.rma(x = motor_cog_late_rma, showweights = TRUE, slab = motor_cog_late[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect")
forest.rma(x = motor_cog_late_rma, showweights = TRUE, slab =motor_cog_late[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect", title("Motor & Cognitive Impairment in Persons with Late Congenital Syphilis"))

#slight dis
forest.rma(x = slight_dis_late_rma, showweights = TRUE, slab = slight_dis_late[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect")
forest.rma(x = slight_dis_late_rma, showweights = TRUE, slab =slight_dis_late[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect", title("Slight Disfigurement in Persons with Late Congenital Syphilis"))

#uni hl
forest.rma(x = uni_hl_late_rma, showweights = TRUE, slab = uni_hl_late[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect")
forest.rma(x = uni_hl_late_rma, showweights = TRUE, slab =uni_hl_late[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect", title("Unilateral Hearing Loss in Persons with Late Congenital Syphilis"))

#interstitial keratitis
forest.rma(x = mc_vision_rma, showweights = TRUE, slab = mc_vision[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect")
forest.rma(x = mc_vision_rma, showweights = TRUE, slab =mc_vision[Final.Analysis == "Selected", study_name],
           mlab = "Pooled Effect", title("Interstitial Keratitis in Persons with Late Congenital Syphilis"))

dev.off()


