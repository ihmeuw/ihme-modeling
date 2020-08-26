#GERD severity GBD 2019
#NAME


rm(list=ls())

##Working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
} else {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
}

my.lib <- paste0(h, "R/")
central_fxn <- paste0(j, "FILEPATH_CENTRAL_FXNS")

##Libraries
pacman::p_load(data.table, plyr, openxlsx, readxl, metafor)

input_path <- paste0(j, "FILEPATH/Severity_Extraction.xlsx")
output_path <- paste0(j, "FILEPATH")
functions_dir <- paste0(j, "FILEPATH_LIB_GBD2017")

date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

#Read in proportion data from mapped severity extraction
severe_data <- as.data.table(read_excel(input_path,range = "Mapped_GBD2019!A1:G5"))
#Calculate standard error
severe_data <- severe_data[ , error:= sqrt(prop_svs*(1-prop_svs)/total_cases)]

#Run metafor on severe-very severe proportion, with forest plot
meta_severe <- rma(yi = severe_data$prop_svs, sei = severe_data$error)

pdf(paste0(output_path, "gerd_svs_forplot_", date, ".pdf"))
forest(meta_severe, slab = severe_data$nid, showweights = T, xlab = "Severe or Very Severe")
dev.off()

print(meta_severe)
#estimate      se    zval    pval   ci.lb   ci.ub 
#0.1848  0.0315  5.8662  <.0001  0.1231  0.2466  *** 

#Predict 1000 draws
severe_prop_draws <- rnorm(1000, coef(meta_severe), 0.0315)

#Create new set of draws by subtracting all draws from 1
mild_prop_draws <- 1 - severe_prop_draws
mild_prop_draws_ordered <- sort(mild_prop_draws)
mean_prop_mm <- mean(mild_prop_draws)
# > mean(mild_prop_draws)
# [1] 0.8163568
lower_prop_mm <- mild_prop_draws_ordered[25]
# > print(lower_prop_mm)
# [1] 0.7517221
upper_prop_mm <- mild_prop_draws_ordered[975]
# > print(upper_prop_mm)
# [1] 0.8776092

#Output both means, uppers and lowers