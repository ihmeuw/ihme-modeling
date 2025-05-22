################################################################################
## DESCRIPTION ##  Pull zinc RR data from extraction sheets to use for RR analysis
## INPUTS ##
## OUTPUTS ##
## AUTHOR ##   
## DATE ##    
################################################################################

save_data_dir <- "FILEPATH"

extraction_sheet1 <- as.data.table(read.xlsx("FILEPATH/cc_zinc_diarrhea_extraction.xlsx", sheet = "extraction"))
extraction_sheet2 <- as.data.table(read.xlsx("FILEPATH/cc_zinc_lri_extraction.xlsx", sheet = "extraction"))
extraction_sheet3 <- as.data.table(read.xlsx("FILEPATH/cc_zinc_growth_extraction.xlsx", sheet = "extraction"))

extraction_sheet <- rbind(extraction_sheet1, extraction_sheet2, extraction_sheet3, fill = T)
extraction_sheet[outcome=="Diarrheal diseases", outcome:="Diarrhea"]
extraction_sheet[outcome=="Lower respiratory infections", outcome:="LRTI"]
extracted_data <- extraction_sheet[!is.na(nid) & outcome %in% c("Diarrhea", "LRTI", "Weight-for-age", "Weight-for-height","Stunting","Height","Height-for-age", "Underweight", "Wasting", "Height")]

extracted_data[outcome_type=="Incidence", cv_incidence:=1]
extracted_data[outcome_type=="Mortality", cv_incidence:=0]
extracted_data <- extracted_data[!is.na(upper) & !is.na(lower)]   #drops two rows for zinc
save_vals <-  c("nid","outcome","effect_size","upper","lower", "cv_incidence","year_start_study","year_end_study", "field_citation_value", "rr_calculate", "rr_numerator", "rr_type", "ages_under_sixmonths")

extracted_data <- extracted_data[, save_vals, with=FALSE]
write.csv(extracted_data, paste0(save_data_dir,"/Zinc_CC_extracted.csv"), row.names = FALSE)


# for growth outcomes need to use other columns to get mean difference

for(out in c("LRTI", "Diarrhea")){
  
  print(out)
  
  data <- extracted_data[outcome == out]
  data[, effect_size:=as.numeric(effect_size)]
  data[, upper:=as.numeric(upper)]
  data[, lower:=as.numeric(lower)]
  data <- data[outcome==out]
  data$log_effect_size <- log(data$effect_size)*-1   #convert from protective to harmful
  #data$log_effect_size <- log(data$effect_size)*1   #keep in protective space
  data$log_se <- (log(data$upper)-log(data$lower))/3.92
  data$intercept <- 1
  #data$cov_incidence <- 0
  keep_vars <- c("nid","outcome","log_effect_size","log_se","intercept", "cv_incidence", "rr_calculate", "rr_type", "rr_numerator")
  
  data <- data[log_se!=Inf]   #just in case- this happens once in omega3-ihd
  data <- data[,keep_vars,with=FALSE]
  write.csv(data, file=paste0(save_data_dir,"/",out, "_data_file.csv"), row.names = FALSE)

}
