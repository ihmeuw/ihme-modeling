

library(data.table)
library(openxlsx)

save_location <- "FILEPATH.csv"

crosswalk_ratios <- fread("FILEPATH.csv")[!is.na(sex),1:6]
data <- as.data.table(read.xlsx(("FILEPATH.xlsx")))

data_group_reviewed <- data[group_review==0, ]
data_to_crosswalk <- data[group_review==1,]

note_modeler_variable <- "note_modeler"

data_to_crosswalk[, id:=seq_len(.N)]
data_to_crosswalk[is.na(standard_error) & !is.na(upper) & !is.na(lower), standard_error:=(upper-lower)/(qnorm(0.975, 0, 1)*2)]
data_to_crosswalk[is.na(standard_error) & is.na(upper) & is.na(lower) & !is.na(effective_sample_size), standard_error:=sqrt(1/effective_sample_size*mean*(1-mean+1/(4*effective_sample_size^2)*qnorm(0.975, 0, 1)^2))]
data_to_crosswalk[is.na(standard_error) & is.na(upper) & is.na(lower) & !is.na(sample_size), standard_error:=sqrt(1/sample_size*mean*(1-mean+1/(4*sample_size^2)*qnorm(0.975, 0, 1)^2))]
setnames(data_to_crosswalk, note_modeler_variable, "note_modeler_temp")

c_age_start<-unique(crosswalk_ratios$age_start)
c_age_end<-unique(crosswalk_ratios$age_end)

## Get closest crosswalk age_start and age_end points
for(i in data_to_crosswalk$id){
  ## In below code, if crosswalk_agestart values considered close to age_start, then selects the lowest 
  ## of the two options. For example age_start 50 is close to 45 and 55, but only 45 selected
  data_to_crosswalk[id==i, crosswalk_agestart := c_age_start[which(abs(c_age_start-age_start)==min(abs(c_age_start-age_start)))]]
  data_to_crosswalk[id==i, crosswalk_ageend := c_age_end[which(abs(c_age_end-age_end)==min(abs(c_age_end-age_end)))]]
  if(data_to_crosswalk[id==i, crosswalk_ageend] <= data_to_crosswalk[id==i, crosswalk_agestart]){
    data_to_crosswalk[id==i, crosswalk_ageend := c_age_end[which(abs(c_age_end-age_end)==min(abs(c_age_end-age_end)))+1]]
  }
}

## Apply crosswalk in new rows
crosswalked_data <- copy(data_to_crosswalk)
for(i in crosswalked_data$id){
  RR_m <- crosswalk_ratios[sex==crosswalked_data[id==i, sex] & age_start==crosswalked_data[id==i, crosswalk_agestart] & age_end==crosswalked_data[id==i, crosswalk_ageend], ratio]
  RR_l <- crosswalk_ratios[sex==crosswalked_data[id==i, sex] & age_start==crosswalked_data[id==i, crosswalk_agestart] & age_end==crosswalked_data[id==i, crosswalk_ageend], lower]
  RR_u <- crosswalk_ratios[sex==crosswalked_data[id==i, sex] & age_start==crosswalked_data[id==i, crosswalk_agestart] & age_end==crosswalked_data[id==i, crosswalk_ageend], upper]
  RR_se <- (RR_u-RR_l)/(qnorm(0.975, 0, 1)*2)
  crosswalked_data[id==i, `:=` (mean=mean*RR_m, standard_error=sqrt(RR_se^2*standard_error^2+standard_error^2*RR_m^2+mean^2*RR_se^2))]
}

## Finalise datafile
data_to_crosswalk[, `:=` (input_type="group_review", group_review=0, note_modeler_temp=paste0(note_modeler_temp, " GR 0 as 12 month to point prevalence crosswalk applied in separate rows."))]
crosswalked_data[, `:=` (cases=mean*sample_size, uncertainty_type="", recall_type="Point", recall_type_value="", note_modeler_temp=paste0(note_modeler_temp, " 12 month to point prevalence crosswalk applied."), lower="", upper="", uncertainty_type_value="Standard error")] 
final_dataset <- rbind(data_to_crosswalk, crosswalked_data)
final_dataset[, `:=` (id=NULL, crosswalk_agestart=NULL, crosswalk_ageend=NULL)]
setnames(final_dataset, "note_modeler_temp", note_modeler_variable)
final_dataset <- rbind(data_group_reviewed, final_dataset)

write.csv(final_dataset, save_location, row.names = F, na="")


