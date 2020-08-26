library(data.table)
library(openxlsx)

## Load data ##
data <- as.data.table(read.xlsx("FILEPATH.xlsx", sheet=1))
data[is.na(standard_error) & !is.na(upper) & !is.na(lower), standard_error:=(upper-lower)/(qnorm(0.975, 0, 1)*2)]
data[is.na(standard_error) & is.na(upper) & is.na(lower) & !is.na(effective_sample_size), standard_error:=sqrt(1/effective_sample_size*mean*(1-mean+1/(4*effective_sample_size^2)*qnorm(0.975, 0, 1)^2))]
data[is.na(standard_error) & is.na(upper) & is.na(lower) & !is.na(sample_size), standard_error:=sqrt(1/sample_size*mean*(1-mean+1/(4*sample_size^2)*qnorm(0.975, 0, 1)^2))]
data[, `:=` (split=0)]
data[is.na(cases), cases:=sample_size*mean]

## Age-split by location ##
for(loc in unique(data$location_id)){
  data_loc <- data[location_id==loc,]
  cases_male <- data_loc[sex=="Male", cases]/(data_loc[sex=="Male", cases]+data_loc[sex=="Female", cases])
  cases_female <- data_loc[sex=="Female", cases]/(data_loc[sex=="Male", cases]+data_loc[sex=="Female", cases])
  cases_male_se <- sqrt(cases_male*(1-cases_male)/(data_loc[sex=="Male", cases]+data_loc[sex=="Female", cases]))
  cases_female_se <- sqrt(cases_female*(1-cases_female)/(data_loc[sex=="Male", cases]+data_loc[sex=="Female", cases]))
  sample_male <- data_loc[sex=="Male", sample_size]/(data_loc[sex=="Male", sample_size]+data_loc[sex=="Female", sample_size])
  sample_female <- data_loc[sex=="Female", sample_size]/(data_loc[sex=="Male", sample_size]+data_loc[sex=="Female", sample_size])
  sample_male_se <- sqrt(sample_male*(1-sample_male)/(data_loc[sex=="Male", sample_size]+data_loc[sex=="Female", sample_size]))
  sample_female_se <- sqrt(sample_female*(1-sample_female)/(data_loc[sex=="Male", sample_size]+data_loc[sex=="Female", sample_size]))
  ratio_male <- cases_male/sample_male
  ratio_female <- cases_female/sample_female
  ratio_male_se <- sqrt((cases_male^2/sample_male^2)*(cases_male_se^2/cases_male^2 + sample_male_se^2/sample_male^2))
  ratio_female_se <- sqrt((cases_female^2/sample_female^2)*(cases_female_se^2/cases_female^2 + sample_female_se^2/sample_female^2))
  
  ## Once sex ratio and se are estimated, the 18-99 estimates are no longer required so these can be removed ##
  data_loc <- data_loc[age_end-age_start<(max(age_end)-min(age_start)),] 
  
  ## Create age-specific rows for males ##
  data_loc_males <- copy(data_loc)
  data_loc_males[,`:=`(mean=mean*ratio_male, standard_error=sqrt(standard_error^2*ratio_male_se^2 + standard_error^2*ratio_male^2 + ratio_male_se^2*mean^2),
               cases=cases_male*cases, sample_size=sample_male*sample_size, split=1, sex="Male")]
  data_loc_females <- copy(data_loc)
  data_loc_females[,`:=`(mean=mean*ratio_female, standard_error=sqrt(standard_error^2*ratio_female_se^2 + standard_error^2*ratio_female^2 + ratio_female_se^2*mean^2),
                       cases=cases_female*cases, sample_size=sample_female*sample_size, split=1, sex="Female")]
  data <- rbind(data, data_loc_males, data_loc_females)
  print(paste0("Finished splitting location ", loc))
}
print(paste0("Finished age-sex splitting"))

data[split==0, `:=` (unit_value_as_published=1, input_type="group_review", specificity="raw", group_review=0, note_modeler=paste0(note_modeler, "GR0 as age-sex split in separate rows."))]  
data[split==1, `:=` (unit_value_as_published=1, specificity="age,sex,location", group_review=1, note_modeler=paste0(note_modeler, "Age-sex split estimates."), extractor="", lower="", upper="", uncertainty_type="", uncertainty_type_value="")] 
data[, `:=` (split=NULL, bundle_id="", seq="", group=nid)]

write.csv(data, "FILEPATH.csv", row.names=F,na="")

