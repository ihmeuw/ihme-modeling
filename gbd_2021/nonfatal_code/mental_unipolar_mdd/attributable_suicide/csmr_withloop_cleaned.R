library(data.table)   
library(plyr)         
location_list <- read.csv("FILEPATH/Location_list_GBD2015.csv")  

######## Create dataframe with suicide death rates #######
for(year in c(1990, 1995, 2000, 2005, 2010, 2015)){
suicide_deaths<-read.csv(file=paste0("FILEPATH/suicide_deaths_", year, ".csv"), header = TRUE)
suicide_death_rates <- suicide_deaths
suicide_death_rates[,9:1008] <- suicide_deaths[,9:1008]/suicide_deaths[,8]
######## Create dataframe with PAFS #######

filelist <- list.files(path=paste0("FILEPATH/", year), pattern = "csv", full.names = TRUE)
datalist <- lapply(filelist, function(x){
  read.csv(file=x, header = TRUE)
})

pafs_initial <- Reduce(function(x, y){rbind(x, y)}, datalist)

pafs <- subset(pafs_initial, pafs_initial$age_group_id>6)

####### Calculate attributable ASR suicide deaths due to mdd ########

#for(location_id in location_list){
suicides_data.table <- data.table(suicide_death_rates)
pafs_data.table <- data.table(pafs)

suicides_melt <- melt.data.table(suicides_data.table, id.vars = names(suicides_data.table)[!(names(suicides_data.table) %like% "draw")], value.name="deaths", variable.name="draws")
pafs_melt <- melt.data.table(pafs_data.table, id.vars = names(pafs_data.table)[!(names(pafs_data.table) %like% "paf")], value.name="pafs", variable.name="draws")
pafs_melt[] <- lapply(pafs_melt, gsub, pattern = "paf", replacement = "draw", fixed = TRUE)
combined_dataset <- join(suicides_melt, pafs_melt, by = c("location_id", "age_group_id", "year_id", "sex_id", "draws"), match="all")
combined_dataset$pafs<-as.numeric(combined_dataset$pafs)
final_dataset <- combined_dataset[,.(mean_value=mean(pafs*deaths), lower_value=quantile((pafs*deaths), 0.025), upper_value=quantile((pafs*deaths), 0.975)), by = .(location_id, year_id, age_group_id, sex_id)]

csmr_dataset<-final_dataset[,.(seq="", seq_parent="", input_type="", modelable_entity_id=1981, modelable_entity_name="Major depressive disorder", 
                                underlying_nid="", nid="", underlying_field_citation_value="", field_citation_value="", file_path="", page_num="", table_num="", source_type="Mixed or estimation", location_name="", location_id, ihme_loc_id="", smaller_site_unit=0, site_memo="", sex=ifelse(sex_id==1, "Male", "Female"), sex_issue=0, year_start=year_id, year_end=year_id, year_issue=0,
                                age_start = ifelse(age_group_id==7, 10, ifelse(age_group_id==8, 15, ifelse(age_group_id==9, 20, ifelse(age_group_id==10, 25, ifelse(age_group_id==11, 30, ifelse(age_group_id==12, 35, ifelse(age_group_id==13, 40, ifelse(age_group_id==14, 45, ifelse(age_group_id==15, 50, ifelse(age_group_id==16, 55, ifelse(age_group_id==17, 60, ifelse(age_group_id==18, 65, ifelse(age_group_id==19, 70, ifelse(age_group_id==20, 75, 80)))))))))))))),
                                age_end = ifelse(age_group_id==7, 15, ifelse(age_group_id==8, 20, ifelse(age_group_id==9, 25, ifelse(age_group_id==10, 30, ifelse(age_group_id==11, 35, ifelse(age_group_id==12, 40, ifelse(age_group_id==13, 45, ifelse(age_group_id==14, 50, ifelse(age_group_id==15, 55, ifelse(age_group_id==16, 60, ifelse(age_group_id==17, 65, ifelse(age_group_id==18, 70, ifelse(age_group_id==19, 75, ifelse(age_group_id==20, 80, 99)))))))))))))),
                                age_issue=0, age_demographer=1, measure="mtspecific", mean=mean_value, lower=lower_value, upper=upper_value, standard_error="", effective_sample_size="", cases="", sample_size="", design_effect="", unit_type="Person", unit_value_as_published=1, measure_issue=0, measure_adjustment=0, uncertainty_type="", uncertainty_type_value=95, representative_name="Nationally and subnationally representative", 
                                urbanicity_type="Mixed/both", recall_type="Point", recall_type_value="", sampling_type="Probability", response_rate="", case_name="self-harm deaths attributable to major depressive disorder", case_definition="self-harm deaths attributable to major depressive disorder", case_diagnostics="", group="", specificity="age,sex,location,year", note_modeler="", note_SR="", extractor="", is_outlier="0",data_sheet_filepath="")]

write.csv(csmr_dataset, file=paste0("FILEPATH/mdd_attr_suicides_csmr_",year,".csv"), row.names=FALSE) 
}

