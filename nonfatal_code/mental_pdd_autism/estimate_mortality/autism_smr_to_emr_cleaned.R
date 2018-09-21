
source("FILEPATH/get_draws.R"))

location_list<-fread("FILEPATH/Location_list_GBD2016.csv"))

smr <- get_draws(gbd_id_field='modelable_entity_id', gbd_id=1995, source = 'epi', location_ids=1, year_ids=c(1990, 1995, 2000, 2005, 2010, 2016), sex_ids=c(1, 2), measure_ids=c(12), gbd_round_id=4)
print("Pulled global SMR estimates")

count = 1
for (location in unique(location_list$location_id)){
  smr_loc <- copy(smr)
  smr_loc <- smr_loc[,location_id:=location]

  acmr <- get_draws(gbd_id_field='modelable_entity_id', gbd_id=1995, source = 'epi', location_ids=location, year_ids=c(1990, 1995, 2000, 2005, 2010, 2016), sex_ids=c(1, 2), measure_ids=c(14), gbd_round_id=4)
  print(paste0("Pulled ACMR estimates for location ", count, " out of ", length(unique(location_list$location_id))))
  
  smr_melt <- melt.data.table(smr_loc, id.vars = names(smr_loc)[!(names(smr_loc) %like% "draw")], value.name="smr", variable.name="draws")
  acmr_melt <- melt.data.table(acmr, id.vars = names(acmr)[!(names(acmr) %like% "draw")], value.name="acmr", variable.name="draws")
  combined_dataset <- merge(smr_melt, acmr_melt, by = c("location_id", "age_group_id", "year_id", "sex_id", "modelable_entity_id", "model_version_id", "draws"))
  
  if(count==1){
    final_dataset <- combined_dataset[age_group_id>4,.(mean=mean((smr-1)*acmr), lower=quantile(((smr-1)*acmr), 0.025), upper=quantile(((smr-1)*acmr), 0.975)), by = .(location_id, year_id, age_group_id, sex_id)]
    print(paste0("Finished estimating EMR for location ", count, " out of ", length(unique(location_list$location_id))))
  } else {
    location_dataset <- combined_dataset[age_group_id>4,.(mean=mean((smr-1)*acmr), lower=quantile(((smr-1)*acmr), 0.025), upper=quantile(((smr-1)*acmr), 0.975)), by = .(location_id, year_id, age_group_id, sex_id)]
    final_dataset<-rbind(final_dataset, location_dataset)
    print(paste0("Finished estimating EMR for location ", count, " out of ", length(unique(location_list$location_id))))
  }
  count = count+1
}
 
csmr_dataset<-final_dataset[,.(seq="", seq_parent="", input_type="", modelable_entity_id=1995, modelable_entity_name="Autism", 
                               underlying_nid="", nid="", underlying_field_citation_value="", field_citation_value="", file_path="", page_num="", table_num="", source_type="Mixed or estimation", location_name="", location_id, ihme_loc_id="", smaller_site_unit=0, site_memo="", sex=ifelse(sex_id==1, "Male", "Female"), sex_issue=0, year_start=year_id, year_end=year_id, year_issue=0,
                               age_start = ifelse(age_group_id==5, 1, ifelse(age_group_id==6, 5, ifelse(age_group_id==7, 10, ifelse(age_group_id==8, 15, ifelse(age_group_id==9, 20, ifelse(age_group_id==10, 25, ifelse(age_group_id==11, 30, ifelse(age_group_id==12, 35, ifelse(age_group_id==13, 40, ifelse(age_group_id==14, 45, ifelse(age_group_id==15, 50, ifelse(age_group_id==16, 55, ifelse(age_group_id==17, 60, ifelse(age_group_id==18, 65, ifelse(age_group_id==19, 70, ifelse(age_group_id==20, 75, ifelse(age_group_id==30, 80, ifelse(age_group_id==31, 85, ifelse(age_group_id==32, 90, 95))))))))))))))))))),
                               age_end = ifelse(age_group_id==5, 4, ifelse(age_group_id==6, 9, ifelse(age_group_id==7, 14, ifelse(age_group_id==8, 19, ifelse(age_group_id==9, 24, ifelse(age_group_id==10, 29, ifelse(age_group_id==11, 34, ifelse(age_group_id==12, 39, ifelse(age_group_id==13, 44, ifelse(age_group_id==14, 49, ifelse(age_group_id==15, 54, ifelse(age_group_id==16, 59, ifelse(age_group_id==17, 64, ifelse(age_group_id==18, 69, ifelse(age_group_id==19, 74, ifelse(age_group_id==20, 79, ifelse(age_group_id==30, 84, ifelse(age_group_id==31, 89, ifelse(age_group_id==32, 94, 99))))))))))))))))))),
                               age_issue=0, age_demographer=1, measure="mtexcess", mean, lower, upper, standard_error="", effective_sample_size="", cases="", sample_size="", design_effect="", unit_type="Person*year", unit_value_as_published=1, measure_issue=0, measure_adjustment=0, uncertainty_type="", uncertainty_type_value=95, representative_name="Nationally and subnationally representative", 
                               urbanicity_type="Mixed/both", recall_type="Not Set", recall_type_value="", sampling_type="Probability", response_rate="", case_name="Excess mortality rate for autism", case_definition="Excess mortality rate for autism", case_diagnostics="", group="", specificity="age,sex,location,year", group_review=1, note_modeler="Estimated from global final SMR estimates and location-specific all-cause mortality rate estimates from DisMod", note_SR="", extractor="", is_outlier="0",data_sheet_filepath="", 
                               cv_icd=0,	cv_lay_interviewer=0,	cv_marketscan_all_2000=0,	cv_marketscan_all_2010=0,	cv_marketscan_all_2012=0,	cv_noncomp_casefind=0,	cv_old_dx_criteria=0)]

write.csv(csmr_dataset, file="FILEPATH/autism_emr_final.csv"), row.names=FALSE) 
