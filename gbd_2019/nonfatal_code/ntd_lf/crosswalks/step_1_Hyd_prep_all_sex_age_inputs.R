#Code description: - Hydrocele all age input prep

#this file cleans sentinel site data to use in regression of LF prevalence to hydrocele

#WHO SS data is appended to hydrocele sci lit data, column names cleaned up and output into a file to 
#be age split



########CLEAN WHO DATA #######################
who_ss<-read.csv("FILEPATH")

#rename columns for consistency:

names(who_ss)[14]<-"pop_mf"
names(who_ss)[15]<-"prev_mf"
names(who_ss)[16]<-"np_mf"

names(who_ss)[20]<-"pop_ict"
names(who_ss)[21]<-"prev_ict"
names(who_ss)[22]<-"np_ict"

#recode mf_prop and ict_prop

who_ss$prev_ict<-who_ss$np_ict/who_ss$pop_ict
who_ss$prev_mf<-who_ss$np_mf/who_ss$pop_mf


#age start and stop for sentinel site data
who_ss$age_start<-15
who_ss$age_end<-99

#sex
who_ss$sex<-"Both"

#pull hydrocele data only 
who_hy<-who_ss[!is.na(who_ss$Hydr_tested),]
#Hydrocele non-missing


#create NID
who_hy$nid<-NID
who_hy$underlying_nid<-NID
who_hy$year_start<-who_hy$Survey_year
who_hy$year_end<-who_hy$Survey_year

who_hy$seq<-NA
who_hy$modelable_entity_id<-ADDRESS
who_hy$modelable_entity_name<-"Hydrocele due to lymphatic filariasis"
who_hy$measure<-"prevalence"
who_hy$group_review<-1
who_hy$is_outlier<-0
who_hy$specificity<-"total"
who_hy$sex<-"Male"
who_hy$crosswalk_parent_seq<-NA
who_hy$seq<-NA
who_hy$location_name<-who_hy$Country
who_hy$site_memo<-who_hy$Site
who_hy$seq<-NA
who_hy$ihme_loc_id<-NA
who_hy$sex_issue<-0
who_hy$year_issue<-0
who_hy$lower<-NA
who_hy$upper<-NA
who_hy$effective_sample_size<-NA
who_hy$standard_error<-NA

who_hy$mean<-who_hy$Nydr_num/who_hy$Hydr_tested
who_hy$sample_size<-who_hy$Hydr_tested
who_hy$cases<-who_hy$Nydr_num

#drop needless variables
drop <- c("IU","ID","Type","Population","Site","MDA_cov_rep","MDA_IU_rep","MDA_IU_obs","MDA_type","Lymph_tested",
          "Hydr_tested","Nydr_num","Hydr_prop",
          "Lymph_prop","Lymph_num","Country","MF_den","MF_com","Count","MDA_round_comp","MDA_fr_year","MDA_fr_date","Survey_year","Survey_date")
who_hy2 = who_hy[,!(names(who_hy) %in% drop)]



who_hy2$age_diff<-who_hy2$age_end-who_hy2$age_start

#append to literature data

hy_lit<-read.csv("FILEPATH")

#drop variables

drop2 <- c("location_name_2","mf_density_pos","location_name_2","mf_density_tot","density.issue","density.notes","design_effect",
            "uncertainty_type","uncertainty_type_value","case_name","case_definition","case_diagnostics","response_rate","unique","group","dominant.vector",
            "vector.Culex","vector.Aedes","vector.Mansonia","vector.Anopheles","extractor","Exclude","note_SR","filarial.species")
           
hy_lit2 = hy_lit[,!(names(hy_lit) %in% drop2)]
  
  
#append the datasets together

hydro_age_split_input<-rbind(hy_lit2,who_hy2)

#output
write.csv(hydro_age_split_input,"FILEPATH")





