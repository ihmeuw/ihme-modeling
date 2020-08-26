#Code description: - Hydrocele all age input prep

#this file cleans sentinel site data to use in regression of LF prevalence to hydrocele

#WHO SS data is appended to hydrocele sci lit data, column names cleaned up and output into a file to 
#be age split

########CLEAN WHO DATA #######################
who_ss<-read.csv("FILENAME")

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

#pull Lymphedema data only 
who_ly<-who_ss[!is.na(who_ss$Lymph_tested),]
#Hydrocele non-missing


#create NID
who_ly$nid<-NID
who_ly$underlying_nid<-NID
who_ly$year_start<-who_ly$Survey_year
who_ly$year_end<-who_ly$Survey_year

who_ly$seq<-NA
who_ly$modelable_entity_id<-ADDRESS
who_ly$modelable_entity_name<-"Lymphedema due to lymphatic filariasis"
who_ly$measure<-"prevalence"
who_ly$group_review<-1
who_ly$is_outlier<-0
who_ly$specificity<-"total"
who_ly$sex<-"Both"
who_ly$crosswalk_parent_seq<-NA
who_ly$seq<-NA
who_ly$location_name<-who_ly$Country
who_ly$site_memo<-who_ly$Site
who_ly$seq<-NA
who_ly$ihme_loc_id<-NA
who_ly$sex_issue<-0
who_ly$year_issue<-0
who_ly$lower<-NA
who_ly$upper<-NA
who_ly$effective_sample_size<-NA
who_ly$standard_error<-NA

who_ly$mean<-who_ly$Lymph_num/who_ly$Lymph_tested
who_ly$sample_size<-who_ly$Lymph_tested
who_ly$cases<-who_ly$Lymph_num

#drop needless variables
drop <- c("IU","ID","Type","Population","Site","MDA_cov_rep","MDA_IU_rep","MDA_IU_obs","MDA_type","Lymph_tested",
          "Hydr_tested","Nydr_num","Hydr_prop",
          "Lymph_prop","Lymph_num","Country","MF_den","MF_com","Count","MDA_round_comp","MDA_fr_year","MDA_fr_date","Survey_year","Survey_date")
who_ly2 = who_ly[,!(names(who_ly) %in% drop)]



who_ly2$age_diff<-who_ly2$age_end-who_ly2$age_start

#append to literature data


#problem reading in file due to NID field
lit1<-read.csv("FILENAME")
lit1 <- mutate(lit1, seq = rownames(lit1))

#drop hydrocele
lit_ly<-lit1[lit1$modelable_entity_id==ADDRESS,]


#drop variables

drop2 <- c("location_name_2","mf_density_pos","location_name_2","mf_density_tot","density.issue","density.notes","design_effect",
            "uncertainty_type","uncertainty_type_value","case_name","case_definition","case_diagnostics","response_rate","unique","group","dominant.vector",
            "vector.Culex","vector.Aedes","vector.Mansonia","vector.Anopheles","extractor","Exclude","note_SR","filarial.species")
           
lit_ly2 = lit_ly[,!(names(lit_ly) %in% drop2)]
 
  
#append the datasets together

lymph_age_sex_split_input<-rbind(lit_ly2,who_ly2)

#need is_outlier set to zero for running split code
lymph_age_sex_split_input$is_outlier<-0

#output
write.csv(lymph_age_sex_split_input,"FILENAME")





