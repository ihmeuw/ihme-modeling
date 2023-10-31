rm(list = ls())

# cleaning of data for diagnostics, year before any modeling steps for all species
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")


source("FILEPATH/get_bundle_version.R")

bundle_version_id <- ADDRESS

# Standard
bundle_version_df <- get_bundle_version(bundle_version_id,fetch="all")

summary(bundle_version_df$unit_value_as_published)

all_data<-bundle_version_df

summary(all_data$unit_value_as_published)

#dropping those outliers for which locations were wrongly tagged
all_data<- subset(all_data, all_data$is_outlier!=1)


#cleaning-drop missing case detail, sample_size
all_data<- subset(all_data, !is.na(cases))
all_data<- subset(all_data, !is.na(sample_size))
all_data<- subset(all_data, sample_size!=0)

#drop any records where sample_size = cases
all_data<- subset(all_data, sample_size!=cases)


#recalculating mean and standard error

all_data$mean<- all_data$cases/all_data$sample_size
all_data$standard_error<- sqrt(((all_data$mean*(1-all_data$mean))/all_data$sample_size))

all_data<- as.data.table(all_data)

#cleaning
all_data$mean[all_data$cases==0] <- 0
all_data[, age_start := round(age_start)]
all_data[, age_end := round(age_end)]

all_data$year_start[all_data$year_start==1889] <- 1989
all_data$year_end[all_data$year_end==1889] <- 1989


#drop any missing case_diagnostics - duplicate values will be eliminated
all_data<-subset(all_data,case_diagnostics!="")

year_check<-subset(all_data,year_start==0)

all_data<- subset(all_data, year_start!=0)

#check case name
table(all_data$case_name)
all_data$case_name[all_data$case_name=="S guineesis"] <- "S guineensis"

#Correcting case name ESPEN 2015 data- assumption of case name by case_diagnostics
all_data[case_diagnostics == "Kato-Katz" & location_name == "Togo" & year_start ==2015, case_name := "S mansoni"]
all_data[case_diagnostics == "Urine Filtration" & location_name == "Togo" & year_start ==2015, case_name := "S haematobium"]


#cannot use unkown species data for crosswalks 
all_data_1<- subset(all_data, case_name!="S unknown")


#######MANSONI###############
#model intercalatum and guineensis
dat_original_mansoni<- subset(all_data_1, case_name=="S mansoni" | case_name=="S intercalatum"| case_name=="S mekongi" |  case_name=="S guineensis")

table(dat_original_mansoni$case_diagnostics)


dat_original_mansoni$case_diagnostics <- as.character(dat_original_mansoni$case_diagnostics)
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz, FEC" | dat_original_mansoni$case_diagnostics=="Kato-Katz, IHA" | dat_original_mansoni$case_diagnostics=="Kato-Katz, sedimentation" | dat_original_mansoni$case_diagnostics=="Kato-Katz, sedimentation, serology" |  dat_original_mansoni$case_diagnostics=="Kato-Katz" | dat_original_mansoni$case_diagnostics=="Direct smear" | dat_original_mansoni$case_diagnostics=="Katoâ\u0080\u0093Katz, sedimentation" | dat_original_mansoni$case_diagnostics=="Two to three stool samples were collected on consecutive\r\ndays around the time of blood sampling, and examined\r\nquantitatively for intestinal helminths and schistosome\r\neggs using the World Health Organization modification of\r\nthe Kato â\u0080\u0093 Katz method (World"] <- "Kato-Katz"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="sedimentation" | dat_original_mansoni$case_diagnostics=="saline sedimentation technique for protozoan and intestinal parasites; digestion technique to determine the intensity of infection" | dat_original_mansoni$case_diagnostics=="sedimentation, FEC"] <- "sed"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="fecal smear, formol ether" | dat_original_mansoni$case_diagnostics=="fecal smear" | dat_original_mansoni$case_diagnostics=="Faecal smear"] <- "Kato-Katz"

dat_original_mansoni<- subset(dat_original_mansoni, dat_original_mansoni$case_diagnostics != "centrifugation" & dat_original_mansoni$case_diagnostics != "FLOTAC" & dat_original_mansoni$case_diagnostics != "gold" & dat_original_mansoni$case_diagnostics!="MFIC" & dat_original_mansoni$case_diagnostics != "" & dat_original_mansoni$case_diagnostics != "CCA or Kato-Katz" & dat_original_mansoni$case_diagnostics != "Hoffman" & dat_original_mansoni$case_diagnostics != "IGM-IFAT" & dat_original_mansoni$case_diagnostics != "IHAT" & dat_original_mansoni$case_diagnostics != "KOH" & dat_original_mansoni$case_diagnostics != "microscopy" & dat_original_mansoni$case_diagnostics!= "MIFC" & dat_original_mansoni$case_diagnostics!= "Not provided" & dat_original_mansoni$case_diagnostics != "Other microscopy"  & dat_original_mansoni$case_diagnostics != "Other molecular" & dat_original_mansoni$case_diagnostics != "Other molecular" & dat_original_mansoni$case_diagnostics != "Unknown" & dat_original_mansoni$case_diagnostics != "NA" & dat_original_mansoni$case_diagnostics != "Two to three stool samples were collected on consecutive\r\ndays around the time of blood sampling, and examined\r\nquantitatively for intestinal helminths and schistosome\r\neggs using the World Health Organization modification of\r\nthe Kato Ã¢Â€Â“ Katz method (World" & dat_original_mansoni$case_diagnostics != "KatoÃ¢Â€Â“Katz, sedimentation")


as.double(dat_original_mansoni$value_num_samples)

#renaming values under case_diagnostics

dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$value_num_samples==0] <- "kk1"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$value_num_samples==1] <- "kk1"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$value_num_samples==2] <- "kk2"

dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics == "Kato-Katz" & is.na(dat_original_mansoni$num_samples)] <- "kk1"
#
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$value_num_samples==3] <- "kk3"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$value_num_samples==4] <- "kk3"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz" & dat_original_mansoni$value_num_samples==5] <- "kk3"



dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="ELISA"] <- "elisa"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="PCR"] <- "pcr"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="CCA"] <- "cca"
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="FEC"] <- "fec"

#assume 1 sample for those rows missing num_samples
dat_original_mansoni$case_diagnostics[dat_original_mansoni$case_diagnostics=="Kato-Katz"] <- "kk1"

unique(dat_original_mansoni$case_diagnostics)

length(which(dat_original_mansoni$case_diagnostics == "kk1")) 
length(which(dat_original_mansoni$case_diagnostics == "kk2")) 
length(which(dat_original_mansoni$case_diagnostics == "kk3")) 
length(which(dat_original_mansoni$case_diagnostics == "cca")) 
length(which(dat_original_mansoni$case_diagnostics == "pcr")) 
length(which(dat_original_mansoni$case_diagnostics == "elisa")) 
length(which(dat_original_mansoni$case_diagnostics == "sed")) 
length(which(dat_original_mansoni$case_diagnostics == "fec")) 
length(which(dat_original_mansoni$case_diagnostics == "NA")) 
length(which(dat_original_mansoni$case_diagnostics == "")) 



###############HAEMATOBIUM#################

dat_original_hema<- subset(all_data_1, case_name=="S haematobium")

dat_original_hema$case_diagnostics <- as.character(dat_original_hema$case_diagnostics)

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="sedimentation" | dat_original_hema$case_diagnostics=="sedimentation of urine" |   dat_original_hema$case_diagnostics=="sedimentation, filtration" | dat_original_hema$case_diagnostics=="sedimentation, microscopy" | dat_original_hema$case_diagnostics=="Urine sedimentation" | dat_original_hema$case_diagnostics=="Urine Sedimentation"] <- "sed"

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "centrifugation"| dat_original_hema$case_diagnostics=="sedimentation, centrifugation" |  dat_original_hema$case_diagnostics=="sedimentation, centrifugation, filtration, microscopy" | dat_original_hema$case_diagnostics=="centrifugation, microscopy" | dat_original_hema$case_diagnostics=="centrifugation, sedimentation" | dat_original_hema$case_diagnostics=="Urine centrifugation"] <- "cen"

dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "urine filtration technique" | dat_original_hema$case_diagnostics=="urine filtration technique, microscopy" | dat_original_hema$case_diagnostics=="microscopy" | dat_original_hema$case_diagnostics=="Filtration" | dat_original_hema$case_diagnostics=="Urine Filtration" | dat_original_hema$case_diagnostics=="urine filtration technique, sedimentation" | dat_original_hema$case_diagnostics=="urinealysis, microscopy" | dat_original_hema$case_diagnostics=="Urine filtration"] <- "filt"
dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics== "haematuria" | dat_original_hema$case_diagnostics== "BIU" | dat_original_hema$case_diagnostics== "Blood in urine" | dat_original_hema$case_diagnostics== "Dip" | dat_original_hema$case_diagnostics== "Urine examination"] <- "dip"

dat_original_hema<- subset(dat_original_hema, dat_original_hema$case_diagnostics != "ELISA" & dat_original_hema$case_diagnostics != "fecal smear" & dat_original_hema$case_diagnostics != "IFAT" & dat_original_hema$case_diagnostics!="IFTB" & dat_original_hema$case_diagnostics != "Kato-Katz" & dat_original_hema$case_diagnostics != "NA" & dat_original_hema$case_diagnostics != "nucleopore filtration" & dat_original_hema$case_diagnostics != "urinealysis, microscopy" & dat_original_hema$case_diagnostics != "Not provided" &  dat_original_hema$case_diagnostics!="Urine Other" &  dat_original_hema$case_diagnostics!="Unknown" & dat_original_hema$case_diagnostics!="" & dat_original_hema$case_diagnostics != "NA")


dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="PCR"] <- "pcr"
dat_original_hema$case_diagnostics[dat_original_hema$case_diagnostics=="CCA"] <- "cca"

unique(dat_original_hema$case_diagnostics)

length(which(dat_original_hema$case_diagnostics == "sed")) 
length(which(dat_original_hema$case_diagnostics == "filt")) 
length(which(dat_original_hema$case_diagnostics == "dip")) 
length(which(dat_original_hema$case_diagnostics == "cca")) 
length(which(dat_original_hema$case_diagnostics == "pcr")) 
length(which(dat_original_hema$case_diagnostics == "cen")) 
length(which(dat_original_hema$case_diagnostics == "NA")) 
length(which(dat_original_hema$case_diagnostics == "")) 


#####JAPONICUM#######


dat_original_japon<- subset(all_data_1, case_name=="S japonicum")
 
dat_original_japon$case_diagnostics <- as.character(dat_original_japon$case_diagnostics)

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" | dat_original_japon$case_diagnostics=="fecal smear" | dat_original_japon$case_diagnostics=="sedimentation" | dat_original_japon$case_diagnostics=="Kato-Katz, fecal smear, sedimentation, nucleopore filtration"] <- "Kato-Katz"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="IHA"] <- "iha"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="nylon silk method"] <- "hatch"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="ELISA"] <- "elisa"

dat_original_japon<- subset(dat_original_japon, dat_original_japon$case_diagnostics != "NA" & dat_original_japon$case_diagnostics != "gold" & dat_original_japon$case_diagnostics != "" & dat_original_japon$case_diagnostics != "NA")


as.double(dat_original_japon$value_num_samples)

#renaming values under case_diagnostics
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$value_num_samples==0] <- "kk1"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$value_num_samples==1] <- "kk1"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$value_num_samples==2] <- "kk2"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$value_num_samples==3] <- "kk3"
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz" & dat_original_japon$value_num_samples==4] <- "kk3"

dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics == "Kato-Katz" & is.na(dat_original_japon$num_samples)] <- "kk1"

#assume  missing num samples to kk1
dat_original_japon$case_diagnostics[dat_original_japon$case_diagnostics=="Kato-Katz"] <- "kk1"

dat_original_japon$sex[dat_original_japon$nid==ADDRESS & dat_original_japon$sample_size==387] <- "Female"
dat_original_japon$sex[dat_original_japon$nid==ADDRESS & dat_original_japon$sample_size==720] <- "Female"
dat_original_japon$sex[dat_original_japon$nid==ADDRESS & dat_original_japon$sample_size==335] <- "Male"
dat_original_japon$sex[dat_original_japon$nid==ADDRESS & dat_original_japon$sample_size==475] <- "Male"


unique(dat_original_japon$case_diagnostics)

length(which(dat_original_japon$case_diagnostics == "kk1"))
length(which(dat_original_japon$case_diagnostics == "kk2"))
length(which(dat_original_japon$case_diagnostics == "kk3"))
length(which(dat_original_japon$case_diagnostics == "iha"))
length(which(dat_original_japon$case_diagnostics == "hatch"))
length(which(dat_original_japon$case_diagnostics == "elisa"))


dat_original_mansoni<- subset(dat_original_mansoni, dat_original_mansoni$case_diagnostics!="Two to three stool samples were collected on consecutive\r\r\ndays around the time of blood sampling, and examined\r\r\nquantitatively for intestinal helminths and schistosome\r\r\neggs using the World Health Organization modification of\r\r\nthe Kato Ã¢Â€Â“ Katz method (World")

all_data_1_cleaned <- rbind(dat_original_mansoni, dat_original_hema, dat_original_japon)

#no changes as a result of the following
all_data_1_cleaned<- subset(all_data_1_cleaned, all_data_1_cleaned$sample_size>all_data_1_cleaned$cases)
all_data_1_cleaned<- subset(all_data_1_cleaned, all_data_1_cleaned$cases>=0)
all_data_1_cleaned<- subset(all_data_1_cleaned, all_data_1_cleaned$mean<=1)



#only keeping prevalence
all_data_1_cleaned<- subset(all_data_1_cleaned, all_data_1_cleaned$measure != "proportion")


#correction to aggregate this data for Bahia-BRA subnational
all_data_1_cleaned[nid==ADDRESS & age_end==91, age_end := 101]
summary(all_data_1_cleaned$age_end)


#update with relevant file paths
openxlsx::write.xlsx(all_data_1_cleaned, sheetName = "extraction", file = '/FILEPATH')

write.csv(all_data_1_cleaned,"FILEPATH")

#########move to stata file step2_dedup_and_agg_data

