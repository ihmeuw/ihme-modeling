#rm(list=ls())

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

library(dplyr)


##############
# Dengue all age data crosswalking
################
dengue_bundle<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'iterative')



#targetting those rows where specificity is missing 

data<- subset(dengue_bundle, specificity=="")

data$sex_id<- 0

data$sex_id[data$sex=="Both"] <- 3
data$sex_id[data$sex=="Female"] <- 2
data$sex_id[data$sex=="Male"] <- 1

data$unique_id <- 1:nrow(data)

# drop 1

#observing overall duplicates based on nid, location_id, sex, year_start, year_end, age_start, age_end, cases, sample_size, measure 
data_1<- data %>% distinct(nid, location_id, sex_id, year_start, year_end, age_start, age_end, cases, sample_size, measure, .keep_all = TRUE)
dropped_1 <- data[!(data$unique_id %in% data_1$unique_id),]


#drop 2
#making sure that there is only one source per demographic cohort (nid not taken into account)
data_2<- data_1 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, cases, sample_size, .keep_all = TRUE)
dropped_2 <- data_1[!(data_1$unique_id %in% data_2$unique_id),]

# drop 3
#checking if there is a difference by sample size
data_3<- data_2 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, cases, .keep_all = TRUE)
dropped_3 <- data_2[!(data_2$unique_id %in% data_3$unique_id),]


loc_years_to_keep<- data_3[data_3$location_id==6 | data_3$location_id== 162 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 162 & data_3$year_start==2017 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 4844 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 4855 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 4860 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99]

#drop the above loc year from the data_3 as we will append these later
data_3_1<- subset(data_3, !(data_3$location_id==6 | data_3$location_id== 162 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 162 & data_3$year_start==2017 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 4844 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 4855 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99 | data_3$location_id== 4860 & data_3$year_start==2018 & data_3$age_start==0 & data_3$age_end==99))

#drop single rows of data so that these dont get dropped and the correct one gets picked (since distinct selects the first occurence of the duplicate, for some locs-years I wanted the second occurence to be picked. So I am just deleting the first occurence since that is wrong, that way there is just one single occurence and that will show up in the dataset)
data_3_2 <- subset(data_3_1, !(data_3_1$location_id==22 & data_3_1$year_start==2001 & data_3_1$cases==0 | data_3_1$location_id==22 & data_3_1$year_start==2003 & data_3_1$cases==0 | data_3_1$location_id==23 & data_3_1$year_start==2018 & data_3_1$cases==3 | data_3_1$location_id==23 & data_3_1$year_start==2018 & data_3_1$cases==217  | data_3_1$location_id==69 & data_3_1$year_start==2018 & data_3_1$cases==212 | data_3_1$location_id==71 & data_3_1$year_start==2007 & data_3_1$cases==126  | data_3_1$location_id==23 & data_3_1$year_start==2018 & data_3_1$cases==3 | data_3_1$location_id==105 & data_3_1$year_start==1999 & data_3_1$cases==0  | data_3_1$location_id==105 & data_3_1$year_start==2000 & data_3_1$cases==0  | data_3_1$location_id==105 & data_3_1$year_start==2001 & data_3_1$cases==0  | data_3_1$location_id==106 & data_3_1$year_start==1999 & data_3_1$cases==0  | data_3_1$location_id==106 & data_3_1$year_start==2001 & data_3_1$cases==0  | data_3_1$location_id==106 & data_3_1$year_start==2002 & data_3_1$cases==0  | data_3_1$location_id==108 & data_3_1$year_start==2005 & data_3_1$cases==0  | data_3_1$location_id==109 & data_3_1$year_start==2013 & data_3_1$cases==0  | data_3_1$location_id==112 & data_3_1$year_start==1999 & data_3_1$cases==0  | data_3_1$location_id==112 & data_3_1$year_start==2000 & data_3_1$cases==0  | data_3_1$location_id==112 & data_3_1$year_start==2003 & data_3_1$cases==0  | data_3_1$location_id==112 & data_3_1$year_start==2004 & data_3_1$cases==0 | data_3_1$location_id==119 & data_3_1$year_start==2013 & data_3_1$cases==0 | data_3_1$location_id==127 & data_3_1$year_start==2017 & data_3_1$cases== 936 | data_3_1$location_id==130  & data_3_1$year_start==1991 & data_3_1$cases==30))

data_4<- data_3_2 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, .keep_all = TRUE)
dropped_4 <- data_3_2[!(data_3_2$unique_id %in% data_4$unique_id),]

#just ordering these by loc id
dropped_4<- dropped_4[order(location_id),] 
data_4<- data_4[order(location_id),] 


#replacing cases with the case number after dropping the ones we still need (these are cause of specifity generally)

data_4$cases[data_4$location_id==130 & data_4$year_start==2013 & data_4$age_start==0 ] <- 234202

data_4$cases[data_4$location_id==130 & data_4$year_start==2003 & data_4$age_start==1 ] <- 6995

data_4$cases[data_4$location_id==130 & data_4$year_start==2004 & data_4$age_start==20 ] <- 8873

data_4$cases[data_4$location_id==130 & data_4$year_start==2005 & data_4$age_start==15 & data_4$age_end==19 ] <- 24705

data_4$cases[data_4$location_id==130 & data_4$year_start==2006 & data_4$age_start==5] <- 32386

data_4$cases[data_4$location_id==131 & data_4$year_start==2017 & data_4$age_start==0 & data_4$age_end==99] <- 67033

data_4$cases[data_4$location_id==180 & data_4$year_start==2017 & data_4$age_start==0 & data_4$age_end==99] <- 2343

data_4$cases[data_4$location_id==212 & data_4$year_start==2017 & data_4$age_start==0 & data_4$age_end==99] <- 121

data_4$cases[data_4$location_id==216 & data_4$year_start==2017 & data_4$age_start==0 & data_4$age_end==99] <- 929

data_4$cases[data_4$location_id==354 & data_4$year_start==2017 & data_4$age_start==6 & data_4$age_end==84] <- 141

data_4$cases[data_4$location_id==369 & data_4$year_start==2018 & data_4$age_start==0 & data_4$age_end==99] <- 160

data_4$cases[data_4$location_id==4868 & data_4$year_start==2001 & data_4$age_start==0 & data_4$age_end==99] <- 1457

data_4$cases[data_4$location_id==4868 & data_4$year_start==2016 & data_4$age_start==0 & data_4$age_end==99] <- 5292

data_4$cases[data_4$location_id==4869 & data_4$year_start==2016 & data_4$age_start==0 & data_4$age_end==99] <- 82

data_4$cases[data_4$location_id==4871 & data_4$year_start==2016 & data_4$age_start==0 & data_4$age_end==99] <- 4037

data_4$cases[data_4$location_id==4873 & data_4$year_start==2016 & data_4$age_start==0 & data_4$age_end==99] <- 15033

data_4$cases[data_4$location_id==4875 & data_4$year_start==2016 & data_4$age_start==0 & data_4$age_end==99] <- 22865

data_4$cases[data_4$location_id==53618  & data_4$year_start==2010 & data_4$age_start==0 & data_4$age_end==99] <- 51

## adding back the locations that we wanted to keep
data_4_2 <- rbind(data_4, loc_years_to_keep)




b4_drop5_loc_years_to_keep<- data_4_2[data_4_2$location_id== 6 & data_4_2$year_start==2017 | data_4_2$location_id== 125 & data_4_2$year_start==2010 & data_4_2$year_end==2011 | data_4_2$location_id==130 & data_4_2$year_start==1990 | data_4_2$location_id==130 & data_4_2$year_start==1991 | data_4_2$location_id==130 & data_4_2$year_start==1992 | data_4_2$location_id==130 & data_4_2$year_start==2003 | data_4_2$location_id==130 & data_4_2$year_start==2004 | data_4_2$location_id==130 & data_4_2$year_start==2005 | data_4_2$location_id==130 & data_4_2$year_start==2006 | data_4_2$location_id==131 & data_4_2$year_start==2004 | data_4_2$location_id==131 & data_4_2$year_start==2005 & data_4_2$year_end==2006 |  data_4_2$location_id==131 & data_4_2$year_start==2006 & data_4_2$year_end==2007 | data_4_2$location_id==132 & data_4_2$year_start==2005 | data_4_2$location_id==132 & data_4_2$year_start==2006 | data_4_2$location_id==132 & data_4_2$year_start==2007 | data_4_2$location_id==135 & data_4_2$year_start==2003 | data_4_2$location_id==135 & data_4_2$year_start==2004 | data_4_2$location_id==135 & data_4_2$year_start==2005  | data_4_2$location_id==135 & data_4_2$year_start==2006 |  data_4_2$location_id==165 & data_4_2$year_start==1999 & data_4_2$year_end==2001 | data_4_2$location_id==354 & data_4_2$year_start==2017 & data_4_2$age_start==6 & data_4_2$sex=="Male" | data_4_2$location_id==4750 | data_4_2$location_id==4751 |  data_4_2$location_id==4752 |  data_4_2$location_id==4753 |  data_4_2$location_id==4754 | data_4_2$location_id==4755 |  data_4_2$location_id==4756 |  data_4_2$location_id==4757 | data_4_2$location_id==4758 |  data_4_2$location_id==4759 |  data_4_2$location_id==4760 |  data_4_2$location_id==4761 |  data_4_2$location_id==4762 |  data_4_2$location_id==4763 |  data_4_2$location_id==4764 |  data_4_2$location_id==4765 |  data_4_2$location_id==4766 |  data_4_2$location_id==4767 |  data_4_2$location_id==4768 |  data_4_2$location_id==4769 |  data_4_2$location_id==4770 |  data_4_2$location_id==4771 |  data_4_2$location_id==4772 |  data_4_2$location_id==4773 |  data_4_2$location_id==4774 |  data_4_2$location_id==4775 |  data_4_2$location_id==4776|  data_4_2$location_id==53619 & data_4_2$year_start==2008 ]
#3274

#drop the above loc year from the data_4_2 as we will append these later
data_4_3<- subset(data_4_2, !(data_4_2$location_id== 6 & data_4_2$year_start==2017 | data_4_2$location_id== 125 & data_4_2$year_start==2010 & data_4_2$year_end==2011 | data_4_2$location_id==130 & data_4_2$year_start==1990 | data_4_2$location_id==130 & data_4_2$year_start==1991 | data_4_2$location_id==130 & data_4_2$year_start==1992 | data_4_2$location_id==130 & data_4_2$year_start==2003 | data_4_2$location_id==130 & data_4_2$year_start==2004 | data_4_2$location_id==130 & data_4_2$year_start==2005 | data_4_2$location_id==130 & data_4_2$year_start==2006 | data_4_2$location_id==131 & data_4_2$year_start==2004 | data_4_2$location_id==131 & data_4_2$year_start==2005 & data_4_2$year_end==2006 |  data_4_2$location_id==131 & data_4_2$year_start==2006 & data_4_2$year_end==2007 | data_4_2$location_id==132 & data_4_2$year_start==2005 | data_4_2$location_id==132 & data_4_2$year_start==2006 | data_4_2$location_id==132 & data_4_2$year_start==2007 | data_4_2$location_id==135 & data_4_2$year_start==2003 | data_4_2$location_id==135 & data_4_2$year_start==2004 | data_4_2$location_id==135 & data_4_2$year_start==2005  | data_4_2$location_id==135 & data_4_2$year_start==2006 |  data_4_2$location_id==165 & data_4_2$year_start==1999 & data_4_2$year_end==2001 | data_4_2$location_id==354 & data_4_2$year_start==2017 & data_4_2$age_start==6 & data_4_2$sex=="Male" | data_4_2$location_id==4750 | data_4_2$location_id==4751 |  data_4_2$location_id==4752 |  data_4_2$location_id==4753 |  data_4_2$location_id==4754 | data_4_2$location_id==4755 |  data_4_2$location_id==4756 |  data_4_2$location_id==4757 | data_4_2$location_id==4758 |  data_4_2$location_id==4759 |  data_4_2$location_id==4760 |  data_4_2$location_id==4761 |  data_4_2$location_id==4762 |  data_4_2$location_id==4763 |  data_4_2$location_id==4764 |  data_4_2$location_id==4765 |  data_4_2$location_id==4766 |  data_4_2$location_id==4767 |  data_4_2$location_id==4768 |  data_4_2$location_id==4769 |  data_4_2$location_id==4770 |  data_4_2$location_id==4771 |  data_4_2$location_id==4772 |  data_4_2$location_id==4773 |  data_4_2$location_id==4774 |  data_4_2$location_id==4775 |  data_4_2$location_id==4776|  data_4_2$location_id==53619 & data_4_2$year_start==2008))


fixing <- subset(data_4_3, !(data_4_3$location_id==6 & data_4_3$year_start==2004 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==6 & data_4_3$year_start==2005 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==6 & data_4_3$year_start==2006 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==6 & data_4_3$year_start==2009 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==12 & data_4_3$year_start==2000 & data_4_3$age_start!=0 & data_4_3$age_end!=99 | data_4_3$location_id==12 & data_4_3$year_start==2001 & data_4_3$age_start!=0 & data_4_3$age_end!=99 | data_4_3$location_id==12 & data_4_3$year_start==2002 & data_4_3$age_start!=0 & data_4_3$age_end!=99 | data_4_3$location_id==12 & data_4_3$year_start==2004 & data_4_3$age_start!=0 & data_4_3$age_end!=99 |  data_4_3$location_id==12 & data_4_3$year_start==2005 & data_4_3$age_start!=0 & data_4_3$age_end!=99 | data_4_3$location_id==13 & data_4_3$year_start==1988 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==13 & data_4_3$year_start==1989 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==13 & data_4_3$year_start==1990 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==13 & data_4_3$year_start==1991 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==13 & data_4_3$year_start==1992 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==13 & data_4_3$year_start==2003 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==13 & data_4_3$year_start==2004 & data_4_3$age_start==0 & data_4_3$age_end==99|  data_4_3$location_id==13 & data_4_3$year_start==2005 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==13 & data_4_3$year_start==2016 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==16 & data_4_3$year_start==1998 & data_4_3$age_start==0 & data_4_3$age_end==99 |   data_4_3$location_id==16 & data_4_3$year_start==1999 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==20 & data_4_3$year_start==2004 & data_4_3$age_start==2 |  data_4_3$location_id==27 & data_4_3$year_start==2017 & data_4_3$age_start==1 |  data_4_3$location_id==67 & data_4_3$year_start==2000 & data_4_3$sex_id==3  | data_4_3$location_id==69 & data_4_3$year_start==2003 & data_4_3$age_start==0 & data_4_3$age_end==99 | data_4_3$location_id==69 & data_4_3$year_start==2004 & data_4_3$age_start==0 & data_4_3$age_end==99 |   data_4_3$location_id==69 & data_4_3$year_start==2005 & data_4_3$age_start==0 & data_4_3$age_end==99 |   data_4_3$location_id==69 & data_4_3$year_start==2006 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==126 & data_4_3$year_start==2007 & data_4_3$age_start==15 & data_4_3$age_end==19 & data_4_3$sex_id==3 | data_4_3$location_id==126 & data_4_3$year_start==2008 & data_4_3$nid==138969  | data_4_3$location_id==127 & data_4_3$year_start==2001 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==127 & data_4_3$year_start==2002 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==127 & data_4_3$year_start==2003 & data_4_3$age_start==0 & data_4_3$age_end==99  |  data_4_3$location_id==127 & data_4_3$year_start==2004 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==127 & data_4_3$year_start==2005 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==127 & data_4_3$year_start==2005 & data_4_3$age_start==0 & data_4_3$age_end==99  |  data_4_3$location_id==127 & data_4_3$year_start==2006 & data_4_3$age_start==0 & data_4_3$age_end==99 |   data_4_3$location_id==127 & data_4_3$year_start==2007 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==127 & data_4_3$year_start==2007 & data_4_3$sex_id==3 |   data_4_3$location_id==127 & data_4_3$year_start==2008 & data_4_3$age_start==0 & data_4_3$age_end==99 |  data_4_3$location_id==127 & data_4_3$year_start==2009 & data_4_3$age_start==0 & data_4_3$age_end==99))

fixing_2 <- subset(fixing, !(fixing$location_id==136  & fixing$year_start==2009  & fixing$year_end==2010  & fixing$age_start==0  & fixing$age_end==99 | fixing$location_id==131 & fixing$year_start==2005  & fixing$year_end==2005 & fixing$age_start!=0 & fixing$age_end!=99 | fixing$location_id==131 & fixing$year_start==2006  & fixing$year_end==2006  & fixing$age_start!=0 & fixing$age_end!=99 |   fixing$location_id==131 & fixing$year_start==2007  & fixing$year_end==2007  & fixing$age_start!=0 & fixing$age_end!=99 |  fixing$location_id==157  & fixing$year_start==2005  & fixing$sex_id!=3 |   fixing$location_id==212  & fixing$year_start==2017  & fixing$age_start==37 |    fixing$location_id==53619  & fixing$year_start==2003  & fixing$sex_id==3 |  fixing$location_id==53619  & fixing$year_start==2006  & fixing$sex_id==3 |  fixing$location_id==53619  & fixing$year_start==2009  & fixing$sex_id==3 | fixing$location_id==127 & fixing$year_start==2003 & fixing$cases==6438 ))

data_4_4<- fixing_2

##appemd locs to keep 
data_5<- rbind(data_4_4, b4_drop5_loc_years_to_keep)

#####moving on to those rows where specificity is not equal to missing so we can use unique group to do this entire process!


data_with_UI<- subset(dengue_bundle, specificity!="")

data_with_UI$sex_id<- 0

data_with_UI$sex_id[data_with_UI$sex=="Both"] <- 3
data_with_UI$sex_id[data_with_UI$sex=="Female"] <- 2
data_with_UI$sex_id[data_with_UI$sex=="Male"] <- 1

data_with_UI$unique_id <- 1:nrow(data_with_UI)

data_with_UI_gr<- subset(data_with_UI, group_review !=0 | is.na(group_review))


# drop 1

#observing overall duplicates based on nid, location_id, sex, year_start, year_end, age_start, age_end, cases, sample_size, measure 
data_with_UI_1<- data_with_UI_gr %>% distinct(nid, location_id, sex_id, year_start, year_end, age_start, age_end, cases, sample_size, measure, .keep_all = TRUE)
dropped_wUI_1 <- data_with_UI_gr[!(data_with_UI_gr$unique_id %in% data_with_UI_1$unique_id),]




#drop 2
#made sure that there is only one source per demographic cohort (nid not taken into account)
data_with_UI_2<- data_with_UI_1 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, cases, sample_size, .keep_all = TRUE)
dropped_wUI_2 <- data_with_UI_1[!(data_with_UI_1$unique_id %in% data_with_UI_2$unique_id),]


# drop 3
#checking if there is a difference by sample size
data_with_UI_3<- data_with_UI_2 %>% distinct(location_id, sex_id, year_start, year_end, age_start, age_end, cases, .keep_all = TRUE)
dropped_wUI_3 <- data_with_UI_2[!(data_with_UI_2$unique_id %in% data_with_UI_3$unique_id),]

data_with_UI_3$group_id <- data_with_UI_3 %>% group_indices(nid, year_start, year_end, location_id) 

group_ids<- unique(data_with_UI_3$group_id)


b4_drop_UI_to_keep_by_group<- data_with_UI_3[data_with_UI_3$group_id== 68 & data_with_UI_3$unique_group==1.1 | data_with_UI_3$group_id== 103 & data_with_UI_3$unique_group==1.1 | data_with_UI_3$group_id== 104 & data_with_UI_3$unique_group==1.1 | data_with_UI_3$group_id== 105 & data_with_UI_3$unique_group==1.1 | data_with_UI_3$group_id== 112 & data_with_UI_3$unique_group==1.2 | data_with_UI_3$group_id== 146 & data_with_UI_3$unique_group==1 | data_with_UI_3$group_id== 147 & data_with_UI_3$unique_group==1.2  | data_with_UI_3$group_id== 148 & data_with_UI_3$unique_group==1.1   | data_with_UI_3$group_id== 153 & data_with_UI_3$unique_group==1 | data_with_UI_3$group_id== 156 & data_with_UI_3$unique_group==1.1  | data_with_UI_3$group_id== 215 & data_with_UI_3$unique_group==1 | data_with_UI_3$group_id== 341 & data_with_UI_3$unique_group==1.1  | data_with_UI_3$group_id== 342 & data_with_UI_3$unique_group==1.3 | data_with_UI_3$group_id== 343 & data_with_UI_3$unique_group==1 | data_with_UI_3$group_id== 346 & data_with_UI_3$unique_group==1.2 | data_with_UI_3$group_id==349 & data_with_UI_3$unique_group==1 | data_with_UI_3$group_id== 350 & data_with_UI_3$unique_group==1.2 | data_with_UI_3$group_id== 351 & data_with_UI_3$unique_group==1.12  | data_with_UI_3$group_id== 352 & data_with_UI_3$unique_group==1.12 | data_with_UI_3$group_id== 418 & data_with_UI_3$unique_group==1  | data_with_UI_3$group_id== 421 & data_with_UI_3$unique_group==1 | data_with_UI_3$group_id== 422 & data_with_UI_3$unique_group==1.2 | data_with_UI_3$group_id== 424 & data_with_UI_3$unique_group==1.12 | data_with_UI_3$group_id== 425 & data_with_UI_3$unique_group==1 | data_with_UI_3$group_id== 426 & data_with_UI_3$unique_group==1.1]

data_with_UI_3<- subset(data_with_UI_3, !(data_with_UI_3$group_id== 68 | data_with_UI_3$group_id== 103  | data_with_UI_3$group_id== 104  | data_with_UI_3$group_id== 105   | data_with_UI_3$group_id== 112  | data_with_UI_3$group_id== 146  | data_with_UI_3$group_id== 147 | data_with_UI_3$group_id== 148  | data_with_UI_3$group_id== 153  | data_with_UI_3$group_id== 156  | data_with_UI_3$group_id== 215  | data_with_UI_3$group_id== 341  | data_with_UI_3$group_id== 342 | data_with_UI_3$group_id== 343 | data_with_UI_3$group_id== 346 | data_with_UI_3$group_id==349 | data_with_UI_3$group_id== 350 | data_with_UI_3$group_id== 351 | data_with_UI_3$group_id== 352  | data_with_UI_3$group_id== 418   | data_with_UI_3$group_id== 421| data_with_UI_3$group_id== 422 | data_with_UI_3$group_id== 424 | data_with_UI_3$group_id== 425 | data_with_UI_3$group_id== 426))

##appemd the groups (nid, loc, year) that were determined, with the correct unique group row 
data_with_UI_4<- rbind(data_with_UI_3, b4_drop_UI_to_keep_by_group)

#dropping group id as that is missing from earlier de-duped dataset (the one with no specificity info)
data_with_UI_5<- subset(data_with_UI_4, select = -c(group_id))

dedup_dengue_data<- rbind(data_with_UI_5, data_5)

#cleaning it further
dedup_dengue_data[, age_start := round(age_start)]
dedup_dengue_data[, age_end := round(age_end)]

dedup_dengue_data<- subset(dedup_dengue_data, cases!="NA")
dedup_dengue_data<- subset(dedup_dengue_data, sample_size!="NA")
dedup_dengue_data<- subset(dedup_dengue_data, sample_size!=0)
dedup_dengue_data<- subset(dedup_dengue_data, sample_size!=cases)
dedup_dengue_data<- subset(dedup_dengue_data, sample_size>cases)

dedup_dengue_data$mean<- dedup_dengue_data$cases/dedup_dengue_data$sample_size
dedup_dengue_data$standard_error<- sqrt(((dedup_dengue_data$mean*(1-dedup_dengue_data$mean))/dedup_dengue_data$sample_size))

dedup_dengue_data<- subset(dedup_dengue_data, measure=="incidence")

#cleaning
dedup_dengue_data$mean[dedup_dengue_data$cases==0] <- 0


source("FILEPATH")
location_set<- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
dedup_dengue_data = subset(dedup_dengue_data, select = -c(ihme_loc_id))
dedup_dengue_data <- merge(dedup_dengue_data, location_set[, c('location_id', 'ihme_loc_id' )], by=c('location_id'), all.x = TRUE)


#saving a copy for the team - this is the de-duped data before sex and age splitting
openxlsx::write.xlsx(dedup_dengue_data, "FILEPATH",  sheetName = "extraction")



##########
## crosswalkking dengue age bundle 
############

dengue_age<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'iterative')
dengue_age_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step='iterative', include_clinical = FALSE) # returns a bundle_version_id
#request_id bundle_version_id request_status

dengue_age_bundle<- get_bundle_version(bundle_version_id = ADDRESS, export=TRUE)

dengue_age_bundle <- dengue_age_bundle[, age_width := age_end - age_start]
dengue_age_bundle <- dengue_age_bundle[age_width <= 25, ]
#3966
dengue_age_bundle[, crosswalk_parent_seq := seq]


# #removing sex-specific data as DISMOD only allows for one or the other.

dengue_age_bundle_v1<- subset(dengue_age_bundle, sex=="Male" | sex=="Female")

#removing group review =0 and keeping group revoew =1 or =missings
dengue_age_bundle_v2<- subset(dengue_age_bundle_v1, group_review !=0 | is.na(group_review))


# 

description <- "dengue_age_specific_data"
openxlsx::write.xlsx(dengue_age_bundle_v2, "FILEPATH",  sheetName = "extraction")

#outlier roraima manually 

cw_dengue_age_specific_bundle <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)


####################################################################################
###saving crosswalk for MAIN BUNDLE! dengue age split data that goes into st/gpr####
######################################################################################

dengue_total_bv<- save_bundle_version(bundle_id=ADDRESS, decomp_step="iterative", include_clinical = FALSE) # returns a bundle_version_id
#request_id bundle_version_id request_status

description <- "sex_and_age_split_dengue_data"
dengue_cw_v <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)

#Attempt2, uploading cases>=0.5
description <- "cases>=0.5, sex_and_age_split_dengue_data"
dengue_cw_v <- save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = "FILEPATH", description = description)



###END####

