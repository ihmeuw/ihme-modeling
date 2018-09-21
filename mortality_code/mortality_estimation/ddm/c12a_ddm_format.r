# 11/14/2013
# prep the DDM data, and put it in the idie data file

rm(list=ls())
library(foreign); library(reshape); library(stringr)
require(plyr)
setwd("FILEPATH")  
date <- Sys.Date()

## set up filepaths

## Part 1
d08_data_filepath <- "FILEPATH/d08_smoothed_completeness.dta"
d00_data_filepath <- "FILEPATH/d00_compiled_deaths.dta"
population_filepath <- "FILEPATH/USABLE_POPULATION_GLOBAL_1970-2013.dta"s
china_filepath <- "FILEPATH/china_completeness_from_G_Y2014M11D17.csv"
save_ddm_est_filepath <- "FILEPATH/estimates_ddm.csv"

## Part 2 (combine with part 1 ones later)
d08filepath <- "FILEPATH/d08_smoothed_completeness.dta"
d05filepath <- "FILEPATH/d05_formatted_ddm.dta"
d00filepath <- "FILEPATH/d00_compiled_population.dta"
idiedatafilepath <- "FILEPATH/raw_5q0_and_45q15_with_nids.dta"  	
## Change this
savefilepath <- "FILEPATH/mortviz_data_test.csv"
archivedfilepath <- paste("FILEPATH/mortviz_data_test_",date,".csv",sep="")

# drop subnational? set to 1 if you want to drop it, to 0 if you want to keep it
dropsubnational <- 0
subnational.list <- c("XAC", "XAM", "XBC", "XBM", "XCB", "XCC", "XCD", "XCE", "XCF", "XCG", "XCH", "XCI", "XCJ", "XCK", "XCL", "XCM", "XCN", "XCO", "XCP", "XCQ", "XEM", "XFC", "XFM", "XGC", "XGG", "XGH", "XGI", "XGJ", "XGK", "XGL", "XGM", "XGN", "XGO", "XGP", "XHC", "XHM", "XIM", "XIR", "XIU", "XJC", "XJM", "XLC", "XMA", "XMB", "XMC", "XMD", "XME", "XMF", "XMG", "XMH", "XMI", "XMJ", "XMK", "XML", "XMM", "XMN", "XMO", "XMP", "XMQ", "XMR", "XMS", "XMT", "XMU", "XMV", "XMW", "XMX", "XMY", "XMZ", "XNC", "XNI", "XQC", "XSC", "XTC", "XTI", "XXC", "XYC", "XZC")



####################
## DDM - Estimates
####################

est <- read.dta(d08_data_filepath)
file <- rbind(file, data.frame(file = "DDM Completeness Estimates",
                               date = file.info(d08_data_filepath)$mtime,
                               stringsAsFactors=F))
file <- rbind(file, data.frame(file = "Deaths data for reporting years",
                               date = file.info(d00_data_filepath)$mtime,
                               stringsAsFactors=F))    
file <- rbind(file, data.frame(file = "Population data",
                               date = file.info(population_filepath)$mtime,
                               stringsAsFactors=F))                                          

# drop subnational data
# only 165 countries have ddm data
if (dropsubnational == 1) {
  est <- est[!(est$iso3 %in% subnational.list),]
  if (length(unique(est$iso3)) != 165) {
    print("not all subnational data were dropped; you will get an error")
    print("check that all subnational isos for each of UK, MEX, IND, and CHN are included in the drop")
    break
  }else
    if (length(unique(est$iso3)) == 165) {
      print("subnational points were dropped successfully")
    }
}else
  if (dropsubnational == 0) {
    print("not dropping subnational data")
  }     

# create age, sex, and source variables
split <- strsplit(est$iso3_sex_source, "_")
iso3.sex.source <- do.call(rbind, split)         
est$iso3 <- iso3.sex.source[,1]
est$sex <- iso3.sex.source[,2]
est$source <- iso3.sex.source[,3]  

# keep only the variables of interest, rename
# take the average of the 3 estimates per iso3-sex-source-year
est <- unique(est[,c("iso3", "iso3_sex_source", "source", "year", "sex", "u5_comp_pred", "pred1", "pred", "lower", "upper", "trunc_pred", "trunc_lower", "trunc_upper")])
names(est) <- c("iso3", "iso3_sex_source", "source", "year", "sex", "u5", "first", "second.med", "second.lower", "second.upper", "trunc.med", "trunc.lower", "trunc.upper")
est <- aggregate(est[,!(names(est) %in% c("iso3","iso3_sex_source","source","year","sex"))], by = list(iso3 = est$iso3,iso3_sex_source = est$iso3_sex_source, sex = est$sex,year = est$year,source = est$source), FUN=mean, na.rm=T)

# create an indicator variable for if we use trunc_med or second_med as the estimate of completeness
est$trunc.or.second <- "second.med"
est$trunc.or.second[grepl("DSP",est$source) | est$source == "SRS" | grepl("VR", est$source)] <- "trunc.med" 

# keep only the censuses, dsp, srs, and vr data
est <- est[est$source == "CENSUS" | grepl("DSP",est$source) | est$source == "SRS" | grepl("VR", est$source),]
#do not make every source with VR in it equal to VR; TUR needs to be split up, as does South Korea
#est$source[grepl("VR",est$source)] <- "VR"
est$source[est$source == "VR1" & est$iso3=="TUR"] <- "VR pre-2009"
est$source[est$source == "VR2" & est$iso3=="TUR"] <- "VR post-2009"

est$source[est$source == "VR1" & est$iso3=="KOR"] <- "VR pre-1978"
est$source[est$source == "VR2" & est$iso3=="KOR"] <- "VR post-1978"

# need to split up the CHN provincial level DSP into it's 3 subgroups
est$source[est$source == "DSP1"] <- "DSP before 1996"
est$source[est$source == "DSP2"] <- "DSP 1996 to 2003"
est$source[est$source == "DSP3"] <- "DSP 2004 and after"

# want to only report estimates for years we actually have data
reportingyears <- read.dta(d00_data_filepath)
reportingyears <- reportingyears[,c("iso3","sex","year","source_type")]

#keep only the censuses, dsp, srs, and vr data
reportingyears <- reportingyears[reportingyears$source_type == "CENSUS" | grepl("DSP",reportingyears$source_type) | grepl("SRS",reportingyears$source_type) | grepl("VR", reportingyears$source_type),]
reportingyears$source_type[grepl("VR", reportingyears$source_type)] <- "VR"  
reportingyears$source_type[grepl("DSP", reportingyears$source_type)] <- "DSP"  
reportingyears$source_type[grepl("SRS", reportingyears$source_type)] <- "SRS"  
reportingyears <- rename(reportingyears, c(source_type = "source"))

# so we know whether something was in the raw dataset, make an indicator variable
# once we merge this, this will only be 1 for the ones that merge
reportingyears$reporting.year <- 1

# merge the lists together to know when we have raw data for a year
est <- merge(est, reportingyears, all.x = T, by = c("iso3", "sex", "year", "source"))
est$reporting.year[is.na(est$reporting.year)] <- 0

# to plot the final estimates, censuses use second.med and vr/srs/dsp use trunc.med
est$final_est <- 0
est$final_est[est$trunc.or.second == "second.med"] <- est$second.med[est$trunc.or.second == "second.med"]
est$final_est[est$trunc.or.second == "trunc.med"] <- est$trunc.med[est$trunc.or.second == "trunc.med"]
est$final_lower <- 0
est$final_lower[est$trunc.or.second == "second.med"] <- est$second.lower[est$trunc.or.second == "second.med"]
est$final_lower[est$trunc.or.second == "trunc.med"] <- est$trunc.lower[est$trunc.or.second == "trunc.med"]  
est$final_upper <- 0   
est$final_upper[est$trunc.or.second == "second.med"] <- est$second.upper[est$trunc.or.second == "second.med"]
est$final_upper[est$trunc.or.second == "trunc.med"] <- est$trunc.upper[est$trunc.or.second == "trunc.med"]     

# create populated weighted estimate for SAU both sexes combined
pop <- read.dta(population_filepath)

# 1. keep only releveant variables and age groups and country
pop <- pop[pop$iso3=="SAU" & pop$sex != "both",names(pop)[!(names(pop) %in% c("gbd_region","countryname_ihme","births","pop_nn","pop_pnn","pop_0","pop_1_4","pop_5_9","pop_10_14","source"))],]

# 2. get the yearly sex-specific population
pop$pop <- rowSums(pop[4:17])
pop <- pop[,names(pop)[names(pop) %in% c("iso3","year","sex","pop")]]

# 3. reshape the data so that we can get the proportion of the population by sex
pop <- reshape(pop,
               direction = "wide",
               idvar = c("iso3","year"),
               v.names = "pop",
               timevar = "sex")
pop$total <- pop$pop.female + pop$pop.male
pop$prop.male <- pop$pop.male/pop$total
pop$prop.female <- pop$pop.female/pop$total 
pop <- pop[,names(pop)[names(pop) %in% c("iso3","year","sex","prop.male","prop.female")]]     

# subset the estimates data to be just the SAU VR data, bring the pop data on, and then weight the estimates
sauest <- est[est$iso3=="SAU" & grepl("VR",est$iso3_sex_source),]
sauest <- merge(sauest,pop,by=c("iso3","year"), all.x =T)
sauest$iso3_sex_source <- NULL

# reshape the data so that we can weight the estimates
sauest2 <- reshape(sauest,
                   direction = "wide",
                   idvar = c("iso3","year","source","trunc.or.second","reporting.year","prop.male","prop.female"),
                   v.names = c("u5","first","second.med","second.lower","second.upper","trunc.med","trunc.lower","trunc.upper","final_est", "final_upper","final_lower"),
                   timevar = "sex")

# don't need to worry about looping over countries or sources, because it's just SAU VR
years <- unique(sauest2$year)
variables <- c("u5","first","second.med","second.lower","second.upper","trunc.med","trunc.lower","trunc.upper","final_est", "final_upper","final_lower")
sauest2$u5 <- NA
sauest2$first <- NA
sauest2$second.med <- NA
sauest2$second.lower <- NA
sauest2$second.upper <- NA
sauest2$trunc.med <- NA
sauest2$trunc.lower <- NA
sauest2$trunc.upper <- NA 
sauest2$final_est <- NA
sauest2$final_lower <- NA   
sauest2$final_upper <- NA                                                                                             
for (year in years) {
  for (variable in variables) {
    malevar <- paste(variable,".male",sep="") 
    femalevar <- paste(variable,".female",sep="") 
    sauest2[sauest2$year==year,variable] <- sauest2[sauest2$year==year,c("prop.male")]*sauest2[sauest2$year==year,malevar] + sauest2[sauest2$year==year,c("prop.female")]*sauest2[sauest2$year==year,femalevar]
    
  }
  
}

sauest2 <- sauest2[,names(sauest2)[!grepl("male",names(sauest2))]]                 
sauest2$sex <- "both"
sauest2$iso3_sex_source <- "SAU_both_VR"
sauest2$trunc.or.second <- NULL

# drop unnecessary variables, final formatting
est$trunc.or.second <- NULL
est$year <- est$year + 0.5  

# append on the SAU data
sauest2 <- sauest2[,c("iso3","sex","year","source","iso3_sex_source","u5","first","second.med","second.lower","second.upper","trunc.med","trunc.lower","trunc.upper","reporting.year","final_est", "final_lower","final_upper")]
est <- rbind(est,sauest2)

# append on the China VR dataset
china_data <- read.csv(china_filepath)
china_data <- china_data[,c("iso3","sex","year","source","iso3_sex_source","u5","first","second.med","second.lower","second.upper","trunc.med","trunc.lower","trunc.upper","reporting.year","final_est", "final_lower","final_upper")]
est <- rbind(est,china_data)


#write.dta(est, save_ddm_est_filepath) 
write.csv(est, save_ddm_est_filepath)




####################
## Prep the ddm data
#####################
# bring in the data
initialdata <- read.dta(d08filepath)
initialdata <- initialdata[,c("iso3", "iso3_sex_source", "year", "u5_comp", "comp_type", "comp", "exclude")]

# split it up into adult and child data; this will allow us to later append the lists together to get 1 list the same format
adult <- initialdata[initialdata$comp_type %in% c("ggb", "seg", "ggbseg"),c("iso3", "iso3_sex_source", "year", "comp_type", "comp", "exclude")]
num.adult <- nrow(adult)
child <- initialdata[!is.na(initialdata$u5_comp),c("iso3", "iso3_sex_source", "year", "u5_comp", "exclude")]
child <- rename(child, c(u5_comp = "comp"))
child$comp_type <- "u5"
# outliers are only for adult completeness, so make the exclude variable 0 for all child completeness data points
child$exclude <- 0

# there are some duplicates for child completeness because of having a child completeness data point in the same year that we also
# have adult completeness data
child <- child[!(duplicated(child[,c("iso3_sex_source","year","comp")])),]
num.child <- nrow(child)

# bring data together    
data <- rbind(adult, child)
# make sure that the append worked okay (didn't add or lose observations)
if (num.adult+num.child != nrow(data)) {
  stop("the append yielded too many or too few observations; didn't equal number of adult observations plus number of child observations")
}else 
  if (num.adult+num.child == nrow(data)) {
    print("the append was successful!")
  }

# some initial formatting
data <- data[order(data$iso3_sex_source, data$year, data$comp_type),]
data <- rename(data, c(exclude = "outlier"))

# create age, sex, and source variables
split <- strsplit(data$iso3_sex_source, "_")
iso3.sex.source <- do.call(rbind, split)         
data$iso3 <- iso3.sex.source[,1]
data$sex <- iso3.sex.source[,2]
data$source <- iso3.sex.source[,3]

# keep only the censuses, dsp, srs, and vr data
data <- data[data$source == "CENSUS" | grepl("DSP", data$source) | data$source == "SRS" | grepl("VR", data$source),]

# create a method_id variable
data$method_id[data$comp_type == "u5"] <- 6
data$method_id[data$comp_type == "seg"] <- 7
data$method_id[data$comp_type == "ggb"] <- 8  
data$method_id[data$comp_type == "ggbseg"] <- 9 

# create type_id variable
data$type_id[data$source == "VR"] <- 1
data$type_id[data$source == "SRS"] <- 2
data$type_id[data$source == "DSP"] <- 3
data$type_id[data$source == "CENSUS"] <- 5 
data$type_id[data$source == "VR1" & data$iso3 == "TUR"] <- 18
data$type_id[data$source == "VR2" & data$iso3 == "TUR"] <- 19      
data$type_id[data$source == "DSP1"] <- 20        
data$type_id[data$source == "DSP2" | data$source == "DSP-1996-2000"] <- 21
data$type_id[data$source == "DSP3" | data$source == "DSP-2004-2010"] <- 22 
data$type_id[data$source == "VR1" & data$iso3 == "KOR"] <- 24
data$type_id[data$source == "VR2" & data$iso3 == "KOR"] <- 25              

# get rid of extraneous variables and match the format of idie_data
data$comp_type <- data$source <- data$iso3_sex_source <- NULL  
data <- rename(data, c(comp = "data_raw"))  
data$process <- "ddm"
data$data_final <- data$data_raw

# need these other variables to append the datasets together
data$source_citation <- ""
data$source_id <- NA
data$shock <- 0
data$adjre_fe <- ""
data$reference <- NA
data$type_short <- ""
data$type_full <- ""
data$type_color <- ""
data$method_short <- ""
data$method_full <- ""
data$method_shape <- ""
data$nid <- NA

# drop subnational data
# only 162 countries have ddm data
if (dropsubnational == 1) {
  data <- data[!(data$iso3 %in% subnational.list),]
  if (length(unique(data$iso3)) != 162) {
    print("not all subnational data were dropped; you will get an error")
    print("check that all subnational isos for each of UK, MEX, IND, and CHN are included in the drop")
    stop("see error statement above")
  }else
    if (length(unique(data$iso3)) == 162) {
      print("subnational points were dropped successfully")
    }
}else
  if (dropsubnational == 0) {
    print("not dropping subnational data")
  }     
num.data.before.years.added <- nrow(data)

###########################################################
## merge the ddm data with other identifying variables that will help with merging on the citations
############################################################
d05 <- read.dta(d05filepath)
d05 <- d05[,names(d05)[names(d05) %in% c("iso3","pop_years","source_type","sex","ggbseg", "seg", "ggb")]]
# drop subnational data
# only 157 countries have ddm data
if (dropsubnational == 1) {
  d05 <- d05[!(d05$iso3 %in% subnational.list),]
  if (length(unique(d05$iso3)) != 157) {
    print("not all subnational data were dropped; you will get an error")
    print("check that all subnational isos for each of UK, MEX, IND, and CHN are included in the drop")
    stop("see error statement above")
  }else
    if (length(unique(d05$iso3)) == 157) {
      print("subnational points were dropped successfully")
    }
}else
  if (dropsubnational == 0) {
    print("not dropping subnational data")
  }

# get the ddm estimate year, and keep the two pop years.  we will use both the estimate year and the floor of that year, because of later merging
pop.years.split <- strsplit(d05$pop_years, "/")
pop.years.split <- do.call(rbind,pop.years.split)

d05$year1 <- pop.years.split[,3]
d05$year2 <- pop.years.split[,5]
d05$year1 <- as.numeric(substr(d05$year1,1,4))
d05$year2 <- as.numeric(d05$year2)

d05$year <- floor((d05$year1 + d05$year2)/2)
d05$estyear <-(d05$year1 + d05$year2)/2

# keep only the VR, SRS, DSP, and census data, and make the type_id variable for the source
d05 <- d05[d05$source_type == "CENSUS" | grepl("DSP",d05$source_type) | grepl("VR",d05$source_type) | grepl("SRS",d05$source_type),]
d05$type_id[grepl("VR", d05$source_type) & d05$iso3 != "TUR"] <- 1
d05$type_id[grepl("SRS",d05$source_type)] <- 2
d05$type_id[d05$source_type=="DSP"] <- 3
d05$type_id[grepl("CENSUS", d05$source_type)] <- 5
d05$type_id[d05$iso3=="TUR" & d05$source_type == "VR1"] <- 18
d05$type_id[d05$iso3=="TUR" & d05$source_type == "VR2"] <- 19
d05$type_id[d05$source_type=="DSP1"] <- 20
d05$type_id[d05$source_type=="DSP2" | d05$source_type == "DSP-1996-2000"] <- 21
d05$type_id[d05$source_type=="DSP3" | d05$source_type == "DSP-2004-2010"] <- 22
d05$type_id[d05$iso3=="KOR" & d05$source_type == "VR1"] <- 24
d05$type_id[d05$iso3=="KOR" & d05$source_type == "VR2"] <- 25
d05$source_type <- NULL

# make a method variable using the ggb, seg, and ggbseg information
# there are some Korea points that are not unique by iso3-pop_years-sex-type_id
# d05[duplicated(d05[,c("iso3","pop_years","sex","type_id")]),]
# create an index variable so that with that variable, the unique identifiers are iso3-pop years - sex-type_id
# ultimate why?: need a unique identifier for the reshape
d05$index <- NA
countries <- unique(d05$iso3)

for (country in countries) {
  sexes <- unique(d05$sex[d05$iso3 == country])
  for (sex in sexes) {
    popyears <- unique(d05$pop_years[d05$iso3 == country & d05$sex == sex])
    for (popyear in popyears) {
      types <- unique(d05$type_id[d05$iso3 == country & d05$sex == sex & d05$pop_years == popyear])
      for (type in types) {
        subset <- d05$iso3 == country & d05$sex == sex & d05$pop_years == popyear & d05$type_id == type
        print(paste(country, sex, popyear, type,sep=": "))
        numberofduplicates <- nrow(d05[subset,])
        rownameslist <- as.character(c(1:numberofduplicates))
        
        if (numberofduplicates > 1) {
          for (rownum in 1:numberofduplicates) {
            
            d05[subset, c("index")][rownum] <- rownameslist[rownum]
          }
        } else {
          d05[subset, c("index")] <- 1
        }
        
        
      }
    }
  }
}

d05 <- reshape(d05,
               direction = "long",
               idvar = c("iso3","pop_years","sex","type_id","index"),
               varying = list(c("ggb","seg","ggbseg")),
               v.names = "data_raw",
               timevar = "method_id2",
               times = c("ggb","seg","ggbseg"))
row.names(d05) <- NULL

# create a method_id variable for merging purposes
d05$method_id[d05$method_id2 == "seg"] <- 7
d05$method_id[d05$method_id2 == "ggb"] <- 8
d05$method_id[d05$method_id2 == "ggbseg"] <- 9
d05$method_id2 <- NULL


# bring the d05 data (years information) onto d08 (data)
data <- merge(data, d05, c("iso3","year","data_raw","sex","method_id","type_id"), all.x = T)
if (nrow(data) != num.data.before.years.added) {
  stop("error: the merge of the data and the data years did not work; too many or too few observations")
  
} else {
  print("ddm data and years merged correctly")
}

# to make sure that observations aren't dropped in the merge with the ddm citations data, get the number of observations in the ddm data
num.obs.ddm.data <- nrow(data)

####################
## get ddm citations
#####################
citations <- read.dta(d05filepath)

# A. get rid of unnecessary variables, and drop subnational
citations$ggb <- citations$seg <- citations$ggbseg <- citations$country <- citations$pop_footnote <- citations$deaths_footnote <- NULL

# drop subnational data
# only 157 countries have ddm data citations
if (dropsubnational == 1) {
  citations <- citations[!(citations$iso3 %in% subnational.list),]
  if (length(unique(citations$iso3)) != 157) {
    print("not all subnational data were dropped; you will get an error")
    print("check that all subnational isos for each of UK, MEX, IND, and CHN are included in the drop, or that new national data for a new country were added")
    stop("see error statement above")
  }else
    if (length(unique(citations$iso3)) == 157) {
      print("subnational points were dropped successfully")
    }
}else
  if (dropsubnational == 0) {
    print("not dropping subnational data")
  }          

# B. make a type_id variable for merging purposes, after keeping only the dsp, srs, census, and vr data
citations <- rename(citations, c(source_type = "source"))
citations <- citations[citations$source == "CENSUS" | grepl("DSP",citations$source) | grepl("SRS",citations$source) | grepl("VR", citations$source),]
citations$source[grepl("VR", citations$source) & !(citations$iso3 %in% c("TUR", "KOR"))] <- "VR"
citations$source[grepl("SRS", citations$source)] <- "SRS"   

citations$type_id[citations$source == "VR"] <- 1
citations$type_id[citations$source == "SRS"] <- 2
citations$type_id[citations$source == "DSP"] <- 3
citations$type_id[citations$source == "CENSUS"] <- 5
citations$type_id[citations$source == "VR1" & citations$iso3 == "TUR"] <- 18        
citations$type_id[citations$source == "VR2" & citations$iso3 == "TUR"] <- 19
citations$type_id[citations$source == "DSP1"] <- 20 
citations$type_id[citations$source == "DSP2" | citations$source == "DSP-1996-2000"] <- 21
citations$type_id[citations$source == "DSP3" | citations$source == "DSP-2004-2010"] <- 22
citations$type_id[citations$source == "VR1" & citations$iso3 == "KOR"] <- 24        
citations$type_id[citations$source == "VR2" & citations$iso3 == "KOR"] <- 25
citations$source <- NULL

# C. get the ddm estimate year.  this will be the midpoint of the population years
popsplit <- strsplit(citations$pop_years, "/")
popyrs.split <- do.call(rbind,popsplit)

citations$year1 <- popyrs.split[,3]
citations$year2 <- popyrs.split[,5]
citations$year1 <- as.numeric(substr(citations$year1,1,4))
citations$year2 <- as.numeric(citations$year2)

citations$year <- (citations$year1 + citations$year2)/2
citations$pop_years <- NULL

# D. format the data so that pop years and pop source & deaths_years and deaths_source are long
#D1. deaths years
dyears.split <- strsplit(citations$deaths_years, " ")

# because the length of each element in the list is different, we need to make each element the same length
lengthof.dyears.elements <- sapply(dyears.split, length)
max.length.dyears <- max(lengthof.dyears.elements) 
number.elements.in.list.dyears <- length(dyears.split)

# fill in elements up to the max length of an element in the list
for (element in 1:number.elements.in.list.dyears) {
  length <- length(dyears.split[[element]])
  
  i <- length + 1
  max.length.dyears2 <- max.length.dyears + 1
  while (i < max.length.dyears2) {
    dyears.split[[element]][i] <- ""   
    i <- i + 1 
  } 
}

# make the data into a dataframe
dyears.split <- do.call(rbind,dyears.split)
dyears.split <- as.data.frame(dyears.split)

# rename the variables something helpful
variablenames <- names(dyears.split)

for (i in 1:max.length.dyears) { 
  variablenames[i] <- gsub("V", "deaths_year-", variablenames[i])     
}
names(dyears.split) <- variablenames

# D2. deaths source
# a few quick fixes that will help with similarities to pop sources (see start of D3: population source splitting)
citations$deaths_source <- gsub("2010 SRS report", "2010_SRS_report", citations$deaths_source)
citations$deaths_source <- gsub("SRS Report", "SRS_report", citations$deaths_source)

dsource.split <- strsplit(citations$deaths_source, "#")

# because the length of each element in the list is different, we need to make each element the same length
lengthof.dsource.elements <- sapply(dsource.split, length)
max.length.dsource <- max(lengthof.dsource.elements) 
number.elements.in.list.dsource <- length(dsource.split)

# fill in elements up to the max length of an element in the list
for (element in 1:number.elements.in.list.dsource) {
  length <- length(dsource.split[[element]])
  
  i <- length + 1
  max.length.dsource2 <- max.length.dsource + 1
  while (i < max.length.dsource2) {
    dsource.split[[element]][i] <- ""   
    i <- i + 1 
  }
  
}

# make the data into a dataframe
dsource.split <- do.call(rbind,dsource.split)
dsource.split <- as.data.frame(dsource.split)

# rename the variables something helpful
variablenames <- names(dsource.split)

for (i in 1:max.length.dsource) { 
  variablenames[i] <- gsub("V", "deaths_source-", variablenames[i])     
}
names(dsource.split) <- variablenames  

# D3. population: split the sources for the censuses used for the population
# the years were already split up        
# there are some population sources that have spaces in it, which will make this difficult; 
# take out the spaces in those sources
# if there are more like this, the code will break, and you'll need to add that new pop source to the SRS report in the 
# next line below duplicate that for deaths sources as well
citations$pop_source <- gsub("2010 SRS report", "2010_SRS_report", citations$pop_source)
citations$pop_source <- gsub("2011 SRS report", "2011_SRS_report", citations$pop_source)
citations$pop_source <- gsub("SRS Report", "SRS_report", citations$pop_source)
citations$pop_source <- gsub("2010 census tables","2010_census_tables",citations$pop_source)          
citations$pop_source <- gsub("2002 CHN Statistical Yearbook","2002_CHN_Statistical_Yearbook",citations$pop_source)            

popsource.split <- strsplit(citations$pop_source, " ")

# Need to make sure that the pop source list is only 2 elements wide (only 2 pop years ever used for a given point)
# if it's not. then it's because something like what happened with the spaces in "2010 SRS report" is happening with 
# another source
max.length.popsource <- max(sapply(popsource.split, length)) 
if(max.length.popsource != 2){
  print("you'll probably get this error: Error: no loop for break/next, jumping to top level")
  print("there is something wrong with spaces in pop source; make sure that there are no extra spaces in any pop source")
  print("extra spaces in pop source will yield incorrect splitting of the pop sources")
  print("try table(citations$pop_source) to figure out where there are extra spaces in the source")
  stop("see error statement above")
} else 
  if (max.length.popsource == 2) { 
    print("the pop sources split correctly")
  }

# make the data into a dataframe
popsource.split <- do.call(rbind,popsource.split)
popsource.split <- as.data.frame(popsource.split)

# rename the variables something helpful
variablenames <- names(popsource.split)

for (i in 1:max.length.popsource) { 
  variablenames[i] <- gsub("V", "pop_source-",variablenames[i])     
}
names(popsource.split) <- variablenames  

# D4. merge on the deaths_source and deaths_years variables and reformat so the data are long
# this will help create years and source combinations for citations
num.citations <- nrow(citations)
citations <- cbind(citations,dyears.split)  
citations <- cbind(citations, dsource.split)  

citations <- reshape(citations,
                     direction = "long",
                     idvar = c("iso3","year","year1", "year2","sex","type_id","pop_source","deaths_source"),
                     varying = list(names(dyears.split),names(dsource.split)),
                     v.names = c("deaths_year", "deaths_source2"),
                     timevar = "deaths_number",
                     sep = "-")  

row.names(citations) <- NULL 

if (num.citations*max.length.dyears != nrow(citations)) {
  stop("the reshape yielded too many or too few observations; didn't equal number of observations in the citations dataset times the number of deaths_years variables")
  
}else if (num.citations*max.length.dyears == nrow(citations)) {
  print("the reshape was successful!")
}

citations$deaths_years <- NULL
citations <- rename(citations, c(deaths_source2 = "deaths_source2"))
temp.num <- nrow(citations)

# D5. merge on the pop sources, and reformat long
citations <- cbind(citations, popsource.split)
citations <- rename(citations, c(year1 = "year-1", year2 = "year-2"))  
yearvariables <- c("year-1", "year-2")               

citations <- reshape(citations,
                     direction = "long",
                     idvar = c("iso3","sex","type_id", "year","deaths_number","deaths_year","deaths_source"),
                     varying = list(names(popsource.split),yearvariables),
                     v.names = c("pop_source2","popyear"),
                     timevar = "pop_number",
                     sep ="-")

if (temp.num*2 != nrow(citations)) {
  stop("the reshape yielded too many or too few observations; didn't equal number of observations in the citations dataset times the number of pop years (which is always 2)")
  
}else if (temp.num*2 == nrow(citations)) {
  print("the reshape was successful!")
}                            

row.names(citations) <- NULL 

# E. create citations for pop and deaths
citations$pop_source <- citations$deaths_source <- NULL
citations <- rename(citations, c(deaths_source2 = "deaths_source", pop_source2 = "pop_source"))

# E1. order the data in a helpful way to see what we're dealing with
# drop the observations for which deaths_year and deaths_source are missing, because these observations will not contain 
# helpful information; they are empty placeholders from the reshape that are no longer needed
citations <- citations[order(citations$iso3,citations$year, citations$deaths_number, citations$pop_number),]
citations$deaths_number <- citations$pop_number <- NULL

citations <- citations[citations$deaths_year != "" & citations$deaths_source != "",] 

# E2. with the remaining data, create helpful citations 
variables <- c("deaths_source", "pop_source")
for (variable in variables) {
  citations[,variable] <- as.character(citations[,variable])      
}

# E2a. edit the deaths sources so that they are helpful in terms of citations
citations$deaths_source[citations$deaths_source %in% c("2010_SRS_report", "SRS_report", "SRS report", "2011 SRS report")] <- "Report"
citations$deaths_source[citations$deaths_source == "SRS_LIBRARY"] <- "Sample Registration System"            
citations$deaths_source[citations$deaths_source %in% c("3rdNatlDSP","CHINA_DSP_LOZANO", "CHN_DSP")] <- "China Disease Surveillance Points"
citations$deaths_source[citations$deaths_source %in% c("WHO_internal", "WHO Danzhen You via Mie Inoue")] <- "World Health Organization Mortality Data. [Unpublished]"              
citations$deaths_source[citations$deaths_source %in% c("AL") & citations$iso3 == "FJI" & citations$type_id == 1] <- "Bureau of Statistics, Ministry of Health"
citations$deaths_source[citations$deaths_source %in% c("WHO", "WHO_causes", "WHO_causesofdeath")] <- "World Health Organization Mortality Database"
citations$deaths_source[citations$deaths_source %in% c("CENSUS", "Census table", "CHN_PROV_CENSUS", "Ethiopia 2007 Census") | (citations$deaths_source == "OECD" & citations$type_id == 5)] <- "Census"            
citations$deaths_source[citations$deaths_source == "Census report"] <- "Report"               
citations$deaths_source[citations$deaths_source == "VR" & citations$iso3 == "ARE"] <- "National Bureau of Statistics"    
citations$deaths_source[citations$deaths_source == "TurkStat_Tabs_MERNIS_data"] <- "Statistical Institute"
citations$deaths_source[citations$deaths_source == "HMD"] <- "Human Mortality Database. University of California, Max Planck Instit. for Demographic Research"   
citations$deaths_source[citations$deaths_source == "IPUMS_HHDEATHS"] <- "Integrated Public Use Microdata Series, International. University of Minnesota"                               
citations$deaths_source[citations$deaths_source == "DYB"] <- "UN Statistical Division Demographic Yearbook"
citations$deaths_source[citations$deaths_source == "AD"] <- "General Register Office for Scotland, Northern Ireland Statistics and Research Agency, Office for National Statistics (United Kingdom)."
citations$deaths_source[citations$deaths_source == "2002 CHN Statistical Yearbook"] <- "Statistical Yearbook"
citations$deaths_source[citations$deaths_source == "12146"] <- "South Africa Population and Housing Census"
print("These are the deaths sources that are in use; are they all full citations to the best of your ability? Keep in mind length limit will be imposed (total citation + year list is less than 244 char)")
sort(unique(citations$deaths_source))

# E2b. add the source type to the source for deaths
citations$deaths_source[citations$type_id == 1] <- paste(citations$deaths_source[citations$type_id == 1], ": Vital Registration", sep= "")              
citations$deaths_source[citations$type_id == 18] <- paste(citations$deaths_source[citations$type_id == 18], ": Vital Registration", sep= "")
citations$deaths_source[citations$type_id == 19] <- paste(citations$deaths_source[citations$type_id == 19], ": Vital Registration", sep= "")
citations$deaths_source[citations$type_id == 23] <- paste(citations$deaths_source[citations$type_id == 23], ": Vital Registration", sep= "")
citations$deaths_source[citations$type_id == 24] <- paste(citations$deaths_source[citations$type_id == 24], ": Vital Registration", sep= "")                            
citations$deaths_source[citations$type_id == 2 & citations$deaths_source != "Sample Registration System"] <- paste(citations$deaths_source[citations$type_id == 2  & citations$deaths_source != "Sample Registration System"], ": Sample Registration System", sep= "")              
citations$deaths_source[(citations$type_id %in% c(3,20,21,22)) & citations$deaths_source != "China Disease Surveillance Points"] <- paste(citations$deaths_source[(citations$type_id %in% c(3,20,21,22)) & citations$deaths_source != "China Disease Surveillance Points"], ": Disease Surveillance Points", sep= "")
citations$deaths_source[citations$type_id == 5 & !(grepl("Census",citations$deaths_source))] <- paste(citations$deaths_source[citations$type_id == 5 & !(grepl("Census",citations$deaths_source))], ": Census", sep= "")   

# E2c. add the source type to the source for pop
popsourcetype <- read.dta(d00filepath)
popsourcetype <- popsourcetype[,c("iso3","sex","year","source_type","pop_source")]
names(popsourcetype)[names(popsourcetype) == "year"] <- "popyear"

# drop subnational, when needed
if (dropsubnational == 1) {
  popsourcetype <- popsourcetype[!(popsourcetype$iso3 %in% subnational.list),]
  if (length(unique(popsourcetype$iso3)) != 185) {
    print("not all subnational data were dropped; you will get an error")
    print("check that all subnational isos for each of UK, MEX, IND, and CHN are included in the drop, or that national data for a country were added")
    stop("see error statement above")
  }else
    if (length(unique(popsourcetype$iso3)) == 185) {
      print("subnational points were dropped successfully")
    }
}else
  if (dropsubnational == 0) {
    print("not dropping subnational data")
  }    

# drop CHN national census 1990; we use the provincial one. this also creates duplicates by iso3-sex-popyear-source_type later on, if it's kept in 
# popsourcetype[duplicated(popsourcetype[,c("iso3","sex","popyear","source_type")]),]
popsourcetype <- popsourcetype[!(popsourcetype$iso3 == "CHN" & popsourcetype$popyear == 1990 & popsourcetype$source_type == "CENSUS_NOT_USABLE"),]                

# there are some duplicates by iso3-sex-popyear-pop_source
# these are BGD and PAK population points that have both SRS and Census pop_sources
# popsourcetype[duplicated(popsourcetype[,c("iso3","sex","popyear","pop_source")]),]
# eg: popsourcetype[(popsourcetype$iso3=="BGD" & popsourcetype$popyear == 1974) | (popsourcetype$iso3=="PAK" & popsourcetype$popyear == 1972),]
# combine that information
popsourcetype <- ddply(popsourcetype, .(iso3, sex, popyear, pop_source), summarize, paste(source_type, collapse = " & "))                  
numberoflastvariable <- dim(popsourcetype)[2]
names(popsourcetype)[numberoflastvariable] <- "pop_source_type"                  

# clean up the source type names and pop source names for merging purposes
popsourcetype$pop_source_type[grepl("DSP",popsourcetype$pop_source_type)] <- "China Disease Surveillance Points"
popsourcetype$pop_source_type[grepl("VR",popsourcetype$pop_source_type)] <- "Vital Registration"
popsourcetype$pop_source_type[grepl("SRS",popsourcetype$pop_source_type)] <- "Sample Registration System"   
popsourcetype$pop_source_type[grepl("CENSUS",popsourcetype$pop_source_type) | popsourcetype$pop_source_type == "2000_CENS_SURVEY"] <- "Census"
popsourcetype$pop_source_type[popsourcetype$pop_source_type %in% c("HOUSEHOLD", "DC", "MOH survey", "SSPC", "SURVEY", "SUSENAS", "SUPAS")] <- "Survey"

popsourcetype$pop_source <- gsub("2011 SRS report", "2011_SRS_report", popsourcetype$pop_source)
popsourcetype$pop_source <- gsub("2010 SRS report", "2010_SRS_report", popsourcetype$pop_source)
popsourcetype$pop_source <- gsub("SRS Report", "SRS_report", popsourcetype$pop_source) 

# do not want certain data in here
popsourcetype <- popsourcetype[popsourcetype$pop_source_type != "NOT_USABLE_MODELED_MEX",]

# merge this information back on, and make sure that no source types are missing
tempnumobs <- nrow(citations)
citations <- merge(citations,popsourcetype, by = c("iso3","sex","popyear","pop_source"), all.x = T)

# some are missing source types
citations$pop_source_type[is.na(citations$pop_source_type) & citations$pop_source == "AD"] <- "Vital Registration"
citations$pop_source_type[is.na(citations$pop_source_type) & citations$pop_source == "IPUMS"] <- "Census"
citations$pop_source_type[is.na(citations$pop_source_type) & citations$pop_source == "2010_census_tables"] <- "Census"                      
citations$pop_source_type[is.na(citations$pop_source_type) & citations$pop_source == "2002_CHN_Statistical_Yearbook"] <- "Census"                      
citations$pop_source_type[is.na(citations$pop_source_type) & citations$pop_source == "DYB_download" & citations$iso3 == "CHN" & citations$popyear == 1989] <- "Census"                                            
citations$pop_source_type[is.na(citations$pop_source_type) & citations$pop_source == "IND_SRS"] <- "Sample Registration System"

nummissing <- nrow(citations[is.na(citations$pop_source_type),])
if (nummissing == 0) {
  print("all pop sources have a type (you may see an error about NAs above; this is because there aren't any rows that are missing a source type")
} else {
  stop("error: not all pop sources have a type.  look for pop sources in both the citations and popsourcetype datasets for sources that have spaces in their names. you may also need to manually enter the pop source type")
  
}

if (nrow(citations) != tempnumobs) {
  stop("error: the merge was not successful; check for duplicates by iso3-sex-pop year-pop source")
  
} else {
  print("the merge was successful")
}

# edit the pop sources so that they are helpful in terms of citations
citations$pop_source[(citations$pop_source %in% c("2011_SRS_report","2010_SRS_report", "SRS_report"))] <- "Report"
citations$pop_source[citations$pop_source %in% c("SRS_LIBRARY", "IND_SRS")] <- "Sample Registration System"            
citations$pop_source[citations$pop_source %in% c("3rdNatlDSP", "CHINA_DSP_LOZANO", "CHN_DSP")] <- "China Disease Surveillance Points"
citations$pop_source[citations$pop_source == "KS" & citations$iso3 == "JPN" & citations$type_id == 1] <- "Ministry of Health and Welfare"              
citations$pop_source[citations$pop_source %in% c("CENSUS", "CHN_PROV_CENSUS", "2010_census_tables")] <- "Census"                          
citations$pop_source[citations$pop_source %in% c("2002_CHN_Statistical_Yearbook")] <- "Statistical Yearbook 2002"                   
citations$pop_source[citations$pop_source == "IPUMS"] <- "Integrated Public Use Microdata Series, International. University of Minnesota"                               
citations$pop_source[grepl("DYB",citations$pop_source)] <- "United Nations Statistical Division Demographic Yearbook"
citations$pop_source[citations$pop_source == "TurkStat_ADNKS"] <- "Statistical Institute"              
citations$pop_source[citations$pop_source == "CARICOM"] <- "Caribbean Community Secretariat"  
citations$pop_source[citations$pop_source == "CELADE"] <- "Latin American & Caribbean Demographic Center Population Division"    
citations$pop_source[citations$pop_source == "IBGE"] <- "Brazilian Institute of Geography and Statistics"  
citations$pop_source[citations$pop_source == "GOV_WEBSITE" & citations$iso3 == "LKA"] <- "Department of Census and Statistics"               
citations$pop_source[citations$pop_source == "Demographic_Research_Bulletin_2007"] <- "Demographic Research Bulletin 2007"  
citations$pop_source[citations$pop_source == "GEO_ONLINE" & citations$iso3=="GEO"] <- "National Statistics Office"  
citations$pop_source[citations$pop_source == "GOV" & citations$iso3=="SVK"] <- "Statistics Office"                    
citations$pop_source[citations$pop_source == "TONGASTATDEP"] <- "Department of Statistics"                   
citations$pop_source[citations$pop_source == "HMD"] <- "Human Mortality Database. U of California, Max Planck Instit. for Demog. Research"                     
citations$pop_source[citations$pop_source == "AD"] <- "Scotland Gen. Reg. Office, N. Ireland Statistics and Research Agency, UK Office for Natl Statistics"
citations$pop_source[citations$pop_source == "stats_south_africa"] <- "Statistics South Africa"
citations$pop_source[citations$pop_source == "133784#URY_NATIONAL_iNSTITUTE_OF_STATISTICS"] <- "National Institute of Statistics"
print("These are the population sources that are in use; are they all full citations to the best of your ability? Keep in mind length limit will be imposed (total citation + year list is less than 244 char)")
sort(unique(citations$pop_source))


# add the source type to the pop source
citations$pop_source[!(citations$pop_source %in% c("Census", "Sample Registration System", "China Disease Surveillance Points"))] <- paste(citations$pop_source[!(citations$pop_source %in% c("Census", "Sample Registration System", "China Disease Surveillance Points"))], ": ",citations$pop_source_type[!(citations$pop_source %in% c("Census", "Sample Registration System", "China Disease Surveillance Points"))], sep = "") 
citations$pop_source_type <- NULL

# E3. create a variable that will be the deaths citation (source: years; source: years), and then for pop citation          
# E3a. want to get the years that are consecutive to be connected with a hyphen.  do this to reduce string length
# first, get the years in sequence for a given observation
citations$deaths_year <- as.numeric(as.character(citations$deaths_year))
citations <- citations[order(citations$iso3,citations$year,citations$deaths_year,citations$sex),]
citations1.deaths <- ddply(citations, .(iso3, sex, type_id, deaths_source), summarize, all_years <- paste(unique(deaths_year), collapse = " "))
numberoflastvariable <- dim(citations1.deaths)[2]
names(citations1.deaths)[numberoflastvariable] <- "deaths_years"

countries <- unique(citations1.deaths$iso3)         

citations1.deaths$dyears <- NA
for (country in countries) {
  sexes <- unique(citations1.deaths$sex[citations1.deaths$iso3 == country])
  for (sex in sexes) {
    sources <- unique(citations1.deaths$deaths_source[citations1.deaths$iso3 == country & citations1.deaths$sex == sex])
    for (source in sources) {
      types <- unique(citations1.deaths$type_id[citations1.deaths$iso3 == country & citations1.deaths$sex == sex & citations1.deaths$deaths_source == source])
      for (type in types) {
        
        # keep only the country-sex-year-type of interest
        subset <- citations1.deaths$iso3 == country & citations1.deaths$sex == sex & citations1.deaths$type_id == type & citations1.deaths$deaths_source == source
                
        # get the list of all the years and figure out how many years are in that list
        yearslist <- as.list(citations1.deaths$deaths_years[subset])
        temp <- strsplit(yearslist[[1]], " ")
        numberofelementsplus <- nrow(as.data.frame(temp)) + 1
        
        
        i <- 1
        while (i < numberofelementsplus) {
          # get the individual dates and put them in a list. this will be used to look at each year above and below that date to determine consecutiveness
          if (i == 1) {
            stringstart <- 1
            stringend <- 4
            date <- as.numeric(substr(as.character(yearslist[[1]][1]),stringstart,stringend))
            
            datelist <- new.env()
            datelist$date[1] <- date
            datelist$startDate <- date
            datelist$endDate <- date
            datelist$result <- ""
            datelist <- as.list(datelist)
            
          }else
            if (i != 1) {
              stringend <- (i*4) + (i - 1)
              stringstart <- stringend-4
              datelist$date[i] <- as.numeric(substr(as.character(yearslist[[1]][1]),stringstart,stringend))
              
            }
          i <- i + 1      
          
        }
        # make the list of years in consecutive order, taking into account different scenarios
        i <- 1
        realnumberofelements <- numberofelementsplus -1
        while (i < numberofelementsplus) {
          if (realnumberofelements == 1) {
            datelist$result <- datelist$date[1]
          } else if (i == 1 & (datelist$date[1] == datelist$date[2]-1)) {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i] 
          } else if (i == 1 & (datelist$date[1] != datelist$date[2]-1)) {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i] 
            datelist$result <- datelist$endDate
          } else if ((i != realnumberofelements & datelist$date[i] == datelist$date[i-1]+1) & (datelist$date[i] == datelist$date[i+1]-1)) {
            datelist$endDate <- datelist$date[i]
            
          } else if ((i != realnumberofelements & datelist$date[i] == datelist$date[i-1]+1) & (datelist$date[i] != datelist$date[i+1]-1)) {
            datelist$endDate <- datelist$date[i]
            if (datelist$result != "") {
              datelist$result <- paste(datelist$result, ", ", datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else {
              datelist$result <- paste(datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            }
          } else if ((i != realnumberofelements & datelist$date[i] != datelist$date[i-1]+1) & (datelist$date[i] == datelist$date[i+1]-1)) {
            datelist$startDate <- datelist$date[i]
            
          } else if ((i != realnumberofelements & datelist$date[i] != datelist$date[i-1]+1) & (datelist$date[i] != datelist$date[i+1]-1)) {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i]
            datelist$result <- paste(datelist$result, ", ", datelist$endDate, sep="")
            
          } else if ((i == realnumberofelements) & (datelist$date[i] == datelist$date[i-1]+1)) {
            datelist$endDate <- datelist$date[i]
            if (datelist$result == "") {
              datelist$result <- paste(datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else if (datelist$result != "" & datelist$startDate != datelist$endDate) {
              datelist$result <- paste(datelist$result, ", ", datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else {
              datelist$result <- paste(datelist$result, ", ",datelist$endDate,sep="")
            } 
          } else {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i] 
            if (datelist$result == "") {
              datelist$result <- paste(datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else {
              datelist$result <- paste(datelist$result, ", ",datelist$endDate,sep="")
            }
          }
          
          i <- i +1
          
          
        }
        citations1.deaths$dyears[subset] <- datelist$result 
      }
    }
  }
}


# deaths source citations
citations1.deaths$deaths_citation <- paste(citations1.deaths$deaths_source, citations1.deaths$dyears,sep=" ")

# E3b. get pop years so that they are consecutive
citations2.pop <- ddply(citations, .(iso3, sex, type_id,pop_source), summarize, all_years <- paste(unique(popyear), collapse = " "))
numberoflastvariable <- dim(citations2.pop)[2]
names(citations2.pop)[numberoflastvariable] <- "pop_years"  

# make the years that are consecutive look cleaner
countries <- unique(citations2.pop$iso3)         

citations2.pop$pyears <- NA
for (country in countries) {
  sexes <- unique(citations2.pop$sex[citations2.pop$iso3 == country])
  for (sex in sexes) {
    sources <- unique(citations2.pop$pop_source[citations2.pop$iso3 == country & citations2.pop$sex == sex])
    for (source in sources) {
      types <- unique(citations2.pop$type_id[citations2.pop$iso3 == country & citations2.pop$sex == sex & citations2.pop$pop_source == source])
      for (type in types) {
        
        # keep only the country-sex-year-type of interest
        subset <- citations2.pop$iso3 == country & citations2.pop$sex == sex & citations2.pop$type_id == type & citations2.pop$pop_source == source
                
        # get the list of all the years and figure out how many years are in that list
        yearslist <- as.list(citations2.pop$pop_years[subset])
        temp <- strsplit(yearslist[[1]], " ")
        numberofelementsplus <- nrow(as.data.frame(temp)) + 1
        
        
        i <- 1
        while (i < numberofelementsplus) {
          # get the individual dates and put them in a list. this will be used to look at each year above and below that date to determine consecutiveness
          if (i == 1) {
            stringstart <- 1
            stringend <- 4
            date <- as.numeric(substr(as.character(yearslist[[1]][1]),stringstart,stringend))
            
            datelist <- new.env()
            datelist$date[1] <- date
            datelist$startDate <- date
            datelist$endDate <- date
            datelist$result <- ""
            datelist <- as.list(datelist)
            
          }else
            if (i != 1) {
              stringend <- (i*4) + (i - 1)
              stringstart <- stringend-4
              datelist$date[i] <- as.numeric(substr(as.character(yearslist[[1]][1]),stringstart,stringend))
              
            }
          i <- i + 1      
          
        }
        # make the list of years in consecutive order, taking into account different scenarios
        i <- 1
        realnumberofelements <- numberofelementsplus -1
        while (i < numberofelementsplus) {
          if (realnumberofelements == 1) {
            datelist$result <- datelist$date[1]
          } else if (i == 1 & (datelist$date[1] == datelist$date[2]-1)) {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i] 
          } else if (i == 1 & (datelist$date[1] != datelist$date[2]-1)) {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i] 
            datelist$result <- datelist$endDate
          } else if ((i != realnumberofelements & datelist$date[i] == datelist$date[i-1]+1) & (datelist$date[i] == datelist$date[i+1]-1)) {
            datelist$endDate <- datelist$date[i]
            
          } else if ((i != realnumberofelements & datelist$date[i] == datelist$date[i-1]+1) & (datelist$date[i] != datelist$date[i+1]-1)) {
            datelist$endDate <- datelist$date[i]
            if (datelist$result != "") {
              datelist$result <- paste(datelist$result, ", ", datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else {
              datelist$result <- paste(datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            }
          } else if ((i != realnumberofelements & datelist$date[i] != datelist$date[i-1]+1) & (datelist$date[i] == datelist$date[i+1]-1)) {
            datelist$startDate <- datelist$date[i]
            
          } else if ((i != realnumberofelements & datelist$date[i] != datelist$date[i-1]+1) & (datelist$date[i] != datelist$date[i+1]-1)) {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i]
            datelist$result <- paste(datelist$result, ", ",datelist$endDate,sep="")
            
          } else if ((i == realnumberofelements) & (datelist$date[i] == datelist$date[i-1]+1)) {
            datelist$endDate <- datelist$date[i]
            if (datelist$result == "") {
              datelist$result <- paste(datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else if (datelist$result != "" & datelist$startDate != datelist$endDate) {
              datelist$result <- paste(datelist$result, ", ", datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else {
              datelist$result <- paste(datelist$result, ", ",datelist$endDate,sep="")
            } 
          } else {
            datelist$startDate <- datelist$date[i]
            datelist$endDate <- datelist$date[i] 
            if (datelist$result == "") {
              datelist$result <- paste(datelist$startDate, "-", substr(as.character(datelist$endDate),3,4),sep="")
            } else {
              datelist$result <- paste(datelist$result, ", ",datelist$endDate,sep="")
            }
          }
          
          i <- i +1
          
          
        }
        citations2.pop$pyears[subset] <- datelist$result 
      }
    }
  }
}

# pop source citation
citations2.pop$pop_citation <- paste(citations2.pop$pop_source, citations2.pop$pyears,sep=" ") 

# E3c. if there are multiple sources per iso3-sex-type, bring them together
citations1.deaths <- ddply(citations1.deaths, .(iso3, sex, type_id), summarize, paste(unique(deaths_citation),collapse = "; "))
numberoflastvariable <- dim(citations1.deaths)[2]
names(citations1.deaths)[numberoflastvariable] <- "deaths_citation"

citations2.pop <- ddply(citations2.pop, .(iso3, sex, type_id), summarize, paste(unique(pop_citation),collapse = "; "))
numberoflastvariable <- dim(citations2.pop)[2]
names(citations2.pop)[numberoflastvariable] <- "pop_citation"    

if (nrow(citations1.deaths) != nrow(citations2.pop)) {
  stop("error: something went wrong, and we have different numbers of observations for deaths and pop citations; do not merge")  
  
} else {
  print("it's okay to merge")
}

# first, bring the 2 citations data sets together
citations <- merge(citations1.deaths, citations2.pop, by = c("iso3","sex","type_id"))  
if (nrow(citations) != nrow(citations2.pop)) {
  stop("error: something went wrong in merging deaths and pop citations; ended up with too many or too few observations")  
  
} else {
  print("merging deaths and pop citations was successful")
}                

# then, make sure that there are no duplicates by iso3-sex-type
if (length(nrow(duplicated(citations))) != 0) {
  stop("there are duplicates by iso3-sex-type; citations are NOT unique by these variables")
  
} else {
  print("there are no duplicates by iso3-sex-type; citations are unique by these variables")
}

# E3d. make sure that the citations are not too long
citations$dcit_length <- str_length(citations$deaths_citation)
citations$dcit_toolong[citations$dcit_length > 244] <- 1

citations$pcit_length <- str_length(citations$pop_citation)
citations$pcit_toolong[citations$pcit_length > 244] <- 1

if (nrow(citations[is.na(citations$pcit_toolong),]) != nrow(citations)) {
  print("error: some pop citations are too long; use the pcit_length variable to investigate which these are")
  print("then, fix it in the pop source citations sections where the sources are made into longer citations; shorten the relevant ones")                
  stop("see error statement above")
} else if (nrow(citations[is.na(citations$dcit_toolong),]) != nrow(citations)){
  print("error: some deaths citations are too long; use the dcit_length variable to investigate which these are")
  print("then, fix it in the deaths source citations sections where the sources are made into longer citations; shorten the relevant ones")                
  stop("see error statement above")            
} else {
  print("all citations are less than 244 characters")
  citations$dcit_length <- citations$dcit_toolong <- citations$pcit_length <- citations$pcit_toolong <- NULL
}

# E3e. cget the number of observations so we can use this as a check when merging back onto the ddm data  
numbercitations <- nrow(citations)      

# E4. Add on the components to later make source_id
# do this by doing the following:
#Assign each iso3 a number
#Assign each sex a number
#Assign each type a number (actually, this is already done)
#Merge that information onto the citations data
#Add these numbers up to get a unique source_id for a country-sex-type citations combination. Start at the max of the current source_ids

# E4a. iso3 unique numbers
countries <- citations[,c("iso3")]
countries <- as.data.frame(countries[!duplicated(countries)])
numberofcountries <- nrow(countries)
rownameslist.countries <- as.character(c(1:numberofcountries))
for (rownum in 1:numberofcountries) {
  countries$iso3number[rownum] <- rownameslist.countries[rownum]
}
names(countries)[names(countries) == "countries[!duplicated(countries)]"] <- "iso3"

citations <- merge(citations, countries, by = "iso3", all.x = T)

# E4b. sex unique number
sex <- c("male", "female", "both")
sexnumber <- c(1,2,3)
sexes <- as.data.frame(cbind(sex,sexnumber))

citations <- merge(citations, sexes, by = "sex", all.x=T)


# E5. merge the citations onto the DDM data        
# to make sure that observations aren't dropped in the append with the idie data, get the number of observations in the ddm data
# the citations only go on the adult data
data$pop_years <- data$year1 <- data$year2 <- data$estyear <- data$index <- NULL   
data.kids <- data[data$method_id == 6,]
data.nokids <- data[data$method_id != 6,]

if (nrow(data) != nrow(data.kids)+nrow(data.nokids)) {
  stop("separating the ggb/seg/ggbdseg from u5 didn't work; figure out why")
  
}else
  if (nrow(data) == nrow(data.kids)+nrow(data.nokids)) {
    print("separating the ggb/seg/ggbdseg from u5 worked")
  }

mergeddata <- merge(data.nokids, citations, all.x=T, by = c("iso3","type_id","sex")) 

# make sure number of observations make sense
if (nrow(mergeddata) == nrow(data.nokids)) {
  print("merge of ddm data and citations was successful")
} else {
  stop("error: merge of ddm data and citations was not successful")
  
}


# bring the adult and kid data back together  
data.kids$deaths_citation <- NA
data.kids$pop_citation <- NA    
data.kids$iso3number <- NA
data.kids$sexnumber <- NA
data2 <- rbind(data.kids, mergeddata)

# make sure the the number of observations wasn't lost
if (nrow(data2) == nrow(data)) {
  print("no observations were lost when adding citations to the adult ddm points, and adding back in the kids points")
} else {
  stop("error: no observations were lost when adding citations to the adult ddm points, and adding back in the kids points")
  
}

num.obs.ddm.with.citations <- nrow(data2)

#########################################
## bring the idie & ddm data together  
###########################################

finaldata <- data2

# B. check to make sure there are no subnational data
if (dropsubnational == 1) {
  isoswithxs <- unique(finaldata$iso3[grepl("X",finaldata$iso3)])
  if (length(isoswithxs) > 2) {
    stop("error: not all subnational dropped")
    
  } else
    if (length(isoswithxs) == 2) {
      print("all subnational dropped")
    }
} else {
  print("don't need to drop subnational")
}

# C. fill in the sourceid for ddm data: add up the maximum source_id that already exists+the unique numbers for iso3, sex, type, deaths citation and pop citation    
#   because certain combinations of unique iso3 number + unique sex number + unique type number etc could yield the same number even if the underlying source is different,
#   make it so that the iso3 number is always in the 1000s place, the sex number is always in the 100s place, and the type number is always in the 1s place
#   don't need to have unique numbers for each deaths and pop citations because they're unique for iso3-sex-type in DDM
finaldata$maxsource_id <- as.numeric(max(finaldata$source_id[!(is.na(finaldata$source_id))&finaldata$process!= "ddm"]))
finaldata$iso3number <- as.numeric(finaldata$iso3number)
finaldata$sexnumber <- as.numeric(finaldata$sexnumber)

subsetforddmsourceid <- finaldata$process=="ddm" & !(is.na(finaldata$iso3number)) & finaldata$method_id!=6 
finaldata$source_id[subsetforddmsourceid] <- as.numeric(finaldata$maxsource_id[subsetforddmsourceid]+(finaldata$iso3number[subsetforddmsourceid]*1000)+(finaldata$sexnumber[subsetforddmsourceid]*100)+(finaldata$type_id[subsetforddmsourceid]*1))

# D. drop unnecessary variables 
finaldata$maxsource_id <- finaldata$iso3number <- finaldata$sexnumber <- NULL

# E. drop SAU "both" VR data points, keep census
finaldata <- finaldata[!(finaldata$iso3=="SAU" & finaldata$process=="ddm" & finaldata$sex == "both" & finaldata$type_id==1),]

#F. add in China VR data for the map
## We do this before in this code, so we don't need to do it again

# save    
date <- Sys.Date()
write.csv(finaldata, savefilepath, row.names=F)
write.csv(finaldata, archivedfilepath, row.names=F)

