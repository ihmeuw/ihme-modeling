## CLEAR ENV
rm(list = ls())

## ESTABLISH FOCAL DRIVES
j <-"filepath"
h <-"filepath"
k <-"filepath"

## GET SHARED FUNCTION
source(paste0(k, "filepath/get_location_metadata.R"))
source(paste0(k, "filepath/get_bundle_data.R"))
source(paste0(k, "filepath/save_bundle_version.R"))
source(paste0(k, "filepath/get_bundle_version.R"))
source(paste0(k, "filepath/save_crosswalk_version.R"))
source(paste0(k, "filepath/upload_bundle_data.R"))
source(paste0(k, "filepath/get_outputs.R"))
source(paste0(k, "filepath/get_age_metadata.R"))
source(paste0(k, "filepath/get_population.R"))

## INSTALL PACKAGES
library(openxlsx)
library(zoo)
library(writexl,lib.loc = "/filepath")
library(tidyverse, lib.loc = "/filepath")
## DATA
start<- 2010
years <- seq(start,2021,1)
hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
countries <- hierarchy[level == 3, location_id]
## read in cause data
cause <- "flu"
if (cause == "measles"){
  prelim_jrf_data <- paste0(j, "/filepath/measlescasesbycountrybymonth.xlsx")
  monthly_prelim_jrf <- as.data.table(read.xlsx(prelim_jrf_data, sheet= "WEB"))

  setnames(monthly_prelim_jrf, c("ISO3", "Year"), c("ihme_loc_id", "year_id"))

  dat <- monthly_prelim_jrf[year_id %in% years]
  dat[, c("Region", "Country") := NULL]

  # melt so have one column of month and one of cases reported
  my_data <- melt(dat, id.vars = c("ihme_loc_id", "year_id"), value.name = "cases", variable.name = "month")
  expand<- expand.grid(ihme_loc_id = unique(my_data$ihme_loc_id), year_id = 2021, month = unique(my_data$month),
                       cases = 0)
  my_data <- rbind(my_data,expand)
} else if (cause == "flu"){
  # load flu
  # average weekly flu cases in a month
  flu_monthly <- fread(paste0(j,"filepath/aggregated_weekly_avg_2010_to_2021.csv"))
  flu_monthly <- flu_monthly[,-c("super_region_id", "super_region_name")]
  my_data<- melt(flu_monthly, id.vars = c("ihme_loc_id", "month", "location_id"), value.name = "cases", variable.name = "year")
  setnames(my_data,'year','year_id')
  my_data$year_id <- as.integer(as.character(my_data$year_id))
  my_data <- my_data[,.(ihme_loc_id, year_id, month, cases)]
}
my_data[, cases := as.numeric(cases)]

## Merge with hierarchy
my_data <- merge(my_data, hierarchy[,.(ihme_loc_id, location_id, super_region_name, region_name)], by="ihme_loc_id")

## DIFFERENT OFFSET OPTIONS
## OFFSET1: Annual Total Cases
# my_data[, annual_total := sum(cases, na.rm = T), by = c("location_id", "year_id")]
# my_data$annual_total[my_data$year_id==2020] <- NA
# ## OPTION 1: USE 2019 ANNUAL TOTAL CASES TO BE 2020 OFFSET
# my_data$annual_total<- na.locf(my_data$annual_total)
#
# ## OPTION 2: Three year average of total cases (from previous three years before 2020)
# total3 <- my_data[year_id %in% c(2017,2018,2019),]
# total3[, annual_total3 := sum(cases, na.rm = T), by = c("location_id")]
# total3[, annual_total2 := annual_total3/3][,annual_total:=NULL][,cases:=NULL]
# total3[,annual_total3 := NULL]
# total3$year_id <- 2020
# total3 <- unique(total3)

## MERGE DATA
# dt1 <- merge(my_data,total3[,c("ihme_loc_id","year_id","month", "annual_total2","location_id")], by = c("location_id","ihme_loc_id","year_id" ,"month"), all.x = T)
# dt1[is.na(dt1$annual_total2),"annual_total2"] <- dt1[is.na(dt1$annual_total2),"annual_total"]
# my_data <- dt1

## create a variable to identify real 0s in 2020
my_data$real_0_2020 <- 0
my_data$real_0_2020[my_data$cases==0 & my_data$year_id %in% c(2020,2021)] <- 1
my_data$real_0 <- 0
my_data$real_0[my_data$cases==0 ] <- 1
########################################## OPTION 1 ####################################
## DATA CLEANING PROCESS
## USE DATA UNTIL FEB 2020

## MERGE POPOLATION AND USE 2019 POP AS 2020/2021 POP
dt3 <- my_data
dt3$year <- dt3$year_id
dt3$year[dt3$year %in% c(2020,2021)] <- 2019
## PULL POPS
pops <- get_population(location_id  = unique(my_data$location_id),
                       year_id      = seq(start,2021,1),
                       sex_id       = 3,
                       age_group_id = 22,
                       decomp_step  = "iterative",
                       gbd_round_id = 7)
pops[,sex_id:=NULL][,run_id:=NULL][,age_group_id:=NULL]
pops$year <- pops$year_id
pops[,year_id :=NULL]

dt4 <- merge(dt3,pops, by = c("location_id","year"))
dt4[,year:=NULL]

my_data <- dt4

## FORMAT DATA
my_data[, month := as.integer(match(month, month.name))]
my_data[, date := as.Date(paste0("2020-", month, "-15"))]
my_data$sex <- "all"
my_data[, age_start:=0][,age_end:=125][,age_name:="0 to 125"]

## CHANGE CASES TO DEATHS SINCE DEATHS IS THE DEFAULT VARIABLE IN REGMOD
setnames(my_data,"cases","deaths")

###############################
## IMPLEMENT 1% TAIL OF AVERAGE YEARLY CASES & 20% MISSING DATA TO HAVE THE FINAL FLU LOC INCLUSION LIST
my_data[(my_data$year_id %in% c(2010:2019) | (my_data$year_id==2020 & my_data$month %in% c(1,2))), annual_yearly_average := mean(deaths, na.rm = T), by = c("location_id")]
m <- unique(my_data[is.na(annual_yearly_average) & (my_data$year_id %in% c(2010:2019) | (my_data$year_id==2020 & my_data$month %in% c(1,2))),"ihme_loc_id"])
## IMPORT DATA RICH LOCS
data_rich <- read.csv(paste0(j,"filepath/locations_four_plus_stars.csv"))
data_rich_locs<- data_rich[,c("ihme_loc_id")]
## 1. 1% TAIL OF AVERAGE YEARLY CASES
rich <- my_data[(my_data$ihme_loc_id %in% unique(data_rich$ihme_loc_id)) & !is.na(my_data$annual_yearly_average),]
quantile(rich$annual_yearly_average, c(.01, .05))

my_data2 <- my_data[annual_yearly_average<=1.0166667,]
df <- my_data[!(ihme_loc_id %in% unique(my_data2$ihme_loc_id))]

## 2. 20%% MISSING
missing <- my_data[!is.na(my_data$deaths) & (my_data$year_id %in% c(2010:2019) | (my_data$year_id==2020 & my_data$month %in% c(1,2)))]
dt8 <- missing %>%
  group_by(ihme_loc_id) %>%
  count()
locs3 <- dt8[dt8$n<98  ,"ihme_loc_id"]
df1 <- my_data[!(ihme_loc_id %in% c(locs3$ihme_loc_id))]
locs3 <- as.data.table(locs3)
locs7 <- unique(my_data2[,"ihme_loc_id"])
# RBIND THE 2 LOC LIST
loc_list <- rbind(locs3,locs7)
loc_list <- rbind(loc_list,m)
# THIS IS OUR EXCLUSION LIST
View(sort(unique(loc_list$ihme_loc_id)))

# THIS IS OUR INCLUSION
data_final1 <- my_data[!(ihme_loc_id %in% c(loc_list$ihme_loc_id))]
LocList <- unique(data_final1[,"ihme_loc_id"])
write.csv(LocList,"/filepath/loc_list_flu_new_data_98.csv",row.names = F)

## SAVE FINAL DATA BY LOCATIONS
final <- data_final1
for (i in c(unique(final$ihme_loc_id))) {
  df <- final[ihme_loc_id==i,]
  df1 <- df[order(year_id, month)]
  write.csv(df1, paste0(j,"/filepath/",cause, i,".csv"), row.names=F)
}

######################################################
### PLOT COMPARSION PLOT
library(dplyr)
library(tidyr)
library(gtools)
library(data.table); library(readstata13); library(ggplot2)
library(plyr)
library(assertable)
library(readr)
library(gridExtra)
library(grid)
library(ggplot2)
library(lattice)
source(paste0(k, "filepath/get_location_metadata.R"))

hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
## LAST KNOT PLACE IN 2017
cause <- "measles"
files <- list.files( path = paste0("/filepath",cause,"/Jan2017/Output/"), pattern = "*.csv", full.names = T)
dat_csv = ldply(files, read_csv)
flu_v1<-as.data.table(dat_csv)
flu_v1$source <- "Jan2017"
## LAST KNOT PLACE IN 2018
files <- list.files( path = paste0("/filepath",cause,"/Jan2018/Output/"), pattern = "*.csv", full.names = T)
dat_csv = ldply(files, read_csv)
flu_v2<-as.data.table(dat_csv)
flu_v2$source <- "Jan2018"
## LAST KNOT PLACE IN 2019
files <- list.files( path = paste0("/filepath/",cause,"/Jan2019/Output/"), pattern = "*.csv", full.names = T)
dat_csv = ldply(files, read_csv)
flu_v3<-as.data.table(dat_csv)
flu_v3$source <- "Jan2019"
## COMBINE DATA
flu <- do.call("rbind", list(flu_v1,flu_v2,flu_v3))

flu[, average := mean(cases, na.rm = T), by = c("location_id","year_id","month")]
avg <- unique(flu[,c(1:7,1015)])
avg$source <- "average"
setnames(avg,"average","cases")
write.csv(avg,paste0("/filepath/",cause,"/",cause,"_avg.csv"),row.names = F)
flu1 <- rbind.fill(flu,avg)
flu1 <- as.data.table(flu1)
flu1$time <- paste0(flu1$year_id,"_",flu1$month)

flu1[, date := as.Date(paste0(year_id,"-", month, "-15"))]

data <- merge(flu1,hierarchy[,c("ihme_loc_id","location_id","location_name")], by = "location_id")

for(l in unique(data$ihme_loc_id)){
  pdf(paste0(j,"/filepath/",cause,"/plot_avg/", l,".pdf"), height = 6, width = 12)
  gg<-ggplot(data = data[data$ihme_loc_id ==l,], aes(x = date)) +
    geom_point(data=data[data$ihme_loc_id == l,], aes(x= date, y=deaths), size = 1) +
    geom_vline(xintercept = seq(as.Date("2010-01-01"),as.Date("2020-01-01"), by = "1 year"), linetype="dotted",
               color = "gray", size=1) +
    geom_line(aes(y = cases, color = source, group = source)) +
    scale_x_date(date_breaks = "1 year", date_labels = "%Y") + theme_classic() +
    ggtitle(paste0(cause,": ",l))

  time_trend<-ggplot(data = data[data$ihme_loc_id ==l ,], aes(x = date)) +
    geom_vline(xintercept = seq(as.Date("2010-01-01"),as.Date("2020-01-01"), by = "1 year"), linetype="dotted",
               color = "gray", size=1) +
    geom_line(aes(y = time_trend, color = source, group = source)) +
    geom_point(data=data[data$ihme_loc_id == l,], aes(x= date, y=trend_residual, color = source), size = 1) +
    scale_x_date(date_breaks = "1 year", date_labels = "%Y") + theme_classic()


  plot <- grid.arrange(gg, time_trend, nrow = 2, ncol=1)
  print(plot)
  dev.off()
}



