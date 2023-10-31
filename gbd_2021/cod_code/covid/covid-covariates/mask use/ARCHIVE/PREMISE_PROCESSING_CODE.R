### collapse and plot individiual Premise covid data
### Written by Parkes Kendrick, Haley Lecsinsky and Erin Frame
# source("/ihme/code/covid-19/user/ctroeger/covid-beta-inputs/src/covid_beta_inputs/mask_use/PREMISE_PROCESSING_CODE.R")
### load libraries
library(readstata13)
library(data.table)
library(ggplot2)
library(gridExtra)
library(dplyr)
library(sf)
library(sp)
library(splines)
library(rgdal)
library(maps)
library(maptools)
library(parallel)


tmpinstall <- system("mktemp -d --tmpdir=/tmp", intern = TRUE)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("ihmeuw/ihme.covid", upgrade = "never")
library(ihme.covid)


## Data Directory
data_dir_m <- "//ihme/covid-19/snapshot-data/best/covid_onedrive/Decrees for Closures/Premise/"
data_dir_m <- "/home/j/Project/covid/data_intake/Premise_data/"
df <- file.info(list.files(data_dir_m, full.names = T, pattern="202."))
data_dir <- rownames(df)[which.max(df$mtime)]

#Other inputs
input_dir <- "/ihme/covid-19/model-inputs/best/static-data/mask_use"
n <- 5 #number of days for the moving average (not currently used)
CORES <- 6 # number of cores to use in processing

#outputs
output_dir <- ihme.covid::get_output_dir(
  root = "/ihme/covid-19/mask-use-outputs/",
  date = "today")

metadata_path <- file.path(output_dir, "metadata.yaml")

us.impact.file<-list.files(data_dir, pattern="us_covid")
#### PREMISE DATA SETS - only need "Impact" for Masks
us_premise_impact <-fread(paste0(data_dir,"/", us.impact.file))

#### Replace census shapefile with COVID shapefile
#tmp_dl <- tempfile()

ST<-readShapePoly("/ihme/covid-19/model-inputs/best/shapefiles/lsid_115/lsvid_723/best/covid.shp")
shapeCRS<-"+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"
proj4string(ST)<- CRS("+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0")


latlong2state <- function(pointsDF) {
  #Subset shapefile to US and remove national level
  states <- ST[ST$parent_id==102 & ST$loc_id!=102,]
  
  # Convert pointsDF to a SpatialPoints object
  # USE THE SAME CRS - PICKING ONE
  pointsCRS <- "+proj=longlat +datum=NAD83 +no_defs +ellps=GRS80 +towgs84=0,0,0"
  pointsSP <- SpatialPoints(pointsDF, proj4string=CRS(pointsCRS))
  
  # Use 'over' to get _indices_ of the Polygons object containing each point
  indices <- over(pointsSP, states)
  
  # Return the loc_id of the Polygons object containing each point - will merge with state name later
  as.vector(indices$loc_id)
}

make_state_impact <- function(i,us_premise_impact){
  points <- as.data.frame(cbind(x=us_premise_impact[i,lon],y=us_premise_impact[i,lat]))
  return(latlong2state(points))
}

states_impact <- mclapply(c(1:nrow(us_premise_impact)), make_state_impact, us_premise_impact, mc.cores=CORES)
states_impact_copy <- copy(states_impact)
us_premise_impact[,line_num := .I]
states_impact_dt <- as.data.table(as.character(states_impact))
setnames(states_impact_dt,"V1","loc_id")
us_premise_impact <- cbind(us_premise_impact,states_impact_dt)

## look at the NA values - a few more than the census bureau but less than 1% so okay
nrow(us_premise_impact[is.na(loc_id)])

states <- ST[ST$parent_id==102 & ST$loc_id!=102,]
dat<-states@data
dat<-dat[,c("loc_id", "loc_name")]
setnames(dat, "loc_name", "state")

us_premise_comb<-copy(us_premise_impact)
#this will drop those locations that did not map
us_premise_impact <- merge(us_premise_comb,dat, by="loc_id")

#Read in the Division and Region data to match by state
state_mapping<-fread(paste0(input_dir, "/state_division_region_map.csv"))
us_premise_impact<-merge(us_premise_impact, state_mapping, by="state")

### get the data ready for plotting
impact_mask_qs<-c("created","campaign_name", "when_you_leave_your_home_do_you_typically_wear_a_face_mask", "why_do_you_not_wear_a_mask_when_you_leave_your_home",
                  "lat", "lon","loc_id", "state", "Division", "Region")
us_impact <- us_premise_impact[,(impact_mask_qs),with=F]

us_impact[,date := as.Date(created)]
us_impact[,max_date := max(date)]

us_impact$mask_wear_yes_no<-ifelse(us_impact$when_you_leave_your_home_do_you_typically_wear_a_face_mask %in% c("yes_always", "yes_sometimes"), 1, 
                                   ifelse(us_impact$when_you_leave_your_home_do_you_typically_wear_a_face_mask=="no_never", 0, NA))
us_impact$mask_wear_cat<-ifelse(us_impact$when_you_leave_your_home_do_you_typically_wear_a_face_mask %in% c("yes_always"), 2,
                                
                                ifelse(us_impact$when_you_leave_your_home_do_you_typically_wear_a_face_mask=="yes_sometimes",1,
                                       ifelse(us_impact$when_you_leave_your_home_do_you_typically_wear_a_face_mask=="no_never", 0, NA)))


data<-copy(us_impact)
data$date<-as.Date(data$date)

#created a set of weeks going forward from 4/18/2020 through end of 2020 to make life easier, and then only keep
#weeks that align with the data - read in as .csv
weeks_all<-read.csv("/home/j/Project/covid/data_intake/Premise_data/weeks.csv")
weeks_all$date<-as.Date(weeks_all$date, format='%m/%d/%Y')
#prep dates as weeks backwards from most recent date! - switching order of the weeks now to make life easier going forward
max<-as.Date(max(data$date))
weeks<-weeks_all[weeks_all$date <= max,]

#questions
questions <- fread(file.path(input_dir, "/usa_questions_asked_new.csv"))
# Try with and without No response factoring into denominator!

## State-level data
prep_data_state <- function(data, questions, timevar = "date"){
  
  data[, date:=as.Date(created)]
  qs <- questions[use==1, question]
  
  plot <- data[, c(qs, "state","loc_id", "date", "campaign_name"), with=F]
  
  
  #identify numeric vs categorical
  numeric_cols <- c()
  for(c in qs){
    message(c)
    plot[get(c) == "No" | get(c) == "no", paste0(c) := 0]
    plot[get(c) == "Yes" | get(c) == "yes", paste0(c) := 1]
    plot[get(c) == "i_m_not_sure", paste0(c) := NA]
    
    if(length(setdiff(unique(plot[,get(c)]), c(0,1,"",NA,"NA"))) == 0){
      numeric_cols <- c(numeric_cols,c)
      plot[,paste0(c) := as.numeric(get(c))]
    }
  }
  
  id.vars <- c("state","loc_id","variable", timevar)
  alpha <- 0.05 # for the confidence intervals
  
  if(length(numeric_cols)>1){
    plot_melt_num <- melt.data.table(plot,id.vars=c("state","loc_id",timevar),measure.vars = numeric_cols)
    plot_melt_num[,proportion := mean(value,na.rm=T),by=id.vars]
    plot_melt_num[,national_proportion := mean(value,na.rm=T), by=c(timevar,"variable")]
    plot_melt_num$type <- "numeric"
  }else{ 
    plot_melt_num <- data.table()}
  if(length(setdiff(qs,numeric_cols))>1){
    plot_melt_char <- melt.data.table(plot,id.vars=c("state","loc_id",timevar),measure.vars = setdiff(qs,numeric_cols))
    plot_melt_char[,answer_count := .N, by=c(id.vars,"value")]
    plot_melt_char[,question_count := .N, by=c(id.vars)]
    plot_melt_char[,proportion := answer_count/question_count]
    # and then at the national level:
    plot_melt_char[,answer_count_nat := .N, by=c(timevar,"variable","value")]
    plot_melt_char[,question_count_nat := .N, by=c(timevar,"variable")]
    plot_melt_char[,national_proportion := answer_count_nat/question_count_nat]
    plot_melt_char$type <- "character"
    
  }else{ 
    plot_melt_char <- data.table()}
  
  total_melt <- rbind(plot_melt_char,plot_melt_num,fill=T)
  
  ###save summary stats
  plot_melt_char <- plot_melt_char[!is.na(value) & !is.na(state) & value!=""]
  plot_melt_char <- unique(plot_melt_char[order(state, get(timevar), variable, value)])
  write.csv(plot_melt_char, paste0(output_dir, "/summary_stats_by_",timevar,".csv"), row.names = F)
  
  
  #add on upper / lower
  ss_no_na <- copy(total_melt[!is.na(value)])
  ss_no_na[,ss := .N, by=id.vars]
  ss_no_na[,ss_nat := .N, by=c(timevar,"variable")]
  ss_no_na <- unique(ss_no_na[,.(ss,ss_nat,state, loc_id, date,variable)])
  total_melt <- merge(total_melt,ss_no_na,by=c("state","loc_id",timevar,"variable"),all.x=T)
  total_melt[,z := qnorm(1-alpha/2)]
  total_melt[,lower := proportion - z*sqrt(proportion*(1-proportion)/ss),by=id.vars]
  total_melt[,upper := proportion + z*sqrt(proportion*(1-proportion)/ss),by=id.vars]
  # same for the national level:
  total_melt[,lower_nat := national_proportion - z*sqrt(national_proportion*(1-national_proportion)/ss_nat),by=c(timevar,"variable")]
  total_melt[,upper_nat := national_proportion + z*sqrt(national_proportion*(1-national_proportion)/ss_nat),by=c(timevar,"variable")]
  total_melt[type == "numeric", value := 1]
  
  total_melt[,value:=gsub("_", " ", value)]
  total_melt[is.na(value) | value =="" | value =="i", value:="No response"]
  print(unique(total_melt$value))
  print("Use this to reorder")
  print(paste0(unique(total_melt$value), collapse = "','"))
  total_melt <- unique(total_melt[!is.na(state)])
  
  return(total_melt)
}


prep_data_moving_average_state <- function(data, questions, n=5){
  
  data[, date:=as.Date(created)]
  qs <- questions[use==1, question]
  
  plot <- data[, c(qs, "state","loc_id", "date", "campaign_name"), with=F]
  
  print("Only works for categorical variables")
  #identify numeric vs categorical
  numeric_cols <- c()
  for(c in qs){
    message(c)
    plot[get(c) == "No" | get(c) == "no", paste0(c) := 0]
    plot[get(c) == "Yes" | get(c) == "yes", paste0(c) := 1]
    plot[get(c) == "i_m_not_sure", paste0(c) := NA]
    
    if(length(setdiff(unique(plot[,get(c)]), c(0,1,"",NA,"NA"))) == 0){
      numeric_cols <- c(numeric_cols,c)
      plot[,paste0(c) := as.numeric(get(c))]
    }
  }
  
  id.vars <- c("state","loc_id","variable", "date")
  alpha <- 0.05 # for the confidence intervals
  
  if(length(numeric_cols)>1){
    plot_melt_num <- melt.data.table(plot,id.vars=c("state","loc_id","date"),measure.vars = numeric_cols)
    plot_melt_num[,proportion := mean(value,na.rm=T),by=id.vars]
    plot_melt_num[,national_proportion := mean(value,na.rm=T), by=c("date","variable")]
    plot_melt_num$type <- "numeric"
  }else{ 
    plot_melt_num <- data.table()}
  if(length(setdiff(qs,numeric_cols))>1){
    plot_melt_char <- melt.data.table(plot,id.vars=c("state","loc_id","date"),measure.vars = setdiff(qs,numeric_cols))
    plot_melt_char[,answer_count := .N, by=c(id.vars,"value")]
    plot_melt_char[,question_count := .N, by=c(id.vars)]
    plot_melt_char[,proportion := answer_count/question_count]
    
    plot_melt_char <-  unique(plot_melt_char[order(state,loc_id, date, variable, value)])
    
    # get lags of what is needed
    lag_answer_cols <- paste0("lag_answer_count", 1:(n-1))
    plot_melt_char[, paste0(lag_answer_cols):=data.table::shift(answer_count, n=1:(n-1), fill=first(answer_count)), by=c("state","loc_id", "variable","value")]
    lag_answer_cols <- c(lag_answer_cols, "answer_count")
    
    lag_question_cols <- paste0("lag_question_count", 1:(n-1))
    plot_melt_char[, paste0(lag_question_cols):=data.table::shift(question_count, n=1:(n-1), fill=first(question_count)), by=c("state","loc_id", "variable","value")]
    lag_question_cols <- c(lag_question_cols, "question_count")
    
    
    # sum across lags
    plot_melt_char[, total_answer_count:= rowSums(plot_melt_char[, lag_answer_cols, with=F]) ]
    plot_melt_char[, total_question_count:= rowSums(plot_melt_char[, lag_question_cols, with=F]) ]
    plot_melt_char[, avg_proportion := total_answer_count/total_question_count]
    
    
    plot_melt_char <- plot_melt_char[, .(state,loc_id, date, variable, value, total_answer_count, total_question_count, avg_proportion,answer_count ,question_count)]
    
    
    #ADD USA - decided not to do this
    # plot_melt_char[, nat_total_answer_count:=sum(total_answer_count), by=c("date","variable","value")]
    # plot_melt_char[, nat_total_answer_count:=sum(total_question_count), by=c("date","variable","value")]
    # plot_melt_char[, nat_avg_proportion:=sum(total_answer_count), by=c("date","variable","value")]
    # 
    
    plot_melt_char$type <- "character"
    
  }else{ 
    plot_melt_char <- data.table()}
  
  total_melt <- plot_melt_char
  
  
  #add on upper / lower
  # Parkes wrote this code, I'm unfamiliar with getting uncertainity using sample size...
  total_melt[,z := qnorm(1-alpha/2)]
  total_melt[,lower := avg_proportion - z*sqrt(avg_proportion*(1-avg_proportion)/total_question_count),by=id.vars]
  total_melt[,upper := avg_proportion + z*sqrt(avg_proportion*(1-avg_proportion)/total_question_count),by=id.vars]
  # same for the national level:
  # total_melt[,lower_nat := national_proportion - z*sqrt(national_proportion*(1-national_proportion)/ss_nat),by=c("date","variable")]
  # total_melt[,upper_nat := national_proportion + z*sqrt(national_proportion*(1-national_proportion)/ss_nat),by=c("date","variable")]
  # total_melt[type == "numeric", value := 1]
  
  total_melt[,value:=gsub("_", " ", value)]
  total_melt[is.na(value) | value =="" | value =="i", value:="No response"]
  print(unique(total_melt$value))
  print("Use this to reorder")
  print(paste0(unique(total_melt$value), collapse = "','"))
  total_melt <- unique(total_melt[!is.na(state)])
  
  return(total_melt)
}

prepped_data_state <- prep_data_state(data, questions)

prepped_avg_data_state <- prep_data_moving_average_state(data, questions, n=5)

#Set this every time for plotting!
order_levels <- c('No response', 'very unconcerned', 'unconcerned','neither concerned nor unconcerned','concerned','very concerned',
                  'no never','yes sometimes','yes always',
                  'i do not own a mask or know how to make one','i do not believe face masks are effective for preventing the spread of covid 19','face masks are uncomfortable','nobody else in my area wears a mask')


prepped_data_state[type=="character", order_value := factor(value, levels = c(order_levels))]


pdf(paste0(output_dir, "/time_trends_state.pdf"),height=8,width=12)

vars_to_plot <- unique(prepped_data_state[type == "character",variable])
for(v in vars_to_plot){
  message(v)
  
  unique_ss <- unique(prepped_data_state[type == "character" & variable == v,.(type,variable,ss_nat,date)])
  unique_ss$proportion <- 1.005
  p_char <- ggplot()+
    geom_bar(data=unique(prepped_data_state[type == "character" & variable == v, .(date,national_proportion, order_value)]),
             aes(x=date,y=national_proportion,fill=order_value),stat="identity")+
    geom_text(data=unique_ss,aes(x=date,y=proportion,label = ss_nat))+
    scale_fill_brewer(palette="Blues")+
    ggtitle(paste0("USA average","\n",gsub("_", " ", v), "?"))+theme_bw()+theme(legend.position="bottom")+
    labs(y="proportion", fill="")
  
  print(p_char)
  
  for(s in unique(prepped_data_state[variable==v]$state)){
    message(s)
    unique_ss <- unique(prepped_data_state[state == s & type == "character" & variable == v,.(state,type,variable,ss,date)])
    unique_ss$proportion <- 1.005
    p_char <- ggplot()+
      geom_bar(data=prepped_data_state[state == s & type == "character" & variable == v & date >= as.Date("2020-07-01")],
               aes(x=as.Date(date),y=proportion,fill=order_value),stat="identity")+
      #geom_text(data=unique_ss,aes(x=date,y=proportion,label = ss))+
      scale_fill_brewer(palette="Blues")+ xlab("Date") + 
      ggtitle(paste0(s,"\n",gsub("_", " ", v), "?"))+theme_bw()+theme(legend.position="bottom")+
      labs(y="Proportion", fill="")
    
    print(p_char)
  }
  
  values <- unique(prepped_avg_data_state[type == "character" & variable == v & value!="No response", value])
  
  for(val in values){
    #line plots
    p_char <- ggplot()+
      geom_line(data=prepped_avg_data_state[type == "character" & variable == v & value==val],
                aes(x=date,y=avg_proportion, color = state))+
      ylim(0,1)+
      ggtitle(paste0("All states","\n",gsub("_", " ", v), "?","\n", n,"-day proportion who responded '", val,"'"))+theme_bw()+theme(legend.position="bottom")+
      labs(y="proportion", color="")
    
    print(p_char)
    
  }
  for(s in unique(prepped_avg_data_state[variable==v]$state)){
    
    sub_data <- prepped_avg_data_state[state == s & type == "character" & variable == v & value!="No response"]
    avg_n <- mean(sub_data$question_count)
    
    
    p_char <- ggplot(data=sub_data)+
      #geom_line(aes(x=date,y=avg_proportion, color = paste0(n,"-day proportion who responded '", val,"'")))+
      geom_line(aes(x=date,y=avg_proportion, color = value))+
      #geom_ribbon(aes(x = date, ymin = lower, ymax=upper), alpha=0.3, color="grey")+
      ylim(0,1)+
      ggtitle(paste0(s,"\n",gsub("_", " ", v), "?","\n", "Avg daily respondents: ", round(avg_n)))+theme_bw()+
      labs(y="proportion", color=paste0(n,"-day proportion who responded"))
    
    print(p_char)
    
  }
  
  
}   


dev.off()

#focus only on quantifying wear, not reasons why ATM:
prep_data<-prepped_data_state[prepped_data_state$variable=="when_you_leave_your_home_do_you_typically_wear_a_face_mask",]
prep_data$new_val<-ifelse(prep_data$value %in% c('yes always', 'yes sometimes'), "yes", prep_data$value)

#merge weeks and aggregate to week level - remove no responses (should be very few ~2 after 4/15 or so)
prep_data_w<-merge(prep_data, weeks, by="date", all.x=TRUE)
prep_data2<-prep_data_w[!is.na(prep_data_w$week) &prep_data_w$value!="No response",]

#create denominator separately to prevent duplication with "yes any"
prep_data_denom<-prep_data2[!duplicated(prep_data2[,c("state","date")]),c("state","loc_id","date","question_count","week")]
prep_data_denom_w<-aggregate(question_count~week+state+loc_id, data=prep_data_denom, FUN=sum)
prep_data_always<-subset(prep_data2, prep_data2$value=="yes always")
prep_data_always_w<-aggregate(answer_count~week+state+loc_id, data=prep_data_always, FUN=sum)

prep_data_yes <- subset(prep_data2, prep_data2$new_val=="yes")

prep_data_yes_w <- aggregate(answer_count~week+state+loc_id, data=prep_data_yes, FUN=sum)
weekly_data_allx <- merge(prep_data_yes_w, prep_data_denom_w, by=c("state","loc_id", "week"))
setnames(weekly_data_allx, "answer_count", "answer_count_any")
weekly_data_all <-merge(weekly_data_allx, prep_data_always_w, by=c("state","loc_id","week"), all.x=TRUE)
setnames(weekly_data_all, "answer_count", "answer_count_always")


##Now get divisional estimates and return below to state data

#-------------------------------------Division--------------------------

prep_data_div <- function(data, questions, timevar = "date"){
  
  data[, date:=as.Date(created)]
  qs <- questions[use==1, question]
  
  plot <- data[, c(qs, "Division", "date", "campaign_name"), with=F]
  
  
  #identify numeric vs categorical
  numeric_cols <- c()
  for(c in qs){
    message(c)
    plot[get(c) == "No" | get(c) == "no", paste0(c) := 0]
    plot[get(c) == "Yes" | get(c) == "yes", paste0(c) := 1]
    plot[get(c) == "i_m_not_sure", paste0(c) := NA]
    
    if(length(setdiff(unique(plot[,get(c)]), c(0,1,"",NA,"NA"))) == 0){
      numeric_cols <- c(numeric_cols,c)
      plot[,paste0(c) := as.numeric(get(c))]
    }
  }
  
  id.vars <- c("Division","variable", timevar)
  alpha <- 0.05 # for the confidence intervals
  
  if(length(numeric_cols)>1){
    plot_melt_num <- melt.data.table(plot,id.vars=c("Division",timevar),measure.vars = numeric_cols)
    plot_melt_num[,proportion := mean(value,na.rm=T),by=id.vars]
    plot_melt_num[,national_proportion := mean(value,na.rm=T), by=c(timevar,"variable")]
    plot_melt_num$type <- "numeric"
  }else{ 
    plot_melt_num <- data.table()}
  if(length(setdiff(qs,numeric_cols))>1){
    plot_melt_char <- melt.data.table(plot,id.vars=c("Division",timevar),measure.vars = setdiff(qs,numeric_cols))
    plot_melt_char[,answer_count := .N, by=c(id.vars,"value")]
    plot_melt_char[,question_count := .N, by=c(id.vars)]
    plot_melt_char[,proportion := answer_count/question_count]
    # and then at the national level:
    plot_melt_char[,answer_count_nat := .N, by=c(timevar,"variable","value")]
    plot_melt_char[,question_count_nat := .N, by=c(timevar,"variable")]
    plot_melt_char[,national_proportion := answer_count_nat/question_count_nat]
    plot_melt_char$type <- "character"
    
  }else{ 
    plot_melt_char <- data.table()}
  
  total_melt <- rbind(plot_melt_char,plot_melt_num,fill=T)
  
  ###save summary stats
  plot_melt_char <- plot_melt_char[!is.na(value) & !is.na(Division) & value!=""]
  plot_melt_char <- unique(plot_melt_char[order(Division, get(timevar), variable, value)])
  write.csv(plot_melt_char, paste0(output_dir, "/summary_stats_Division_by_",timevar,".csv"), row.names = F)
  
  
  #add on upper / lower
  ss_no_na <- copy(total_melt[!is.na(value)])
  ss_no_na[,ss := .N, by=id.vars]
  ss_no_na[,ss_nat := .N, by=c(timevar,"variable")]
  ss_no_na <- unique(ss_no_na[,.(ss,ss_nat,Division,date,variable)])
  total_melt <- merge(total_melt,ss_no_na,by=c("Division",timevar,"variable"),all.x=T)
  total_melt[,z := qnorm(1-alpha/2)]
  total_melt[,lower := proportion - z*sqrt(proportion*(1-proportion)/ss),by=id.vars]
  total_melt[,upper := proportion + z*sqrt(proportion*(1-proportion)/ss),by=id.vars]
  # same for the national level:
  total_melt[,lower_nat := national_proportion - z*sqrt(national_proportion*(1-national_proportion)/ss_nat),by=c(timevar,"variable")]
  total_melt[,upper_nat := national_proportion + z*sqrt(national_proportion*(1-national_proportion)/ss_nat),by=c(timevar,"variable")]
  total_melt[type == "numeric", value := 1]
  
  total_melt[,value:=gsub("_", " ", value)]
  total_melt[is.na(value) | value =="" | value =="i", value:="No response"]
  print(unique(total_melt$value))
  print("Use this to reorder")
  print(paste0(unique(total_melt$value), collapse = "','"))
  total_melt <- unique(total_melt[!is.na(Division)])
  
  return(total_melt)
}


prep_data_moving_average_div <- function(data, questions, n=5){
  
  data[, date:=as.Date(created)]
  qs <- questions[use==1, question]
  
  plot <- data[, c(qs, "Division", "date", "campaign_name"), with=F]
  
  print("Only works for categorical variables")
  #identify numeric vs categorical
  numeric_cols <- c()
  for(c in qs){
    message(c)
    plot[get(c) == "No" | get(c) == "no", paste0(c) := 0]
    plot[get(c) == "Yes" | get(c) == "yes", paste0(c) := 1]
    plot[get(c) == "i_m_not_sure", paste0(c) := NA]
    
    if(length(setdiff(unique(plot[,get(c)]), c(0,1,"",NA,"NA"))) == 0){
      numeric_cols <- c(numeric_cols,c)
      plot[,paste0(c) := as.numeric(get(c))]
    }
  }
  
  id.vars <- c("Division","variable", "date")
  alpha <- 0.05 # for the confidence intervals
  
  if(length(numeric_cols)>1){
    plot_melt_num <- melt.data.table(plot,id.vars=c("Division","date"),measure.vars = numeric_cols)
    plot_melt_num[,proportion := mean(value,na.rm=T),by=id.vars]
    plot_melt_num[,national_proportion := mean(value,na.rm=T), by=c("date","variable")]
    plot_melt_num$type <- "numeric"
  }else{ 
    plot_melt_num <- data.table()}
  if(length(setdiff(qs,numeric_cols))>1){
    plot_melt_char <- melt.data.table(plot,id.vars=c("Division","date"),measure.vars = setdiff(qs,numeric_cols))
    plot_melt_char[,answer_count := .N, by=c(id.vars,"value")]
    plot_melt_char[,question_count := .N, by=c(id.vars)]
    plot_melt_char[,proportion := answer_count/question_count]
    
    plot_melt_char <-  unique(plot_melt_char[order(Division, date, variable, value)])
    
    # get lags of what is needed
    lag_answer_cols <- paste0("lag_answer_count", 1:(n-1))
    plot_melt_char[, paste0(lag_answer_cols):=data.table::shift(answer_count, n=1:(n-1), fill=first(answer_count)), by=c("Division", "variable","value")]
    lag_answer_cols <- c(lag_answer_cols, "answer_count")
    
    lag_question_cols <- paste0("lag_question_count", 1:(n-1))
    plot_melt_char[, paste0(lag_question_cols):=data.table::shift(question_count, n=1:(n-1), fill=first(question_count)), by=c("Division", "variable","value")]
    lag_question_cols <- c(lag_question_cols, "question_count")
    
    
    # sum across lags
    plot_melt_char[, total_answer_count:= rowSums(plot_melt_char[, lag_answer_cols, with=F]) ]
    plot_melt_char[, total_question_count:= rowSums(plot_melt_char[, lag_question_cols, with=F]) ]
    plot_melt_char[, avg_proportion := total_answer_count/total_question_count]
    
    
    plot_melt_char <- plot_melt_char[, .(Division, date, variable, value, total_answer_count, total_question_count, avg_proportion,answer_count ,question_count)]
    
    
    plot_melt_char$type <- "character"
    
  }else{ 
    plot_melt_char <- data.table()}
  
  total_melt <- plot_melt_char
  
  
  total_melt[,z := qnorm(1-alpha/2)]
  total_melt[,lower := avg_proportion - z*sqrt(avg_proportion*(1-avg_proportion)/total_question_count),by=id.vars]
  total_melt[,upper := avg_proportion + z*sqrt(avg_proportion*(1-avg_proportion)/total_question_count),by=id.vars]
  
  total_melt[,value:=gsub("_", " ", value)]
  total_melt[is.na(value) | value =="" | value =="i", value:="No response"]
  print(unique(total_melt$value))
  print("Use this to reorder")
  print(paste0(unique(total_melt$value), collapse = "','"))
  total_melt <- unique(total_melt[!is.na(Division)])
  
  return(total_melt)
}

prepped_data_div <- prep_data_div(data, questions)
prepped_avg_data_div <- prep_data_moving_average_div(data, questions, n=5)

#Set this every time for plotting!
order_levels <- c('No response', 'very unconcerned', 'unconcerned','neither concerned nor unconcerned','concerned','very concerned',
                  'no never','yes sometimes','yes always',
                  'i do not own a mask or know how to make one','i do not believe face masks are effective for preventing the spread of covid 19','face masks are uncomfortable','nobody else in my area wears a mask')


prepped_data_div[type=="character", order_value := factor(value, levels = c(order_levels))]


pdf(paste0(output_dir, "/time_trends_division.pdf"),height=8,width=12)

vars_to_plot <- unique(prepped_data_div[type == "numeric",variable])
for(v in vars_to_plot){
  message(v)
  for(s in unique(prepped_data_div[variable==v]$Division)){
    message(s)
    p_num <- ggplot(prepped_data_div[Division == s & type == "numeric" & variable == v],aes(date,proportion))+
      geom_point(aes(date,proportion,size=ss,alpha=0.5, color = "Division trend"))+
      geom_linerange(aes(x=date,ymin = lower, ymax = upper, color= "Division trend"))+
      geom_point(data=prepped_data_div[variable == v],aes(date,national_proportion, color="USA national trend"))+
      geom_smooth(data=prepped_data_div[variable == v],aes(date,national_proportion, color= "USA national trend"),alpha=0.3)+
      geom_smooth(data=prepped_data_div[Division == s & variable == v],aes(date,proportion, color= "Division trend"),alpha=0.3)+
      scale_color_manual(values=c("darkred", "darkblue"))+
      scale_alpha_continuous(guide=F)+
      ylim(0,1)+
      ggtitle(paste0(s,"\n",gsub("_", " ", v), "?"))+theme_bw()+theme(legend.position="bottom")+
      labs(y="proportion 'yes'", color="", size="Division sample size")
    
    print(p_num)
  }
}

vars_to_plot <- unique(prepped_data_div[type == "character",variable])
for(v in vars_to_plot){
  message(v)
  
  unique_ss <- unique(prepped_data_div[type == "character" & variable == v,.(type,variable,ss_nat,date)])
  unique_ss$proportion <- 1.005
  p_char <- ggplot()+
    geom_bar(data=unique(prepped_data_div[type == "character" & variable == v, .(date,national_proportion, order_value)]),
             aes(x=date,y=national_proportion,fill=order_value),stat="identity")+
    geom_text(data=unique_ss,aes(x=date,y=proportion,label = ss_nat))+
    scale_fill_brewer(palette="Blues")+
    #geom_line(aes(date,proportion,color=value))+
    #geom_linerange(aes(x=date,ymin = lower, ymax = upper))+
    #facet_wrap(~variable)+
    ggtitle(paste0("USA average","\n",gsub("_", " ", v), "?"))+theme_bw()+theme(legend.position="bottom")+
    labs(y="proportion", fill="")
  
  print(p_char)
  
  for(s in unique(prepped_data_div[variable==v]$Division)){
    message(s)
    unique_ss <- unique(prepped_data_div[Division == s & type == "character" & variable == v,.(Division,type,variable,ss,date)])
    unique_ss$proportion <- 1.005
    p_char <- ggplot()+
      geom_bar(data=prepped_data_div[Division == s & type == "character" & variable == v],
               aes(x=date,y=proportion,fill=order_value),stat="identity")+
      geom_text(data=unique_ss,aes(x=date,y=proportion,label = ss))+
      scale_fill_brewer(palette="Blues")+
      #geom_line(aes(date,proportion,color=value))+
      #geom_linerange(aes(x=date,ymin = lower, ymax = upper))+
      #facet_wrap(~variable)+
      ggtitle(paste0(s,"\n",gsub("_", " ", v), "?"))+theme_bw()+theme(legend.position="bottom")+
      labs(y="proportion", fill="")
    
    print(p_char)
  }
  
  values <- unique(prepped_avg_data_div[type == "character" & variable == v & value!="No response", value])
  
  for(val in values){
    #line plots
    p_char <- ggplot()+
      geom_line(data=prepped_avg_data_div[type == "character" & variable == v & value==val],
                aes(x=date,y=avg_proportion, color = Division))+
      ylim(0,1)+
      ggtitle(paste0("All Divisions","\n",gsub("_", " ", v), "?","\n", n,"-day proportion who responded '", val,"'"))+theme_bw()+theme(legend.position="bottom")+
      labs(y="proportion", color="")
    
    print(p_char)
    
  }
  for(s in unique(prepped_avg_data_div[variable==v]$Division)){
    
    sub_data <- prepped_avg_data_div[Division == s & type == "character" & variable == v & value!="No response"]
    avg_n <- mean(sub_data$question_count)
    
    
    p_char <- ggplot(data=sub_data)+
      #geom_line(aes(x=date,y=avg_proportion, color = paste0(n,"-day proportion who responded '", val,"'")))+
      geom_line(aes(x=date,y=avg_proportion, color = value))+
      #geom_ribbon(aes(x = date, ymin = lower, ymax=upper), alpha=0.3, color="grey")+
      ylim(0,1)+
      ggtitle(paste0(s,"\n",gsub("_", " ", v), "?","\n", "Avg daily respondents: ", round(avg_n)))+theme_bw()+
      labs(y="proportion", color=paste0(n,"-day proportion who responded"))
    
    print(p_char)
    
  }
  
  
}   
dev.off()

#Focus for models is % wear: 
prep_data_division<-prepped_data_div[prepped_data_div$variable=="when_you_leave_your_home_do_you_typically_wear_a_face_mask",]
prep_data_div_w<-merge(prep_data_division, weeks, by="date", all.x=TRUE)
prep_data_div2<-prep_data_div_w[!is.na(prep_data_div_w$week) &prep_data_div_w$value!="No response",]
prep_data_div2$new_value<-ifelse(prep_data_div2$value %in% c('yes always', 'yes sometimes'), "yes", prep_data_div2$value)


prep_data_div_denom<-prep_data_div2[!duplicated(prep_data_div2[,c(1,2)]),c(1,2,6,20)]
prep_data_div_denom_w<-aggregate(question_count~week+Division, data=prep_data_div_denom, FUN=sum)
prep_data_always_d<-subset(prep_data_div2, prep_data_div2$value=="yes always")
prep_data_always_d_w<-aggregate(answer_count~week+Division, data=prep_data_always_d, FUN=sum)

prep_data_yes_d<-subset(prep_data_div2, prep_data_div2$new_val=="yes")
prep_data_yes_d_w<-aggregate(answer_count~week+Division, data=prep_data_yes_d, FUN=sum)
weekly_data_all_dx<-merge(prep_data_yes_d_w, prep_data_div_denom_w, by=c("Division", "week"))
setnames(weekly_data_all_dx, "answer_count", "answer_count_any")
weekly_data_all_div<-merge(weekly_data_all_dx, prep_data_always_d_w, by=c("Division","week"), all.x=TRUE)
setnames(weekly_data_all_div, "answer_count", "answer_count_always")

weekly_data_all_div$answer_count_always<-ifelse(is.na(weekly_data_all_div$answer_count_always),0, weekly_data_all_div$answer_count_always)


#-------------------- Find states with <30 counts for any week

#First, merge all data with division via "state_mapping"
weekly_data_all<-merge(weekly_data_all, state_mapping, by="state")
#unique states that have at least one week <30 counts
states_less_30<-weekly_data_all[weekly_data_all$question_count<30 & weekly_data_all$week < 14,]
#set of those states
states_u30<-weekly_data_all[weekly_data_all$state %in% unique(states_less_30$state),]
#set of the states with >=30 for all four weeks
states_g30<-weekly_data_all[!(weekly_data_all$state %in% unique(states_less_30$state)),]

# Plot sample size among states that get Divisional value
pdf(paste0(output_dir, "/time_trends_small_samples.pdf"),height=8,width=12)
  ggplot(states_u30, aes(x=week, y=question_count)) + facet_wrap(~state, scales="free_y") + 
    geom_line() + theme_bw() + scale_x_continuous(breaks = seq(0,max(states_u30$week),2))
dev.off()


# #drop the counts for the states with <30
states_u30s<-states_u30[,c("state", "loc_id", "week", "Division", "Region")]
#and merge with divisional data
states_u30d<-merge(states_u30s, weekly_data_all_div, by=c('Division', "week"))
#order in same order
states_u30d<-states_u30d[,c("state","loc_id","week","answer_count_any","question_count", "answer_count_always", "Division","Region")]
states_g30<-states_g30[,c("state","loc_id","week","answer_count_any","question_count", "answer_count_always", "Division","Region")]

#bind back to original data
#states_weekly <- data.frame(rbind(states_u30, states_g30))

states_weekly<-data.frame(rbind(states_u30d, states_g30))

#now get proportions
states_weekly$prop_any<-states_weekly$answer_count_any/states_weekly$question_count
states_weekly$prop_always<-states_weekly$answer_count_always/states_weekly$question_count

#VERIFY EVERY STATE HAS CORRECT # OBSERVATIONS:
for (state in unique(states_weekly$state)){
  sub<-states_weekly[states_weekly$state==state,]
  wks<-nrow(sub)
  if(wks==max(weeks$week)){
    print(paste0(state, " has all weeks"))
  } else{
    if (wks < max(weeks$week)){
      print(paste0("STOP! ", state, " is missing a week"))
    }
  }
}

# #Fix Vermont as a one-off for week 5 -no back filling for week 5 in most recent data.
#May need to do this each time - 1 respondent on 5/14 and several on 5/23, but none in between...
#keep vermont-specific state information, but use division-specific week 5 info
new_rowA<-states_weekly[states_weekly$state=="Vermont" & states_weekly$week==4,1:3]
new_rowA$week<-5
new_rowB<-states_weekly[states_weekly$Division=="New England" & states_weekly$week==5,4:10]
new_rowB<-new_rowB[1,]
new_row<-cbind(new_rowA, new_rowB)
states_weekly<-rbind(states_weekly, new_row)

# Vermont is missing after week 6"
fill_vermont <- states_weekly[states_weekly$Division == "New England" & states_weekly$state=="Maine" & states_weekly$week > 6,]
fill_vermont$state <- "Vermont"
states_weekly <- rbind(states_weekly, fill_vermont)

#order by state then week - and write to "best" location!
states_weekly<-states_weekly[order(states_weekly$state, states_weekly$week),]
write.csv(states_weekly, file.path(output_dir, "Weekly_Premise_Mask_Use.csv"), row.names=FALSE)

print("finished processing - now writing PREMISE_PROCESSING_CODE.metadata.yaml")

yaml::write_yaml(
    list(
      script = "PREMISE_PROCESSING_CODE.R",
      output_dir = output_dir,
      input_files = ihme.covid::get.input.files()
    ),
  file = metadata_path
)
