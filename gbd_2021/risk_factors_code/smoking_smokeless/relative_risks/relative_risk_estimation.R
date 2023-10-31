###########################################################################################################################################
#Run SLT Meta-analyses
#NOTE: This code interactive - ie. you must change setttings for specific goals (all smokeless tobacco relative risks run out of here
#and require different settings as you go)
#########################################################################################################################################

rm(list = setdiff(ls(), "all_pooled"))

## OS locals
os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
  hpath <- "FILEPATH"
} else {
  jpath <- "/FILEPATH"
  hpath <- "FILEPATH"
}


library(data.table)
library(magrittr)
library(dplyr)
library(metafor)
library(ggplot2)

#set some stuff
interest <- "stroke"
tob_type <- #"chewing tobacco" or "snuff"
by_sex <- F
by_fatal <- F

if(by_sex){
  run_sex <- "male"
}

#get data
indata <- fread("FILEPATH") 
indata <- indata[, lapply(.SD, tolower)] 

#narrow down study estimates to one estimate per age-sex-tobacco-type for any studies with more than one
df <- indata[, .(author, pmid, year_start, year_end, exposed_definition, unexposed_definition, tobacco_type, tobacco_type_name, tobac_user_freq, sex, age_start, age_end, measure, outcome, mean, 
                 lower, upper, covariates_other, notes, run_outcome, group_review, region, adj_smoking, freq, measure, by_amt_consumed, fatal, year)]

#basic drops 
df[, c("lower", "upper", "mean", "group_review"):=lapply(.SD, as.numeric), .SDcols = c("lower", "upper", "mean", "group_review")]
df <- df[is.na(group_review)]
df <- df[!(adj_smoking == 0)]
df <- df[by_amt_consumed != 1]

if (interest != "ihd"){
  df <- df[run_outcome == interest]
}else {
  df <- df[run_outcome == interest]
}


#if fatal only
if (by_fatal){
  df <- df[fatal == 1]
}

#tob type drop as specified
if(!is.null(tob_type)){
  if(tob_type == "chewing tobacco"){
    df <- df[tobacco_type == tob_type]
  }
  df <- df[tobacco_type == tob_type] # | tobacco_type == "unspecified" | tobacco_type=="multiple products"]
} else{
  df <- df[region == country]
}

#both sex drop as specified
if(by_sex){
  df <- df[sex == run_sex]
}

#drop estimates from overlapping frequency categories
df[, repeats:=.N, by = c("author", "year", "tobacco_type_name", "outcome", "sex")]
df[, .(author, year, sex, tobacco_type_name, outcome, freq,fatal, repeats)]
df <- df[!(repeats > 1 & (grepl("ever", freq)))]

#convert to log space
df[, log_lower:=log(lower)]
df[, log_upper:= log(upper)]
df[, log_se:=(log_upper - log_lower)/(2*1.96)]
df[, log_rr:=log(mean)]

setkeyv(df, "sex")

#deal with some specific studies with separate RR estimates by subsets of the cancer outcome

if(interest == "ihd"){
  if (tob_type == "snuff"){
    bolin <- rma.uni(yi = log_rr, sei = log_se , method = "DL", data = df[author=="bolinder"])
    bol <- predict(bolin)
    bol_ests <- data.table(log_mean = bol$pred, log_lower = bol$ci.lb, log_upper = bol$ci.ub)
    
    df[author=="bolinder", log_rr:=bol_ests$log_mean]
    df[author=="bolinder", log_lower:=bol_ests$log_lower]
    df[author=="bolinder", log_upper:=bol_ests$log_upper]
    df <- unique(df, by = c("author", "freq", "sex"))
  } else{
    huhta <- rma.uni(yi = log_rr, sei = log_se , method = "DL", data = df[author=="huhtasaari"])
    huh <- predict(huhta)
    huh_ests <- data.table(log_mean = huh$pred, log_lower = huh$ci.lb, log_upper = huh$ci.ub)
    
    df[author=="huhtasaari", log_rr:=huh_ests$log_mean]
    df[author=="huhtasaari", log_lower:=huh_ests$log_lower]
    df[author=="huhtasaari", log_upper:=huh_ests$log_upper]
    df <- unique(df, by = c("author", "freq", "sex"))
  }
  
}

if(tob_type == "snuff" & interest == "oesophageal"){
  lager <- rma.uni(yi = log_rr, sei = log_se , method = "DL", data = df[author=="lagergren 2000"])
  lag <- predict(lager)
  lag_ests <- data.table(log_mean = lag$pred, log_lower = lag$ci.lb, log_upper = lag$ci.ub)
  
  zendeh <- rma.uni(yi = log_rr, sei = log_se , method = "DL", data = df[author=="zendehdel 2008"])
  zen <- predict(zendeh)
  zen_ests <- data.table(log_mean = zen$pred, log_lower = zen$ci.lb, log_upper = zen$ci.ub)
  
  df[author=="lagergren 2000", log_rr:=lag_ests$log_mean]
  df[author=="lagergren 2000", log_lower:=lag_ests$log_lower]
  df[author=="lagergren 2000", log_upper:=lag_ests$log_upper]

  df[author=="zendehdel 2008", log_rr:=zen_ests$log_mean]
  df[author=="zendehdel 2008", log_lower:=zen_ests$log_lower]
  df[author=="zendehdel 2008", log_upper:=zen_ests$log_upper]
  df <- unique(df, by = c("author", "freq"))
  
}

if(tob_type == "chewing tobacco" & interest == "pharyngeal"){
  jus <- rma.uni(yi = log_rr, sei = log_se , method = "DL", data = df[author=="jussawalla 1971"])
  jest <- predict(jus)
  j_ests <- data.table(log_mean = jest$pred, log_lower = jest$ci.lb, log_upper = jest$ci.ub)
  
  df[author=="jussawalla 1971", log_rr:=j_ests$log_mean]
  df[author=="jussawalla 1971", log_lower:=j_ests$log_lower]
  df[author=="jussawalla 1971", log_upper:=j_ests$log_upper]
  df <- unique(df, by = "author")
}

if (tob_type == "chewing tobacco" & interest == "stroke"){
  for (sx in c("male", "female")){
    gaja <- rma.uni(yi = log_rr, sei = log_se , method = "DL", data = df[author=="gajalakshmi" & sex == sx])
    gaj <- predict(gaja)
    gaj_ests <- data.table(log_mean = gaj$pred, log_lower = gaj$ci.lb, log_upper = gaj$ci.ub)
    
    df[author=="gajalakshmi" & sex == sx, log_rr:=gaj_ests$log_mean]
    df[author=="gajalakshmi" & sex == sx, log_lower:=gaj_ests$log_lower]
    df[author=="gajalakshmi" & sex == sx, log_upper:=gaj_ests$log_upper]
  }
  df <- unique(df, by = c("author", "freq", "sex"))
}

#run meta-analysis
meta <- rma.uni(yi = log_rr, sei = log_se , method = "DL", data = df)
est <- predict.rma(meta, transf = exp)

#combine full pooled results with individual component risks
pooled <- data.table(author = "Pooled results", mean = est$pred, lower = est$ci.lb, upper = est$ci.ub)
woods <- df[, .(author, freq, tobacco_type, sex, mean, lower , upper)]
woods <- rbind(woods, pooled, fill = T)

#fix so it'll stop plotting things by the same author in the same row
woods[, count:=seq(.N), by = "author"]
woods[count == 1, num:=""]
woods[count == 2, num:=" (b)"]
woods[count == 3, num:=" (c)"]
woods[count == 4, num:=" (d)"]

woods[, study:=paste0(author, num)]

#bind pooled results to output table
woods[study == "Pooled results", tobacco_type:=tob_type]
woods[, outcome:=interest]
if(by_sex){woods[study == "Pooled results", sex:=run_sex]}

#if running all causes in sequence and you want a full pooled results table generated -- but you'll have to create an empty data.table called all_pooled at beginning
if(F){
  all_pooled <- rbind(woods[study == "Pooled results", .(outcome, tobacco_type, sex, mean, lower, upper)], all_pooled, fill =T)
}


  #deal with reaally wide CIs that go past reasonable x limits 
  n_upper <- 5
  woods[upper > n_upper, upper_real:=upper]
  woods[upper> n_upper, upper:=n_upper]
  
  #set some cause-specific formatting
  if (interest != "ihd" & interest != "stroke"){
    woutcome <- paste0(interest, "_cancer")
    presentable <- paste0(interest, " cancer")
  }else{
    if(by_fatal){
      woutcome <- interest 
      interest <- paste0(interest, "_fatal")
      presentable <- paste0("Fatal ", toupper(interest))
    }else{
      woutcome <- interest
      #interest <- paste0("ihd_with_MI")
      presentable <- paste0(toupper(interest))
    }

  }
  
  
  focus <- ifelse(is.null(country), tob_type, country)
  
  
  #GGPLOT
  
  p <- ggplot(woods, aes(y=study, x=mean, xmin=lower, xmax=upper, col = freq))+
    #Add data points and color them black
    geom_point() +
    #Add 'special' points for the summary estimates, by making them diamond shaped
    geom_point(data=woods[author == "Pooled results"], color='black', shape=18, size=4)+
    #add the CI error bars
    geom_errorbarh(height=.1) +
    #Specify the limits of the x-axis and relabel it to something more meaningful
    scale_x_continuous(limits=c(0,n_upper), name='Relative Risk')+
    #Give y-axis a meaningful label
    ylab('Reference')+
    #Add a vertical dashed line indicating an effect size of zero, for reference
    geom_vline(xintercept=1, color='black', linetype='dashed')+
    #Create sub-plots (i.e., facets) based on levels of setting
    #And allow them to have their own unique axes (so authors don't redundantly repeat)
    facet_grid(sex~., scales= 'free', space='free') +
    ggtitle(paste0("Meta-analysis, ", focus , " - all sources - ", presentable)) +
    #Apply my APA theme
    theme_classic()
  
  ########################################
  
  
  outpath <- paste0("FILEPATH")
  date <- format(Sys.Date(), "%m%d%y")
  
  if(is.null(tob_type)){
    if (by_sex){
      if(run_sex == "male"){
      plot_out <- "FILEPATH"
      csv_out <- "FILEPATH"
      } else {
        plot_out <- "FILEPATH"
        csv_out <- "FILEPATH"
      }
    } else {
      plot_out <- "FILEPATH"
      csv_out <- "FILEPATH"
    }
  } else {
    if(by_sex){
      if(run_sex == "male"){
        no_space_tob <- gsub(" ", "_", tob_type)
        plot_out <- "FILEPATH"
        csv_out <- "FILEPATH"
      } else {
        no_space_tob <- gsub(" ", "_", tob_type)
        plot_out <- "FILEPATH"
        csv_out <- "FILEPATH"
      }
    }else {
      no_space_tob <- gsub(" ", "_", tob_type)
      plot_out <- "FILEPATH"
      csv_out <- "FILEPATH"
    }
  }


#print pdf of forest plot
pdf(plot_out)
  print(p)
dev.off()

#write csv of final formatted data

if("upper_real" %in% names(woods)){
  woods[!is.na(upper_real), upper:=upper_real]
}
woods[, c("upper_real", "count", "num"):=NULL]

write.csv(woods, csv_out)