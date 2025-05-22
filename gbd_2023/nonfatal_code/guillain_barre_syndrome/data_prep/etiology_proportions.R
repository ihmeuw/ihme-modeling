###########################################################
### Author: USER
### Date: 3/12/2025
### Project: GBD Nonfatal Estimation
### Purpose: GBS Splits
### Lasted edited: USER June 2024
###########################################################

##Setup
rm(list=ls())
#install.packages("meta", repos = "FILEPATH", dependencies=TRUE, lib= "FILEPATH")

if (Sys.info()[1] == "Linux"){
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

set.seed(98736)

##Set directories and objects
# option 1 
## 1. calculates non_covid as the difference between all specified and covid
## 2. squeezes non covid conditions to the difference between all specified and covid
## 3. other neurological disorders is the inverse of all specified from meta analysis 
##    And covid is from meta analysis

#option 2 
## 1. calculates non_covid as 1 - covid from meta analysis
## 2. calculates other neurological disorders as 1 - all specified from meta analysis
## 3. squeezes all_specified and other neurological to non_covid
## 4. named conditions other than covid are then squeezed into this squeezed all_specified
## 5. covid is from meta analysis , other neuro is squeezed.

option <- 2

custom_input <- "FILEPATH"
gbs <- "FILEPATH"

##Get packages
library(data.table)
library(meta)
#library(meta, lib.loc="FILEPATH")
library(arm)
library(boot)

create_prop <- function(cat, option=1){
  
  results <- data.table(name = character(0), mean = numeric(0), lower = numeric(0), upper = numeric(0))
  
  for (x in cat) {
    meta <- metaprop(data = get(x), event = cases, n = sample_size,
                     studylab = citation, comb.random = T, level = .95)
    sum <- summary(meta)
    random <- sum$random
    new_row <- c(random$TE, random$lower, random$upper)
    new_row <- as.list(inv.logit(new_row))
    new_row <- c(x, new_row)
    results <- rbind(results, new_row)
  }
  print("covid" %in% cat)
  if("covid" %in% cat){
    if(option ==1){
      all_spec_row <- results[name == "all_specified"]
      cov_spec <- results[name == "covid" | name =="all_specified" , ]
      spec <- cov_spec[name == "covid"]
      sum_non_covid <- results[!name == "covid" & !name =="all_specified" , ]
      
      # non covid is the difference between all specified and covid
      non_covid <- data.table(name = "non_covid",
                              mean= all_spec_row[,mean] - spec[,mean]  ,
                              lower =  all_spec_row[,lower]  - spec[,upper],
                              upper = all_spec_row[,upper] - spec[,lower])
      
      
      cov_spec <- rbind(cov_spec, non_covid)
      
      squeezed <- copy(results)
      squeezed <- squeezed[!name == "all_specified", ]
      squeezed <- squeezed[!name == "covid"]
      squeezed <- rbind(squeezed, cov_spec[name == "non_covid", .(name, mean, lower, upper)])
      
      # squeeze named conditions other than covid into this non_covid 
      all_specified <- as.numeric(squeezed[name == "non_covid", .(mean)]) 
      squeezed[!name == "non_covid", total := sum(mean)]
      squeezed[!name == "non_covid", mean := mean * all_specified / total]
      squeezed[!name == "non_covid", lower := lower * all_specified / total]
      squeezed[!name == "non_covid", upper := upper * all_specified / total]
      
      ##Other neurological disorders is the inverse of all specified
      all_spec_row[name == "all_specified", name := "other_neurological"]
      all_spec_row[name == "other_neurological", mean := 1- mean]
      all_spec_row[name == "other_neurological", lower := 1- upper]
      all_spec_row[name == "other_neurological", upper := 1 - lower]
      
      # bind squeezed named conditions covid from meta analysis and other neurological
      squeezed <- squeezed[, .(name, mean, lower, upper)]
      squeezed <- squeezed[!name == "non_covid"]
      squeezed <- rbind(squeezed, all_spec_row[, .(name, mean, lower, upper)])
      squeezed <- rbind(squeezed, cov_spec[name == "covid", .(name, mean, lower, upper)])
      
      
    }else if(option == 2){
      # option 2
      cov_spec <- results[name == "covid" | name =="all_specified" , ]
      spec <- cov_spec[name == "covid"]
      all_spec_row <- results[name == "all_specified"]
      
      sum_non_covid <- results[!name == "covid" & !name =="all_specified" , ]
      #non covid is 1- covid
      non_cov<- data.table(name = "non_covid",
                           mean=1 - spec[,mean],
                           lower =  1- spec[,lower],
                           upper = 1 - spec[,upper])
      #other nero is 1- all specified
      test_other <-data.table(name = "other_neurological",
                              mean=1- all_spec_row[,mean],
                              lower =  1- all_spec_row[,upper],
                              upper = 1 - all_spec_row[,lower])
      
      cov_spec <- rbind(all_spec_row, non_cov, test_other)
      all_specified <- as.numeric(cov_spec[name == "non_covid", .(mean)]) 
      # squeeze other neuro , and all specified to non covid
      squeeze1 <- cbind(cov_spec, all_specified)
      cov_spec[!name == "non_covid" , total := sum(mean)]
      cov_spec[!name == "non_covid" , mean := mean * all_specified / total]
      cov_spec[!name == "non_covid" , lower := lower * all_specified / total]
      cov_spec[!name == "non_covid", upper := upper * all_specified / total]
      
      
      # squeeze named conditions from meta analysis to new squeezed all_specified
      squeezed <- copy(results)
      all_spec_row <- cov_spec[name == "all_specified"]
      squeezed <- squeezed[!name == "all_specified", ]
      squeezed <- squeezed[!name == "covid"]
      squeezed <- rbind(squeezed, cov_spec[name == "all_specified", .(name, mean, lower, upper)])
      all_specified <- as.numeric(squeezed[name == "all_specified", .(mean)]) 
      squeezed[!name == "all_specified", total := sum(mean)]
      squeezed[!name == "all_specified", mean := mean * all_specified / total]
      squeezed[!name == "all_specified", lower := lower * all_specified / total]
      squeezed[!name == "all_specified", upper := upper * all_specified / total]
      
      # bind squeezed named conditions, covid from meta analysis and squeezed other neuro
      squeezed <- squeezed[!name == "all_specified", .(name, mean, lower, upper)]
      squeezed <- rbind(squeezed, cov_spec[name == "other_neurological", .(name, mean, lower, upper)])
      squeezed <- rbind(squeezed, results[name == "covid", .(name, mean, lower, upper)])
      
      
      
    }else{
      #stop not valid option
      stop("Invalid option")
    }}else{
      
      results <- data.table(name = character(0), mean = numeric(0), lower = numeric(0), upper = numeric(0))
      
      for (x in cat) {
        meta <- metaprop(data = get(x), event = cases, n = sample_size,
                         studylab = citation, comb.random = T, level = .95)
        sum <- summary(meta)
        random <- sum$random
        new_row <- c(random$TE, random$lower, random$upper)
        new_row <- as.list(inv.logit(new_row))
        new_row <- c(x, new_row)
        results <- rbind(results, new_row)
      }
      
      ##squeeze to all_specified
      squeezed <- copy(results)
      all_specified <- as.numeric(squeezed[name == "all_specified", .(mean)])
      squeeze1 <- cbind(squeezed, all_specified)
      squeezed[!name == "all_specified", total := sum(mean)]
      squeezed[!name == "all_specified", mean := mean * all_specified / total]
      squeezed[!name == "all_specified", lower := lower * all_specified / total]
      squeezed[!name == "all_specified", upper := upper * all_specified / total]
      
      ##Other neurological disorders is the difference between 100% and all_specified
      squeezed[name == "all_specified", name := "other_neurological"]
      squeezed[name == "other_neurological", mean := 1- mean]
      squeezed[name == "other_neurological", lower := 1- upper]
      squeezed[name == "other_neurological", upper := 1 - lower]
      
      squeezed <- squeezed[, .(name, mean, lower, upper)]
      
      
    }
  
  return(squeezed)
}


##Get and format data
data <- "FILEPATH"
# where there is covid data fill all_specified with the same values
#data <- data[!is.na(value_prop_covid)  ,value_prop_all_specified := value_prop_covid]


setnames(data, "cases", "sample_size")
data[, all_specified := round(sample_size * value_prop_all_specified)]
data[, influenza := round(sample_size * value_prop_influenza)]
data[, URI := round(sample_size * value_prop_URI)]
data[, GI := round(sample_size * value_prop_GI)]
data[, other_infectious := round(sample_size * value_prop_other_infectious)]
data[,covid := round(sample_size * value_prop_covid)]
all_specified <- data[!is.na(all_specified)]
all_specified[, cases := all_specified]
influenza <- data[!is.na(influenza)]
influenza[, cases := influenza]
URI <- data[!is.na(URI)]
URI[, cases := URI]
GI <- data[!is.na(GI)]
GI[, cases := GI]
covid <- data[!is.na(covid)]
covid[, cases := covid]
other_infectious <- data[!is.na(other_infectious)]
other_infectious[, cases := other_infectious]

##Frame for Results

categories_covid <- c("all_specified", "influenza", "URI", "GI", "other_infectious","covid")
categories <- c("all_specified", "influenza", "URI", "GI", "other_infectious")
##squeeze to all_specified
squeezed_covid <- create_prop(categories_covid,option)
squeezed <- create_prop(categories)


# write.csv(squeezed, paste0(gbs, "FILEPATH"), row.names = F)
# write.csv(squeezed_covid, paste0(gbs, "FILEPATH"), row.names = F)
