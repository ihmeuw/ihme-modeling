######################################################
### Title: Cirrhosis & Liver Cancer Redistribution

arguments <- commandArgs()[-(1:5)]

## Source libraries and packages ---------------------
library(plyr)
library(data.table)
library(dplyr)
library(magrittr)
library(tidyr)
library(foreach) 
library(doParallel)

source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH') #06_06_cirrhosis_redistribution_functions.R

## Variables that change------------------------------
testing <- F
annual <- F

cod_gbd_round <- 7
cod_correct_decomp <- "iterative"
cod_correct_version <- 244
cod_year_end <- 2022

como_gbd_round = 7
como_decomp <- "iterative"
como_year_end <- 2020
como_version <- 821

pop_round <- 7
pop_decomp <- "iterative"


## Read arguments and make necessary variables--------

if (testing){
  paf_directory <- 'FILEPATH'
  location <- 102
} else {
  paf_directory <- arguments[1]
  
  param_map <- fread('FILEPATH')
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  
  location   <- param_map[task_id, location]
}

print(location)
years       <- c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022)

measures      <- c(3, 4) #yld & yll
sexes         <- c(1, 2)
causes        <- c(420, 524) #liver cancer & cirrhosis (here, due to alcohol because that is how they are defined upstream)
ages          <- c(8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)


## Read in PAFs pt 1 (completeness check) -----------------------------------

files <- list.files('FILEPATH')  %>%
  .[grep(paste0("_",location,"_"), .)]

print(paste0("Loading ", length(files), " files from specified directory and locations"))

paf_full <- rbindlist(lapply(files, aggregateFiles)) 
paf_full <- paf_full[year_id %in% years] 

print(nrow(paf_full))

## Read in burden data -----------------------------------

cause_list <- c(522, 523, 524, 525, 971, 418, 419, 420, 421, 996)
pops <- get_population(location_id=location, year_id=years, sex_id = sexes, age_group_id = ages, gbd_round_id = pop_round, decomp_step = pop_decomp)

yld <- rbindlist(lapply(cause_list, read_burden, annual = annual, measure_id = 3, years = years)) 
yld <- yld[year_id %in% years] %>%
  pop_standardize(., pops)

yll <- rbindlist(lapply(cause_list, read_burden, annual = annual, measure_id = 4, years = years)) 
yll <- yll[year_id %in% years] %>%
  melt(id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))  


## Combine burden data -----------------------------------
cause_total <- rbind(yld, yll) %>%
  setnames(old = "variable", new = "draw") %>%
  .[,draw :=  gsub("draw_", "", draw) %>% as.numeric] %>%
  .[,sex_id := as.numeric(sex_id)] %>%
  .[measure_id == 3, measure := "yld"] %>%
  .[measure_id == 4, measure := "yll"] %>%  
  .[cause_id %in% c(522, 418), etiology := "hep_b"]%>%
  .[cause_id %in% c(523, 419), etiology := "hep_c"]%>%
  .[cause_id %in% c(524, 420), etiology := "alcohol"]%>%
  .[cause_id %in% c(525, 421), etiology := "other"]%>%
  .[cause_id %in% c(971, 996), etiology := "nash"]%>%
  .[cause_id %in% c(522, 523, 524, 525, 971), parent := "cirrhosis"]%>%
  .[cause_id %in% c(418, 419, 420, 421, 996), parent := "liver_cancer"]

## Read in PAFs --------------------------------------------- 

print(paste0("Defined data frame with ", nrow(paf_full), " observations, and ", ncol(paf_full), " variables"))

paf <- paf_full[cause_id %in% causes] %>%
  melt(id.vars = c("year_id", "age_group_id", "cause_id", "sex_id", "measure")) %>%
  setnames(old = c("variable", "value"), new = c("draw", "paf")) %>%
  .[,sex_id := as.numeric(sex_id)] %>%
  .[,draw :=  gsub("paf_", "", draw) %>% as.numeric] %>%
  .[cause_id == 524, cause_id := 521] %>% ### correcting a historical remnant from when we didn't do this redistribution-- this is actually more correct
  .[cause_id == 420, cause_id := 417] %>%
  .[cause_id == 521, parent := "cirrhosis"] %>%
  .[cause_id == 417, parent := "liver_cancer"] %>%
  .[,cause_id := NULL]


## Combine the burden and PAF data ------------------------------
paf_new <- merge(paf, cause_total, by = c("age_group_id", "year_id", "sex_id", "draw", "measure", "parent"), all.x = T)%>%
  redistribute_pafs() %>% 
  .[,draw :=  paste0("paf_", draw)]

paf_new <- dcast(paf_new, age_group_id + sex_id + year_id + measure + cause_id ~ draw, value.var = "new_paf")

paf_full <- paf_full[!cause_id %in% causes]%>%
  rbind(., paf_new) %>%
  .[cause_id != 521 & cause_id != 704 & cause_id != 417]

paf_full[cause_id == 421, cause_id := 1021]

## Write data --------------------------------------------------


for (m in c("yld", "yll")){
    paf_sub <- paf_full[measure == m] %>%
      .[,measure := NULL]
    write.csv(paf_sub, 'FILEPATH', row.names = F)
}

