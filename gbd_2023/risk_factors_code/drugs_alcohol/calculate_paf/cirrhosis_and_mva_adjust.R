######################################################
### Title: Cirrhosis & Liver Cancer Redistribution
######################################################

arguments <- commandArgs(trailingOnly = TRUE)

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
source('FILEPATH')

## Variables that change------------------------------
testing <- F
annual <- F
pause_yld <- F


release <- version
# draw_num <- 100

cod_correct_version <- version
cod_year_end <- 2024

como_year_end <- 2023
como_version <- version 


## Read arguments and make necessary variables--------

if (testing){
  paf_directory <- 'FILEPATH'
  location <- 102
} else {
  paf_directory <- arguments[1]
  task_map_fp <- arguments[2]
  draw_num <- arguments[3] %>% as.integer
  task_map <- fread(task_map_fp)
  
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  
  location   <- task_map[task_id, location]
}

print(location)
years       <- c(1990,1995,2000,2005,2010,2015,2020,2022,2023,2024)

measures      <- c(3, 4) #yld & yll
sexes         <- c(1, 2)
causes        <- c(417, 521) #liver cancer & cirrhosis, parents causes to be re-distributed. 
ages          <- c(8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)


## Read in PAFs pt 1 (completeness check) -----------------------------------

files <- list.files(paste0('FILEPATH'))  %>%
  .[grep(paste0("_",location,"_"), .)]

print(paste0("Loading ", length(files), " files from specified directory and locations"))

paf_full <- rbindlist(lapply(files, aggregateFiles)) 
paf_full <- paf_full[year_id %in% years]

nrow(paf_full)


if (!nrow(paf_full) == expect1){
  print("Incorrect number of rows- error!")
  
  table(paf_full$year_id)
  test <- paf_full[year_id %in% c(2023, 2024)]
  table(test$sex_id, test$cause_id)
  
  quit()
}

## Read in burden data -----------------------------------
cause_list <- c(522, 523, 524, 525, 971, 418, 419, 420, 421, 996) # causes to be re-distributed to
# cause_metadata[cause_id %in% cause_list]
pops <- get_population(location_id=location, year_id=years, sex_id = sexes, age_group_id = ages, release_id = release)


if (!pause_yld){
yld <- rbindlist(lapply(cause_list, read_burden, annual = annual, measure_id = 3, years = years)) 
unique(yld$year_id)

# duplicate results of 2023 for 2024 (NEED TO CHECK NEW VERION LATER)
temp <- yld[year_id == 2023]
temp$year_id <- 2024
yld <- rbind(yld, temp)

yld <- yld[year_id %in% years] %>%
  num_yld(., pops)
} 

yll <- rbindlist(lapply(cause_list, read_burden, annual = annual, measure_id = 4, years = years)) 



yll[, version_id := NULL]
yll <- yll[year_id %in% years] %>%
  melt(id.vars = c("age_group_id", "cause_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id"))  

if (pause_yld){
  yld <- copy(yll)
  yld$measure_id <- 3
}


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
summary(cause_total)

## Read in PAFs --------------------------------------------- 



paf_full$year_id %>% table

print(paste0("Defined data frame with ", nrow(paf_full), " observations, and ", ncol(paf_full), " variables"))


paf <- paf_full[cause_id %in% causes] %>%
  melt(id.vars = c("year_id", "age_group_id", "cause_id", "sex_id", "measure")) %>%
  setnames(old = c("variable", "value"), new = c("draw", "paf")) %>%
  .[,sex_id := as.numeric(sex_id)] %>%
  .[,draw :=  gsub("paf_", "", draw) %>% as.numeric] %>%
  .[cause_id == 524, cause_id := 521] %>% 
  .[cause_id == 420, cause_id := 417] %>%
  .[cause_id == 521, parent := "cirrhosis"] %>%
  .[cause_id == 417, parent := "liver_cancer"] %>%
  .[,cause_id := NULL]


## Combine the burden and PAF data ------------------------------
paf_new <- merge(paf, cause_total, by = c("age_group_id", "year_id", "sex_id", "draw", "measure", "parent"), all.x = T) %>%
  redistribute_pafs() %>% 
  .[,draw :=  paste0("paf_", draw)]

nas <- paf_new[is.na(paf$value)]
if (nrow(nas)> 0){
  message(paste0("There are ", nrow(nas), " NAs in the PAF data. Exiting."))
  quit()
  
}

paf_new <- dcast(paf_new, age_group_id + sex_id + year_id + measure + cause_id ~ draw, value.var = "new_paf")

paf_full <- rbindlist(list(paf_full[!cause_id %in% causes], paf_new), use.names = T) %>%
  .[, measure_id := ifelse(measure=="yld",3,4)] %>%
  .[cause_id != 521 & cause_id != 417 & cause_id != 704] 



## Write data --------------------------------------------------
paf_full[,.(year_id, age_group_id, cause_id, measure, sex_id)]
table(paf_full$measure, paf_full$sex_id)
table(paf_full$year_id, paf_full$age_group_id)
table(paf_full$cause_id)


if(nrow(paf_full) == expect){
  paf_full[,measure := NULL]
  paf_draw <- names(paf_full)[grepl("paf_", names(paf_full))]
  draw <- gsub("paf_", "draw_", paf_draw)
  setnames(paf_full, old = paf_draw, new = draw)
  fwrite(paf_full, paste0('FILEPATH'), row.names = F)
  
  fwrite(paf_full[1,1:3], paste0('FILEPATH'), row.names = F)
} else {
  print("Wrong number of rows-- error!")
}

print("done!")



