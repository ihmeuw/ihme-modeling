###############################################################################################################
## The script calculates asymptomatic and symptomatic proportions of BPH based on four population-based studies
###############################################################################################################

library(plyr)
library(msm, lib.loc="FILEPATH")
library(metafor, lib.loc="FILEPATH")
library(readxl)
library(data.table)
library(dplyr)
library(gtools) 



###########################################################################################################
## Step 1: Prep each country data by age groups
###########################################################################################################
#Japen
    japan <- as.data.table(read.csv("FILEPATH"))
    japan <- japan %>%
      arrange(I.PSS) %>%
      mutate(percent_japan = cum_percent - lag(cum_percent, default = first(cum_percent)))
    japan$percent_japan[japan$percent_japan<0] <-0
    
    japan$total_sample_size <- 239
    japan <- as.data.table(japan)
    japan[, sample_size := total_sample_size * percent_japan]
    japan[, total := sample_size * I.PSS]
    sum(japan$total)/239
    round(sum(japan$total)/239, digits = 0)
    
    ## Calculate se: SEp = sqrt [ p(1 - p) / n ] from https://stattrek.com/estimation/standard-error.aspx
    japan[,  se := sqrt((0.637*(1-0.637)) /239)]
    japan[,  se_symp := sqrt((0.363*(1-0.363)) /239)]
    
    
#France
    france <- as.data.table(read.csv("FILEPATH"))
    france <- france %>%
      arrange(I.PSS) %>%
      mutate(percent_france = cum_percent - lag(cum_percent, default = first(cum_percent))) 
    france$percent_france[france$percent_france<0] <-0
    
    
    france$total_sample_size <- 1793
    france <- as.data.table(france)
    france[, sample_size := total_sample_size * percent_france]
    france[, total := sample_size * I.PSS]
    sum(france$total)/1793
    round(sum(france$total)/1793, digits = 0)
    
    ## Calculate se: SEp = sqrt [ p(1 - p) / n ] from https://stattrek.com/estimation/standard-error.aspx
    france[,  se := sqrt((0.6905*(1-0.6905)) /1793)]
    france[,  se_symp := sqrt((0.3095*(1-0.3095)) /1793)]
    
#Scotland
    Scotland <- as.data.table(read.csv("FILEPATH"))
    Scotland <- Scotland %>%
      arrange(I.PSS) %>%
      mutate(percent_scot = cum_percent - lag(cum_percent, default = first(cum_percent))) 
    Scotland$percent_scot[Scotland$percent_scot<0] <-0
    
    Scotland$total_sample_size <- 1337
    Scotland <- as.data.table(Scotland)
    Scotland[, sample_size := total_sample_size * percent_scot]
    Scotland[, total := sample_size * I.PSS]
    sum(Scotland$total)/1337
    round(sum(Scotland$total)/1337, digits = 0)
    
    ## Calculate se: SEp = sqrt [ p(1 - p) / n ] from https://stattrek.com/estimation/standard-error.aspx
    Scotland[,  se := sqrt((0.720946*(1-0.720946)) /1337)]
    Scotland[,  se_symp := sqrt(( 0.279054*(1- 0.279054)) /1337)]
    
    
#USA
    USA <- as.data.table(read.csv("FILEPATH"))
    USA <- USA %>%
      arrange(I.PSS) %>%
      mutate(percent_USA = cum_percent - lag(cum_percent, default = first(cum_percent))) 
    USA$percent_USA[USA$percent_USA<0] <-0
    
    
    USA$total_sample_size <- 1312
    USA <- as.data.table(USA)
    USA[, sample_size := total_sample_size * percent_USA]
    USA[, total := sample_size * I.PSS]
    sum(USA$total)/1312
    
    ## Calculate se: SEp = sqrt [ p(1 - p) / n ] from https://stattrek.com/estimation/standard-error.aspx
    USA[,  se := sqrt((0.627154*(1-0.627154)) /1312)]
    USA[,  se_symp := sqrt((0.372846*(1-0.372846)) /1312)]
    round(sum(USA$total)/1312, digits = 0)

    
#################################################################################################
## MR-BRT
#################################################################################################
    data <- as.data.table(read_excel("FILEPATH"))
    
    data$log_ratio <- log(data$proportion)
    
    # Log standard error
    data$delta_log_se <- sapply(1:nrow(data), function(i) {
      ratio_i <- data[i, "proportion"]
      ratio_se_i <- data[i, "standard_error"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })
    
    
    repo_dir <- "FILEPATH"
    source(paste0(repo_dir, "mr_brt_functions.R"))
    
    fit1 <- run_mr_brt(
      output_dir = paste0("FILEPATH"),
      model_label = paste0("BPH_severity_split_symp"),
      data = data,
      mean_var = "log_ratio",
      se_var = "delta_log_se",
      overwrite_previous = TRUE,
      method = "trim_maxL",
      study_id = "study_id"
    )    
    
    
    
#################################################################################################
## Asymptomatic proportion calculation
#################################################################################################  
    orig<- 0.4722187  #GBD 2017 proportion
    se <- (0.4866667-0.4593333)/(2*1.96) #GBD 2017 se
    scale <- (1-0.327)/orig
    draws <- rnorm(1000,orig, se)
    draws1 <- draws*scale
    quantile(draws1, c(0.025, 0.975)) 
    
    
    
#################################################################################################
## Save raw data to a bundle
#################################################################################################     
    
    scot <- as.data.table(read.csv("FILEPATH"))
    japan <- as.data.table(read.csv("FILEPATH"))
    usa <- as.data.table(read.csv("FILEPATH"))
    france <- as.data.table(read.csv("FILEPATH"))
    
    scot$location_id <- 434
    japan$location_id <- 35424
    usa$location_id <- 546
    france$location_id <- 80
    
    usa$year_start <- 1993
    usa$year_end <- 1993
    usa$note_sr <- "year represents the year of publication; couldn't access full paper"
    france$year_start <- 1992 
    france$year_end <- 1992 
    france$note_sr <- "year from abstract; couldn't access full paper"
    japan$year_start <- 1992
    japan$year_end <- 1993
    scot$year_start <- 1990
    scot$year_end <- 1990
    
    usa$age_start <- 40
    usa$age_end <- 79
    france$age_start <- 50
    france$age_end <- 84
    japan$age_start <- 40
    japan$age_end <- 79
    scot$age_start <- 40
    scot$age_end <- 79
    
    usa$sample_size <- 2115
    france$sample_size <- 2011
    japan$sample_size <- 289
    scot$sample_size <- 1994
    
    master <- rbind.fill( scot,japan, usa,france)
    master <- as.data.table(master)
    
    ## For uploader validation - add in needed columns
    master$seq <- NA
    master$sex <- "Male"
    locs <- read.csv("FILEPATH")
    master <- join(master, locs[,c("location_id", "location_name")], by="location_id")
    master$nid <- 424287
    master$year_issue <- 0
    master$age_issue <- 0
    master$sex_issue <- 0
    master$measure <- "proportion"
    master$source_type <- "Survey - cross-sectional"
    master$extractor <- "USERNAME"
    master$is_outlier <- 0
    master$cases <- master$sample_size * master$mean
    master$measure_adjustment <- 0
    master$uncertainty_type <- "Confidence interval"
    master$recall_type <- "Point"
    master$urbanicity_type <- "Mixed/both"
    master$unit_type <- "Person"
    master$representative_name <- "Subnationally representative"
    master$note_modeler <- ""
    master$unit_value_as_published <- 1
    master$underlying_nid <- ""
    
write.xlsx(master, "FILEPATH",  sheetName = "extraction")
    
    
