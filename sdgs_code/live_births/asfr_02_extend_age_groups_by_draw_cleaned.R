################################################################################
## Purpose: Calculates 10-14 and 50-54 ASFR 
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr, parallel, haven, splines) # load packages and install if not installed

root <- "FILEPATH"
username <- "FILEPATH"

################################################################################
### Arguments 
################################################################################

locsetid <- 21
mod_id <- 12
################################################################################ 
### Paths for input and output 
################################################################################
dir <- "FILEPATH"
cwdir <- "FILEPATH"
################################################################################
### Functions 
################################################################################

source("FILEPATH") # load get_locations
codes <- get_locations(2016) %>% as.data.table %>% .[, .(ihme_loc_id, location_id)]

#######################
## Step 1: Load Crosswalk
#######################

load("FILEPATH")


################################################################################
### Data 
################################################################################
setwd("FILEPATH")
asfr_final <- list.files(getwd(), "[[:digit:]]") %>% mclapply(., function(file) {
    
    df <- fread(file)
    df <- melt(df, id.vars = c("location_id", "year_id", "age_group_id"), variable.name = "draw", value.name = "asfr")
    df[, age_group := as.character(5*(age_group_id-5))]
    asfr_w <- dcast(df, location_id + year_id + draw ~ age_group, value.var = "asfr")
    setnames(asfr_w, grep("[[:digit:]]", names(asfr_w), value = T), paste("asfr", grep("[[:digit:]]", names(asfr_w), value = T), sep = "_"))
    
    ## PREDICT
    asfr_w[, asfr_10 := predict(young.loess, newdata = asfr_w)]
    asfr_w[, asfr_50 := predict(old.loess, newdata = asfr_w)]
    
    ## CLEAN
    fin <- melt.data.table(asfr_w, id.vars = c('year_id', 'location_id', 'draw'), variable.name = "age_group_id", value.name = "asfr")
    fin[, age_group_id := as.numeric(gsub("asfr_", "", age_group_id))]
    fin[, age_group_id := (age_group_id/5) + 5]
    
    #clean for merge with upload template
    fin[, sex_id := 2]
    
    
    
    print(paste0(file, ", Done."))
    
    return(fin)
    
}, mc.cores = 15) %>% rbindlist 


######################
## Step 4: Save Draws by Age group ID
######################

dir.create("FILEPATH")

dfs <- split(asfr_final, by = "age_group_id")

mclapply(dfs, function(df) {
    
    saveRDS("FILEPATH")
}, mc.cores = 15)

#######################
## Step 5: Collapse Draws for Upload
#######################

collapsed <- asfr_final[, .(mean_value = mean(asfr), lower_value = quantile(asfr, .025), upper_value = quantile(asfr, .975)), 
                        by = .(location_id, year_id, age_group_id, sex_id)]


######################
## Step 5: Create all Age-Sex Template to Be Used in Upload
######################

upload_template <- expand.grid(year_id = c(1950:2016),
                               sex_id = c(1,2),
                               age_group_id = c(2:20, 30:32, 235),
                               location_id = codes[, unique(location_id)]) %>% as.data.table

upload <- merge(upload_template, collapsed, by = c("location_id", "year_id", "age_group_id", "sex_id"), all.x = T)

upload[is.na(mean_value), mean_value := 0]

upload[, ':=' (covariate_id = 13, covariate_name_short = "ASFR")]

upload <- upload[, .(location_id, year_id, sex_id, age_group_id, mean_value, lower_value, upper_value, covariate_name_short, covariate_id)]

######################
## Step 6: Write
######################

write.csv(upload, "FILEPATH")


################################################################################ 
### End
################################################################################