################################################################################
## Purpose: Scales outputs of GPR down location hierarchy, accounting for split
## in Indian states AP and Telangana from 2014
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, parallel, boot, magrittr, ggplot2, RMySQL) # load packages and install if not installed
 
root <- "FILEPATH"
username <- "FILEPATH"

code_dir <- "FILEPATH"
source("FILEPATH") # Rake function
source("FILEPATH") # Locations function

################################################################################
### Arguments 
################################################################################
 
cores <- 5
mod_id <- commandArgs()[3]
rake <- T
scale_ap_tel <- T
tfr_bound <- 9.5

################################################################################ 
### Paths for input and output 
################################################################################

input_path <- "FILEPATH"

################################################################################
### Data 
################################################################################

if (rake) {
    
    if (scale_ap_tel) {
        
        ap_loc_ids <- c(4841, 43908, 43872) #A, AR, AU
        tel_loc_ids <- c(4871, 43938, 43902) #T, TR, TU
        ap_total <- fread("FILEPATH")
        ap_total <- ap_total[ihme_loc_id %in% paste0("IND_",ap_loc_ids)]
        
        all_draws <- list.files(getwd(), pattern = "sim")
        ap_tel_patterns <- paste0("IND_", c(ap_loc_ids, tel_loc_ids))
        ap_tel_draws <- lapply(ap_tel_patterns, function (x) grep(x, all_draws, value = T)) %>% unlist
        ap_tel_draws <- mclapply(ap_tel_draws, fread, mc.cores = cores) %>% rbindlist
        
        write.csv(ap_tel_draws, "FILEPATH", row.names = F)
        
        setnames(ap_total, c("ihme_loc_id","year_id", "pre_rake"), c("ap_loc", "year", "ap_tot"))
        ap_total[, year := year + .5]
        ap_total[, c("scl", "amp") := NULL]
        ap_total[ap_loc %like% 4841, ':=' (tel_loc = "IND_4871", type = 'T')] #applying same reference for AP total to combined, U, and Rural for both states
        ap_total[ap_loc %like% 43908, ':='(tel_loc = "IND_43938", type = 'R')]
        ap_total[ap_loc %like% 43872, ':='(tel_loc = "IND_43902", type = 'U')]
        ap_total <- melt.data.table(ap_total, id.vars = c("year", "ap_tot", "type"), variable.name = "state_loc", value.name = "ihme_loc")
        
        ap_tel_pop <- get_population(location_id = c(ap_loc_ids, tel_loc_ids), year_id = -1, age_group_id = c(8:14), sex_id = 2, location_set_id = 21)
        ap_tel_pop <- ap_tel_pop[, .(population = sum(population), ihme_loc = paste0("IND_", location_id)), by = .(location_id, year_id)]
        setnames(ap_tel_pop, "year_id", "year")
        ap_tel_pop[, year := year + .5]
        ap_tel_pop[, location_id := NULL]
        
        ap_tel_draws <- merge(ap_tel_draws, ap_tel_pop, by = c("ihme_loc", "year"))
        ap_tel_draws[, draw_mean := mean(mort), by = .(ihme_loc, year)]
        ap_tel_draws <- merge(ap_tel_draws, ap_total, by = c("ihme_loc", "year"))
        ap_tel_draws[, child_sum := sum(draw_mean*population)/sum(population), by = .(year, type)]
        ap_tel_draws[, scale_ratio := ap_tot/child_sum]
        ap_tel_draws[, mort := mort * scale_ratio]
        ap_tel_draws <- ap_tel_draws[, .(ihme_loc, year, sim, mort)]
        
        lapply(ap_tel_patterns, function(x) {write.csv(ap_tel_draws[ihme_loc == x], paste0("gpr_", x, "_1_10_sim.txt") , row.names = F)})
        
        }
    
    gpr <- run_rake(get.df = T, collapse.draws = T)
    write.csv(gpr, "FILEPATH", row.names = F)
    
    list_data_rich <- mclapply(list.files(input_path, pattern = "5.txt"), fread, mc.cores = cores) %>% rbindlist %>% .[, .(year_id = year - .5, ihme_loc_id, pre_rake = med, scl = 5, amp = 1)]
    list_data_poor <- mclapply(list.files(input_path, pattern = "10.txt"), fread, mc.cores = cores) %>% rbindlist %>% .[, .(year_id = year - .5, ihme_loc_id, pre_rake = med, scl = 10, amp = 1)]
    pre_rake <- rbindlist(list(list_data_rich, list_data_poor))
    write.csv(pre_rake, paste0(root, "FILEPATH"), row.names = F)
    
} else {
    
    list_2_20 <- mclapply(list.files(input_path, pattern = "_2_20.txt"), fread, mc.cores = cores) %>% rbindlist
    list_1_5 <- mclapply(list.files(input_path, pattern = "_1_5.txt"), fread, mc.cores = cores) %>% rbindlist 
 gpr <- rbindlist(list(list_2_20, list_1_5))
    setnames(gpr, "year", "year_id")
    gpr[, year_id := year_id - .5]
    
}

################################################################################ 
### End
################################################################################