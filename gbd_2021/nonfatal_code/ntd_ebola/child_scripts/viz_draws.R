# Purpose - Visualize fatal draws from run folder for model vetting, total death counts and age/sex proportions
# Plot - 1) current draws directory 2) input data 3) comparison draws directory
#'[ General Set-Up]

# clear environment
rm(list = ls())

# set run dir -- current draws directory
data_root <- "FILEPATH"
run_file <- fread(paste0(data_root, "FILEPATH"))
run_path <- run_file[nrow(run_file), run_folder_path]
params_dir <- paste0(FILEPATH'/params')
draws_dir <- paste0(run_path, '/draws')
interms_dir <- paste0(run_path, '/interms')
data_dir   <- paste0(params_dir, "/data")   

# set round id
gbd_round_id <- ADDRESS

# load packages
library(ggplot2)
library(gridExtra)
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")

#### Main Execution

# input data
ebola_death <- fread(paste0(data_dir, "FILEPATH"))
wa_death       <- fread(paste0(data_dir, "FILEPATH"))
imported_death <- fread(paste0(data_dir, "FILEPATH"))
input_all_death <- rbind(ebola_death, wa_death, imported_death, fill = TRUE)
death_location_ids <- unique(input_all_death[,location_id])

# comparison draws directory
comp_draws_dir <- 'FILEPATH'
  
# function to plot
sum_graph <- function(loc_id, comp_draws_dir, input_data, curr_draws_dir){
  
  loc_name <- input_data[location_id == loc_id, location_name]
  case_data <- input_data[location_id == loc_id]
  case_data <- case_data[, .('sum_cases' = sum(mean)), by = 'year_id']
  
  draws <- fread(paste0(curr_draws_dir, "/deaths/", loc_id,".csv"))
  sum_draws <- draws[, lapply(.SD, sum), by = "year_id", .SDcols = paste0("draw_", 0:999)]
  mean_draws <- melt(sum_draws, measure.vars = paste0("draw_", 0:999))
  mean_draws <- mean_draws[, .('val' = mean(value)), by = "year_id"]
  mean_draws[, location_id := loc_id]
  
  comp_draws <- fread(paste0(comp_draws_dir, "/deaths/",loc_id,".csv"))
  comp_sum_draws <- comp_draws[, lapply(.SD, sum), by = "year_id", .SDcols = paste0("draw_", 0:999)]
  comp_mean_draws <- melt(comp_sum_draws, measure.vars = paste0("draw_", 0:999))
  comp_mean_draws <- comp_mean_draws[, .('val' = mean(value)), by = "year_id"]
  comp_mean_draws[, location_id := loc_id]
  setnames(comp_mean_draws, 'val', 'val_comp')
  
  graph_data <- merge(mean_draws, case_data, by = 'year_id', all.x = TRUE)
  graph_data <- merge(graph_data, comp_mean_draws, by = c('location_id','year_id'))
  
  p <- ggplot(graph_data, aes(x = year_id,  y = val)) + # current draws
    geom_point(size = 1, col = 'blue') +
    geom_text(aes(label = round(val,0)), hjust=-.1, vjust=1, col = 'blue') +
    geom_point(aes(y = val_comp), size = 2, alpha = .3, col = 'green') +
    geom_text(aes(label = round(val_comp,0)), hjust=0, vjust=0, col = 'green') +
    geom_point(aes(y = sum_cases), size = 1, col = 'red') +
    geom_text(aes(label = round(sum_cases,0)), hjust=0, vjust=-1, col = 'red') +
    theme_bw() +
    theme(axis.text.x = element_text(angle = 90, vjust = 0.5)) +
    scale_x_continuous(labels = as.character(1980:2022), breaks = 1980:2022) +
    labs(x = "Year_Id", y = "Deaths", title = paste0("Deaths by Year for ", loc_name,"\nblue - current estimate ; red - raw data; green - gbd 2020 july handoff")) +
    scale_y_continuous(breaks = scales::pretty_breaks(n = 5))
  

  print(p)
}


pdf(paste0(interms_dir, "FILEPATH"))
for (lid in death_location_ids){
  sum_graph(loc_id = lid, input_data = input_all_death, curr_draws_dir = draws_dir, comp_draws_dir = comp_draws_dir)
  Sys.sleep(1)
}
dev.off()
cat('wrote viz_death_draws')


# age proportions

# look at age proportions by country results
age_md <- get_age_metadata(gbd_round_id = gbd_round_id, age_group_set_id = 19)
setnames(age_md, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_md <- age_md[,.(age_group_id, age_start, age_end)]

loc_md <- get_location_metadata(35)
loc_md <- loc_md[,.(location_id, location_name)]

calc_age_proportion <- function(loc_id, comp_results, input_data, curr_draws_dir){
  
  draws <- fread(paste0(curr_draws_dir, "/deaths/",loc_id,".csv"))
  sum_draws <- draws[, lapply(.SD, sum), by = c("age_group_id", "sex_id"), .SDcols = paste0("draw_", 0:999)]
  mean_draws <- melt(sum_draws, measure.vars = paste0("draw_", 0:999))
  mean_draws <- mean_draws[, .('val' = mean(value)), by = c("age_group_id", "sex_id")]
  mean_draws[, location_id := loc_id]
  # add location name
  loc_name <- loc_md[location_id == loc_id, location_name]
  
  # make proportions
  mean_draws[, val := val / sum(val)]
  
  # include age information
  mean_draws <- merge(mean_draws, age_md, by = "age_group_id", all.x = TRUE) 
  mean_draws[age_group_id == 4, c("age_start", "age_end") := .(0.0767, 1)]
  mean_draws[age_group_id == 5, c("age_start", "age_end") := .(1, 5)]
  
  p <- ggplot(mean_draws, aes(as.factor(round(age_start,3)), val, fill = as.factor(sex_id))) +
    geom_bar(stat = "identity", position = "dodge") +
    labs(x = "age_start", y = "proportion", title = paste0("Age-Sex proportions in estimates across all years for ", loc_name), fill = "sex_id") +
    theme_bw()
  
  print(p)
}


pdf(paste0(interms_dir, "FILEPATH"))
for (lid in death_location_ids){
  calc_age_proportion(loc_id = lid, curr_draws_dir = draws_dir)
  Sys.sleep(1)
}
dev.off()
cat('wrote viz_age_sex_draws')
