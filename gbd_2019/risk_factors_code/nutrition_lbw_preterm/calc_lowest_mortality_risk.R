rm(list=ls())

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_demographics.R"  )


# --------------------------
# Change column names & drop unnecessary columns 

keep_needed_cols <- function(data){
  
  data <- data[, list(ga_bw, dim_x1, dim_x2, gpr_y_obs, gpr_y_std)]
  
  return(data)
  
}

change_col_names <- function(data){
  
  setnames(data, c("dim_x1", "dim_x2", "gpr_y_obs", "gpr_y_std"), c("ga", "bw", "ln_mortality_odds", "ln_mortality_odds_std_err"))
  
  return(data)
  
}

convert_to_risk <- function(x){ 
  x = x / (1 + x)  
  return(x) 
}

drop_non_me_bins <- function(data){
  
  me_map <- fread("FILEPATH")
  
  data <- merge(me_map[!is.na(modelable_entity_id), list(ga_bw, modelable_entity_id, parameter)], data, all.x = T)
  
  return(data)
}

return_draws <- function(data, num_draws = 999, tmrels = NULL){
  
  newvars <- paste0("draw_",c(0:num_draws))
  
  data[, (newvars) := as.list(exp(rnorm(num_draws,ln_mortality_odds,ln_mortality_odds_std_err))), by = list(ga,bw)]
  
  data[, (newvars) := lapply(.SD, "convert_to_risk"), .SDcols = newvars]
  
  mins <- data[ga_bw %in% tmrels, lapply(.SD, max), .SDcols = newvars ]
  
  data[, (newvars) := mget(newvars) / mins]
  
  
  return(data)
  
}

set_tmrels_and_less_than_1_to_1 <- function(data, tmrels){
  
  source("FILEPATH/find_non_draw_cols.R")
  
  id_cols <- find_non_draw_cols(data, label = "draw_")
  
  data <- melt(data, id.vars = id_cols)
  
  data[value < 1 | ga_bw %in% tmrels, value := 1]
  
  data <- dcast(data, as.formula(paste(paste(id_cols, collapse = "+"), "~variable")), value.var = "value")
  
  return(data)
  
}

prep_3d_matrix <- function(data, reverse = F, threshold_for_nas = NA){
  
  grid <- expand.grid(dim_x1 = unique(data$dim_x1), dim_x2 = unique(data$dim_x2))
  data <- data[order(dim_x1, dim_x2), .(y_obs, dim_x1, dim_x2)]
  
  data <- merge(data, grid, all = T)
  
  if(reverse == T){
    data <- data[order(dim_x1, dim_x2, decreasing = T)]
  }
  
  data.matrix <- matrix(data = data$y_obs, nrow = nrow(data[, .N, by = dim_x2]), ncol = nrow(data[, .N, by = dim_x1]), dimnames = list(unique(data$dim_x2), unique(data$dim_x1)))
  
  if(is.integer(threshold_for_nas)){
    data.matrix[data.matrix <= threshold_for_nas] <- NA
  }
  
  return(data.matrix)
  
}

prep_for_upload <- function(data, cause.list){
  
  rr_metadata <- data.table(expand.grid(location_id = 1, rei_id = 339, mortality = 1, morbidity = 0, cause_id = cause.list, year_id = year_ids, sex_id = sex, age_group_id = age))
  
  cols_to_keep <- names(data)[grepl(names(data), pattern = "draw_|ga_bw|parameter")]
  
  data <- data[, cols_to_keep, with = F]
  data[, location_id := 1]
  
  data <- merge(rr_metadata, data, by = "location_id", allow.cartesian = TRUE)
  
  return(data)
  
}


tmrels = c("38_40_3500_4000", "38_40_4000_4500", "40_42_3500_4000", "40_42_4000_4500")
cause.list = c(302, 322, 328, 329, 332, 337, 381, 382, 383, 384, 385, 686)
year_ids <- get_demographics(gbd_team = "epi", gbd_round_id = 6)$year_id


# Vet 

vet_gbd19_gbd17 <- function(age, sex){
  
  gbd19rr <- fread(paste0("FILEPATH"))
  gbd17rr <- fread(paste0("FILEPATH"))
  
  gbd19rr_cols = find_non_draw_cols(gbd19rr)
  gbd19rr <- melt(gbd19rr, id.vars = gbd19rr_cols)
  gbd19rr <- gbd19rr[, .(value = mean(value)), by = eval(gbd19rr_cols)]
  
  gbd17rr_cols = find_non_draw_cols(gbd17rr)
  gbd17rr <- melt(gbd17rr, id.vars = gbd17rr_cols)
  gbd17rr <- gbd17rr[, .(value = mean(value)), by = eval(gbd17rr_cols)]
  
  gbd19rr[, round := "gbd19"]
  gbd17rr[, round := "gbd17"]
  
  gbdrrs <- rbind(gbd17rr, gbd19rr, fill = T)
  gbdrrs <- gbdrrs[year_id == 2010 & cause_id == cause.list[[1]]]
  
  me_map <- fread("FILEPATH")
  
  gbdrrs <- merge(gbdrrs[, -c("modelable_entity_id")], me_map)
  
  gbdrrs <- gbdrrs[order(ga_label, bw_label)]
  
  pdf(paste0("FILEPATH"), width = 20, height = 18)
  
  gg <- ggplot(gbdrrs) + 
    geom_col(aes(x = ga_bw, y = value, fill = round), position = "dodge") + 
    theme(axis.text.x = element_text(angle = 90, hjust = 1))
  
  print(gg)
  
  dev.off()
  
}


for(age in 2:3){
  
  for(sex in 1:2){
    
    input_file <- paste0("FILEPATH") 
    
    dt <- fread(input_file)
    
    dt <- keep_needed_cols(dt)
    
    dt <- change_col_names(dt)
    
    dt <- drop_non_me_bins(dt)
    
    dt <- return_draws(dt, num_draws = 999, tmrels = tmrels)
    
    dt <- prep_for_upload(dt, cause.list)
    
    write.csv(dt[, -c("ga_bw")], paste0("FILEPATH"), row.names = F)
    
    dt <- set_tmrels_and_less_than_1_to_1(data = dt, tmrels)
    
    write.csv(dt[, -c("ga_bw")], paste0("FILEPATH"), row.names = F)

    vet_gbd19_gbd17(age = age, sex = sex)

    
  }
}


