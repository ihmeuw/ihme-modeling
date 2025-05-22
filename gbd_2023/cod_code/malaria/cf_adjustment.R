
################################################################################
##****** load libraries, source functions, etc.
rm(list=ls())

library(data.table)
library(dplyr)
library(reshape)
library(ggplot2)
source("ADDRESS/get_draws.R")
source("ADDRESS/get_location_metadata.R")
source("ADDRESS/get_model_results.R")
source("ADDRESS/get_population.R")
source("ADDRESS/get_age_metadata.R")
source("ADDRESS/get_envelope.R")
source("ADDRESS/get_outputs.R")
source("ADDRESS/get_cod_data.R")

# functions
mean_ui <- function(x){
  y <- dplyr::select(x, starts_with("draw"))
  y$mean <- apply(y, 1, mean)
  y$lower <- apply(y, 1, function(x) quantile(x, c(.025)))
  y$upper <- apply(y, 1, function(x) quantile(x, c(.975)))
  
  w <- y[, c('mean', 'lower', 'upper')]
  
  z <- dplyr::select(x, -contains("draw"))
  
  v <- cbind(z, w)
  v <- as.data.table(v)
  return(v)
  
}

scaleFUN <- function(x) sprintf("%.2f", x)

plot_compare_input <- function(plot_data, sex, measure){
  plot_data <- as.data.table(plot_data)
  plot_data$year_id <- as.factor(plot_data$year_id)
  if (measure == "deaths"){
    m_label <- "Deaths"
  } else{
    m_label <- measure
  }
  
  pdf(paste0(plot_outpath, "malaria_cf_adjusted_", measure, "_vs_prev_adj_", sex, "_", date,".pdf"),width=12,height=8)
  for (loc in unique(plot_data$location_name)){
    message(paste0("\nPlotting ", loc))
    p <- ggplot(data = plot_data[location_name == loc,], aes(x = year_id, y = mean, group=interaction(version), 
                                                             color = version), alpha= 0.75) +
      geom_ribbon(data=plot_data[location_name == loc & version == "Previous adj",], aes(x=year_id, ymin=lower, ymax=upper, fill = "Previous adj"),
                  alpha = 0.15, colour = NA) +
      geom_ribbon(data=plot_data[location_name == loc & version == "Adjusted",], aes(x=year_id, ymin=lower, ymax=upper, fill = "Adjusted"),
                  alpha = 0.15, colour = NA) +
      facet_wrap(~age_group_name, scales = "free_y") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, vjust = 0.65))+
      scale_x_discrete(breaks= c(1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022,2024))+
      scale_y_continuous(labels=scaleFUN)+
      ggtitle(paste0("Comparing Malaria adjusted fatal ", measure, " vs previous adjusted; sex: ", sex, ", location: ", loc))+
      ylab(paste0(m_label)) + xlab("Year") +
      scale_color_manual(values = c("New capped" = "red", "Old capped" = "blue")) +
      scale_fill_manual(values = c("New capped" = "red", "Old capped" = "blue"))+
      labs(subtitle = "CFs capped to max spline adjusted CoD CF (any loc <5, MOZ >5) by age/sex")
    print(p)
    
  }
  dev.off()
}

plot_compare_input_noci <- function(plot_data, sex, measure){
  plot_data <- as.data.table(plot_data)
  plot_data$year_id <- as.factor(plot_data$year_id)
  if (measure == "deaths"){
    m_label <- "Deaths"
  } else{
    m_label <- measure
  }
  
  pdf(paste0(plot_outpath, "malaria_cf_adjusted_", measure, "_vs_no_cap_no_ci_", sex, "_", date,".pdf"),width=12,height=8)
  for (loc in unique(plot_data$location_name)){
    sub <- plot_data[plot_data$location_name==loc,]
    message(paste0("\nPlotting ", loc))
    p <- ggplot(data = sub, aes(x = year_id, y = mean, group=interaction(version), 
                                color = version), alpha= 0.75) +
      geom_line()+
      facet_wrap(~age_group_name, scales = "free_y") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, vjust = 0.65))+
      scale_x_discrete(breaks= c(1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022,2024))+
      scale_y_continuous(labels=scaleFUN)+
      ggtitle(paste0("Comparing Malaria capped fatal ", measure, " vs no cap; sex: ", sex, ", location: ", loc))+
      ylab(paste0(m_label)) + xlab("Year") +
      scale_color_manual(values = c("New capped" = "red", "Not capped" = "blue"))+
      labs(subtitle = "CFs capped to max spline adjusted CoD CF (any loc <5, MOZ >5) by age/sex")
    print(p)
    
  }
  dev.off()
}



plot_3_noci <- function(plot_data, sex, measure){
  plot_data <- as.data.table(plot_data)
  plot_data$year_id <- as.factor(plot_data$year_id)
  if (measure == "deaths"){
    m_label <- "Deaths"
  } else{
    m_label <- measure
  }
  
  pdf(paste0(plot_outpath, "malaria_cf_adjusted_", measure, "_vs_nocap_oldcap_no_ci_", sex, "_", date,".pdf"),width=12,height=8)
  for (loc in unique(plot_data$location_name)){
    sub <- plot_data[plot_data$location_name==loc,]
    message(paste0("\nPlotting ", loc))
    p <- ggplot(data = sub, aes(x = year_id, y = mean, group=interaction(version), 
                                color = version), alpha= 0.75) +
      geom_line()+
      facet_wrap(~age_group_name, scales = "free_y") +
      theme_bw() +
      theme(axis.text.x = element_text(angle = 90, vjust = 0.65))+
      scale_x_discrete(breaks= c(1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2022,2024))+
      scale_y_continuous(labels=scaleFUN)+
      ggtitle(paste0("Comparing Malaria capped fatal ", measure, " vs no cap and Old cap; sex: ", sex, ", location: ", loc))+
      ylab(paste0(m_label)) + xlab("Year") +
      scale_color_manual(values = c("New capped" = "red", "Old capped" = "blue", "Not capped" = "darkgreen"))+
      labs(subtitle = "CFs capped to max spline adjusted CoD CF (any loc <5, MOZ >5) by age/sex")
    print(p)
    
  }
  dev.off()
}
################################################################################
##****** Specify params
release_id <- ADDRESS
# # 
param_map <- fread('FILEPATH/malaria_cap_endemic_param.csv')
param_map <- as.data.table(param_map)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))

# ################################################################################
loc <- param_map[task_id, location_id]
# 
message("Now processing location: ", loc)

path <- "FILEPATH"
param_path <- paste0(path, "FILEPATH/")
input_path <- paste0(path, "FILEPATH/")
output_path <- paste0(path, "FILEPATH/")
plot_outpath <- paste0(path, "FILEPATH/")

################################################################################
##****** pull location and age metadata for later plotting
# locs for plotting
locs <- get_location_metadata(release_id = release_id, location_set_id = ADDRESS)
locs <- locs[,.(location_id, location_name, ihme_loc_id)]

#ages for plotting
ages <- get_age_metadata(release_id = release_id)
ages <- ages[,.(age_group_id, age_group_name)]

end_map <- fread(paste0("FILEPATH"))

end_map <- as.data.table(end_map)

end_map <- end_map[ , pf_overall:=sum(pf_endemic), by=c("location_id", "location_name")]
end_map <- end_map[ ,pv_overall:=sum(pv_endemic), by=c("location_id", "location_name")]
end_map <- end_map[ ,end_overall:=sum(any_malaria_endemic), by=c("location_id", "location_name")]

end_map <- end_map[,.(location_id, location_name, pf_overall, pv_overall, end_overall)]
end_map <- end_map %>% group_by(location_id, location_name) %>% unique()
end_map <- as.data.table(end_map)
endem_locs <- end_map[end_map$end_overall >0,]
date <- Sys.Date()

# ################################################################################
# ##****** read in data
message("Reading in data")
df <- fread(paste0(input_path, "1_", loc, ".csv"))

if (loc %in% endem_locs$location_id){
  # read in envelope
  cod_env <- get_envelope(year_id = c(1980:2023),sex_id =  c(1,2,3),
                          location_id =  26,age_group_id =  age_group_ids,
                          release_id=release_id, with_hiv = 1)
  
  # read in max cfs
  max_cf <- fread(paste0(path, "FILEPATH"))
  max_cf <- max_cf[,.(age_group_id, age_group_name, sex_id, pred)]
  setnames(max_cf, "pred", "cap")
  #   ################################################################################
  #   ##****** subset to just problematic data and apply correction
  message("Begin data adjustments")
  # subset envelope to just desired columns
  cod_env  <- cod_env[,.(age_group_id, location_id, year_id, sex_id, mean)]
  setnames(cod_env, "mean", "envelope")
  
  # now subset to just endemic for processing
  df <- as.data.table(df)
  df_age_group2 <- df[df$age_group_id == 2,]
  
  #need to drop age group 2 from processing as it's non-endemic
  
  sub <- df[!(df$age_group_id == 2),]
  
  final_df <- NULL
  
  message(paste0("Now capping ", loc))
  
  # continue processing endemic
  vars <- paste0("draw_", 0:999)
  cols <- colnames(sub)
  cols <- cols[!(cols %in% vars)]
  sub <- melt(sub, id = cols)
  
  setnames(sub, c("variable", "value"), c("draw_id", "draws"))
  
  # merge on to df
  sub <- left_join(sub, cod_env, by = c("location_id", "year_id", "age_group_id", "sex_id"))
  
  # calculate CF
  sub <- as.data.table(sub)
  cols <- colnames(sub)
  cols <- cols[!(cols %in% c("draws", "envelope"))]
  sub[, cf_draws := draws/envelope, by = cols]
  
  
  # now merge on max cf for each age/sex
  adj_df <- left_join(sub, max_cf, by = c("age_group_id", "sex_id"))
  
  # for rows where the cf > max_cf set the cf to be the max_cf
  adj_df$cf_draws <- ifelse((adj_df$cf_draws > adj_df$cap), adj_df$cap, adj_df$cf_draws)
  
  # save a df with adjusted cfs for plotting
  adj_cf_sub <-adj_df
  adj_cf_sub <- as.data.frame(adj_cf_sub)
  #
  #   # re-calculate death counts using new cfs
  cols <- colnames(adj_df)
  cols <- cols[!(cols %in% c("draws", "cf"))]
  adj_df[, adjusted_counts := cf_draws*envelope, by = cols]
  
  # get rid of envelope and max_cf column
  adj_df$envelope <- NULL
  adj_df$cap <- NULL
  adj_df$cf_draws <- NULL
  adj_df$draws <- NULL
  
  #setnames(adj_df, "adjusted_counts", "draws")
  
  # cast back out to wide format
  df_cast <- cast(adj_df, location_id+year_id+age_group_id+sex_id~draw_id, mean)
  
  rm(adj_cf_sub, adj_df, sub)
  
  # do final data adjustments
  # add back on age group 2 data
  final_df <- rbind(df_cast, df_age_group2)
}else{
  final_df <- df
}
# # ################################################################################
# # ##****** save out estimates
message("Saving out results")

write.csv(final_df, paste0(output_path, "1_", loc, ".csv"), row.names = FALSE)

message("Finished saving file")

# # ################################################################################
# # ##****** Plot results along with original to visualize differences
# # # when finished capping, run this portion of code for plotting
# files <- list.files("FILEPATH/", pattern = ".csv", full.names = TRUE)
# temp <- lapply(files, fread, sep=",")
# df <- rbindlist(temp)
# 
# 
# files <- list.files("FILEPATH/", pattern = ".csv", full.names = TRUE)
# temp <- lapply(files, fread, sep=",")
# cap_df <- rbindlist(temp)
# 
# files <- list.files("FILEPATH/", pattern = ".csv", full.names = TRUE)
# temp <- lapply(files, fread, sep=",")
# old_cap <- rbindlist(temp)
# 
# rm(temp)
# # # subset to endemic locations
# df <- df[df$location_id %in% endem_locs$location_id,]
# cap_df <- cap_df[cap_df$location_id %in% endem_locs$location_id,]
# old_cap <- old_cap[old_cap$location_id %in% endem_locs$location_id,]
# # drop age group id 2
# df <- df[!(df$age_group_id ==2),]
# cap_df <- cap_df[!(cap_df$age_group_id ==2),]
# old_cap <- old_cap[!(old_cap$age_group_id ==2),]
# 
# cod_env <- get_envelope(year_id = unique(df$year_id),sex_id =  unique(df$sex_id),
#                         location_id =  unique(endem_locs$location_id),
#                         age_group_id =  unique(df$age_group_id), release_id=release_id,
#                         with_hiv = 1)
# cod_env_glob <- get_envelope(year_id = unique(df$year_id),sex_id =  unique(df$sex_id),
#                              location_id =  1,age_group_id =  unique(df$age_group_id),
#                              release_id=release_id, with_hiv = 1)
# ################################################################################
# ##****** PLOTTING
# # subset envelope to just desired columns
# cod_env  <- cod_env[,.(age_group_id, location_id, year_id, sex_id, mean)]
# setnames(cod_env, "mean", "envelope")
# cod_env_glob  <- cod_env_glob[,.(age_group_id, location_id, year_id, sex_id, mean)]
# setnames(cod_env_glob, "mean", "envelope")
# 
# 
# df_cf <- left_join(df, cod_env, by = c("age_group_id", "location_id", "sex_id", "year_id"))
# cap_cf <- left_join(cap_df, cod_env, by = c("age_group_id", "location_id", "sex_id", "year_id"))
# old_cap_cf <- left_join(old_cap, cod_env, by = c("age_group_id", "location_id", "sex_id", "year_id"))
# 
# # # calculate CF
# vars <- paste0("draw_", 0:999)
# cols <- colnames(df_cf)
# cols <- cols[!(cols %in% c(vars, "envelope"))]
# df_cf <- df_cf[, lapply(.SD, function(x) x/envelope), .SDcols=vars, by=cols]
# 
# cols <- colnames(cap_cf)
# cols <- cols[!(cols %in% c(vars, "envelope"))]
# cap_cf <- cap_cf[, lapply(.SD, function(x) x/envelope), .SDcols=vars, by=cols]
# 
# cols <- colnames(old_cap_cf)
# cols <- cols[!(cols %in% c(vars, "envelope"))]
# old_cap_cf <- old_cap_cf[, lapply(.SD, function(x) x/envelope), .SDcols=vars, by=cols]
# 
# # # summarize
# df <- mean_ui(df)
# cap_df <- mean_ui(cap_df)
# old_cap <- mean_ui(old_cap)
# 
# df_cf <- mean_ui(df_cf)
# cap_cf <- mean_ui(cap_cf)
# old_cap_cf <- mean_ui(old_cap_cf)
# 
# # add version
# df$version <- "Not Capped"
# cap_df$version <- "New Capped"
# old_cap$version <- "Old Capped"
# 
# df_cf$version <- "Not Capped"
# cap_cf$version <- "New Capped"
# old_cap_cf$version <- "Old Capped"
# 
# # bind together
# both <- rbind(df, cap_df, old_cap)
# 
# both_cf <- rbind(df_cf, cap_cf, old_cap_cf)
# 
# # write.csv(both, paste0(plot_outpath, "counts_plotting_data_capped_cf_",date,"_adj_compare.csv"), row.names = FALSE)
# # write.csv(both_cf, paste0(plot_outpath, "cf_plotting_data_capped_cf_",date,"_adj_compare.csv"), row.names = FALSE)
# 
# both <- both[!(both$age_group_id ==2),]
# both_cf <- both_cf[!(both_cf$age_group_id ==2),]
# 
# # add on loc and age metadata
# both <- left_join(both, locs, by = "location_id")
# both_cf <- left_join(both_cf, locs, by = "location_id")
# both <- left_join(both, ages, by = "age_group_id")
# both_cf <- left_join(both_cf, ages, by = "age_group_id")
# 
# # factor ages for plotting
# both$sex_name <- ifelse(both$sex_id == 1, "Male", "Female")
# both$age_group_name <- factor(both$age_group_name, levels = c( "Late Neonatal", "1-5 months",
#                                                                "6-11 months", "12 to 23 months","2 to 4","5 to 9", "10 to 14",
#                                                                "15 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49",
#                                                                "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84", "85 to 89",
#                                                                "90 to 94", "95 plus"))
# 
# both_cf$sex_name <- ifelse(both_cf$sex_id == 1, "Male", "Female")
# both_cf$age_group_name <- factor(both_cf$age_group_name, levels = c( "Late Neonatal", "1-5 months",
#                                                                      "6-11 months", "12 to 23 months","2 to 4","5 to 9", "10 to 14",
#                                                                      "15 to 19", "20 to 24", "25 to 29", "30 to 34", "35 to 39", "40 to 44", "45 to 49",
#                                                                      "50 to 54", "55 to 59", "60 to 64", "65 to 69", "70 to 74", "75 to 79", "80 to 84", "85 to 89",
#                                                                      "90 to 94", "95 plus"))
# # adjust some subnat names
# both$location_name <- ifelse(both$location_id==25344, "Niger, subnational", both$location_name)
# both$location_name <- ifelse(both$location_id ==533, "Georgia, subnat", both$location_name)
# both$location_name <- ifelse(both$location_id ==53620, "Punjab, PAK", both$location_name)
# 
# both_cf$location_name <- ifelse(both_cf$location_id==25344, "Niger, subnational", both_cf$location_name)
# both_cf$location_name <- ifelse(both_cf$location_id ==533, "Georgia, subnat", both_cf$location_name)
# both_cf$location_name <- ifelse(both_cf$location_id ==53620, "Punjab, PAK", both_cf$location_name)
# 
# # break out into different plotting dfs
# both <- as.data.table(both)
# both_cf <- as.data.table(both_cf)
# 
# both_cf_f <- both_cf[both_cf$sex_id == 2,]
# both_cf_m <- both_cf[both_cf$sex_id == 1,]
# 
# both_f <- both[both$sex_id == 2,]
# both_m <- both[both$sex_id == 1,]
# 
# # now plot
# message("Plotting results")
# 
# # first plot with the upper and lower ci
# # plot_compare_input(both_f, "Females", "deaths")
# # plot_compare_input(both_m, "Males", "deaths")
# #
# # plot_compare_input(both_cf_f, "Females", "CF")
# # plot_compare_input(both_cf_m, "Males", "CF")
# 
# # now plot without the ci
# plot_compare_input_noci(both_f, "Females", "deaths")
# plot_compare_input_noci(both_m, "Males", "deaths")
# 
# plot_compare_input_noci(both_cf_f, "Females", "CF")
# plot_compare_input_noci(both_cf_m, "Males", "CF")
# 
# ## plot 3 comparisons
# plot_3_noci(both_f, "Females", "deaths")
# plot_3_noci(both_m, "Males", "deaths")
# 
# plot_3_noci(both_cf_f, "Females", "CF")
# plot_3_noci(both_cf_m, "Males", "CF")
# 
# message("Plotting complete, end of script")
################################################################################
##****** End of script