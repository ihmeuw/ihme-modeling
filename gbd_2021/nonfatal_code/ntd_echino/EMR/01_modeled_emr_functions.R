
###############################################################################
## Purpose: Functions for EMR regression using MR BRT, GBD 2020
###############################################################################

get_emr_data <- function(model_id){
}

remove_clinical_or_location_data <- function(data, locations_to_remove = NULL){
  message("Starting removal of clinical or location data")
  tmp_dt <- data
  
  print("FILEPATH", )
  clinical_nids <- as.data.table(read.csv("FILEPATH"))

    
  merge_dt <- as.data.frame(tmp_dt) %>% left_join(clinical_nids[,c(ADDRESS)]) %>% as.data.table()
  # Remove specified clinical data
  if(remove_data == TRUE){
    merge_dt <- merge_dt[us_data != 1, ]
 
  }
  if(remove_taiwan == TRUE){  
    merge_dt <- merge_dt[taiwan_data != 1, ] 

  }
  if(remove_poland == TRUE){  
    merge_dt <- merge_dt[poland_data != 1, ] 

  }
  if(remove_russia == TRUE){  
    merge_dt <- merge_dt[russia_data != 1, ] 
 
  }
  if(remove_singapore == TRUE){  
    merge_dt <- merge_dt[singapore_data != 1, ] 
  
  }
  if(remove_inpatient == TRUE){  
    merge_dt <- merge_dt[inpatient != 1, ] 
     }
  

  merge_dt <- merge_dt[ ! merge_dt$location_id %in% locations_to_remove, ]

  return(merge_dt)
}

reformat_data_for_modeled_emr <- function(data) {

  dt <- data 
  
  

  dt[age_end >100, age_end := 100]
  
  ## Add variables for midpoint age, sex (male=0, female=1), and midyear 

  dt[, midage := (age_start+ age_end)/2]
  dt[, sex_binary := ifelse(sex_id==2,1,0)]
  dt[, year_id := round((year_start+year_end)/2)]
  
  ## Add study ID to include random effects to MR-BRT model
  dt[, id := .GRP, by = c("nid", "location_id")]
  
  ## Convert EMR mean and standard error to log space

  dt[, log_ratio := log(mean)]


  dt$delta_log_se <- sapply(1:nrow(dt), function(i) {
    ratio_i <- dt[i, "mean"]
    ratio_se_i <- dt[i, "standard_error"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  print(summary(dt$delta_log_se))
    
  return(dt)
}

plot_input_haq_data <- function(data, plot_output_path, haq_data){
  message("Starting plot of input data")
  tmp_dt <- as.data.table(data)
  haq <- haq_data
  #Visualize HAQ and age spread of input data
  haq2 <- haq[with(haq, order(haqi_mean)), ]
  haq2 <- as.data.table(haq2[haq2$year_id==2020, ])
  haq2[,index:=.I]
  haq2 <- merge(haq2, locs[,c("location_id", "super_region_name")], by = "location_id")
  

  gg1 <- ggplot() + geom_point(data = haq2, aes(x=super_region_name, y= haqi_mean, color = super_region_name), 
                               position = position_jitter(w = .1, h = 0)) +
    scale_color_discrete(name = "Super Region") +
    labs(y="HAQ") + 
    ggtitle(("HAQ Distribution by Super Region")) +
    theme_classic() +
    theme(legend.text=element_blank(), legend.title=element_blank(), text = element_text(size=8)) +
    theme(legend.position = "none") +
    scale_x_discrete(labels=c("Eur/Asia", "HI", "LAC", "NAME", "SA", "Asia/Oceania", "SSA")) +

    xlab("Super Region") 
  gg1 
  ggsave(gg1, filename = paste0(plot_output_path, cause_name, "_haq_dist_", date, ".pdf"), width = 6, height = 6)
  
  tmp_dt[, age_grp := round(tmp_dt$midage, -1)] 
  
  print("Starting plot 2")
  gg2 <- ggplot() +
    geom_point(data = dt, aes(x = haqi_mean, y = mean, color = as.factor(sex_binary)))+
    facet_wrap(~tmp_dt$age_grp) +
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) + 
    scale_color_discrete(name = "Sex", labels = c("Male", "Female")) +
    labs(x = "HAQ", y = "EMR") + 
    ggtitle(paste0("EMR input distribution vs HAQ faceted by age")) +
    theme_classic() +
    theme(text = element_text(size = 10, color = "black"), legend.position = "bottom") +
    coord_cartesian(xlim = NULL, ylim = NULL) 
  gg2
  ggsave(gg2, filename = paste0(plot_output_path, cause_name, "_input_data_plot_", date, ".pdf"), width = 10, height=8)
  
  message(paste("Plots saved to", plot_output_path))
  
}

map_emr_input <- function(data){ 
  message("Starting map function")
  ## Pull in EMR input data used in MR-BRT regression with trim
  print("Modify dataframe")
  df <- train_data 
  df <- merge(df, locs[,c("location_id", "location_name", "region_name", "ihme_loc_id", "level")], by = "location_id")
  df[, c("underlying_nid", "year_start", "year_end", "age_start", "age_end", "measure_id", "mean", "lower", "upper",
         "standard_error", "sex_binary", "haqi_mean", "log_ratio", "delta_log_se", "intercept")] <- NULL
  df_trimmed <- as.data.table(df[df$w==0, ])
  df_total<- as.data.table(df)
  
  ## Create mapping variable - one count per row of data by location and trim
  print("Creating mapping variable - one count per row by location and trim")
  df_trimmed <- df_trimmed[, list(mapvar_trim = .N), by=.(location_id)]
  df_total <- df_total[, list(mapvar_total = .N), by=.(location_id)]
  df_merge <- merge(df_trimmed, df_total, by="location_id", all.y=T)
  df_merge[is.na(mapvar_trim), mapvar_trim:=0]
  df_merge[,mapvar := round(mapvar_trim/mapvar_total*100)]
  setnames(df_total, "mapvar_total", "mapvar")
  
 
  
  df_csv <- merge(df_merge, locs[,c("location_id", "location_name")], by="location_id")
  df_csv <- df_csv[order(df_csv$mapvar, decreasing = TRUE),] 
  setnames(df_csv, c("mapvar_trim", "mapvar_total", "mapvar"), c("Total trimmed", "Total input", "% trimmed"))
  df_csv <- setcolorder(df_csv, c("location_id", "location_name", "Total input", "Total trimmed", "% trimmed"))

  write.csv(df_csv, paste0(FILEPATH))
 
}  
graph_mrbrt_results <- function(results, predicts, include_annotations = 1){
  data_dt <- as.data.table(results$train_data)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  model_dt <- as.data.table(predicts)
  quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.50)), digits = 2)
  model_dt <- model_dt[, haqi_mean := round(haqi_mean, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$X_haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")
  coefs <- fit1$model_coefs[, c("x_cov", "beta_soln", "beta_var", "gamma_soln")]
  coefs$gamma_soln <- coefs$gamma_soln[1]
  coefs$variance_with_gamma <- coefs$beta_var + coefs$gamma_soln^2
  coefs <- coefs[, c("x_cov", "beta_soln", "variance_with_gamma")]
  coefs[,c("beta_soln","variance_with_gamma")] = format(round(coefs[,c("beta_soln", "variance_with_gamma")],3))
  
  if (include_annotations == 0) {
    gg_subset <- ggplot() +
      geom_jitter(data = data_dt, aes(x = midage, y = exp(log_ratio), color = as.factor(excluded)), width=0.6, alpha=0.2, size=2)+
      scale_fill_manual(guide = F, values = c("midnightblue", "purple")) + 
      labs(x = "Age", y = "EMR") + 
      ggtitle(paste0("ADDRESS")) +
      theme_classic() +
      theme(text = element_text(size = 15, color = "black")) +
      geom_line(data = model_dt, aes(x = midage, y = exp(Y_mean), color = as.character(haqi_mean), linetype = sex), size=1.5) + 
 
      scale_color_manual(name = "", values = c( "#999999", "#E69F00", "#0072B2", "#56B4E9", "#009E73",  "#D55E00"), 
                         labels = c("Included EMR", "Trimmed EMR", "50th Percentile HAQ", "Min HAQ", "Max HAQ")) +
      guides(colour=guide_legend(override.aes=list(shape=c(16,16,16,NA,NA), linetype=c(0,0,1,1,1), size=c(3,3,2,2,2)))) +
  
      annotation_custom(grob = tableGrob(coefs, rows = NULL), 
                        xmin=15, xmax=50, ymin=0.2)
    return(gg_subset)
  }
  
  else {
    gg_subset<- ggplot() +
      geom_jitter(data = data_dt, aes(x = midage, y = exp(log_ratio), color = as.factor(excluded)), width=0.6, alpha=0.2, size=2)+
      scale_fill_manual(guide = F, values = c("midnightblue", "purple")) + 
      labs(x = "Age", y = "EMR") + 
      ggtitle(paste0("MR-BRT model results overlay on EMR input data (normal space)")) +
      theme_classic() +
      theme(text = element_text(size = 15, color = "black")) +
      geom_line(data = model_dt, aes(x = midage, y = exp(Y_mean), color = as.character(haqi_mean), linetype = sex), size=1.5) + 

      scale_color_manual(name = "", values = c( "#999999", "#E69F00", "#0072B2", "#56B4E9", "#009E73",  "#D55E00"), 
                         labels = c("Included EMR", "Trimmed EMR", "50th Percentile HAQ", "Min HAQ", "Max HAQ")) +
      guides(colour=guide_legend(override.aes=list(shape=c(16,16,16,NA,NA), linetype=c(0,0,1,1,1), size=c(3,3,2,2,2)))) +

      annotation_custom(grob = tableGrob(coefs, rows = NULL), 
                        xmin=15, xmax=50, ymin=0.2)
    return(gg_subset)
    
  }
  
}

graph_mrbrt_log <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  model_dt <- as.data.table(predicts)
  quant_haq <- round(quantile(model_dt$haqi_mean, probs = seq(0,1, by = 0.50)), digits = 2)
  model_dt <- model_dt[, haqi_mean := round(haqi_mean, digits =2)]
  model_dt <- model_dt[model_dt$haqi_mean %in% quant_haq, ]
  model_dt$X_haqi_mean <- as.character(model_dt$haqi_mean)
  model_dt$sex <- ifelse(model_dt$sex_binary == 1, "Female", "Male")
  coefs <- fit1$model_coefs[, c("x_cov", "beta_soln", "beta_var", "gamma_soln")]
  coefs$gamma_soln <- coefs$gamma_soln[1]
  coefs$variance_with_gamma <- coefs$beta_var + coefs$gamma_soln^2
  coefs <- coefs[, c("x_cov", "beta_soln", "variance_with_gamma")]
  coefs[,c("beta_soln","variance_with_gamma")] = format(round(coefs[,c("beta_soln", "variance_with_gamma")],3))
  gg_subset<- ggplot() +
    geom_jitter(data = data_dt, aes(x = midage, y = log_ratio, color = as.factor(excluded)), alpha=0.2, size=2)+
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) + 
    labs(x = "Age", y = "EMR") + 
    ggtitle(paste0("MR-BRT model results overlay on EMR input data (log space)")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_line(data = model_dt, aes(x = midage, y = Y_mean, color = as.character(haqi_mean), linetype = sex), size=1.5) + 

    scale_color_manual(name = "", values = c( "#999999", "#E69F00", "#0072B2", "#56B4E9", "#009E73",  "#D55E00"), 
                       labels = c("Included EMR", "Trimmed EMR", "50th Percentile HAQ", "Min HAQ", "Max HAQ")) +
    guides(colour=guide_legend(override.aes=list(shape=c(16,16,16,NA,NA), linetype=c(0,0,1,1,1), size=c(3,3,2,2,2))))

  return(gg_subset)
}

merge_haqi_covariate <- function (data) {
  print("Getting HAQi covariate")
  haq <- as.data.table(get_covariate_estimates(
    covariate_id=ADDRESS,
    gbd_round_id=ADDRESS,
    decomp_step='ADDRESS'
  ))
  
  haq <- haq[, c("location_id", "year_id", "mean_value")]
  setnames(haq, "mean_value", "haqi_mean")
  dt[,index:=.I]
  n <- nrow(dt)
  dt <- as.data.table(merge(dt,haq, by=c("location_id","year_id")))
  dt[, c("index", "year_id")] <- NULL
  
  if(nrow(dt)!=n){
 
  }
  return(list(dt, haq))
}  

apply_mrbrt_ratios_and_format <- function(data, seq_of_dummy_emr, extractor) {
  final_dt <- as.data.table(data)
  seq_emr <- seq_of_dummy_emr
  final_dt[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
  
  ## Convert to normal space from log space
  final_dt[, `:=` (mean = exp(Y_mean), standard_error = deltamethod(~exp(x1), Y_mean, Y_se^2)), by = c("Y_mean", "Y_se")]
  
  ## For uploader validation - add in required columns
  final_dt[, crosswalk_parent_seq := seq_emr]
  final_dt[, sex := ifelse(sex_binary == 1, "Female", "Male")]
  final_dt[, year_start := year_id]
  final_dt[, year_end := year_id]
  final_dt[, nid := ADDRESS]
  final_dt[, year_issue := 0]
  final_dt[, age_issue := 0]
  final_dt[, sex_issue := 0]
  final_dt[, measure := "mtexcess"]
  final_dt[, source_type := "ADDRESS"]
  final_dt[, extractor := extractor]
  final_dt[, is_outlier := 0]
  final_dt[, measure_adjustment := 1]
  final_dt[, recall_type := "Point"]
  final_dt[, urbanicity_type := "Mixed/both"]
  final_dt[, unit_type := "Person"]
  final_dt[, representative_name := "ADDRESS"]
  final_dt[, note_modeler := paste0("ADDRESS")] 
  final_dt[, unit_value_as_published := 1]
  final_dt <- left_join(final_dt, locs[,c("location_id", "location_name")], by="location_id")
  final_dt[,c("sex_binary", "year_id", "log_ratio", "delta_log_se", "midage", "Y_mean", "Y_se", "Y_mean_lo", "Y_mean_hi")] <- NULL
  
  # Set standard error greater than 1 to 1, occurs in clinical data
  print("Setting standard error >1 = 1")
  final_dt <- as.data.table(final_dt)
  final_dt[standard_error >1, standard_error  := 1]
  
  message("Finished, returning reformatted data")
  return(final_dt)
  
}
