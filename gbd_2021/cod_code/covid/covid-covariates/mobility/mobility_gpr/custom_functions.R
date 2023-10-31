### CUSTOM FUNCTIONS USED IN THE MOBILITY DATA IMPUTATION MODEL
source("FILEPATH/get_location_metadata.R")

error.if.not.exists <- function(name) {
  if (!exists(name)) {
    stop(sprintf("The variable '%s' does not exist. You must define it before you source custom_functions.R", name))
  }
}

error.if.not.exists("locs")

mav <- function(x,n){stats::filter(x,rep(1/n,n), sides=2)}
mav_end <- function(x,n){stats::filter(x,rep(1/n,n), sides=2)}
mav_end_one <- function(x,n){stats::filter(x,rep(1/n,n), sides=1)}

rolling_fun_simple <- function(df,n){
  setkey(df,ihme_loc_id,date)
  df[,change_from_normal_avg:=as.numeric(mav(change_from_normal,n)),by=ihme_loc_id]
  # deal with the start and end of the time series
  df[, change_from_normal_avg_end := as.numeric(mav_end_one(change_from_normal,n)),by=ihme_loc_id]
  # reverse and re-calculate
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,-1))
  df[, change_from_normal_avg_start := as.numeric(mav_end_one(change_from_normal,n)),by=ihme_loc_id]
  # and change back to what to should be:
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,1))
  
  
  # threshold dates:
  # early dates:
  df[,date := as.Date(date)]
  df[,start_date := ifelse(date < min(date)+((n/2)),1,0),by="ihme_loc_id"]
  df[,end_date := ifelse(date > max(date)-((n/2)),1,0),by="ihme_loc_id"]
  df[start_date == 1 & is.na(change_from_normal_avg), change_from_normal_avg := change_from_normal_avg_start]
  df[end_date == 1 & is.na(change_from_normal_avg), change_from_normal_avg := change_from_normal_avg_end]
  
  
  df[,c("change_from_normal_avg_end","change_from_normal_avg_start") := NULL]
  
  
  df
  
}

rolling_fun <- function(df,n){
  
  # take into consideration the fact that not all places will have 20 data points
  df[,count := .N, by=c("ihme_loc_id")]
  
  setkey(df,ihme_loc_id,date)
  df[count >= n,change_from_normal_avg:=as.numeric(mav(change_from_normal,n)),by=ihme_loc_id]
  # deal with the start and end of the time series
  df[count >= n-10, change_from_normal_avg_end := as.numeric(mav_end(change_from_normal,n-10)),by=ihme_loc_id]
  # reverse and re-calculate
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,-1))
  df[count >= n-10, change_from_normal_avg_start := as.numeric(mav_end(change_from_normal,n-10)),by=ihme_loc_id]
  # and change back to what to should be:
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,1))
  
  
  df[count >= n-15, change_from_normal_avg_end_2 := as.numeric(mav_end(change_from_normal,n-15)),by=ihme_loc_id]
  # reverse and re-calculate
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,-1))
  df[count >= n-15, change_from_normal_avg_start_2 := as.numeric(mav_end(change_from_normal,n-15)),by=ihme_loc_id]
  # and change back to what to should be:
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,1))
  
  
  df[count >= n-15, change_from_normal_avg_end_3 := as.numeric(mav_end_one(change_from_normal,n-15)),by=ihme_loc_id]
  # reverse and re-calculate
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,-1))
  df[count >= n-15, change_from_normal_avg_start_3 := as.numeric(mav_end_one(change_from_normal,n-15)),by=ihme_loc_id]
  # and change back to what to should be:
  df <- setorderv(df,cols=c("ihme_loc_id","date"),c(1,1))
  
  
  # threshold dates:
  # early dates:
  df[,date := as.Date(date)]
  df[,start_date := ifelse(date < min(date)+((n/2)),1,0),by="ihme_loc_id"]
  df[,end_date := ifelse(date > max(date)-((n/2)),1,0),by="ihme_loc_id"]
  
  
  df[start_date == 1 & is.na(change_from_normal_avg),
     change_from_normal_avg := ifelse(!is.na(change_from_normal_avg_start), change_from_normal_avg_start,
                                      ifelse(!is.na(change_from_normal_avg_start_2), change_from_normal_avg_start_2,
                                             change_from_normal_avg_start_3))]
  
  df[end_date == 1 & is.na(change_from_normal_avg),
     change_from_normal_avg := ifelse(!is.na(change_from_normal_avg_end), change_from_normal_avg_end,
                                      ifelse(!is.na(change_from_normal_avg_end_2), change_from_normal_avg_end_2,
                                             change_from_normal_avg_end_3))]
  
  
  df[,c("change_from_normal_avg_end","change_from_normal_avg_start",
        "change_from_normal_avg_end_2", "change_from_normal_avg_end_3", "change_from_normal_avg_start_2",
        "change_from_normal_avg_start_3") := NULL]
  
  
  df
  
}


time_series_raw_data <- function(global_path_plot, all_mobility_decrease, loc_list){
  
  pdf(global_path_plot, height=8,width=12)
  for(l in loc_list){
    message(l)
    print(ggplot(all_mobility_decrease[ihme_loc_id == l],
                 aes(date,change_from_normal,color=source))+
            geom_line(aes(date,change_from_normal_avg,color=source))+
            geom_point()+
            ggtitle(unique(locs[ihme_loc_id==l,location_name])))
  }
  dev.off()
}


post_pred_plot <- function(pred_plot_path,df_compiled, loc_list){
  
  pdf(pred_plot_path, height=8,width=12)
  for(l in intersect(loc_list, unique(df_compiled$ihme_loc_id))){
    print(l)
    print(ggplot()+
            geom_point(data=df_compiled[ihme_loc_id == l],
                       aes(date, change_from_normal_GOOGLE,
                           color="GOOGLE", shape=as.factor(GOOGLE_imputed)))+
            geom_point(data=df_compiled[ihme_loc_id == l],
                       aes(date, change_from_normal_FACEBOOK,
                           color="FACEBOOK", shape=as.factor(FACEBOOK_imputed)))+
            ylab("Mobility change from normal") +
            scale_color_discrete(name="Data Source") +
            scale_shape_discrete(name="", labels=c("Observed", "Imputed")) +
            ggtitle(unique(locs[ihme_loc_id==l,location_name])))
  }
  
  dev.off()
}


post_outliering_plot <- function(post_out_path, df_compiled, loc_list){
  
  pdf(post_out_path, height=8,width=12)
  for(l in intersect(loc_list, unique(df_compiled$ihme_loc_id))){
    message(l)
    p <- ggplot()+
      geom_point(data=df_compiled[ihme_loc_id == l],
                 aes(date, change_from_normal_GOOGLE,
                     color="GOOGLE"))+
      geom_line(data=df_compiled[ihme_loc_id == l],
                aes(date, all_rolling_mean), color="black")+
      ylab("Mobility change from normal") +
      scale_color_discrete(name="Data Source") +
      ggtitle(unique(locs[ihme_loc_id==l,location_name]))
    
    if(nrow(df_compiled[ihme_loc_id == l & out_flag_google == 1]) > 0){
      p <- p+geom_point(data=df_compiled[ihme_loc_id == l & out_flag_google == 1],
                        aes(date,change_from_normal_GOOGLE,color="OUTLIER"))
    }
    
    print(p)
    
  }
  dev.off()
}


post_out_moving_avg <- function(new_moving_avg_path,df_compiled_model, loc_list){
  pdf(new_moving_avg_path,width=12,height=8)
  for(l in intersect(loc_list, unique(df_compiled_model$ihme_loc_id))){
    print(l)
    print(ggplot(df_compiled_model[ihme_loc_id == l])+geom_point(aes(date,value,color=variable))+
            geom_line(aes(date,change_from_normal_avg))+
            ggtitle((locs[ihme_loc_id == l,location_name])))
  }
  dev.off()
}

# forecasting functions
reverse_ts <- function(y)
{
  ts(rev(y), start=tsp(y)[1L], frequency=frequency(y))
}
# Function to reverse a forecast
reverse_forecast <- function(object)
{
  h <- length(object[["mean"]])
  f <- frequency(object[["mean"]])
  object[["x"]] <- reverse_ts(object[["x"]])
  object[["mean"]] <- ts(rev(object[["mean"]]),
                         end=tsp(object[["x"]])[1L]-1/f, frequency=f)
  object[["lower"]] <- object[["lower"]][h:1L,]
  object[["upper"]] <- object[["upper"]][h:1L,]
  return(object)
}

forecast_loop <- function(all_gpr){
  
  global_min <- min(all_gpr[is.na(val),year_id],na.rm=T)
  global_max <- max(all_gpr[is.na(val),year_id],na.rm=T)
  
  
  all_forecasted <- data.table()
  for(l in unique(all_gpr$ihme_loc_id)){
    message(l)
    country_data <- all_gpr[ihme_loc_id == l]
    min_year <- min(country_data[!is.na(val),year_id])
    max_year <- max(country_data[!is.na(val),year_id])
    country_data <- country_data[year_id >= min_year & year_id <= max_year]
    
    
    # create a time series for the data that are present
    data_ma_new = ts(na.omit(country_data$gpr_mean), start=c(min_year), end=c(max_year))
    
    forecast_fit_to_plot <- data.table()

    
    new_time_series <-data_ma_new %>%
      reverse_ts()
    
    if(min_year != global_min){
      fit_og_forecast <- holt(new_time_series,damped=TRUE, phi = 0.9,h=((min_year)-global_min))
      reverse_fit <- reverse_forecast(fit_og_forecast)
      reverse_fit_to_plot <- data.table(mean = as.numeric(reverse_fit$mean), lower = as.numeric(reverse_fit$lower[2]), upper = as.numeric(reverse_fit$upper[2]), year_id = seq(global_min,min_year-1,1), version = "backcasted")
      
    } else{
      reverse_fit_to_plot <- data.table()
    }
    
    
    data_present_to_plot <- data.table(mean = as.numeric(data_ma_new), year_id = seq(min_year,max_year,1), version = "rolling mean")
    
    all_data <- rbind(reverse_fit_to_plot,forecast_fit_to_plot,fill=T)
    all_data <- rbind(all_data,data_present_to_plot,fill=T)
    all_data$ihme_loc_id <- l
    all_data[,c("upper","lower") := NULL]
    
    all_forecasted <- rbind(all_forecasted,all_data,fill=T)
    
  }
  
  all_forecasted
}
