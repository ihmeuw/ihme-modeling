### crosswalk survey data to reference data (admisnitrative data)
# This proceses is done by looking for overlap between sub-optimal data points (surveys)
# and admisntrative recrods. Then taking the lgo ratio of these value run an age spline thru them and 
# apply that age patternt all other data points
library(mgcv)
library(plyr)
library(parallel)

string_passed <- as.character(commandArgs())
print(string_passed)
write_dir <- string_passed[4]

## read in data
avg_12<- read.csv(paste0(write_dir,"for_cw_ip_12_month_avg.csv"), stringsAsFactors = FALSE)
fraction <- read.csv(paste0(write_dir,"preped_ip_fractions.csv"), stringsAsFactors = FALSE)
admin <- read.csv(paste0(write_dir, "for_cw_ip_admin.csv"), stringsAsFactors = FALSE)

## check to make sure some things are cosnistent
fraction$sex<- ifelse(fraction$sex=='male', 'Male', fraction$sex)
fraction$sex<- ifelse(fraction$sex=='both', 'Both', fraction$sex)
fraction$sex<- ifelse(fraction$sex=='female', 'Female', fraction$sex)
fraction$cv_mics <- ifelse(is.na(fraction$cv_mics), 0, fraction$cv_mics)

# drop the points that are outliered and split up
# fractions into 12 month recall and 1 month recall
admin <- admin[admin$is_outlier!=1,]
avg_12 <- avg_12[avg_12$is_outlier !=1,]
fraction <- fraction[fraction$is_outlier!=1,]
frac_12 <- fraction[fraction$cv_12_month_recall==1,]
frac_1 <- fraction[fraction$cv_1_month_recall==1,]


### define some helper function
## when I amtch data they do not need to be exact matches on year... just close enough
year_rounder <-function(df){
  df$year_round <- df$age_end
  df$year_round <- round(df$year_round/2,0)
  df$year_round <- df$year_round*2
  
  return(df)
}

##when I merge so I dont get _x, _y could do it in suffixes but whatever
adjust_labels <- function(df){
  df$mean_2 <- df$mean
  df$standard_error_2 <- df$standard_error
  
  return(df)
}

## calc standard error of a multiplied measurements 
std_err <- function(df,mu_x, mu_y, se_x, se_y){
  temp <- (mu_x**2)*(se_y**2) + (mu_y**2)*(se_x**2) +(se_y**2)*(se_x**2) 
  temp <- temp**(1/2)
  df$standard_error_old <- df$standard_error
  df$standard_error<- temp
  return(df)
}



## run a spline model. Takes overlap of data and the df that we would like to predict one
spline_model <- function(df, df_out){
  
  ## df contains the over lap data
  # df out contains the data that we wish to make predictions on
  
  # inverse variance weight 
  df$weight <- 1/df$standard_error**2
  ## prep df_out
  df_out$mean_2 <- df_out$mean
  df_out[df_out$mean==1, 'mean'] <- .99
  df$age_mean <- (df$age_end + df$age_start)/2
  form <-as.formula('I(log(mean/mean_2)) ~ s(age_mean, bs="ps")' )
  
  ## for each possible sex estimate the correction factor
  for(i in c("Both", "Female", 'Male')){
  
    if(i =="Both"){
      ## for both sexes combine all estimates?
      model <- gam(form,
                   data= df, weights = weight)
    }else{
      model<- gam(form , data=df[df$sex==i,],weights = weight)
    }
    
    
    print(summary(model))
    ### predict out corretion factor and error
    df_out[df_out$sex==i, "cf"] <- exp(predict(model, newdata= df_out[df_out$sex==i, ]))
    df_out[df_out$sex==i, "pred_se"] <- predict(model, newdata= df_out[df_out$sex==i, ],se.fit=TRUE)$se.fit
    df_out[df_out$sex==i, 'adj']<-1 # denote that it is adjusted
    
  }
  
  ## combine standard errors
  df_out <- std_err(df_out, df_out$mean, df_out$cf, df_out$standard_error, df_out$pred_se)
  ## apply correction factor
  df_out$mean <- df_out$mean*df_out$cf
  
  
  
  df_out$log_beds <- NULL
  df_out$log_ldi <- NULL
 
  return(df_out)
}

remove_na <- function(df){
  df[is.na(df$cv_mics), 'cv_mics']<-0
  df[is.na(df$cv_whs), 'cv_whs']<- 0
  df[df$cv_mics !=1, 'cv_mics']<-0
  df[df$cv_whs!=1, 'cv_whs']<- 0
  return(df)
}



## round years
avg_12_pred <- year_rounder(avg_12)
frac_12_pred <- year_rounder(frac_12)
frac_1_pred <- year_rounder(frac_1)
admin <- year_rounder(admin)


## fix labels 
avg_12_pred <-adjust_labels(avg_12_pred)
frac_12_pred <- adjust_labels(frac_12_pred)
frac_1_pred <- adjust_labels(frac_1_pred)

### add on data
admin$age_mean <- NULL
admin$sample_size <- NULL
admin$cv_whs <- NULL
admin$cv_mics <- NULL


## columns that I want to merge on
cols <- c("age_start", "age_end", "year_start",  "location_id", "sex")

## merge everyhting together
avg_12_pred <- merge(admin, 
                     avg_12_pred[,c("mean_2", "standard_error_2", "cv_whs", "cv_mics", cols)],
                     by=cols)

frac_12_pred <- merge(admin, 
                      frac_12_pred[,c("mean_2", "standard_error_2",  cols)],
                      by=cols)

frac_1_pred <- merge(admin,
                     frac_1_pred[,c("mean_2", "standard_error_2",  cols)],
                     by=cols)

## read in data that we will be apply correction factors to 
avg_12<- read.csv(paste0(write_dir, "for_cw_ip_12_month_avg.csv"), stringsAsFactors = FALSE)
frac_12 <- read.csv(paste0(write_dir,"for_cw_ip_12_month_frac.csv"), stringsAsFactors = FALSE)
frac_1 <- read.csv(paste0(write_dir, "for_cw_ip_1_month_frac.csv"), stringsAsFactors = FALSE)
print(nrow(avg_12))
print(nrow(frac_12))
print(nrow(frac_1))

## data frame with "_pred" are data frame that have overlap between survey data and admin data
avg_12  <- spline_model(avg_12_pred, avg_12)
frac_12 <- spline_model(frac_12_pred,frac_12 )
frac_1<- spline_model(frac_1_pred, frac_1)

## write results
write.csv(avg_12, paste0(write_dir, "cwd_ip_12_month_avg.csv"))
write.csv(frac_12, paste0(write_dir, "cwd_ip_frac.csv"))
write.csv(frac_1, paste0(write_dir, "cwd_ip_1_month_avg.csv"))

print("END!!!!!!!!!!!")

