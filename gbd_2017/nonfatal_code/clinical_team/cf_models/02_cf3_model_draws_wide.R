
rm(list = ls())
source('filepath')
source('filepath')
source('filepath')
source('filepath')
source('filepath')
library(mortcore, lib = "filepath")


library(readstata13)
library(ggplot2)
library(tidyr)
library(lme4)
library(plyr)
library(gtools)
library(RMySQL)
library(MASS)
library(dplyr)
library(magrittr)
library(readr)

## Chagne up prep, between US and Taiwan
locs <- get_location_metadata(location_set_id = 35, gbd_round = 5)
usa_all <- fread("filepath")
colnames(usa_all)[1] <- 'sex_id'
usa_all <- usa_all[, c('sex_id', 'age_start', 'bundle_id', 'prevalence')]

prep_all_years <- fread('filepath')
prep_all_years$V1 <- NULL
taiwan_loess <- fread('filepath')

bun_df <- loadBundles(unique(prep_all_years$bundle_id))

taiwan_loess <- merge(taiwan_loess, bun_df, by = 'bundle_id')
taiwan_loess$cause_name <- NULL
taiwan_loess$V1 <- NULL
taiwan_loess[sex_id == 1, source := 'Taiwan men']
taiwan_loess[sex_id == 2, source := 'Taiwan women']

prep_all_years <- prep_all_years[location_id != 8]
prep_all_years$prevalence <- NULL
prep_all_years <- merge(prep_all_years, usa_all, by = c('sex_id', 'age_start', 'bundle_id'))

prep_all_years <- rbind(prep_all_years, taiwan_loess, fill = T)

super <- fread('filepath')
colnames(super)[2] <- 'location_id'
super <- merge(super, locs, by = 'location_id')

#region <- merge(region, locs, by = 'location_id')

## Only want to model these

prep_all_years[, age_squared := age_start*age_start]
prep_all_years[, age_third := age_start*age_start*age_start]

super[, age_squared := age_start*age_start]
super[, age_third := age_start*age_start*age_start]
super[, logit_cost := logit(cost_over_ldi)]
#super[, predicted_diarrhea := predict(mod, newdata = super, allow.new.levels = TRUE)] 

## Make draws and append
copy_draws <- function(data){
  append_draws <- list()
  for(draws in 0:999){
    append_draws[[paste0(draws)]] <- copy(data)
    append_draws[[paste0(draws)]]$draw <- draws
  }
  append_draws <- rbindlist(append_draws)
  return(append_draws)
}

## Function to make draws, copy onto a dataframe, and predict out with a formula 1000 times
## Takes in a model as an input
## Indicate type based on the amount of data present (changing the formula)
make_draws <- function(type, mod){
  #print('making draws')
  if(class(mod) == 'lmerMod'){
    betas <- fixef(mod)
    
    
  }
  else if (class(mod) == 'lm' | class(mod) == 'glm'){
    betas <- na.omit(mod$coefficients)
    #betas <- unlist(mod$coefficients)
    #betas[is.na(betas)] <- 0
    #print(betas)
  }
  beta_names <- names(betas)
  cov_matrix <- as.matrix(vcov(mod))
  #print(cov_matrix)
  beta_draws <- as.data.table(mvrnorm(n = 1000,
                                      mu = betas, cov_matrix))
  ## Make draws from covariance matrix using mvrnorm
  beta_draws[, draw := .I-1]
  ## Change the column names of columns that (should) always be there
  setnames(beta_draws, c('(Intercept)', 'poly(age_start, 3, raw = T)1', 'poly(age_start, 3, raw = T)2', 'poly(age_start, 3, raw = T)3'), 
           c('intercept', 'age_start1_beta', 'age_start2_beta', 'age_start3_beta'))
  
  ## Grep for the column names if they're there to change
  
  if ('op_envelope' %in% colnames(beta_draws)){
    setnames(beta_draws, grep('op_envelope', names(beta_draws), value = TRUE), 'op_envelope_beta')
  }
  #print(names(beta_draws))
  if('logit(cost_over_ldi)' %in% colnames(beta_draws)) {
    #if (grepl('logit', names(beta_draws) == TRUE)){
    #print('YES')
    colnames(beta_draws)[length(names(beta_draws))-1] <- 'cost_ldi_beta'
    #setnames(beta_draws, grep('logit', names(beta_draws), value = TRUE), 'cost_ldi_beta')
  }
  if ('sex_id' %in% colnames(beta_draws)){
    setnames(beta_draws, grep('sex_id', names(beta_draws), value = TRUE), 'sex_id_beta')
  }
  
  df <- copy_draws(super)
  df <- merge(df, beta_draws, by = 'draw')
  
  ## Change column names to easier thing, but need to check if they exist
  
  ## Implement formula, type = 1 if 2 sexes
  ## annoyingly hard-code the different combinations, based on what columns
  if (type == 1){
    ## all covariates present
    if('sex_id_beta' %in% colnames(df) & 'op_envelope_beta' %in% colnames(df) & 'cost_ldi_beta' %in% colnames(df)){
      print('1')
      df[, draw_pred := intercept + sex_id_beta*sex_id +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third +
           op_envelope_beta*op_envelope + cost_ldi_beta*logit_cost]
      
    }
    ## sex and envelope present, no cost_ldi_beta
    else if ('sex_id_beta' %in% colnames(df) & 'op_envelope_beta' %in% colnames(df) & !('cost_ldi_beta' %in% colnames(df))){
      print('2')
      df[, draw_pred := intercept + sex_id_beta*sex_id +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third +
           op_envelope_beta*op_envelope]
      
    }
    ## Just envelope and cost_ldi
    else if (!('sex_id_beta' %in% colnames(df)) & 'op_envelope_beta' %in% colnames(df) & ('cost_ldi_beta' %in% colnames(df))){
      print('3')
      df[, draw_pred := intercept +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third +
           op_envelope_beta*op_envelope + cost_ldi_beta*logit_cost]
      
    }
    ## Just sex_id
    else if ('sex_id_beta' %in% colnames(df) & !('op_envelope_beta' %in% colnames(df)) & !('cost_ldi_beta' %in% colnames(df))){
      print('4')
      df[, draw_pred := intercept + sex_id_beta*sex_id +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third]
      
    }
    ## Just envelope
    else if (!('sex_id_beta' %in% colnames(df)) & 'op_envelope_beta' %in% colnames(df) & !('cost_ldi_beta' %in% colnames(df))){
      print('5')
      df[, draw_pred := intercept +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third +
           op_envelope_beta*op_envelope]
      
    }
    ## Just cost_ldi
    else if (!('sex_id_beta' %in% colnames(df)) & !('op_envelope_beta' %in% colnames(df)) & !('cost_ldi_beta' %in% colnames(df))){
      print('6')
      df[, draw_pred := intercept +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third +
           cost_ldi_beta*logit_cost]
      
    }
    
  }
  
  ## Type == 2 returned if model only run on one sex
  else if (type == 2){
    if('op_envelope_beta' %in% colnames(df) & 'cost_ldi_beta' %in% colnames(df)){
      print('1')
      df[, draw_pred := intercept +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third +
           op_envelope_beta*op_envelope + cost_ldi_beta*logit_cost]
      
    }
    ## envelope present, no cost_ldi_beta
    else if ('op_envelope_beta' %in% colnames(df) & !('cost_ldi_beta' %in% colnames(df))){
      print('2')
      df[, draw_pred := intercept + 
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third +
           op_envelope_beta*op_envelope]
      
    }
    ## no envelope, cost_ldi_present
    else if (!('op_envelope_beta' %in% colnames(df)) & ('cost_ldi_beta' %in% colnames(df))){
      print('3')
      df[, draw_pred := intercept  +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third + 
           cost_ldi_beta*logit_cost]
      
    }
    ## neither envelope or cost_ldi
    else if (!('op_envelope_beta' %in% colnames(df)) & !('cost_ldi_beta' %in% colnames(df))){
      print('4')
      df[, draw_pred := intercept +
           age_start1_beta*age_start + age_start2_beta*age_squared + age_start3_beta*age_third]
      
    }
  }
  
  df[, draw_pred := exp(draw_pred)]
  return(df)
}

## Make a function to add to apply
## Takes argument as prep_all_years for now (will make df)


# }

## May be deprecated/not in use
model <- function(df){
  print(class(df))
  if (length(unique(df$sex_id)) >1 &
      length(unique(df[!is.na(prevalence), ]$location_id)) >1 ){
    base <- tryCatch({
      type <- 1
      lmer(log(prevalence) ~
             sex_id + poly(age_start, 3, raw = T) + op_envelope + logit(cost_over_ldi) + (1|location_id), ## Try with age-random effect
           data = df[prevalence < 10000])
      
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  ## Both sexes, one location present
  else if (length(unique(df$sex_id)) >1 & 
           length(unique(df[!is.na(prevalence), ]$location_id)) == 1 ){
    base <- tryCatch({
      type <- 1
      glm(log(prevalence) ~ sex_id + poly(age_start, 3, raw = T) + op_envelope + logit(cost_over_ldi), data = df[prevalence < 100000])
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  ## single sex, both locations
  else if (length(unique(df$sex_id)) == 1 &
           length(unique(df[!is.na(prevalence), ]$location_id)) >1 ){
    type <- 2
    base <- tryCatch({lmer(log(prevalence) ~
                             poly(age_start, 3, raw = T) + op_envelope + logit(cost_over_ldi) + (1|location_id),
                           data = df[prevalence < 10000])
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  ## Single sex, single location
  else if (length(unique(df$sex_id)) == 1 &
           length(unique(df[!is.na(prevalence), ]$location_id)) == 1 ){
    type <- 2
    base <- tryCatch({glm(log(prevalence) ~
                            poly(age_start, 3, raw = T) + op_envelope + logit(cost_over_ldi),
                          data = df[prevalence < 10000])
      
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  print('made model, onto draws')
  draws(base)
  return (base)
}

draws <- function(base){
  if(class(base) == 'lmerMod' | class(base) == 'lm' | class(base) == 'glm'){
    print(bundle)
   
    prep_all_years[, pred := predict(base, newdata = prep_all_years, allow.new.levels = TRUE)] ## Predicted USA model based off only USA data
    
    super[, pred := predict(base, newdata = super, allow.new.levels = TRUE)] ## Predicted USA model based off only USA data
    
    ## Make draws
    #print('Making draws')
    df <- make_draws(type, base)
    df$bundle_name <- bundle
    df <- merge(df, bun_df, by = 'bundle_name')
    df <- df[, c('draw', 'bundle_id', 'location_id', 'age_start', 'sex_id', 'draw_pred')]
    colnames(df)[6] <- 'prevalence'
    # 
    # ## Plot to look how the draws look
   
   
    dt <- dcast(df, location_id + age_start + sex_id + bundle_id ~ draw, value.var = 'prevalence')
    setnames(dt, grep('[[:digit:]]', names(dt), value = TRUE), paste0('prevalence_', 0:999))
    
    return(dt)
   
  }
}

## RUNS IN a FOR LOOP
### Changing it to get rid of logit(cost_over_ldi) here
all_bundles <- data.frame()
for (bundle in sort(unique(prep_all_years$bundle_name))){
#for (bundle in c(1,3)){
  if (length(unique(prep_all_years$sex_id)) >1 &
      length(unique(prep_all_years[bundle_name == bundle][!is.na(prevalence), ]$location_id)) >1 ){
    base <- tryCatch({
      type <- 1
      lmer(log(prevalence) ~
             sex_id + poly(age_start, 3, raw = T) + op_envelope + (1|location_id), ## Try with age-random effect
           data = prep_all_years[bundle_name == bundle & prevalence < 10000])
      
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  ## Both sexes, one location present
  else if (length(unique(prep_all_years$sex_id)) >1 & 
           length(unique(prep_all_years[bundle_name == bundle][ !is.na(prevalence), ]$location_id)) == 1 ){
    base <- tryCatch({
      type <- 1
      glm(log(prevalence) ~ sex_id + poly(age_start, 3, raw = T) + op_envelope,
          data = prep_all_years[bundle_name == bundle & prevalence < 10000])
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  ## single sex, both locations
  else if (length(unique(prep_all_years$sex_id)) == 1 &
           length(unique(prep_all_years[bundle_name == bundle][!is.na(prevalence), ]$location_id)) >1 ){
    type <- 2
    base <- tryCatch({lmer(log(prevalence) ~
                             poly(age_start, 3, raw = T) + op_envelope + (1|location_id),
                           data = prep_all_years[bundle_name == bundle & prevalence < 10000])
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  ## Single sex, single location
  else if (length(unique(prep_all_years[bundle_name == bundle]$sex_id)) == 1 &
           length(unique(prep_all_years[bundle_name == bundle][!is.na(prevalence), ]$location_id)) == 1 ){
    type <- 2
    base <- tryCatch({glm(log(prevalence) ~
                            poly(age_start, 3, raw = T) + op_envelope  ,
                          data = prep_all_years[bundle_name == bundle & prevalence < 10000])
      
    },
    error = function(cond){
      message(paste0('bundle ', bundle, ' modeling broke'))
      print(cond)
      return(NA)
    })
  }
  #print(paste0('model class is ', class(base)))
  
  if(class(base) == 'lmerMod' | class(base) == 'lm' | class(base) == 'glm'){
    print(bundle)

    prep_all_years[, pred := predict(base, newdata = prep_all_years, allow.new.levels = TRUE)] ## Predicted USA model based off only USA data

    super[, pred := predict(base, newdata = super, allow.new.levels = TRUE)] ## Predicted USA model based off only USA data
    
    super_preds <- super[, c('location_id', 'age_start', 'sex_id', 'pred')]
    
    ## Make draws
    #print('Making draws')
    df <- make_draws(type, base)
    df$bundle_name <- bundle
    df <- merge(df, bun_df, by = 'bundle_name')
    df <- df[, c('draw', 'bundle_id', 'location_id', 'age_start', 'sex_id', 'draw_pred')]
    colnames(df)[6] <- 'prevalence'
    
    
    
    if(nrow(df) > 0){
      dt <- dcast(df, location_id + age_start + sex_id + bundle_id ~ draw, value.var = 'prevalence')

      ## Merge on actual prediction
      dt <- merge(super_preds, dt, by = c('location_id', 'age_start', 'sex_id'))
      setnames(dt, 'pred', 'model_prediction')
      setnames(dt, grep('[[:digit:]]', names(dt), value = TRUE), paste0('prevalence_', 0:999))
      b <- unique(dt$bundle_id)[1]
      dt[, model_prediction := exp(model_prediction)]
      write_csv(dt, paste0(filepath))
      all_bundles <- rbind(all_bundles, dt)
      #bun_list[count] <- dt
      #count <- count + 1
    } else{
      next
    }
  }
}

date <- Sys.Date()
date <- gsub('-', '_', date)


write.csv(all_bundles, paste0('filepath'))
