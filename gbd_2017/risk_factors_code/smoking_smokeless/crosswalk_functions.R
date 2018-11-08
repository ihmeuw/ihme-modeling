####################################
# Tobacco crosswalk functions
##############################

library(data.table)
library(magrittr)

global_xwalk <- function(df, gold_standard, crosswalk_vars){
  #First, make the df only include sources with the gold-standard present
  dat <- copy(df[!is.na(get(gold_standard))])
  #make sure crosswalk vars are numerics
  invisible(dat[, (crosswalk_vars):=lapply(.SD, function(x) as.numeric(x)), .SDcols = crosswalk_vars])
  
  #Create a data table to store values from each type-freq combination
  betas <- invisible(data.table( type_freq = crosswalk_vars, beta = NA_real_, rsquared = NA_real_, se = NA_real_, sample_size = NA_integer_))
  
  #Loop through all freq-types, subsetting and estimating beta for relationship entre freq-type i and gold-standard
  
  for (i in betas$type_freq) {
    dt <- invisible(dat[!is.na(get(i))])
    
    if(nrow(dt)>0){
      model <- lm(formula = (get(gold_standard) ~ get(i) - 1), data = dt)
      
      #get regression coefficient, variance of beta, r-squared of the model, and the standard error
      beta_i <- unname(coef(model)[1])
      if (!is.na(beta_i)){
        var_error <- mean((model$residuals)^2)
        rsquared_i <- summary(model)$adj.r.squared #r-squared value of the regression - needs to be > .8 for inclusion
        se_i <- summary(model)$coefficients[1,2] #standard error of the regression coefficient
        sample_size_i <- nrow(dt)
        
        #attach all this captured info into the betas table
        invisible(betas[type_freq == i, c("beta", "var_of_error", "rsquared", "se", "sample_size"):=list(beta_i, var_error, rsquared_i, se_i, sample_size_i)])
      }
    }
    else{betas[type_freq == i, no_info:=1]}
  }
  
  return(betas)
  
}

space_time_xwalk <- function(df, gold_standard, location_id, crosswalk_vars, wspace = .5, wtime = .05, pspace = .5, nkeep = 100){
  #' Calculate adjustment coefficients specific to space and time
  
  #Varlists
  location_ids <- c(location_id, "region_id", "super_region_id")
  id_vars <- c(location_id)
  
  ## Separate metadata from required variables
  cols <- c("survey_id","survey_name", "age_start", "age_end", "sex_id", location_id, crosswalk_vars)
  meta_cols <- setdiff(names(df), cols)
  metadata <- df[, c("survey_id", meta_cols), with=F]
  dt <- df[, c(cols), with=F]
  
  #make sure crosswalk vars are numerics
  dt[, (crosswalk_vars):=lapply(.SD, function(x) as.numeric(x)), .SDcols = crosswalk_vars] %>% invisible
  
  #Merge in region and super-region info
  dt <- merge(dt, locs, by=location_id)
  
  #make some dummy vars for larger age categorizations
  dt[(age_start <=14) & (age_end <= 15), kid_dum:=1] %>% invisible
  dt[(age_start %in% seq(15, 19)) & (age_end %in% seq(15, 19)), teen_dum:=1] %>% invisible
  dt[is.na(kid_dum), kid_dum:=0] %>% invisible
  dt[is.na(teen_dum), teen_dum:=0] %>% invisible
  dt[survey_name == "GLOBAL_YOUTH_TOBACCO_SURVEY", gyts:=1] %>% invisible
  dt[is.na(gyts), gyts:=0] %>% invisible
  dt[age_start>=15, kid_dum:=0] %>% invisible
  
  #Separate out df into training and split tables (any surveys with gold_standard --> training those without --> split)
  training<- dt[!is.na(get(gold_standard))]
  split <- dt[is.na(get(gold_standard))]
  
  #Create a data table to store values from each type-freq combination
  betas <-  CJ(crosswalk_vars, unique(locs$region_id))
  betas <- as.data.table(betas)
  setnames(betas, names(betas), c("type_freq", "region_id"))
  betas <- betas[!is.na(type_freq)]
  betas <- merge(betas, unique(locs[, c("region_id", "super_region_id"), with=F], by = "region_id"), by = "region_id")
  
  #Create a data table to store values from each type-freq combination for age_group 7
  betas7 <-  data.table(type_freq = crosswalk_vars)
  betas8 <- data.table(type_freq = crosswalk_vars)
  
  #get crosswalk betas for teens + adults
  for (i in (1:nrow(betas))) {
    
    type_i <- betas[i, type_freq]
    rid_i <- betas[i, region_id]
    srid_i <- betas[i, super_region_id]
    
    #space weights
    weights <- training[!is.na(get(type_i)) & kid_dum == 0 & teen_dum ==0] # | is.na(age_dum)]
    #if(nrow(weights)>0){
    weights[, spacewt:=NA_real_] %>% invisible
    weights[super_region_id == srid_i, spacewt:=(1 - wspace)] %>% invisible
    weights[region_id == rid_i, spacewt:= 1] %>% invisible
    weights[is.na(spacewt), spacewt:=0] %>% invisible
    
    #keep the n-closest OR keep all those of the same region, if enough info
    setorderv(weights, "spacewt", order= -1)
    
    if(nrow(weights[spacewt==1]) >= nkeep){
      weights <- weights[spacewt==1]
    }else if((nrow(weights[spacewt==1]) < nkeep) & nrow(weights[spacewt>0]) > 0){
      weights <- weights[spacewt>0]
    } else if (nrow(weights[spacewt >0]) == 0){
      betas[i, insufficient_data:=1] %>% invisible
      next()
    }
    
    
    
    #estimate relationship between gold standard and that freq-type in that space-time domain
    model <- lm(get(gold_standard) ~ get(type_i) -1 , data = weights)
    
    #get regression coefficient, variance of beta, r-squared of the model, and the standard error
    beta_i <- unname(coef(model)[1])
    if (!is.na(beta_i)){
      rsquared_i <- summary(model)$adj.r.squared #r-squared value of the regression - needs to be > .8 for inclusion
      se_i <- summary(model)$coefficients[1,2] #standard error of the regression coefficient
      var_error <- mean((model$residuals)^2)
      sample_size_i <- nrow(weights[spacewt > 0])
      
      #add this info to the estimated row in the betas table
      betas[i, c("beta", "rsquared", "se", "var_of_error", "sample_size"):=list(beta_i, rsquared_i, se_i, var_error, sample_size_i)]
    } else{
      (betas[i, insufficient_data:=1])
    }
  }
  
  #get crosswalk coefficients for age group 7 using a global lm with
  for (type_i in unique(betas7$type_freq)){
    weights <- training[!is.na(get(type_i)) & kid_dum ==1 ] %>% invisible
    chk_length <- ifelse(length(unique(weights[, get(type_i)])) >1, T, F) # to spot rare scenarios where there's perfect correlation
    #because all values of the var to be crosswalked are the same (foryouths, always zero when this issue comes up)
    if(nrow(weights) > 0 & chk_length){
      model7 <- lm(formula = (get(gold_standard) ~ get(type_i) -1), data = weights[kid_dum == 1])
      
      beta_i <- unname(coef(model7)[1])
      rsquared_i <- summary(model7)$adj.r.squared #r-squared value of the regression - needs to be > .8 for inclusion
      var_error <- mean((model7$residuals)^2) #mean squared error of the prediction (ie variance of the error in smoking appendix language)
      se_i <- summary(model7)$coefficients[1,2] #standard error of the regression coefficient
      sample_size_i <- nrow(weights)
      insufficient_data <- 0
      
      betas7[type_freq==type_i, c("beta", "rsquared", "se", "sample_size","var_of_error", "insufficient_data"):=list(beta_i,
                                                                                                                     rsquared_i, se_i, sample_size_i, var_error, insufficient_data)] %>% invisible
      
    } else{
      betas7[type_freq==type_i, insufficient_data:=1] %>% invisible
    }
  }
  
  betas7[, young:=1] %>% invisible
  betas[, young:=0] %>% invisible
  
  betas <- rbind(betas, betas7, fill = T)
  
  
  #get crosswalk coefficients for age group 8 using a global lm with
  for (type_i in unique(betas8$type_freq)){
    weights <- training[!is.na(get(type_i)) & teen_dum == 1]
    chk_length <- ifelse(length(unique(weights[, get(type_i)])) >1, T, F)
    if(nrow(weights) > 0 & chk_length){
      model8 <- lm(get(gold_standard) ~ get(type_i) -1, data = weights[teen_dum == 1])
      var_error <- mean((model8$residuals)^2)
      beta_i <- unname(coef(model8)[1])
      rsquared_i <- summary(model8)$adj.r.squared #r-squared value of the regression - needs to be > .8 for inclusion
      se_i <- summary(model8)$coefficients[1,2] #standard error of the regression coefficient
      sample_size_i <- nrow(weights)
      insufficient_data <- 0
      
      betas8[type_freq==type_i, c("beta", "rsquared", "se", "sample_size","var_of_error", "insufficient_data"):=list(beta_i,
                                                                                                                     rsquared_i, se_i, sample_size_i,var_error, insufficient_data)] %>% invisible
      
    } else{
      betas8[type_freq==type_i, insufficient_data:=1] %>% invisible
    }
  }
  
  betas8[, teen:=1] %>% invisible
  betas[, teen:=0] %>% invisible
  
  betas <- rbind(betas, betas8, fill = T)
  
  return(betas[is.na(insufficient_data)])
  
  
}

youth_xwalk <- function(df, age_lower, age_upper, st_betas){
  
  #separate out young that can be crosswalked
  youthb <- st_betas[young ==1]
  youthvars <- youthb[, type_freq] %>% unique
  youths <- df[((age_start <= (age_upper- 1)) & (age_end <= age_upper)) & (var == gold_standard | var %in% youthvars)]
  mmkay <- youths[var == gold_standard]
  youths <- youths[var %in% youthvars] #Verified: no gold-standard stuff gets through for youths
  
  #drop nids with a gold_standard estimate
  kids_alright <- mmkay[, nid] %>% unique
  youths <- youths[!(nid %in% kids_alright)]
  
  youths[, orig_var:=var]
  
  #merge youth betas and youth crosswalkables into same table
  setnames(youthb, c("type_freq", "se", "sample_size"), c("var", "se_beta", "ss_beta"))
  youths <- merge(youths, youthb[, .(var, beta, se_beta, ss_beta, var_of_error)], by = "var" )
  
  
  #crosswalk, remerge, and bind back in at the end
  youths[, xwalk_pe:= var_of_error + ((mean**2)*se_beta)]
  youths[, orig_mean:=mean]
  youths[, mean:=mean*beta]
  youths[, orig_variance:=variance]
  youths[, variance:=variance + xwalk_pe]
  youths[, xwalk:= 1]
  
  #in case of multiple vars per one NID, keep var with highest sample_size for xwalk (since the lowest ones are scraping the threshold - best to go with higher ss)
  youths[, reps:=.N, by = c("nid", "age_start", "age_end", "year_id", "location_id", "sex_id")]
  youths[,max_ss:=max(ss_beta), by = "nid" ]
  youths <- youths[ss_beta == max_ss]
  youths[, c("beta", "se_beta", "xwalk_pe", "ss_beta"):=NULL]
  
  #bind back gold-standard youth ests
  mmkay[, xwalk:=0]
  
  youths <- rbind(youths, mmkay, fill = T)
  youths[, var:=gold_standard]
  
  #drop anyone in df with age_start younger than 15
  #df <- df[!(age_start <= (age_upper - 1) & age_end %in% seq(age_lower, age_upper))]
  #df <- df[!(age_end < 10)]
  
  return(youths)
}

teen_xwalk <- function(df, age_lower, age_upper, st_betas){
  
  #separate out young that can be crosswalked
  youthb <- st_betas[teen ==1]
  youthvars <- youthb[, type_freq] %>% unique
  youths <- df[((age_start <= (age_upper) & (age_start >=age_lower)) & age_end %in% seq(age_lower, age_upper)) & (var == gold_standard | var %in% youthvars)]
  mmkay <- youths[var == gold_standard]
  youths <- youths[var %in% youthvars] #Verified: no gold-standard stuff gets through for youths
  
  #drop nids with a gold_standard estimate
  kids_alright <- mmkay[, nid] %>% unique
  youths <- youths[!(nid %in% kids_alright)]
  
  youths[, orig_var:=var]
  
  #merge youth betas and youth crosswalkables into same table
  setnames(youthb, c("type_freq", "se", "sample_size"), c("var", "se_beta", "ss_beta"))
  youths <- merge(youths, youthb[, .(var, beta, se_beta, ss_beta, var_of_error)], by = "var" )
  
  #crosswalk, remerge, and bind back in at the end
  youths[, xwalk_pe:= var_of_error + ((mean**2)*se_beta)]
  youths[, orig_mean:=mean]
  youths[, mean:=mean*beta]
  youths[, variance:=variance + xwalk_pe]
  youths[, xwalk:= 1]
  
  #in case of multiple vars per one NID, keep var with highest sample_size for xwalk (since the lowest ones are scraping the threshold - best to go with higher ss)
  youths[, reps:=.N, by = c("nid", "age_start", "age_end", "year_id", "location_id", "sex_id")]
  youths[,max_ss:=max(ss_beta), by = "nid" ]
  youths <- youths[ss_beta == max_ss]
  youths[, c("beta", "se_beta", "xwalk_pe", "ss_beta"):=NULL]
  
  #bind back gold-standard youth ests
  mmkay[, xwalk:=0]
  
  youths <- rbind(youths, mmkay, fill = T)
  youths[, var:=gold_standard]
  
  #drop anyone in df with age_start younger than 15
  #df <- df[!(age_start <= (age_upper - 1) & age_end %in% seq(age_lower, age_upper))]
  #df <- df[!(age_end < 10)]
  
  return(youths)
}

crosswalk_data <- function(df, st_betas, global_betas, gold_standard, interest){
  #' Using derived betas from the crosswalk to actually adjust the data
  if("V1" %in% names(df)){
    df[, V1:=NULL]
  }
  
  if("V1" %in% names(st_betas)){
    st_betas[, V1:=NULL]
  }
  
  if("V1" %in% names(global_betas)){
    global_betas[, V1:=NULL]
  }
  
  st <- copy(st_betas)
  global <- copy(global_betas)
  
  #change up names from original output
  setnames(st, c("type_freq" ,"beta", "se", "sample_size", "rsquared", "var_of_error"), c( "var" ,"st_beta", "st_se", "st_sample_size", "st_rsquared",  "st_var_error"))
  setnames(global, c("beta", "se", "sample_size", "rsquared", "type_freq", "var_of_error"),
           c("global_beta", "global_se", "global_sample_size", "global_rsquared", "var", "global_var_error"))
  
  ids <- c("survey_id", "region_id")
  xwalk_types <- st[, unique(var)] %>% as.vector()
  #xcols <- names(split)[which(split[, names(split) %in% xwalk_types])]
  xcols <- df[var %in% xwalk_types, var] %>% unique
  df <- df[var == gold_standard | var %in% xcols]
  
  
  #create unique identifier by survey (nid, country, year, age, sex)
  df[, survey_id:=.GRP, by = c("nid", "location_id", "year_id", "age_start", "age_end", "sex_id")]
  
  #df <- df[, c("survey_id", "nid", "survey_name", "location_id", "region_id", "year_start", "year_end",
  #"age_start", "age_end", "sex_id", "var", "mean", "variance", "sample_size"), with=F]
  
  #melt for merge with betas
  #xwalked <- melt.data.table(df, id.vars = ids, measure.vars = xcols, variable.name = "type_freq", na.rm = T)
  df <- merge(df, st[, c("var", "region_id", "st_beta", "st_se", "st_sample_size", "st_rsquared", "st_var_error"), with=F],
              by = c("var", "region_id"), all.x=T)
  
  #merge in global betas if st_beta is NA
  df <-   merge(df, global[, c("var", "global_beta", "global_se", "global_sample_size", "global_rsquared", "global_var_error"), with=F],
                by= c("var"), all.x=T)
  
  surv_ids <- df[var==gold_standard, survey_id] %>% unique
  training <- df[survey_id %in% surv_ids]
  training <- training[var == gold_standard]
  
  df <- df[!(survey_id %in% surv_ids)]
  
  #multiply beta * original value to get crosswalked value
  df[!is.na(st_beta), xmean:=mean*st_beta]
  df[is.na(st_beta), xmean:=mean*global_beta]
  
  #drop those with no crosswalk
  df <- df[!is.na(xmean)]
  
  #if survey has multiple type-vars, keep the one with the highest r-squared value
  setkeyv(df, "survey_id")
  df[, surv_count:=.N, by = "survey_id"]
  df[, r2:=ifelse(is.na(st_rsquared), global_rsquared, st_rsquared)]
  df[, r2:=ifelse(is.na(st_rsquared), global_rsquared, st_rsquared) ]
  df[, max_r2:=max(r2), by = "survey_id"]
  xwalked <- df[(max_r2 == r2)]
  
  if(nrow(unique(xwalked, by = "survey_id")) != nrow(xwalked)){ print("Surveys are being double-crosswalked!")}
  
  #merge back in to metadata
  xwalked[!is.na(st_beta), x_se:=st_se] # var showing which errors to use when determining prediction error
  xwalked[!is.na(st_beta), x_ss:=st_sample_size]
  xwalked[!is.na(st_beta), x_var_error:=st_var_error]
  xwalked[is.na(st_beta) & !is.na(global_beta), x_se:=global_se]
  xwalked[is.na(st_beta) & !is.na(global_beta), x_ss:=global_sample_size]
  xwalked[is.na(st_beta) & !is.na(global_beta), x_var_error:=global_var_error]
  
  #mark which method predicted which
  xwalked[x_se == st_se & x_ss == st_sample_size, xwalk_method := "st"]
  xwalked[x_se == global_se & x_ss == global_sample_size, xwalk_method:= "global"]
  
  #mark as crosswalked source and calculate crosswalk prediction error
  ids <- c("survey_id", "source", "survey_name", "nid", "location_id", "region_id", "year_id",
           "age_start", "age_end", "sex_id", "var", "variance", "sample_size", "imputed_ss", "xwalk", "xwalk_method","orig_mean", "orig_var")
  xwalked[, xwalk:=1]
  xwalked[, xwalk_pe:= x_var_error + ((mean**2)*x_se)]
  xwalked[, variance:=variance + xwalk_pe]
  xwalked[, orig_var:=var]
  
  #changes names and merge back into main dataset
  xwalked[, orig_mean:=mean]
  xwalked[, c("mean", "max_r2"):=NULL]
  setnames(xwalked, "xmean", "mean")
  xwalked[, var:=gold_standard]
  
  
  xwalked <- xwalked[, c(ids, "mean"), with=F]
  
  #bind in xwalked rows to output dataset for age_sex dfting
  df <- rbind(training, xwalked, fill = T)
  #df <- rbind(df, youths, fill = T)
  df[is.na(xwalk), xwalk:=0]
  
  df[, survey_name] %>% unique
  
  df <- df[, c(ids, "mean"), with = F]
  
  return(df)
}
