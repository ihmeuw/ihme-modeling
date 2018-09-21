

space_time_xwalk <- function(df, gold_standard, location_id, crosswalk_vars, wspace = .5, wtime = .05, pspace = .5, nkeep = 100){

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
  
  #make some dummy variables for larger age categorizations
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
  
  #get crosswalk coefficients for age group 7 using a global lm
  for (type_i in unique(betas7$type_freq)){
    weights <- training[!is.na(get(type_i)) & kid_dum ==1 ] %>% invisible
    if(nrow(weights) > 0){
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
  
  
  #get crosswalk coefficients for age group 8 using a global lm 
  for (type_i in unique(betas8$type_freq)){
    weights <- training[!is.na(get(type_i)) & teen_dum == 1]
    if(nrow(weights) > 0){
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