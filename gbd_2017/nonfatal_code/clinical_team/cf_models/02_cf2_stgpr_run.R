

rm(list = ls())

## Maybe fix it not getting to pdf?
closeAllConnections()
library(ggplot2)
library(data.table)
library(lme4)
library(htmlwidgets, lib = 'filepath')
library(merTools, lib = 'filepath')
library(boot)
library(RMySQL)
library(slackr)
library(mortcore, lib = "filepath")
library(parallel)
library(magrittr)
library(readr)
library(nlme)

source('filepath')
source('filepath')
source('filepath')
source('filepath')

print(commandArgs(trailingOnly = T))
bundle <- commandArgs()[5]
make_draws <- as.logical(commandArgs()[6])
print(make_draws)

## For vetting

prep_data <- fread('filepath')
prep_data[age_group_id == 28, age_end := 1]
prep_data[cf2 == '.', cf2 := NA]
prep_data$cf2 <- as.numeric(prep_data$cf2)
prep_data[, cf2_adjust := cf2-1]
prep_data[cf2_adjust == 0, cf2_adjust := 0.00001]

locs <- get_location_metadata(35)
locs <- locs[, c('location_id', 'location_name', 'region_name', 'super_region_name', 'level')]
locs_to_merge <- locs[, c('location_id', 'region_name', 'super_region_name', 'location_name')]

##### RUN ###############
print(class(bundle))
print(bundle)

bun_df <- fread('filepath')
bun <- unique(bun_df[bundle_id == bundle]$bundle_name)
print(bun)




lri <- prep_data[bundle_id == bundle]
lri <- merge(lri, locs[, c('location_id', 'region_name', 'super_region_name', 'location_name')], by = 'location_id')
#print(head(lri))

## Will have to add more outliers when they come up
## Make high values outliers
lri[, is_outlier := 0][location_id == 16 & age_start == 70 & sex_id == 1 & bundle_id == 19, is_outlier := 1]
lri[cf2 > 1000000, is_outlier := 1]

lri <- lri[is_outlier == 0][!(is.na(cf2))]

#### MODEL #####

## CF2: Same polynomial model adjusted for sex
## only takes in inpatient envelope, haqi mean with random effects on location
#rm(base)

## If only one source, take out random efects
## base is for vetting the p-values and not having to eye the t-scores for easier looks
lo <- FALSE
if(length(unique(lri$age_start)) <= 4) {
  base_lme4_loc <- loess(log(cf2) ~ age_start + sex_id + location_id,
                     data = lri[cf2 <10000 & is_outlier != 1], parametric = c('sex_id', 'location_id'))
  base_lme4 <- loess(log(cf2) ~ age_start + sex_id,
                     data = lri[cf2 <10000 & is_outlier != 1], parametric = c('sex_id'))
  print('lo')
  lo <- TRUE
} else if(length(unique(lri$location_id)) == 1){
  
  base_lme4 <- glm(log(cf2) ~ poly(age_start, 3) + sex_id + ip_envelope,
              data = lri[cf2 < 10000])
} else{
  
  base_lme4 <- lmer(log(cf2) ~ poly(age_start, 3) + sex_id + ip_envelope + (1|location_id),
                    data = lri[cf2 < 10000])
} 
#summary(base)
### Make predictions ####
## Predictions are all in log space for CF2
preddf <- fread('filepath')
preddf <- preddf[year_id == 2010 & location_id != 533 & age_group_id.x != 164][, pred := NULL]
preddf <- unique(preddf[age_group_id.x != 33][, age_group_id.y := NULL][, V1 := NULL])
preddf[, pred := predict(base_lme4, newdata = preddf, allow.new.levels = T)]
## Predict the location-specific ones for where we have data
## overwrites the prediction
if(lo == TRUE){
  preddf[location_id %in% lri$location_id, pred := predict(base_lme4_loc,
                                                           newdata = preddf[location_id %in% lri$location_id], 
                                                           allow.new.levels = TRUE)]
}

setnames(preddf, 'age_group_id.x', 'age_group_id')

## Add one back to the prediction to line up with CF2 for residuals (and not CF2 adjust)
locs[location_id %in% preddf$location_id, keep := 1]
locs[!(location_id %in% preddf$location_id), keep := 0]

## Calculate residuals in prep_data
## want everything in log space

## For input data: predict out with model that's location-specific
if(lo == TRUE){
  lri[, pred := predict(base_lme4_loc, newdata = lri, allow.new.levels = T)]
} else{
  lri[, pred := predict(base_lme4, newdata = lri, allow.new.levels = T)]
  
}



## Get residual by scaling the prediction back
## Pred is in log space and has already had 1 added back
lri[, pred_resid_log := log(cf2) - (pred)]

ggplot() +
  geom_point(data = lri, aes(x = age_start, y = cf2, color = location_name)) +
  #geom_point(data = lri, color = 'red') + 
  geom_point(data = preddf[!(is.na(pred))][location_id %in% lri$location_id], aes(x = age_start, y = exp(pred)), alpha = 0.2, color = 'blue') + 
  facet_wrap(~location_id)

## For adding back on:

if(make_draws == TRUE){
  print('MAKING DRAWS')
  
  if(lo == TRUE){
    print('loess draws')
    ## Make location and non-locationspecific predictions
    preds_locs <- predict(base_lme4_loc, newdata = preddf[location_id %in% lri$location_id], allow.new.levels = TRUE, se = TRUE)
    preds <- predict(base_lme4, newdata = preddf[!(location_id%in% lri$location_id) ], allow.new.levels = T, se = TRUE)
    pred_dt <- data.table(preds = preds$fit,
                          se = preds$se.fit)
    pred_locs_dt <- data.table(preds = preds_locs$fit,
                          se = preds_locs$se.fit)
    pred_dt <- rbind(pred_dt, pred_locs_dt)
    
    ## Need to resort preddf
    preddf1 <- preddf[!(location_id %in% lri$location_id)]
    preddf2 <- preddf[location_id %in% lri$location_id]
    preddf <- rbind(preddf1, preddf2)
    preddf <- cbind(preddf, pred_dt)
    preddf$ID <- seq.int(nrow(preddf))
    print(names(preddf))
    na_df <- preddf[is.na(preds)]
    na_df[, c('pred', 'preds') := 1][, se := 0]
    draws_df <- preddf[!(is.na(preds))] ## Decreases the length
    draws_df <- rbind(draws_df, na_df)
    
    ## Now need to get 1000 draws of every row
    test_draws <- rbindlist(lapply(c(1:nrow(draws_df)), function(i){
      single_draw_fit <- draws_df[i]$preds
      single_draw_se <- draws_df[i]$se
      dt <- data.table(draw_pred = rnorm(1000, single_draw_fit, single_draw_se))
      dt[, ID := draws_df[i]$ID]
      dt[, draw := seq.int(nrow(dt))][, draw := draw - 1]
      dt[, draw := paste0('indv_cf_', draw)]
    }))
    
    
    preddf <- merge(preddf, test_draws, by = 'ID', all.x = TRUE, all.y = TRUE)
    
    preddf <- preddf[!(is.na(draw_pred))]
    
    ## Get same columns as other
    preddf <- preddf[, .(location_id, sex_id, age_start, age_end, age_group_id, ip_envelope, op_envelope, 
                         haqi_mean, pred, draw, draw_pred)]
    
    
    
  } else{
    print('mixed effects draws')
    test <- predictInterval(base_lme4, newdata = preddf, n.sims = 1000, level = 0.9, stat = 'mean', returnSims = TRUE)
    preds <- data.table(attr(test, 'sim.results'))
    setnames(preds, grep('[[:digit:]]', names(preds), value = TRUE), paste0('incidence_', 0:999))
    
    preddf <- cbind(preddf[, c('location_id', 'sex_id', 'age_start', 'age_end', 'age_group_id', 'ip_envelope', 'haqi_mean', 'pred')],
                    preds)
    preddf <- melt(preddf, measure = patterns('incidence_'), variable.name = 'draw', value.name = c('draw_pred'))
    
    means <- preddf[, .(mean_draw = mean(draw_pred)),
                    by = .(location_id, sex_id, age_start, age_end, age_group_id)]
            
    preddf <- merge(preddf, means, by = c('location_id', 'sex_id', 'age_start', 'age_end', 'age_group_id'))
    setkey(preddf, 'draw')
    ## vet plots
    ggplot(data = preddf[location_id == 6]) + 
      geom_point(aes(x = age_start, y = exp(draw_pred), color = 'draw predictions')) + 
      geom_point(aes(x = age_start, y = exp(pred), color = 'predictions'))
  }
  
  
  old <- Sys.time()
  ## Get draws, based off of CF2-1 (need to adjust post-hoc)
  
  
  
  savedfs <- split(preddf, by = 'draw')
  
  draws_df <- rbindlist(mclapply(c(1:1000), function(draw_num){
    
    print(draw_num)
    firststagedf <- savedfs[draw_num][[1]]
    stlocsdf <- firststagedf[, 'location_id'] %>%
      merge(locs, by = 'location_id') %>% unique
    
    ## Calculate space distance for each location
    prep_locs <- lri[, c('location_id', 'region_name', 'super_region_name', 'location_name')] %>% unique
    ## Spits out data frame with distances from datapoints for the predictions
    ## Give ref to know what location it's referring to
    
    ############ Space weighting ##################
    spdistdf <- rbindlist(lapply(unique(stlocsdf$location_id), function(x){
      loc_ref <- locs_to_merge[location_id == x]
      ## Use reference super region and region
      stlocsdf$ref <- x
      
      ## Want just spdist of 0 and 1 for if country/if not country
      copy(prep_locs)[location_id == x, spdist := 0][location_id != x, spdist := 1][, ref := x]
     
    
    }))
    
    zeta <- 0.94
    ## Assign weights relative to how many input sources there are and whether they are equal to predicted countries
    ## Now weight adds up to 1
    ## Calculate residual
    for (l in unique(spdistdf$ref)){
      spdistdf[spdist == 0 & ref == l, spweight := zeta][spdist == 1 & ref == l, spweight := (1-zeta)/nrow(spdistdf[ref == l & spdist == 1])] ## Divide by number of other sources
    }
    ################## Age weighting #############
    ## Get out individual ages (similar to stlocsdf)
    
    st_agesdf <- data.table(ages = unique(firststagedf$age_group_id))
    st_agesdf <- st_agesdf[ages != 33]
    ref_ages <- data.table(ref_age = unique(firststagedf$age_group_id))
    ref_ages <- ref_ages[ref_age != 33]
    
    ## Calculate distance
    st_agesdf[, age_group_position := (factor(ages, levels = c(164, 28, 5:20, 30:32, 235)))] 
    ref_ages[, ref_age := (factor(ref_age, levels = c(164, 28, 5:20, 30:32, 235)))]
    
    ## Map and calculate distances
    st_agesdf <- st_agesdf[, .(ref_age = ref_ages$ref_age),
                           by = .(ages, age_group_position)]
    
    st_agesdf[, age_dist := abs(as.numeric(age_group_position)-as.numeric(ref_age))]
    
    omega <- 0.5
    st_agesdf[, age_wt := 1/(exp(omega*abs(age_dist)))]
    st_agesdf$age_group_position <- NULL
    setnames(st_agesdf, 'ages', 'age_group_id')
    st_agesdf_1 <- copy(st_agesdf)
    
    
    residsdf <- lri[is_outlier == 0, .(location_id, location_name, sex_id, age_start, age_group_id,
                                       cf2, pred, pred_resid_log)]
    
    stpreddf <- rbindlist(lapply(unique(spdistdf$ref), function(x){
    
      weight_df <- data.table()
      for (age in unique(residsdf$age_group_id)){
    
        ## Apply age map
        age_set_1 <- st_agesdf[age_group_id == age]
        resid_subset <- residsdf[, .(sex_id, location_id, pred_resid_log, as.factor(age_group_id))] %>%
          setnames('V4', 'ref_age')
        
        resid_subset <- merge(resid_subset, age_set_1, by = 'ref_age')
        resid_subset[, age_wt := age_wt/sum(age_wt)]
        
        ## Merge on for single loc and age, getting weight for that individual age and location
        ## Have input data going into the Taiwan prediction at a single age
        ## Merge on space weights
        subset_1 <- spdistdf[ref == x]
        newdf_1 <- merge(resid_subset, subset_1, by = 'location_id')
        
        #newdf_1 <- merge(residsdf[age_group_id == age], subset_1, by = 'location_id')
        
        ## merge on age weights
    
        newdf_1[, age_space_weight := spweight*age_wt] ## calculate net weight
        
        ## Collapses to a single value with the location, sex, and age, along with the weighted residual
        test <- newdf_1[, .(weighted_resid_0.5 = weighted.mean(pred_resid_log, w = age_space_weight, na.rm = TRUE)),
                        by = .(ref,sex_id, age_group_id)]
        setnames(test, 'ref', 'location_id')
        weight_df <- rbind(weight_df, test)
      }
      
      return(weight_df)
    })) %>% merge(unique(lri[, .(age_group_id, age_start, age_end)]))
    
    print('STPREDDF')
    print(nrow(stpreddf))
    
    firststagedf <- savedfs[draw_num][[1]]
    
    
    print('FIRSTSTAGEDF')
    print(nrow(firststagedf))
    
    preddf <- merge(firststagedf[, .(location_id, sex_id, age_start, draw_pred)], stpreddf, by = c('location_id', 'sex_id', 'age_start'))
    preddf[, log_stpred := draw_pred + weighted_resid_0.5]
    
    preddf <- merge(preddf, locs_to_merge[, c('location_id', 'location_name')], by = 'location_id')
    preddf[location_name == 'United States', location_name := 'Marketscan']
    
    preddf[, mod_incidence := exp(log_stpred)]
    
    preddf[, draw := draw_num]
    preddf[, year_id := 2010]
    preddf[, bundle_id := bundle]
    preddf <- preddf[, .(location_id, sex_id, age_start, age_end, mod_incidence, draw)]
    
    return(preddf)
    ## Create and write
    
  }, mc.cores = 5))
  
  #draws_df <- copy(preddf)
  casted <- dcast(draws_df, location_id + sex_id + age_start + age_end ~ draw, value.var = 'mod_incidence')
  
  setnames(casted, grep('[[:digit:]]', names(casted), value = TRUE), paste0('incidence_', 0:999))
  new <- Sys.time()-old
  print(new)
  casted$bundle_id <- bundle
  
  print('WRITING DRAWS WIDE')
  write_csv(casted, paste0('filepath'))
  
} else{
  print('Not making draws')
  preddf[, pred := predict(base_lme4, newdata = preddf, allow.new.levels = T)]
  ggplot(data = lri[cf2 < 10000], aes(x = age_start, y = cf2)) + 
    geom_point(aes(y = exp(pred), color = 'prediction')) + 
    geom_point(aes(y = cf2, color = 'input_data')) + 
    facet_wrap(location_id ~ sex_id) + 
    ## Do I need to add exp(1) to the residual??? Or something else
    geom_segment(aes(xend = age_start, yend = exp(pred)))
  
  
  firststagedf <- copy(preddf)
  
  stlocsdf <- firststagedf[, 'location_id'] %>%
    merge(locs, by = 'location_id') %>% unique
  
  ## Calculate space distance for each location
  locs_to_merge <- get_location_metadata(35)
  locs_to_merge <- locs_to_merge[, c('location_id', 'region_name', 'super_region_name', 'location_name')]
  
  prep_locs <- lri[, c('location_id', 'region_name', 'super_region_name', 'location_name')] %>% unique
  ## Spits out data frame with distances from datapoints for the predictions
  ## Give ref to know what location it's referring to
  
  
  ############ Space weighting ##################
  spdistdf <- rbindlist(mclapply(unique(stlocsdf$location_id), function(x){
    loc_ref <- locs_to_merge[location_id == x]
    ## Use reference super region and region
    stlocsdf$ref <- x
    
    ## Want just spdist of 0 and 1 for if country/if not country
    copy(prep_locs)[location_id == x, spdist := 0][location_id != x, spdist := 1][, ref := x]
    
    
  }, mc.cores = 5))
  
  
  zeta <- 0.96
  ## Assign weights relative to how many input sources there are and whether they are equal to predicted countries
  ## Now weight adds up to 1
  ## Calculate residual
  for (l in unique(spdistdf$ref)){
    spdistdf[spdist == 0 & ref == l, spweight := zeta][spdist == 1 & ref == l, spweight := (1-zeta)/nrow(spdistdf[ref == l & spdist == 1])] ## Divide by number of other sources
  }
  
  ################## Age weighting #############
  
  ## Get out individual ages (similar to stlocsdf)
  
  st_agesdf <- data.table(ages = unique(firststagedf$age_group_id))
  st_agesdf <- st_agesdf[ages != 33]
  ref_ages <- data.table(ref_age = unique(firststagedf$age_group_id))
  ref_ages <- ref_ages[ref_age != 33]
  
  ## Calculate distance
  st_agesdf[, age_group_position := (factor(ages, levels = c(164, 28, 5:20, 30:32, 235)))] 
  ref_ages[, ref_age := (factor(ref_age, levels = c(164, 28, 5:20, 30:32, 235)))]
  
  ## Map and calculate distances
  st_agesdf <- st_agesdf[, .(ref_age = ref_ages$ref_age),
                         by = .(ages, age_group_position)]
  
  st_agesdf[, age_dist := abs(as.numeric(age_group_position)-as.numeric(ref_age))]
  
  ## Set omega and the age weights based on distance in age group id's
  omega <- 0.5
  st_agesdf[, age_wt := 1/(exp(omega*abs(age_dist)))]
  st_agesdf$age_group_position <- NULL
  setnames(st_agesdf, 'ages', 'age_group_id')
  #age_map$ref_age <- NULL
  
  ## age_group_id.x is the group from the model
  ## ref_age_group is the group to merge onto
  
  ## Calculate predicted residual
  ## get weighted mean of spatial log
  ## Do I want weighting by sdi quintile?? I think that'd make sense
  ## So Taiwan would take in zero data from Phillipines
  ## Would make sense.... but we'll get to it
  
  ## resids: location id is where the actual data comes from
  ## ref refers to the predicted country that the weighted residual is going to affect
  ## Returns single weighted residual for each location
  ## Again, residuals in log space
  
  residsdf <- lri[is_outlier == 0, .(location_id, location_name, sex_id, age_start, age_group_id, cf2, pred, pred_resid_log)]
  #residsdf[age_group_id == 235, age_group_id := 33]
  stpreddf <- rbindlist(mclapply(unique(spdistdf$ref), function(x){
    weight_df <- data.table()
    for (age in unique(residsdf$age_group_id)){
      ## Apply age map
      age_set <- st_agesdf[age_group_id == age]
      resid_subset <- residsdf[, .(sex_id, location_id, pred_resid_log, as.factor(age_group_id))] %>%
        setnames('V4', 'ref_age')
      
      ## merge on age weights
      age_weight_df <- merge(resid_subset, age_set, by = c('ref_age'))
      
      ## Subset by age, blown up with age weights
      ages_subset <- age_weight_df[age_group_id == age]
      ## Scale to 1
      ages_subset[, age_wt := age_wt/sum(age_wt)]
      
      ## all_age_weighted ia a dt of each input location with each age_group_id with the age_wt relative to the ref_ages ( so 4*20*20)
      
      
      ## Merge on for single loc and age, getting weight for that individual age and location
      ## Have input data going into the Taiwan prediction at a single age
      ## Merge on space weights
      subset <- spdistdf[ref == x]
      newdf <- merge(resid_subset, subset, by = 'location_id')
      #newdf <- merge(newdf, age_weight_df, by = 'ref_age')
      
      ## merge on age weights
      ## Both 163 rows. It's literally the space weights + age weights
      newdf <- merge(newdf, ages_subset, by = c('ref_age', 'sex_id', 'location_id', 'pred_resid_log'))
      newdf[, age_space_weight := spweight*age_wt] ## calculate net weight
      
      ## Collapses to a single value with the location, sex, and age, along with the weighted 
      newdf <- merge(newdf, unique(lri[, c('age_group_id', 'age_start', 'age_end')]), by = 'age_group_id')
      
      test <- newdf[, .(weighted_resid_0.5 = weighted.mean(pred_resid_log, w = age_space_weight, na.rm = TRUE)),
                    by = .(ref, sex_id, age_start)]
      setnames(test, 'ref', 'location_id')
      weight_df <- rbind(weight_df, test)
      print(head(weight_df))
    }
    ## ages_subset is the age weight for the ref age
    
    return(weight_df)
  }, mc.cores = 5))
  
  ## Plot weighted residuals
  
  plot_data <- merge(lri[, c('location_id', 'sex_id', 'age_start','cf2', 'pred')], stpreddf, by = c('location_id', 'sex_id', 'age_start'))
  
  ggplot(data = plot_data, aes(x = age_start, y = cf2)) + 
    
    geom_point(aes(y = cf2, color = 'input_data'), size = 3) + 
    facet_wrap(location_id ~ sex_id) + 
    geom_segment(aes(xend = age_start, yend = exp(pred + weighted_resid_0.5))) + 
    geom_point(data = plot_data, aes(y = exp(pred + weighted_resid_0.5)))
  
  firststagedf <- firststagedf[year_id == 2010]
  firststagedf[, exp_pred := exp(pred)]
  preddf <- merge(firststagedf[, .(location_id, sex_id, age_start, pred)], stpreddf, by = c('location_id', 'sex_id', 'age_start'))
  preddf[, log_stpred := pred + weighted_resid_0.5]
  
  preddf[, modeled_cf2 := exp(log_stpred)]
  if(interactive()) { ## SEE HOW PREDS COMPARE AGAINST THE FIRST STAGE
    
    ggplot() + geom_point(data = lri[location_id == 16], aes(x = age_start, y = cf2), shape = 19, size = 3, alpha = 0.5) +
      geom_point(data = preddf[location_id == 16], aes(x = age_start, y =exp(log_stpred), color = 'second_stage'), size = 1.15, color = 'blue') +
      geom_point(data = preddf[location_id == 16], aes(x = age_start, y = exp(pred) + 1, color = 'first_stage'), size = 1.15, color = 'red') +
      
      facet_wrap(~ sex_id)
  } 
  