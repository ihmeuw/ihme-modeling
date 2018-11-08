

########### PREP AND LOAD #############

rm(list = ls())


closeAllConnections()
library(ggplot2)
library(data.table)
library(lme4)
library(htmlwidgets, lib = 'FILEPATH')
library(merTools, lib = 'FILEPATH')
library(boot)
library(RMySQL)
library(slackr)
library(mortcore, lib = "FILEPATH")
library(parallel)
library(magrittr)
library(readr)
library(nlme)
FILEPATH
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')

print(commandArgs(trailingOnly = T))
#bundle <- 3
#make_draws <- 'F'
bundle <- commandArgs()[5]
make_draws <- commandArgs()[6]
make_draws <- as.logical(make_draws)
print(make_draws)

prep_data <- fread('FILEPATH')
prep_data[age_group_id == 28, age_end := 1]
prep_data[cf1 == '.', cf1 := NA]
prep_data$cf1 <- as.numeric(prep_data$cf1)

locs <- get_location_metadata(35)
locs <- locs[, c('location_id', 'location_name', 'region_name', 'super_region_name', 'level')]
locs_to_merge <- locs[, c('location_id', 'region_name', 'super_region_name', 'location_name')]

##### RUN ###############
print(class(bundle))
print(bundle)

bun_df <- fread('FILEPATH')
bun <- unique(bun_df[bundle_id == bundle]$bundle_name)
print(bun)

lri <- prep_data[bundle_id == bundle]
lri <- merge(lri, locs[, c('location_id', 'region_name', 'super_region_name', 'location_name')], by = 'location_id')

#print(head(lri))


lri[, is_outlier := 0][location_id == 16 & age_start == 70 & sex_id == 1 & bundle_id == 19, is_outlier := 1]

lri <- lri[is_outlier == 0]

lri[cf1 == 1, cf1 := 0.999]

lri <- lri[!(is.na(cf1))]
lo <- FALSE
if(length(unique(lri$age_start)) <= 4) {
  base_lme4 <- loess(logit(cf1) ~ age_start + sex_id,
                  data = lri[cf1 > 0 & is_outlier != 1], parametric = 'sex_id')
  print('lo')
  lo <- TRUE
} else if(all(unique(lri$location_id) == c(6, 102))){
  print('only Marketscan and Taiwan')
  base_lme4 <- lmer(logit(cf1) ~ poly(age_start, 3) + sex_id +
                 op_envelope + ip_envelope + (1|location_id),
               data = lri[cf1 > 0 & is_outlier != 1])
  print('1')

} else if(length(unique(lri$location_id)) == 1) {
  print('only one location')
  base_lme4 <- glm(logit(cf1) ~ poly(age_start, 3) + sex_id +
                 op_envelope + ip_envelope,
               data = lri[cf1 > 0 & is_outlier != 1])
  print('2')
  
} else{
  base_lme4 <- lmer(logit(cf1) ~ poly(age_start, 3) + sex_id +
                 op_envelope + ip_envelope + 
                 (1|location_id),
               data = lri[cf1 > 0 & is_outlier != 1])
  print('3')

}

## Predict off of base

preddf <- fread('FILEPATH')
preddf[, c('pred', 'cf1_pred') := NULL]
preddf <- preddf[year_id == 2010 & location_id != 533 & age_group_id.x != 164][, pred := NULL]
preddf <- unique(preddf[age_group_id.x != 33][, age_group_id.y := NULL][, V1 := NULL])
preddf[, pred := predict(base_lme4, newdata = preddf, allow.new.levels = T)]

setnames(preddf, 'age_group_id.x', 'age_group_id')

locs[location_id %in% preddf$location_id, keep := 1]
locs[!(location_id %in% preddf$location_id), keep := 0]


lri[, pred := (predict(base_lme4, newdata = lri, allow.new.levels = T))]
lri[, pred_resid_logit := logit(cf1) - pred]


# Draws
## Look at first stage predictions
ggplot(data = lri, aes(x = age_start, y = cf1)) + 
  geom_point(aes(y = inv.logit(pred), color = 'prediction')) + 
  geom_point(aes(y = cf1, color = 'input_data')) + 
  facet_wrap(location_id ~ sex_id) + 
  geom_segment(aes(xend = age_start, yend = inv.logit(pred)))


ggplot(data = lri, aes(x = age_start, y = inv.logit(pred))) + 
  geom_line() + 
  facet_wrap(location_id ~ sex_id)


if(make_draws == TRUE){
  
  print('MAKING DRAWS')

  if(lo == TRUE){
    preds <- predict(base_lme4, newdata = preddf, allow.new.levels = T, se = TRUE)
    pred_dt <- data.table(preds = preds$fit,
                          se = preds$se.fit)
    preddf <- cbind(preddf, pred_dt)
    preddf$ID <- seq.int(nrow(preddf))
    print(names(preddf))
    draws_df <- preddf[!(is.na(preds))] 
    
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
  }else{
    test <- predictInterval(base_lme4, newdata = preddf, n.sims = 1000, level = 0.9, stat = 'mean', returnSims = TRUE)
    preds <- data.table(attr(test, 'sim.results'))
    setnames(preds, grep('[[:digit:]]', names(preds), value = TRUE), paste0('indv_cf_', 0:999))
    
    preddf <- cbind(preddf[, c('location_id', 'sex_id', 'age_start', 'age_end', 'age_group_id', 'ip_envelope', 'op_envelope', 'haqi_mean', 'pred')], preds)
    preddf <- melt(preddf, measure = patterns('indv_cf_'), variable.name = 'draw', value.name = c('draw_pred'))
    
    means <- preddf[, .(mean_draw_cf1 = mean(draw_pred)),
                    by = .(location_id, sex_id, age_start, age_end, age_group_id)]
    
    ## Vet upper and lower after bootstrapping
    lowers <- preddf[, .(mean_draw = inv.logit(quantile(draw_pred, 0.025))),
                     by = .(location_id, sex_id, age_start, age_end, age_group_id)]
    uppers <- preddf[, .(mean_draw = inv.logit(quantile(draw_pred, 0.975))),
                     by = .(location_id, sex_id, age_start, age_end, age_group_id)]
    
    preddf <- merge(preddf, means, by = c('location_id', 'sex_id', 'age_start', 'age_end', 'age_group_id'))
    setkey(preddf, 'draw')
    ## vet plots
    ggplot(data = preddf[location_id == 8]) + 
      geom_point(aes(x = age_start, y = inv.logit(draw_pred), color = 'draw predictions')) + 
      geom_point(aes(x = age_start, y = inv.logit(pred), color = 'predictions')) + 
      facet_wrap(~sex_id)
    
    ggplot(data = lowers, aes(x = age_start, y = mean_draw)) + 
      geom_point(data = lowers, aes(color = 'lower CI')) + 
      geom_point(data = uppers, aes(color = 'upper CI'))
  }
  
  old <- Sys.time()
  
  savedfs <- split(preddf, by = 'draw')
  
  draws_df <- rbindlist(mclapply(c(1:1000), function(draw_num){
    
    firststagedf <- savedfs[draw_num][[1]]
    stlocsdf <- firststagedf[, 'location_id'] %>%
      merge(locs, by = 'location_id') %>% unique
    
    ## Calculate space distance for each location
    prep_locs <- lri[, c('location_id', 'region_name', 'super_region_name', 'location_name')] %>% unique
  
    
    ############ Space weighting ##################
    spdistdf <- rbindlist(lapply(unique(stlocsdf$location_id), function(x){
      loc_ref <- locs_to_merge[location_id == x]
      ## Use reference super region and region
      stlocsdf$ref <- x
      
      ## Want just spdist of 0 and 1 for if country/if not country
      copy(prep_locs)[location_id == x, spdist := 0][location_id != x, spdist := 1][, ref := x]
      
      
    }))
    
    zeta <- 0.94
    
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
    #age_map$ref_age <- NULL
    
    
    residsdf <- lri[is_outlier == 0, .(location_id, location_name, sex_id, age_start, age_group_id, cf1, pred, pred_resid_logit)]
    stpreddf <- rbindlist(lapply(unique(spdistdf$ref), function(x){
      weight_df <- data.table()
      for (age in unique(residsdf$age_group_id)){
        ## Apply age map
        age_set_1 <- st_agesdf[age_group_id == age]
        resid_subset <- residsdf[, .(sex_id, location_id, pred_resid_logit, as.factor(age_group_id))] %>%
          setnames('V4', 'ref_age')
        
        resid_subset <- merge(resid_subset, age_set_1, by = 'ref_age')
        resid_subset[, age_wt := age_wt/sum(age_wt)]
       
        subset_1 <- spdistdf[ref == x]
        newdf_1 <- merge(resid_subset, subset_1, by = 'location_id')
        
    
        
    
        newdf_1[, age_space_weight := spweight*age_wt] ## calculate net weight
        
        ## Collapses to a single value with the location, sex, and age, along with the weighted residual
        test <- newdf_1[, .(weighted_resid_0.5 = weighted.mean(pred_resid_logit, w = age_space_weight, na.rm = TRUE)),
                        by = .(ref,sex_id, age_group_id)]
        setnames(test, 'ref', 'location_id')
        weight_df <- rbind(weight_df, test)
      }
     
      return(weight_df)
    })) %>% merge(unique(lri[, .(age_group_id, age_start, age_end)]))
    
    print('STPREDDF')
    print(nrow(stpreddf))
    
    firststagedf <- firststagedf
    firststagedf[, inv_logit_pred := inv.logit(draw_pred)]
    
    print('FIRSTSTAGEDF')
    print(nrow(firststagedf))
    
    preddf <- merge(firststagedf[, .(location_id, sex_id, age_start, draw_pred)], stpreddf, by = c('location_id', 'sex_id', 'age_start'))
    preddf[, logit_stpred := draw_pred + weighted_resid_0.5]
    
    preddf <- merge(preddf, locs_to_merge[, c('location_id', 'location_name')], by = 'location_id')
    preddf[location_name == 'United States', location_name := 'Marketscan']
    
    preddf[, mod_indv_cf := inv.logit(logit_stpred)]
    print(draw_num)
    preddf[, draw := draw_num]
    preddf[, year_id := 2010]
    preddf[, bundle_id := bundle]
    preddf <- preddf[, .(location_id, sex_id, age_start, age_end, age_group_id, mod_indv_cf, draw)]
    
    print('PREDDF BEFORE RETURNING TO RBINDLIST')
    print(nrow(preddf))
    
    return(preddf)
    ## Create and write
   
  }, mc.cores = 5))
  
  #draws_df <- unique(draws_df)
  casted <- dcast(draws_df, location_id + sex_id + age_start + age_end + age_group_id ~ draw,
                  value.var = 'mod_indv_cf')
  setnames(casted, grep('[[:digit:]]', names(casted), value = TRUE), paste0('indv_cf_', 0:(length(names(casted))-6)))
  
  casted$bundle_id <- bundle
  print('CASTED ROWS ARE:')
  print(nrow(casted))
  print(ncol(casted))
 
  write_csv(casted, paste0('FILEPATH'))

} else{
  
  print('NOT MAKING DRAWS')
  preddf[, pred := predict(base_lme4, newdata = preddf, allow.new.levels = T)]
  
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
  
  omega <- 0.5
  st_agesdf[, age_wt := 1/(exp(omega*abs(age_dist)))]
  st_agesdf$age_group_position <- NULL
  setnames(st_agesdf, 'ages', 'age_group_id')
  st_agesdf_1 <- copy(st_agesdf)
  
  residsdf <- lri[is_outlier == 0, .(location_id, location_name, sex_id, age_start, age_group_id, cf1, pred, pred_resid_logit)]
  #residsdf[age_group_id == 235, age_group_id := 33]
  stpreddf <- rbindlist(mclapply(unique(spdistdf$ref), function(x){
    weight_df <- data.table()
    for (age in unique(residsdf$age_group_id)){
      ## Apply age map
      age_set_1 <- st_agesdf[age_group_id == age]
      resid_subset <- residsdf[, .(sex_id, location_id, pred_resid_logit, as.factor(age_group_id))] %>%
        setnames('V4', 'ref_age')
      
      resid_subset <- merge(resid_subset, age_set_1, by = 'ref_age')
      
      
  
      ## Subset by age, blown up with age weights
      ages_subset_1 <- resid_subset[age_group_id == age]
      ## Scale to 1
      ages_subset_1[, age_wt := age_wt/sum(age_wt)]
      
  
      ## Merge on space weights
      subset_1 <- spdistdf[ref == x]
      newdf_1 <- merge(resid_subset, subset_1, by = 'location_id')
      
  
      
      ## merge on age weights
  
      newdf_1[, age_space_weight := spweight*age_wt] ## calculate net weight
      
      ## Collapses to a single value with the location, sex, and age, along with the weighted residual
      test <- newdf_1[, .(weighted_resid_0.5 = weighted.mean(pred_resid_logit, w = age_space_weight)),
                      by = .(ref,sex_id, age_group_id)]
      setnames(test, 'ref', 'location_id')
      weight_df <- rbind(weight_df, test)
    }
    
    return(weight_df)
  }, mc.cores = 5)) %>% merge(unique(lri[, .(age_group_id, age_start, age_end)]))
  
  ## Plot weighted residuals
  
  plot_data <- merge(lri[, c('location_id', 'sex_id', 'age_start','cf1', 'pred')], stpreddf, by = c('location_id', 'sex_id', 'age_start'))
  
  ggplot(data = plot_data, aes(x = age_start, y = cf1)) +
    # geom_point(aes(y = inv.logit(pred), color = 'prediction')) +
    geom_point(aes(y = cf1, color = 'input_data', size = 3)) +
    facet_wrap(location_id ~ sex_id) +
    geom_segment(aes(xend = age_start, yend = inv.logit(pred + weighted_resid_0.5))) +
    geom_point(data = plot_data, aes(y = inv.logit(pred + weighted_resid_0.5)))
  
  
  ## Need to get age weights
  
  firststagedf <- firststagedf[year_id == 2010]
  firststagedf[, inv_logit_pred := inv.logit(pred)]
  preddf <- merge(firststagedf[, .(location_id, sex_id, age_start, pred)], stpreddf, by = c('location_id', 'sex_id', 'age_start'))
  preddf[, logit_stpred := pred + weighted_resid_0.5]
  
  preddf[, modeled_cf1 := inv.logit(logit_stpred)]
  
  
  if(interactive()) { ## SEE HOW PREDS COMPARE AGAINST THE FIRST STAGE
    
    ggplot() + geom_point(data = lri[location_id == 102], aes(x = age_start, y = cf1), shape = 19, size = 3, alpha = 0.5) +
      geom_point(data = preddf[location_id == 102], aes(x = age_start, y = inv.logit(logit_stpred), color = 'second_stage'), size = 1.15, color = 'blue') +
      geom_point(data = preddf[location_id == 102], aes(x = age_start, y = inv.logit(pred), color = 'first_stage'), size = 1.15, color = 'red') +
  
      facet_wrap(~ sex_id)
  
  }
}
  
  