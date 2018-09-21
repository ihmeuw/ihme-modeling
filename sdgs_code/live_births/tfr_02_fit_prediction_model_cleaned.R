################################################################################
## Purpose: Fits first stage linear model for ST-GPR, and creates smoothed prior (total fertility)
## Outputs: Smoothed prior for GPR
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, ggplot2, lme4, boot, magrittr, haven) # load packages and install if not installed
 
root <- "FILEPATH"
username <- ifelse(Sys.info()[1]=="Windows","[username]",Sys.getenv("USER"))
code_dir <- "FILEPATH"
h <- "FILEPATH"
         
################################################################################
### Arguments 
################################################################################

predict_re <- T

#Toggle when change anything
mod_id <- ifelse(Sys.info()[1]=="Windows",59, commandArgs()[3])

form <- ""

sep_CHN <- T

#ST parameters
lam <-.6

zet <- .99

linear_to_gpr <- F

non_sampling <- T

#TFR BOUND
tfr_bound <- 9.5

################################################################################ 
### Paths for input and output 
################################################################################

input_path <- "FILEPATH"
output_path <- "FILEPATH"

################################################################################
### Data 
################################################################################

tfr.data <- fread("FILEPATH")


################################################################################
### Functions 
################################################################################

source("FILEPATH") # load get_locations
codes <- get_locations(2016) %>% as.data.table
merge.codes <- codes[,list(location_id, region_id, ihme_loc_id)]
source("FILEPATH") # get demographics spacetime smoothing function
source("FILEPATH") #get demographics spacetime location function that uses adapted location hierarchy
source("FILEPATH") #load get_covariates

################################################################################
### Code 
################################################################################

############################################
## Step 1: Apply logit transformations 
## to estimates and their variances
############################################


    tfr.data[,logit_bound_tfr := logit(tfr/tfr_bound)]
    
    # apply the delta method (for transforming variance to logit space) 
    # Note: have to take into account factor by which you divide TFR when converting it to logit space to be modeled
    # Delta method: Var(G(X)) = Var(X) * G'(X)^2

    tfr.data[!is.na(var), logit_bound_var := var*(1/(tfr_bound*(tfr/tfr_bound)*(1-(tfr/tfr_bound))))^2]


############################################
## WITH SEPARATE CHINA MODEL
############################################

if (sep_CHN){ 
    
    ###########################################################
    ## Step A2: pull in 5q0 as China covariate
    ###########################################################
    
        #Pulling from latest non-shock results
        child <- read_dta("FILEPATH") %>% as.data.table %>% .[sex == "both", .(ihme_loc_id, year = year -.5, mean_est = q_u5_med)]
        
        
        child <- merge(child, merge.codes, by = "ihme_loc_id")
        child <- child[grepl("CHN", ihme_loc_id)]
        setnames(child, "mean_est", "under5_mort")
        child[, c("ihme_loc_id", "region_id") := NULL]
        
        china.whole <- copy(child[location_id == 44533,])
        china.whole[,location_id := 6]
        child <- rbindlist(list(china.whole, child))
        
        tfr.data <- merge(tfr.data, child, by=c("location_id", "year"), all.x=T)
    
    ############################################################
    ## Step A3: Set up and fit Brazil Intercept-shift model to address source-specific biases
    ############################################################
    
        bra.data <- tfr.data[ihme_loc_id %like% "BRA"]
        
        bra.data <- merge(bra.data, codes[, .(location_id, location_name)], by = "location_id")
        bra.data[!is.na(tfr) & id %like% "nhss", cat := "nhss"]
        bra.data[!is.na(tfr) & id %like% "stat_bureau", cat := "VR"]
        bra.data[!is.na(tfr) & is.na(cat), cat := "Other"]
        bra_form <- as.formula("logit_bound_tfr ~ log(ldi) + medu  + cat")
        
        state_models <- sapply(unique(bra.data$location_id), function(loc) {
            
            state_model <- lm(bra_form, data = bra.data[!is.na(tfr) & location_id == loc])
            
            coefs <- coef(state_model)
            
            ## Is non-VR greater than VR, if so adjust VR upwards--if not, adjust non-VR upwards
            
            adjust_VR <- (coefs[5] < 0) 
            
            bra.data[!is.na(tfr) & location_id == loc, pred:= predict(state_model)][!is.na(tfr) & location_id == loc, resid := logit_bound_tfr - pred]
            
            if (adjust_VR) {
                
               bra.data[!is.na(tfr) & location_id == loc, adjusted := log(ldi)*coefs[2] + medu*coefs[3] + coefs[1] + resid]
                
            } else {
        
                bra.data[!is.na(tfr) & location_id == loc, adjusted := log(ldi)*coefs[2] + medu*coefs[3] + coefs[1] + coefs[5] + resid]
            }
            

            return(list(coef(state_model), unique(bra.data[location_id == loc]$location_name)))
            
        }, simplify = F, USE.NAMES = T)
        

        ## adjust data and clean columns 
        intshiftcols <- c("pred", "resid", "cat", "adjusted", "location_name")
        bra.data[, logit_bound_tfr := adjusted]
        bra.data[, (intshiftcols) := NULL]
        
        ## combine adjusted brazil data with rest of countries
        tfr.data <- tfr.data[!ihme_loc_id %like% "BRA"]
        tfr.data <- rbindlist(list(tfr.data, bra.data), use.names = T, fill = T)

    ############################################################
    ## Step A3: Set up and fit Non-china and China and PRK models 
    ############################################################
        tfr.data[, iso3 := substr(ihme_loc_id, 1,3)] #allows us to apply same country effects to subnationals
            
    ## Non-China
        model_form <- as.formula("logit_bound_tfr ~ log(ldi) + medu + (1|iso3) + (1 + log(ldi)+ medu|region_id)")
        model <- lmer(model_form, data= tfr.data[!grepl("CHN", ihme_loc_id) & level == 3]) 
        
    ## China
    
        CHN <- tfr.data[grepl("CHN", ihme_loc_id)]
        model_form_CHN <- as.formula("logit_bound_tfr ~ log(ldi) + under5_mort + (1|location_id)")
        model_CHN <- lmer(model_form_CHN, data=CHN)
        
    ## PRK: predict based on global average
        
        model_form_PRK <- as.formula("logit_bound_tfr ~ log(ldi) + medu")
        model_PRK <- lm(model_form_PRK, data = tfr.data[!grepl("CHN", ihme_loc_id) & level == 3])
        
    
    ############################################################
    ## Step A4: Make first-stage predictions for prepped data
    ###########################################################
        
        # first stage predictions
        
        if (predict_re) {
            
            tfr.data[!(ihme_loc_id %in% c("CHN", "PRK")), logit_bound_tfr_pred := predict(model, tfr.data[!(ihme_loc_id %in% c("CHN", "PRK"))], allow.new.levels = T)]
            
            tfr.data[grepl("CHN", ihme_loc_id), logit_bound_tfr_pred := predict(model_CHN , tfr.data[grepl("CHN", ihme_loc_id)], allow.new.levels = T)]
            
            tfr.data[grepl("PRK", ihme_loc_id), logit_bound_tfr_pred := predict(model_PRK, tfr.data[grepl("PRK", ihme_loc_id)], allow.new.levels = T)]
            
            print("Predicted Using Random Effects")
            
            tfr.data[, iso3 := NULL]
        } else {
            
            tfr.data[!(ihme_loc_id %in% c("CHN", "PRK")), logit_bound_tfr_pred := summary(model)$coefficients[1] + log(ldi)*summary(model)$coefficients[2] + medu*summary(model)$coefficients[3]]
            
            tfr.data[grepl("CHN", ihme_loc_id), logit_bound_tfr_pred := summary(model_CHN)$coefficients[1] + log(ldi)*summary(model_CHN)$coefficients[2] + under5_mort*summary(model_CHN)$coefficients[3]]
            
            tfr.data[grepl("PRK", ihme_loc_id), logit_bound_tfr_pred := summary(model_PRK)$coefficients[1] + log(ldi)*summary(model_PRK)$coefficients[2] + medu*summary(model_PRK)$coefficients[3]]
            
        }

###############################################
## WITHOUT SEPARATE CHINA MODEL       
###############################################
        
} else {
    
    ############################################################
    ## Step B3: Set up and fit model
    ############################################################
    
    model_form <- as.formula("logit_bound_tfr ~ log(ldi) + medu + (1|location_id) + (1 + log(ldi)+ medu|region_id)") #added regional intercept compared to last year
    model <- lmer(model_form, data= tfr.data) 
    
    
    ############################################################
    ## Step B4: Make first-stage predictions for prepped data
    ###########################################################
    
    tfr.data[, logit_bound_tfr_pred := predict(model, tfr.data, allow.new.levels = T)]
    
}
    

    
###########################################################
## Calculating residuals 
###########################################################
    # calculate residuals 
    
    tfr.data[,resid := logit_bound_tfr - logit_bound_tfr_pred]
    
    stopifnot(nrow(tfr.data[!is.na(tfr) & is.na(var)]) == 0)
    

###########################################################
## Exclude missing location-years
###########################################################

    
    tfr.data <- tfr.data[!is.na(logit_bound_tfr_pred)]
    
    
    
###########################################################
## Space-Time Smoothing
###########################################################
    
    #merge on adapted location hierarchy
    adapted_st_regions <- get_spacetime_loc_hierarchy()
    
    setnames(adapted_st_regions, "region_name", "adapted_region_name" )
    
    to.smooth <- merge(tfr.data, adapted_st_regions, by = c("ihme_loc_id", "location_id"), all.y = T, allow.cartesian = T)
    
    # specifies region in which to borrow strength over space for subnationals
    to.smooth[, iso3 := as.character(adapted_region_name)]
    

    # run the smoothing
    resids <- resid_space_time(data = to.smooth , min_year=min(to.smooth[,year], na.rm=T), max_year=max(to.smooth[,year], na.rm=T), lambda= lam, zeta = zet, lambda_by_density = T, lambda_by_year = F) %>% as.data.table 

    
    # merge residuals and tfr values together
    smoothed <- merge(to.smooth, resids, all = T, by=c("year", "ihme_loc_id", "adapted_region_name"))
    
    #keep keeps and drop columns for new regions
    smoothed <- smoothed[keep == 1]
    smoothed[, logit_bound_tfr_pred_smooth := logit_bound_tfr_pred + pred.2.resid]
    

    
    # put underscores in region names (where conventional), replace region name with country code if subnational and borrowed strength from other subnationals
    
    smoothed[, region_name := gsub(" ", "_", region_name)]
    smoothed[, adapted_region_name := gsub(" ", "_", region_name)]
    
    #indexing as inputs to GPR
    smoothed[, category := "data"]
    
    # exponentiating modeled predictions to get sensible tfrs
    smoothed[, c("tfr_pred", "tfr_pred_smooth") := list(inv.logit(logit_bound_tfr_pred)*tfr_bound, inv.logit(logit_bound_tfr_pred_smooth)*tfr_bound)]
    

    #non-sampling error
    
    if (non_sampling) {
     

        smoothed[!is.na(tfr), res := logit_bound_tfr-logit_bound_tfr_pred_smooth]
        smoothed[, med := median(res, na.rm = T), by = ihme_loc_id] 
        smoothed[, mad := 1.4826*median(abs(res-med), na.rm = T), by = ihme_loc_id] #MAD multiplier to estimate standard deviation
        smoothed[, med := NULL]
        
        print(range(smoothed$mad))
        write.csv(smoothed, "FILEPATH", row.names = F)
    }

    ## Termed MSE because consistent with GPR code, but feeding in the MAD instead, squaring MAD to move it into variance space
    smoothed[, mse := mad^2]
    
    ## If location is missing MAD because no data or has MSE of 0 because of only one data point, give it the regional
    smoothed[, rgmed := median(res, na.rm = T), by = region_id]
    smoothed[, rgmad := 1.4826*(median(abs(res-rgmed), na.rm = T)), by = region_id]
    smoothed[is.na(mse) | mse == 0, mse := rgmad^2]
    smoothed[, rgmed := NULL]

    ## Adding non-sampling error from above
    smoothed[(data_rich == F | vital == F) & !is.na(tfr), logit_bound_var := mse + logit_bound_var]
    
    # save smoothed data
    write.csv(smoothed[, .(ihme_loc_id, year, logit_bound_tfr_pred, logit_bound_tfr_pred_smooth, mse, lambda, zeta)], "FILEPATH", row.names = F)
    
    # save smoothed data with only the lines with actual data 
    write.csv(smoothed[!is.na(tfr), .(year, ihme_loc_id, adjusted_vr, logit_bound_tfr, logit_bound_var, category)], "FILEPATH", row.names = F)


################################################################################ 
### End
################################################################################