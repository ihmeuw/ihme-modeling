

  rm(list=ls())
  library(foreign); library(plyr); library(argparse); library(lme4); library(stringr); library(haven); library(readstata13); library(data.table); library(mortdb, lib = FILEPATH)
  

## set parameters
  end_year <- gbd_year

  source("functions/space_time.r")

  old_ap <- get_locations()
  old_ap <- old_ap[old_ap$ihme_loc_id %in% c("IND_44849", "XSU", "XYG"),]
  codes <- get_locations()
  codes <- rbind(codes, old_ap)
  region_map <- codes[,c("ihme_loc_id","region_name","super_region_name","level")]


  level_1 <- unique(codes$ihme_loc_id[codes$level_1 == 1]) # All nationals without subnationals
  level_2 <- unique(codes$ihme_loc_id[codes$level_2 == 1 | codes$level == -1]) # All subnationals at the India state level, minus parents
  level_3 <- unique(codes$ihme_loc_id[codes$level_3 == 1]) # All subnationals at the India state/urbanicity level, minus parents
  level_4 <- unique(codes$ihme_loc_id[codes$level_4 == 1]) # All subnational at the GBR UTLA level, minus parents

  keep_level_2 <- unique(codes$ihme_loc_id[(codes$level_1 == 0 & codes$level_2 == 1 & codes$level_3 == 0) | codes$level == -1])
  keep_level_3 <- unique(codes$ihme_loc_id[codes$level_1 == 0 & codes$level_3 == 1 & codes$level_4 == 0])
  keep_level_4 <- unique(codes$ihme_loc_id[codes$level_1 == 0 & codes$level_4 == 1])
  keep_level_1 <- unique(codes$ihme_loc_id[!(codes$ihme_loc_id %in% keep_level_2) & !(codes$ihme_loc_id %in% keep_level_3) & !(codes$ihme_loc_id %in% keep_level_4)])

######################
## Prep completeness data
######################

## Read in adult and child completeness
  ddm <- setDT(read_dta(FILEPATH))
  ddm$year <- floor(ddm$year)
  ddm <- ddm[ddm$year >= 1950,]
  ddm <- ddm[order(ddm$iso3_sex_source, ddm$year, ddm$detailed_comp_type),]
  ddm <- ddm[!(ihme_loc_id == "IND_43916" & grepl("CCMP", detailed_comp_type) & grepl("SRS", iso3_sex_source))]

  ss <- grepl("SAU",ddm$ihme_loc_id) & grepl("VR", ddm$iso3_sex_source)
  mar <- grepl("MAR",ddm$ihme_loc_id) & grepl("VR", ddm$iso3_sex_source)
  ddm <- ddm[(grepl("both", iso3_sex_source) & !ss & !mar) | (!grepl("both", iso3_sex_source) & (ss | mar)) | 
               grepl("CCMP", detailed_comp_type),]

## Drop some outliers
  ddm <- ddm[!(grepl("CHN_44533&&both&&DSP",ddm$iso3_sex_source) & ddm$comp_type == "u5" & ddm$year < 1996),]
  ddm <- ddm[!(ddm$iso3_sex_source == "DOM&&both&&VR" & ddm$comp_type == "u5" & ddm$year > 2009),]
  ddm <- ddm[!(ddm$iso3_sex_source == "MEX_4664&&both&&VR" & ddm$comp_type != "u5" & ddm$year %in% c(1988,1989)),]
  ddm <- ddm[!(ddm$iso3_sex_source %in% c("XIR&&both&&SRS","XIU&&both&&SRS") & ddm$comp_type == "u5" & ddm$year < 1990),] 
  ddm <- ddm[!(ddm$iso3_sex_source == "BOL&&both&&VR" & ddm$comp_type == "u5" & ddm$year %in% c(2000,2001,2002,2003)),]
  
## For CCMP methods, take weighted average to get both sexes combined
  ddm_no_ccmp <- ddm[!(grepl("CCMP", detailed_comp_type))]
  ccmp_only <- ddm[(grepl("CCMP", detailed_comp_type))]

  # read in all ages deaths
  deaths_combined <- fread(FILEPATH)
  deaths_combined <- deaths_combined[, list(ihme_loc_id, sex, popyear1, popyear2, deaths)]

  # Extract ihme loc id
  deaths_combined[, ihme_loc_id := regmatches(ihme_loc_id, regexpr('^.*(?=&&)', ihme_loc_id, perl=T))]

  ccmp_only[, sex := regmatches(iso3_sex_source, regexpr('(?<=&&).*(?=&&)', iso3_sex_source, perl=T))]
  ccmp_only[, source_type := regmatches(iso3_sex_source, regexpr('(?<=&&).{2,6}$', iso3_sex_source, perl=T))]
  stopifnot(all(sort(unique(ccmp_only$sex)) == c('female', 'male')))

  # Merge deaths data onto ccmp_only
  ccmp_only <- merge(ccmp_only, deaths_combined, by.x=c('ihme_loc_id', 'sex', 'year1', 'year2'),
                     by.y = c('ihme_loc_id', 'sex', 'popyear1', 'popyear2'), all.x=T)
  
  # Adjust deaths by completness
  ccmp_only[, deaths := deaths/comp]
  ccmp_only <- ccmp_only[source_type %in% c("VR", "DSP", "SRS", "MCCD")]

  sex_spec <- ccmp_only[ihme_loc_id %in% c("SAU", "MAR")]
  ccmp_only <- ccmp_only[!(ihme_loc_id %in% c("SAU", "MAR"))]
  
  # Get weighted average between sexes
  ccmp_only <- ccmp_only[, weighted.mean(comp, deaths, na.rm=T), by=c('ihme_loc_id','year1', 'year2', 'year',
                                                                          'comp_type', 'detailed_comp_type', 'nid',
                                                                          'underlying_nid', 'source_type')]
  setnames(ccmp_only, 'V1', 'comp')
  # Generate iso3sexsource and Id variable for binding
  ccmp_only[, iso3_sex_source := paste0(ihme_loc_id, '&&both&&', source_type)]
  ccmp_only[, 'source_type' := NULL]
  
  # Get rid of unnecessary columns for binding
  sex_spec[, c('sex', 'id', 'source_type', 'deaths') := NULL]
  
  ccmp_only <- rbind(ccmp_only, sex_spec, use.names=T)
  ddm <- rbind(ddm_no_ccmp, ccmp_only, use.names=T, fill=T)
  
  # regenerate id variable
  ddm[, id := NULL]
  ddm[, id := .GRP, by=c('ihme_loc_id', 'year', 'iso3_sex_source')]
  ddm[comp_type == "u5", id := NA]
  
## Put comp in log space
  ddm$comp <- log(ddm$comp, 10)

## Split out adult and child comp
  child <- ddm[ddm$comp_type == "u5", c("ihme_loc_id", "iso3_sex_source", "year", "comp")]
  adult <- ddm[ddm$comp_type != "u5", c("ihme_loc_id", "iso3_sex_source", "year", "comp", "detailed_comp_type", "id",'nid')]
  setnames(adult, "detailed_comp_type", "comp_type")

## Use loess to generate a full time series of child comp
  years <- 1950:end_year
  child_loess <- ddply(child, c("ihme_loc_id", "iso3_sex_source"),
                       function(x) {
                         cat(paste(x$iso3_sex_source[1], "\n")); flush.console()
                         if (nrow(x) <= 3) {
                           p <- rep(mean(x$comp), length(years))
                         } else {
                           gaps <- (x$year[-1] - x$year[-length(x$year)])
                           #alpha = max(gaps)*5
                           if(grepl("PAN|MNG", x$iso3_sex_source[1])) {
                              alpha <- 1
                            } else if (grepl("PHL", x$iso3_sex_source[1])) {
                              alpha <- 0.8
                            } else if(grepl("UZB|TUR", x$iso3_sex_source[1])) {
                              alpha <- 0.7
                            } else {
                              alpha <- 5
                            }
                           m <- loess(comp ~ year, data=x, control=loess.control(surface="direct"), span=alpha, model=T)
                           p <- rep(NA, length(years))
                           p[years >= min(x$year) & years <= max(x$year)] <- predict(m, newdata=data.frame(year=min(x$year):max(x$year)))
                           p[years < min(x$year)] <- p[years==min(x$year)]
                           p[years > max(x$year)] <- p[years==max(x$year)]
                           for (ii in which(gaps>=5)) {
                             int <- (years >= x$year[ii] & years <= x$year[ii+1])
                             p[int] <- approx(c(x$year[ii], x$year[ii+1]), c(p[years==x$year[ii]],p[years==x$year[ii+1]]), n=sum(int))$y
                           }
                         }
                         data.frame(year=years, u5_comp_pred=p)
                       })
  child_loess <- merge(child_loess, child, all=T)
  names(child_loess)[names(child_loess)=="comp"] <- "u5_comp"
  write.csv(child_loess,FILEPATH,row.names=F)

## Make adult a square data frame
  square <- data.frame(iso3_sex_source = rep(unique(ddm$iso3_sex_source), length(1950:end_year)),
                       year = rep(1950:end_year, each=length(unique(ddm$iso3_sex_source))), stringsAsFactors=F)
  # Pull out ihme_loc_id from iso3_sex_source
  locnames <- data.frame(str_split_fixed(square$iso3_sex_source,"&&",3), stringsAsFactors =FALSE)
  square$ihme_loc_id <- locnames$X1

  square <- merge(square, codes, all.x=T)
  adult <- merge(square, adult, all=T)

## Merge child comp onto the adult comp
  adult <- merge(adult, child_loess, by=c("iso3_sex_source", "ihme_loc_id", "year"), all=T)
  adult$u5_comp_pred[is.na(adult$u5_comp_pred)] <- 0

  adult <- adult[order(adult$iso3_sex_source, adult$year, adult$comp_type),]


######################
## First stage
######################
    
    ## Determine data to exclude (we want to keep 3 ddm methods)
    
    # toggles for 2 options:
    high_low <- T
    extreme2 <- F
    
    select_ddm_methods <- function(x){

      x <- setDT(x)
      
      source <- strsplit(unique(x$iso3_sex_source), "&&")[[1]][3]
      n_not_na <- nrow(x[!is.na(x$comp),])
      x$exclude <- 0
      
      if(n_not_na<=3){

        x$exclude[is.na(x$comp)] <- 1
        ccmp_present <- nrow(x[!is.na(x$comp) & x$comp_type %like% 'ccmp',])

          mean_comp <- mean(x$comp, na.rm=T)
          x$diff_from_mean <- abs(x$comp - mean_comp)
          x$exclude[which.max(x$diff_from_mean)] <- 1
          x$diff_from_mean <- NULL


      } else {
        
        if(high_low==T){
          x$exclude[is.na(x$comp)] <- 1
          x$exclude[x$comp==max(x[x$exclude==0]$comp)] <- 1
          x$exclude[x$comp==min(x[x$exclude==0]$comp)] <- 1
        }
        
        if(extreme2==T){
          #if more than 3, exclude 1 farthest from mean, repeat
          mean_comp1 <- mean(x$comp, na.rm=T)
          x$diff_from_mean1 <- abs(x$comp - mean_comp1)
          x$exclude[which.max(x$diff_from_mean1)] <- 1
          #repeat
          mean_comp2 <- mean(x[x$exclude==0,"comp"], na.rm=T)
          x$diff_from_mean2 <- abs(x$comp - mean_comp2)
          x$diff_from_mean2[x$exclude==1] <- 0
          x$exclude[which.max(x$diff_from_mean2)] <- 1
          #remove unneeded columns
          x$diff_from_mean1 <- NULL
          x$diff_from_mean2 <- NULL
        }
        
      }
      return(x)
    }
    
      adult$id[is.na(adult$id)] <- 999
      adult <- ddply(adult, c("iso3_sex_source", "year", "nid"), select_ddm_methods)
      
    ## Outlier ggb, seg, ggbseg if we don't have ccmp, but we do have ccmp for a different year
      outlier_based_on_ccmp_availability <- function(x){
        x <- setDT(x)
        ccmp_present <- nrow(x[!is.na(x$comp) & tolower(x$comp_type) %like% 'ccmp',])
        if(ccmp_present > 0){
         for(yy in unique(x$year)){
           if(nrow(x[!is.na(x$comp) & tolower(x$comp_type) %like% 'ccmp' & year == yy,]) == 0){
             x[year == yy, exclude := 1]
           }
         } 
        }
        return(x)
      }
      adult <- ddply(adult, "iso3_sex_source", outlier_based_on_ccmp_availability)    
      adult <- setDT(adult)
                   
    ## Outliers
      adult$exclude[adult$ihme_loc_id == "TON" & adult$year == 1991 & adult$comp_type == "seg"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43877&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43883&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43886&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43887&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43898&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43901&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43906&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "IND_43891&&both&&SRS" & adult$year == 2006 & adult$comp_type == "ggb"] <- 1

      keep_ccmp_only <- c("IND_43904&&both&&SRS", "IND_43911&&both&&SRS", "IND_4844&&both&&SRS", "IND_4852&&both&&SRS",
                          "IND_4855&&both&&SRS", "IND_4856&&both&&SRS", "IND_4860&&both&&SRS", "IND_4867&&both&&SRS",
                          "IND_4868&&both&&SRS", "IND_4875&&both&&SRS", "IND&&both&&SRS", "IND_43922&&both&&SRS",
                          "IND_43934&&both&&SRS")
      adult$exclude[adult$iso3_sex_source %in% keep_ccmp_only & adult$year == 2006 & adult$comp_type %in% c("ggb","seg","ggbseg")] <- 1
      adult$exclude[adult$iso3_sex_source %in% keep_ccmp_only & adult$year == 2006 & tolower(adult$comp_type) %like% "ccmp"] <- 0
      
      adult$exclude[adult$iso3_sex_source =="IND&&both&&SRS" & adult$year == 1976 & adult$comp_type %in% c("ggb","seg","ggbseg")] <- 1
      adult$exclude[adult$iso3_sex_source =="IND&&both&&SRS" & adult$year == 1976 & tolower(adult$comp_type) %like% "ccmp"] <- 0
      
      keep_ccmp_only <- c("TWN&&both&&VR", "MKD&&both&&VR", "MNE&&both&&VR", "SRB&&both&&VR", "BGR&&both&&VR")
      adult$exclude[adult$iso3_sex_source %in% keep_ccmp_only & adult$comp_type %in% c("ggb","seg","ggbseg")] <- 1
      adult$exclude[adult$iso3_sex_source %in% keep_ccmp_only & tolower(adult$comp_type) %like% "ccmp"] <- 0
      
      adult$exclude[adult$iso3_sex_source == "SVK&&both&&VR" & adult$year == 1970] <- 1
      adult$exclude[adult$iso3_sex_source == "CYP&&both&&VR" & adult$comp_type == "ggb"] <- 1
      adult$exclude[adult$iso3_sex_source == "EGY&&both&&VR" & adult$comp_type == "ggbseg" & adult$year >=1975 & adult$year <=2005] <- 1
      adult$exclude[adult$iso3_sex_source == "MDV&&both&&VR" & adult$comp_type %in% c("ggb", "seg") & adult$year == 2010] <- 1
      adult$exclude[adult$iso3_sex_source == "TWN&&both&&VR" & adult$year == 1970] <- 1
      
    ## Combine the South and East Asia super-regions
      adult$super_region_name[adult$super_region_name %in% c("South Asia", "East Asia")] <- "South & East Asia/Pacific"
      adult$super_region_name[adult$ihme_loc_id %in% c("MUS","SYC")] <- "South & East Asia/Pacific"
    
    ## Split the data by type (VR/SRS/DSP are analyzed separately)
      data <-  NULL
      data[[1]] <- adult[grepl("VR|SRS|DSP|MCCD|CR", adult$iso3_sex_source),]
      data[[2]] <- adult[!grepl("VR|SRS|DSP|MCCD|CR", adult$iso3_sex_source),]

      data[[1]]$source_type <- str_split_fixed(data[[1]]$iso3_sex_source, "&&", 3)[,3]

      data[[1]]$source_type[grepl("VR", data[[1]]$source_type)] <- "VR"
      data[[1]]$source_type[grepl("SRS", data[[1]]$source_type)] <- "SRS"
      data[[1]]$source_type[grepl("DSP", data[[1]]$source_type)] <- "DSP"
     
     # get standard locations for regression
     locs_for_regression <- get_locations()
     national_parents <- locs_for_regression[level==4, parent_id]
     standard_locs <- unique(get_locations()$location_id)
     locs_for_regression[, standard := as.numeric(location_id %in% c(standard_locs, national_parents, 44533))]
     standard_locs <- unique(locs_for_regression[standard==1]$ihme_loc_id)
     
     lm_input <- data[[1]]
     lm_input <- lm_input[lm_input$exclude==0 & lm_input$ihme_loc_id %in% standard_locs,] 

       m <- lm(comp ~ u5_comp_pred, data=lm_input)
       coefs <- NULL
       coefs[1] <- m$coefficients[1]
       coefs[2] <- m$coefficients[2]
       data[[1]]$pred.1 <- coefs[1] + coefs[2]*data[[1]]$u5_comp_pred
      
      data[[1]]$resid <- data[[1]]$comp - data[[1]]$pred.1
      data[[1]]$resid[data[[1]]$exclude == 1] <- NA

      data[[2]]$pred.1 <- 0
      data[[2]]$resid <- data[[2]]$comp - data[[2]]$pred.1
      data[[2]]$resid[data[[2]]$exclude == 1] <- NA

    ## Save the first stage model
      r_squared <- NULL
      r_squared['normal'] <- summary(m)$r.squared
      r_squared['adjusted'] <- summary(m)$adj.r.squared

    ######################
    ## Second stage
    ######################
      

    ## Apply space-time to the residuals (separately for VR/SRS and other)
    hyper_param <- fread(FILEPATH)
    lambda_list <- hyper_param$lambda
    names(lambda_list) <- hyper_param$iso3_sex_source
      for (ii in 1:2) {
        temp <- resid_space_time(data[[ii]], lambda=lambda_list, zeta=0.95, max_year = end_year)
        data[[ii]] <- merge(data[[ii]], temp)
        data[[ii]]$pred.2.raw <- data[[ii]]$pred.1 + data[[ii]]$pred.resid
        data[[ii]] <- loess_resid(data[[ii]])
      }

    ### Save dataset
      for (ii in 1:2) {
        data[[ii]] <- data[[ii]][order(data[[ii]]$region_name, data[[ii]]$iso3_sex_source, data[[ii]]$year, data[[ii]]$comp_type),]
      }

######################
## Uncertainty
######################

## Calculate raw standard deviation
  for (ii in 1:2) {
    data[[ii]]$region_name[grepl("Sub-Saharan", data[[ii]]$region_name)] <- "Sub-Saharan Africa"
    if (ii == 2) data[[ii]]$region_name[grepl("Latin America", data[[ii]]$region_name)] <- "Latin America"
    data[[ii]]$resid <- data[[ii]]$comp - data[[ii]]$pred.2.final
    data[[ii]]$resid[data[[ii]]$exclude==1] <- NA

    sd <- tapply(data[[ii]]$resid, data[[ii]]$ihme_loc_id, function(x) 1.4826*median(abs(x-median(x, na.rm=T)), na.rm=T))

    ## calculate region-level variance to apply to locations that don't have any comp/resid (DEU LBN etc.)
    reg_sd <- tapply(data[[ii]]$resid, data[[ii]]$region_name, function(x) 1.4826*median(abs(x-median(x, na.rm=T)), na.rm=T))
    use_reg <- names(sd[is.na(sd) | sd == 0])

    for (rr in names(sd)) {
      if(!(rr %in% use_reg)) {
        data[[ii]]$sd[data[[ii]]$ihme_loc_id == rr] <- sd[rr]
      } else {
        reg <- unique(data[[ii]]$region_name[data[[ii]]$ihme_loc_id == rr])
        data[[ii]]$sd[data[[ii]]$ihme_loc_id == rr] <- reg_sd[reg]
      }
    }
  }

## Simulate from the original mean and variance; truncate these distributions and recalculate the mean, variance, and confidence bounds
  set.seed(4463)
  nsims <- 10000
  for (ii in 1:2) {
    sims <- matrix(NA, nrow=nrow(data[[ii]]), ncol=nsims)
    for (jj in 1:nrow(sims)) sims[jj,] <- rnorm(n=nsims, mean=data[[ii]]$pred.2.final[jj], sd=data[[ii]]$sd[jj])
    ## calculate non-truncated, anti-logged mean and confidence intervals
    data[[ii]]$pred <- apply(sims, 1, function(x) mean(10^(x)))
    data[[ii]]$lower <- apply(sims, 1, function(x) quantile(10^(x), 0.025, na.rm = TRUE))
    data[[ii]]$upper <- apply(sims, 1, function(x) quantile(10^(x), 0.975, na.rm = TRUE))

    ## calculate truncated, anti-logged mean, sd, and confidence intervals
    for (jj in 1:nrow(sims)) {
      sims[jj,][sims[jj,] > 0] <- 0
      sims[jj,][sims[jj,] > log(0.95, 10)] <- 0
    }
    data[[ii]]$trunc_sd <- apply(sims, 1, function(x) sd(x))
    data[[ii]]$trunc_pred <- apply(sims, 1, function(x) mean(10^(x)))
    data[[ii]]$trunc_lower <- apply(sims, 1, function(x) quantile(10^(x), 0.025, na.rm = TRUE))
    data[[ii]]$trunc_upper <- apply(sims, 1, function(x) quantile(10^(x), 0.975, na.rm = TRUE))

  }

### Remove from log space
  data[[1]]$source_type <- NULL
  all <- rbind(data[[1]], data[[2]])

  all <- all[,c("region_name", "location_name", "location_id", "ihme_loc_id", "iso3_sex_source", "year", "u5_comp", "u5_comp_pred", "comp_type", "comp", "pred.1", "pred.2.final",
                "sd", "pred", "lower", "upper", "exclude", "trunc_sd", "trunc_pred", "trunc_lower", "trunc_upper")]
  names(all)[names(all)=="pred.1"] <- "pred1"
  names(all)[names(all)=="pred.2.final"] <- "pred2"

  all <- as.data.table(all)
  vars <- c("u5_comp", "u5_comp_pred", "comp", "pred1", "pred2")
  all[, (vars) := lapply(.SD, function(x) {10^x}), .SDcols=vars]

### Make the regions proper again
  all <- all[, c("region_name", "location_name", "location_id"):=NULL]
  all <- merge(all, codes, all.x=T)

## Calculate final completeness values to be used in 45q15
  all_final_comp <- copy(data.table(all))
  split <- strsplit(all$iso3_sex_source, "&&")
  iso3.sex.source <- do.call(rbind, split)
  all_final_comp[, iso3 := iso3.sex.source[,1]]
  all_final_comp[, sex := iso3.sex.source[,2]]
  all_final_comp[, source := iso3.sex.source[,3]]

# Use untruncated completeness values for non VR, SRS, DSP sources
# Use truncated completeness values for VR, SRS, DSP sources
  all_final_comp[, final_comp := pred]
  all_final_comp[grepl("VR|DSP|SRS|MCCD", source), final_comp := trunc_pred]

# Calculate usable completeness estimate -- mean of sims for census/survey, mean of trunc sims for VR/SRS/DSP
  all_final_comp <- all_final_comp[, list(ihme_loc_id, sex, source, year, final_comp, sd, u5_comp_pred, u5_comp)]
  all_final_comp <- all_final_comp[, lapply(.SD, mean, na.rm = T), .SDcols = c('final_comp', 'sd', 'u5_comp_pred', 'u5_comp'), by = c('ihme_loc_id', 'source', 'year', 'sex')]

# find sex-specific data
  all_final_comp[, id := seq(.N)]
  all_final_comp[, grp:= .GRP, by=c('ihme_loc_id','source','year')]
  all_final_comp[, nsexes:=.N, by=grp]
  all_final_comp_sex_specific <- all_final_comp[nsexes>1]
  all_final_comp <- all_final_comp[nsexes==1]
  all_final_comp_sex_specific[,c('grp','nsexes'):=NULL]
  all_final_comp[,c('grp','nsexes'):=NULL]
  
# Duplicate for both sexes for rows
  all_final_comp_male <- all_final_comp[sex == "both"]
  all_final_comp_female <- all_final_comp[sex == "both"]
  all_final_comp_male[, sex := "male"]
  all_final_comp_female[, sex := "female"]
  all_final_comp <- rbind(all_final_comp, all_final_comp_male,
                          all_final_comp_female, all_final_comp_sex_specific)

# We need to duplicate SSPC-DC so that it matches with both SSPC and DC
  all_final_comp_DC <- all_final_comp[source == "SSPC-DC"]
  all_final_comp_DC[source == "SSPC-DC", source := "DC"]
  all_final_comp_SSPC <- all_final_comp[source == "SSPC-DC"]
  all_final_comp_SSPC[source == "SSPC-DC", source := "SSPC"]
  all_final_comp <- rbind(all_final_comp, all_final_comp_DC, all_final_comp_SSPC)


# Recombine sources
  all_final_comp[grepl("VR", source) & ihme_loc_id == "KOR", source := "VR"]
  all_final_comp[grepl("DSP", source) & grepl("CHN_", ihme_loc_id), source := "DSP"]
  all_final_comp[grepl("SRS", source) & grepl("IND", ihme_loc_id), source := "SRS"]

  all_final_comp[!is.na(final_comp), adjust := 1]
  all_final_comp[final_comp >= 0.95 & !is.na(final_comp) & grepl("VR|DSP|SRS|MCCD|CR", source), final_comp := 1]
  all_final_comp[final_comp >=1 & !is.na(final_comp) & grepl("VR|DSP|SRS|MCCD|CR", source), adjust := 3]

# Create minimum completeness value for a given location-source to mark locations in which it is complete for all_final_comp years
  all_final_comp[, min_comp := min(final_comp), by = c('ihme_loc_id', 'source')]
  all_final_comp[min_comp >= 1 & !is.na(min_comp) & grepl("VR|DSP|SRS|MCCD|CR", source), adjust_full_time_series := 3]

# Scale values from 0.90 to 0.95 because we are treating 0.95 as 100% complete
  all_final_comp[, resid := final_comp - 0.90]
  all_final_comp[final_comp >= 0.90 & final_comp < 0.95 & grepl("VR|DSP|SRS|MCCD|CR", source), final_comp := 0.90 + 2*resid]

## Use national-level completeness for subnationals if the national if fully complete for all_final_comp years, except for Brazil.
  national_complete <- all_final_comp[adjust_full_time_series == 3 & !(ihme_loc_id %in% c("BRA", "IRN")), list(ihme_loc_id, source, adjust_full_time_series, sd)]
  national_complete <- unique(national_complete)
  setnames(national_complete, c("ihme_loc_id", "sd", "adjust_full_time_series"), c("parent_loc_id", "parent_sd", "parent_adjust"))
  national_complete[parent_loc_id == "CHN_44533", parent_loc_id := "CHN"]

## Merge locations where nationals are fully complete in all_final_comp years
  all_final_comp[grepl("_", ihme_loc_id), parent_loc_id := substr(ihme_loc_id, 1,3)]
  all_final_comp <- merge(all_final_comp, national_complete, by = c('parent_loc_id', 'source'), all.x = T)
  all_final_comp[parent_adjust == 3 & !is.na(final_comp), final_comp := 1]
  all_final_comp[parent_adjust == 3 & !is.na(final_comp), sd := parent_sd]
  all_final_comp[parent_adjust == 3 & !is.na(final_comp), adjust := 3]
  all_final_comp[, c('parent_adjust', 'parent_sd', 'parent_loc_id', 'min_comp', 'adjust_full_time_series', 'resid') := NULL]

# Use IDN SUSENAS national completeness values for its subnationals
  idn_locs <- unique(all_final_comp[grepl("IDN_", ihme_loc_id) & ihme_loc_id != "IDN", ihme_loc_id])

  idn_susenas_subnational <- rbindlist(lapply(idn_locs, function(x) {
    tmp_idn <- all_final_comp[ihme_loc_id == "IDN" & source == "SUSENAS"]
    tmp_idn[, ihme_loc_id := x]

    return(tmp_idn)
  }))

  all_final_comp <- all_final_comp[!(grepl("IDN_", ihme_loc_id) & source == "SUSENAS")]
  all_final_comp <- setDT(rbind(all_final_comp, idn_susenas_subnational))
  
###################
## Save and plot
###################

## Save scaled final comp for c10
  write.dta(all_final_comp, file = FILEPATH)

## save normal file
  write.dta(all, file = FILEPATH)
