## Reads in prepped .rds object and subs in indicated GBD parameters
## Output object is read to run through fitmod()
read_spec_object <- function(loc, j, start.year = 1970, stop.year, trans.params.sub = TRUE, 
                             pop.sub = TRUE,  prev.sub = TRUE, art.sub = TRUE, sexincrr.sub = TRUE, 
                             popadjust = TRUE, age.prev = FALSE, paediatric, anc.rt = FALSE, geoadjust=TRUE,
                             anc.prior.sub = TRUE, lbd.anc = FALSE, use_2019 = TRUE,gbdyear =gbdyear, run.name = run.name,
                             test.sub_prev_granular = NULL){


  #Do this for now as something is weird with the new PJNZ files - don't need subpop anyway
  #Eventually these hsould all be regenerated with subpopulations

  if(file.exists(paste0('FILEPATH/2023/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/2023/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/2022/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/2022/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/2021/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/2021/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/2020/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/2020/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/2019_extended/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/2019_extended/', loc, '.rds'))
    
  } else if (file.exists(paste0('FILEPATH/2019/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/2019/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/2018/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/2018/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/2015/', loc, '.rds'))){
    dt <- readRDS(paste0('FILEPATH/2015/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/', loc, '.rds'))
    
  } else if(file.exists(paste0('FILEPATH/', loc, '.rds'))) {
    dt <- readRDS(paste0('FILEPATH/', loc, '.rds'))
  } 
  
  
  if(lbd.anc){
      replace <- as.data.table(readRDS(paste0('FILEPATH/', loc, '.rds')))

    if(grepl("KEN",loc)){
      replace <- replace[which(subpop == attr(dt,"eppd")$ancsitedat$subpop[1])]
    }


    if(grepl("SOM",loc)){
      replace <- replace[subpop %in% "Remaining females" & type=="ancss"]
    }


    if(!geoadjust){
      replace[,offset := NULL]
    }
    replace[,'age' := as.numeric(replace[,age])]
    replace[,'agspan' := as.numeric(replace[,agspan])]
    replace[,'site' := as.character(replace[,site])]
    replace[,'high_risk' := FALSE]
    # replace <- replace[type == 'ancss',]
    if(!geoadjust){
      replace[,adm0_mean:=NULL]
      replace[,adm0_lower:=NULL]
      replace[,adm0_upper:=NULL]
      replace[,site_pred:=NULL]

    }

    attr(dt, 'eppd')$ancsitedat <- replace


  }
  

 ## Substitute IHME data
  ## Population parameters
  if(pop.sub){
    ## TODO fix this workflow
    print('Substituting demographic parameters')
    if(grepl('IND', loc)){
      demp <- create_spectrum_demog_param(loc, start.year, stop.year)
      projp <- create_hivproj_param(loc, start.year, stop.year)
      attr(dt, 'specfp') <- create_spectrum_fixpar(projp, demp, proj_start = start.year, proj_end = stop.year, popadjust=popadjust)
      attr(dt, 'specfp')$ss$time_epi_start <- 1985
    }
    
      specfp <- sub.pop.params.specfp(attr(dt, 'specfp'), loc, j)
      specfp <- update_spectrum_fixpar(specfp, proj_start = start.year, proj_end = stop.year,time_epi_start = specfp$ss$time_epi_start, popadjust=popadjust)
      attr(dt, 'specfp') <- specfp
    }
  ## Pediatric inputs
  if(paediatric){
    print('Preparing paediatric module inputs')
    dt <- sub.paeds(dt, loc, j, start.year = 1970, stop.year = stop.year, gbdyear = gbdyear, run.name = run.name)
  }
  ## Transition parameters
  if(trans.params.sub) {
    print('Substituting transition parameters')
    #dt <- sub.off.art(dt, loc, j)
    dt <- sub.on.art(dt, loc, j)
    dt <- sub.cd4.prog(dt, loc, j)
  }
  ## Extrapolated ART
  if(art.sub){
    print('Substituting ART data')
    if(loc == 'ZAF_488'){
      dt <- sub.art(dt,loc, F)
    }else{
      dt <- sub.art(dt,loc, T)
    }
  }
  ## Group 1 inputs
  loc.table <- fread("FILEPATH/hiv_model_strategy_2024.csv")
  if(grepl('1', loc.table[ihme_loc_id == loc, group]) | loc %in% c("STP","COM","MRT","MAR")){
    ## Prevalence surveys
    if(prev.sub) {
      print("Substituting prevalence surveys")
      
      ##adding in age.prev arg
      if(age.prev){
        dt <- sub.prev.granular(dt, loc, test.sub_prev_granular)
        attr(dt, 'specfp')$fitincrr <- 'regincrr'
      } else{
        dt <- sub.prev(loc, dt)	
        attr(dt, 'specfp')$fitincrr <- FALSE
      }
    }
    
    ##adding in another placeholder arg
    ## ANC data

    if(geoadjust){
      
      print("Merging ANC bias offsets")
      # if(geoadj_test){
      #   dt <- geo_adj_old(loc, dt, j, uncertainty=TRUE)
      #   
      # }else{
        dt <- geo_adj(loc, dt, j, uncertainty=TRUE)
        
      # }

    } 
    
    if(sexincrr.sub){
      print('Substituting sex incrr')
      dt <- sub.sexincrr(dt, loc, j)
    }
    
    if(anc.prior.sub){
      print("Substituting ANC bias prior")
      # dt <- gbdeppaiml::sub.anc.prior(dt,loc)
      dt <- sub.anc.prior(dt,loc)
    }
    
    if(anc.prior.sub){
      print("Substituting ANC bias prior")
      #dt <- gbdeppaiml::sub.anc.prior(dt,loc)
      dt <- sub.anc.prior(dt,loc)
    }
    


    ## Subsetting KEN counties from province, updated 5/5/2020

    if(grepl('KEN', loc)){
      ken.anc.path <- paste0('FILEPATH/kenya_anc_map.csv')
      ken.anc <- fread(ken.anc.path)
      if(loc %in% ken.anc$ihme_loc_id){
        county.sites <- ken.anc[ihme_loc_id == loc, site]
        prov.sites <- unique(attr(dt, "eppd")$ancsitedat$site)


        attr(dt, "eppd")$anc.used[] <- FALSE

        temp1 <- attr(dt, "eppd")$anc.prev[]
        temp1 <- temp1[0,]

        temp2 <- attr(dt, "eppd")$anc.n[]
        temp2 <- temp2[0,]

        for(site in unique(county.sites)){
          attr(dt, "eppd")$anc.used[grepl(site,names(attr(dt, "eppd")$anc.used))] <- TRUE
          keep_index <- which(grepl(site,rownames(attr(dt, "eppd")$anc.prev)))
          temp1 <- rbind(temp1,attr(dt, "eppd")$anc.prev[keep_index,,drop =FALSE])
          temp2 <- rbind(temp2,attr(dt, "eppd")$anc.n[keep_index,,drop =FALSE])

        }

        attr(dt, "eppd")$anc.prev <- temp1
        attr(dt, "eppd")$anc.n <- temp2
        attr(dt, "eppd")$anc.used <- attr(dt, "eppd")$anc.used[attr(dt, "eppd")$anc.used]
        attr(dt, 'eppd')$ancsitedat$used[!(attr(dt, 'eppd')$ancsitedat$site %in% county.sites | grepl(loc.table[ihme_loc_id == loc, location_name], attr(dt, 'eppd')$ancsitedat$site))] <- FALSE
        attr(dt, 'eppd')$ancsitedat <- attr(dt, 'eppd')$ancsitedat[which(attr(dt, 'eppd')$ancsitedat$used==TRUE),]
     }
    }
    
    ## Subsetting NGA counties from province, updated 5/5/2020
    if(grepl('NGA', loc)){
      NGA.anc.path <- paste0('FILEPATH/NGA_anc_map.csv')
      NGA.anc <- fread(NGA.anc.path)
      if(loc %in% NGA.anc$ihme_loc_id){
        county.sites <- NGA.anc[ihme_loc_id == loc, site]
        prov.sites <- unique(attr(dt, "eppd")$ancsitedat$site)
        
        
        attr(dt, "eppd")$anc.used[] <- FALSE
        
        temp1 <- attr(dt, "eppd")$anc.prev[]
        temp1 <- temp1[0,]
        
        temp2 <- attr(dt, "eppd")$anc.n[]
        temp2 <- temp2[0,]
        
        for(site in unique(county.sites)){
          attr(dt, "eppd")$anc.used[grepl(site,names(attr(dt, "eppd")$anc.used))] <- TRUE
          keep_index <- which(grepl(site,rownames(attr(dt, "eppd")$anc.prev)))
          temp1 <- rbind(temp1,attr(dt, "eppd")$anc.prev[keep_index,,drop =FALSE])
          temp2 <- rbind(temp2,attr(dt, "eppd")$anc.n[keep_index,,drop =FALSE])
          
        }
        
        attr(dt, "eppd")$anc.prev <- temp1
        attr(dt, "eppd")$anc.n <- temp2
        attr(dt, "eppd")$anc.used <- attr(dt, "eppd")$anc.used[attr(dt, "eppd")$anc.used]
        attr(dt, 'eppd')$ancsitedat$used[!(attr(dt, 'eppd')$ancsitedat$site %in% county.sites | grepl(loc.table[ihme_loc_id == loc, location_name], attr(dt, 'eppd')$ancsitedat$site))] <- FALSE
        attr(dt, 'eppd')$ancsitedat <- attr(dt, 'eppd')$ancsitedat[which(attr(dt, 'eppd')$ancsitedat$used==TRUE),]
      }
    }
    # 
    if(!anc.rt){
      attr(dt, 'eppd')$ancrtsite.prev <- NULL
      attr(dt, 'eppd')$ancrtsite.n <- NULL
      attr(dt, 'eppd')$ancrtcens <- NULL
    }

    attr(dt, 'specfp')$prior_args <- list(logiota.unif.prior = c(log(1e-14), log(0.000025)))
    attr(dt, 'specfp')$group <- '1'
    
  }else{
    ## Group 2 inputs
    print('Appending vital registration death data')
    dt <- append.vr(dt, loc, run.name)
    attr(dt, 'specfp')$group <- '2'
    attr(dt, 'specfp')$mortadjust = 'simple'
    print('Appending case notification data')
    dt <- append.diagn(dt, loc, run.name)
    attr(dt, 'specfp')$incid_func <- NULL
    attr(dt, 'specfp')$incidinput <- NULL
    attr(dt, 'specfp')$eppmod <- 'rlogistic'
    attr(dt, 'specfp')$ss$time_epi_start <- 1970
    
    print('Appending CIBA age/sex incrr priors')
    dt <- append.ciba.incrr(dt, loc, run.name)
    
  }
  ## Append fertility rate ratios for countries in SSA
  if(loc.table[ihme_loc_id == loc, super_region_name] == 'Sub-Saharan Africa'){
    print('Appending FRR')
    dt <- add_frr_noage_fp(dt)
  }

  
    return(dt)
    
}

## A modified version of create_spectrum_fixpar in spectrum.R in EPPASM, which is used to create a specfp object from projp and demp
## In order to bypass reading in pjnz files, this function can be used to format the specfp object so it's ready to run through simmod
## NOTE that when major changes are made to create_spectrum_fixpar(), they may need to be added to this function
update_spectrum_fixpar <- function(specfp, hiv_steps_per_year = 10L, proj_start = start.year, proj_end = stop.year,time_epi_start = 1970,
                                   AGE_START = 15L, popadjust=TRUE, artelig200adj=TRUE, who34percelig=0){
  
  ## ########################## ##
  ##  Define model state space  ##
  ## ########################## ##
  
  ## Parameters defining the model projection period and state-space
  ss <- list(proj_start = proj_start,
             popadjust = popadjust,
             PROJ_YEARS = as.integer(proj_end - proj_start + 1L),
             AGE_START  = as.integer(AGE_START),
             hiv_steps_per_year = as.integer(hiv_steps_per_year),
             time_epi_start=time_epi_start)
  
  ## populuation projection state-space
  ss$NG <- 2
  ss$pDS <- 2               # Disease stratification for population projection (HIV-, and HIV+)
  
  ## macros
  ss$m.idx <- 1
  ss$f.idx <- 2
  
  ss$hivn.idx <- 1
  ss$hivp.idx <- 2
  
  ss$pAG <- 81 - AGE_START
  ss$ag.rate <- 1
  ss$p.fert.idx <- 16:50 - AGE_START
  ss$p.age15to49.idx <- 16:50 - AGE_START
  ss$p.age15plus.idx <- (16-AGE_START):ss$pAG
  
  
  ## HIV model state-space
  ss$h.ag.span <- as.integer(c(2,3, rep(5, 6), 31))   # Number of population age groups spanned by each HIV age group [sum(h.ag.span) = pAG]
  ss$hAG <- length(ss$h.ag.span)          # Number of age groups
  ss$hDS <- 7                             # Number of CD4 stages (Disease Stages)
  ss$hTS <- 3                             # number of treatment stages (including untreated)
  
  ss$ag.idx <- rep(1:ss$hAG, ss$h.ag.span)
  ss$agfirst.idx <- which(!duplicated(ss$ag.idx))
  ss$aglast.idx <- which(!duplicated(ss$ag.idx, fromLast=TRUE))
  
  
  ss$h.fert.idx <- which((AGE_START-1 + cumsum(ss$h.ag.span)) %in% 15:49)
  ss$h.age15to49.idx <- which((AGE_START-1 + cumsum(ss$h.ag.span)) %in% 15:49)
  ss$h.age15plus.idx <- which((AGE_START-1 + cumsum(ss$h.ag.span)) >= 15)
  
  ## Paediatric state space
  if(paediatric){
    ss$pAGu5 <- 5 ## under 5 ages
    ss$hDSu5 <- 7 ## cd4 percent
    ss$hMT <- 4 ## perinatal, bf0, bf6, bf12
    ss$pAGu15 <- 10 ## 5-15 ages
    ss$hDSu15 <- 6 ##under 15 cd4 count categories
    ss$u5.elig.groups <- list('30' = 1, '25' = 2, '20' = 3, '15' = 4, '10' = 5, '5' = 6, '0' = 7)
    ss$u15.elig.groups <- list('1000' = 1, '750' = 2, '500' = 3, '350' = 4, '200' = 5, '0' = 6)  
    ss$prenat.opt <- c('tripleARTdurPreg', 'tripleARTbefPreg', 'singleDoseNevir', 'prenat_optionB', 'prenat_optionA', 'dualARV')
    
  }
  

  
  
  invisible(list2env(ss, environment())) # put ss variables in environment for convenience
  
  specfp$ss <- ss
  specfp$SIM_YEARS <- ss$PROJ_YEARS
  specfp$proj.steps <- proj_start + 0.5 + 0:(ss$hiv_steps_per_year * (specfp$SIM_YEARS-1)) / ss$hiv_steps_per_year
  
  ##extend fertility rate ratios if necessary
  if(dim(specfp$frr_cd4)[3] < specfp$SIM_YEARS){
    diff <-  specfp$SIM_YEARS - dim(specfp$frr_cd4)[3]
    table <- matrix(specfp$frr_cd4[,,dim(specfp$frr_cd4)[3]], nrow = 7, ncol = 8)
    while(diff != 0){
      specfp$frr_cd4 <- abind::abind(specfp$frr_cd4, table)
      diff <-  specfp$SIM_YEARS - dim(specfp$frr_cd4)[3]
      
    }

  }
  
  if(dim(specfp$frr_art)[4] < specfp$SIM_YEARS){
    diff <-  specfp$SIM_YEARS - dim(specfp$frr_art)[4]
    table <- specfp$frr_art[,,,dim(specfp$frr_art)[4]]
    while(diff != 0){
      specfp$frr_art <- abind::abind(specfp$frr_art, table)
      diff <-  specfp$SIM_YEARS - dim(specfp$frr_art)[4]
      
    }
    
  }
  
  

  specfp$frr_cd4 = specfp$frr_cd4[,,1:specfp$SIM_YEARS]
  specfp$frr_art = specfp$frr_art[,,,1:specfp$SIM_YEARS]
  ## ######################## ##
  ##  Demographic parameters  ##
  ## ######################## ##
  
  ## Calcuate the net-migration and survival up to AGE_START for each birth cohort.
  ## For cohorts born before projection start, this will be the partial
  ## survival since the projection start to AGE_START, and the corresponding lagged "births"
  ## represent the number in the basepop who will survive to the corresponding age.
  
  cumnetmigr <- array(0, dim=c(NG, PROJ_YEARS))
  cumsurv <- array(1, dim=c(NG, PROJ_YEARS))
  if(AGE_START > 0){
    for(i in 2:PROJ_YEARS){  # start at 2 because year 1 inputs are not used
      for(s in 1:2){
        for(j in max(1, AGE_START-(i-2)):AGE_START){
          ii <- i+j-AGE_START
          cumsurv[s,i] <- cumsurv[s,i] * specfp$Sx[j,s,ii]
          if(j==1)
            cumnetmigr[s,i] <- specfp$netmigr[j,s,ii] * (1+2*specfp$Sx[j,s,ii])/3
          else
            cumnetmigr[s,i] <- cumnetmigr[s,i]*specfp$Sx[j,s,ii] + specfp$netmigr[j,s,ii] * (1+specfp$Sx[j,s,ii])/2
        }
      }
    }
  }
  
  ## initial values for births
  birthslag <- array(0, dim=c(NG, PROJ_YEARS))             # birthslag(i,s) = number of births of sex s, i-AGE_START years ago
  birthslag[,1:AGE_START] <- t(specfp$basepop[AGE_START:1,])  # initial pop values (NOTE REVERSE ORDER). Rest will be completed by fertility during projection
  
  specfp$birthslag <- birthslag
  specfp$cumsurv <- cumsurv
  specfp$cumnetmigr <- cumnetmigr
  
  
  ## set population adjustment
  popadjust <- TRUE
  specfp$popadjust <- popadjust
  if(!length(setdiff(proj_start:proj_end, dimnames(specfp$targetpop)[[3]]))){
    specfp$entrantpop <- specfp$targetpop[1,,as.character(proj_start:proj_end)]
  }
  if(popadjust & is.null(specfp$targetpop))
    stop("targetpop does not span proj_start:proj_end")

  ## ###################### ##
  ##  HIV model parameters  ##
  ## ###################### ##
  
  if(is.null(specfp$relinfectART)){
    specfp$relinfectART <- 0.15
  }
  
  ## Update eligibility threshold from CD4 <200 to <250 to account for additional
  ## proportion eligible with WHO Stage 3/4.
  if(artelig200adj){
    specfp$artcd4elig_idx <- replace(specfp$artcd4elig_idx, specfp$artcd4elig_idx==5L, 4L)
  }
  ## percentage of those with CD4 <350 who are based on WHO Stage III/IV infection
  specfp$who34percelig <- who34percelig
  
  ##TODO could add GBD 2017 age 15 hiv prevalence, add ART coverage at age 15
  
  specfp$netmig_hivprob <- 0.4*0.22
  specfp$netmighivsurv <- 0.25/0.22
  
  attr(dt, 'specfp')$incid_func <- 'id'
  
  ## Circumcision parameters (default no effect)
  specfp$circ_incid_rr <- 0.0  # no reduction
  specfp$circ_prop <- array(0.0, c(ss$pAG, ss$PROJ_YEARS),
                        list(age = ss$AGE_START + 1:ss$pAG - 1L,
                             year = ss$proj_start + 1:ss$PROJ_YEARS - 1L))
  
  return(specfp)
}


  