create_spectrum_fixpar <- function(projp, demp, hiv_steps_per_year = 10L, proj_start = projp$yr_start, proj_end = projp$yr_end,
                                   AGE_START = 15L, relinfectART = projp$relinfectART, time_epi_start = projp$t0,
                                   popadjust=FALSE, targetpop=demp$basepop, artelig200adj=TRUE, who34percelig=0,
                                   frr_art6mos=projp$frr_art6mos, frr_art1yr=projp$frr_art6mos){
  
  ## ########################## ##
  ##  Define model state space  ##
  ## ########################## ##

  ## Parameters defining the model projection period and state-space
  ss <- list(proj_start = proj_start,
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
  ss$pAGu5 <- 5 ## under 5 ages
  ss$hDSu5 <- 7 ## cd4 percent
  ss$hMT <- 4 ## perinatal, bf0, bf6, bf12
  ss$pAGu15 <- 10 ## 5-15 ages
  ss$hDSu15 <- 6 ##under 15 cd4 count categories
  ss$u5.elig.groups <- list('30' = 1, '25' = 2, '20' = 3, '15' = 4, '10' = 5, '5' = 6, '0' = 7)
  ss$u15.elig.groups <- list('1000' = 1, '750' = 2, '500' = 3, '350' = 4, '200' = 5, '0' = 6)  
  ss$prenat.opt <- c('tripleARTdurPreg', 'tripleARTbefPreg', 'singleDoseNevir', 'prenat_optionB', 'prenat_optionA', 'dualARV')
  

  invisible(list2env(ss, environment())) # put ss variables in environment for convenience

  fp <- list(ss=ss)
  fp$SIM_YEARS <- ss$PROJ_YEARS
  fp$proj.steps <- proj_start + 0.5 + 0:(ss$hiv_steps_per_year * (fp$SIM_YEARS-1)) / ss$hiv_steps_per_year
  
  ## ######################## ##
  ##  Demographic parameters  ##
  ## ######################## ##

  ## linearly interpolate basepop if proj_start falls between indices
  bp_years <- as.integer(dimnames(demp$basepop)[[3]])
  bp_aidx <- max(which(proj_start >= bp_years))
  bp_dist <- 1-(proj_start - bp_years[bp_aidx]) / diff(bp_years[bp_aidx+0:1])
  basepop_allage <- rowSums(sweep(demp$basepop[,, bp_aidx+0:1], 3, c(bp_dist, 1-bp_dist), "*"),,2)

  fp$basepop <- basepop_allage[(AGE_START+1):81,]
  fp$Sx <- demp$Sx[(AGE_START+1):81,,as.character(proj_start:proj_end)]

  fp$asfr <- demp$asfr[,as.character(proj_start:proj_end)] # NOTE: assumes 15-49 is within projection age range
  ## Note: Spectrum averages ASFRs from the UPD file over 5-year age groups.
  ##       Prefer to use single-year of age ASFRs as provided. The below line will
  ##       convert to 5-year average ASFRs to exactly match Spectrum.
  ## fp$asfr <- apply(apply(fp$asfr, 2, tapply, rep(3:9*5, each=5), mean), 2, rep, each=5)
  
  fp$srb <- sapply(demp$srb[as.character(proj_start:proj_end)], function(x) c(x,100)/(x+100))
  
  ## Spectrum adjusts net-migration to occur half in current age group and half in next age group
  netmigr.adj <- demp$netmigr
  netmigr.adj[-1,,] <- (demp$netmigr[-1,,] + demp$netmigr[-81,,])/2
  netmigr.adj[1,,] <- demp$netmigr[1,,]/2
  netmigr.adj[81,,] <- netmigr.adj[81,,] + demp$netmigr[81,,]/2

  fp$netmigr <- netmigr.adj[(AGE_START+1):81,,as.character(proj_start:proj_end)]


  ## Calcuate the net-migration and survival up to AGE_START for each birth cohort.
  ## For cohorts born before projection start, this will be the partial
  ## survival since the projection start to AGE_START, and the corresponding lagged "births"
  ## represent the number in the basepop who will survive to the corresponding age.
  
  cumnetmigr <- array(0, dim=c(NG, PROJ_YEARS))
  cumsurv <- array(1, dim=c(NG, PROJ_YEARS))
  if(AGE_START > 0)
    for(i in 2:PROJ_YEARS)  # start at 2 because year 1 inputs are not used
      for(s in 1:2)
        for(j in max(1, AGE_START-(i-2)):AGE_START){
          ii <- i+j-AGE_START
          cumsurv[s,i] <- cumsurv[s,i] * demp$Sx[j,s,ii]
          if(j==1)
            cumnetmigr[s,i] <- netmigr.adj[j,s,ii] * (1+2*demp$Sx[j,s,ii])/3
          else
            cumnetmigr[s,i] <- cumnetmigr[s,i]*demp$Sx[j,s,ii] + netmigr.adj[j,s,ii] * (1+demp$Sx[j,s,ii])/2
        }
  
  ## initial values for births
  birthslag <- array(0, dim=c(NG, PROJ_YEARS))             # birthslag(i,s) = number of births of sex s, i-AGE_START years ago
  birthslag[,1:AGE_START] <- t(basepop_allage[AGE_START:1,])  # initial pop values (NOTE REVERSE ORDER). Rest will be completed by fertility during projection
  
  fp$birthslag <- birthslag
  fp$cumsurv <- cumsurv
  fp$cumnetmigr <- cumnetmigr


  ## set population adjustment
  fp$popadjust <- popadjust
  if(!length(setdiff(proj_start:proj_end, dimnames(targetpop)[[3]]))){
    fp$entrantpop <- targetpop[AGE_START,,as.character(proj_start:proj_end)]
    fp$targetpop <- targetpop[(AGE_START+1):81,,as.character(proj_start:proj_end)]
  }
  if(popadjust & is.null(fp$targetpop))
    stop("targetpop does not span proj_start:proj_end")


  ## calculate births during calendar year
  ## Spectrum births output represents births midyear previous year to midyear
  ## current year. Adjust births for half year offset
  fp$births <- demp$births[as.character(proj_start:proj_end)]
  fp$births[-1] <- (fp$births[-1] + fp$births[-PROJ_YEARS]) / 2
  
  ## ###################### ##
  ##  HIV model parameters  ##
  ## ###################### ##

  fp$relinfectART <- projp$relinfectART

  fp$incrr_sex <- projp$incrr_sex[as.character(proj_start:proj_end)]
  
  ## Use Beer's coefficients to distribution IRRs by age/sex
  Amat <- create_beers(17)
  fp$incrr_age <- apply(projp$incrr_age, 2:3, function(x)  Amat %*% x)[AGE_START + 1:pAG, , as.character(proj_start:proj_end)]
  fp$incrr_age[fp$incrr_age < 0] <- 0
  
  
  projp.h.ag <- findInterval(AGE_START + cumsum(h.ag.span) - h.ag.span, c(15, 25, 35, 45))  # NOTE: Will not handle AGE_START < 15 presently
  fp$cd4_initdist <- projp$cd4_initdist[,projp.h.ag,]
  fp$cd4_prog <- (1-exp(-projp$cd4_prog[,projp.h.ag,] / hiv_steps_per_year)) * hiv_steps_per_year
  fp$cd4_mort <- projp$cd4_mort[,projp.h.ag,]
  fp$art_mort <- projp$art_mort[,,projp.h.ag,]
  fp$artmx_timerr <- projp$artmx_timerr

  frr_agecat <- as.integer(rownames(projp$fert_rat))
  frr_agecat[frr_agecat == 18] <- 17
  fert_rat.h.ag <- findInterval(AGE_START + cumsum(h.ag.span[h.fert.idx]) - h.ag.span[h.fert.idx], frr_agecat)

  fp$frr_cd4 <- array(1, c(hDS, length(h.fert.idx), PROJ_YEARS))
  fp$frr_cd4[,,] <- rep(projp$fert_rat[fert_rat.h.ag, as.character(proj_start:proj_end)], each=hDS)
  fp$frr_cd4 <- sweep(fp$frr_cd4, 1, projp$cd4fert_rat, "*")
  fp$frr_cd4 <- fp$frr_cd4 * projp$frr_scalar
  
  fp$frr_art <- array(1.0, c(hTS, hDS, length(h.fert.idx), PROJ_YEARS))
  fp$frr_art[1,,,] <- fp$frr_cd4 # 0-6 months
  fp$frr_art[2:3, , , ] <- sweep(fp$frr_art[2:3, , , ], 3, projp$frr_art6mos[fert_rat.h.ag] * projp$frr_scalar, "*") # 6-12mos, >1 years

  ## ART eligibility and numbers on treatment

  fp$art15plus_num <- projp$art15plus_num[,as.character(proj_start:proj_end)]
  fp$art15plus_isperc <- projp$art15plus_numperc[, as.character(proj_start:proj_end)] == 1

  ## convert percentage to proportion
  fp$art15plus_num[fp$art15plus_isperc] <- fp$art15plus_num[fp$art15plus_isperc] / 100

  ## eligibility starts in projection year idx
  fp$specpop_percelig <- rowSums(with(projp$artelig_specpop[-1,], mapply(function(elig, percent, year) rep(c(0, percent*as.numeric(elig)), c(year - proj_start, proj_end - year + 1)), elig, percent, year)))
  fp$artcd4elig_idx <- findInterval(-projp$art15plus_eligthresh[as.character(proj_start:proj_end)], -c(999, 500, 350, 250, 200, 100, 50))

  ## Update eligibility threshold from CD4 <200 to <250 to account for additional
  ## proportion eligible with WHO Stage 3/4.
  if(artelig200adj)
    fp$artcd4elig_idx <- replace(fp$artcd4elig_idx, fp$artcd4elig_idx==5L, 4L)

  fp$pw_artelig <- with(projp$artelig_specpop["PW",], rep(c(0, elig), c(year - proj_start, proj_end - year + 1)))  # are pregnant women eligible (0/1)

  ## percentage of those with CD4 <350 who are based on WHO Stage III/IV infection
  fp$who34percelig <- who34percelig

  fp$art_dropout <- projp$art_dropout[as.character(proj_start:proj_end)]/100
  fp$median_cd4init <- projp$median_cd4init[as.character(proj_start:proj_end)]
  fp$med_cd4init_input <- as.integer(fp$median_cd4init > 0)
  fp$med_cd4init_cat <- replace(findInterval(-fp$median_cd4init, - c(1000, 500, 350, 250, 200, 100, 50)),
                                !fp$med_cd4init_input, 0L)

  fp$tARTstart <- min(unlist(apply(fp$art15plus_num > 0, 1, which)))

  ## New ART patient allocation options
  fp$art_alloc_method <- projp$art_alloc_method
  fp$art_alloc_mxweight <- projp$art_prop_alloc[1]

  ## Scale mortality among untreated population by ART coverage
  fp$scale_cd4_mort <- projp$scale_cd4_mort
  
  ## Vertical transmission and survival to AGE_START for lagged births
  
  fp$verttrans_lag <- setNames(c(rep(0, AGE_START), projp$verttrans[1:(PROJ_YEARS-AGE_START)]), proj_start:proj_end)

  ## calculate probability of HIV death in each year
  hivqx <- apply(projp$hivdeaths[1:AGE_START,,], c(1,3), sum) / apply(projp$hivpop[1:AGE_START,,], c(1,3), sum)
  hivqx[is.na(hivqx)] <- 0.0

  ## probability of surviving to AGE_START for each cohort (product along diagonal)
  cumhivsurv <- sapply(1:(PROJ_YEARS - AGE_START), function(i) prod(1-hivqx[cbind(1:15, i-1+1:15)]))

  fp$paedsurv_lag <- setNames(c(rep(1, AGE_START), cumhivsurv), proj_start:proj_end)

  ## ## EQUIVALENT CODE, easier to read
  ## fp$paedsurv_lag <- rep(1.0, PROJ_YEARS)
  ## for(i in 1:(PROJ_YEARS-AGE_START))
  ##   for(j in 1:AGE_START)
  ##     fp$paedsurv_lag[i+AGE_START] <- fp$paedsurv_lag[i+AGE_START] * (1 - hivqx[j, i+j-1])

  ## HIV prevalence and ART coverage among age 15 entrants
  hivpop14 <- projp$age14hivpop[,,,as.character(proj_start:(proj_end-1))]
  pop14 <- projp$age14totpop[ , as.character(proj_start:(proj_end-1))]
  hiv14 <- colSums(hivpop14,,2)
  art14 <- colSums(hivpop14[5:7,,,],,2)

  fp$entrantprev <- cbind(0, hiv14/pop14) # 1 year offset because age 15 population is age 14 in previous year
  fp$entrantartcov <- cbind(0, art14/hiv14)
  fp$entrantartcov[is.na(fp$entrantartcov)] <- 0
  colnames(fp$entrantprev) <- colnames(fp$entrantartcov) <- as.character(proj_start:proj_end)

  hiv_noart14 <- colSums(hivpop14[1:4,,,])
  artpop14 <- hivpop14[5:7,,,]

  fp$paedsurv_cd4dist <- array(0, c(hDS, NG, PROJ_YEARS))
  fp$paedsurv_artcd4dist <- array(0, c(hTS, hDS, NG, PROJ_YEARS))

  cd4convert <- rbind(c(1, 0, 0, 0, 0, 0, 0),
                      c(1, 0, 0, 0, 0, 0, 0),
                      c(1, 0, 0, 0, 0, 0, 0),
                      c(0, 1, 0, 0, 0, 0, 0),
                      c(0, 0, 0.67, 0.33, 0, 0, 0),
                      c(0, 0, 0, 0, 0.35, 0.21, 0.44))

  ## Convert age 5-14 CD4 distribution to adult CD4 distribution and normalize to
  ## sum to 1 in each sex and year.
  for(g in 1:NG)
    for(i in 2:PROJ_YEARS){
      
      if((hiv14[g,i-1] - art14[g,i-1]) > 0)
        fp$paedsurv_cd4dist[,g,i] <- hiv_noart14[,g,i-1] %*% cd4convert / (hiv14[g,i-1] - art14[g,i-1])
      if(art14[g,i-1]){
        fp$paedsurv_artcd4dist[,,g,i] <- artpop14[,,g,i-1] %*% cd4convert / art14[g,i-1]

        ## if age 14 has ART population in CD4 above adult eligibilty, assign to highest adult
        ## ART eligibility category.
        idx <- fp$artcd4elig_idx[i]
        if(idx > 1){
          fp$paedsurv_artcd4dist[,idx,g,i] <- fp$paedsurv_artcd4dist[,idx,g,i] + rowSums(fp$paedsurv_artcd4dist[,1:(idx-1),g,i, drop=FALSE])
          fp$paedsurv_artcd4dist[,1:(idx-1),g,i] <- 0
        }
      }
    }
  
  fp$netmig_hivprob <- 0.4*0.22
  fp$netmighivsurv <- 0.25/0.22
  
  ## Circumcision parameters (default no effect)
  fp$circ_incid_rr <- 0.0  # no reduction
  fp$circ_prop <- array(0.0, c(ss$pAG, ss$PROJ_YEARS),
                        list(age = ss$AGE_START + 1:ss$pAG - 1L,
                             year = ss$proj_start + 1:ss$PROJ_YEARS - 1L))
  
  class(fp) <- "specfp"

  return(fp)
}


prepare_rtrend_model <- function(fp, iota=0.0025){
  fp$iota <- iota
  fp$tsEpidemicStart <- NULL
  fp$eppmod <- "rtrend"
  return(fp)
}


prepare_rspline_model <- function(fp, numKnots=NULL, tsEpidemicStart=fp$ss$time_epi_start+0.5){

  if(!exists("numKnots", fp))
    fp$numKnots <- 7

  fp$tsEpidemicStart <- fp$proj.steps[which.min(abs(fp$proj.steps - tsEpidemicStart))]
  epi_steps <- fp$proj.steps[fp$proj.steps >= fp$tsEpidemicStart]
  proj.dur <- diff(range(epi_steps))
  rvec.knots <- seq(min(epi_steps) - 3*proj.dur/(fp$numKnots-3), max(epi_steps) + 3*proj.dur/(fp$numKnots-3), proj.dur/(fp$numKnots-3))
  fp$rvec.spldes <- rbind(matrix(0, length(fp$proj.steps) - length(epi_steps), fp$numKnots),
                          splines::splineDesign(rvec.knots, epi_steps))

  if(!exists("rtpenord", fp))
    fp$rtpenord <- 2L
  if(!exists("eppmod", fp))
    fp$eppmod <- "rspline"
  fp$iota <- NULL

  return(fp)
}


update.specfp <- function (fp, ..., keep.attr = TRUE, list = vector("list")){
  dots <- substitute(list(...))[-1]
  newnames <- names(dots)
  for (j in seq_along(dots)) {
    if (keep.attr) 
      attr <- attributes(fp[[newnames[j]]])
    fp[[newnames[j]]] <- eval(dots[[j]], fp, parent.frame())
    if (keep.attr) 
      attributes(fp[[newnames[j]]]) <- c(attr, attributes(fp[[newnames[j]]]))
  }
  listnames <- names(list)
  for (j in seq_along(list)) {
        if (keep.attr) 
          attr <- attributes(fp[[listnames[j]]])
        fp[[listnames[j]]] <- eval(list[[j]], fp, parent.frame())
        if (keep.attr) 
          attributes(fp[[listnames[j]]]) <- c(attr, attributes(fp[[listnames[j]]]))
  }
  return(fp)
}


#########################
####  Model outputs  ####
#########################

## modprev15to49 <- function(mod, fp){colSums(mod[fp$ss$p.age15to49.idx,,fp$ss$hivp.idx,],,2) / colSums(mod[fp$ss$p.age15to49.idx,,,],,3)}
prev.spec <- function(mod, fp){ attr(mod, "prev15to49") }
incid.spec <- function(mod, fp){ attr(mod, "incid15to49") }
fnPregPrev.spec <- function(mod, fp) { attr(mod, "pregprev") }

calc_prev15to49 <- function(mod, fp){
  colSums(mod[fp$ss$p.age15to49.idx,,2,],,2)/colSums(mod[fp$ss$p.age15to49.idx,,,],,3)
}

calc_incid15to49 <- function(mod, fp){
  c(0, colSums(attr(mod, "infections")[fp$ss$p.age15to49.idx,,-1],,2)/colSums(mod[fp$ss$p.age15to49.idx,,1,-fp$ss$PROJ_YEARS],,2))
}

calc_pregprev <- function(mod, fp){
  warning("not yet implemented")
}



#' Age-specific mortality
#'
#' Calculate all-cause mortality rate by single year of age and sex from a
#' \code{spec} object.
#'
#' Mortality in year Y is calculated as the number of deaths occurring from the
#' mid-year of year Y-1 to mid-year Y, divided by the population size at the
#' mid-year of year Y-1.
#' !!! NOTE: This might be different from the calculation in Spectrum. Should
#' confirm this with John Stover.
#'
#' @param mod output of simmod of class \code{\link{spec}}.
#' @return 3-dimensional array of mortality by age, sex, and year.
agemx.spec <- function(mod, nonhiv=FALSE){
  if(nonhiv)
    deaths <- attr(mod, "natdeaths")
  else
    deaths <- attr(mod, "natdeaths") + attr(mod, "hivdeaths")
  pop <- mod[,,1,]+ mod[,,2,]

  mx <- array(0, dim=dim(pop))
  mx[,,-1] <-deaths[,,-1] / pop[,,-dim(pop)[3]]

  return(mx)
}


#' Non-HIV age-specific mortality
#'
#' Calculate all-cause mortality rate by single year of age and sex from a
#' \code{spec} object.
#'
#' Mortality in year Y is calculated as the number of non-HIV deaths occurring
#' from the mid-year of year Y-1 to mid-year Y, divided by the population size
#' at the mid-year of year Y-1.
#' !!! NOTE: This might be different from the calculation in Spectrum. Should
#' confirm this with John Stover.
#'
#' @param mod output of simmod of class \code{\link{spec}}.
#' @return 3-dimensional array of mortality by age, sex, and year.
natagemx.spec <- function(mod){
  deaths <- attr(mod, "natdeaths")
  pop <- mod[,,1,]+ mod[,,2,]

  mx <- array(0, dim=dim(pop))
  mx[,,-1] <-deaths[,,-1] / pop[,,-dim(pop)[3]]

  return(mx)
}

hivagemx.spec <- function(mod){
  deaths <- attr(mod, "natdeaths")
  pop <- mod[,,1,]+ mod[,,2,]

  mx <- array(0, dim=dim(pop))
  mx[,,-1] <-deaths[,,-1] / pop[,,-dim(pop)[3]]

  return(mx)
}



#' Prevalene by arbitrary age groups
#'
#' @param sidx sex (1 = Male, 2 = Female, 0 = Both)
#' Notes: Assumes that AGE_START is 15 and single year of age.
#' @useDynLib eppasm ageprevC
ageprev <- function(mod, aidx=NULL, sidx=NULL, yidx=NULL, agspan=5, expand=FALSE, VERSION="C"){

  if(length(agspan)==1)
    agspan <- rep(agspan, length(aidx))
  
  if(expand){
    dimout <- c(length(aidx), length(sidx), length(yidx))
    df <- expand.grid(aidx=aidx, sidx=sidx, yidx=yidx)
    aidx <- df$aidx
    sidx <- df$sidx
    yidx <- df$yidx
    agspan <- rep(agspan, times=length(sidx)*length(yidx))
  } 
  
  if(VERSION != "R") { 

    prev <- .Call(ageprevC, mod,
                  as.integer(aidx), as.integer(sidx),
                  as.integer(yidx), as.integer(agspan))
    
  } else {

    idx <- data.frame(aidx=aidx, sidx=sidx, yidx=yidx, agspan=agspan)
    idx$gidx <- seq_len(nrow(idx))
    
    ## Add M/F entries with same id if sidx = 0.
    ## This is probably a pretty inefficient way of doing this...
    
    if(any(idx$sidx == 0)){
      idx <- rbind(idx[idx$sidx != 0,], transform(idx[idx$sidx == 0,], sidx = 1), transform(idx[idx$sidx == 0,], sidx = 2))
      idx <- idx[order(idx$gidx, idx$sidx),]
    }
    
    idx$id <- seq_len(nrow(idx))
    
    increment <- unlist(lapply(idx$agspan, seq_len))-1
    id_idx <- rep(idx$id, idx$agspan)
    
    g_idx <- idx$gidx[id_idx]
    a_idx <- idx$aidx[id_idx] + increment
    s_idx <- idx$sidx[id_idx]
    y_idx <- idx$yidx[id_idx]
    
    hivn <- fastmatch::ctapply(mod[cbind(a_idx, s_idx, 1, y_idx)], g_idx, sum)
    hivp <- fastmatch::ctapply(mod[cbind(a_idx, s_idx, 2, y_idx)], g_idx, sum)
    prev <- hivp/(hivn+hivp)

  }

  if(expand)
    prev <- array(prev, dimout)
    
  return(prev)
}

ageincid <- function(mod, aidx=NULL, sidx=NULL, yidx=NULL, agspan=5, arridx=NULL){

  if(is.null(arridx)){
    if(length(agspan)==1)
      agspan <- rep(agspan, length(aidx))
    
    dims <- dim(mod)
    idx <- expand.grid(aidx=aidx, sidx=sidx, yidx=yidx)
    arridx_inf <- idx$aidx + (idx$sidx-1)*dims[1] + (idx$yidx-1)*dims[1]*dims[2]
    arridx_hivn <- idx$aidx + (idx$sidx-1)*dims[1] + (pmax(idx$yidx-2, 0))*dims[1]*dims[2]
    agspan <- rep(agspan, times=length(sidx)*length(yidx))
  } else if(length(agspan)==1){
    ## arridx_hivn  NEED ADJUST arridx FOR PREVIOUS YEAR
    agspan <- rep(agspan, length(arridx))
  }

  agidx_inf <- rep(arridx_inf, agspan)
  agidx_hivn <- rep(arridx_hivn, agspan)
  allidx_inf <- agidx_inf + unlist(sapply(agspan, seq_len))-1
  allidx_hivn <- agidx_hivn + unlist(sapply(agspan, seq_len))-1

  inf <- fastmatch::ctapply(attr(mod, "infections")[allidx_inf], agidx_inf, sum)
  hivn <- fastmatch::ctapply(mod[,,1,][allidx_hivn], agidx_hivn, sum)
  
  incid <- inf/hivn
  if(!is.null(aidx))
    incid <- array(incid, c(length(aidx), length(sidx), length(yidx)))
  return(incid)
}


ageinfections <- function(mod, aidx=NULL, sidx=NULL, yidx=NULL, agspan=5, arridx=NULL){

  if(is.null(arridx)){
    if(length(agspan)==1)
      agspan <- rep(agspan, length(aidx))
    
    dims <- dim(mod)
    idx <- expand.grid(aidx=aidx, sidx=sidx, yidx=yidx)
    arridx_inf <- idx$aidx + (idx$sidx-1)*dims[1] + (idx$yidx-1)*dims[1]*dims[2]
    arridx_hivn <- idx$aidx + (idx$sidx-1)*dims[1] + (pmax(idx$yidx-2, 0))*dims[1]*dims[2]
    agspan <- rep(agspan, times=length(sidx)*length(yidx))
  } else if(length(agspan)==1){
    ## arridx_hivn  NEED ADJUST arridx FOR PREVIOUS YEAR
    agspan <- rep(agspan, length(arridx))
  }

  agidx_inf <- rep(arridx_inf, agspan)
  allidx_inf <- agidx_inf + unlist(sapply(agspan, seq_len))-1

  inf <- fastmatch::ctapply(attr(mod, "infections")[allidx_inf], agidx_inf, sum)
  
  if(!is.null(aidx))
    inf <- array(inf, c(length(aidx), length(sidx), length(yidx)))
  return(inf)
}

ageartcov <- function(mod, aidx=NULL, sidx=NULL, yidx=NULL, agspan=5, arridx=NULL,
                      h.ag.span=c(2, 3, 5, 5, 5, 5, 5, 5, 31)){
  
  if(is.null(arridx)){
    if(length(agspan)==1)
      agspan <- rep(agspan, length(aidx))
    
    dims <- dim(mod)
    idx <- expand.grid(aidx=aidx, sidx=sidx, yidx=yidx)
    arridx <- idx$aidx + (idx$sidx-1)*dims[1] + (idx$yidx-1)*dims[1]*dims[2]
    agspan <- rep(agspan, times=length(sidx)*length(yidx))
  } else {
    stop("NOT YET IMPLEMENTED FOR arridx inputs")
    if(length(agspan)==1)
      agspan <- rep(agspan, length(arridx))
  }
  
  agidx <- rep(arridx, agspan)
  sidx.ag <- rep(idx$sidx, agspan)
  yidx.ag <- rep(idx$yidx, agspan)
  allidx <- agidx + unlist(sapply(agspan, seq_len))-1

  h.ag.idx <- rep(seq_along(h.ag.span), h.ag.span)
  haidx <- h.ag.idx[rep(idx$aidx, agspan) + unlist(sapply(agspan, seq_len))-1]

  ## ART coverage with HA age groups
  artpop <- colSums(attr(mod, "artpop"),,2)
  artcov <- artpop / (artpop + colSums(attr(mod, "hivpop"),,1))

  hdim <- dim(artcov)
  hallidx <- haidx + (sidx.ag-1)*hdim[1] + (yidx.ag-1)*hdim[1]*hdim[2]

  artp <- fastmatch::ctapply(mod[,,2,][allidx]*artcov[hallidx], agidx, sum) # number on ART
  hivp <- fastmatch::ctapply(mod[,,2,][allidx], agidx, sum)
  
  artcov <- artp/hivp
  if(!is.null(aidx))
    artcov <- array(artcov, c(length(aidx), length(sidx), length(yidx)))
  return(artcov)
}


#' Age-specific prevalence among pregnant women
#' 
#' @param expand whether to expand aidx, yidx, sidx, and agspan
agepregprev <- function(mod, fp,
                        aidx=3:9*5-fp$ss$AGE_START+1L,
                        yidx=1:fp$ss$PROJ_YEARS,
                        agspan=5,
                        expand=FALSE){
  sidx <- fp$ss$f.idx # only women get pregnant

  if(length(agspan)==1)
    agspan <- rep(agspan, length(aidx))
  
  if(expand){
    idx <- expand.grid(aidx=aidx, sidx=sidx, yidx=yidx)
    idx$agspan <- rep(agspan, times=length(sidx)*length(yidx))
  } else
    idx <- data.frame(aidx=aidx, sidx=sidx, yidx=yidx, agspan=agspan)

  idx$id <- seq_len(nrow(idx))

  increment <- unlist(lapply(idx$agspan, seq_len))-1
  a_idx <- rep(idx$aidx, idx$agspan) + increment
  s_idx <- rep(idx$sidx, idx$agspan)
  y_idx <- rep(idx$yidx, idx$agspan)
  yminus1_idx <- rep(pmax(idx$yidx-1, 1), idx$agspan)
  id_idx <- rep(idx$id, idx$agspan)

  ## get index in asfr and ha_frr array
  fert_idx <- match(a_idx, fp$ss$p.fert.idx)
  hfert_idx <- match(fp$ss$ag.idx[a_idx], fp$ss$h.fert.idx)

  hivp <- (mod[cbind(a_idx, s_idx, 2, y_idx)] + mod[cbind(a_idx, s_idx, 2, yminus1_idx)]) / 2
  hivn <- (mod[cbind(a_idx, s_idx, 1, y_idx)] + mod[cbind(a_idx, s_idx, 1, yminus1_idx)]) / 2

  ## Calculate age-specific FRR given the CD4 and ART duration distribution
  hivpop_fert <- attr(mod, "hivpop")[ , fp$ss$h.fert.idx, fp$ss$f.idx, ]
  artpop_fert <- attr(mod, "artpop")[ , , fp$ss$h.fert.idx, fp$ss$f.idx, ]
  ha_frr <- (colSums(hivpop_fert * fp$frr_cd4) + colSums(artpop_fert * fp$frr_art,,2)) / (colSums(hivpop_fert) + colSums(artpop_fert,,2))
  
  births_a <- fp$asfr[cbind(fert_idx, y_idx)] * (hivn + hivp)
  pregprev_a <- 1 - hivn / (hivn + ha_frr[cbind(hfert_idx, y_idx)] * hivp)

  fastmatch::ctapply(pregprev_a * births_a, id_idx, sum) / fastmatch::ctapply(births_a, id_idx, sum)
}



#' Age-specific ART coverage among pregnant women
#' 
#' @param expand whether to expand aidx, yidx, sidx, and agspan
agepregartcov <- function(mod, fp,
                          aidx=3:9*5-fp$ss$AGE_START+1L,
                          yidx=1:fp$ss$PROJ_YEARS,
                          agspan=5,
                          expand=FALSE){
  sidx <- fp$ss$f.idx # only women get pregnant
  
  if(length(agspan)==1)
    agspan <- rep(agspan, length(aidx))
  
  if(expand){
    idx <- expand.grid(aidx=aidx, sidx=sidx, yidx=yidx)
    idx$agspan <- rep(agspan, times=length(sidx)*length(yidx))
  } else
    idx <- data.frame(aidx=aidx, sidx=sidx, yidx=yidx, agspan=agspan)
  
  idx$id <- seq_len(nrow(idx))
  
  increment <- unlist(lapply(idx$agspan, seq_len))-1
  a_idx <- rep(idx$aidx, idx$agspan) + increment
  s_idx <- rep(idx$sidx, idx$agspan)
  y_idx <- rep(idx$yidx, idx$agspan)
  yminus1_idx <- rep(pmax(idx$yidx-1, 1), idx$agspan)
  id_idx <- rep(idx$id, idx$agspan)
  
  ## get index in asfr and ha_frr array
  fert_idx <- match(a_idx, fp$ss$p.fert.idx)
  hfert_idx <- match(fp$ss$ag.idx[a_idx], fp$ss$h.fert.idx)
  
  hivp <- (mod[cbind(a_idx, s_idx, 2, y_idx)] + mod[cbind(a_idx, s_idx, 2, yminus1_idx)]) / 2
  hivn <- (mod[cbind(a_idx, s_idx, 1, y_idx)] + mod[cbind(a_idx, s_idx, 1, yminus1_idx)]) / 2
  
  ## Calculate age-specific FRR given the CD4 and ART duration distribution
  hivpop_fert <- attr(mod, "hivpop")[ , fp$ss$h.fert.idx, fp$ss$f.idx, ]
  artpop_fert <- attr(mod, "artpop")[ , , fp$ss$h.fert.idx, fp$ss$f.idx, ]
  wgt_hivp <- colSums(hivpop_fert * fp$frr_cd4)
  wgt_art <- colSums(artpop_fert * fp$frr_art,,2)

  ha_frr <- (wgt_hivp + wgt_art) / (colSums(hivpop_fert) + colSums(artpop_fert,,2))
  ha_artcov <- wgt_art / (wgt_hivp + wgt_art)

  births_a <- fp$asfr[cbind(fert_idx, y_idx)] * (hivn + hivp)
  pregprev_a <- 1 - hivn / (hivn + ha_frr[cbind(hfert_idx, y_idx)] * hivp)
  artcov_a <- ha_artcov[cbind(hfert_idx, y_idx)]
  
  fastmatch::ctapply(artcov_a * pregprev_a * births_a, id_idx, sum) / fastmatch::ctapply(pregprev_a * births_a, id_idx, sum)
}



incid_sexratio.spec <- function(mod){
  inc <- ageincid(mod, 1, 1:2, seq_len(dim(mod)[4]), 35)[,,]
  inc[2,] / inc[1,]
}


calc_nqx.spec <- function(mod, fp, n=45, x=15, nonhiv=FALSE){
  mx <- agemx(mod, nonhiv)
  return(1-exp(-colSums(mx[x+1:n-fp$ss$AGE_START,,])))
}


pop15to49.spec <- function(mod){colSums(mod[1:35,,,],,3)}
artpop15to49.spec <- function(mod){colSums(attr(mod, "artpop")[,,1:8,,],,4)}
artpop15plus.spec <- function(mod){colSums(attr(mod, "artpop"),,4)}

artcov15to49.spec <- function(mod, sex=1:2){
  n_art <- colSums(attr(mod, "artpop")[,,1:8,sex,,drop=FALSE],,4)
  n_hiv <- colSums(attr(mod, "hivpop")[,1:8,sex,,drop=FALSE],,3)
  return(n_art / (n_hiv+n_art))
}

artcov15plus.spec <- function(mod, sex=1:2){
  n_art <- colSums(attr(mod, "artpop")[,,,sex,,drop=FALSE],,4)
  n_hiv <- colSums(attr(mod, "hivpop")[,,sex,,drop=FALSE],,3)
  return(n_art / (n_hiv+n_art))
}

age15pop.spec <- function(mod){colSums(mod[1,,,],,2)}


## Finding corresponding child cd4 bin from input eligibility
get_paed_cd4bin <- function(CD4elig, age, ss){
  invisible(list2env(ss, environment()))
  if(age < 5){
    j <- 1
    while(as.integer(CD4elig) < as.integer(names(u5.elig.groups)[j])){
      j <- j + 1
    }
    out <- min(u5.elig.groups[[j]] + 1, 7)
  } else{
    j <- 1
    while(as.integer(CD4elig) < as.integer(names(u15.elig.groups)[j])){
      j <- j + 1
    }
    out <- min(u5.elig.groups[[j]] + 1, 6)
  }
  return(out)
}


