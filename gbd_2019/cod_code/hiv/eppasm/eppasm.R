simmod.specfp <- function(fp, VERSION="C"){

  if(!exists("popadjust", where=fp))
    fp$popadjust <- FALSE

  if(!exists("incidmod", where=fp))
    fp$incidmod <- "eppspectrum"

  if(VERSION != "R"){
    fp$eppmodInt <- match(fp$eppmod, c("rtrend", "directincid"), nomatch=0) # 0: r-spline;
    fp$incidmodInt <- match(fp$incidmod, c("eppspectrum", "transm"))-1L  # -1 for 0-based indexing
    mod <- .Call(eppasmC, fp)
    class(mod) <- "spec"
    return(mod)
  }

##################################################################################

  if(requireNamespace("fastmatch", quietly = TRUE))
    ctapply <- fastmatch::ctapply
  else
    ctapply <- tapply

  fp$ss$DT <- 1/fp$ss$hiv_steps_per_year

  ## Attach state space variables
  invisible(list2env(fp$ss, environment())) # put ss variables in environment for convenience

  birthslag <- fp$birthslag
  pregprevlag <- rep(0, PROJ_YEARS)

  ## initialize projection
  pop <- array(0, c(pAG, NG, pDS, PROJ_YEARS))
  pop[,,1,1] <- fp$basepop
  hivpop <- array(0, c(hDS, hAG, NG, PROJ_YEARS))
  artpop <- array(0, c(hTS, hDS, hAG, NG, PROJ_YEARS))
  
  ## paediatric
  if(exists('paedbasepop', where = fp)){
    popu5 <- array(0, c(pAGu5, NG, pDS, PROJ_YEARS))
    popu5[,,1,1] <- fp$paedbasepop[1:5,]
    hivpopu5 <- array(0, c(hMT, hDSu5, pAGu5, NG, PROJ_YEARS))
    dimnames(hivpopu5) <- list(transmission = c('BF0', 'BF6', 'BF12', 'perinatal'), cat = 1:7, age = 0:4, sex = c('Male', 'Female'), year = 1:PROJ_YEARS)
    artpopu5 <- array(0, c(hTS, hDSu5, pAGu5, NG, PROJ_YEARS))
    dimnames(artpopu5) <- list(transmission = c('ART0MOS', 'ART6MOS', 'ART1YR'), cat = 1:7, age = 0:4, sex = c('Male', 'Female'), year = 1:PROJ_YEARS)
    
    
    popu15 <- array(0, c(pAGu15, NG, pDS, PROJ_YEARS))
    popu15[,,1,1] <- fp$paedbasepop[6:15,]
    hivpopu15 <- array(0, c(hMT, hDSu15, pAGu15, NG, PROJ_YEARS))
    dimnames(hivpopu15) <- list(transmission = c('BF0', 'BF6', 'BF12', 'perinatal'), cat = 1:6, age = 5:14, sex = c('Male', 'Female'), year = 1:PROJ_YEARS)
    artpopu15 <- array(0, c(hTS, hDSu15, pAGu15, NG, PROJ_YEARS))
    dimnames(artpopu15) <- list(transmission = c('ART0MOS', 'ART6MOS', 'ART1YR'), cat = 1:6, age = 5:14, sex = c('Male', 'Female'), year = 1:PROJ_YEARS)
    u15hivdeaths.out <- array(0, c(15, NG, PROJ_YEARS))
    u15deaths.out <- array(0, c(15, NG, PROJ_YEARS))
    hivbirths.out <- array(0, c(NG, PROJ_YEARS))
    u15infections <- array(0, c(pAGu5 + pAGu15, NG, PROJ_YEARS))
    ## tracking under 1 infections to inform under 1 incidence splits
    prop.trans <- array(0, c(3, PROJ_YEARS))
    dimnames(prop.trans) <- list(transmission = c('perinatal', 'BF0', 'BF6'), year = 1:PROJ_YEARS)
  }
  
  ## initialize output
  prev15to49 <- numeric(PROJ_YEARS)
  incid15to49 <- numeric(PROJ_YEARS)
  sexinc15to49out <- array(NA, c(NG, PROJ_YEARS))
  paedsurvout <- rep(NA, PROJ_YEARS)
  hivdeaths_est <- array(0, c(14, NG, PROJ_YEARS ))
  infections <- array(0, c(pAG, NG, PROJ_YEARS))
  hivdeaths <- array(0, c(pAG, NG, PROJ_YEARS))
  natdeaths <- array(0, c(pAG, NG, PROJ_YEARS))

  popadj.prob <- array(0, c(pAG, NG, PROJ_YEARS))

  if(fp$eppmod != "directincid"){
    ## outputs by timestep
    incrate15to49.ts.out <- rep(NA, length(fp$rvec))
    rvec <- if(fp$eppmod == "rtrend") rep(NA, length(fp$proj.steps)) else fp$rvec

    prev15to49.ts.out <- rep(NA, length(fp$rvec))
  }

  entrant_prev_out <- numeric(PROJ_YEARS)
  hivp_entrants_out <- array(0, c(NG, PROJ_YEARS))

  ## store last prevalence value (for r-trend model)
  prevlast <- 0


  for(i in 2:fp$SIM_YEARS){
    ## ################################### ##
    ##  Single-year population projection  ##
    ## ################################### ##
    ## age adults
    pop[-c(1,pAG),,,i] <- pop[-(pAG-1:0),,,i-1]
    pop[pAG,,,i] <- pop[pAG,,,i-1] + pop[pAG-1,,,i-1] # open age group
    
    ## age 15 entrants
    if(exists('paedbasepop', where = fp)){
      if(exists("popadjust", where=fp) & fp$popadjust){
        entrant_prev <- popu15[10,,2,i - 1] / (popu15[10,,2,i - 1] + popu15[10,,1,i - 1])
        hivn_entrants <- fp$paedtargetpop[15,,i-1]*(1-entrant_prev)
        hivp_entrants <- fp$paedtargetpop[15,,i-1]*entrant_prev
      }else{
        hivn_entrants <- popu15[10,,1,i-1]
        hivp_entrants <- popu15[10,,2,i-1]
      }
      artpop15 <- apply(artpopu15[,,pAGu15,,i-1], 3, sum)
      fp$entrantartcov[,i] <- ifelse(hivp_entrants == 0, 0, artpop15/hivp_entrants)
    }else{
      ## Add lagged births into youngest age group
      entrant_prev <- fp$entrantprev[,i]
      
      if(exists("popadjust", where=fp) & fp$popadjust){
        hivn_entrants <- fp$entrantpop[,i-1]*(1-entrant_prev)
        hivp_entrants <- fp$entrantpop[,i-1]*entrant_prev
      } else {
        hivn_entrants <- birthslag[,i-1]*fp$cumsurv[,i-1]*(1-entrant_prev / fp$paedsurv_lag[i-1]) + fp$cumnetmigr[,i-1]*(1-pregprevlag[i-1]*fp$netmig_hivprob)
        hivp_entrants <- birthslag[,i-1]*fp$cumsurv[,i-1]*entrant_prev + fp$cumnetmigr[,i-1]*entrant_prev
      }
    }
    entrant_prev_out[i] <- sum(hivp_entrants) / sum(hivn_entrants+hivp_entrants)
    hivp_entrants_out[,i] <- sum(hivp_entrants)
    
    pop[1,,hivn.idx,i] <- hivn_entrants
    pop[1,,hivp.idx,i] <- hivp_entrants
    
    hiv.ag.prob <- pop[aglast.idx,,hivp.idx,i-1] / apply(pop[,,hivp.idx,i-1], 2, ctapply, ag.idx, sum)
    hiv.ag.prob[is.nan(hiv.ag.prob)] <- 0
    
    hivpop[,,,i] <- hivpop[,,,i-1]
    hivpop[,-hAG,,i] <- hivpop[,-hAG,,i] - sweep(hivpop[,-hAG,,i-1], 2:3, hiv.ag.prob[-hAG,], "*")
    hivpop[,-1,,i] <- hivpop[,-1,,i] + sweep(hivpop[,-hAG,,i-1], 2:3, hiv.ag.prob[-hAG,], "*")
    hivpop[,1,,i] <- hivpop[,1,,i] + sweep(fp$paedsurv_cd4dist[,,i], 2, hivp_entrants * (1-fp$entrantartcov[,i]), "*")
    
    if(i > fp$tARTstart){
      artpop[,,,,i] <- artpop[,,,,i-1]
      artpop[,,-hAG,,i] <- artpop[,,-hAG,,i] - sweep(artpop[,,-hAG,,i-1], 3:4, hiv.ag.prob[-hAG,], "*")
      artpop[,,-1,,i] <- artpop[,,-1,,i] + sweep(artpop[,,-hAG,,i-1], 3:4, hiv.ag.prob[-hAG,], "*")
      artpop[,,1,,i] <- artpop[,,1,,i] + sweep(fp$paedsurv_artcd4dist[,,,i], 3, hivp_entrants * fp$entrantartcov[,i], "*")
    }
    
    ## adult survival and migration
    deaths <- sweep(pop[,,,i], 1:2, (1-fp$Sx[,,i]), "*")
    hiv.sx.prob <- 1-apply(deaths[,,2], 2, ctapply, ag.idx, sum) / apply(pop[,,2,i], 2, ctapply, ag.idx, sum)
    hiv.sx.prob[is.nan(hiv.sx.prob)] <- 0
    pop[,,,i] <- pop[,,,i] - deaths
    natdeaths[,,i] <- rowSums(deaths,,2)
    
    hivpop[,,,i] <- sweep(hivpop[,,,i], 2:3, hiv.sx.prob, "*")
    if(i > fp$tARTstart)
      artpop[,,,,i] <- sweep(artpop[,,,,i], 3:4, hiv.sx.prob, "*")
    
    ## net migration
    netmigsurv <- fp$netmigr[,,i]*(1+fp$Sx[,,i])/2
    mr.prob <- 1+netmigsurv / rowSums(pop[,,,i],,2)
    ## setting a limit of 50% migration
    mr.prob[mr.prob < 0.5] <- 0.5
    hiv.mr.prob <- apply(mr.prob * pop[,,2,i], 2, ctapply, ag.idx, sum) /  apply(pop[,,2,i], 2, ctapply, ag.idx, sum)

    hiv.mr.prob[is.nan(hiv.mr.prob)] <- 0
    pop[,,,i] <- sweep(pop[,,,i], 1:2, mr.prob, "*")
    
    hivpop[,,,i] <- sweep(hivpop[,,,i], 2:3, hiv.mr.prob, "*")
    if(i > fp$tARTstart)
      artpop[,,,,i] <- sweep(artpop[,,,,i], 3:4, hiv.mr.prob, "*")
    ## fertility
    births.by.age <- rowSums(pop[p.fert.idx, f.idx,,i-1:0])/2 * fp$asfr[,i]
    if(exists("popadjust", where=fp) & fp$popadjust){
      adj <- sum(births.by.age)/fp$births[i]
      births.by.age <- births.by.age / adj
      }
    births.by.h.age <- ctapply(births.by.age, ag.idx[p.fert.idx], sum)
    births <- fp$srb[,i] * sum(births.by.h.age)
    if(i+AGE_START <= PROJ_YEARS)
      birthslag[,i+AGE_START-1] <- births
    
    if(exists('paedbasepop', where = fp)){
      pop5 <- popu5[pAGu5,,,i-1]
      popu5[-1,,,i] <- popu5[-(pAGu5),,,i-1]
      popu5[1,,1,i] <- births
      hivn_entrants <- popu15[pAGu15,,hivn.idx,i-1]
      hivp_entrants <- popu15[pAGu15,,hivp.idx,i-1]
      popu15[-1,,,i] <- popu15[-(pAGu15),,,i-1]
      popu15[1,,,i] <- pop5
      
      hivpop5 <- hivpopu5[,,pAGu5,,i-1]
      hivpopu5[,,-1,,i] <- hivpopu5[,,-pAGu5,,i-1]
      hivpopu15[,,-1,,i] <- hivpopu15[,,-pAGu15,,i-1]
      ## Splitting HIV + 5 year olds across CD4 count categories
      hivpopu15[,1,1,,i] <- 0.7143 * hivpop5[,1,]
      hivpopu15[,2,1,,i] <- (0.2857 * hivpop5[,1,]) + (0.6 * hivpop5[,2,])
      hivpopu15[,3,1,,i] <- (0.4 * hivpop5[,2,]) + (0.8333 * hivpop5[,3,])
      hivpopu15[,4,1,,i] <- (0.1667 * hivpop5[,3,]) + (0.7693 * hivpop5[,4,])
      hivpopu15[,5,1,,i] <- (0.231 * hivpop5[,4,]) + (0.889 * hivpop5[,5,])
      hivpopu15[,6,1,,i] <- (0.111 * hivpop5[,5,]) + hivpop5[,6,] + hivpop5[,7,]
      
      artpop5 <- artpopu5[,,pAGu5,,i-1]
      
      artpopu5[,,-1,,i] <- artpopu5[,,-pAGu5,,i-1]
      artpopu15[,,-1,,i] <- artpopu15[,,-pAGu15,,i-1]
      ## Splitting HIV + 5 year olds across CD4 count categories
      artpopu15[,1,1,,i] <- 0.7143 * artpop5[,1,]
      artpopu15[,2,1,,i] <- (0.2857 * artpop5[,1,]) + (0.6 * artpop5[,2,])
      artpopu15[,3,1,,i] <- (0.4 * artpop5[,2,]) + (0.8333 * artpop5[,3,])
      artpopu15[,4,1,,i] <- (0.1667 * artpop5[,3,]) + (0.7693 * artpop5[,4,])
      artpopu15[,5,1,,i] <- (0.231 * artpop5[,4,]) + (0.889 * artpop5[,5,])
      artpopu15[,6,1,,i] <- (0.111 * artpop5[,5,]) + artpop5[,6,] + artpop5[,7,]

      ## paediatric survival
      u5deaths <- sweep(popu5[,,,i], 1:2, (1 - fp$paed_Sx[1:5,,i]), '*')
      hiv.sx.prob <- 1 - u5deaths[,,hivp.idx]/popu5[,,hivp.idx,i]
      hiv.sx.prob[is.nan(hiv.sx.prob)] <- 1
      popu5[,,,i] <- popu5[,,,i] - u5deaths
      
      hivpopu5[,,,,i] <- sweep(hivpopu5[,,,,i], 3:4, hiv.sx.prob, "*")
      
      if(i > fp$tARTstart){
        artpopu5[,,,,i] <- sweep(artpopu5[,,,,i], 3:4, hiv.sx.prob, "*")
      }
      u15deaths <- sweep(popu15[,,,i], 1:2, (1 - fp$paed_Sx[6:15,,i]), '*')
      hiv.sx.prob <- 1 - u15deaths[,,hivp.idx]/popu15[,,hivp.idx,i]
      hiv.sx.prob[is.nan(hiv.sx.prob)] <- 1
      popu15[,,,i] <- popu15[,,,i] - u15deaths
      
      hivpopu15[,,,,i] <- sweep(hivpopu15[,,,,i], 3:4, hiv.sx.prob, "*")
      if(i > fp$tARTstart){
        artpopu15[,,,,i] <- sweep(artpopu15[,,,,i], 3:4, hiv.sx.prob, "*")
      }
      
      ## paediatric migration
      netmigsurv <- fp$paed_mig[,,i]*(1+fp$paed_Sx[,,i])/2
      mr.prob.u5 <- 1+netmigsurv[1:5] / rowSums(popu5[,,,i],,2)
      mr.prob.u5[!is.finite(mr.prob.u5)] <- 1

      mr.prob.u5[mr.prob.u5 < 0.5] <- 0.5
      hiv.mr.prob <- (mr.prob.u5 * popu5[,,hivp.idx,i]) /popu5[,,hivp.idx,i]
      hiv.mr.prob[!is.finite(hiv.mr.prob)] <- 1
      popu5[,,,i] <- sweep(popu5[,,,i], 1:2, mr.prob.u5, "*")
      
      hivpopu5[,,,,i] <- sweep(hivpopu5[,,,,i], 3:4, hiv.mr.prob, "*")
      if(i > fp$tARTstart){
        artpopu5[,,,,i] <- sweep(artpopu5[,,,,i], 3:4, hiv.mr.prob, "*")
      }
      mr.prob.u15 <- 1+netmigsurv[6:15] / rowSums(popu15[,,,i],,2)
      mr.prob.u15[!is.finite(mr.prob.u15)] <- 1

      mr.prob.u15[mr.prob.u15 < 0.5] <- 0.5
      hiv.mr.prob <- (mr.prob.u15 * popu15[,,hivp.idx,i]) /popu15[,,hivp.idx,i]
      hiv.mr.prob[!is.finite(hiv.mr.prob)] <- 1
      popu15[,,,i] <- sweep(popu15[,,,i], 1:2, mr.prob.u15, "*")
      
      hivpopu15[,,,,i] <- sweep(hivpopu15[,,,,i], 3:4, hiv.mr.prob, "*")
      if(i > fp$tARTstart){
        artpopu15[,,,,i] <- sweep(artpopu15[,,,,i], 3:4, hiv.mr.prob, "*")
      }
      
    }




    ## ########################## ##
    ##  Disease model simulation  ##
    ## ########################## ##
    ## events at dt timestep
    for(ii in seq_len(hiv_steps_per_year)){
      
      ts <- (i-2)/DT + ii

      grad <- array(0, c(hDS, hAG, NG))

      if(fp$eppmod != "directincid"){
        ## incidence

        ## calculate r(t)
        if(fp$eppmod %in% c("rtrend", "rtrend_rw"))
          rvec[ts] <- calc_rtrend_rt(fp$proj.steps[ts], fp, rvec[ts-1], prevlast, pop, i, ii)
        else
          rvec[ts] <- fp$rvec[ts]
        
        ## number of infections by age / sex
        if(exists("incidmod", where=fp) && fp$incidmod == "transm")
          infections.ts <- calc_infections_simpletransm(fp, pop, hivpop, artpop, i, ii, rvec[ts])
        else
          infections.ts <- calc_infections_eppspectrum(fp, pop, hivpop, artpop, i, ii, rvec[ts])

        incrate15to49.ts.out[ts] <- attr(infections.ts, "incrate15to49.ts")
        prev15to49.ts.out[ts] <- attr(infections.ts, "prevcurr")
        prevlast <- attr(infections.ts, "prevcurr")

        pop[,,hivn.idx,i] <- pop[,,hivn.idx,i] - DT*infections.ts
        pop[,,hivp.idx,i] <- pop[,,hivp.idx,i] + DT*infections.ts
        infections[,,i] <- infections[,,i] + DT*infections.ts

        grad <- grad + sweep(fp$cd4_initdist, 2:3, apply(infections.ts, 2, ctapply, ag.idx, sum), "*")
        incid15to49[i] <- incid15to49[i] + sum(DT*infections.ts[p.age15to49.idx,])
      }

      ## disease progression and mortality
      grad[-hDS,,] <- grad[-hDS,,] - fp$cd4_prog * hivpop[-hDS,,,i]  # remove cd4 stage progression (untreated)
      grad[-1,,] <- grad[-1,,] + fp$cd4_prog * hivpop[-hDS,,,i]      # add cd4 stage progression (untreated)

      if(fp$scale_cd4_mort == 1){
        cd4mx_scale <- hivpop[,,,i] / (hivpop[,,,i] + colSums(artpop[,,,,i]))
        cd4mx_scale[!is.finite(cd4mx_scale)] <- 1.0
        cd4_mort_ts <- fp$cd4_mort * cd4mx_scale
      }else if(exists('mortadjust', where = fp)){
        art.not.cov <- sum(hivpop[,,,i]) / sum(hivpop[,,,i] + colSums(artpop[,,,,i]))
        art.not.cov[!is.finite(art.not.cov)] <- 1.0
        if(fp$mort_scalar_type == 'exp'){
          scalar <- exp(-(fp$cd4_mort_adjust) * (1-art.not.cov))
          cd4_mort_ts <- fp$cd4_mort * scalar
        }else{
          cd4_mort_ts <- fp$cd4_mort_adjust * art.not.cov * fp$cd4_mort
        }
      } else{
        cd4_mort_ts <- fp$cd4_mort
      }
      ## Remove hivdeaths from pop
      hivdeaths.ts <- DT*(colSums(cd4_mort_ts * hivpop[,,,i]) + colSums(fp$art_mort * fp$artmx_timerr[ , i] * artpop[,,,,i],,2))
      calc.agdist <- function(x) {d <- x/rep(ctapply(x, ag.idx, sum), h.ag.span); d[is.na(d)] <- 0; d}
      hivdeaths_est.ts <- apply(hivdeaths.ts, 2, rep, h.ag.span) * apply(pop[,,hivp.idx,i], 2, calc.agdist)  # HIV deaths by single-year age

      if(exists('deaths_dt', where = fp)){
        ag.idx.5 <- c(unlist(lapply(1:13, function(x){rep(x, 5)})), 14)
        hivdeaths_est[,,i] <- hivdeaths_est[,,i] + apply(hivdeaths_est.ts, 2, ctapply, ag.idx.5, sum)
        # delta_ts <- fp$deaths_dt[,,ts] / hivdeaths_est.ts
        # delta_ts[!is.finite(delta_ts)] <- 1
        # deaths_dt_hivag <- apply(fp$deaths_dt[,,ts], 2, ctapply, ag.idx, sum)
        # delta_ts_hivag <- deaths_dt_hivag / hivdeaths.ts
        # delta_ts_hivag[!is.finite(delta_ts_hivag)] <- 1
        delta_ts <- 1
        delta_ts_hivag <- 1
      }else{
        delta_ts <- 1
        delta_ts_hivag <- 1
      }
      
      grad <- grad - sweep(cd4_mort_ts * hivpop[,,,i] ,2:3, delta_ts_hivag, '*')            # HIV mortality, untreated

      # hivdeaths_p.ts <- hivdeaths_est.ts * delta_ts
      hivdeaths_p.ts <- hivdeaths_est.ts
      pop[,,2,i] <- pop[,,2,i] - hivdeaths_p.ts
      hivdeaths[,,i] <- hivdeaths[,,i] + hivdeaths_p.ts

      ## ART initiation
      if(i >= fp$tARTstart) {

        gradART <- array(0, c(hTS, hDS, hAG, NG))

        ## progression and mortality
        gradART[1:2,,,] <- gradART[1:2,,,] - 2.0 * artpop[1:2,,,, i]      # remove ART duration progression (HARD CODED 6 months duration)
        gradART[2:3,,,] <- gradART[2:3,,,] + 2.0 * artpop[1:2,,,, i]      # add ART duration progression (HARD CODED 6 months duration)

        gradART <- gradART - sweep(fp$art_mort * fp$artmx_timerr[ , i] * artpop[,,,,i], 3:4, delta_ts_hivag, "*")   # ART mortality

        ## ART dropout
        ## remove proportion from all adult ART groups back to untreated pop
        grad <- grad + fp$art_dropout[i]*colSums(artpop[,,,,i])
        gradART <- gradART - fp$art_dropout[i]*artpop[,,,,i]

        ## calculate number eligible for ART
        artcd4_percelig <- 1 - (1-rep(0:1, times=c(fp$artcd4elig_idx[i]-1, hDS - fp$artcd4elig_idx[i]+1))) *
          (1-rep(c(0, fp$who34percelig), c(2, hDS-2))) *
          (1-rep(fp$specpop_percelig[i], hDS))

        art15plus.elig <- sweep(hivpop[,h.age15plus.idx,,i], 1, artcd4_percelig, "*")

        ## calculate pregnant women
        if(fp$pw_artelig[i]){
          births.dist <- sweep(fp$frr_cd4[,,i] * hivpop[,h.fert.idx,f.idx,i], 2,
                               births.by.h.age / (ctapply(pop[p.fert.idx, f.idx, hivn.idx, i], ag.idx[p.fert.idx], sum) + colSums(fp$frr_cd4[,,i] * hivpop[,h.fert.idx,f.idx,i]) + colSums(fp$frr_art[,,,i] * artpop[ ,,h.fert.idx,f.idx,i],,2)), "*")
          if(fp$artcd4elig_idx[i] > 1)
            art15plus.elig[1:(fp$artcd4elig_idx[i]-1),h.fert.idx-min(h.age15plus.idx)+1,f.idx] <- art15plus.elig[1:(fp$artcd4elig_idx[i]-1),h.fert.idx-min(h.age15plus.idx)+1,f.idx] + births.dist[1:(fp$artcd4elig_idx[i]-1),]
        }

        ## calculate number to initiate ART based on number or percentage

        artpop_curr_g <- colSums(artpop[,,h.age15plus.idx,,i],,3) + DT*colSums(gradART[,,h.age15plus.idx,],,3)
        artnum.ii <- c(0,0) # number on ART this ts
        if(DT*ii < 0.5){
          for(g in 1:2){
            if(!any(fp$art15plus_isperc[g,i-2:1])){  # both number
              artnum.ii[g] <- c(fp$art15plus_num[g,i-2:1] %*% c(1-(DT*ii+0.5), DT*ii+0.5))
            } else if(all(fp$art15plus_isperc[g,i-2:1])){  # both percentage
              artcov.ii <- c(fp$art15plus_num[g,i-2:1] %*% c(1-(DT*ii+0.5), DT*ii+0.5))
              artnum.ii[g] <- artcov.ii * (sum(art15plus.elig[,,g]) + artpop_curr_g[g])
            } else if(!fp$art15plus_isperc[g,i-2] & fp$art15plus_isperc[g,i-1]){ # transition number to percentage
              curr_coverage <- artpop_curr_g[g] / (sum(art15plus.elig[,,g]) + artpop_curr_g[g])
              artcov.ii <- curr_coverage + (fp$art15plus_num[g,i-1] - curr_coverage) * DT/(0.5-DT*(ii-1))
              artnum.ii[g] <- artcov.ii * (sum(art15plus.elig[,,g]) + artpop_curr_g[g])
            }
          }
        } else {
          for(g in 1:2){
            if(!any(fp$art15plus_isperc[g,i-1:0])){  # both number
              artnum.ii[g] <- c(fp$art15plus_num[g,i-1:0] %*% c(1-(DT*ii-0.5), DT*ii-0.5))
            } else if(all(fp$art15plus_isperc[g,i-1:0])) {  # both percentage
              artcov.ii <- c(fp$art15plus_num[g,i-1:0] %*% c(1-(DT*ii-0.5), DT*ii-0.5))
              artnum.ii[g] <- artcov.ii * (sum(art15plus.elig[,,g]) + artpop_curr_g[g])
            } else if(!fp$art15plus_isperc[g,i-1] & fp$art15plus_isperc[g,i]){  # transition number to percentage
              curr_coverage <- artpop_curr_g[g] / (sum(art15plus.elig[,,g]) + artpop_curr_g[g])
              artcov.ii <- curr_coverage + (fp$art15plus_num[g,i] - curr_coverage) * DT/(1.5-DT*(ii-1))
              artnum.ii[g] <- artcov.ii * (sum(art15plus.elig[,,g]) + artpop_curr_g[g])
            }
          }
        }

        artpop_curr_g <- colSums(artpop[,,h.age15plus.idx,,i],,3) + DT*colSums(gradART[,,h.age15plus.idx,],,3)
        art15plus.inits <- pmax(artnum.ii - artpop_curr_g, 0)

        ## calculate ART initiation distribution
        if(!fp$med_cd4init_input[i]){

          if(fp$art_alloc_method == 4L){ ## by lowest CD4
            
            ## Calculate proportion to be initiated in each CD4 category
            artinit <- array(0, dim(art15plus.elig))
            remain_artalloc <- art15plus.inits
            for(m in hDS:1){
              elig_hm <- colSums(art15plus.elig[m,,])
              init_prop <- ifelse(elig_hm == 0, elig_hm, pmin(1.0, remain_artalloc / elig_hm, na.rm=TRUE))
              artinit[m , , ] <- sweep(art15plus.elig[m,,], 2, init_prop, "*")
              remain_artalloc <- remain_artalloc - init_prop * elig_hm
            }

          } else {
            
            expect.mort.weight <- sweep(fp$cd4_mort[, h.age15plus.idx,], 3,
                                        colSums(art15plus.elig * fp$cd4_mort[, h.age15plus.idx,],,2), "/")          
            artinit.weight <- sweep(fp$art_alloc_mxweight * expect.mort.weight, 3, (1 - fp$art_alloc_mxweight)/colSums(art15plus.elig,,2), "+")
            artinit <- pmin(sweep(artinit.weight * art15plus.elig, 3, art15plus.inits, "*"),
                            art15plus.elig)
            
            ## Allocation by average mortality across CD4, trying to match Spectrum
            ## artelig_by_cd4 <- apply(art15plus.elig, c(1, 3), sum)
            ## expectmort_by_cd4 <- apply(art15plus.elig * fp$cd4_mort[, h.age15plus.idx,], c(1, 3), sum)
            
            ## artinit_dist <- fp$art_alloc_mxweight * sweep(artelig_by_cd4, 2, colSums(artelig_by_cd4), "/") +
            ##   (1 - fp$art_alloc_mxweight) * sweep(expectmort_by_cd4, 2, colSums(expectmort_by_cd4), "/")
            
            ## artinit_prob <- sweep(artinit_dist, 2, art15plus.inits, "*") / artelig_by_cd4
            ## artinit <- sweep(art15plus.elig, c(1, 3), artinit_prob, "*")
            ## artinit <- pmin(artinit, art15plus.elig, na.rm=TRUE)
          }

        } else {

          CD4_LOW_LIM <- c(500, 350, 250, 200, 100, 50, 0)
          CD4_UPP_LIM <- c(1000, 500, 350, 250, 200, 100, 50)

          medcd4_idx <- fp$med_cd4init_cat[i]

          medcat_propbelow <- (fp$median_cd4init[i] - CD4_LOW_LIM[medcd4_idx]) / (CD4_UPP_LIM[medcd4_idx] - CD4_LOW_LIM[medcd4_idx])

          elig_below <- colSums(art15plus.elig[medcd4_idx,,,drop=FALSE],,2) * medcat_propbelow
          if(medcd4_idx < hDS)
            elig_below <- elig_below + colSums(art15plus.elig[(medcd4_idx+1):hDS,,,drop=FALSE],,2)

          elig_above <- colSums(art15plus.elig[medcd4_idx,,,drop=FALSE],,2) * (1.0-medcat_propbelow)
          if(medcd4_idx > 1)
            elig_above <- elig_above + colSums(art15plus.elig[1:(medcd4_idx-1),,,drop=FALSE],,2)

          initprob_below <- pmin(art15plus.inits * 0.5 / elig_below, 1.0, na.rm=TRUE)
          initprob_above <- pmin(art15plus.inits * 0.5 / elig_above, 1.0, na.rm=TRUE)
          initprob_medcat <- initprob_below * medcat_propbelow + initprob_above * (1-medcat_propbelow)

          artinit <- array(0, dim=c(hDS, hAG, NG))

          if(medcd4_idx < hDS)
            artinit[(medcd4_idx+1):hDS,,] <- sweep(art15plus.elig[(medcd4_idx+1):hDS,,,drop=FALSE], 3, initprob_below, "*")
          artinit[medcd4_idx,,] <- sweep(art15plus.elig[medcd4_idx,,,drop=FALSE], 3, initprob_medcat, "*")
          if(medcd4_idx > 0)
            artinit[1:(medcd4_idx-1),,] <- sweep(art15plus.elig[1:(medcd4_idx-1),,,drop=FALSE], 3, initprob_above, "*")
        }

        artinit <- pmin(artinit, hivpop[ , , , i] + DT * grad)
      
        grad[ , h.age15plus.idx, ] <- grad[ , h.age15plus.idx, ] - artinit / DT
        gradART[1, , h.age15plus.idx, ] <- gradART[1, , h.age15plus.idx, ] + artinit / DT
        artpop[,,,, i] <- artpop[,,,, i] + DT * gradART
      }

      hivpop[,,,i] <- hivpop[,,,i] + DT * grad
    }    

    ## ## Code for calculating new infections once per year to match prevalence (like Spectrum)
    ## ## incidence
    ## prev.i <- sum(pop[p.age15to49.idx,,2,i]) / sum(pop[p.age15to49.idx,,,i]) # prevalence age 15 to 49
    ## incrate15to49.i <- (fp$prev15to49[i] - prev.i)/(1-prev.i)

    ## Direct incidence input
    if(fp$eppmod == "directincid"){
      if(fp$incidpopage == 0L) # incidence for 15-49 population
        p.incidpop.idx <- p.age15to49.idx
      else if(fp$incidpopage == 1L) # incidence for 15+ population
        p.incidpop.idx <- p.age15plus.idx
      incrate.i <- fp$incidinput[i]

      sexinc <- incrate.i*c(1, fp$incrr_sex[i])*sum(pop[p.incidpop.idx,,hivn.idx,i-1])/(sum(pop[p.incidpop.idx,m.idx,hivn.idx,i-1]) + fp$incrr_sex[i]*sum(pop[p.incidpop.idx, f.idx,hivn.idx,i-1]))
      agesex.inc <- sweep(fp$incrr_age[,,i], 2, sexinc/(colSums(pop[p.incidpop.idx,,hivn.idx,i-1] * fp$incrr_age[p.incidpop.idx,,i])/colSums(pop[p.incidpop.idx,,hivn.idx,i-1])), "*")
      infections[,,i] <- agesex.inc * pop[,,hivn.idx,i-1]
      
      pop[,,hivn.idx,i] <- pop[,,hivn.idx,i] - infections[,,i]
      pop[,,hivp.idx,i] <- pop[,,hivp.idx,i] + infections[,,i]
      
      hivpop[,,,i] <- hivpop[,,,i] + sweep(fp$cd4_initdist, 2:3, apply(infections[,,i], 2, ctapply, ag.idx, sum), "*")
      incid15to49[i] <- sum(infections[p.age15to49.idx,,i])
    }

    ## adjust population to match target population size
    if(exists("popadjust", where=fp) & fp$popadjust){
      popadj.prob[,,i] <- fp$targetpop[,,i] / rowSums(pop[,,,i],,2)
      hiv.popadj.prob <- apply(popadj.prob[,,i] * pop[,,2,i], 2, ctapply, ag.idx, sum) /  apply(pop[,,2,i], 2, ctapply, ag.idx, sum)
      hiv.popadj.prob[is.nan(hiv.popadj.prob)] <- 0

      pop[,,,i] <- sweep(pop[,,,i], 1:2, popadj.prob[,,i], "*")
      hivpop[,,,i] <- sweep(hivpop[,,,i], 2:3, hiv.popadj.prob, "*")
      if(i >= fp$tARTstart)
        artpop[,,,,i] <- sweep(artpop[,,,,i], 3:4, hiv.popadj.prob, "*")
      
      u5.adj <- fp$paedtargetpop[1:5,,i] / rowSums(popu5[,,,i],,2)
      popu5[,,,i] <- sweep(popu5[,,,i], 1:2, u5.adj, "*")
      hivpopu5[,,,,i] <- sweep(hivpopu5[,,,,i], 3:4, u5.adj, "*")
      if(i >= fp$tARTstart)
        artpopu5[,,,,i] <- sweep(artpopu5[,,,,i], 3:4, u5.adj, "*")

      u15.adj <- fp$paedtargetpop[6:15,,i] / rowSums(popu15[,,,i],,2)
      popu15[,,,i] <- sweep(popu15[,,,i], 1:2, u15.adj, "*")
      hivpopu15[,,,,i] <- sweep(hivpopu15[,,,,i], 3:4, u15.adj, "*")
      if(i >= fp$tARTstart)
        artpopu15[,,,,i] <- sweep(artpopu15[,,,,i], 3:4, u15.adj, "*")
      
    }

    ## prevalence among pregnant women
    hivn.byage <- ctapply(rowMeans(pop[p.fert.idx, f.idx, hivn.idx,i-1:0]), ag.idx[p.fert.idx], sum)
    hivp.byage <- rowMeans(hivpop[,h.fert.idx, f.idx,i-1:0],,2)
    artp.byage <- rowMeans(artpop[,,h.fert.idx, f.idx,i-1:0],,3)
    
    pregprev <- sum(births.by.h.age * (1 - hivn.byage / (hivn.byage + colSums(fp$frr_cd4[,,i] * hivp.byage) + colSums(fp$frr_art[,,,i] * artp.byage,,2)))) / sum(births.by.age)
    if(i+AGE_START <= PROJ_YEARS & !exists('paedbasepop', where = fp))
      pregprevlag[i+AGE_START-1] <- pregprev
    if(exists('paedbasepop', where = fp) & pregprev > 0){
      ## calculate vertical transmission
      ## MTC transmission is weighted avg of those receiving PMTCT and not treated
      ## Calculate percent of women by treatment option
      ## Default is no treatment
      pregprev.num <- pregprev * sum(births)
      needPMTCT <- pregprev.num
      treat.opt <- list('no_proph' = 1)
      
      for(n in prenat.opt){
        treat.opt[[n]] <- 0
      }
      sum3 <- as.numeric(sum(unlist(ifelse(fp$pmtct_isperc[fp$pmtct_isperc$year == (proj_start + i - 1),prenat.opt], fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1),prenat.opt]/100 * needPMTCT, fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1),prenat.opt]))))
      ## set sum3 equal to total need, unless sum3 > need
      sum3 <- max(sum3, needPMTCT)
      on.treat <- 0
      for(n in prenat.opt){
        if(fp$pmtct_isperc[fp$pmtct_isperc$year == (proj_start + i - 1),n]){
          treat.opt[[n]] <- fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), n] / 100
        }else{
          treat.opt[[n]] <- ifelse(sum3 == 0, 0, fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), n] / sum3)
        }
        on.treat <- on.treat + treat.opt[[n]]
      }
      if(on.treat > 1){on.treat <- 1}
      treat.opt[['no_proph']] <- 1 - on.treat
      
      ## proportion of hiv+ preg women by cd4

      proplt200 <- (sum(births.by.h.age * (1 - hivn.byage / (hivn.byage + colSums(fp$frr_cd4[5:7,,i] * hivp.byage[5:7,]) + colSums(fp$frr_art[,5:7,,i] * artp.byage[,5:7,],,2)))) / sum(births.by.age)) / pregprev
      prop200to350 <- (sum(births.by.h.age * (1 - hivn.byage / (hivn.byage + colSums(fp$frr_cd4[3:4,,i] * hivp.byage[3:4,]) + colSums(fp$frr_art[,3:4,,i] * artp.byage[,3:4,],,2)))) / sum(births.by.age)) / pregprev
      propgt350 <- (sum(births.by.h.age * (1 - hivn.byage / (hivn.byage + colSums(fp$frr_cd4[1:2,,i] * hivp.byage[1:2,]) + colSums(fp$frr_art[,1:2,,i] * artp.byage[,1:2,],,2)))) / sum(births.by.age)) / pregprev
      
      ## adjust transmission rates for opt a and opt b in cd4 lt 350
      if(treat.opt[['prenat_optionA']] + treat.opt[['prenat_optionB']] > (propgt350)){
        if(propgt350 <= 0){
          excess.ratio <- 0
        }else{
            excess.ratio <- (treat.opt[['prenat_optionA']] + treat.opt[['prenat_optionB']]) /propgt350 - 1
            optA.trans.rate <- (fp$MTCtrans[fp$MTCtrans$regimen == 'Option_A', 'perinatal_trans_pct']/100) * (1 + excess.ratio)
            optB.trans.rate <- (fp$MTCtrans[fp$MTCtrans$regimen == 'Option_B', 'perinatal_trans_pct']/100) * (1 + excess.ratio)
          }
      }else{
        optA.trans.rate <- fp$MTCtrans[fp$MTCtrans$regimen == 'Option_A', 'perinatal_trans_pct']/100
        optB.trans.rate <- fp$MTCtrans[fp$MTCtrans$regimen == 'Option_B', 'perinatal_trans_pct']/100
      }
      ## on treatment
      PTR <- treat.opt[['prenat_optionA']] * optA.trans.rate +
                treat.opt[['prenat_optionB']]* optB.trans.rate +
                treat.opt[['dualARV']] * (fp$MTCtrans[fp$MTCtrans$regimen == 'WHO_06_dual_ARV', 'perinatal_trans_pct'] / 100) +
                treat.opt[['singleDoseNevir']] * (fp$MTCtrans[fp$MTCtrans$regimen == 'single_dose_nevriapine', 'perinatal_trans_pct'] / 100) +
                treat.opt[['tripleARTbefPreg']] * (fp$MTCtrans[fp$MTCtrans$regimen == 'ART' & fp$MTCtrans$definition == 'start_pre_preg', 'perinatal_trans_pct'] / 100) +
                treat.opt[['tripleARTdurPreg']] * (fp$MTCtrans[fp$MTCtrans$regimen == 'ART' & fp$MTCtrans$definition == 'start_dur_preg', 'perinatal_trans_pct'] / 100)
      ## not on treatment
      ## incident infections
      prop.incident.infections <- min(sum(infections[1:35,f.idx,i])/sum(hivpop[,h.fert.idx,f.idx,i]), propgt350)
      PTR <- PTR + (prop.incident.infections * (fp$MTCtrans[fp$MTCtrans$regimen == 'no_prophylaxis' & fp$MTCtrans$definition == 'incident_infection', 'perinatal_trans_pct']/100) * treat.opt[['no_proph']])
      propgt350 <- max(propgt350 - prop.incident.infections, 0)
      PTR <- PTR + treat.opt[['no_proph']] * ((proplt200 * (fp$MTCtrans[fp$MTCtrans$regimen == 'no_prophylaxis' & fp$MTCtrans$definition == 'exisiting_LT200CD4', 'perinatal_trans_pct'] / 100)) +    
              (prop200to350 * (fp$MTCtrans[fp$MTCtrans$regimen == 'no_prophylaxis' & fp$MTCtrans$definition == 'exisiting_200to350CD4', 'perinatal_trans_pct'] / 100)) + 
              (propgt350 * (fp$MTCtrans[fp$MTCtrans$regimen == 'no_prophylaxis' & fp$MTCtrans$definition == 'exisiting_GT340CD4', 'perinatal_trans_pct'] / 100)))
      ## HIV births is birth prevalence (perinatal transmission)
      hiv.births <- max(0, pregprev.num * PTR)
      prop.trans[1, i] <- hiv.births
      ## BF transmission

      treat.opt <- list()
      for(n in c(prenat.opt, 'postnat_optionA', 'postnat_optionB')){
        treat.opt[[n]] <- 0
      }
      sum3 <- as.numeric(sum(unlist(ifelse(fp$pmtct_isperc[fp$pmtct_isperc$year == (proj_start + i - 1),prenat.opt], fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1),prenat.opt]/100 * needPMTCT, fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1),prenat.opt]))))
      ## set sum3 equal to total need, unless sum3 > need
      sum3 <- max(sum3, needPMTCT)
      
      ##triple ART
      if(fp$pmtct_isperc[fp$pmtct_isperc$year == (proj_start + i - 1), 'tripleARTdurPreg']){
        tripleARTdurPregNum <- fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), 'tripleARTdurPreg'] * sum3 / 100
      }else{
        tripleARTdurPregNum <- fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), 'tripleARTdurPreg']
      }
      if(fp$pmtct_isperc[fp$pmtct_isperc$year == (proj_start + i - 1), 'tripleARTbefPreg']){
        tripleARTbefPregNum <- fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), 'tripleARTbefPreg'] * sum3 / 100
      }else{
        tripleARTbefPregNum <- fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), 'tripleARTbefPreg']
      }
      if((sum3 - tripleARTbefPregNum - tripleARTdurPregNum) > 0){
        treat.opt[['postnat_optionA']] <- min(1, fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), 'postnat_optionA'] / (sum3 - tripleARTbefPregNum - tripleARTdurPregNum))
        treat.opt[['postnat_optionB']] <- min(1, fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), 'postnat_optionB'] / (sum3 - tripleARTbefPregNum - tripleARTdurPregNum))
      }
      on.treat <- treat.opt[['postnat_optionA']] + treat.opt[['postnat_optionB']]
      for(n in prenat.opt){
        if(fp$pmtct_isperc[fp$pmtct_isperc$year == (proj_start + i - 1),n]){
          treat.opt[[n]] <- fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), n] / 100
        }else{
          treat.opt[[n]] <- ifelse(sum3 == 0, 0, fp$pmtct_num[fp$pmtct_num$year == (proj_start + i - 1), n] / sum3)
        }
        on.treat <- on.treat + treat.opt[[n]]
      }
      
      if(on.treat > 1){
        on.treat <- 1
        }
      treat.opt[['no_proph']] <- 1 - on.treat
      
      ## breastfeeding transmission
      BFTR <- calcBFtransmissions(1,3,i, treat.opt, fp, artpop, artp.byage, proplt200, prop200to350, propgt350, prop.incident.infections)
      
      newInfFromBFLT6 <- as.numeric((pregprev.num - hiv.births) * BFTR)
      prop.trans[2, i] <- newInfFromBFLT6
      cumNewInfFromBF <- newInfFromBFLT6
      
      BFTR <- calcBFtransmissions(4, 6, i,treat.opt, fp, artpop, artp.byage, proplt200, prop200to350, propgt350, prop.incident.infections)
      newInfFromBF6TO12 <- as.numeric((pregprev.num - hiv.births - newInfFromBFLT6) * BFTR)
      prop.trans[3, i] <- newInfFromBF6TO12
      cumNewInfFromBF <- cumNewInfFromBF + newInfFromBF6TO12
      
      
      ## perinatal infections
      hivpopu5[4,,1,m.idx,i] <- hiv.births * as.numeric(fp$srb[m.idx,i]) * fp$paed_distnewinf * fp$paed_Sx[1,m.idx,i]
      hivpopu5[4,,1,f.idx,i] <- hiv.births * fp$srb[f.idx,i] * fp$paed_distnewinf * fp$paed_Sx[1,f.idx,i]
      hivbirths.out[m.idx,i] <-  as.numeric(hiv.births) * as.numeric(fp$srb[m.idx,i])
      hivbirths.out[f.idx,i] <-  as.numeric(hiv.births) * as.numeric(fp$srb[f.idx,i])

      ## bf infections
      hivpopu5[1,,1,m.idx,i] <- newInfFromBFLT6 * fp$srb[m.idx,i] * fp$paed_distnewinf
      hivpopu5[1,,1,f.idx,i] <- newInfFromBFLT6 * fp$srb[f.idx,i] * fp$paed_distnewinf
      hivpopu5[2,,1,m.idx,i] <- newInfFromBF6TO12 * fp$srb[m.idx,i] * fp$paed_distnewinf
      hivpopu5[2,,1,f.idx,i] <- newInfFromBF6TO12 * fp$srb[f.idx,i] * fp$paed_distnewinf
      u15infections[1, m.idx, i] <- sum(hivpopu5[,,1,m.idx,i])
      u15infections[1, f.idx, i] <- sum(hivpopu5[,,1,f.idx,i])
      
      ## remove hiv+ from pop
      popu5[1, m.idx, hivn.idx, i] <- popu5[1, m.idx, hivn.idx, i] - sum(hivpopu5[,,1,m.idx,i])
      popu5[1, m.idx, hivp.idx, i] <- popu5[1,m.idx,hivp.idx,i] + sum(hivpopu5[,,1,m.idx,i])
      popu5[1, f.idx, hivn.idx, i] <- popu5[1, f.idx, hivn.idx, i] - sum(hivpopu5[,,1,f.idx,i])
      popu5[1, f.idx, hivp.idx, i] <- popu5[1,f.idx,hivp.idx,i] + sum(hivpopu5[,,1,f.idx,i])
   
      ## hiv-free survival for under-1 hiv+ population
      u1deaths <- popu5[1,,hivp.idx,i] * (1 - fp$paed_Sx[1,,i])
      popu5[1,,hivp.idx,i] <- popu5[1,,hivp.idx,i] - u1deaths
      hiv.sx.prob <- fp$paed_Sx[1,,i]
      hivpopu5[,,1,,i] <- sweep(hivpopu5[,,1,,i], 3, hiv.sx.prob, "*")
      netmigsurv <- fp$paed_mig[1,,i]*(1+fp$paed_Sx[1,,i])/2
      mr.prob.u1 <- 1+netmigsurv / rowSums(popu5[1,,,i])
      mr.prob.u1[!is.finite(mr.prob.u1)] <- 1
      hiv.mr.prob <- mr.prob.u1
      popu5[1,,hivp.idx,i] <- popu5[1,,hivp.idx,i] * mr.prob.u1
      hivpopu5[,,1,,i] <- sweep(hivpopu5[,,1,,i], 3, hiv.mr.prob, "*")
      
      

      
      ## age 1, bf12 transmission
      percentExposed = (pregprev.num - hiv.births - cumNewInfFromBF) / sum(births)
      BFTR = calcBFtransmissions(7, 12, i,treat.opt, fp, artpop, artp.byage, proplt200, prop200to350, propgt350, prop.incident.infections)
      m.bf12 <- popu5[2,m.idx,hivn.idx,i] * percentExposed * BFTR * fp$paed_distnewinf
      if(length(m.bf12) > 1){
        hivpopu5[3,,2,m.idx,i] <- m.bf12
      }
      popu5[2,m.idx,hivn.idx,i] <- popu5[2,m.idx,hivn.idx,i] - sum(hivpopu5[3,,2,m.idx,i])
      popu5[2,m.idx,hivp.idx,i] <- popu5[2,m.idx,hivp.idx,i] + sum(hivpopu5[3,,2,m.idx,i])
      f.bf12 <- popu5[2,f.idx,hivn.idx,i] * fp$paed_distnewinf * percentExposed * BFTR
      if(length(f.bf12) > 1){
        hivpopu5[3,,2,f.idx,i] <- f.bf12
      }
      popu5[2,f.idx,hivn.idx,i] <- popu5[2,f.idx,hivn.idx,i] - sum(hivpopu5[3,,2,f.idx,i])  
      popu5[2,f.idx,hivp.idx,i] <- popu5[2,f.idx,hivp.idx,i] + sum(hivpopu5[3,,2,f.idx,i])
      cumNewInfFromBF <- cumNewInfFromBF + sum(hivpopu5[3,,2,,i])  
      u15infections[2, m.idx, i] <- sum(m.bf12)
      u15infections[2, f.idx, i] <- sum(f.bf12)
    
      ## age 2, bf12 transmission
      percentExposed <- percentExposed * (1 - BFTR)
      BFTR = calcBFtransmissions(13, 18, i,treat.opt, fp, artpop, artp.byage, proplt200, prop200to350, propgt350, prop.incident.infections)
      m.bf12 <- popu5[3,m.idx,hivn.idx,i] * fp$paed_distnewinf * percentExposed * BFTR
      if(length(m.bf12) > 1){
        hivpopu5[3,,3,m.idx,i] <- hivpopu5[3,,3,m.idx,i] + m.bf12
      }
      popu5[3,m.idx,hivn.idx,i] <- popu5[3,m.idx,hivn.idx,i] - sum(m.bf12)
      popu5[3,m.idx,hivp.idx,i] <- popu5[3,m.idx,hivp.idx,i] + sum(m.bf12)
      f.bf12 <- popu5[3,f.idx,hivn.idx,i] * fp$paed_distnewinf * percentExposed * BFTR
      if(length(f.bf12) > 1){
        hivpopu5[3,,3,f.idx,i] <-hivpopu5[3,,3,f.idx,i] +  f.bf12
      }
      popu5[3,f.idx,hivn.idx,i] <- popu5[3,f.idx,hivn.idx,i] - sum(f.bf12)  
      popu5[3,f.idx,hivp.idx,i] <- popu5[3,f.idx,hivp.idx,i] + sum(f.bf12)
      cumNewInfFromBF <- cumNewInfFromBF + sum(hivpopu5[3,,3,,i])        
      u15infections[3, m.idx, i] <- sum(m.bf12)
      u15infections[3, f.idx, i] <- sum(f.bf12)
    
      #Calculating need for child ART
      unmetNeed <- 0
      childEligibilityAge <- unique(fp$paed_arteligibility[,c('year', 'age_below_all_treat_mos')])$age_below_all_treat_mos
      for (age in 0:4){
        if((age + 1) * 12 > childEligibilityAge[i]){
          CD4elig <- as.character(min(fp$paed_arteligibility[fp$paed_arteligibility$year == (proj_start + i - 1) & fp$paed_arteligibility$age_start <= age, 'cd4_pct_thresh']))
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          unmetNeed <- unmetNeed + sum(hivpopu5[,bin:7, age + 1, , i])
        }else{
          unmetNeed <- unmetNeed + sum(hivpopu5[,, age + 1, , i])
        }
      }
      for (age in 5:14){
        if((age + 1) * 12 > childEligibilityAge[i]){
          CD4elig <- as.character(min(fp$paed_arteligibility[fp$paed_arteligibility$year == (proj_start + i - 1) & fp$paed_arteligibility$age_start <= age, 'cd4_count_thresh']))
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          unmetNeed <- unmetNeed + sum(hivpopu15[,bin:6, age - 4, , i])
        }else{
          unmetNeed <- unmetNeed + sum(hivpopu15[,, age - 4, , i])
        }
      }
      
      onFLART <- sum(artpopu5[,,,,i]) + sum(artpopu15[,,,,i])
      needForFLART <- unmetNeed + onFLART   
      
      if(fp$artpaed_isperc[i - 1]){
        ARTlastYear <- needForFLART * as.numeric(fp$artpaed_num[i - 1]) / 100
      }else{
        ARTlastYear <- as.numeric(fp$artpaed_num[i - 1])
      }
      if(fp$artpaed_isperc[i]){
        ARTthisYear <- needForFLART * as.numeric(fp$artpaed_num[i]) / 100
      }else{
        ARTthisYear <- as.numeric(fp$artpaed_num[i])
      }
      
      newFLART <-((ARTthisYear + ARTlastYear) / 2 ) - onFLART
      if(newFLART < 0){
        newFLART <- 0
      }
      #Increase number starting ART to account for those who will die in the first year
      #They will be exposed to 1/2 year of mortality risk
      v1 <- sum(artpopu5[,,,,i] * fp$art_mort_u5)
      v2 <- sum(artpopu15[,,,,i] * fp$art_mort_u15)
      onARTDeaths <- (v1 + v2) / 2
      
      
      print(v1)
      print(v2)
      print(i)
      newFLART <- newFLART + onARTDeaths
  
      print(needForFLART)
      print(onFLART)
      print(newFLART)
      print(onARTDeaths)
      if(i < fp$tARTstart){
        v1 <- 0
        v2 <- 0
        newFLART <- 0
        onARTDeaths <- 0
      }
      if(needForFLART < (onFLART + newFLART)){
        needForFLART <- onFLART + newFLART
      }
      #Distribute according to IeDEA data
      temp <- 0
      for(age in 0:4){
        if((age + 1) * 12 > childEligibilityAge[i]){
          CD4elig <- as.character(min(fp$paed_arteligibility[fp$paed_arteligibility$year == (proj_start + i - 1) & fp$paed_arteligibility$age_start <= age, 'cd4_pct_thresh']))
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          temp <- temp + (sum(hivpopu5[,bin:7, age + 1, , i]) * fp$paed_artdist[i, age +1])
        }else{
          temp <- temp + (sum(hivpopu5[,, age + 1, , i]) * fp$paed_artdist[i, age +1])
        }
      }
      for(age in 5:14){
        if((age + 1) * 12 > childEligibilityAge[i]){
          CD4elig <- as.character(min(fp$paed_arteligibility[fp$paed_arteligibility$year == (proj_start + i - 1) & fp$paed_arteligibility$age_start <= age, 'cd4_count_thresh']))
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          temp <- temp + (sum(hivpopu15[,bin:6, age - 4, , i]) * fp$paed_artdist[i, age +1])
        }else{
          temp <- temp + (sum(hivpopu15[,, age - 4, , i]) * fp$paed_artdist[i, age +1])
        }
      }
      adj <- ifelse(temp > 0, newFLART/temp, 1)
      ## Actually distribute ART
      newChildARTu5 <- array(0, c(4, 7, 5, 2))
      newChildARTu15 <- array(0, c(4, 6, 10, 2))
      for(age in 0:4){
        if((age + 1) * 12 > childEligibilityAge[i]){
          CD4elig <- as.character(min(fp$paed_arteligibility[fp$paed_arteligibility$year == (proj_start + i - 1) & fp$paed_arteligibility$age_start <= age, 'cd4_pct_thresh']))
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          newChildARTu5[,bin:7,age + 1,] <- min(1, adj *  fp$paed_artdist[i, age +1]) * hivpopu5[,bin:7, age + 1, , i]
        }else{
          newChildARTu5[,,age + 1,] <- min(1, adj * fp$paed_artdist[i, age +1]) * hivpopu5[,, age + 1, , i]
        }
      }
      for(age in 5:14){
        if((age + 1) * 12 > childEligibilityAge[i]){
          CD4elig <- as.character(min(fp$paed_arteligibility[fp$paed_arteligibility$year == (proj_start + i - 1) & fp$paed_arteligibility$age_start <= age, 'cd4_count_thresh']))
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          newChildARTu15[,bin:6,age - 4,] <- min(2, adj  * fp$paed_artdist[i, age +1]) *  hivpopu15[,bin:6, age - 4, , i]
        }else{
          newChildARTu15[,,age - 4,] <- min(1, adj * fp$paed_artdist[i, age +1]) * hivpopu15[,, age - 4, , i]
        }
      }
      
      ## Get CTX coverage
      ## off-ART u5 plus eligible for ART 5-15
      posu5pop <- sum(hivpopu5[,,,,i])
      eligible5to15 <- 0
      for(age in 5:14){
        if((age + 1) * 12 > childEligibilityAge[i]){
          CD4elig <- as.character(min(fp$paed_arteligibility[fp$paed_arteligibility$year == (proj_start + i - 1) & fp$paed_arteligibility$age_start <= age, 'cd4_count_thresh']))
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          eligible5to15 <- eligible5to15 + sum(hivpopu15[,bin:6, age - 4, , i]) 
        }else{
          bin <- get_paed_cd4bin(CD4elig, age, fp$ss)
          eligible5to15 <- eligible5to15 + sum(hivpopu15[,bin:6, age - 4, , i])
        }
      }
      needCTX <- posu5pop + eligible5to15
      if(needCTX > 0){
        if(fp$cotrim_isperc[i]){
          CTXcoverage <- fp$cotrim_num[i]
        }else{
          CTXcoverage <- min(1, fp$cotrim_num[i]/needCTX)
        }
      }else{
        CTXcoverage <- 0
      }
      CTXcoverage <- min(CTXcoverage, 1)
      ## Calculate child HIV/AIDS deaths and progression
      ## U15 deaths, with accounting for cotrim
      u15hivdeaths <- (1 - as.numeric((fp$childCTXeffect[fp$childCTXeffect$ART_status == 'noART', 'hivmort_reduction']) * CTXcoverage)) * hivpopu15[,,,,i] * fp$cd4_mort_u15
      ## U15 progression
      u15prog <- array(0, c(4, 5, 10, 2))
      for(tt in length(dimnames(hivpopu15)$transmission)){
        u15prog[tt,,,] <- fp$prog_u15* hivpopu15[tt,1:5,,,i]
      }

      ## exits
      u15prog.temp <- array(0, c(4,6,10,2))
      u15prog.temp[,1:5,,] <- u15prog


      if(any(hivpopu15[,,,,i] < (newChildARTu15 + u15hivdeaths + u15prog.temp))){
        lt0 <- (hivpopu15[,,,,i] - (newChildARTu15 + u15hivdeaths + u15prog.temp)) < 0
        if(sum(newChildARTu15[lt0]) > 0){
          newChildARTu15[lt0] <- (newChildARTu15 + (hivpopu15[,,,,i] - (newChildARTu15 + u15hivdeaths + u15prog.temp)))[lt0]
        }else{
          u15hivdeaths[lt0] <- (u15hivdeaths + (hivpopu15[,,,,i] - (newChildARTu15 + u15hivdeaths  + u15prog.temp)))[lt0]
        }
      }
      hivpopu15[,,,,i] <- hivpopu15[,,,,i] - newChildARTu15 - u15hivdeaths
      hivpopu15[,1:5,,,i] <- hivpopu15[,1:5,,,i] - u15prog
      
      ## entrants
      hivpopu15[,2:6,,,i] <- hivpopu15[,2:6,,,i] + u15prog
      
      ## U5 deaths, with accounting for cotrim
      u5hivdeaths <- (1 - (as.numeric(fp$childCTXeffect[fp$childCTXeffect$ART_status == 'noART', 'hivmort_reduction']) * CTXcoverage)) * hivpopu5[,,,,i] * fp$cd4_mort_u5
      ## U5 progression
      u5prog <- array(0, c(4, 6, 5, 2))
      for(tt in length(dimnames(hivpopu5)$transmission)){
        u5prog[tt,,,] <- fp$prog_u5* hivpopu5[tt,1:6,,,i]
      }
      ## exits
      u5prog.temp <- array(0, c(4,7,5,2))
      u5prog.temp[,1:6,,] <- u5prog
      if(any(hivpopu5[,,,,i] < (newChildARTu5 + u5hivdeaths + u5prog.temp))){
        lt0 <- (hivpopu5[,,,,i] - (newChildARTu5 + u5hivdeaths + u5prog.temp)) < 0
        if(sum(newChildARTu5[lt0]) > 0){
          newChildARTu5[lt0] <- (newChildARTu5 + (hivpopu5[,,,,i] - (newChildARTu5 + u5hivdeaths  + u5prog.temp)))[lt0]
        }else{
          u5hivdeaths[lt0] <- (u5hivdeaths + (hivpopu5[,,,,i] - (newChildARTu5 + u5hivdeaths  + u5prog.temp)))[lt0]
        }
      }
      hivpopu5[,,,,i] <- hivpopu5[,,,,i] - newChildARTu5 - u5hivdeaths
      hivpopu5[,1:6,,,i] <- hivpopu5[,1:6,,,i] - u5prog

      ## entrants
      hivpopu5[,2:7,,,i] <- hivpopu5[,2:7,,,i] + u5prog

      
      ## Those on ART, under 5
      entrants6Mu5 <- apply(newChildARTu5, 2:4, sum)
      ## Entrants are subject to mortality
      ## v2 is deaths in those entering LT6M group
      v2 <- (1 - (as.numeric(fp$childCTXeffect[fp$childCTXeffect$ART_status == 'withART', 'hivmort_reduction']) * CTXcoverage)) * entrants6Mu5 * ((fp$art_mort_u5[1,,,] + fp$art_mort_u5[2,,,]) / 2)
      entrants6Mu5 <- entrants6Mu5 - v2
      ## v1 is deaths in those entering GT12M group
      v1 <- (1 - (as.numeric(fp$childCTXeffect[fp$childCTXeffect$ART_status == 'withART', 'hivmort_reduction']) * CTXcoverage)) * artpopu5[1,,,,i] * ((fp$art_mort_u5[1,,,] + fp$art_mort_u5[2,,,]) / 2)
      entrants12M <- artpopu5[1,,,,i] - v1
      u5hivdeaths <- apply(u5hivdeaths, 2:4, sum)
      u5hivdeaths <- u5hivdeaths + v1 + v2
      ## Add new patients to ART LT6M
      artpopu5[1,,,,i] <- entrants6Mu5
      ## Those dying on ART in GT12M
      exits12M <- (1 - (as.numeric(fp$childCTXeffect[fp$childCTXeffect$ART_status == 'withART', 'hivmort_reduction']) * CTXcoverage)) * artpopu5[3,,,,i] * fp$art_mort_u5[3,,,]
      u5hivdeaths <- u5hivdeaths + exits12M
      artpopu5[3,,,,i] <- artpopu5[3,,,,i] + entrants12M - exits12M
      
      ## Those on ART, under 15
      entrants6Mu15 <- apply(newChildARTu15, 2:4, sum)
      ## Entrants are subject to mortality
      ## v2 is deaths in those entering LT6M group
      v2 <- (1 - as.numeric((fp$childCTXeffect[fp$childCTXeffect$ART_status == 'withART', 'hivmort_reduction']) * CTXcoverage)) * entrants6Mu15 * ((fp$art_mort_u15[1,,,] + fp$art_mort_u15[2,,,]) / 2)
      entrants6Mu15 <- entrants6Mu15 - v2
      ## v1 is deaths in those entering GT12M group
      v1 <- (1 - as.numeric((fp$childCTXeffect[fp$childCTXeffect$ART_status == 'withART', 'hivmort_reduction']) * CTXcoverage)) * artpopu15[1,,,,i] * ((fp$art_mort_u15[1,,,] + fp$art_mort_u15[2,,,]) / 2)
      entrants12M <- artpopu15[1,,,,i] - v1
      u15hivdeaths <- apply(u15hivdeaths, 2:4, sum)
      u15hivdeaths <- u15hivdeaths + v1 + v2
      ## Add new patients to ART LT6M
      artpopu15[1,,,,i] <- entrants6Mu15
      ## Those dying on ART in GT12M
      exits12M <- as.numeric((1 - (fp$childCTXeffect[fp$childCTXeffect$ART_status == 'withART', 'hivmort_reduction'] * CTXcoverage))) * artpopu15[3,,,,i] * fp$art_mort_u15[3,,,]
      u15hivdeaths <- u15hivdeaths + exits12M
      artpopu15[3,,,,i] <- artpopu15[3,,,,i] + entrants12M - exits12M
      
      popu15[,,2,i] <- popu15[,,2,i] - apply(u15hivdeaths, 2:3, 'sum')
      popu5[,,2,i] <- popu5[,,2,i] - apply(u5hivdeaths, 2:3, 'sum')
      
      ## just in case
      hivpopu5[hivpopu5 < 0] <- 0
      hivpopu15[hivpopu15 < 0] <- 0
      popu5[popu5 < 0] <- 0
      popu15[popu15 < 0] <- 0
      artpopu5[artpopu5 < 0] <- 0
      artpopu15[artpopu15 < 0] <- 0
      
      u15hivdeaths.out[1:5,,i] <- apply(u5hivdeaths, 2:3, sum)
      u15hivdeaths.out[6:15,,i] <- apply(u15hivdeaths, 2:3, sum)
      # u15deaths.out[1:5,,i] <- apply(u5deaths, 1:2, sum)
      # u15deaths.out[6:15,,i] <- apply(u15deaths, 1:2, sum)

     }
    
    u15deaths.out[1:5,,i] <- apply(u5deaths, 1:2, sum)
    u15deaths.out[6:15,,i] <- apply(u15deaths, 1:2, sum)
    ## prevalence and incidence 15 to 49
    prev15to49[i] <- sum(pop[p.age15to49.idx,,hivp.idx,i]) / sum(pop[p.age15to49.idx,,,i])
    incid15to49[i] <- sum(incid15to49[i]) / sum(pop[p.age15to49.idx,,hivn.idx,i-1])
  }
  
  if(exists('paedbasepop', where = fp)){
    attr(pop, 'hivpopu5') <- apply(hivpopu5, 2:5, sum)
    attr(pop, 'hivpopu15') <- apply(hivpopu15, 2:5, sum)
    attr(pop, 'artpopu5') <-  apply(artpopu5, 2:5, sum)
    attr(pop, 'artpopu15') <-  apply(artpopu15, 2:5, sum)
    attr(pop, 'hivdeathsu15') <- u15hivdeaths.out
    attr(pop, 'deathsu15') <- u15deaths.out
    attr(pop, 'birthprev') <- hivbirths.out
    attr(pop, 'popu5') <- popu5
    attr(pop, 'popu15') <- popu15
    attr(pop, 'infectionsu15') <- u15infections
    attr(pop, 'under1incidence') <- prop.trans
  }


  attr(pop, "prev15to49") <- prev15to49
  attr(pop, "incid15to49") <- incid15to49
  attr(pop, "sexinc") <- sexinc15to49out
  attr(pop, "hivpop") <- hivpop
  attr(pop, "artpop") <- artpop

  attr(pop, "infections") <- infections
  attr(pop, "hivdeaths") <- hivdeaths
  attr(pop, "natdeaths") <- natdeaths
  attr(pop, "popadjust") <- popadj.prob

  attr(pop, "pregprevlag") <- pregprevlag

  if(fp$eppmod != "directincid"){
    attr(pop, "incrate15to49_ts") <- incrate15to49.ts.out
    attr(pop, "prev15to49_ts") <- prev15to49.ts.out
  }

  attr(pop, "entrantprev") <- entrant_prev_out
  attr(pop, "hivp_entrants") <- hivp_entrants_out
  class(pop) <- "spec"
  return(pop)
}
