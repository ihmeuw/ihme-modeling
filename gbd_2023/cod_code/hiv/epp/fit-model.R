fitmod <- function(obj, ..., B0 = 1e5, B = 1e4, B.re = 3000, number_k = 500, D=0, opt_iter=0){
  
  ## ... : updates to fixed parameters (fp) object to specify fitting options
  
  likdat <<- attr(obj, 'likdat')  # put in global environment for IMIS functions.
  fp <<- attr(obj, 'eppfp')
  fp <<- update(fp, ...)
  
  # ## If IMIS fails, start again
  fit <- try(stop(""), TRUE)
  while(inherits(fit, "try-error")){
    start.time <- proc.time()
    fit <- try(IMIS(B0, B, B.re, number_k, D, opt_iter, fp=fp, likdat=likdat))
    fit.time <- proc.time() - start.time
  }
  fit$fp <- fp
  fit$likdat <- likdat
  fit$time <- fit.time
  
  class(fit) <- "eppfit"
  
  return(fit)
}


sim_rvec_rwproj <- function(rvec, firstidx, lastidx, first_projidx, firstidx.mean, dt){
  logrvec <- log(rvec)
  mean <- median(diff(logrvec[firstidx.mean:lastidx]))  ## define the mean of the random walk
  sd <- sqrt(mean(diff(logrvec[firstidx:lastidx])^2)) #sqrt(mean(diff(logrvec[firstidx:lastidx])^2))  
  projidx <- if (first_projidx<length(rvec)){projidx <- (first_projidx +1):length(rvec)} else {projidx <- 0}  # (lastidx+1):length(rvec)   ## start the projection from 2005
  
  ## simulate differences in projection log(rvec)
  ## variance increases with time: sigma^2*(t-t1) [Hogan 2012]
  
  ## Note: implementation below matches R code from Dan Hogan and Java code
  ## from Tim Brown. Implies that variance grows at rate dt*(t-t1)
  ldiff <- rnorm(length(projidx), mean, sd)  #*sqrt((1+dt*(projidx-lastidx-1)))  ## assume the SD won't increase with time + rvec is decreasing with years
  
  rvec[projidx] <- exp(logrvec[first_projidx] + cumsum(ldiff))
  return(rvec)
}

## simulate incidence and prevalence
simfit.eppfit <- function(fit, rwproj=FALSE){
  fit$param <- lapply(seq_len(nrow(fit$resample)), function(ii) fnCreateParam(fit$resample[ii,], fit$fp))
  
  if(rwproj){
    if(exists("eppmod", where=fit$fp) && fit$fp$eppmod == "rtrend")
      stop("Random-walk projection is only used with r-spline model")
    
    fit$rvec.spline <- sapply(fit$param, "[[", "rvec")
    firstidx <- which(fit$fp$proj.steps == fit$fp$tsEpidemicStart)
    lastidx <- (fit$likdat$lastdata.idx-1)/fit$fp$dt+1
    
    ## replace rvec with random-walk simulated rvec
    fit$param <- lapply(fit$param, function(par){par$rvec <- sim_rvec_rwproj(par$rvec, firstidx, lastidx, fit$fp$dt); par})
  }
  
  fp.list <- lapply(fit$param, function(par) update(fit$fp, list=par))
  mod.list <- lapply(fp.list, simmod)
  
  fit$rvec <- sapply(mod.list, attr, "rvec")
  fit$prev <- sapply(mod.list, prev)
  fit$incid <- mapply(incid, mod = mod.list, fp = fp.list)
  fit$popsize <- sapply(mod.list, rowSums)
  fit$pregprev <- mapply(fnPregPrev.epp, mod.list, fp.list)
  
  return(fit)
}


## Function to do the following:
## (1) Read data, EPP subpopulations, and popualation inputs
## (2) Prepare timestep inputs for each EPP subpopulation

prepare_epp_fit <- function(pjnz, proj.end=2017.5, num_knots = num_knots){

  ## epp
  print("eppd")
  eppd <- read_epp_data(pjnz)
  print("epp.subp")
  epp.subp <- read_epp_subpops(pjnz)
  print("epp.input")
  if(loc %in% c("KEN_35623", "KEN_35640", "KEN_35662")) {
    temp.pjnz <- paste0(root, "WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/UNAIDS_country_data/2015/KEN_44797/KEN_44797.PJNZ")
    epp.input <- ken_ne_input(temp.pjnz)
  #These Nigeria subnationals read in different columns for epp$art, so need some adjustment
  } else if(loc %in% c("NGA_25319","NGA_25325", "NGA_25331", "NGA_25333","NGA_25342" ,"NGA_25350")) {
    
    epp.input <- read_epp_input_nga(pjnz)
    
  } else {
    epp.input <- read_epp_input(pjnz)
  }
  
  if(grepl("NGA_",loc) & nrow(epp.input$art.specpop) < 1){
    specpop.sub <- data.frame(specpop=c("PW","TBHIV"),
                              percelig=c(0.10000000,0.01776023),
                              yearelig=c(2013,2010))
    epp.input$art.specpop <- specpop.sub
  }
  
  epp.subp.input <- fnCreateEPPSubpops(epp.input, epp.subp, eppd)

  ## output
  val <- setNames(vector("list", length(eppd)), names(eppd))


  set.list.attr <- function(obj, attrib, value.lst)
    mapply(function(set, value){ attributes(set)[[attrib]] <- value; set}, obj, value.lst)

  val <- set.list.attr(val, "eppd", eppd)
  val <- set.list.attr(val, "likdat", lapply(eppd, fnCreateLikDat, anchor.year=epp.input$start.year))
  val <- set.list.attr(val, "eppfp", lapply(epp.subp.input, fnCreateEPPFixPar, proj.end = proj.end, num.knots = num_knots))
  val <- set.list.attr(val, "country", attr(eppd, "country"))
  val <- set.list.attr(val, "region", names(eppd))

  return(val)
}
