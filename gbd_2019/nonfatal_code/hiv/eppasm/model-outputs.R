estci2 <- function(x, na.rm=TRUE){
  if(is.vector(x)) x <- matrix(x, 1)
  nd <- length(dim(x))
  val <- apply(x, seq_len(nd-1), function(y) c(mean(y, na.rm=na.rm),
                                               sd(y, na.rm=na.rm),
                                               quantile(y, c(0.5, 0.025, 0.975), na.rm=na.rm)))
  val <- aperm(val, c(2:nd, 1))
  dimnames(val)[[nd]] <- c("mean", "se", "median", "lower", "upper")
  val
}

sub_na <- function(x, v=0){x[is.na(x)] <- v; x}

summary_outputs <- function(fit, modlist){
  out <- list()

  startyr <- fit$fp$ss$proj_start
  endyr <- fit$fp$ss$proj_start + fit$fp$ss$PROJ_YEARS-1L

  out$prev <- sapply(modlist, prev)
  out$incid <- sapply(modlist, incid)
  out$incid[min(which(out$incid[,1] > 0)),] <- 0  # remove initial seed incidence
  out$transmrate <- estci2(sub_na(out$incid / out$prev))
  out$prev <- estci2(out$prev)
  out$incid <- estci2(out$incid)
  if(!is.null(attr(modlist[[1]], "pregprev")))
    out$pregprev <- estci2(sapply(modlist, fnPregPrev))

  out$artcov15plus <- estci2(sub_na(sapply(modlist, artcov15plus)))
  out$artcov15to49 <- estci2(sub_na(sapply(modlist, artcov15to49)))

  ## Sex ratio of incidence 14-49
  out$incidsexratio <- lapply(modlist, ageincid, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                              yidx=startyr:endyr - startyr+1L, agspan=35)
  out$incidsexratio <- estci2(sub_na(sapply(out$incidsexratio, function(x) x[,2,] / x[,1,])))


  ann <- c("prev", "incid", "artcov15plus", "artcov15to49", "transmrate", "incidsexratio")
  if(exists("pregprev", out)) ann <- c(ann, "pregprev")
  out[ann] <- lapply(out[ann], function(x){dimnames(x)[[1]] <- startyr:endyr; x})


  ## Prevalence, incidence, and ART coverage by age categories and sex
  agegr3 <- lapply(modlist, ageprev, aidx=c(15, 25, 35, 50)-fit$fp$ss$AGE_START+1L, sidx=1:2,
                   yidx=1999:endyr - startyr+1L, agspan=c(10, 10, 15, 31))
  age15to49 <- lapply(modlist, ageprev, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=1999:endyr - startyr+1L, agspan=35)
  age15plus <- lapply(modlist, ageprev, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=1999:endyr - startyr+1L, agspan=66)
  agegr3 <- estci2(abind::abind(agegr3, rev.along=0))
  age15to49 <- estci2(abind::abind(age15to49, rev.along=0))
  age15plus <- estci2(abind::abind(age15plus, rev.along=0))
  out$agegr3prev <- abind::abind(agegr3, age15to49, age15plus, along=1)

  agegr3 <- lapply(modlist, ageincid, aidx=c(15, 25, 35, 50)-fit$fp$ss$AGE_START+1L, sidx=1:2,
                   yidx=1999:endyr - startyr+1L, agspan=c(10, 10, 15, 31))
  age15to49 <- lapply(modlist, ageincid, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=1999:endyr - startyr+1L, agspan=35)
  age15plus <- lapply(modlist, ageincid, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=1999:endyr - startyr+1L, agspan=66)
  agegr3 <- estci2(abind::abind(agegr3, rev.along=0))
  age15to49 <- estci2(abind::abind(age15to49, rev.along=0))
  age15plus <- estci2(abind::abind(age15plus, rev.along=0))
  out$agegr3incid <- abind::abind(agegr3, age15to49, age15plus, along=1)

  agegr3 <- lapply(modlist, ageinfections, aidx=c(15, 25, 35, 50)-fit$fp$ss$AGE_START+1L, sidx=1:2,
                   yidx=1999:endyr - startyr+1L, agspan=c(10, 10, 15, 31))
  age15to49 <- lapply(modlist, ageinfections, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=1999:endyr - startyr+1L, agspan=35)
  age15plus <- lapply(modlist, ageinfections, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=1999:endyr - startyr+1L, agspan=66)
  agegr3 <- estci2(abind::abind(agegr3, rev.along=0))
  age15to49 <- estci2(abind::abind(age15to49, rev.along=0))
  age15plus <- estci2(abind::abind(age15plus, rev.along=0))
  out$agegr3infections <- abind::abind(agegr3, age15to49, age15plus, along=1)

  dimnames(out$agegr3prev)[1:3] <- dimnames(out$agegr3incid)[1:3] <- dimnames(out$agegr3infections)[1:3] <-
    list(agegr=c("15-24", "25-34", "35-49", "50+", "15-49", "15+"), sex=c("Male", "Female"), year=1999:endyr)


  agegr3 <- lapply(modlist, ageartcov, aidx=c(15, 25, 35, 50)-fit$fp$ss$AGE_START+1L, sidx=1:2,
                   yidx=2005:endyr - startyr+1L, agspan=c(10, 10, 15, 31))
  age15to49 <- lapply(modlist, ageartcov, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=2005:endyr - startyr+1L, agspan=35)
  age15plus <- lapply(modlist, ageartcov, aidx=15-fit$fp$ss$AGE_START+1L, sidx=1:2,
                      yidx=2005:endyr - startyr+1L, agspan=66)
  agegr3 <- estci2(abind::abind(agegr3, rev.along=0))
  age15to49 <- estci2(abind::abind(age15to49, rev.along=0))
  age15plus <- estci2(abind::abind(age15plus, rev.along=0))
  out$agegr3artcov <- abind::abind(agegr3, age15to49, age15plus, along=1)
  dimnames(out$agegr3artcov)[1:3] <- list(agegr=c("15-24", "25-34", "35-49", "50+", "15-49", "15+"), sex=c("Male", "Female"), year=2005:endyr)


  ## Age-specific prevalence in survey years
  out$ageprevdat <- data.frame(fit$likdat$hhsage.dat[c("survyear", "year", "sex", "agegr")],
                               estci2(sapply(modlist, ageprev, arridx=fit$likdat$hhsage.dat$arridx, agspan=5)))

  ## Incidence relative to 25-29y
  out$relincid <- lapply(modlist, ageincid, aidx=3:9*5-fit$fp$ss$AGE_START+1L, sidx=1:2,
                         yidx=c(2001, 2006, 2011, 2016)- startyr+1L, agspan=5)
  out$relincid <- estci2(abind::abind(lapply(out$relincid, function(x) sweep(x, 2:3, x[3,,], "/")), rev.along=0))
  dimnames(out$relincid)[1:3] <- list(agegr=paste0(3:9*5, "-", 3:9*5+4), sex=c("Male", "Female"), year=2001+0:3*5)

  return(out)
}


create_outputs <- function(fit){
  paramlist <- lapply(seq_len(nrow(fit$resample)), function(ii) fnCreateParam(fit$resample[ii,], fit$fp))
  fplist <- lapply(paramlist, function(par) update(fit$fp, list=par))
  modlist <- lapply(fplist, simmod)
  
  out <- summary_outputs(fit, modlist)
  out
}

#' @param fitlist list of model fits to aggregate.
#' @param fitnat single fit to use as the base fit with aggregation.
#' @param both flag indicating whether to return outputs for both aggregated fit and individual fits (default TRUE).
create_aggr_outputs <- function(fitlist, fitnat, both=TRUE){

  modlist <- aggr_specfit(fitlist)
  summary_outputs(fitnat, modlist)
}
