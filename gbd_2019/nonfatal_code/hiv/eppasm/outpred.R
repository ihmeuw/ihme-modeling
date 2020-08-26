outpred_prev <- function(fit, out, subsample=NULL){

  hhs <- out$hhs

  if(!is.null(subsample))
    fit$resample <- fit$resample[sample.int(nrow(fit$resample), subsample), ]

  dat <- prepare_hhsageprev_likdat(hhs, fit$fp)
  fit <- simfit(fit, ageprevdat=dat, pregprev=FALSE, ageincid=FALSE, ageinfections=FALSE,
                relincid=FALSE, entrantprev=FALSE)

  ## Impute missing standard errors based on model prevalence
  na_i <- which(is.na(dat$sd.W.hhs))
  na_p <- rowMeans(fit$ageprevdat)[na_i]
  dat$sd.W.hhs[na_i] <- sqrt(na_p * (1 - na_p) / dat$n_eff[na_i]) / dnorm(qnorm(na_p))


  M <- fit$ageprevdat
  qM <- qnorm(M)
  qpred <- array(rnorm(length(qM), qM, dat$sd.W.hhs), dim(qM))

  elpd <- log(rowMeans(exp(ldbinom(dat$x_eff, dat$n_eff, M))))
  resid <- rowMeans(M) - dat$prev
  rse <- apply(M, 1, sd) / rowMeans(M)
  mae <- rowMeans(abs(M - dat$prev))
  rmse <- sqrt(rowMeans((M - dat$prev)^2))

  elpd_q <- log(rowMeans(dnorm(dat$W.hhs, qM, dat$sd.W.hhs)))
  rse_q <- apply(qM, 1, sd) / rowMeans(qM)
  resid_q <- rowMeans(qM) - dat$W.hhs
  mae_q <- rowMeans(abs(qM - dat$W.hhs))
  rmse_q<- sqrt(rowMeans((qM - dat$W.hhs)^2))
  qq <- mapply(function(f, x) f(x), apply(qpred, 1, ecdf), dat$W.hhs)  

  vars <- intersect(c("country", "eppregion", "survyear", "year", "sex", "agegr", "prev", "se"), names(dat))
  data.frame(dat[vars],
             elpd, resid, rse, mae, rmse, elpd_q, resid_q, rse_q, mae_q, rmse_q, qq)
}

outpred_hhs <- function(fit, newdata, subsample=NULL){

  if(!is.null(subsample))
    fit$resample <- fit$resample[sample.int(nrow(fit$resample), subsample), ]
  
  if(fit$fp$eppmod == "rspline")
    fit <- rw_projection(fit)
  if(fit$fp$eppmod == "rhybrid")
    fit <- extend_projection(fit, proj_years = fit$fp$ss$PROJ_YEARS)

  ## simulate model projections
  param_list <- lapply(seq_len(nrow(fit$resample)), function(ii) fnCreateParam(fit$resample[ii,], fit$fp))
  
  fp_list <- lapply(param_list, function(par) update(fit$fp, list=par))
  mod_list <- lapply(fp_list, simmod)

  ## new data for prediction
  dat <- prepare_hhsageprev_likdat(newdata, fit$fp)

  ## predicted prevalence
  M <- vapply(mod_list, ageprev, numeric(nrow(dat)),
              aidx = dat$aidx,
              sidx = dat$sidx,
              yidx = dat$yidx,
              agspan = dat$agspan)
  M <- matrix(M, nrow(dat))
  qM <- qnorm(M)
  qpred <- array(rnorm(length(qM), qM, dat$sd.W.hhs), dim(qM))

  dat$elpd_q <- log(rowMeans(dnorm(dat$W.hhs, qM, dat$sd.W.hhs)))
  dat$se_q <- apply(qM, 1, sd)
  dat$resid_q <- rowMeans(qM) - dat$W.hhs
  dat$pred <- rowMeans(M)
  dat$se_p <- apply(M, 1, sd)
  dat$resid <- rowMeans(M) - dat$prev
  dat$qq <- mapply(function(f, x) f(x), apply(qpred, 1, ecdf), dat$W.hhs)  

  dat
}


outpred_ancsite <- function(fit, testdat){
  
  if(fit$fp$eppmod == "rspline")
    fit <- rw_projection(fit)
  if(fit$fp$eppmod == "rhybrid")
    fit <- extend_projection(fit, proj_years = fit$fp$ss$PROJ_YEARS)

  ## simulate model projections
  param_list <- lapply(seq_len(nrow(fit$resample)), function(ii) fnCreateParam(fit$resample[ii,], fit$fp))

  fp_list <- lapply(param_list, function(par) update(fit$fp, list=par))
  mod_list <- lapply(fp_list, simmod)

  ## Site-level ANC data
  b_site <- Map(sample_b_site, mod_list, fp_list,
                list(fit$likdat$ancsite.dat), resid = FALSE)

  ## Prediction data
  newdata <- prepare_ancsite_likdat(testdat, fit$fp)
  
  ancsite_ll <- mapply(ll_ancsite_conditional, mod_list, fp_list,
                       b_site = b_site,
                       MoreArgs = list(newdata = newdata))

  ancsite_pred <- mapply(sample_ancsite_pred, mod_list, fp_list,
                         b_site = b_site,
                         MoreArgs = list(newdata = newdata))


  testdat$elpd <- log(rowMeans(exp(ancsite_ll)))
  testdat$resid <- rowMeans(ancsite_pred) - newdata$df$W
  testdat$qq <- mapply(function(f, x) f(x), apply(ancsite_pred, 1, ecdf), newdata$df$W)

  testdat
}


outpred_incid <- function(fit, newdata, subsample=NULL){

  if(!is.null(subsample))
    fit$resample <- fit$resample[sample.int(nrow(fit$resample), subsample), ]
  
  if(fit$fp$eppmod == "rspline")
    fit <- rw_projection(fit)
  if(fit$fp$eppmod == "rhybrid")
    fit <- extend_projection(fit, proj_years = fit$fp$ss$PROJ_YEARS)

  ## simulate model projections
  param_list <- lapply(seq_len(nrow(fit$resample)), function(ii) fnCreateParam(fit$resample[ii,], fit$fp))
  
  fp_list <- lapply(param_list, function(par) update(fit$fp, list=par))
  mod_list <- lapply(fp_list, simmod)

  ## new data for prediction
  dat <- prepare_hhsincid_likdat(newdata, fit$fp)

  ## predicted prevalence
  lM <- log(vapply(mod_list, incid, numeric(fit$fp$ss$PROJ_YEARS))[dat$idx,])
  lM <- matrix(lM, nrow(dat))
  incid_ll <- vapply(mod_list, ll_hhsincid, hhsincid.dat = dat, numeric(nrow(dat)))
  incid_ll <- matrix(incid_ll, nrow(dat))

  lpred <- array(rnorm(length(lM), lM, dat$log_incid.se), dim(lM))
  
  dat$elpd <- log(rowMeans(exp(incid_ll)))
  dat$se_l <- apply(lM, 1, sd)
  dat$resid_l <- rowMeans(lM) - dat$log_incid
  dat$pred <- rowMeans(exp(lM))
  dat$se_p <- apply(exp(lM), 1, sd)
  dat$resid <- rowMeans(exp(lM)) - dat$incid
  dat$qq <- mapply(function(f, x) f(x), apply(lpred, 1, ecdf), dat$log_incid)  

  dat
}
