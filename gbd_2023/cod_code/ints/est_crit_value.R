rmvn <- function(n, mu, sig) { ## MVN random deviates
  L <- mroot(sig)
  m <- ncol(L)
  t(mu + L %*% matrix(rnorm(m*n), m, n))
}

est_crit_val <- function(mdl, newdata, N = 10000, prob = 0.95) {
  Vb <- vcov(mdl)
  pred  <- predict(mdl, critPred, se.fit = TRUE)
  se.fit <- pred$se.fit
  
  set.seed(4352162)
  
  BUdiff <- rmvn(N, mu = rep(0, nrow(Vb)), sig = Vb)
  
  Cg <- predict(mdl, critPred, type = "lpmatrix")
  simDev <- Cg %*% t(BUdiff)
  
  absDev <- abs(sweep(simDev, 1, se.fit, FUN = "/"))
  masd <- apply(absDev, 2L, max)
  crit <- quantile(masd, prob = prob, type = 8)
  return(crit)
}