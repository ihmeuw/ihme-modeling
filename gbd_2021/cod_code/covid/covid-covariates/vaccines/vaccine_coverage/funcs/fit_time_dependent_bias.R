.fit_time_dependent_bias  <- function(y,             # Target data
                                      y_hat,         # Initial estimate
                                      upper_bound=NULL,
                                      lower_bound=NULL,
                                      plots=FALSE     # Plot model fits?
) {
  
  sel <- max(which(!is.na(y_hat)))
  if (sel < length(y)) y[(sel+1):length(y)] <- NA
  
  # Starting parameters
  x_shift <- 0

  # Shift y axis to keep y_hat = y at t = 1
  #sel <- min(!is.na(y_hat))
  #y_shift <- abs(y[sel] - y_hat[sel])
  #tmp_y_hat <- y_hat - y_shift
  
  # Naive bias estimate adjustment at last day of observed data
  sel <- which.max(y) 
  b_0 <- y[sel] / y_hat[sel]
  #b_0 <- b_0[!is.na(b_0)]
  #b_0 <- b_0[length(b_0)]
  tmp_y_hat <- y_hat * b_0
  
  par(mfrow=c(1,1))
  plot(y_hat, col='blue')
  points(y)
  points(tmp_y_hat, col='purple')
  
  # Do right shift
  check <- tmp_y_hat >= y
  do_right_shift <- ifelse(all(check[!is.na(check)]), T, F) # If y_hat > y do right shift

  while (do_right_shift) {
    
    # Shift x axis until all y_hat > y
    x_shift <- x_shift + 1
    tmp_y_hat <- shift(tmp_y_hat, x_shift)
    tmp_y_hat <- .extend_end_values(tmp_y_hat)
    
    check <- tmp_y_hat >= y
    do_right_shift <- ifelse(all(check[!is.na(check)]), T, F)
  }
  
  points(tmp_y_hat, col='deeppink')
  

  # Do left shift
  
  check <- y >= tmp_y_hat
  do_right_shift <- ifelse(any(check[!is.na(check)]), T, F) 

  while (do_left_shift) {
    
    # Shift x axis until all y_hat > y
    x_shift <- x_shift - 1
    tmp_y_hat <- shift(tmp_y_hat, x_shift)
    tmp_y_hat <- .extend_end_values(tmp_y_hat)

    check <- y >= tmp_y_hat
    do_left_shift <- ifelse(any(check[!is.na(check)]), T, F) 
  }

  points(tmp_y_hat, col='green4')
  

  
  ##
  df <- data.frame(residual = y/tmp_y_hat, index = seq_along(y))
  
  .fit_func <- function(par, x) {
    t_max <- length(x)
    prop_time <- seq_along(x)/t_max
    est <- pbeta(prop_time, shape1=par[1], shape2=par[2])
    se <- (est - x)^2
    mean(se[!is.na(se)])
  }

  fit <- optim(par=c(2, 2), 
               fn=.fit_func,
               x=df$residual,
               method='L-BFGS-B',  #'Nelder-Mead',
               upper=upper_bound,
               lower=lower_bound,
               control=list(maxit=1e6),
               hessian=TRUE)
  
  b_t <- pbeta(prop_time, shape1=fit$par[1], shape2=fit$par[2])
  y_hat_adjusted <- (.extend_end_values(shift(y_hat, x_shift)))* b_0 * b_t
  
###

  
  if (plots) {
    
    par(mfrow=c(1,4))
    plot(y_hat, col='blue', ylim=c(0,1),
         main='Naive bias adjustment')
    points(y)
    lines(tmp_y_hat, col='purple', lwd=3)
    
    plot(df$residual, main='Time dependent bias adjustment')
    lines(b_t, col='green3', lwd=3)
    
    plot(y, ylim=c(0,1),
         main='Bias adjusted estimate')
    lines(y_hat, col='goldenrod', lwd=3)
    lines(y_hat_adjusted, col='red', lwd=3)
    
    plot(y_hat - y_hat_adjusted, type='l', lwd=3, col='brown',
         main='Distribution of bias')
    abline(h=0, lty=2)
    
    par(mfrow=c(1,1))
  }
  
  return(
    list(fit=y_hat_adjusted,
         x_shift=x_shift,
         y_shift=y_shift,
         b_0=b_0,
         b_t=b_t)
  )

}

