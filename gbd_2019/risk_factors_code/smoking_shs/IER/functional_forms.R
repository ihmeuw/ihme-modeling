power2.divisor <- 1e10

power2 <- list(
  
	parameters = c("alpha", "beta", "gamma"),
	
	eval = function(z, params, ...)
	  { out <- ifelse(z > params$tmrel, 1+(params$alpha*(1-exp(-params$beta*((z-params$tmrel))^params$gamma))), 1); out},
	
	eval.2013 = function(z, params, ...)
	  { out <- ifelse(z > params$tmrel, 1+(params$alpha*(1-exp(-params$beta*((z-params$tmrel)/power2.divisor)^params$gamma))), 1); out},
	eval.2013.mean = function(z, params, ...)
	  { out <- ifelse(z > params$tmrel.mean, 1+(params$alpha.mean*(1-exp(-params$beta.mean*((z-params$tmrel.mean)/power2.divisor)^params$gamma.mean))), 1); out},
	
	eval.2010 = function(z, params, ...)
	  { out <- ifelse(z > params$tmrel, 1+(params$alpha*(1-exp(-params$beta*(z-params$tmrel)^params$gamma))), 1); out},
	eval.2010.mean = function(z, params, ...)
	  { out <- ifelse(z > params$tmrel.mean, 1+(params$alpha.mean*(1-exp(-params$beta.mean*(z-params$tmrel.mean)^params$gamma.mean))), 1); out},
	
	func = function(rr, z, tmrel, alpha, beta, gamma, ...)
	  { rr~1+(alpha*(1-exp(-beta*(z-tmrel)/power2.divisor^gamma)))*(z > tmrel)},
	func.ratio = function(rr, z.num, z.den, tmrel, alpha, beta, gamma, ...)
	  {rr~(1+(alpha*(1-exp(-beta*((z.num-tmrel)/power2.divisor)^gamma))))/(1+(alpha*(1-exp(-beta*((z.den-tmrel)/power2.divisor)^gamma))))}
	
)

simplified <- list(
  
  parameters = c("beta", "rho"),
  
  eval = function(z, params, ...)
    { out <- ifelse(z > params$tmrel, exp(params$beta * (z - params$tmrel)^params$rho), 1); out},
  eval.mean = function(z, params, ...)
    { out <- ifelse(z > params$tmrel, exp(params$beta.mean * (z - params$tmrel.mean)^params$rho.mean), 1); out}
  
)

loglin <- list(
  
  parameters = c("beta", "rho"),
  
  eval = function(z, params, ...)
  { out <- ifelse(z > params$tmrel, exp(params$beta * log(z/params$tmrel) + params$gamma * (z - params$tmrel)), 1); out}

)

phaseII <- list(
  
  parameters = c("alpha", "b", "gamma"),
  
  eval = function(z, params, ...)
  { out <- ifelse(z > params$tmrel, 1+(params$alpha*(1-exp(-(params$b/params$alpha)*((z-params$tmrel))^params$gamma))), 1); out},
  
  
  func = function(rr, z, tmrel, alpha, b, gamma, ...)
  { rr~1+(alpha*(1-exp(-(b/alpha)*(z-tmrel)/power2.divisor^gamma)))*(z > tmrel)},
  func.ratio = function(rr, z.num, z.den, tmrel, alpha, beta, gamma, ...)
  {rr~(1+(alpha*(1-exp(-(b/alpha)*((z.num-tmrel)/power2.divisor)^gamma))))/(1+(alpha*(1-exp(-(b/alpha)*((z.den-tmrel)/power2.divisor)^gamma))))}
  
)


