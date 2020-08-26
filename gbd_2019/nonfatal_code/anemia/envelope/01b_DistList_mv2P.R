
require(actuar)
EULERS_CONSTANT = 0.57721566490153286060651209008240243104215933593992



#############################################################
# weibull :: the weibull family of PDFs, class A 
#        class A, standard R distribution 
#############################################################

xweibull = function(x, shape, scale){
  x*dweibull(x,shape=shape,scale=scale) 
} 

x2weibull = function(x, shape, scale){
  x^2*dweibull(x,shape=shape,scale=scale) 
} 


weibull_mv2p = function(mn,vr){
  F1 = function(x, mn, vr){ 
    x = abs(x)
    mn1 = integrate(xweibull, 0, Inf, shape=x[1], scale=x[2])$value   
    m2 = integrate(x2weibull, 0, Inf, shape=x[1], scale=x[2])$value   
    vr1 = m2 - mn1^2 
    (mn1 - mn)^2 + (vr1-vr)^2 
  }
  
  F2 = function(x, mn, vr){ 
    a = x[1] #shape  
    b = x[2] #scale 
    mn1 = b*gamma(1+1/a)
    vr1 = b^2*(gamma(1+2/a)- (gamma(1+1/a))^2)
    (mn1 - mn)^2 + (vr1-vr)^2 
  }
  
  xi = c(mn/sqrt(vr), mn)  
  est1 = try(abs(optim(xi, F1, mn=mn, vr=vr)$par), silent=T)
  converged <- ifelse(length(est1) == 1, FALSE, TRUE)
	#if(length(est1) == 1) {converged = FALSE}
	#if(length(est1) == 2) {converged = TRUE}

  est2 = abs(optim(xi, F2, mn=mn, vr=vr)$par) 
  
  if(!converged){
    est = est2
    }
  if(converged){
    if(F1(est1,mn,vr)< F2(est2,mn,vr)) {est = est1}
    if(F1(est1,mn,vr)> F2(est2,mn,vr)) {est = est2}
    # est = if(F1(est1,mn,vr)< F2(est2,mn,vr)){est1}else{est2}
  }
  list(shape=est[1], scale = est[2]) 
} 



weibullOBJ = list(

    name="weibull", 

    mv2par = weibull_mv2p, 

    dF = function(x,p){dweibull(x, shape=p[1], scale=p[2])}, 
    
    tailF = function(p,tau,lt) 
    {
      #browser()  
      p = unlist(p)  
      pweibull(tau, shape=p[1], scale=p[2], lower.tail=lt)
    }, 
    
    plotF = function(x,p,clr)
    { 
      p=unlist(p)
      #browser() 
      lines(x,dweibull(x, shape=p[1], scale=p[2]),col = clr)  
    } 
)  



#############################################################
# gamma :: the gamma family of PDFs, class A
#          class A, standard R distribution 
#############################################################

gamma_mv2p = function(mn, vr){list(shape = mn^2/vr,rate = mn/vr)}


gammaOBJ = list(

    name="gamma", 

    mv2par = gamma_mv2p, 

    dF = function(x,p){dgamma(x, shape=p[1], rate=p[2])}, 

    tailF = function(p,tau,lt)
    {
      pgamma(tau, shape=p[1], rate=p[2], lower.tail=lt)
    }, 
    
    plotF = function(x,p,clr)
    {
      lines(x,dgamma(x, shape = p[1], rate=p[2]), col = clr)  
    }  
)  

#############################################################
# mirgamma :: the mirrored gamma family of PDFs
#          class M 
#############################################################

mirgamma_mv2p = function(mn, vr){gamma_mv2p(XMAX-mn, vr)} 

dmirgamma = function(x, shape, rate){
  dgamma(XMAX-x, shape=shape, rate=rate)
}

pmirgamma = function(q, shape, rate, lower.tail=TRUE){ 
  #NOTE: with mirroring, take the other tail
  pgamma(XMAX-q, shape, rate, lower.tail=!lower.tail) 
}

qmirgamma = function(p, shape, rate)
{
  qgamma(XMAX-z, shape=shape, rate=rate)
} 


#rmirgamma = function(n, shape, rate)
#{
#
#  rgamma(n, , )
#}

mirgamma_mv2p = function(mn, vr){
 list(
   shape = (XMAX - mn)^2/vr,
   rate = (XMAX-mn)/vr 
 ) 
}

mirgammaOBJ = list(

    name="mirgamma", 

    mv2par = mirgamma_mv2p, 

    dF = function(x,p){dmirgamma(x, shape=p[1], rate=p[2])}, 

    tailF = function(p,tau,lt)
    {
      pmirgamma(tau, shape=p[1], rate=p[2], lower.tail=lt)
    }, 
    
    plotF = function(x,p,clr)
    {
      lines(x,dmirgamma(x, shape = p[1], rate=p[2]), col = clr)  
    }  
)  


#############################################################
# mgumbel :: the mirrored gumbel family of PDFs, class A
#        class M, defined below 
#
# NOTE: XMAX should be defined as a global parameter
#  
#############################################################

dmgumbel = function(x, alpha, scale)
{
  dgumbel(XMAX-x, alpha, scale)
}

pmgumbel = function(q, alpha, scale, lower.tail) 
{ 
  #NOTE: with mirroring, take the other tail
  pgumbel(XMAX-q, alpha, scale, lower.tail=ifelse(lower.tail,FALSE,TRUE)) 
}

qmgumbel = function(p, alpha, scale)
{
  qgumbel(XMAX-z, alpha, scale)
} 

rmgumbel = function(n, alpha, scale)
{
  mn = alpha + scale*EULERS_CONSTANT
  rgumbel(n, alpha+XMAX-2*mn, scale)
}

mgumbel_mv2p = function(mn, vr){
 list(
   alpha = XMAX - mn - EULERS_CONSTANT*sqrt(vr)*sqrt(6)/pi,
   scale = sqrt(vr)*sqrt(6)/pi
 ) 
}
 
mgumbelOBJ = list(
  
  name="mgumbel", 
  
  initF = function(D)
  {
   inits =list(
     alpha = XMAX - mean(D) + EULERS_CONSTANT*sd(D)*sqrt(6)/pi,
     scale = sd(D)*sqrt(6)/pi
   )
    fitdist(XMAX-D, "gumbel", start=inits)$estimate 
  }, 

  mv2par = mgumbel_mv2p, 

  dF = function(x,p){dmgumbel(x, alpha=p[1], scale=p[2])},  
  
  tailF = function(p,tau,lt) 
  { #NOTE: take the other tail 
    pmgumbel(tau, alpha=p[1], scale=p[2], lower.tail=lt)
  }, 
  
  plotF = function(x,p,clr)
  {
    lines(x,dmgumbel(x, alpha=p[1], scale=p[2]),col = clr)  
  } 
)  


#############################################################
# mirlnorm :: the log-normal family of PDFs
#          class A, standard R distribution 
#############################################################

mirlnorm_mp2p = function(DATA, tau, lower.tail=TRUE){ 
  lnorm_mp2p(XMAX-DATA, XMAX-tau-1e-10, lower.tail) 
} 

dmirlnorm = function(x, meanlog, sdlog){
  dlnorm(XMAX-x, meanlog=meanlog, sdlog=sdlog)
}

pmirlnorm = function(q, meanlog, sdlog, lower.tail=TRUE){ 
  #NOTE: with mirroring, take the other tail
  plnorm(XMAX-q, meanlog, sdlog, lower.tail=!lower.tail) 
}

qmirlnorm = function(p, meanlog, sdlog)
{
  qlnorm(XMAX-z, meanlog=meanlog, sdlog=sdlog)
} 

mirlnorm_mme = function(DATA){
  fitdist(XMAX-DATA, "lnorm", method = "mme")$estimate
} 


mirlnorm_mv2p = function(mn, vr){
  F = function(x, mn, vr){
    x = abs(x) 
    mu = x[1]; sigma = x[2]
    (XMAX - mn - exp(mu+sigma^2/2))^2 + (vr - (exp(sigma^2)-1)*exp(2*mu+sigma^2))^2 
  }
  xi = c(log(mn), 0)  
  eval = F(xi, mn, vr)
  if(eval == eval){ 
    est = abs(optim(xi, F, mn=mn, vr=vr)$par)
  } else {
    est = c(0,0) 
    print("The lognormal is probably unsuitable") 
  } 
  list(meanlog = est[1], sdlog = est[2]) 
} 



