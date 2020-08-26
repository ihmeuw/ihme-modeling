### R script to fit Bayesian errors-in-variables probit regression model to infer the mapping between RDT and Microscopy-measured prevalence

library(truncnorm)
library(MASS)
library(DPpackage)

### Read in obs data
    
obs.data <- read.csv("FILEPATH")

# Rather than recode to swap covariate and target variables, I'm swapping their values in the obs. data read in
x <- obs.data$RDT.Positive
obs.data$RDT.Positive <- obs.data$Smear.Positive
obs.data$Smear.Positive <- x

### Prior hyperparameters
c.alpha <- 1
c.beta <- 1


### Full version (with errors-in-variables treatment)
n.obs <- length(obs.data$Smear.Positive)
n.tot <- sum(obs.data$Sample.size)

data.augmented <- list()
for (i in 1:n.obs) {data.augmented[[i]] <- list()
data.augmented[[i]]$y.rdt <- numeric(obs.data$Sample.size[i])
data.augmented[[i]]$y.rdt[1:(obs.data$RDT.Positive[i])] <- 1
data.augmented[[i]]$y.smear <- numeric(obs.data$Sample.size[i])
data.augmented[[i]]$y.smear[1:(obs.data$Smear.Positive[i])] <- 1}

# Naive x_i = probit^-1(p_mic) estimates and q_i's
xi.init <- qnorm(qbeta(0.5,obs.data$Smear.Positive+1,obs.data$Sample.size-obs.data$Smear.Positive+1))

qi.init <- list()
for (i in 1:n.obs) {qi.init[[i]] <- list()
lower.lims <- upper.lims <- numeric(obs.data$Sample.size[i])
lower.lims[data.augmented[[i]]$y.smear==0] <- -Inf
upper.lims[data.augmented[[i]]$y.smear==1] <- Inf
qi.init[[i]] <- rtruncnorm(obs.data$Sample.size[i],a=lower.lims,b=upper.lims,mean=xi.init[i],sd=1)}

# Initialize DPM model
DPM.init <- DPdensity(xi.init,prior=list(a0=3,b0=1,tau1=8,tau2=8,m2=0,s2=1,nu1=200,psiinv1=1000),status=T,mcmc=list(nburn=1000,nskip=0,nsave=1,ndisplay=1))


# Initilize z_i's and alpha, beta
alpha.init <- rnorm(1,0,c.alpha^2)
beta.init <- rnorm(1,0,c.beta^2)

zi.init <- list()
for (i in 1:n.obs) {zi.init[[i]] <- list()
lower.lims <- upper.lims <- numeric(obs.data$Sample.size[i])
lower.lims[data.augmented[[i]]$y.rdt==0] <- -Inf
upper.lims[data.augmented[[i]]$y.rdt==1] <- Inf
zi.init[[i]] <- rtruncnorm(obs.data$Sample.size[i],a=lower.lims,b=upper.lims,mean=c(xi.init[i]*beta.init+alpha.init),sd=1)}

# Gibbs sample posterior
n.burn <- 100
n.save <- 100
output <- matrix(0,nrow=n.save,ncol=2)

zi <- zi.init
alpha <- alpha.init
beta <- beta.init
qi <- qi.init
DPM <- DPM.init
xi <- xi.init
for (k in 1:(n.burn+n.save)) {

# alpha,beta | zi,xi
B.star.inv <- diag(c(1/c.alpha,1/c.beta))
X.list <- list()
for (i in 1:n.obs) {X.list[[i]] <- rep(xi[i],obs.data$Sample.size[i])}
X <- X.list[[1]]
for (i in 2:n.obs) {X <- c(X,X.list[[i]])}
X <- cbind(X*0+1,X)
Z <- zi[[1]]
for (i in 2:n.obs) {Z <- c(Z,zi[[i]])}
beta.star <- c(0,0)

B.hat <- solve(B.star.inv + t(X)%*%X)
beta.hat <- B.hat%*%(B.star.inv%*%beta.star+t(X)%*%Z)
    
alpha.beta <- mvrnorm(1,beta.hat,B.hat)
alpha <- alpha.beta[1]
beta <- alpha.beta[2]

for (i in 1:n.obs) {
lower.lims <- upper.lims <- numeric(obs.data$Sample.size[i])
lower.lims[data.augmented[[i]]$y.rdt==0] <- -Inf
upper.lims[data.augmented[[i]]$y.rdt==1] <- Inf
zi[[i]] <- rtruncnorm(obs.data$Sample.size[i],a=lower.lims,b=upper.lims,mean=c(xi[i]*beta+alpha),sd=1)}

# DPM | xi
DPM <- DPdensity(xi,prior=list(a0=3,b0=1,tau1=8,tau2=8,m2=0,s2=1,nu1=200,psiinv1=1000),status=F,mcmc=list(nburn=10,nskip=0,nsave=1,ndisplay=1),state=DPM$state)

# xi | DPM, zi, qi
for (i in 1:n.obs) {
xi.post.mean <- DPM$save.state$randsave[i*2-1]
xi.post.var <-  DPM$save.state$randsave[i*2]^2

for (j in 1:obs.data$Sample.size[i]) {
xi.post.var.new <- 1/(1/xi.post.var+1)
xi.post.mean <- xi.post.var.new*(xi.post.mean/xi.post.var+qi[[i]][j]/1)
xi.post.var <- xi.post.var.new
}

for (j in 1:obs.data$Sample.size[i]) {
xi.post.var.new <- 1/(1/xi.post.var+beta^2)
xi.post.mean <- xi.post.var.new*(xi.post.mean/xi.post.var+(zi[[i]][j]-alpha)*beta)
xi.post.var <- xi.post.var.new}

xi[i] <- rnorm(1,xi.post.mean,sqrt(xi.post.var))
}

# qi | xi
for (i in 1:n.obs) {
lower.lims <- upper.lims <- numeric(obs.data$Sample.size[i])
lower.lims[data.augmented[[i]]$y.smear==0] <- -Inf
upper.lims[data.augmented[[i]]$y.smear==1] <- Inf
qi[[i]] <- rtruncnorm(obs.data$Sample.size[i],a=lower.lims,b=upper.lims,mean=xi[i],sd=1)}

if (k > n.burn) {output[k-n.burn,] <- c(alpha,beta)}

cat("Progress: ", as.integer(k/(n.burn+n.save)*100),"%     alpha: ",signif(alpha,2),"   beta: ",signif(beta,2),"    M: ",DPM$save.state$thetasave[5],"\n",sep="")

}

colMeans(output)
quantile(output[,1],probs=c(0.025,0.975))
quantile(output[,2],probs=c(0.025,0.975))
output[which.max(output[,3]),]
AIC<-2*2-2*output[which.max(output[,3]),3]
AIC

par(mai=c(1.2,1.2,0.8,0.8))
plot(-1,-1,xlim=c(0,1),ylim=c(0,1),xaxs='i',yaxs='i',xaxt='n',yaxt='n',xlab="",ylab="")

for (i in 1:length(obs.data$Sample.size)) {
kx <- obs.data$Smear.Positive[i]
ky <- obs.data$RDT.Positive[i]
n <- obs.data$Sample.size[i]
if (kx>0 & kx < n) {xrange <- qbeta(c(0.05/2,1-0.05/2),kx+1,n-kx+1)} else if (kx==0) {xrange <- c(0,qbeta(1-0.05,kx+1,n-kx+1))} else {xrange <- c(qbeta(0.05,kx+1,n-kx+1),1)}
if (ky>0 & ky < n) {yrange <- qbeta(c(0.05/2,1-0.05/2),ky+1,n-ky+1)} else if (ky==0) {yrange <- c(0,qbeta(1-0.05,ky+1,n-ky+1))} else {yrange <- c(qbeta(0.05,ky+1,n-ky+1),1)}
xmax <- kx/n
ymax <- ky/n
lines(xrange,c(ymax,ymax),col="grey92")
lines(c(xmax,xmax),yrange,col="grey92")}

for (i in 1:length(obs.data$Sample.size)) {
kx <- obs.data$Smear.Positive[i]
ky <- obs.data$RDT.Positive[i]
n <- obs.data$Sample.size[i]
xmax <- kx/n
ymax <- ky/n
points(xmax,ymax,pch=19,cex=0.5,col="grey85")}

xi <- pnorm(seq(-5,5,by=0.01))
curves <- matrix(0,nrow=n.save,ncol=length(xi))
for (i in 1:n.save) {
curves[i,] <- pnorm(qnorm(xi)*output[i,2]+output[i,1])
}
median <- lower.ci <- upper.ci <- xi*0
for (i in 1:length(xi)) {median[i] <- median(curves[,i])
lower.ci[i] <- quantile(curves[,i],0.05/2)
upper.ci[i] <- quantile(curves[,i],1-0.05/2)}
lines(xi,lower.ci,lty=2)
lines(xi,upper.ci,lty=2)
lines(xi,median,lty=1,lwd=2)

abline(a=0,b=1,col="grey" )

#legend("topleft",c(expression(Phi^{-1}*(p[Mic])==alpha+beta*Phi^{-1}*(p[RDT])),expression(Phi^{-1}*(p[Mic])==Phi^{-1}*(p[RDT]))),cex=0.6,bty='n',lty=1,lwd=2,col=c("black","grey"))

axis(1,at=seq(0,1,by=0.2),cex.axis=0.7,tck=-0.02,padj=-1.8)
axis(2,at=seq(0,1,by=0.2),cex.axis=0.7,tck=-0.02,hadj=0.4,las=2)
mtext("Prevalence [RDT]",side=1,line=2,cex=1.2)
mtext("Prevalence [Microscopy]",side=2,line=2,cex=1.2)
#mtext("Fitted model of RDT to Microscopy prevalence",side=3,line=0.8,cex=1.5)


box()

#dev.off()

colMeans(output)


# Plot for correlation 

obs.RDT.prevalence<- x/obs.data$Sample.size
obs.micro.prevalence<-obs.data$RDT.Positive/obs.data$Sample.size
predicted.micro.prevalence<-pnorm(-0.28+(0.93*qnorm(obs.RDT.prevalence)))

plot(obs.micro.prevalence,predicted.micro.prevalence, xlab="Observed Microscopy Prevalence",ylab="Predicted Microscopy Prevalence")
abline(a=0,b=1,col="grey" )

cor(obs.micro.prevalence,predicted.micro.prevalence)


# Correlation with leave out one cross validation


### Full version (with errors-in-variables treatment)
results.of.each<-matrix(0,nrow=length(obs.data$RDT.Positive),ncol=3)

for (m in 1:length(obs.data$RDT.Positive))
{
  obs.data.r<-obs.data[-m,]
  
  n.obs <- length(obs.data.r$Smear.Positive)
  n.tot <- sum(obs.data.r$Sample.size)
  
  data.augmented <- list()
  for (i in 1:n.obs) {data.augmented[[i]] <- list()
                      data.augmented[[i]]$y.rdt <- numeric(obs.data.r$Sample.size[i])
                      data.augmented[[i]]$y.rdt[1:(obs.data.r$RDT.Positive[i])] <- 1
                      data.augmented[[i]]$y.smear <- numeric(obs.data.r$Sample.size[i])
                      data.augmented[[i]]$y.smear[1:(obs.data.r$Smear.Positive[i])] <- 1}
  
  # Naive x_i = probit^-1(p_mic) estimates and q_i's
  xi.init <- qnorm(qbeta(0.5,obs.data.r$Smear.Positive+1,obs.data.r$Sample.size-obs.data.r$Smear.Positive+1))
  
  qi.init <- list()
  for (i in 1:n.obs) {qi.init[[i]] <- list()
                      lower.lims <- upper.lims <- numeric(obs.data.r$Sample.size[i])
                      lower.lims[data.augmented[[i]]$y.smear==0] <- -Inf
                      upper.lims[data.augmented[[i]]$y.smear==1] <- Inf
                      qi.init[[i]] <- rtruncnorm(obs.data.r$Sample.size[i],a=lower.lims,b=upper.lims,mean=xi.init[i],sd=1)}
  
  # Initialize DPM model
  DPM.init <- DPdensity(xi.init,prior=list(a0=3,b0=1,tau1=8,tau2=8,m2=0,s2=1,nu1=200,psiinv1=1000),status=T,mcmc=list(nburn=1000,nskip=0,nsave=1,ndisplay=1))
  
  
  # Initilize z_i's and alpha, beta
  alpha.init <- rnorm(1,0,c.alpha^2)
  beta.init <- rnorm(1,0,c.beta^2)
  
  zi.init <- list()
  for (i in 1:n.obs) {zi.init[[i]] <- list()
                      lower.lims <- upper.lims <- numeric(obs.data.r$Sample.size[i])
                      lower.lims[data.augmented[[i]]$y.rdt==0] <- -Inf
                      upper.lims[data.augmented[[i]]$y.rdt==1] <- Inf
                      zi.init[[i]] <- rtruncnorm(obs.data.r$Sample.size[i],a=lower.lims,b=upper.lims,mean=c(xi.init[i]*beta.init+alpha.init),sd=1)}
  
  # Gibbs sample posterior
  n.burn <- 100
  n.save <- 100
  output <- matrix(0,nrow=n.save,ncol=3)
  
  zi <- zi.init
  alpha <- alpha.init
  beta <- beta.init
  qi <- qi.init
  DPM <- DPM.init
  xi <- xi.init
  for (k in 1:(n.burn+n.save)) {
    
    # alpha,beta | zi,xi
    B.star.inv <- diag(c(1/c.alpha,1/c.beta))
    X.list <- list()
    for (i in 1:n.obs) {X.list[[i]] <- rep(xi[i],obs.data.r$Sample.size[i])}
    X <- X.list[[1]]
    for (i in 2:n.obs) {X <- c(X,X.list[[i]])}
    X <- cbind(X*0+1,X)
    Z <- zi[[1]]
    for (i in 2:n.obs) {Z <- c(Z,zi[[i]])}
    beta.star <- c(0,0)
    
    B.hat <- solve(B.star.inv + t(X)%*%X)
    beta.hat <- B.hat%*%(B.star.inv%*%beta.star+t(X)%*%Z)
    
    alpha.beta <- mvrnorm(1,beta.hat,B.hat)
    alpha <- alpha.beta[1]
    beta <- alpha.beta[2]
    
    #Likelihood
    
    log.likehood.vec<-NULL
    for (i in 1:n.obs)
    {
      log.likehood.vec[i]<-dbinom(obs.data.r$RDT.Positive[i],obs.data.r$Sample.size[i],pnorm(xi[i]*beta+alpha),log=T)
    }
    log.likehood<-sum(log.likehood.vec)
    
    # zi | alpha,beta,xi
    for (i in 1:n.obs) {
      lower.lims <- upper.lims <- numeric(obs.data.r$Sample.size[i])
      lower.lims[data.augmented[[i]]$y.rdt==0] <- -Inf
      upper.lims[data.augmented[[i]]$y.rdt==1] <- Inf
      zi[[i]] <- rtruncnorm(obs.data.r$Sample.size[i],a=lower.lims,b=upper.lims,mean=c(xi[i]*beta+alpha),sd=1)}
    
    # DPM | xi
    DPM <- DPdensity(xi,prior=list(a0=3,b0=1,tau1=8,tau2=8,m2=0,s2=1,nu1=200,psiinv1=1000),status=F,mcmc=list(nburn=10,nskip=0,nsave=1,ndisplay=1),state=DPM$state)
    
    # xi | DPM, zi, qi
    for (i in 1:n.obs) {
      xi.post.mean <- DPM$save.state$randsave[i*2-1]
      xi.post.var <-  DPM$save.state$randsave[i*2]^2
      
      for (j in 1:obs.data.r$Sample.size[i]) {
        xi.post.var.new <- 1/(1/xi.post.var+1)
        xi.post.mean <- xi.post.var.new*(xi.post.mean/xi.post.var+qi[[i]][j]/1)
        xi.post.var <- xi.post.var.new
      }
      
      for (j in 1:obs.data.r$Sample.size[i]) {
        xi.post.var.new <- 1/(1/xi.post.var+beta^2)
        xi.post.mean <- xi.post.var.new*(xi.post.mean/xi.post.var+(zi[[i]][j]-alpha)*beta)
        xi.post.var <- xi.post.var.new}
      
      xi[i] <- rnorm(1,xi.post.mean,sqrt(xi.post.var))
    }
    
    # qi | xi
    for (i in 1:n.obs) {
      lower.lims <- upper.lims <- numeric(obs.data.r$Sample.size[i])
      lower.lims[data.augmented[[i]]$y.smear==0] <- -Inf
      upper.lims[data.augmented[[i]]$y.smear==1] <- Inf
      qi[[i]] <- rtruncnorm(obs.data.r$Sample.size[i],a=lower.lims,b=upper.lims,mean=xi[i],sd=1)}
    
    
    
    if (k > n.burn) {output[k-n.burn,] <- c(alpha,beta,log.likehood)}
    
    cat("Progress: ", as.integer(k/(n.burn+n.save)*100),"%     alpha: ",signif(alpha,2),"   beta: ",signif(beta,2),"    log.likelihood: " , signif(log.likehood,2),"    M: ",DPM$save.state$thetasave[5],"\n",sep="")
    
    
    
  }
  
  results.of.each[m,]<-c(colMeans(output)[1],colMeans(output)[2],output[which.max(output[,3]),][3])
  
}

results.of.each

obs.RDT.prevalence<- x/obs.data$Sample.size
obs.micro.prevalence<-obs.data$RDT.Positive/obs.data$Sample.size

predicted.micro.prevalence<-NULL
for (m in 1:length(obs.RDT.prevalence))
{
  predicted.micro.prevalence[m]<-pnorm(results.of.each[m,1]+(results.of.each[m,2]*qnorm(obs.RDT.prevalence[m])))
}
par(cex.lab=1.6,cex.main=1.8)
plot(obs.micro.prevalence,predicted.micro.prevalence, xlab="Observed Microscopy Prevalence",ylab="Predicted Microscopy Prevalence", main ="Correlation Between Observed and Predicted Microscopy Prevalence")
abline(a=0,b=1,col="grey" )

cor(obs.micro.prevalence,predicted.micro.prevalence)



