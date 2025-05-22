## USERNAME
## DATE
## Fit ensemble weights to minimize D, the KS statistic - no holdouts

eKS <- function(Data, distlist) {
    
    tryCatch({
        # setup parallel backend 
        cl <- parallel::makeCluster(29)
        registerDoParallel(cl)
        clusterCall(cl, function(x) .libPaths(x), .libPaths())
        
        source("/FILEPATH/pdf_families.R")
        ## make sure we only fit on positive values
        Data <- replace(Data, Data<=0, NA)
        Data = log(Data)
        Data = Data[!is.na(Data)]
        Data = exp(Data)
        
        ## What weights will optimize the D statistic on the entire vector of data given just mean and variance?
        min_KS <- function(Data,weights,mean,variance,xmin,xmax) {
            
            ## create CDF of Data
            cdfDATA <- ecdf(Data) 
            
            XMIN <<- xmin
            XMAX <<- xmax
            x = seq(xmin,xmax,length=1000)
            fx = 0*x
            
            ## cdf of Data at our x values
            cdfDATAYout <- cdfDATA(sort(x))
            
            wts = weights
            ## loop through weights and create density vector given mean and sd
            ## some fits fail to converge so here I test to make sure which distributions do fit and re-scale the weights if needed
            wFIT <- wts
            for(z in length(wts):1){ 
                LENGTH <- length(formals(unlist(distlist[[z]]$mv2par)))
                if (LENGTH==4) {
                    est <- try((unlist(distlist[[z]]$mv2par(mean,variance,XMIN=XMIN,XMAX=XMAX))),silent=T)
                } else {
                    est <- try((unlist(distlist[[z]]$mv2par(mean,variance))),silent=T)
                }
                if(class(est)=="try-error") {
                    wFIT[z] <- 0
                }
            }
            
            filter <- wFIT[!wFIT==0]
            wFIT = wFIT/sum(wFIT)
            
            ## loop through re-scaled weights and create density vector given mean and sd
            for(z in 1:length(wFIT)){
                LENGTH <- length(formals(unlist(distlist[[z]]$mv2par)))
                if (LENGTH==4) {
                    est <- try((unlist(distlist[[z]]$mv2par(mean,variance,XMIN=XMIN,XMAX=XMAX))),silent=T)
                } else {
                    est <- try((unlist(distlist[[z]]$mv2par(mean,variance))),silent=T)
                }
                fxj = try(distlist[[z]]$dF(x,est),silent=T)
                if(class(est)=="try-error") {
                    fxj = rep(0,length(fx))
                }
                fxj[!is.finite(fxj)] <- 0
                fx = (fx + fxj*wFIT[[z]])
            }
            
            fx[!is.finite(fx)] <- 0
            fx[length(fx)] <- 0
            fx[1] <- 0
            
            den<-approxfun(x, fx, yleft=0, yright=0) #density function
            
            cdfFITYout = c()
            for(val in x) {
                v<-try(integrate(den, min(x), val)$value)
                if (class(v)=="try-error") {
                    v <- NA
                }
                cdfFITYout <- append(cdfFITYout,v)
            }
            
            xloc <- which.max(abs(cdfDATAYout-cdfFITYout))
            D <- abs(cdfDATAYout[xloc]-cdfFITYout[xloc])
            y0 <- cdfDATAYout[xloc]
            y1 <- cdfFITYout[xloc]
            xPOSITION <- x[which(cdfDATAYout == y0)[1]]
            
            D
        } ## end min_KS function
        
        ########################################################################
        ## Find optimal weights
        ########################################################################
        M <- mean(Data,na.rm=T)
        VAR <- var(Data,na.rm=T)
        XMIN <<- min(Data,na.rm=T)
        XMAX <<- max(Data,na.rm=T)
        XMIN <- min(Data,na.rm=T)
        XMAX <- max(Data,na.rm=T)
        
        optimOUT <- foreach(j=1:100, .combine=rbind, .multicombine=TRUE,
                            .packages = c("data.table","dplyr","dfoptim","fitdistrplus","actuar","zipfR"),
                            .export = c("distlist","Data","min_KS","M","VAR","Data"),
                            .errorhandling = "remove") %dopar% {
            source("/FILEPATH/pdf_families.R")
            set.seed( as.integer((as.double(Sys.time())*1000+Sys.getpid()) %% 2^31+j) )
            counter = 0
            success <- FALSE
            while(!success) {
                counter = counter + 1
                params.i = runif(length(distlist),0,1)
                params.i = params.i/sum(params.i)
                print(paste0("Optim iteration ",j,", fit ",counter))
                out <- try(nmkb(params.i, min_KS, Data=Data, mean=M, variance=VAR, 
                                xmin=min(Data,na.rm=T), xmax=max(Data,na.rm=T), 
                                lower=0, upper=1, control=list(maxfeval=250)),silent=T)
                success <- class(out)=="list"
            }
            oo = data.table(params=out$par,vals=out$value,result=j)
            oo
        }
        
        bestPARS = optimOUT[ ,.SD[vals==min(vals)]]
        bestPARS = bestPARS %>% dplyr::group_by(vals) %>% mutate(rank = dense_rank((result))) %>% dplyr::filter(rank==1)
        bestPARS = bestPARS[,"params"][[1]]
        
        bestPARS_scaled = bestPARS/sum(bestPARS)
        
        get_weights <- function(bestPARS) {
            wts <- bestPARS
            wFIT <- wts
            for(z in length(wts):1){ 
                LENGTH <- length(formals(unlist(distlist[[z]]$mv2par)))
                if (LENGTH==4) { 
                    est <- try((unlist(distlist[[z]]$mv2par(M,VAR,XMIN=XMIN,XMAX=XMAX))),silent=T)
                } else {
                    est <- try((unlist(distlist[[z]]$mv2par(M,VAR))),silent=T)
                }
                if(class(est)=="try-error") {
                    wFIT[z] <- 0
                }
            }
            wFIT = wFIT/sum(wFIT)
            names(wFIT) <- names(distlist)
            return(wFIT)
        }
        
        bestWTS <- get_weights(bestPARS_scaled)
        bestWTS_vec <- bestWTS
        names(bestWTS_vec) <- NULL
        
        ########################################################################
        ## Calculate the D statistic
        ########################################################################

        Edensity <- get_edensity(weights=bestWTS, min=min(Data,na.rm=T), max=max(Data,na.rm=T), mean=M, variance=VAR, distlist = distlist)
        
        ## calculate D statistic on testing data
        cdfDATA <- ecdf(Data)
        x = seq(XMIN,XMAX,length=1000)
        ## cdf of Data at our x values
        cdfDATAYout <- cdfDATA(sort(x))
        
        ## now create CDF from our ensemble testing density at our x values
        den<-approxfun(x, Edensity, yleft=0, yright=0) #density function
        cdfFITYout = c()
        for(val in x) {
            v<-try(integrate(den, min(x), val)$value)
            if (class(v)=="try-error") {
                v <- NA
            }
            cdfFITYout <- append(cdfFITYout,v)
        }
        
        xloc <- which.max(abs(cdfDATAYout-cdfFITYout))
        D <- abs(cdfDATAYout[xloc]-cdfFITYout[xloc])
        
        bOUT <- t(data.frame(bestWTS))
        bOUT <- data.frame(bOUT)
        rownames(bOUT) <- NULL
        
        stopCluster(cl)
        
        return(list(best_weights=bOUT,KS_statistic=D))
        
    }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}

################################################################################
################################################################################
## Plot_KS_fit function
## Purpose:takes data, set of weights, mean, variance, xmin, xmax, and plots the the CDF 
## of both the data and ensemble fit along with the KS statistic.
################################################################################
################################################################################

plot_KS_fit <- function(Data,weights,mean,variance,xmin,xmax,title,dlist) {
    ## create CDF of Data
    cdfDATA <- ecdf(Data) 
    
    XMIN <<- xmin
    XMAX <<- xmax
    x = seq(xmin,xmax,length=1000)
    fx = 0*x
    wts = weights
    ## loop through weights and create density vector given mean and sd
    ## some fits fail to converge so here I test to make sure which distributions do fit and re-scale the weights if needed
    wFIT <- wts
    for(z in length(wts):1){
        distn = names(weights)[[z]]
        LENGTH <- length(formals(unlist(dlist[paste0(distn)][[1]]$mv2par)))
        if (LENGTH==4) {
            est <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(mean, variance, XMIN=XMIN, XMAX=XMAX)),silent=T)
        } else {
            est <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(mean, variance)),silent=T)
        }
        if(class(est)=="try-error") {
            wFIT[z] <- 0
        }
    }
    
    filter <- wFIT[!wFIT==0]
    wFIT = wFIT/sum(wFIT)
    
    ## loop through re-scaled weights and create density vector given mean and sd
    for(z in 1:length(wFIT)){
        distn = names(weights)[[z]]
        LENGTH <- length(formals(unlist(dlist[paste0(distn)][[1]]$mv2par)))
        if (LENGTH==4) {
            est <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(mean, variance, XMIN=XMIN, XMAX=XMAX)),silent=T)
        } else {
            est <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(mean, variance)),silent=T)
        }
        fxj = try(dlist[paste0(distn)][[1]]$dF(x,est),silent=T)
        if(class(est)=="try-error") {
            fxj = rep(0,length(fx))
        }
        fx = (fx + fxj*wFIT[[z]])
    }
    fx[is.infinite(fx)] <- 0 
    ## cdf of Data at our x values
    cdfDATAYout <- cdfDATA(sort(x))
    
    ## now create CDF from our ensemble density at our x values
    den<-approxfun(x, fx, yleft=0, yright=0) #density function
    cdfFITYout = c()
    
    for(val in x) {
        v<-try(integrate(den, min(x), val)$value)
        if (class(v)=="try-error") {
            v <- NA
        }
        cdfFITYout <- append(cdfFITYout,v)
    }
    
    xloc <- which.max(abs(cdfDATAYout-cdfFITYout))
    D <- abs(cdfDATAYout[xloc]-cdfFITYout[xloc])
    y0 <- cdfDATAYout[xloc]
    y1 <- cdfFITYout[xloc]
    xPOSITION <- x[which(cdfDATAYout == y0)[1]]
    
    plot(x,cdfDATAYout, verticals=TRUE, do.points=FALSE, col="dodgerblue3",main=title,ylab="CDF",xlab="X") 
    points(x,cdfFITYout, verticals=TRUE, do.points=FALSE, col="darkseagreen3", add=TRUE)
    points(c(xPOSITION, xPOSITION), c(y0, y1), pch=16, col="firebrick4") 
    segments(xPOSITION, y0, xPOSITION, y1, col="firebrick4", lty="dotted") 
}

################################################################################
################################################################################
## get_edensity: create density provided set of named weights, min, max, mean and variance of data, 
## along with distribution list sources from pdf_families.R
################################################################################
################################################################################

get_edensity <- function(weights,min,max,mean,variance,distlist) {
    XMIN <<- min
    XMAX <<- max
    x = seq(XMIN,XMAX,length=1000)
    fx = 0*x
    
    for(z in 1:length(weights)){
        distn = names(weights)[[z]]
        LENGTH <- length(formals(unlist(dlist[paste0(distn)][[1]]$mv2par)))
        if (LENGTH==4) {
            est <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(mean, variance, XMIN=XMIN, XMAX=XMAX)),silent=T)
        } else {
            est <- try(unlist(dlist[paste0(distn)][[1]]$mv2par(mean, variance)),silent=T)
        }
        fxj = try(distlist[paste0(distn)][[1]]$dF(x,est),silent=T)
        if(class(est)=="try-error") {
            fxj = rep(0,length(fx))
        }
        fx = (fx + fxj*weights[[z]])
    }
    
    fx[!is.finite(fx)] <- 0 
    return(fx)
}

################################################################################
################################################################################
## plot_fit: creates a plot of the ensemble density, histograms of data, and all of the distribution fits
## Args: Data=vector of data that was fit, weights=best performing ensemble weights, edensity=density vector provided by
## get_edensity along with distribution list sources from pdf_families.R
################################################################################
################################################################################

# edensity = ensemble density
plot_fit <- function(Data,weights,edensity,title,distlist) {
    XMIN <<- min(Data)
    XMAX <<- max(Data)
    ymax = max(hist(Data, plot=FALSE)$density)*1.5
    mnx = min(Data)
    mpx = quantile(Data,.99)[1]*1.5
    hist(Data, probability = TRUE, ylim = c(0, ymax), main = paste0(title), xlim=c(mnx,mpx), cex.main=1.5) 
    xx = seq(min(Data),max(Data), length.out=length(edensity))
    nD = length(weights)
    
    colourCount = nD
    getPalette = colorRampPalette(brewer.pal(12, "Paired"))
    col_vector = t(t(getPalette(colourCount)))
    
    wplot = sort(weights,decreasing=TRUE)
    x = seq(min(Data),max(Data), length.out=length(edensity))
    for(i in 1:nD){
        n = names(wplot)[[i]]
        w = round(wplot[[i]],digits=2)
        w = as.character(w)
        LENGTH <- length(formals(unlist(dlist[paste0(n)][[1]]$mv2par)))
        if (LENGTH==4) {
            f <- try(unlist(distlist[paste0(n)][[1]]$mv2par(mean(Data,na.rm=T), var(Data,na.rm=T),XMIN=min(Data,na.rm=T),XMAX=max(Data,na.rm=T))),silent=T)
        } else {
            f <- try(unlist(distlist[paste0(n)][[1]]$mv2par(mean(Data,na.rm=T), var(Data,na.rm=T))),silent=T)  
        }
        if(class(f)=="try-error") next
        c = col_vector[[i]]
        distlist[paste0(n)][[1]]$plotF(xx, f, col_vector[i])
        text(max(mpx)*0.9, ymax*(1-i/(nD+1)), paste0(w," ",n), col=paste0(c), cex=1.5)
    }
    lines(x, edensity, col = "black", lwd=5)
}
