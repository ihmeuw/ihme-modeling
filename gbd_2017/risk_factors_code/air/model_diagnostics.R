#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: air_pm
# Purpose: Helper functions that extract model fits from Stan and then create some diagnostic plots
#***********************************************************************************************************************
 
#***********************************************************************************************************************
#this function will extract draws of the parameters fit in the stan model and then create some diagnostic plots of them:
#1. histograms of the parameter distributions
#2. chain diagnostic plots to show how the model converges
#the function will then save a csv of the parameter draws and then return the draws as a list
extractDraws <- function(mod_fit,
                         name,
                         model.parameters,
                         sources,
                         b,
                         tmrel=sample(tmrel.file[[1]], draws.required),
                         output.draws=T,
                         ...) {
  
  # extract parameters into an R list
  draws <- extract(mod_fit, inc_warmup=FALSE)
  
  # append the tmrel
  draws.df <- data.frame(draws)
  draws.df$tmrel=tmrel
  if(is.null(draws.df$b)){draws.df$b=b}
  
  # extract chains with warmup for better diagnostic plots
  draws_all <- extract(mod_fit, permuted=FALSE, inc_warmup=TRUE)

    # draw histograms of each parameter and plots of chain diagnostics
    grid.newpage()
    pushViewport(viewport(layout=grid.layout(2, length(model.parameters[1:3]))))
    
    for (i in 1:length(model.parameters)) {

      p <- model.parameters[i]
      
      #delta will have its own plot, due to need of by-source
      if (p=="delta") {
        
        #create graphs of the deltas, by source
        #parameter histograms
        deltas <- data.frame(value=draws[["delta"]]) %>% melt %>% as.data.table
        
        
        find <- as.character(unique(deltas$variable))
        replace <- sources
        
        deltas <- findAndReplace(deltas, 
                                 input.vector=find, 
                                 output.vector=replace, 
                                 input.variable.name="variable", 
                                 output.variable.name="source")
        
        delta.plot <- ggplot(deltas, aes(x=value, fill=source)) + geom_density(alpha=0.5) +
          xlim(0, quantile(draws[["delta"]], .975)) +
          ggtitle(paste0(name,': delta by source'))
        
        print(delta.plot)
        
      } else {
        
      #parameter histograms
      df1 <- data.frame(value=draws[[p]])
      
      plt1 <- ggplot(df1, aes(x=value)) + geom_density(fill='steelblue', alpha=0.5) + 
        ggtitle(paste0(name,'_',p))
      
      #print and set on upper row of viewport
      print(plt1, vp=viewport(layout.pos.row=1, layout.pos.col=i))
      
      #chain diagnostics
      df2 <- data.frame(value=unlist(lapply(1:chains, function(c) as.matrix(draws_all[,,p][,c]))))
      df2$chain <- as.factor(rep(1:chains, each=floor(iter / thin)))
      df2$draw <- rep(1:floor(iter / thin), chains) * thin
      
      plt2 <- ggplot(df2, aes(x=draw, y=value, color=chain)) + 
        geom_line(alpha=0.5) + 
        scale_y_log10() + 
        ggtitle(paste0(name,'_',p)) + 
        geom_vline(xintercept=warmup, alpha=0.5, linetype='longdash')
      
      #print and set on lower row of viewport
      print(plt2, vp=viewport(layout.pos.row=2, layout.pos.col=i))
      
      }
      
    }
      
  if (output.draws==T) {
       draws.df.lite <- draws.df[, -grep("predicted_RR*",names(draws.df))]
    draws.df.lite$lp__ <- NULL
    # export as a .csv to feed into PAF calculation
    write.csv(draws.df.lite, paste0(output.dir,"/params_",name,".csv"))
  }
  
  return(draws)
  
}
#***********************************************************************************************************************
 
#***********************************************************************************************************************
#this function will extract predictions created by the stan model and then create diagnostic plots of them:
#1a. scatter showing the predicted RRs vs observed RRs
#1b. scatter showing the predicted RRs vs observed RRs, but with the x being the midpoint of the contrast instead of exposed
#1c. scatter showing the predicted RRs vs observed RRs, but with the x being a line of the entire contrast
#2a-c. same plots with a truncated version (typically less than 300ug/m3 - values relevant to ambient air)
extractPred <- function(mod_fit,
                        name,
                        input.dir=data.dir,
                        output.pred=T,
                        output.graphs=T,
                        phase,
                        tmrel=tmrel.mean,
                        b) {
  
  pred.values=get(paste0("test.exposure.",phase))
  
  # make a dataframe of all the estimates and data
  # extract data
  rr_draws <- extract(mod_fit, par=c('predicted_RR'))[['predicted_RR']]
  # empty dataframe
  rr_data <- data.table(exposure=numeric(0), cf_exposure=numeric(0), mn=numeric(0), lower=numeric(0), upper=numeric(0), observed=numeric(0), Size=numeric(0), Type=character(0))
  # extract predictions
  for (i in 1:length(pred.values)) {
    draws <- rr_draws[,i]
    e <- pred.values[i]
    tmp <- data.frame(
      exposure=e,
      cf_exposure=NA,
      mn=mean(draws),
      lower=quantile(draws, .025),
      upper=quantile(draws, .975),
      observed=NA,
      Size=NA,
      Type=NA
    )
    rr_data <- rbind(rr_data, tmp)
  }
  
  # reload data (creates object called draw.data)
  load(file.path(input.dir, paste0(name,'.RData')))
  
  # append on observed
  for (i in 1:dim(draw.data)[1]) {
    sz <- 1 / draw.data[i, log_se]
    tmp <- data.frame(
      exposure=draw.data[i, conc],
      cf_exposure=draw.data[i, conc_den],
      mn=NA,
      lower=NA,
      upper=NA,
      observed=exp(draw.data[i, log_rr]),
      Size=sz,
      Type=draw.data[i, source]
    )
    rr_data <- rbind(rr_data, tmp)
  }

  
  # plot a truncated version (less than 120 ug/m3)
  
  plotPred <- function(exposure.max) {
    
    grid.newpage()
    pushViewport(viewport(layout=grid.layout(2, 2)))
    
    colors <- c("OAP"="#F8766D","HAP"="#7CAE00","SHS"="#00BFC4","AS"="#C77CFF")
    
    # first plot is RR vs exposure
    p1 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_point(aes(y=observed, size=Size, color=Type)) + 
      ggtitle(paste0(name)) + xlab('exposure') + ylab('RR') + 
      scale_color_manual(values=colors)
    
    # second plot is RR vs a line showing the exposure contrast (exposure to cf_exposure)
    p2 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_errorbarh(aes(xmax=exposure, xmin=cf_exposure, y=observed, alpha=Size, color=Type)) + 
      ggtitle(paste0(name)) + xlab('exposure vs control') + ylab('RR')+ 
      scale_color_manual(values=colors)
    
    # third plot is RR vs exposure contrast midpoint
    p3 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_point(aes(y=observed, x=(exposure+cf_exposure)/2, size=Size, color=Type)) + 
      ggtitle(paste0(name)) + xlab('contrast mid = (exposure+cf_exposure)/2') + ylab('RR')+ 
      scale_color_manual(values=colors)
    
    # fourth plot is RR vs exposure with the x logged
    p4 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_point(aes(y=observed, x=(exposure+cf_exposure)/2, size=Size, color=Type)) +
      scale_x_log10() +
      ggtitle(paste0(name)) + xlab('log(exposure)') + ylab('RR')+ 
      scale_color_manual(values=colors)
    
    
    #print and set on upper row of viewport
    print(p1 + theme(legend.position="none"), vp=viewport(layout.pos.row=1, layout.pos.col=1))
    print(p2 + theme(legend.position="none"), vp=viewport(layout.pos.row=2, layout.pos.col=1))
    print(p3, vp=viewport(layout.pos.row=1, layout.pos.col=2))
    print(p4, vp=viewport(layout.pos.row=2, layout.pos.col=2))
    
    
  }
  
  lapply(c(120, pred.values %>% max), plotPred)
  
  #add another page with phase 1 plotted on top of phase 2
  if(phase=="phaseII"){
    grid.newpage()
    pushViewport(viewport(layout=grid.layout(1, 2)))
    
    # first plot is RR vs exposure
    p1 <- ggplot(rr_data[exposure <= 120], aes(x=exposure)) +
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_abline(slope=b, intercept=1-b*tmrel.mean, color='red') +
      ggtitle(paste0(name)) + xlab('exposure') + ylab('RR')
    
    
    #second plot is RR vs exposure with the x logged
    p2 <- ggplot(rr_data[exposure <= 120], aes(x=exposure)) + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_abline(slope=b, intercept=(1-b*tmrel.mean), color='red') +
      scale_x_log10() +
      ggtitle(paste0(name)) + xlab('log(exposure)') + ylab('RR')
    
    
    #print and set on upper row of viewport
    print(p1 + theme(legend.position="none"), vp=viewport(layout.pos.row=1, layout.pos.col=1))
    print(p2, vp=viewport(layout.pos.row=1, layout.pos.col=2))
    
  }
  
  
  
  return(rr_data)
  
}


#------------------------------Old Functions-----------------------------------------------------------------------------------------
#***********************************************************************************************************************
#this function will extract draws of the parameters fit in the stan model and then create some diagnostic plots of them:
#1. histograms of the parameter distributions
#2. chain diagnostic plots to show how the model converges
#the function will then save a csv of the parameter draws and then return the draws as a list
extractDrawsOld <- function(mod_fit,
                         name,
                         model.parameters,
                         sources,
                         tmrel=sample(tmrel.file[[1]], draws.required),
                         output.draws=T,
                         ...) {
  
  # extract parameters into an R list
  draws <- extract(mod_fit, inc_warmup=FALSE)
  
  # append the tmrel
  draws.df <- data.frame(draws)
  draws.df$tmrel=tmrel
  
  # extract chains with warmup for better diagnostic plots
  draws_all <- extract(mod_fit, permuted=FALSE, inc_warmup=TRUE)
  
  # draw histograms of each parameter and plots of chain diagnostics
  grid.newpage()
  pushViewport(viewport(layout=grid.layout(2, length(model.parameters[1:3]))))
  
  for (i in 1:length(model.parameters)) {
    
    p <- model.parameters[i]
    
    #delta will have its own plot, due to need of by-source
    if (p=="delta") {
      
      #create graphs of the deltas, by source
      #parameter histograms
      deltas <- data.frame(value=draws[["delta"]]) %>% melt %>% as.data.table
      
      
      find <- as.character(unique(deltas$variable))
      replace <- sources
      
      deltas <- findAndReplace(deltas, 
                               input.vector=find, 
                               output.vector=replace, 
                               input.variable.name="variable", 
                               output.variable.name="source")
      
      delta.plot <- ggplot(deltas, aes(x=value, fill=source)) + geom_density(alpha=0.5) +
        xlim(0, quantile(draws[["delta"]], .975)) +
        ggtitle(paste0(name,': delta by source'))
      
      print(delta.plot)
      
    } else {
      
      #parameter histograms
      df1 <- data.frame(value=draws[[p]])
      
      plt1 <- ggplot(df1, aes(x=value)) + geom_density(fill='steelblue', alpha=0.5) + 
        ggtitle(paste0(name,'_',p))
      
      #print and set on upper row of viewport
      print(plt1, vp=viewport(layout.pos.row=1, layout.pos.col=i))
      
      #chain diagnostics
      df2 <- data.frame(value=unlist(lapply(1:chains, function(c) as.matrix(draws_all[,,p][,c]))))
      df2$chain <- as.factor(rep(1:chains, each=floor(iter / thin)))
      df2$draw <- rep(1:floor(iter / thin), chains) * thin
      
      plt2 <- ggplot(df2, aes(x=draw, y=value, color=chain)) + 
        geom_line(alpha=0.5) + 
        scale_y_log10() + 
        ggtitle(paste0(name,'_',p)) + 
        geom_vline(xintercept=warmup, alpha=0.5, linetype='longdash')
      
      #print and set on lower row of viewport
      print(plt2, vp=viewport(layout.pos.row=2, layout.pos.col=i))
      
    }
    
  }
  
  if (output.draws==T) {
    draws.df.lite <- draws.df[, c(model.parameters[1:3], 'tmrel')]
    # export as a .csv to feed into PAF calculation
    write.csv(draws.df.lite, paste0(output.dir,"/params_",name,".csv"))
  }
  
  return(draws)
  
}
#***********************************************************************************************************************

#***********************************************************************************************************************
#this function will extract predictions created by the stan model and then create diagnostic plots of them:
#1a. scatter showing the predicted RRs vs observed RRs
#1b. scatter showing the predicted RRs vs observed RRs, but with the x being the midpoint of the contrast instead of exposed
#1c. scatter showing the predicted RRs vs observed RRs, but with the x being a line of the entire contrast
#2a-c. same plots with a truncated version (typically less than 300ug/m3 - values relevant to ambient air)
extractPredOld <- function(mod_fit,
                        name,
                        pred.values=test.exposure,
                        input.dir=data.dir,
                        output.pred=T,
                        output.graphs=T) {
  
  # make a dataframe of all the estimates and data
  # extract data
  rr_draws <- extract(mod_fit, par=c('predicted_RR'))[['predicted_RR']]
  # empty dataframe
  rr_data <- data.table(exposure=numeric(0), cf_exposure=numeric(0), mn=numeric(0), lower=numeric(0), upper=numeric(0), observed=numeric(0), Size=numeric(0), Type=character(0))
  # extract predictions
  for (i in 1:length(pred.values)) {
    draws <- rr_draws[,i]
    e <- pred.values[i]
    tmp <- data.frame(
      exposure=e,
      cf_exposure=NA,
      mn=mean(draws),
      lower=quantile(draws, .025),
      upper=quantile(draws, .975),
      observed=NA,
      Size=NA,
      Type=NA
    )
    rr_data <- rbind(rr_data, tmp)
  }
  
  # reload data (creates object called draw.data)
  load(file.path(input.dir, paste0(name,'.RData')))
  
  # append on observed
  for (i in 1:dim(draw.data)[1]) {
    sz <- 1 / draw.data[i, log_se]
    tmp <- data.frame(
      exposure=draw.data[i, conc],
      cf_exposure=draw.data[i, conc_den],
      mn=NA,
      lower=NA,
      upper=NA,
      observed=exp(draw.data[i, log_rr]),
      Size=sz,
      Type=draw.data[i, source]
    )
    rr_data <- rbind(rr_data, tmp)
  }

  # plot a truncated version (less than 300 ug/m3)
  
  plotPred <- function(exposure.max) {
    
    colors <- c("OAP"="#F8766D","HAP"="#7CAE00","SHS"="#00BFC4","AS"="#C77CFF")
    
    grid.newpage()
    pushViewport(viewport(layout=grid.layout(2, 2)))
    
    # first plot is RR vs exposure
    p1 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_point(aes(y=observed, size=Size, color=Type),na.rm=T) + 
      ggtitle(paste0(name)) + xlab('exposure') + ylab('RR')+
      scale_color_manual(values=colors)+
      scale_y_continuous(limits=c(1,rr_data[exposure <= exposure.max,3*max(upper,na.rm=T)]))
    
    # second plot is RR vs a line showing the exposure contrast (exposure to cf_exposure)
    p2 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_errorbarh(aes(xmax=exposure, xmin=cf_exposure, y=observed, alpha=Size, color=Type),na.rm=T) + 
      ggtitle(paste0(name)) + xlab('exposure vs control') + ylab('RR')+
      scale_color_manual(values=colors)+
      scale_y_continuous(limits=c(1,rr_data[exposure <= exposure.max,3*max(upper,na.rm=T)]))
    
    # third plot is RR vs exposure contrast midpoint
    p3 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_point(aes(y=observed, x=(exposure+cf_exposure)/2, size=Size, color=Type),na.rm=T) + 
      ggtitle(paste0(name)) + xlab('contrast mid = (exposure+cf_exposure)/2') + ylab('RR')+
      scale_color_manual(values=colors)+
      scale_y_continuous(limits=c(1,rr_data[exposure <= exposure.max,3*max(upper,na.rm=T)]))
    
    # third plot is RR vs exposure with the x logged
    p4 <- ggplot(rr_data[exposure <= exposure.max], aes(x=exposure)) + 
      geom_ribbon(aes(ymin=lower, ymax=upper), alpha=0.5, color='steelblue', fill='steelblue') + 
      geom_line(aes(y=mn), alpha=0.5, color='navy') + 
      geom_point(aes(y=observed, x=(exposure+cf_exposure)/2, size=Size, color=Type),na.rm=T) +
      scale_x_log10() +
      ggtitle(paste0(name)) + xlab('log(exposure)') + ylab('RR')+
      scale_color_manual(values=colors)+
      scale_y_continuous(limits=c(1,rr_data[exposure <= exposure.max,3*max(upper,na.rm=T)]))
    
    
    #print and set on upper row of viewport
    print(p1 + theme(legend.position="none"), vp=viewport(layout.pos.row=1, layout.pos.col=1))
    print(p2 + theme(legend.position="none"), vp=viewport(layout.pos.row=2, layout.pos.col=1))
    print(p3, vp=viewport(layout.pos.row=1, layout.pos.col=2))
    print(p4, vp=viewport(layout.pos.row=2, layout.pos.col=2))
    
  }
  
  lapply(c(300, test.exposure %>% max), plotPred)
  
  return(rr_data)
  
}
