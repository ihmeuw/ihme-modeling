#############################################################################################################
## Description: this file contains the functions needed to fit the summary birth history models and to
##              produce estimates both in and out of sample. It can be used with either the ever-married
##              or the all-women samples -- this is set by whatever data or model is loaded in the code
##              that sources this file
#############################################################################################################

  set.seed(123)
  library(arm)
  library(plyr)

##################################################################################
# DISPLAYER
#
# Displays on the screen which simulation it's on
##################################################################################

  displayer = function(x, header = NULL, end = NULL) {
    if( is.null(header) & !is.null(end) ) header = paste("end =", end)
    if( is.null(header) & is.null(end) )  header = ""
    output = paste(format(c(x, 10^6), scientific = FALSE), ". ", sep = "")[1]
    if( (x-1)/10 == trunc((x-1)/10) ) output = paste("[", header, "]", output, sep = "")
    if( x/10 == trunc(x/10) ) output = paste(output, "\n")
    if( x == end ) output = paste(output, "\n")
    cat(output); flush.console()
  }
  
##################################################################################
# LOGIT
#
# logit(p) = logit(p/(1-p)).  Converts p = 0 to p = 0.001 (so it won't be undefined)
##################################################################################

  logit = function(p) {
    p[p == 0] = 0.001
    log(p/(1-p))
  }

##################################################################################
# MINI.SIM.RANEF
#
# Fits the model described on the data provided and performs a single simulation
# with this model (i.e. adds uncertainty from model parameters)
##################################################################################

  mini.sim.ranef = function(model.formula, data, method, group) {
    if(FALSE) {
      ## used to test the function
      model.formula = q5.true ~ cdceb + (1|iso3) + (1|gbdregion)
      data = bytime.list[["t.4"]]
      group = "t.4"
      method = "period"
    }

    ## transform dependent variable, if necessary
    dep.var = all.vars(model.formula)[1]
    if(sum(data[,dep.var] > 1, na.rm = TRUE) != 0) data[,dep.var] = data[,dep.var]/1000
    if(sum(data[,"cdceb"] > 1, na.rm = TRUE) != 0) data[,"cdceb"] = data[,"cdceb"]/1000
    data[,dep.var] = logit(data[,dep.var])
    data[,"cdceb"] = logit(data[,"cdceb"])

    ## keep only data for this model
    data = data[!is.na(data[,dep.var]),]
    if(method == "cohort") data = data[data$group == group,]

    ## fit the model
    fit = lmer(model.formula, data = data, REML = FALSE)
    
    ## extract the independent variables
    indep.var = attr(terms(fit), "term.labels")
        
    ## Obtain random effect posterior distribution for the mean fit,
    error.original = unlist(sigma.hat(fit)$sigma)
    grouping = names(fit@flist)
    names(error.original)[-1] = grouping
    
    re.dist.original <- vector("list", length(ranef(fit)))
    names(re.dist.original) <- grouping
    for (rr in 1:length(re.dist.original)) {
      re.dist.original[[rr]] <- cbind(ranef(fit)[[rr]], se.ranef(fit)[[rr]])
      names(re.dist.original[[rr]]) = c("RE.mean", "RE.sd")
      re.dist.original[[rr]]$RE.var = re.dist.original[[rr]]$RE.sd^2
      re.dist.original[[rr]][,names(re.dist.original)[[rr]]] = rownames(re.dist.original[[rr]])
    }

    ## Prediction for the given simulation
    b.draw = mvrnorm(1, fixef(fit), vcov(fit))      ## Random draw from the fixed effects
    x.draw = data
    x.draw$q5.pred = fit@X %*% b.draw
    for (rr in 1:length(re.dist.original)) {        ## Random draw from the random effects
      x.draw = merge(x.draw, re.dist.original[[rr]][c(names(re.dist.original)[rr], "RE.mean", "RE.var")], all.x = TRUE)
      x.draw[,paste("RE.", names(re.dist.original)[rr], sep="")] <- mvrnorm(1, x.draw$RE.mean, diag(x.draw$RE.var))
      x.draw <- x.draw[,names(x.draw)[!names(x.draw) %in% c("RE.mean", "RE.var")]]
      x.draw$q5.pred <- x.draw$q5.pred + x.draw[,paste("RE.", names(re.dist.original)[rr], sep="")] ## add in random effect
    }
    x.draw$q5.pred = invlogit(x.draw$q5.pred)*1000
    
    ## Clean up and output results
    x.draw = x.draw[order(x.draw$dhs.no),]
    list(prediction = x.draw[,if(method == "cohort") c("dhs.no", "group", "q5.pred") else c("dhs.no", "q5.pred")],
         original.fit = fit,
         re.dist = re.dist.original,
         error = error.original)
  }
    
##################################################################################
# SIM.RANEF
#
# Uses mini.sim.ranef to fit the described model with the data provided and to produce
# multiple simulations. Calculates estimates with uncertainty from these simulations.
##################################################################################

  sim.ranef = function(n.sims, model.formula, data, method, group = NULL, group.alt = NULL, save.sims = FALSE) {

    if(FALSE) {
      # used to test the function
      n.sims = 1
      model.formula = q5.true ~ cdceb + (1|iso3) + (1|gbdregion)
      data = bytime.list[["t.4"]]
      group.alt = "t.4"
      method = "period"
      group.alt <- NULL
      save.sims <- FALSE
    }

    if(method == "cohort") group.alt = NULL
    if(method == "period") group = NULL
  
    ## get a prediction for each sim and save model parameters
    for(sim.count in 1:n.sims) { ## loop through all sims
      displayer(sim.count, if(method == "cohort") group else group.alt, n.sims)
      
      if(sim.count == 1) { ## for first sim, save all model output (this is the same across sims, only preds change)
        temp = mini.sim.ranef(model.formula, data, method, group)
        output = temp[["prediction"]]
        fit = temp[["original.fit"]]
        re.dist = temp[["re.dist"]]
        error = temp[["error"]]
        colnames(output)[colnames(output) == "q5.pred"] = "q5.pred.1"
        
        if(n.sims != 1) { ## if there is more than one sim, we need to set up a matrix to hold all of the predictions
          temp = matrix(ncol = n.sims-1, nrow = nrow(output))
          colnames(temp) = paste("q5.pred.", 2:n.sims, sep = "")
          output = cbind(output, temp)
        }
      } else { ## for each subsequent sim past 1, we are only intrested in the prediction
        temp = mini.sim.ranef(model.formula, data, method, group)[["prediction"]][,"q5.pred"]
        output[,paste("q5.pred.", sim.count, sep = "")] = temp
      }
    } ## close loop through sims

    ## format results if we've done simulations (i.e. calculate quantiles and mean)
    if(n.sims != 1) {
      summary.output =
      data.frame(if(method == "cohort") output[,c("dhs.no", "group")] else dhs.no = output$dhs.no, 
               q5.pred = apply(output[,grep("q5.pred", colnames(output))], 1, mean),
               q5.pred.upper = apply(output[,grep("q5.pred", colnames(output))], 1, quantile, probs = 0.975), 
               q5.pred.lower = apply(output[,grep("q5.pred", colnames(output))], 1, quantile, probs = 0.025))
      colnames(summary.output)[1] = "dhs.no"
    } else {
      summary.output = NULL
    }

    ## output results
    if(save.sims) { ## if we did sims, include summary results and all simulations
        list(summary = summary.output,
                       sim.data = output,
                       original.fit = fit,
                       re.dist = re.dist,
                       error = error)
    } else { ## if we did not do sims, report only model parameters
        list(summary = summary.output,
             original.fit = fit,
             re.dist = re.dist,
             error = error)
    }
  }

##################################################################################
# PREDICTION.SIM
#
# prediction.sim uses either prediction.cohort or prediction.period to make predictions
# from the provided (already fit) models and data. If uncertainty is turned on, prediction.sim
# uses prediction.cohort or prediction.period repeatedly, and each of these functions produces
# simulations that include uncertainty from the model parameters. If uncertainty is turned off
# prediction.sim uses prediction.cohort or prediction.period once and each of these functions
# produces an estimate that doesn't include any simulated uncertainty
#
# NOTES:
#
# (1) prediction.cohort and prediction.period use a similar method to mini.sim.ranef to
# produce estimates with uncertainty. The difference is that the model is not refit (and the
# data we are predicting for may not have been used to fit the model at all) and there is
# the ability to produce an estimate directly without simulation which is lacking in
# mini.sim.ranef
# (2) this function is written to work with models that include both a country
# and a region random effect. 
##################################################################################

  prediction.cohort = function(data.input, varnames, fe, reg_re, iso_re, errors, group, grouping.categories, uncertainty) {
    test = FALSE
    if(test) {
      # used to test the function
      data.input = data.byage
      group = "15-19"
      grouping.categories = grouping.categories
      uncertainty = FALSE
      error = output.mac.errors[output.mac.errors$group=="15-19"]
      indep.var = output.mac.varnames[output.mac.varnames$type=="indepvar" & output.mac.varnames$group=="15-19",]
      dep.var.notrans = output.mac.varnames[output.mac.varnames$type=="depvar" & output.mac.varnames$group=="15-19",]
      dep.var = output.mac.varnames[output.mac.varnames$type=="tranform" & output.mac.varnames$group=="15-19",]
      output.var = paste(dep.var.notrans$varname, ".pred", sep = "")
      fe=fe[fe$group=="15-19",]
    }
    
    if(!test){

      error = errors
      indep.var = varnames[varnames$type=="indepvar",]
      dep.var.notrans = varnames[varnames$type=="depvar",] 
      dep.var = varnames[varnames$type=="tranform",]
      output.var = paste(dep.var.notrans$varname, ".pred", sep = "")
      iso_re = iso_re
      reg_re = reg_re

     }

    # make any necessary changes to the data
    reg_re$gbdregion <- as.character(reg_re$gbdregion)
    iso_re$iso3 <- as.character(iso_re$iso3)
    iso_re$group <- as.character(iso_re$group)
    reg_re$group <- as.character(reg_re$group)

    ## keep only the data that we want to apply this model to
    data.input = data.input[data.input$group == group,]
    index.temp = if(length(indep.var$varname) != 1) apply(is.na(data.input[,c(indep.var$varname)]), 1, sum) else is.na(data.input[,c(indep.var$varname)])
    index.temp = index.temp == 0
    data.input = data.input[index.temp,]
    data.input$q5 = 0

    ## transform data if necessary
    x.draw = data.input
    if(sum(x.draw$cdceb > 1, na.rm = TRUE) != 0) x.draw$cdceb = x.draw$cdceb/1000
    x.draw$cdceb = logit(x.draw$cdceb)
    x.draw[,toString(dep.var.notrans$varname)] = 0
    
 
    ## if we're not interested in uncertainty, predict directly from the saved model parameters
    if(!uncertainty) {
      
      # merge on random effects to make them easier to handle
      x.draw <- merge(x.draw, iso_re, by=c("iso3", "group"), all.x=TRUE)
      x.draw <- merge(x.draw, reg_re, by=c("gbdregion", "group"), all.x=TRUE)
      
      # if there is no region or iso effect, replace with 0
      x.draw[is.na(x.draw$iso3.re), c("iso3.re")] <- 0
      x.draw[is.na(x.draw$region.re), c("region.re")] <- 0
      
      # apply the model q5 ~ cdceb + ceb + pr1 + pr2 + (1|iso3) + (1|gbdregion)
      
      x.draw$q5 = x.draw$cdceb*fe[fe$param=="cdceb",]$fixef.fit. +
                  x.draw$ceb*fe[fe$param=="ceb",]$fixef.fit. + 
                  x.draw$pr1*fe[fe$param=="pr1",]$fixef.fit. +
                  x.draw$pr2*fe[fe$param=="pr2",]$fixef.fit. +
                  fe[fe$param=="(Intercept)",]$fixef.fit.  +   
                  x.draw$region.re +
                  x.draw$iso3.re 

    } 

    ## backtransform and format predictions
    x.draw$q5 = invlogit(x.draw$q5)*1000
    x.draw <- rename(x.draw, c("q5"="q5.pred"))
    x.draw[,c(grouping.categories, c("group", "q5.pred"))]
  }

########################################################
##### prediction.period
#######################################################
  
  prediction.period = function(data.input, 
                               varnames, 
                               fe, 
                               reg_re, 
                               iso_re, 
                               errors, 
                               group, 
                               grouping.categories, 
                               uncertainty) {
    test = FALSE
    if(test) {
      data.input = bytime.list[["t.20"]]
      group = "t.20"
      grouping.categories = grouping.categories
      uncertainty = FALSE
      error = output.map.errors[output.map.errors$group=="t.20",]
      indep.var = output.map.varnames[output.map.varnames$type=="indepvar" & output.map.varnames$group=="t.20",]
      dep.var.notrans = output.map.varnames[output.map.varnames$type=="depvar" & output.map.varnames$group=="t.20",]
      dep.var = output.map.varnames[output.map.varnames$type=="tranform" & output.map.varnames$group=="t.20",]
      output.var = paste(dep.var.notrans$varname, ".pred", sep = "")
      fe=output.map.fe[output.map.fe$group=="t.20",]
      reg_re <- output.map.reg_re[output.map.reg_re$group == "t.20",]
      iso_re <- output.map.iso_re[output.map.iso_re$group == "t.20",]      
    }
    
    if(!test){
      
      error = errors
      indep.var = varnames[varnames$type=="indepvar",]
      dep.var.notrans = varnames[varnames$type=="depvar",] 
      dep.var = varnames[varnames$type=="tranform",]
      output.var = paste(dep.var.notrans$varname, ".pred", sep = "")
      iso_re = iso_re
      reg_re = reg_re
      
    }
    
    # make any necessary changes to the data
    reg_re$gbdregion <- as.character(reg_re$gbdregion)
    iso_re$iso3 <- as.character(iso_re$iso3)
    iso_re$group <- as.character(iso_re$group)
    reg_re$group <- as.character(reg_re$group)
    
    ## keep only the data that we want to apply this model to
    index.temp = if(length(indep.var$varname) != 1) apply(is.na(data.input[,c(indep.var$varname)]), 1, sum) else is.na(data.input[,c(indep.var$varname)])
    index.temp = index.temp == 0
    data.input = data.input[index.temp,]
    data.input$q5 = 0
    
    ## transform data if necessary
    x.draw = data.input
    if(sum(x.draw$cdceb > 1, na.rm = TRUE) != 0) x.draw$cdceb = x.draw$cdceb/1000
    x.draw$cdceb = logit(x.draw$cdceb)
    x.draw[,toString(dep.var.notrans$varname)] = 0
    
    
    ## if we're not interested in uncertainty, predict directly from the saved model parameters
    if(!uncertainty) {
      
      # merge on random effects to make them easier to handle

      x.draw <- merge(x.draw, iso_re, by=c("iso3"), all.x=TRUE)
      x.draw <- merge(x.draw, reg_re, by=c("gbdregion"), all.x=TRUE)
      
      # if there is no region or iso effect, replace with 0
      x.draw[is.na(x.draw$iso3.re), c("iso3.re")] <- 0
      x.draw[is.na(x.draw$region.re), c("region.re")] <- 0
      
      # apply the model q5 ~ cdceb + ceb + pr1 + pr2 + (1|iso3) + (1|gbdregion)
      
      x.draw$q5.pred = x.draw$cdceb*fe[fe$param=="cdceb",]$fixef.fit. +
        fe[fe$param=="(Intercept)",]$fixef.fit.  +   
        x.draw$region.re +
        x.draw$iso3.re 
  
    } 

    ## backtransform and format predictions
    x.draw$q5.pred = invlogit(x.draw$q5.pred)*1000
    x.draw[,c(grouping.categories, "q5.pred")]
  }
  
  prediction.sim = 
  function(data.input, varnames, fe, reg_re, iso_re, errors, group, grouping.categories, uncertainty = FALSE, n.sims = NULL, method) {
    if(FALSE) {
      # used to test the function
      g = "15-19"
      data.input = subset(dhs.byage, group == g & holdout == 1)
      model = q5.models.holdout[[g]][[ho]]
      group = g
      grouping.categories = "dhs.no"
      uncertainty = TRUE
      n.sims = 10
      method = "cohort"
    }

    ## choose method
    if(method == "cohort") prediction = prediction.cohort
    if(method == "period") prediction = prediction.period
    
    ## if we're not interested in uncertainty, predict only once and don't do any simulation
    if(!uncertainty) {
      summary.output = prediction(data.input = data.input, 
                                  varnames = varnames, 
                                  fe = fe, 
                                  reg_re = reg_re, 
                                  iso_re = iso_re, 
                                  errors = errors, 
                                  group = group, 
                                  grouping.categories = grouping.categories, 
                                  uncertainty = uncertainty)
      
    ## if we are interested in uncertainty, predict multiple times simulating from the model parameters
    } else {
      for(sim.count in 1:n.sims) {
        displayer(sim.count, group, n.sims)
        
        if(sim.count == 1) { ## after first sim, set up matrix to hold subsequent sims
          output = prediction(data.input = data.input,  varnames = varnames, 
                              fe = fe, 
                              reg_re = reg_re, 
                              iso_re = iso_re, 
                              errors = errors,
                              group = group, 
                              grouping.categories = grouping.categories, 
                              uncertainty = uncertainty)
          
          colnames(output)[colnames(output) == "q5.pred"] = "q5.pred.1"
          temp = matrix(ncol = n.sims-1, nrow = nrow(output))
          colnames(temp) = paste("q5.pred.", 2:n.sims, sep = "")
          output = cbind(output, temp)
        } else { ## produce subsequent sims
          temp = prediction(data.input = data.input, varnames = varnames, 
                            fe = fe, 
                            reg_re = reg_re, 
                            iso_re = iso_re, 
                            errors = errors,
                            group = group, 
                            grouping.categories = grouping.categories, 
                            uncertainty = uncertainty)
          output[,paste("q5.pred.", sim.count, sep = "")] = temp$q5.pred
        }
      }

      ## format output by finding the mean and quantiles of the simulations
      summary.output =
      data.frame(output[,grouping.categories], 
                 q5.pred = apply(output[,grep("q5.pred", colnames(output))], 1, mean, na.rm = TRUE),
                 q5.pred.upper = apply(output[,grep("q5.pred", colnames(output))], 1, quantile, probs = 0.975, na.rm = TRUE), 
                 q5.pred.lower = apply(output[,grep("q5.pred", colnames(output))], 1, quantile, probs = 0.025, na.rm = TRUE))
                 
      if(length(grouping.categories) == 1) colnames(summary.output)[1] = grouping.categories
    }

    ## output results
    list(summary = summary.output, sim.data = if(uncertainty) output else summary.output)
  }
      
    
