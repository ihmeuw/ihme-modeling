#############################################################################################
## GATHER_DATA
## This function compiles the data from all available methods and  runs a LOESS' regression on the compiled data
## it outputs a compiled file and graphs showing the estimates by survey and by location
#############################################################################################

gather_data <- function(dir, grouping.categories, n.sims, filetags="", graphs="both") {

  startdir <- getwd()
  
  library(foreign)
  library(grid)
  library(lattice)
  setwd(dir)
  
  n.files <- length(filetags)
  grouping.categories <- unique(c("gbdregion", grouping.categories))
  
## Set up displayer
  displayer = function(x, heading = NULL, end = NULL) {
    if( is.null(heading) & !is.null(end) ) heading = paste("end =", end)
    output = paste(format(c(x, 10^6), scientific = FALSE), ". ", sep = "")[1]
    if( (x-1)/10 == trunc((x-1)/10) ) output = paste("[", heading, "]", output, sep = "")
    if( x/10 == trunc(x/10) ) output = paste(output, "\n")
    if( x == end ) output = paste(output, "\n")
    cat(output); flush.console()
  }

## Load MAC
  output.use <- output.use.sim <- NULL

  if(file.exists(paste("MAC_results", filetags[1], ".rdata", sep=""))) {
    cat("...loading MAC \n"); flush.console()
    ## compile all individual files
    temp1 <- temp2 <- NULL
    for (ii in 1:length(filetags)) {
      tag <- filetags[ii]
      load(paste("MAC_results", tag, ".rdata", sep=""))
      temp1 <- rbind(temp1, output)
      temp2 <- rbind(temp2, output.sim)
      rm(reftime, data.uncertainty, output, output.sim); gc()
    }
    output <- temp1
    output.sim <- temp2
    rm(temp1, temp2); gc()

    ## formatting
    output$method <- "MAC"
    output.sim$method <- "MAC"
    colnames(output) = gsub("reftime.pred", "reftime", colnames(output))
    colnames(output.sim) = gsub("reftime.pred", "reftime", colnames(output.sim))

    ## save 15-19 estimates and then remove from the dataset
    save.15to19 = subset(output, group == "15-19")
    save.15to19$t = save.15to19$svdate - save.15to19$reftime
    save.15to19$svdate <- round(save.15to19$svdate, 3)
    output = subset(output, group != "15-19")
    output.sim = subset(output.sim, group != "15-19")
    output = output[,colnames(output) != "group"]
    output.sim = output.sim[,colnames(output.sim) != "group"]
    
    output.use <- rbind(output.use, output)
    output.use.sim <- rbind(output.use.sim, output.sim)

    attributes(output.use$q5.pred) <- NULL 
    for (var in grep("q5.pred", names(output.use.sim))) attributes(output.use.sim[,var]) <- NULL 
    rm(output, output.sim); gc()
  }
  
## Load MAP
  if(file.exists(paste("MAP_results", filetags[1], ".rdata", sep=""))) {
    cat("...loading MAP \n"); flush.console()  
    # compile all individual files
    temp1 <- temp2 <- NULL
    for (ii in 1:length(filetags)) {
      tag <- filetags[ii]
      load(paste("MAP_results", tag, ".rdata", sep=""))
      
      output <- data.uncertainty[[1]][[1]]
      for (x in 2:length(data.uncertainty)) output <- rbind(output, data.uncertainty[[x]][[1]])
      output$method <- "MAP"
      colnames(output) = gsub("reftime.pred", "reftime", colnames(output))
      output = subset(output, reftime <= 25)

      output.sim = data.uncertainty[[1]][[2]]
      for(x in 2:length(data.uncertainty)) output.sim = rbind(output.sim, data.uncertainty[[x]][[2]])
      output.sim$method = "MAP"
      colnames(output.sim) = gsub("reftime.pred", "reftime", colnames(output.sim))
      output.sim = subset(output.sim, reftime <= 25)
    
      temp1 <- rbind(temp1, output)
      temp2 <- rbind(temp2, output.sim)
      rm(data.uncertainty); gc()
    }

    attributes(temp1$q5.pred) <- NULL   
    for (var in grep("q5.pred", names(temp2))) attributes(temp2[,var]) <- NULL   
        
    output.use <- rbind(output.use, temp1[,c(grouping.categories, "method", "reftime", names(temp1)[grepl("q5.pred", names(temp1), fixed=T)])])
    output.use.sim <- rbind(output.use.sim, temp2[,c(grouping.categories, "method", "reftime", names(temp2)[grepl("q5.pred", names(temp2), fixed=T)])])
    rm(temp1, temp2); gc()
  }


## Load TFBC
  if(file.exists(paste("TFBC_results", filetags[1], ".rdata", sep=""))) {
    cat("...loading TFBC \n"); flush.console()  
    ## compile all individual files
    temp1 <- temp2 <- NULL
    for (ii in 1:length(filetags)) {
      tag <- filetags[ii]
      load(paste("TFBC_results", tag, ".rdata", sep=""))
      temp1 <- rbind(temp1, output)
      temp2 <- rbind(temp2, output.sim)
      rm(reftime, data.uncertainty, output, output.sim); gc()
    }
    output <- temp1
    output.sim <- temp2
    rm(temp1, temp2); gc()

    ## formatting
    output$method <- "TFBC"
    output.sim$method <- "TFBC"
    colnames(output) = gsub("reftime.pred", "reftime", colnames(output))
    colnames(output.sim) = gsub("reftime.pred", "reftime", colnames(output.sim))

    attributes(output$q5.pred) <- NULL 
    for (var in grep("q5.pred", names(output.sim))) attributes(output.sim[,var]) <- NULL 
    
    output = output[,colnames(output) != "group"]
    output.sim = output.sim[,colnames(output.sim) != "group"]

    output.use <- rbind(output.use, output)
    output.use.sim <- rbind(output.use.sim, output.sim)
    rm(output, output.sim); gc()
  }
  
## Load TFBP
  if(file.exists(paste("TFBP_results", filetags[1], ".rdata", sep=""))) {
    cat("...loading TFBP \n"); flush.console()  
    ## compile all individual files
    temp1 <- temp2 <- NULL
    for (ii in 1:length(filetags)) {
      tag <- filetags[ii]
      load(paste("TFBP_results", tag, ".rdata", sep=""))

      output <- data.uncertainty[[1]][[1]]
      for (x in 2:length(data.uncertainty)) output <- rbind(output, data.uncertainty[[x]][[1]])
      output$method <- "TFBP"
      colnames(output) = gsub("reftime.pred", "reftime", colnames(output))
      output = subset(output, reftime <= 25)

      output.sim = data.uncertainty[[1]][[2]]
      for(x in 2:length(data.uncertainty)) output.sim = rbind(output.sim, data.uncertainty[[x]][[2]])
      output.sim$method = "TFBP"
      colnames(output.sim) = gsub("reftime.pred", "reftime", colnames(output.sim))
      output.sim = subset(output.sim, reftime <= 25)

      temp1 <- rbind(temp1, output)
      temp2 <- rbind(temp2, output.sim)
      rm(data.uncertainty); gc()
    }
    
    attributes(temp1$q5.pred) <- NULL   
    for (var in grep("q5.pred", names(temp2))) attributes(temp2[,var]) <- NULL   
        
    output.use <- rbind(output.use, temp1[,c(grouping.categories, "method", "reftime", names(temp1)[grepl("q5.pred", names(temp1), fixed=T)])])
    output.use.sim <- rbind(output.use.sim, temp2[,c(grouping.categories, "method", "reftime", names(temp2)[grepl("q5.pred", names(temp2), fixed=T)])])
    rm(temp1, temp2); gc()
  }

## Set up data for LOESS (weights)
  output.use$t = output.use$svdate - output.use$reftime
  output.use.sim$t = output.use.sim$svdate - output.use.sim$reftime
  output.use$svdate <- round(output.use$svdate, 3)
  output.use.sim$svdate <- round(output.use$svdate, 3)
  
  output = output.use
  output.sim = output.use.sim

  output = subset(output, !is.na(reftime))
  output.sim = subset(output.sim, !is.na(reftime))
  
  #create group variable by appending all grouping categories - usually iso3, svdate, and, for subnational, the location.id
  output$group <- output[,grouping.categories[1]]
  for (ii in 2:length(grouping.categories)) output$group <- paste(output$group, output[,grouping.categories[ii]], sep=" - ")
  output$group <- factor(output$group)
  output.sim$group <- output.sim[,grouping.categories[1]]
  for (ii in 2:length(grouping.categories)) output.sim$group <- paste(output.sim$group, output.sim[,grouping.categories[ii]], sep=" - ")
  output.sim$group <- factor(output.sim$group)
  
  #assign weights for loess based on 1/number of points in each method  - so each method is weighted equally
  output$weights = NA
  for(level in levels(output$group)) {
    temp = table(output[output$group == level, "method"]) 
    temp = 1/temp
    for(m in names(temp)) output[output$group == level & output$method == m, "weights"] = temp[m]
  }
  output.sim$weights = NA
  for(level in levels(output.sim$group)) {
    temp = table(output.sim[output.sim$group == level, "method"]) 
    temp = 1/temp
    for(m in names(temp)) output.sim[output.sim$group == level & output.sim$method == m, "weights"] = temp[m]
  }
  
  output = output[order(output$group, output$method, output$reftime),]
  output.sim = output.sim[order(output.sim$group, output.sim$method, output.sim$reftime),]
  
  output[] = lapply(output, function(x) if(is.factor(x)) factor(x) else x)
  output.sim[] = lapply(output.sim, function(x) if(is.factor(x)) factor(x) else x)

  loess.data = output.sim
  rm(list = objects()[!objects() %in% c("output", "loess.data", "displayer", "save.15to19", "grouping.categories", "n.sims", "filetags", "dir", "graphs", "startdir")]); gc()

## LOESS
  loess.uncertainty = vector("list", nlevels(loess.data$group))
  names(loess.uncertainty) = levels(loess.data$group)
  num.methods <- length(unique(loess.data$method))

  ## loop through surveys (defined by grouping.categories)
  for(j in levels(loess.data$group)) {
    ## loop through sims
    for(sim.no in 1:n.sims) {
      ## make predictions
      displayer(sim.no, heading = j, end = n.sims)
      q5.interest = paste("q5.pred.", sim.no, sep = "")
      temp = subset(loess.data, group == j)[,c("t", q5.interest, "weights")]
      colnames(temp)[2] = "q5"
      fit = loess(q5 ~ t, data = temp, weights = weights, span = 0.5)
      ## store predictions for each simulation
      if(sim.no == 1) {
        if (sum(!is.na(fit$fitted)) != 0) {
          intermediate.output = predict(fit, data.frame(t = seq(min(temp$t), max(temp$t), by = 0.2)))
          intermediate.output = data.frame(group = j, t = seq(min(temp$t), max(temp$t), by = 0.2), q5.pred.1 = intermediate.output)
        } else {
          intermediate.output = data.frame(group = j, t = seq(min(temp$t), max(temp$t), by = 0.2), q5.pred.1 = NA)
        }
        if(n.sims != 1) {
          temp = matrix(ncol = n.sims-1, nrow = nrow(intermediate.output))
          colnames(temp) = paste("q5.pred.", 2:n.sims, sep = "")
          intermediate.output = cbind(intermediate.output, temp)
        }
      } else {
        intermediate.output[,q5.interest] = predict(fit, data.frame(t = seq(min(temp$t), max(temp$t), by = 0.2)))
      }
    }
    ## format predictions; calculate uncertainty with correction
    if(n.sims != 1) {
      intermediate.output$uncert.mean = apply(intermediate.output[,grep("q5.pred", names(intermediate.output))], 1, mean, na.rm = TRUE)
      intermediate.output$uncert.sd = apply(intermediate.output[,grep("q5.pred", names(intermediate.output))], 1, sd, na.rm = TRUE)
      intermediate.output$uncert.sd = intermediate.output$uncert.sd*num.methods   
    
      summary.output =
      data.frame(intermediate.output[,c("group", "t")],
               q5.pred = intermediate.output$uncert.mean,
               q5.pred.upper = intermediate.output$uncert.mean + qnorm(0.05/2, lower.tail = FALSE)*intermediate.output$uncert.sd,
               q5.pred.lower = intermediate.output$uncert.mean - qnorm(0.05/2, lower.tail = FALSE)*intermediate.output$uncert.sd)               
    } else {
      summary.output = intermediate.output
      colnames(summary.output)[colnames(summary.output) == "q5.pred.1"] = "q5.pred"
    }
    loess.uncertainty[[j]] = summary.output
  }
  lapply(loess.uncertainty, head)

## Appends all methods together
  loess.uncertainty = do.call("rbind", loess.uncertainty)
  loess.uncertainty$method = "LOESS of all methods"
  loess.uncertainty = merge(unique(output[,c("group", grouping.categories)]), loess.uncertainty, all.y = TRUE)

  output = merge(output, loess.uncertainty, all = TRUE)
  if(file.exists(paste("MAC_results", filetags[1], ".rdata", sep=""))) output = merge(output, save.15to19, all = TRUE)

  #Keep needed variables and paste grouping categories together 
  if (n.sims == 1) output = output[,c(grouping.categories, "group", "method", "t", "q5.pred")] else output = output[,c(grouping.categories, "group", "method", "t", "q5.pred", "q5.pred.lower", "q5.pred.upper")]
  output$gbdregion[is.na(output$gbdregion)|output$gbdregion==""] <- "Other"
  output$group <- output[,grouping.categories[1]]
  for (ii in 2:length(grouping.categories)) output$group <- paste(output$group, output[,grouping.categories[ii]], sep=" - ")
  output$group <- factor(output$group)
  output <- output[order(output$group, output$method, output$t),]

## Save and make graphs
  if (graphs == "both" & length(unique(output$group)) == 1) graphs <- "survey" 
  
  setwd(dir)
  if (graphs %in% c("both", "survey")) { 
    pdf(paste("strResultsDir", "plots of indirect methods - by survey.pdf", sep = ""), onefile = TRUE, width = 11, height = 8.5)
    print(
    xyplot(q5.pred ~ t | group, output, groups = method,
          layout = c(1,1), auto.key = TRUE,
          scales = list(relation = "free"),
          panel = panel.superpose,
          panel.groups = function(x, y, group.number, type, lwd, ...) {
            if(group.number != 1) panel.xyplot(x, y, ...)
            if(group.number == 1) panel.xyplot(x, y, type = "l", lwd = 2, ...)
          })
    )
    dev.off()
  }
  
  #subset grouping categories to graph all survey years for same country on one graph
  grouping.categories <- grouping.categories[!grouping.categories%in%c("svdate","source")]
  output$group <- output[,grouping.categories[1]]
  for (ii in 2:length(grouping.categories)) output$group <- paste(output$group, output[,grouping.categories[ii]], sep=" - ")
  output$group[output$gbdregion == ""] <- paste("Other", output$group[output$gbdregion == ""], sep="")
  output$group <- factor(output$group)

  if (graphs %in% c("both", "country")) { 
    pdf(paste("strResultsDir", "plots of indirect methods - by country.pdf", sep = ""), onefile = TRUE, width = 11, height = 8.5)
    print(
    xyplot(q5.pred ~ t | group, output, groups = method,
          svdate = output$svdate,
          layout = c(1,1), auto.key = TRUE,
          scales = list(relation = "free"),
          panel = panel.superpose,
          panel.groups = function(x, y, svdate, group.number, subscripts, type, lwd, ...) {
            svdate = svdate[subscripts]
  
            if(group.number != 1) panel.xyplot(x, y, ...)
            if(group.number == 1) for(sv in unique(svdate)) panel.xyplot(x[svdate == sv], y[svdate == sv], type = "l", lwd = 2, ...)
          })
    )
    dev.off()
  } 
  
  output = output[,names(output)!="group"]
  write.dta(output, "all_methods_results.dta")

  setwd(startdir)
  return("gather data done!")

}  # end function
