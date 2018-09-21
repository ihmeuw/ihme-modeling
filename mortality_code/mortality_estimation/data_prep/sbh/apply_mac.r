#############################################################################################
## APPLY_MAC
## This function formats the data and then applies the Maternal Age Cohort method to it and saves the output to the specified file.
#############################################################################################

apply_mac <- function(dir, grouping.categories, all.women, subset=NULL, filetag=NULL, uncertainty=FALSE, n.sims=1, fund.uncert=FALSE, codedir = "strCodeDirectory" ) {

# Include libraries
  library(arm)
  library(foreign)

# Load MAC model parameters
  startdir <- getwd()
  setwd("strSurveyDirectory")
  source("strFunctionDirectory/02a. functions - RE model with uncertainty.r")
  

# load the model parameters

  ## output.mac
  output.mac.varnames <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.mac/output.mac.varnames.csv", sep=""),stringsAsFactors=FALSE)
  output.mac.fe <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.mac/output.mac.fe.csv", sep=""),stringsAsFactors=FALSE)
  output.mac.iso_re<- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.mac/output.mac.iso_re.csv", sep=""),stringsAsFactors=FALSE)
  output.mac.reg_re <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.mac/output.mac.reg_re.csv", sep=""),stringsAsFactors=FALSE)
  output.mac.errors <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.mac/output.mac.errors.csv", sep=""),stringsAsFactors=FALSE)
  
  ## output.mac.reftime
  output.mac.reftime.varnames <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.mac.reftime/output.mac.reftime.varnames.csv", sep=""))
  output.mac.reftime.fe <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.mac.reftime/output.mac.reftime.fe.csv", sep=""))

  setwd("strSurveyDataDirectory")
  age.grouping <- c("15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-49")
  
# Get GBD analytical regions for grouping
  grouping.categories <- unique(c("gbdregion", grouping.categories))

  codes <-  read.dta("strCountryCodesFile",convert.factor=F,convert.underscore=T)
  codes <- unique(codes[(codes$indic.cod == 1), c("iso3", "gbd.analytical.region.name")])  
  codes$gbdregion[grepl("Asia", codes$gbd.analytical.region.name)] <- "Asia"
  codes$gbdregion[grepl("Lat|Car", codes$gbd.analytical.region.name)] <- "Latin America and the Caribbean"
  codes$gbdregion[codes$gbd.analytical.region.name %in% c("Eastern Sub-Saharan Africa", "Southern Sub-Saharan Africa")] <- "Sub-Saharan Africa, South/East"
  codes$gbdregion[codes$gbd.analytical.region.name %in% c("Western Sub-Saharan Africa", "Central Sub-Saharan Africa")] <- "Sub-Saharan Africa, West/Central"
  codes$gbdregion[codes$gbd.analytical.region.name == "North Africa and Middle East"] <- "North Africa / Middle East"
  codes <- codes[order(codes$iso3),c("iso3", "gbdregion")]

# Load MAC data and merge on regions
  data.byage <- read.dta("MAC_totals.dta", convert.underscore = TRUE, convert.factors=FALSE)
  for (ii in 1:ncol(data.byage)) attributes(data.byage[,ii]) <- NULL
  colnames(data.byage)[grep("group", colnames(data.byage), value = FALSE)] = "group"
  data.byage <- merge(data.byage, codes, by="iso3", all.x=T)
  data.byage$gbdregion[is.na(data.byage$gbdregion)] <- ""
  data.byage <- data.byage[,c(grouping.categories, "group", "n.mothers", "ceb", "cd")]
  colnames(data.byage)[colnames(data.byage) == "n.mothers"] = "n"
  head(data.byage)
  
  if (!is.null(subset)) data.byage <- merge(data.byage, subset, by=grouping.categories[grouping.categories!="gbdregion"], all=F)

# Manipulate data to apply coefficients
  data.byage$cdceb = (data.byage$cd/data.byage$ceb)*1000
  data.byage$parity = data.byage$ceb/data.byage$n
  data.byage$ceb = data.byage$ceb/data.byage$n
  data.byage$cd = data.byage$cd/data.byage$n  

  parityratios = reshape(subset(data.byage, group %in% age.grouping[1:3], c(grouping.categories, "group", "parity")),
                         direction = "wide", idvar = grouping.categories,
                         timevar = "group")
  parityratios$pr1 = parityratios[,paste("parity", age.grouping[1], sep = ".")]/parityratios[,paste("parity", age.grouping[2], sep = ".")]
  parityratios$pr2 = parityratios[,paste("parity", age.grouping[2], sep = ".")]/parityratios[,paste("parity", age.grouping[3], sep = ".")]
  data.byage = merge(data.byage, parityratios[,c(grouping.categories, grep("pr", colnames(parityratios), value = TRUE))], all.x = TRUE)
  rm(parityratios)
  head(data.byage)
  
# Apply coefficients
# from 5q0 model
# calls prediction.sim
  age.grouping <- age.grouping[age.grouping %in% unique(data.byage$group)]
  data.uncertainty = lapply(age.grouping, function(ag) prediction.sim(data.input = data.byage, 
                                                                      varnames = output.mac.varnames[output.mac.varnames$group==ag,],
                                                                      fe = output.mac.fe[output.mac.fe$group==ag,],
                                                                      reg_re = output.mac.reg_re[output.mac.reg_re$group==ag,], 
                                                                      iso_re = output.mac.iso_re[output.mac.iso_re$group==ag,],
                                                                      errors = output.mac.errors[output.mac.errors$group==ag,], 
                                                                      group = ag,
                                                                      grouping.categories = grouping.categories, 
                                                                      uncertainty = uncertainty, 
                                                                      n.sims = n.sims, 
                                                                      method = "cohort"))
  names(data.uncertainty) = age.grouping
  


  # from reftime model
  reftime = 
  lapply(age.grouping, 
  function(g) {

    temp <- data.byage[data.byage$group==g,]
    temp.fe <- output.mac.reftime.fe[output.mac.reftime.fe$group==g,]
    temp$reftime.pred = temp$cdceb*temp.fe[temp.fe$param=="cdceb",]$fixef.fit +
                        temp$ceb*temp.fe[temp.fe$param=="ceb",]$fixef.fit +
                        temp$pr1*temp.fe[temp.fe$param=="pr1",]$fixef.fit +
                        temp$pr2*temp.fe[temp.fe$param=="pr2",]$fixef.fit + 
                        temp.fe[temp.fe$param=="(Intercept)",]$fixef.fit
    temp <- temp[,c(grouping.categories,c("group", "reftime.pred"))]
    return(temp)
  })
  names(reftime) = age.grouping
  

  for(x in 1:length(data.uncertainty)) for(y in 1:length(data.uncertainty[[x]])) data.uncertainty[[x]][[y]]$group = age.grouping[x]
  for(x in age.grouping) for(y in 1:length(data.uncertainty[[x]])) data.uncertainty[[x]][[y]] = merge(reftime[[x]], data.uncertainty[[x]][[y]], all = TRUE)
  
  output = do.call("rbind", lapply(data.uncertainty, function(x) x[[1]]))
  output.sim = do.call("rbind", lapply(data.uncertainty, function(x) x[[2]]))
  colnames(output.sim)[colnames(output.sim) == "q5.pred"] = "q5.pred.1"

# Save results
  save(output, output.sim, data.uncertainty, reftime, file = paste("MAC_results", filetag, ".rdata", sep=""))
  setwd(startdir)
  return("MAC done!")
  
}  # end function









