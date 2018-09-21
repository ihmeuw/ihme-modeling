#############################################################################################
## APPLY_TFBC
## This function formats the data and then applies the Time Since First Birth Cohort method to it and saves the output to the specified file.
#############################################################################################

apply_tfbc <- function(dir, grouping.categories, all.women, subset=NULL, filetag=NULL, uncertainty=FALSE, n.sims=1, fund.uncert=FALSE, codedir = "strCodeDirectory") {

# Include libraries
  library(arm)
  library(foreign)

# Load TFBC model parameters
  startdir <- getwd()
  setwd("strSurveyDirectory")
  source("strFunctionDirectory/02a. functions - RE model with uncertainty.r")
  setwd("strSurveyDataDirectory")
  timefb.grouping <- c("0-4", "5-9", "10-14", "15-19", "20-24", "25-29", "30-34")


  ##  bring in the coefficients for output.tfbc
  output.tfbc.varnames <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.tfbc/output.tfbc.varnames.csv", sep=""),stringsAsFactors=FALSE )
  output.tfbc.fe <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.tfbc/output.tfbc.fe.csv", sep=""),stringsAsFactors=FALSE )
  output.tfbc.iso_re<- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.tfbc/output.tfbc.iso_re.csv", sep=""),stringsAsFactors=FALSE )
  output.tfbc.reg_re <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.tfbc/output.tfbc.reg_re.csv", sep=""),stringsAsFactors=FALSE )
  output.tfbc.errors <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.tfbc/output.tfbc.errors.csv", sep=""),stringsAsFactors=FALSE )
  
  ## output.tfbc.reftime
  output.tfbc.reftime.varnames <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.tfbc.reftime/output.tfbc.reftime.varnames.csv", sep=""))
  output.tfbc.reftime.fe <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.tfbc.reftime/output.tfbc.reftime.fe.csv", sep=""))

    
#  Get GBD analytical regions for grouping
  grouping.categories <- unique(c("gbdregion", grouping.categories))

  codes <-  read.dta("strCountryCodesFile",convert.factor=F,convert.underscore=T)
  codes <- unique(codes[(codes$indic.cod == 1), c("iso3", "gbd.analytical.region.name")])
  codes$gbdregion[grepl("Asia", codes$gbd.analytical.region.name)] <- "Asia"
  codes$gbdregion[grepl("Lat|Car", codes$gbd.analytical.region.name)] <- "Latin America and the Caribbean"
  codes$gbdregion[codes$gbd.analytical.region.name %in% c("Eastern Sub-Saharan Africa", "Southern Sub-Saharan Africa")] <- "Sub-Saharan Africa, South/East"
  codes$gbdregion[codes$gbd.analytical.region.name %in% c("Western Sub-Saharan Africa", "Central Sub-Saharan Africa")] <- "Sub-Saharan Africa, West/Central"
  codes$gbdregion[codes$gbd.analytical.region.name == "North Africa and Middle East"] <- "North Africa / Middle East"
  codes <- codes[order(codes$iso3),c("iso3", "gbdregion")]
  
# Load TFBC data and merge on regions
  data.bytimefb <- read.dta("TFBC_totals.dta", convert.underscore = TRUE, convert.factors=F)
  for (ii in 1:ncol(data.bytimefb)) attributes(data.bytimefb[,ii]) <- NULL
  colnames(data.bytimefb)[grep("group", colnames(data.bytimefb), value = FALSE)] = "group"
  data.bytimefb <- merge(data.bytimefb, codes, by="iso3", all.x=T)
  data.bytimefb$gbdregion[is.na(data.bytimefb$gbdregion)] <- ""
  data.bytimefb <- data.bytimefb[,c(grouping.categories, "group", "n.mothers", "ceb", "cd")]
  colnames(data.bytimefb)[colnames(data.bytimefb) == "n.mothers"] = "n"
  head(data.bytimefb)
  
  if (!is.null(subset)) data.bytimefb <- merge(data.bytimefb, subset, by=grouping.categories[grouping.categories!="gbdregion"], all=F)

# Manipulate data to apply coefficients
  data.bytimefb$cdceb = (data.bytimefb$cd/data.bytimefb$ceb)*1000
  data.bytimefb$parity = data.bytimefb$ceb/data.bytimefb$n
  data.bytimefb$ceb = data.bytimefb$ceb/data.bytimefb$n
  data.bytimefb$cd = data.bytimefb$cd/data.bytimefb$n

  parityratios = reshape(subset(data.bytimefb, group %in% timefb.grouping[1:5], c(grouping.categories, "group", "parity")),
                         direction = "wide", idvar = grouping.categories,
                         timevar = "group")
  parityratios$pr1 = parityratios[,paste("parity", timefb.grouping[1], sep = ".")]/parityratios[,paste("parity", timefb.grouping[2], sep = ".")]
  parityratios$pr2 = parityratios[,paste("parity", timefb.grouping[2], sep = ".")]/parityratios[,paste("parity", timefb.grouping[3], sep = ".")]
  data.bytimefb = merge(data.bytimefb, parityratios[,c(grouping.categories, grep("pr", colnames(parityratios), value = TRUE))], all.x = TRUE)
  rm(parityratios)
  head(data.bytimefb)

# Apply coefficients
  # from 5q0 model
  timefb.grouping <- timefb.grouping[timefb.grouping %in% unique(data.bytimefb$group)]
  data.uncertainty = lapply(timefb.grouping,
                            function(ag) prediction.sim(data.input = data.bytimefb, 
                                                        varnames = output.tfbc.varnames[output.tfbc.varnames$group==ag,],
                                                        fe = output.tfbc.fe[output.tfbc.fe$group==ag,],
                                                        reg_re = output.tfbc.reg_re[output.tfbc.reg_re$group==ag,], 
                                                        iso_re = output.tfbc.iso_re[output.tfbc.iso_re$group==ag,],
                                                        errors = output.tfbc.errors[output.tfbc.errors$group==ag,], 
                                                        group = ag,
                                                        grouping.categories = grouping.categories, 
                                                        uncertainty = uncertainty, 
                                                        n.sims = n.sims, 
                                                        method = "cohort"))



  names(data.uncertainty) = timefb.grouping


reftime = 
  lapply(timefb.grouping, 
         function(g) {
           temp <- data.bytimefb[data.bytimefb$group==g,]
           temp.fe <- output.tfbc.reftime.fe[output.tfbc.reftime.fe$group==g,]
           temp$reftime.pred = temp$cdceb*temp.fe[temp.fe$param=="cdceb",]$fixef.fit +
           temp$ceb*temp.fe[temp.fe$param=="ceb",]$fixef.fit +
           temp$pr1*temp.fe[temp.fe$param=="pr1",]$fixef.fit +
           temp$pr2*temp.fe[temp.fe$param=="pr2",]$fixef.fit + 
           temp.fe[temp.fe$param=="(Intercept)",]$fixef.fit
           temp <- temp[,c(grouping.categories,c("group", "reftime.pred"))]
           return(temp)
         })

  names(reftime) = timefb.grouping


  for(x in 1:length(data.uncertainty)) for(y in 1:length(data.uncertainty[[x]])) data.uncertainty[[x]][[y]]$group = timefb.grouping[x]
  for(x in timefb.grouping) for(y in 1:length(data.uncertainty[[x]])) data.uncertainty[[x]][[y]] = merge(reftime[[x]], data.uncertainty[[x]][[y]], all = TRUE)

  output = do.call("rbind", lapply(data.uncertainty, function(x) x[[1]]))
  output.sim = do.call("rbind", lapply(data.uncertainty, function(x) x[[2]]))
  colnames(output.sim)[colnames(output.sim) == "q5.pred"] = "q5.pred.1"

# Save results
  save(output, output.sim, data.uncertainty, reftime, file = paste("TFBC_results", filetag, ".rdata", sep=""))
  setwd(startdir)
  return("TFBC done!")
  

}  # end function








