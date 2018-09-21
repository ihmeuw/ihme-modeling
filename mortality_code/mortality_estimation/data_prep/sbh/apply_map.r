#############################################################################################
## APPLY_MAP
## This function formats ths data and then applies the Maternal Age Period method  to it and saves the output to the specified file.
#############################################################################################

apply_map <- function(dir, grouping.categories, all.women, subset=NULL, filetag=NULL, uncertainty=FALSE, n.sims=1, fund.uncert=FALSE, codedir = "strCodeDirectory") {

# include libraries 
  library(arm)
  library(foreign)
 
# Load MAP model parameters
  startdir <- getwd()
  setwd("strSurveyDirectory")
  source("strFunctionDirectory/02a. functions - RE model with uncertainty.r")
  load(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/distributions - age.rdata", sep=""))
  setwd("strSurveyDataDirectory")
    
# bring in the coefficients for output.map

  output.map.varnames <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.map/output.map.varnames.csv", sep=""),stringsAsFactors=FALSE)
  output.map.fe <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.map/output.map.fe.csv", sep=""), stringsAsFactors=FALSE)
  output.map.iso_re<- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.map/output.map.iso_re.csv", sep=""), stringsAsFactors=FALSE)
  output.map.reg_re <- read.csv(paste("strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.map/output.map.reg_re.csv", sep=""), stringsAsFactors=FALSE)
  output.map.errors <- read.csv(paste("/strFunctionDirectory", if (all.women) "All women" else "Ever-married women", "/fitted_models/output.map/output.map.errors.csv", sep=""), stringsAsFactors=FALSE)


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
  
# Load MAP data
  data = vector("list", 2)
  names(data) = c("ceb", "cd")
  data$ceb = read.dta("MAP_ageceb.dta", convert.underscore = TRUE, convert.factors=FALSE)
  for (ii in 1:ncol(data$ceb)) attributes(data$ceb[,ii]) <- NULL
  data$cd = read.dta("MAP_agecd.dta", convert.underscore = TRUE, convert.factors=FALSE)
  for (ii in 1:ncol(data$cd)) attributes(data$cd[,ii]) <- NULL
  
  for(x in 1:length(data)) colnames(data[[x]])[grep("sample.weight", colnames(data[[x]]))] = "n.mothers"
  for(x in 1:length(data)) colnames(data[[x]])[grep("group", colnames(data[[x]]))] = "agegroup"  
  for(x in 1:length(data)) data[[x]] <- merge(data[[x]], codes, by="iso3", all.x=T)
  for(x in 1:length(data)) data[[x]] <- data[[x]][!is.na(data[[x]]$gbdregion),]  ## we can't predict for countries outside of the regions which
                                                                                 ## the MAP model is fit for since there are no CEB and CD
                                                                                 ## distributions
  if (!is.null(subset)) for(x in 1:length(data)) data[[x]] <- merge(data[[x]], subset, by=grouping.categories[grouping.categories!="gbdregion"], all=F)
  lapply(data, head)

# Merge in CEB/CD period distributions
  for(type in names(data)) data[[type]] = merge(data[[type]], distributions[[type]], all.x = TRUE)
  lapply(data, head)

  for(type in names(data)) data[[type]][,grep("t\\.", colnames(data[[type]]), value = TRUE)] = data[[type]][,grep("t\\.", colnames(data[[type]]), value = TRUE)] * data[[type]]$n.mothers * data[[type]][,type]
  lapply(data, head)
  
# Calculate total CEB/CD for every year prior to the survey
  total.by.time = lapply(data, function(type) aggregate(type[,paste("t.", 0:24, sep = "")], by = as.list(type[,grouping.categories]), sum, na.rm = TRUE))
  for(x in 1:length(total.by.time)) total.by.time[[x]] = merge(unique(data[[x]][,grouping.categories]), total.by.time[[x]], all.y = TRUE)
  lapply(total.by.time, head)
  
  t = as.numeric(substring(grep("t\\.", colnames(total.by.time[[1]]), value = TRUE), first = 3))

  bytime.list = vector("list", length(t))
  names(bytime.list) = paste("t.", t, sep = "")  
  
  for(tp in names(bytime.list)) {
    bytime.list[[tp]] = total.by.time[["ceb"]][, c(grouping.categories, tp)]
    names(bytime.list[[tp]]) = c(grouping.categories, "ceb")
    
    bytime.list[[tp]] = merge(bytime.list[[tp]], total.by.time[["cd"]][, c(grouping.categories, tp)], all = TRUE)
    names(bytime.list[[tp]]) = c(grouping.categories, "ceb", "cd")
    
    bytime.list[[tp]]$cdceb = (bytime.list[[tp]]$cd/bytime.list[[tp]]$ceb)*1000
    
    bytime.list[[tp]] = subset(bytime.list[[tp]], !is.na(cdceb))
  }
  lapply(bytime.list, head)[1:2]
  
  bytime.list = lapply(bytime.list, function(x) subset(x, cdceb < 1000))
  lapply(bytime.list, summary)

# Apply MAP coefficients
  data.uncertainty = lapply(paste("t.", 0:24, sep = ""), function(x) prediction.sim(data.input = bytime.list[[x]], 
                                                                                    varnames = output.map.varnames[output.map.varnames$group==x,],
                                                                                    fe = output.map.fe[output.map.fe$group==x,],
                                                                                    reg_re = output.map.reg_re[output.map.reg_re$group==x,], 
                                                                                    iso_re = output.map.iso_re[output.map.iso_re$group==x,],
                                                                                    errors = output.map.errors[output.map.errors$group==x,], 
                                                                                    group = x,
                                                                                    grouping.categories = grouping.categories, 
                                                                                    uncertainty = uncertainty, 
                                                                                    n.sims = n.sims, 
                                                                                    method = "period"))


  names(data.uncertainty) = paste("t.", 0:24, sep = "")
  
  for(tp in 1:25) {
    for(x in 1:length(data.uncertainty[[tp]])) {
      if(is.null(data.uncertainty[[tp]][[x]])) {
        data.uncertainty[[tp]][[x]] = NULL
      } else {
        data.uncertainty[[tp]][[x]] = data.frame(data.uncertainty[[tp]][[x]][,grouping.categories], reftime = tp - 0.5, data.uncertainty[[tp]][[x]][,grep("q5", colnames(data.uncertainty[[tp]][[x]]), value = TRUE)])
        if(ncol(data.uncertainty[[tp]][[x]]) == length(grouping.categories) + 2) {
          if (x == 1) colnames(data.uncertainty[[tp]][[x]])[ncol(data.uncertainty[[tp]][[x]])] = "q5.pred"
          if (x == 2) colnames(data.uncertainty[[tp]][[x]])[ncol(data.uncertainty[[tp]][[x]])] = "q5.pred.1"         
        } 
      }
    }
  }

# Save results
  save(data.uncertainty, file=paste("MAP_results", filetag, ".rdata", sep=""))
  setwd(startdir)
  return("MAP done!")
  

}  # end function