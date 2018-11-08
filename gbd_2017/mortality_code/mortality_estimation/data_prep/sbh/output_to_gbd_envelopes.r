#############################################################################################
## OUTPUT_TO_GBD_ENVELOPES
##
## This function formats the data for child GPR and outputs it to GBD Envelopes
#############################################################################################

output_to_gbd_envelopes <- function(dir, survey.file.name, survey.database.name, nid) {
  library(foreign)
  setwd(dir)
  library(data.table)
  
  data <- read.dta("all_methods_results.dta", convert.factors=F)
  
  data$q5 <- data$q5_pred
  if (sum(names(data)=="source")==0) data$source <- survey.database.name
  data$source.date <- floor(data$svdate)
  data$in.direct <- "indirect"
  data$compiling.entity <- "new"
  data$data.age <- "new"
  
  data$survey <- paste(data$iso3, data$svdate, sep="_")
  data <- lapply(unique(data$survey), function(x) subset(data, survey==x))
  
  for (s in 1:length(data)) {
    if (length(unique(data[[s]][data[[s]]$method!="LOESS of all methods", "method"])) == 1) {
      data[[s]] <- data[[s]][data[[s]]$method != "LOESS of all methods",]
      data[[s]]$in.direct <- paste("indirect, ", unique(data[[s]]$method), " only", sep="")
    } else {
      data[[s]] <- data[[s]][data[[s]]$method == "LOESS of all methods",]
      data[[s]]$reftime = data[[s]]$svdate - data[[s]]$t
      data[[s]]$reftime = abs(data[[s]]$reftime - floor(data[[s]]$reftime) - 0.5) < 0.1     # Selects only the midpoint estimate for every year from Combined estimates
      data[[s]] = subset(data[[s]], reftime == 1)
      data[[s]] <- data[[s]][,names(data[[s]])!="reftime"]
    }
  }
  data <- do.call("rbind", data)
  
  # fill in source column if empty
  if(!("source" %in% colnames(data))){
    data$source = survey.file.name
  }
  
  if(is.null(data$ihme_loc_id)){
    data$ihme_loc_id <- data$iso3
    data <- data[,c("iso3", "ihme_loc_id", "t", "q5", "source", "source.date", "in.direct", "compiling.entity", "data.age")]
  }else{
    data <- data[,c("iso3", "ihme_loc_id", "t", "q5", "source", "source.date", "in.direct", "compiling.entity", "data.age")]
  }
  data$nid <- nid
  
  gbd.env.dir <- "FILEPATH"
  dir.exists <- function(d) {
    de <- file.info(d)$isdir
    ifelse(is.na(de), FALSE, de)
  }
  ifelse(!dir.exists(gbd.env.dir), dir.create(gbd.env.dir), FALSE)
  gbd.env.file <- paste("/EST_", survey.file.name, "_v5Q0_IHME", sep="")
  write.dta(data, paste(gbd.env.dir, gbd.env.file,".dta", sep=""))
  return("output to GBD Envelopes done!")
  
}  # end function