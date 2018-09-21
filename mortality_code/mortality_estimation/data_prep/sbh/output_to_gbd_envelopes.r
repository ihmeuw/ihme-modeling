#############################################################################################
## OUTPUT_TO_GBD_ENVELOPES
## This function formats and outputs the data for child GPR 
#############################################################################################

output_to_gbd_envelopes <- function(dir, survey.file.name, survey.database.name) {

  library(foreign)
  setwd(dir)
  
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
      data[[s]]$reftime = abs(data[[s]]$reftime - floor(data[[s]]$reftime) - 0.5) < 0.1     # Selects only the midpoint estimate for every year from combined estimates
      data[[s]] = subset(data[[s]], reftime == 1)
      data[[s]] <- data[[s]][,names(data[[s]])!="reftime"]
    }
  }
  data <- do.call("rbind", data)
  
  #keep all variables for gbd (ihme_loc_id identifies the subnational locations)
  if(is.null(data$ihme_loc_id)){
	data$ihme_loc_id <- data$iso3
    data <- data[,c("iso3", "ihme_loc_id", "t", "q5", "source", "source.date", "in.direct", "compiling.entity", "data.age")]
  }else{
    data <- data[,c("iso3", "ihme_loc_id", "t", "q5", "source", "source.date", "in.direct", "compiling.entity", "data.age")]
  }
  
  gbd.env.dir <- "strResultsDirectory"
  gbd.env.file <- paste("EST_", survey.file.name, sep="")
  write.dta(data, paste(gbd.env.dir, gbd.env.file,".dta", sep=""))
  
  return("output done!")

}  # end function
