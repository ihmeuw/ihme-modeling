#####################################################
## The purpose of this code is to 
## Convert Diarrhea Prevalence from surveys from period to point 
# using draws and prep the data for upload into Epi database
# and is written for Decomp Step 1 in GBD 2019.
######################################################
## Load data and packages 
	library(matrixStats)
	library(plyr)
	library(msm)
	
	period <- read.csv("filepath")
	locs <- read.csv("filepath")
	period <- join(period, locs, by="ihme_loc_id")
	duration <- read.csv("filepath")
## use global value only for duration ##
	duration <- subset(duration, super_region_name=="Global")

##########################################################
## Convert 
## conversion from period to point should be:
# period * duration / (duration + recall - 1)
	prevs <- period[,c("scalar_diarrhea","se","recall_period","super_region_name")]
# find linear floor for transformation
	prevs$scalar_diarrhea[prevs$scalar_diarrhea==0] <- median(prevs$scalar_diarrhea[prevs$scalar_diarrhea > 0]) * 0.01

	prevs$ln_diarrhea <- log(prevs$scalar_diarrhea)
	prevs <- data.frame(prevs, duration)

## Use delta method to get ln_std_error
  prevs$ln_std <- sapply(1:nrow(prevs), function(i) {
	ratio_i <- prevs[i, "scalar_diarrhea"]
	ratio_se_i <- prevs[i, "se"]
	deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })

## Do it by draw to maintain uncertainty ##
	prev.point <- c()
	for(i in 1:1000){
	  prevs[,paste0("draw_",i)] <- exp(rnorm(n=length(prevs$ln_diarrhea), mean=prevs$ln_diarrhea, sd=prevs$ln_std)) 
	  prevs[,paste0("period_",i)] <- prevs[,paste0("draw_",i)] * prevs[,paste0("duration_",i)] / (prevs[,paste0("duration_",i)] + prevs$recall_period*7 - 1)
	  prev.point <- cbind(prev.point, prevs[,paste0("period_",i)])
	}

	mean.point <- rowMeans(prev.point)
	std.point <- rowSds(prev.point)
	# this also works
	std.point <- sapply(prev.point, 2, function(x) sd(x))

	period$mean <- mean.point
	period$standard_error <- std.point

## rename for clarity ##
	colnames(period)[colnames(period)=="se"] <- "scalar_se"

	period$cases <- period$sample_size * period$mean
	period$note_modeler <- paste("Converted to point prevalence using duration draws. Period prevalence was", round(period$scalar_diarrhea,4),".",period$note_modeler)
	period$duration <- rowMeans(duration[,7:1006])
	period$duration_lower <- as.numeric(quantile(duration[,7:1006], 0.025))
	period$duration_upper <- as.numeric(quantile(duration[,7:1006], 0.975))

## Prepare missing values ##
	period$mean[is.na(period$mean)] <- 0
	period$standard_error[is.na(period$standard_error)] <- ""

## Reassign case count using new mean ##
	period$cases <- period$sample_size * period$mean

### Prepare some fields necessary for Epi Db ###
	period$cv_diag_selfreport <- 1
	period$urbanicity_type <- "Mixed/both"
	period$recall_type <- "Point"
	period$unit_type <- "Person"
	period$unit_value_as_published <- 1
	period$measure_issue <- 0
	period$measure_adjustment <- 1
	period$urban <- substr(period$location, nchar(as.character(period$location))-4, nchar(as.character(period$location)))
	period$representative_name <- ifelse(period$location=="none","Nationally and subnationally representative",
										 ifelse(period$urban=="Urban","Representative of urban areas only",
												ifelse(period$urban=="Rural","Representative of rural areas only",
													   "Representative for subnational location only")))
	period$age_issue <- 0
	period$age_demographer <- 0
	period$measure <- "prevalence"
	period$year_issue <- 0
	period$sex_issue <- 0
	period$smaller_site_unit <- 0
	period$source_type <- "Survey - cross-sectional"
	period$extractor <- "username"
	period$is_outlier <- 0

### Subnational Locations cause problems ###
# Function for capitalizing first letter
	simpleCap <- function(x) {
	  s <- strsplit(x, " ")[[1]]
	  paste(toupper(substring(s, 1,1)), substring(s, 2),
			sep="", collapse=" ")
	}

# fix some random naming issues
	locations <- as.character(period$location)
	bracket <- grepl("]",locations)
	split <- unlist(strsplit(locations[bracket==T], "] "))
	split <- split[seq(2,580,2)]
	problem <- period[bracket==T,]
	good <- period[bracket==F,]
	problem$location_update <- split
	good$location_update <- good$location
	period <- rbind.data.frame(good, problem)
	period$location_update <- with(period, ifelse(location_update=="arunachalpradesh, Rural","Arunachal Pradesh, Rural",
												  ifelse(location_update=="ArunachalPradesh, Rural","Arunachal Pradesh, Rural",
														 ifelse(location_update=="arunachalpradesh, Urban","Arunachal Pradesh, Urban",
																ifelse(location_update=="ArunachalPradesh, Urban","Arunachal Pradesh, Urban",as.character(location_update))))))
	period$location_ascii_name <- sapply(period$location_update, simpleCap)

## Fix NIDs India 1998 ##
	period$nid[period$ihme_loc_id=="IND" & period$year_start==1998] <- 19950

## Fix Continuous DHS in Peru ##
	period$nid[period$survey=="MACRO_DHS" & period$ihme_loc_id=="PER" & period$year_start>=2003] <- 20663

######################################################
## Done! ##
######################################################
write.csv(period, "filepath", row.names=F)
