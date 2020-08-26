####################################################################################
## Convert LRI Prevalence from surveys from period to point using new uncertainty ##
## The input for this file is the tabulated results from the "lri_collapse_survey_data.R"
## file. This uses draws and a mean duration to convert from period prevalence
## reported in the surveys to point prevalence used in modeling.
####################################################################################

## Load libraries and data ##
library(matrixStats)
library(boot)
library(openxlsx)

period <- read.xlsx("filepath")
duration <- read.csv("filepath")
locs <- read.csv("filepath")

## Make it possible to calculate the period prevalence for non-seasonal adjusted data ##
# period$base_se <- with(period, sqrt(base_lri * (1-base_lri)/sample_size))
# period$se <- period$base_se
# period$scalar_lri <- period$base_lri

## Some values are missing, replace so that they can be used! ##
# period$missing <- ifelse(period$scalar_lri==0,1,0)
# period$cv_had_fever <- ifelse(period$scalar_lri == 0, ifelse(period$good_no_fever + period$poor_no_fever!=0,0, period$cv_had_fever), period$cv_had_fever)
# period$scalar_lri <- ifelse(period$scalar_lri == 0, ifelse(period$good_no_fever!=0,period$good_no_fever,period$poor_no_fever), period$scalar_lri)

#period$se <- ifelse(period$missing == 1, sqrt(period$scalar_lri*(1-period$scalar_lri)/period$sample_size), period$se)

## Drop still missing values ##
#period <- subset(period, scalar_lri != 0)

## conversion from period to point should be:
# period * duration / (duration + recall - 1)

prevs <- period[,c("scalar_lri","se","recall_period")]
prevs$upper <- prevs$scalar_lri + prevs$se * 1.96
prevs$lower <- prevs$scalar_lri - prevs$se * 1.96
prevs$lower[prevs$lower<=0] <- min(prevs$lower[prevs$lower>=0], na.rm=T)

prevs$scalar_lri[prevs$scalar_lri==0] <- median(prevs$scalar_lri) * 0.01 # This is an approach used in DisMod for the linear floor

prevs$ln_lri <- logit(prevs$scalar_lri)
prevs$ln_upper <- logit(prevs$upper)
prevs$ln_lower <- logit(prevs$lower)
prevs$ln_std <- (prevs$ln_upper - prevs$ln_lri)/1.96

prev.point <- c()
for(i in 1:1000){
  prevs[,paste0("duration_",i)] <- duration[,paste0("duration_",i)]
  prevs[,paste0("draw_",i)] <- inv.logit(rnorm(n=length(prevs$ln_lri), mean=prevs$ln_lri, sd=prevs$ln_std))
  prevs[,paste0("period_",i)] <- prevs[,paste0("draw_",i)] * prevs[,paste0("duration_",i)] / (prevs[,paste0("duration_",i)] + prevs$recall_period*7 - 1)
  prev.point <- cbind(prev.point, prevs[,paste0("period_",i)])
}

mean.point <- rowMeans(prev.point)
std.point <- apply(prev.point, 1, sd)

period$mean <- mean.point
period$standard_error <- std.point
## rename for clarity ##
colnames(period)[colnames(period)=="se"] <- "scalar_se"

period$cases <- period$sample_size * period$mean
period$note_modeler <- paste("Converted to point prevalence using duration draws. Period prevalence was", round(period$scalar_lri,4),".",period$note_modeler)

period$duration <- rowMeans(duration[,7:1006])
period$duration_lower <- as.numeric(quantile(duration[,7:1006], 0.025))
period$duration_upper <- as.numeric(quantile(duration[,7:1006], 0.975))

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
                                                   "Subnationally representative only")))
period$age_issue <- 0
period$age_demographer <- 0
period$measure <- "prevalence"
period$year_issue <- 0
period$sex_issue <- 0
period$smaller_site_unit <- 0
period$source_type <- "Survey - cross-sectional"
period$extractor <- "ctroeger"
period$is_outlier <- 0

# Function for capitalizing first letter
simpleCap <- function(x) {
  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
        sep="", collapse=" ")
}

# fix some random naming issues
locations <- as.character(period$location)
bracket <- grepl("] ",locations)
split <- unlist(strsplit(locations[bracket==T], "] "))
split <- split[seq(2,length(split),2)]
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
period$nid[period$ihme_loc_id=="IND" & period$year_start==1998] <- 19950
period$nid[period$survey=="MACRO_DHS" & period$ihme_loc_id=="PER" & period$year_start>=2003] <- 20663

period <- subset(period, !is.na(standard_error))

write.csv(period, "filepath")