####################################################################################
## Convert LRI Prevalence from surveys from period to point using new uncertainty ##
####################################################################################

## Load libraries and data ##
library(matrixStats)
library(boot)
library(openxlsx)
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# new extractions 
period <- read.csv("FILEPATH")


duration <- read.csv("FILEPATH")
locs     <- get_location_metadata(location_set_id = 35, release_id = 16)


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
period$is_outlier <- 0

### Subnational Locations cause problems ###
# Function for capitalizing first letter
simpleCap <- function(x) {
  s <- strsplit(x, " ")[[1]]
  paste(toupper(substring(s, 1,1)), substring(s, 2),
        sep="", collapse=" ")
}

period <- subset(period, !is.na(standard_error))

######################################################
## SAVE ##
######################################################


## Decomp 4
write.csv(period, "FILEPATH", row.names=F)
