## Process Diarrhea UbCov Data, account for seasonality ##
############### Steps ##################
# 1). Get summary prevalence by month
# 2). Develop simple region seasonal model
# 3). Determine scalar for month and geography
# 4). Apply scalar to raw data
# 5). Tabulate month-adjusted prevalence, save collapsed data

  library(survey)
  library(haven)
  library(ggplot2)
  library(boot)
  library(plyr)
  library(data.table)
  library(lme4)
invisible(sapply(list.files("/FILEPATH", full.names = T), source))

keep.names <- c("ihme_loc_id","year_start","year_end","psu","nid","survey_name","strata","pweight","sex_id","age_year","age_month","int_month", 
                "had_diarrhea","had_diarrhea_recall_period_weeks","admin_1","admin_2")

###############################################################
## Append all files that were extracted ##
filenames <- list.files(path="/FILEPATH/00_winnower_output/", full.names=T)
d.input <- data.frame()
n <- 1
for(f in filenames){
  print(paste0("On survey ",n, " of ", length(filenames)))
  df <- data.frame(read_dta(f))
  df <- df[,which(names(df) %in% keep.names)]
#   df <- subset(df, age_year <= 4)
  df$had_diarrhea[is.na(df$had_diarrhea)] <- 9
  df$tabulate <- ave(df$had_diarrhea, df$nid, FUN= function(x) min(x))
  df <- subset(df, tabulate!=9)
  d.input <- rbind.fill(d.input, df)
  n <- n+1
}
################################################################

write.csv(d.input, "/FILEPATH/compiled_data.csv", row.names=F)
d.input <- fread("/FILEPATH/compiled_data.csv")

locs <- get_location_metadata(location_set_id = 35, release_id = 16)
diarrhea <- data.frame(d.input)[,keep.names]



	diarrhea <- join(diarrhea, locs[,.(location_id, ihme_loc_id, location_name)], by="ihme_loc_id")
	rm(d.input)

	diarrhea$had_diarrhea[is.na(diarrhea$had_diarrhea)] <- 9
	diarrhea$tabulate <- ave(diarrhea$had_diarrhea, diarrhea$nid, FUN= function(x) min(x))
	diarrhea <- subset(diarrhea, tabulate!=9)

## Fill missing values ##
full <- diarrhea

diarrhea$psu <- as.numeric(diarrhea$psu)
diarrhea <- subset(diarrhea, !is.na(psu))
diarrhea$pweight[is.na(diarrhea$pweight)] <- 1

################################################
## Part 2 ##
## Get monthly prevalence by survey ##
################################################
	diarrhea$survey_month <- diarrhea$int_month

	diarrhea <- subset(diarrhea, !is.na(int_month))

	nid <- unique(diarrhea$nid)

	df.month <- data.frame()
	num <- 1
	for(n in nid){
	  print(paste0("On survey number ",num," of ",length(nid)))
	  temp <- subset(diarrhea, nid==n & had_diarrhea!=9 & age_year <= 4)
	  #table(temp$had_diarrhea, temp$survey_month)
	  if(length(unique(temp$psu))>1){
		## Set svydesign
		dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
		month.prev <- svyby(~had_diarrhea, ~survey_month, dclus, svymean, na.rm=T)
		month.prev$ihme_loc_id <- unique(substr(temp$ihme_loc_id,1,3))
		month.prev$year_id <- max(unique(temp$year_end))
		month.prev$sample_size <- svyby(~had_diarrhea, ~survey_month, dclus, unwtd.count, na.rm=T)$count
		month.prev$region_name <- unique(temp$region_name)
		month.prev$super_region_name <- unique(temp$super_region_name)
		month.prev$nid <- unique(temp$nid)
		df.month <- rbind.data.frame(df.month, month.prev)
	  }
	  num <- num + 1
	}

	write.csv(df.month, "/FILEPATH/month_tabulations_2019.csv", row.names=F)

##################################################
## Part 3 ##
## Develop seasonality model ##
##################################################
	tab <- df.month
	tab$mean <- tab$had_diarrhea
	tab$month <- tab$survey_month
	tab$country <- tab$ihme_loc_id
	tab$year_id <- as.numeric(tab$year_id)
	tab <- subset(tab, !is.na(mean))
	tab <- subset(tab, mean!=0)
	tab <- subset(tab, mean!=1)

	tab$ln_mean <- logit(tab$mean)

pdf(paste0("/FILEPATH/seasonal_fit_plots_2019.pdf"))
	pred.df <- data.frame()
	curves <- data.frame()
	for(r in  unique(tab$region_name[!is.na(tab$region_name)])){
	  sub <- subset(tab, region_name==r)
	  pred.ov <- data.frame(month = 1:12, year_id=2015)
	  if(length(unique(sub$ihme_loc_id))>1){
		mod <- lmer(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + year_id + (1|ihme_loc_id), weights=sample_size, data=sub)
		pred.ov$pred <- inv.logit(predict(mod, data.frame(pred.ov), re.form=NA))
	  } else {
		mod <- lm(ln_mean ~ sin((month)*pi/6) + cos((month)*pi/6) + year_id, weights=sample_size, data=sub)
		pred.ov$pred <- inv.logit(predict(mod, data.frame(pred.ov)))
	  }
	  pred.ov$mean.pred <- mean(pred.ov$pred)
	  pred.ov$scalar <- 1-(pred.ov$pred - pred.ov$mean.pred)/pred.ov$pred
	  pred.ov$amplitude <- max(pred.ov$pred) - min(pred.ov$pred)
	  pred.ov$peak <- pred.ov$month[pred.ov$pred==max(pred.ov$pred)]
	  pred.ov$nadir <- pred.ov$month[pred.ov$pred==min(pred.ov$pred)]
	  pred <- join(sub, pred.ov, by="month")
	  pred <- subset(pred, month < 13)
	  pred$final_mean <- pred$mean * pred$scalar
	  f <- ggplot(data=pred, aes(x=month, y=mean, col=country)) + geom_point(aes(size=sample_size)) + scale_size(range=c(1,4)) + guides(size=F) +
		geom_line(data=pred.ov, aes(x=month, y= pred), col="black", lwd=1.25) + geom_hline(yintercept=pred$mean.pred, lty=2) + theme_bw() +
		ylab("Model Input Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Country") +
		ggtitle(paste0(r, "\nDiarrhea Seasonality"))
	  print(f)
	  rm <- melt(pred[,c("month","country","mean","final_mean")], id.vars=c("month", "country"))

	  rm$shift <- ifelse(rm$variable=="mean",rm$month-0.1,rm$month+0.1)
	  g <- ggplot() + geom_point(data=rm, aes(x=shift, y=value, col=country, shape=variable), size=3) + scale_x_continuous("Month", breaks=1:12, labels=1:12) +
		geom_line(data=pred.ov, aes(x=month, y=pred), col="black",lwd=1.25) + geom_hline(yintercept=pred$mean.pred, lty=2) + scale_color_discrete("Country") +
		theme_bw() + ylab("Prevalence") + scale_shape_manual("", labels=c("Unadjusted","Adjusted for\nseasonality"), values=c(16,18)) + ggtitle(paste0(r, "\nDiarrhea prevalence adjusted for seasonality"))
	  print(g)

	  pred.df <- rbind.data.frame(pred.df, pred)
	  pred.ov$region_name <- r
	  pred.ov$super_region_name <- unique(sub$super_region_name)
	  curves <- rbind.data.frame(curves, pred.ov)
	}
	for(s in unique(curves$super_region_name)){
	  o <- ggplot(data=subset(curves, super_region_name==s), aes(x=month, y=pred, col=region_name)) + geom_line(lwd=1.25) + theme_bw() +
		ylab("Modeled Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Region Name") +
		ggtitle(s)
	  print(o)
	  o <- ggplot(data=subset(curves, super_region_name==s), aes(x=month, y=1/scalar, col=region_name)) +
		geom_hline(yintercept=1, col="black", lty=2) + geom_line(lwd=1.25) + theme_bw() +
		ylab("Modeled Prevalence") + scale_x_continuous("Month", breaks=1:12, labels=1:12) + scale_color_discrete("Region Name") +
		ggtitle(s)
	  print(o)
}
dev.off()
# end loop

write.csv(pred.df, paste0("/FILEPATH/seasonal_diarrhea_prediction_2019.csv"))
write.csv(curves, "/FILEPATH/monthly_scalars_2019.csv")

########################################################
#########################################################

########################################################
## Can start here if the seasonality already calculated ##
	curves <- read.csv("/FILEPATH/monthly_scalars_2019.csv")
  
 ########################################################
## Part 4. We have a month/country scalar, now apply to raw data
########################################################
	diarrhea <- full
	diarrhea$ihme_loc_id <- diarrhea$iso3
	pred.df <- read.csv("/FILEPATH/seasonal_diarrhea_prediction_2019.csv")
	#scalar <- pred.df[,c("scalar","survey_month","nid")]
	scalar <- curves[,c("scalar","month","region_name")]
	scalar$survey_month <- scalar$month
	diarrhea$survey_month <- diarrhea$int_month

	diarrhea <- join(diarrhea, locs[,.(location_id, region_name)])
	diarrhea <- join(diarrhea, scalar, by=c("region_name","survey_month"))


## Create a subnational map, helps to correctly location GBD subnationals later ##
locs$iso3 <- substr(locs$ihme_loc_id,1,3)
  lookup <- diarrhea[,c("location_name","ihme_loc_id","admin_1","iso3","admin_2")]
  lookup <- join(lookup, locs, by=c("ihme_loc_id"))
  lookup <- subset(lookup, exists_subnational==1)
  lookup <- unique(lookup)
  lookup$location <- lookup$admin_1

  subnat_map <- read.csv("/FILEPATH/gbd_subnat_map.csv")
  lookup <- join(lookup, subnat_map, by="location")
  lookup$name_lower <- tolower(lookup$admin_1)
  locs$name_lower <- tolower(locs$location_name)
  lookup <- join(lookup, locs, by="name_lower")

  write.csv(lookup, "/FILEPATH/survey_loc_lookup_diarrhea.csv", row.names=F)
	subnat_map <- read.csv("/FILEPATH/gbd_subnat_map.csv")
		setnames(subnat_map, c("parent_id", "location"), c("iso3","admin_1"))
#  subnat_map$subnat <- 1
  
	diarrhea <- join(diarrhea, locs[,.(location_id, ihme_loc_id)], by="location_id")
	diarrhea$iso3 <- substr(diarrhea$ihme_loc_id,1,3)
	diarrhea <- join(diarrhea, subnat_map, by=c("admin_1","iso3"))
	diarrhea$subnat[is.na(diarrhea$subnat)] <- 0

## Identify known subnationals ##
	diarrhea$subname <- ifelse(diarrhea$subnat==1, as.character(diarrhea$ihme_loc_id_mapped), diarrhea$iso3)

	diarrhea$nid.new <- ifelse(diarrhea$subnat==1, paste0(diarrhea$nid,"_",diarrhea$subname), diarrhea$nid)

## Pull only new subnationals? ##
	# nid.new <- unique(diarrhea$nid.new[diarrhea$subnat==1 & diarrhea$iso3 %in% c("NGA","PHL","PAK","POL","ITA")])

## Round ages
	diarrhea$age_year <- floor(diarrhea$age_year)
## Set missing sex to both
	diarrhea$sex_id[is.na(diarrhea$sex_id)] <- 3
## prep diarrhea column (missing is set to 9) ##
	diarrhea$had_diarrhea[diarrhea$had_diarrhea==9] <- NA
	diarrhea$pweight[is.na(diarrhea$pweight)] <- 1

## Loop ##
############################################################################################
num <- 1
df.final <- data.frame()
for(n in unique(diarrhea$nid.new)){
  print(paste0("On survey number ",num," of ", length(unique(diarrhea$nid.new))))
  temp <- subset(diarrhea, nid.new==n)
# Drop if had_diarrhea is missing #
  temp <- subset(temp, !is.na(had_diarrhea))

# It is possible some surveys will be empty so skip those #
  if(nrow(temp)>1){

# If all pweights are 1 (i.e. missing), set psu to 1:nrow #
  row.count <- 1:nrow(temp)
  if(min(temp$pweight)==1){
      temp$psu <- row.count
  }

# If all psu are missing, set psu to 1:nrow and pweight to 1 #
  if(is.na(min(temp$psu))){
    temp$psu <- row.count
    temp$pweight <- 1
  }

# If all psu are the same, set psu to 1:nrow and pweight to 1 #
  if(length(unique(temp$psu))==1){
    temp$psu <- row.count
    temp$pweight <- 1
  }

    temp$recall_period <- ifelse(is.na(temp$had_diarrhea_recall_period_weeks),2,temp$had_diarrhea_recall_period_weeks)
    recall_period <- max(temp$recall_period, na.rm=T)

    loop.dummy <- max(temp$scalar)

# Does survey month exist? #
    if(is.na(loop.dummy)){
      temp$scalar_diarrhea <- temp$had_diarrhea
    } else {
      temp$scalar_diarrhea <- temp$had_diarrhea * temp$scalar
    }

# Set svydesign #
    dclus <- svydesign(id=~psu, weights=~pweight, data=temp)

# Get survey prevalence #
    prev <- svyby(~scalar_diarrhea, ~sex_id + age_year, dclus, svymean, na.rm=T)
    prev$sample_size <- svyby(~scalar_diarrhea, ~sex_id + age_year, dclus, unwtd.count, na.rm=T)$count
    prev$base_mean <- svyby(~had_diarrhea, ~sex_id + age_year, dclus, svymean, na.rm=T)$had_diarrhea
    prev$ihme_loc_id <- unique(temp$ihme_loc_id)
    prev$location <- unique(temp$subname)
    prev$year_start <- min(temp$year_start)
    prev$year_end <- max(temp$year_end)
    prev$nid <- unique(temp$nid)
    prev$sex <- ifelse(prev$sex_id==2,"Female","Male")
    prev$age_start <- prev$age_year
    prev$age_end <- prev$age_year + 1
    prev$recall_period <- recall_period
    prev$survey <- unique(temp$survey_name)
    survey_admin_name <- paste(unique(temp$admin_1), collapse=", ")
    prev$survey_admin_name <- survey_admin_name
    prev$note_modeler <- paste0("Diarrhea prevalence adjusted for seasonality. The original value was ", round(prev$base_mean,4),".")
    df.final <- rbind.data.frame(df.final, prev)
    num <- num + 1
  }
}

## Export for review ##
#df.final <- subset(df.final, !is.na(location))
	write.csv(df.final, "/home/j//WORK/12_bundle/diarrhea/3/01_input_data/01_nonlit/diarrhea_collapsed_survey_2022.csv", row.names = F)

