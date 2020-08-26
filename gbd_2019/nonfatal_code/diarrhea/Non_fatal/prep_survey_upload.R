###############################################
## Prep Diarrhea survey data for upload ##
###############################################
	library(plyr)
	library(ggplot2)
	library(zoo)
	source("/filepath/get_bundle_data.R")
	locs <- read.csv("filepath")
	subnat_map <- read.csv("filepath")
	df <- read.csv("filepath")

## Drop if age is greater than 100 ##
	df <- subset(df, age_end<99)

## Fix some things in survey data ##
	df <- join(df, locs[,c("location_id","location_ascii_name","level","location_name", "ihme_loc_id")], by="ihme_loc_id")

	df$location_name <- df$location_ascii_name

	df$sampling_type <- "Cluster"
	df$gbd_round <- 2019

## Drop really small populations ##
	df <- subset(df, sample_size>=10)
## Drop if value is zero ##
	df <- subset(df, mean!=0)

## Want adults in five year bins ##
	keep.names <- c("ihme_loc_id","location","year_start","year_end","nid","sex","age_start","age_end","location_id","location_ascii_name","location_name")
	df.old <- df[df$age_end>5,c("cases","sample_size",keep.names)]
	df <- df[df$age_end<=5,]
	df.old$age_cut <- cut(df.old$age_end, seq(0,100,5), labels=F)
	age_match <- data.frame(age_cut=1:20, age_start=seq(0,95,5), age_end=seq(5,100,5))
	col.df <- aggregate(cbind(cases, sample_size) ~ 
	                      ihme_loc_id + location + year_start + year_end + nid + sex + age_cut + location_id + location_ascii_name + location_name, data=df.old, FUN=sum)
	col.df$mean <- col.df$cases/col.df$sample_size
	col.df$note_modeler <- "Values were collapsed to five year age bins"
	col.df$measure <- "prevalence"
	col.df <- join(col.df, age_match, by="age_cut")
	col.df$age_end[col.df$age_end==100] <- 99
	col.df$source_type <- "Survey - cross-sectional"

	keep.cols <- c("bundle_id","seq","nid","underlying_nid","input_type","modelable_entity_id","modelable_entity_name","field_citation_value","file_path","response_rate",
				   "page_num","table_num","source_type","location_name","ihme_loc_id","smaller_site_unit","site_memo","sex","sex_issue","year_start","year_end",
				   "year_issue","age_start","age_end","age_issue","age_demographer","measure","mean","lower","upper","standard_error","effective_sample_size","cases","sample_size","unit_type",
				   "unit_value_as_published","measure_issue","measure_adjustment","uncertainty_type","uncertainty_type_value","representative_name","urbanicity_type","recall_type",
				   "recall_type_value","sampling_type","case_name","case_definition","case_diagnostics","group","specificity","group_review","note_modeler","extractor","is_outlier",
				   "note_sr","gbd_round","design_effect")
	template <- read.csv("filepath")
	template <- template[,keep.cols]

	out.df <- rbind.fill(template, df, col.df)
	out.df <- out.df[2:length(out.df$nid),]
	out.df[,c("sex_issue","year_issue","age_issue","source_type","age_demographer","measure_adjustment","extractor","is_outlier")] <- na.locf(out.df[,
			c("sex_issue","year_issue","age_issue","source_type","age_demographer","measure_adjustment","extractor","is_outlier")])
	head(out.df)
	tail(out.df)

out.df$representative_name <- ifelse(out.df$location==out.df$ihme_loc_id,"Nationally and subnationally representative","Representative for subnational location only")

## Fix other things ##
	out.df$nid[out.df$nid==5379] <- 5401
	out.df$nid[out.df$nid==6825] <- 6842
	out.df$nid[out.df$nid==6860] <- 6874
	out.df$nid[out.df$nid==6596] <- 6970
	out.df$nid[substr(out.df$ihme_loc_id,1,3)=="IND" & out.df$year_start==1998] <- 19950
	out.df$nid[out.df$nid==43518] <- 43526
	out.df$nid[out.df$nid==43549] <- 43552
	out.df$nid[out.df$nid==6956] <- 6970
	out.df$nid[out.df$nid==23165] <- 23183
	out.df$nid[out.df$nid==286722] <- 286772
	out.df$nid[out.df$nid==19803] <- 19794

## Try to add information to map to GBD subnationals ##
	subnat_map <- join(subnat_map, locs[,c("location_id","ihme_loc_id")], by="ihme_loc_id")
	subnat_map <- subnat_map[,c("parent_id","location","location_name_mapped","ihme_loc_id_mapped","location_id")]
	colnames(subnat_map)[c(1,5)] <- c("ihme_loc_id","location_id_mapped")
	subnat_map$subnat <- 1

	out.df <- join(out.df, subnat_map, by=c("ihme_loc_id","location"))
	out.df$ihme_loc_id <- ifelse(!is.na(out.df$ihme_loc_id_mapped), as.character(out.df$ihme_loc_id_mapped), as.character(out.df$ihme_loc_id))

	out.df$ihme_loc_id <- ifelse(is.na(out.df$ihme_loc_id_mapped) & out.df$location!=out.df$ihme_loc_id, as.character(out.df$location), as.character(out.df$ihme_loc_id))

	out.df <- out.df[,colnames(out.df)!="location_id"]
	out.df <- out.df[,colnames(out.df)!="location_name"]

	out.df$ihme_loc_id <- ifelse(out.df$location_ascii_name=="Ukraine","UKR",out.df$ihme_loc_id)

	out.df <- join(out.df, locs[,c("ihme_loc_id","location_id","location_name")], by="ihme_loc_id")

	out.df <- subset(out.df, !is.na(location_id))

	survey_ids <- data.frame(nid=unique(out.df$nid), drop_id=1)

## Pull epi data ##
 epi <- get_bundle_data(bundle_id=3, decomp_step="step2")
#
# ## Find where there is NID overlap in survey data ##
	epi <- join(epi, survey_ids, by="nid")
	drop <- subset(epi, drop_id==1)
	drop <- drop[,c("seq")]

## Add some miscellaneous things ##
	out.df$urbanicity_type <- "Mixed/both"
	out.df$unit_type <- "Person"
	out.df$source_type <- "Survey - cross-sectional"
	out.df$recall_type <- "Point"
	out.df$unit_value_as_published <- 1

	final.df <- rbind.fill(out.df, drop)

	final.df[is.na(final.df)] <- ""

	write.csv(final.df, "filepath")

######################################################################
## Diagnostic plots ##
######################################################################
epi$previous_mean <- epi$mean
out.df <- join(df, locs, by="ihme_loc_id")
e <- epi[,c("age_start","sex","nid","previous_mean","year_start","location_id")]
d <- out.df[,c("age_start","sex","nid","mean","year_start","location_id","location_name","region_name","super_region_name","level")]
plot.df <- join(df, e, by=c("age_start","sex","nid","year_start", "location_id"))

pdf("/filepath.pdf", height=8, width=11)
  ggplot(plot.df, aes(x=previous_mean, y=mean, col=super_region_name)) + geom_point() + theme_bw() + geom_abline(intercept=0, slope=1) + ggtitle("LRI survey data")

  ## Box plot by region ##
  box <- plot.df[,c("mean","previous_mean","region_name","location_name","level")]
  box <- melt(box, id.vars = c("region_name","location_name","level"))
  regions <- unique(box$region_name)
  for(r in regions){
    s <- subset(box, region_name==r)
    p <- ggplot(subset(s, level==3), aes(x=location_name, y=value, col=variable)) + geom_boxplot(position=position_dodge(width=0.9)) + theme_bw() + ylab("Value") +
      xlab("Location name") + scale_color_discrete(labels=c("Current","Used GBD 2016")) +
      theme(axis.text.x=element_text(angle=50, hjust=1)) + ggtitle(paste0("Input data for\n",r))
    print(p)

    if(max(s$level, na.rm=T)==4){
      g <- ggplot(subset(s, level==4), aes(x=location_name, y=value, col=variable)) + geom_boxplot(position=position_dodge(width=0.9))+ theme_bw() + ylab("Value") +
        xlab("Location name") + scale_color_discrete(labels=c("Current","Used GBD 2016")) +
        theme(axis.text.x=element_text(angle=50, hjust=1)) + ggtitle(paste0("Input data for\n",r," subnationals"))
      print(g)
    }
  }
dev.off()
############################################################################################################
## After fixing in Excel, upload! ##
source("filepath/upload_epi_data.R")
upload_epi_data(bundle_id=3, filepath = "filepath")
