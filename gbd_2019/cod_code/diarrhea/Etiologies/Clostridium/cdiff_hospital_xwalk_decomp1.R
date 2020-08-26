#####################################################
## Crosswalk C diff hospital data outside DisMod ##
#####################################################
source("/filepath/get_epi_data.R")
source("/filepath/get_location_metadata.R")
source("/filepath/upload_epi_data.R")
library(plyr)
library(ggplot2)
library(lme4)
library(boot)
locs <- read.csv("/filepath/ihme_loc_metadata_2017.csv")
ages <- read.csv("/filepath/age_mapping.csv")
haqi <- read.csv("/filepath/haqi_covariate.csv")

# df <- get_epi_data(bundle_id=14)
# # Save a copy before changing
# df <- data.frame(df)
# df[is.na(df)] <- ""
# write.csv(df, "filepath")
df <- read.csv("filepath")
new_df <- read.csv("filepath")
new_seq <- new_df$seq

df <- rbind.fill(df, new_df)

df <- subset(df, measure=="incidence")

df$original_mean <- df$mean

df <- subset(df, is_outlier==0)
df <- subset(df, is.na(group_review))
# Start with taking the mean, cutting by 10 year bands #
df$age_end <- ifelse(df$age_end>=100,99, df$age_end)
df$age_mean <- (df$age_end + df$age_start)/2
hist(df$age_mean)
df$age_cut <- cut(df$age_mean, c(0,5,100))
df <- df[,!colnames(df) %in% "ihme_loc_id"]
df <- join(df, locs[,c("location_id","ihme_loc_id", "region_name","super_region_name")], by="location_id")
#df$iso3 <- substr(df$ihme_loc_id,1,3)
df$iso3 <- df$ihme_loc_id
df$type <- ifelse(df$extractor=="users","Hospital","Reference")
ggplot(df, aes(x=age_start, y=mean, col=type)) + geom_point() + facet_wrap(~super_region_name) + theme_bw()
## Subset into inpatient, reference categories ##
## Remember to take out extractor == "users" after new hospital data uploaded ##
#inpatient <- subset(df, cv_inpatient_sample==1)
inpatient <- subset(df, extractor == "users" 
                    & note_modeler!="Used the 'inpt_anydx_ind_rate' as the input value for this model corrected for HAQI 
                    from hospital data and crosswalked to marketscan reference category")
inpatient$mean <- inpatient$mean / inpatient$haqi_cf
reference <- subset(df, extractor != "usersc" & measure=="incidence")

hist(reference$age_start)
hist(inpatient$age_start)

## Use iso3 to collapse as it provides more crosswalk vaues, lower uncertainty in regression ##
## Collapse by location and age group ##
mean_inp <- aggregate(mean ~ iso3 + age_cut, data=inpatient, function(x) mean(x))
mean_ref <- aggregate(mean ~ iso3 + age_cut, data=reference, function(x) mean(x))

colnames(mean_inp)[3] <- "inp_mean"
colnames(mean_ref)[3] <- "ref_mean"

## Make a ratio of reference to inpatient prevalence ##
xw <- join(mean_inp, mean_ref, by=c("iso3","age_cut"))
xw <- subset(xw, !is.na(ref_mean) & ref_mean>0)
xw$ihme_loc_id <- xw$iso3

xw$ratio <- xw$inp_mean / xw$ref_mean # xw$ref_mean / xw$inp_mean
xw <- join(xw, locs[,c("location_id","level","region_name","super_region_name","location_name","ihme_loc_id")], by="ihme_loc_id")

## Factor age group, include random intercept by super_region_name ##
xw <- subset(xw, ratio<1)
 ggplot(xw, aes(x=age_cut, y=ratio, col=super_region_name)) + geom_boxplot()
# ggplot(xw, aes(x=ref_mean, y=inp_mean, col=super_region_name)) + geom_point() + geom_abline(intercept=0, slope=1) + scale_y_log10() + scale_x_log10()
# ggplot(inpatient, aes(x=age_cut, y=mean, col=super_region_name)) + geom_boxplot()
 ggplot(reference, aes(x=age_cut, y=mean, col=as.factor(cv_marketscan))) + geom_boxplot()

xw$ln_ratio <- logit(xw$ratio)
mod <- lm(ln_ratio ~ factor(age_cut), data=xw)
summary(mod)

# make prediction table
adjdata <- with(inpatient, expand.grid(age_cut=unique(age_cut)))
adjdata$ln_xwalk <- predict(mod, newdata=adjdata)

adj_table <- adjdata

xw <- join(xw, adj_table, by=c("age_cut"))
xw$adjusted <- xw$inp_mean / inv.logit(xw$ln_xwalk)
i <- data.frame(xw[,c("iso3","region_name","super_region_name","adjusted","age_cut")])
colnames(i)[4] <- "mean"
i$type <- "Inpatient"
r <- data.frame(xw[,c("iso3","region_name","super_region_name","ref_mean","age_cut")])
colnames(r)[4] <- "mean"
r$type <- "Reference"
bx <- rbind(r,i)

## Join that back on the inpatient data ##
inp_df <- join(inpatient, adj_table, by=c("age_cut"))

inp_df$ln_mean <- logit(inp_df$mean)
inp_df$xwalk <- inv.logit(inp_df$ln_xwalk)
inp_df$mean_adj <- inp_df$mean / inp_df$xwalk
inp_df$se_xwalk <- inp_df$standard_error * (1+inp_df$xwalk)

inp_df$standard_error <- inp_df$se_xwalk
inp_df$mean <- as.numeric(inp_df$mean_adj)

inp_df[is.na(inp_df)] <- ""
inp_df$lower <- ""
inp_df$upper <- ""

pdf("filepath")
  ggplot(adj_table, aes(x=age_cut, y=exp(ln_xwalk))) + geom_point(size=2) + theme_bw()
  ggplot(bx, aes(x=age_cut, y=mean, col=type)) + geom_boxplot()
  ggplot(inp_df, aes(x=age_start, y=mean)) + geom_point() + facet_wrap(~super_region_name) + theme_bw()
dev.off()

inp_df <- subset(inp_df, seq %in% new_seq)

write.csv(inp_df, "filepath", row.names=F)

## HAQI correction ##
h_cor <- read.csv("filepath")
inp_hq <- join(inp_df, h_cor, by=c("nid","year_start","year_end","location_id"))
inp_hq$mean <- inp_hq$mean / inp_hq$haqi_cf
inp_hq$standard_error <- inp_hq$standard_error / inp_hq$haqi_cf
inp_hq$note_modeler <- paste0(inp_hq$note_modeler, " corrected for HAQI from hospital data and crosswalked to marketscan reference category")

write.csv(inp_hq, "filepath", row.names=F)

## Upload ##
upload_epi_data(bundle_id=14, filepath="filepath")
