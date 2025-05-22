###########################################################################
# Author:   USERNAME
# Date:     DATE
# Purpose:  RHD endemicity index creation for RHD modeling pipeline
# Steps
##          1. Load in all libraries and set arguments 
##          2. Build new year's index dataset
##          3. Run PCA and GMM to create clustered index
##          4. Create uncertainty in order to flag locations
##             that are highly uncertain 
##          5. Pull in previous year's endemicity index, flag 
##             locations that change endemicity status, and 
##             evaluate if they should 
##          6. Write final endemicity index for current year
###########################################################################


########################################################
#Step 1. Load in all libraries and set arguments 
########################################################


###### Libraries ######
packages <- c("data.table", "R.utils", "raster", "data.table", "ggplot2", "rgdal", "sf", "stringr", "tidyverse",
              "cowplot", "scales", "ggrepel", "kableExtra", "dplyr", "gridExtra", "formattable", "extrafont", "data.table", "magrittr")
suppressMessages(invisible(lapply(packages, library, character.only = TRUE)))

#install.packages('ggradar',lib='/FILEPATH/')
library(ggradar, lib.loc = '/FILEPATH')
library(factoextra, lib.loc = '/FILEPATH')

# Load central libraries
suppressMessages(sourceDirectory("/FILEPATH/", modifiedOnly=FALSE))

###### Paths ######
outdir <- "/FILEPATH/"



###### Arguments ######
# list the arguments to pull the data
release_id <- 16
decomp <- 'iterative'
year <- 2023
codcorrect_version <- 7936 
como_version <- 7936
daly_version <- 7936
burdenator_version <- 7936 

# list the cause
cause <- 492

# read in mapping function for this year
source("/FIELPATH/gbd2023_map.R")


# pull in cause/risk/location metadata
cause_metadata <- get_cause_metadata(cause_set_id =2, release_id = release_id) # reporting hierarchy
risk_metadata <- get_rei_metadata(rei_set_id = 1,release_id = release_id) # reporting hierarchy
loc_metadata <- get_location_metadata(location_set_id =9, release_id = release_id) # set 91 = locations in the public domain
age_meta <- get_age_metadata(age_group_set_id = 24, release_id = release_id)



########################################################
#Step 2. Build new year's index dataset
########################################################


#SDI
sdi <- get_covariate_estimates(
  covariate_id=881,
  location_id="all",
  release_id=release_id, 
  year_id = year
)
sdi$sdi_mean <- sdi$mean_value
sdi2 <- sdi[,c(6,4,14)]

#HAQI
haqi <- get_covariate_estimates(
  covariate_id=1099,
  location_id="all",
  release_id=release_id, 
  year_id =year
)
haqi$haqi_mean <- haqi$mean_value
haqi2 <-haqi[,c(6,4,14)]

#merge SDI, HAQI
rhdset<- merge(sdi2, haqi2, by= c("location_id", "year_id"))

#Sanitation
sanitation <- get_covariate_estimates(
  covariate_id=142,
  location_id="all",
  release_id=release_id, 
  year_id =year
)

sanitation$sanitation_mean <- sanitation$mean_value
sanitation2 <-sanitation[,c(6,4,14)]
rhdset<- merge(rhdset, sanitation2, by= c("location_id","year_id"), all.x=TRUE)

#Water
water <- get_covariate_estimates(
  covariate_id=160,
  location_id="all",
  release_id=release_id, 
  year_id =year
)

water$water_mean <- water$mean_value
water2 <-water[,c(6,4,14)]
rhdset<- merge(rhdset, water2, by= c("location_id","year_id"), all.x=TRUE)

#Underweight
underweight <-get_covariate_estimates(
  covariate_id=1230,
  location_id='all',
  release_id=release_id, 
  year_id =year
)

underweight$underweight_mean <- underweight$mean_value
underweight2 <-underweight[,c(6,4,14)]
rhdset<- merge(rhdset, underweight2, by= c("location_id","year_id"), all.x=TRUE)

#CoD from JACC 2022
allcod10_14<-get_outputs(topic='cause', release_id=9, compare_version_id=7936,
                         cause_id = 294,  year_id = 2022, age_group_id = 7, sex_id = 3, 
                         metric_id = 3, measure_id = 1, location_id='all')

allcod10_14$val10_14 <-allcod10_14$val
allcod10_14_2 <-allcod10_14[,c(3,21)]


rhdset<- merge(rhdset, allcod10_14_2, by= c("location_id"))
rhdset1<- merge(rhdset, loc_metadata, by="location_id")

#final rhd parent dataset
rhdset_final <- rhdset1[,c(1:8, 17,22)]
finalrhd<- subset(rhdset_final, !is.na(rhdset_final$water_mean))
finalrhd<- subset(finalrhd, !is.na(finalrhd$val10_14))

#invert variables where necessary
finalrhd$underw_inv <- (finalrhd$underweight_mean*-1) 
finalrhd$val10_14inv <- (finalrhd$val10_14*-1)

finalrhd1 <- finalrhd[,c(1:6,11:12,9:10)] 


#rescale all variables
finalrhd2<- finalrhd1
finalrhd2$sdi_mean <- scales::rescale(finalrhd2$sdi_mean, to=c(0,1))
finalrhd2$haqi_mean <- scales::rescale(finalrhd2$haqi_mean, to=c(0,1))
finalrhd2$sanitation_mean <- scales::rescale(finalrhd2$sanitation_mean, to=c(0,1))
finalrhd2$water_mean <- scales::rescale(finalrhd2$water_mean, to=c(0,1))
finalrhd2$underw_inv <- scales::rescale(finalrhd2$underw_inv, to=c(0,1))
finalrhd2$val10_14inv <- scales::rescale(finalrhd2$val10_14inv, to=c(0,1))



########################################################
#Step 3. Run PCA and GMM to create clustered index
########################################################


#run pca for all variables with sdi and all mort (no gini)
pc1 <- prcomp(finalrhd2[,c(3:8)],
              center = TRUE,
              scale. = TRUE)
#examine outputs of pc1
table1 <- pc1$rotation
print(table1)
#write.csv(table1, paste0(outdir, "table1_2021.csv"))
#summarize outputs of pc1
summary(pc1)

#reverse the signs (not for 2000 or 1990)
pc1$rotation <- -1*pc1$rotation
pc1$x <- -1*pc1$x
print(pc1$sdev^2)

#prepare to attach back to original dataset
#(dont multiply for 2000 or 1990)
pc1$x <- pc1$x
rhdfinal2 <- cbind(finalrhd2, pc1$x)

#rescale PC1
rhdfinal2$PC1 <- scales::rescale(rhdfinal2$PC1 , to=c(0,1))

#screeplot
screeplot(pc1, type = "l")
abline(h=1)

#####Just map pc1
rhdfinal2$mapvar <- rhdfinal2$PC1
#rhdfinal2$mapvar <- 1-rhdfinal2$mapvar

xmin <- min(rhdfinal2$mapvar, na.rm=TRUE)
xmax <- max(rhdfinal2$mapvar,na.rm=TRUE)/1.25
# set bin limits based on the 'xmin' and 'xmax'
lim <- seq(floor(xmin), ceiling(xmax), length.out = 11)
# resets upper and lower limit of bins to match the actual max and min values
lim[1] <- floor(min(rhdfinal2$mapvar, na.rm=TRUE))
lim[11] <- ceiling(max(rhdfinal2$mapvar, na.rm=TRUE))
# round limits
lim <- round(lim, digits = ifelse(max(rhdfinal2$mapvar,na.rm=TRUE)<15, 1, 0))

limlen <- length(lim)-1

# label bins
labs <- c(paste0("< ", lim[2]), paste0(lim[2:(limlen-1)], " to < ", lim[3:limlen]), paste0(">= ", lim[limlen]))

message(paste0(lim, sep=" | "))
message(paste0(labs, sep=" | "))


#pdf(paste0(outdir, "Rheumatic Heart Disease Endemicity Index ", year, ".pdf"), width=15, height=8.3)
gbd_map(rhdfinal2, 
        limits=lim, # change to whatever bins make sense for your data
        sub_nat='none', # prints map with topic paper level subnationals
        labels=labs, # label bins in the legend
        title= paste0("Rheumatic Heart Disease Endemicity Continuous Index ", year),
        pattern=NULL, col="YlGnBu", col.reverse=TRUE, na.color="gray",
        legend.title=NULL, legend.columns=1, legend.cex=1, legend.shift=c(0, 0), inset=FALSE)
dev.off()





# #Prepare for GMM

#diagnostic plots
pca1 <-rhdfinal2
summary(pca1$mapvar)
plot(pca1$mapvar)
fviz_pca_var(pc1, col.var = "black")

#run gmm 
pca1mc <- Mclust(pca1$PC1,4)
#check best cluster number, then go back and fill in number
#pca1mc$G


#more diagnostic plots
plot(pca1mc, "classification", xlab ="PCA Index Value (0-1)")
plot(pca1mc, "density", xlab ="PCA Index Value (0-1)")

#Check BIC
pca1mcbic <- mclustBIC(pca1$PC1)
plot(pca1mcbic)
summary(pca1mcbic)


#check quantile
plot(pca1mc, what = "classification")


#Calculating the breaks hist() would use
brx <- pretty(range(pca1$PC1), 
              n = 20)

#Adding the classification to the dataframe for the colors.
pca1$classification <- as.factor(pca1mc$classification)
pca1$uncertainty <-pca1mc$uncertainty
pca1$z <- as.factor(pca1mc$z)

#Plotting the histograms, adding the density (scaled * 80) and adding a 2nd y-axis to show that scale
ggplot(pca1, aes(x=pca1$PC1, y= after_stat(density), fill= classification)) + 
  #geom_histogram( alpha = 0.7)+
  geom_density(aes(y = after_stat(density) , col=classification, fill = NULL), size = 1) +
  scale_color_manual(breaks = c("1", "2", "3","4"), 
                    values=c("red", "tan1", "skyblue1","#2c7bb6"))+
  # scale_y_continuous(sec.axis = sec_axis(~./10))+
  theme_bw()+
  ylab("Count of Locations")+
  xlab("PC1 Score")+
  ylim(0,25)+ labs(colour = "GMM Classification")

# assining cluster to the original data set
rhdfinal2mc <- cbind(pca1, mapvar1 = pca1mc$classification)

rhdfinal3 <- rhdfinal2mc
rhdfinal3$mapvar <- rhdfinal3$mapvar1

gbd_map(rhdfinal3, 
        limits=c(1,2,3,4,5), # change to whatever bins make sense for your data
        sub_nat='topic', # prints map with topic paper level subnationals
        labels=labs, # label bins in the legend
        title= paste0("Rheumatic Heart Disease Endemicity Continuous Index ", year),
        pattern=NULL, col.reverse=FALSE, na.color="gray",
        legend.title=NULL, legend.columns=1, legend.cex=1, legend.shift=c(0, 0), inset=FALSE)
dev.off()


########################################################
#Step 4. Create uncertainty in order to flag locations
#        that are highly uncertain 
########################################################



#bootstrap so that uncertainty can be produced
pca1mc$parameters[["mean"]]
boot <- MclustBootstrap(pca1mc)
par(mfrow = c(1,4))
plot(boot, what = 'pro') 
par(mfrow = c(1,4)) 
plot(boot, what = 'mean') 
par(mfrow = c(1,1))
rhdfinal2mc1 <- rhdfinal2mc

#this will produce uncertainty, and flag highly uncertain values

rhdfinal2mc1<-rhdfinal2mc1 %>% 
  group_by(classification) %>% 
  mutate(hi_uncertain = ifelse(uncertainty> quantile(uncertainty,probs=0.80),1,0)) 

#plot of uncertainty by cluster
hist(rhdfinal2mc1$uncertainty)
ggplot(rhdfinal2mc1, aes(x=uncertainty)) + 
  geom_histogram(aes(y=..density..,group=hi_uncertain,color=hi_uncertain), fill="white")+
  geom_density(alpha=.2, fill="#FF6666")+
  facet_wrap(~classification)

#flag unceratinties in border cluster
rhdfinal2mc1$mapvar <- ifelse(rhdfinal2mc1$mapvar1==1|rhdfinal2mc1$mapvar1==2,1,"")


#change variable names in preparation for joining below
rhdfinal2mc1[, paste0("endemic",year)]  <- ifelse(rhdfinal2mc1$mapvar==1, "endemic", "non-endemic")
rhdfinal2mc1[, paste0("pc1_",year)] <- rhdfinal2mc1$PC1


########################################################
#Step 5. Pull in previous year's endemicity index, flag 
#        locations that change endemicity status, and 
#        evaluate if they should 
########################################################


#read in previous years endemicity, then join and flag highly uncertain locations that changed endemicity status 
end <- read.csv('/FILEPATH/FILENAME.csv')
end22 <- end

end22_21_w_unc <- merge(rhdfinal2mc1, end22, by="location_id")
end22_21_w_unc$endemic_2023 <- ifelse(end22_21_w_unc$endemic2023=="endemic", 1,0)

end22_21_w_unc$switchend22_21 <- ifelse(end22_21_w_unc$endemic_2023!=end22_21_w_unc$endemic_2022,1,0)

switchend22_21<- subset(end22_21_w_unc, end22_21_w_unc$switchend22_21==1 & end22_21_w_unc$hi_uncertain==0)
print(switchend22_21$location_name)

#check printed list by hand, investigate data/consult experts to decide if any should actually be switched
#GBD 2023: none of flagged locations should be switched. 


#change values with high uncertainty back to original endemicity
final_end_22 <- end22_21_w_unc
final_end_22<- within(final_end_22, endemic_2023[switchend22_21==1] <- 1)

#confirm
final_end_22$switchend22_21 <- ifelse(final_end_22$endemic_2022!=final_end_22$endemic_2023,1,0)

########################################################
#Step 6. Write final endemicity index for current year
######################################################## 

rhdtowrite <- final_end_22[, c(1,27)]
 
write.csv(rhdtowrite, paste0(outdir, "endemicity", year ,".csv"))

