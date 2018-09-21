################################################################################
## Description: Calculates under-5 VR completeness
################################################################################

  rm(list=ls())
  library(foreign); library(zoo)

  if (Sys.info()[1] == "Linux") root <- "FILEPATH" else root <- "FILEPATH"
  setwd("FILEPATH")
    
  # TEMP_DIR
  dir <- "FILEPATH"
  data <- read.table(paste(dir, "raw.5q0.unadjusted.txt", sep=""),stringsAsFactors=F,header=T)

## bring in location file from Shared database

  source("FILEPATH/shared/functions/get_locations.r")
  codes <- read.csv("FILEPATH/locations.csv", stringsAsFactors = F)
  codes <- codes[codes$level_all == 1 | codes$ihme_loc_id=="IND_44849",]
  codes <- codes[,c("ihme_loc_id","region_name")]
  names(codes) <- c("ihme_loc_id","gbd_region")

  ## add a caribbeanI region to the country codes dataset (important for VR-bias adjustments) 
  codes$gbd_region[codes$ihme_loc_id %in% c("GUY","TTO","BLZ","JAM","ATG","BHS","BMU","BRB","DMA","GRD","VCT","LCA","PRI")] <- "CaribbeanI"
  
###################
## Inital Data Prep
###################
## before we assess for completeness -- need to eliminate outliers and shock years from the dataset
  data <- data[data$outlier!=1 & data$shock!=1,]               

## merge in gbd region
  data <- merge(data,codes,by="ihme_loc_id",all.x=T)
  data$type <- data$in.direct
  data$in.direct <- NULL

## Correct Rapid Mortality (ZAF) so it's treated as VR and meshes with rest of code
data$source[grepl("Rapid Mortality Surveillance", data$source) & data$ihme_loc_id == "ZAF"] <- "ZAF RapidMortality Report 2011 - based on VR"

## make a vr dummy variable indicating which data observations are "vr"
## assign zaf rapid mortality report to vr
  data$vr <- 0
  
  data$vr[grepl(x=data$source,pattern="VR",ignore.case=TRUE) |
          grepl(x=data$source,pattern="vital registration",ignore.case=TRUE) |
          grepl(x=data$source,pattern="DSP",ignore.case=TRUE) |
          grepl(x=data$source,pattern="SRS",ignore.case=TRUE) |
          grepl("MCHS", data$source, ignore.case = TRUE) |
          grepl("Maternal and Child Health Surveillance",data$source, ignore.case = TRUE)] <- 1
  
## categorize data sources
  data$category <- NA
  data$category[(grepl(x=data$source,pattern="DHS",ignore.case=T) | 
                grepl(x=data$source,pattern="demographic and health",ignore.case=T)) & 
                data$type=="direct"] <- "dhs direct"
  data$category[(grepl("DHS",data$source,ignore.case=T) | 
                grepl("demographic and health",data$source,ignore.case=T)) &
                (data$type=="indirect" | data$type=="indirect, MAC only")] <- "dhs indirect"
  data$category[(grepl("WFS",data$source,ignore.case=T) | grepl("world",data$source,ignore.case=T))] <- "wfs"
  data$category[(grepl("MICS",data$source,ignore.case=T) | grepl("multiple indicator cluster", data$source,ignore.case=T)) &
                (data$type=="indirect" | data$type=="indirect, MAC only")] <- "mics"
  data$category[grepl("census",data$source,ignore.case=T) & (data$type=="indirect" | data$type=="indirect, MAC only")] <- "census" 
  data$category[grepl("CDC", data$source,ignore.case=T) & (data$type=="indirect" | data$type=="indirect, MAC only")] <- "cdc"
  data$category[data$vr==1] <- "vr"
  data$category[is.na(data$category)] <- "other"
  
## change 0 values of 5q0 to be 0.0001 so that the VR regression doesn't break
data$q5[data$q5== 0] <- 0.0001

##change source of VR- transmonee to be just VR, as these don't represent different VR systems in a country
data$source[data$source == "VR - TransMONEE"] <- "VR" 

#manually fix source so that Syria, Guyana, Korea, South Africa and its subnationals are treated as having two VR systems [Turkey does too, but is changed upstream]
  data$source[data$ihme_loc_id == "SYR" & data$vr == 1 & data$year > 2000] <- "VR1"
  data$source[data$ihme_loc_id == "GUY" & data$vr == 1 & data$year > 1965] <- "VR1"
  data$source[data$ihme_loc_id == "KAZ" & data$vr == 1 & data$year > 2008] <- "VR1"
  data$source[data$ihme_loc_id == "KOR" & data$vr == 1 & data$year > 1999] <- "VR1"
  data$source[data$ihme_loc_id == "IRN" & data$vr == 1 & data$year == 1991.5] <- "VR1"
  data$source[data$ihme_loc_id == "THA" & data$vr == 1 & data$year > 2003] <- "VR1"
  
#fix for BGD, which has SRS systems named inconsistently
data$source[data$ihme_loc_id == "BGD" & grepl("SRS", data$source)] <- "SRS"
 
###################
# Determine VR Bias Countries
###################

## loop through VR systems to determine if they are biased
  for(ihme_loc_id in unique(data$ihme_loc_id)) {
    #get different vr sources in this country
    vrsources <- unique(data$source[data$vr == 1 & data$ihme_loc_id == ihme_loc_id])
    for(src in vrsources){

        gbd_region <- as.character(data$gbd_region[data$ihme_loc_id == ihme_loc_id][1])

        # for countries that also have non-vr data
        if(nrow(data[data$vr == 0 & data$ihme_loc_id == ihme_loc_id,]) == 0){
          data$category[data$ihme_loc_id == ihme_loc_id & data$vr == 1] <- "vr_only"
        }else{

          # skip vr systems where there is no overlap with other sources
          vr.years <- range(floor(data$year[data$source == src & data$ihme_loc_id==ihme_loc_id]))
          other.years <- range(floor(data$year[data$vr ==0 & data$ihme_loc_id==ihme_loc_id]))
          if (sum(vr.years[1]:vr.years[2] %in% other.years[1]:other.years[2])==0){
            data$category[data$ihme_loc_id == ihme_loc_id & data$source == src] <- "vr_no_overlap"
            next
          }

          # regression of 5q0 on year with a vr dummy variable
          linear_model <- lm(log(q5,base=10) ~ year + vr,data=data[data$ihme_loc_id == ihme_loc_id & (data$vr == 0|data$source == src),])

          data$biascoef[data$source == src & data$ihme_loc_id == ihme_loc_id] <- coef(linear_model)["vr"]
          # assign to categories based on significance of the VR dummy
          pvalue <- summary(linear_model)$coefficients[,"Pr(>|t|)"]["vr"]
          if (pvalue < .05) {
            data$category[data$source == src & data$ihme_loc_id == ihme_loc_id] <- "vr_biased"
          } else {
            data$category[data$source == src & data$ihme_loc_id == ihme_loc_id] <- "vr_unbiased"
          }
      }
    } # end vr system loop
  } #end country loop

############################################
## Exceptions
############################################
  #Three categories of (biased or corrected in some way) data here:
  #1) Data that is biased and gets corrected - gets both types of variance added on  (to_correct = 1)
  #2) Data that is unbiased doesn't get corrected, but runs through the correction code for the additional bias variance and also gets add'l source.type variance
  #3) Data that is no-overlap but maybe biased, so can't go through the correction code, but gets add'l source.type variance (FSM, OMN, CarI)
  #AND
  #4) Data that is unbiased and receives no correction or additional variance

  # actually run correction and adjust upwards: to_correct = 1
  # add bias from correction code: corr_code_bias = 1
  # source-type variance category: category = "biased" OR "unbiased"

  #FIRST: variance category corrections
  ########################
  # First removing countries that are estimated to have over-complete VR -- we don't believe these countries are biased
  # Except India 
  data$category[data$category=="vr_biased" & data$biascoef >= 0 & data$vr == 1] <- "vr_unbiased"
  data$biascoef <- NULL

  # EXCEPTIONS: for SRS countries (e.g. PAK and BGD) that are vr - unbiased, still want to add additional data variance
  data$category[data$ihme_loc_id == c("SRB") & data$vr == 1] <- "vr_unbiased"
  data$category[data$ihme_loc_id %in% c("LBY", "UKR", "CRI") & data$vr == 1] <- "vr_biased"
  
  #Exception to some ZAF provinces 
  # VR is deemed complete because VR captures the upward trend in HIV and other survey sources didn't
  data$category[data$ihme_loc_id %in% c("ZAF_483", "ZAF_484", "ZAF_487", "ZAF_488", "ZAF_489", "ZAF_490") & data$vr == 1] <- "vr_biased"
  
  #Test running the VR in Russia as one series that is unbiased
  data$category[data$ihme_loc_id == "RUS" & data$source == "VR"] <- "vr_unbiased"

  # correct several countries that switched
  data$category[data$ihme_loc_id %in% c("COL", "PAN", "TKM", "ALB", "ZAF_486") & data$source == "VR"] <- "vr_biased"
  
  ###################################################
  #Everything that is "vr_biased" before here actually gets corrected upwards
  #If to_correct is true and data is "overcomplete" [above most data], no bias variance is added
  data$to_correct <- (data$category == "vr_biased")

  ##################################################
  ####Now switch countries that don't get corrected, but get run through code for variance
  # Set PAK, BGD, LBY, China and India subnational as incomplete
  # For SRS countries (PAK, BGD,IND) and China and India subnational we just want to add additional data variance (not HKG & MAC)
  data$category[(data$ihme_loc_id %in% c("PAK","BGD","IND","CHN_44533") | ((grepl("CHN",data$ihme_loc_id)) & !(data$ihme_loc_id %in% c("CHN_354", "CHN_361"))) | grepl("IND",data$ihme_loc_id)) & data$vr == 1] <- "vr_biased"
  data$to_correct[(data$ihme_loc_id %in% c("PAK","BGD","IND","CHN_44533") |((grepl("CHN",data$ihme_loc_id)) & !(data$ihme_loc_id %in% c("CHN_354", "CHN_361"))) | grepl("IND",data$ihme_loc_id)) & data$vr == 1] <- F
  #some old IND VR that actually needs correction
  data$to_correct[data$ihme_loc_id == "IND" & data$source == "VR"] <- T

  #Setting IRQ Kohli SRS point from the 70s to be biased for variance purposes
  data$category[data$ihme_loc_id == "IRQ" & grepl("SRS", data$source)] <- "vr_biased"
  data$to_correct[data$ihme_loc_id == "IRQ" & grepl("SRS", data$source)] <- F

  ####################################################
  #Correction code assignment
  #everything up to here that is vr_biased can be run through the correction code
  data$corr_code_bias <- NULL
  data$corr_code_bias <- (data$category == "vr_biased")

  #Don't add correction code bias variance to MCHS data (CHN nat & sub) though we do want "biased" source.type variance
  data$corr_code_bias[grepl("Maternal and Child Health Surveillance|MCHS", data$source)] <- F

  ####################################################
  #Assign no overlap and vr only countries to be biased or unbiased (can't go through correction code, but add st variance to biased)
  #and print names that are still unassigned to these two categories
  #1) find countries that are no overlap or vr only
  #2) assign to 'vr_biased' or 'vr_unbiased'
  data$category[data$category %in% c("vr_no_overlap","vr_only") & data$source == "VR" & data$ihme_loc_id %in% c("MNE","ARE")] <- "vr_unbiased"
  data$category[data$ihme_loc_id == "KOR" & data$source == "VR1"] <- "vr_unbiased"
  data$category[data$ihme_loc_id == "ZAF" & grepl("RapidMortality", data$source)] <- "vr_unbiased"
  data$category[data$category %in% c("vr_no_overlap","vr_only") & data$source == "VR" & data$ihme_loc_id %in% c("FSM","OMN")] <- "vr_biased"
  #CaribbeanI vr-only countries should be assigned to biased for variance purposes though they are adjusted differently than other sources as
  #there is no in-country data to use as comparison (see 02_fit...)
  data$category[data$category == "vr_only" & data$gbd_region == "CaribbeanI"] <- "vr_biased"
  #This is a no-overlap series from Russia
  # VR-SSA from South Africa is a no-overlap VR point. Assign to VR-biased 
  data$category[data$ihme_loc_id == "ZAF" & data$source == "VR-SSA"] <- "vr_biased"

# VR is not complete in KGZ 
data$category[data$ihme_loc_id == "KGZ" & data$source == "VR"] <- "vr_biased"

# manually set VR to be incomplete in Limpopo
data$category[data$ihme_loc_id == "ZAF_486" & data$source == "VR"] <- "vr_biased"

  #all other vr-only sources are assigned to complete
  data$category[data$category == "vr_only"] <- "vr_unbiased"


  #print countries that are still vr_only or no overlap. These countries should be assigned to biased or unbiased
  print("The following country/sources need to be assigned to vr biased or vr unbiased")
  print(unique(data[,c("ihme_loc_id","source","category")][data$category %in% c("vr_only","vr_no_overlap"),]))

####################################################
## Keeping Relevant Variables and Saving the Dataset
####################################################
  names(data)[names(data) == "q5"] <- "mort"

  data <- data[,c("ihme_loc_id","location_name","gbd_region","year","mort",
                  "source","source.date","type","vr","category",
                  "corr_code_bias","to_correct","log10.sd.q5","ptid")]
  
## write out the data file with the adjusted VR data
  write.table(data, file=paste(dir,"raw.5q0.adjusted.txt",sep=""),sep="\t",col.names=T,row.names=F)
  write.table(data, file=paste(dir,"/archive/raw.5q0.adjusted - ",format(Sys.time(), format = "%m-%d-%y"),".txt",sep=""),sep = "\t",col.names=T,row.names=F)
