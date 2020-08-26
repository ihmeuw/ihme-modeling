## Combine GEMS 1 and University of Virginia PCR results data ##
## The purpose of this file is to take the TAC and the original
## GEMS data and combine them into a usable file for further analyses ##
#########################################################################

library(plyr)

## This is the GEMS-1 file
gems1 <- read.csv("filepath")

## This is the UVA supplied TAC PCR results file:
gems.uva <- read.csv("filepath")
# Common names from GEMS1
#colnames(gems.uva)[1:2] <- c("LAB_SPECIMEN_ID", "CHILDID")
# Get pathogen names, add 'tac_' in front
#names <- colnames(gems.uva)[4:37]
#colnames(gems.uva)[4:37] <- paste("tac_", names, sep="")

# Function for converting TAC results to binary yes/no on pathogen detection
#x <- lapply(gems.uva[,4:37], function(x) ifelse((10^((35-x)/3.22)-1)>0,1,0))
#x <- data.frame(x)

# Call this new data frame 'binary_' pathogen name
#colnames(x) <- paste("binary_", names, sep="")
#gems.uva <- data.frame(gems.uva, x)

## New dataset provided on 12/2/15 ##
tac <- read.csv("filepath")
colnames(tac)[1] <- "LAB_SPECIMEN_ID"
# Combine ST/LT ETEC
tac$ETEC <- ifelse(tac$ST_ETEC<tac$LT_ETEC, tac$ST_ETEC, tac$LT_ETEC)

# Rename pathogens
colnames(tac)[6] <- "norovirus"
colnames(tac)[9] <- "adenovirus"
colnames(tac)[11] <- "campylobacter"
colnames(tac)[13] <- "entamoeba"
names <- colnames(tac)[5:25]
colnames(tac)[5:25] <- paste("tac_", names, sep="")

gems <- join(gems1, tac, by="LAB_SPECIMEN_ID")


# Join to master GEMS dataset
#gems <- join(gems1, gems.uva, by="LAB_SPECIMEN_ID")

## Create some variables
site.names <- data.frame(c("Gambia", "Mali","Mozambique","Kenya","India","Bangladesh","Karachi"), 1:7)
colnames(site.names) <- c("site.names","SITE")
gems <- join(gems, site.names, by="SITE")

# Is it a case
gems$case <- as.numeric(gems$Type)
gems$case <- ifelse(gems$case==1,1,0)
# What is the HAZ at diarrhea?
haz <- ifelse(is.na(gems$F7_HAZ), gems$F4B_HAZ, gems$F7_HAZ)
gems$haz <- ifelse(is.na(haz), 0, haz)

# For schooling, the codebook indicates 1=None, 2=<Primary, 3=Primary, 4=Secondary, 5=Post-Secondary, 6=Religious, 7=Unknown
# This is not exactly, but mostly ordinal.
# This gets the school level of the primary caretaker for cases (F4A) and controls (F7)
gems$school <- ifelse(is.na(gems$F4A_PRIM_SCHL), gems$F7_PRIM_SCHL, gems$F4A_PRIM_SCHL)
gems$school <- ifelse(gems$case==0, gems$F7_PRIM_SCHL, gems$F4A_PRIM_SCHL)
# The main GEMS paper appears to make education binomial: completed/not completed primary school (above/below 4) and does include religious (6)
primary.education <- ifelse(gems$school<4,0,ifelse(gems$school>6,0,1))
gems$primary.education <- primary.education

# Age 
age <- ifelse(is.na(gems$F2_AGE), gems$F6_AGE, gems$F2_AGE)
# Number of people in household
num.people <- ifelse(is.na(gems$F4A_PPL_HOUSE), gems$F7_PPL_HOUSE, gems$F4A_PPL_HOUSE)
num.children <- ifelse(is.na(gems$F4A_YNG_CHILDREN), gems$F7_YNG_CHILDREN, gems$F4A_YNG_CHILDREN)

# Floor type
floor <- ifelse(is.na(gems$F4A_FLOOR), gems$F7_FLOOR, gems$F4A_FLOOR)
floor.type <- ifelse(floor<3,0,1)

# Main water source
water.main <- ifelse(is.na(gems$F4A_MS_WATER), gems$F7_MS_WATER, gems$F4A_MS_WATER)
# Choices to make:
# Binary improved/unimproved
improved <- ifelse(water.main<4,1,ifelse(water.main<7,0,ifelse(water.main<12,1,ifelse(water.main==15,1,ifelse(water.main==17,1,0)))))
# Ordinal from best (piped into house), piped into yard, improved, to unimproved
water.type <- ifelse(water.main==1,1,ifelse(water.main==2,2,ifelse(improved==1,3,4)))

# Child feces disposal
disposal <- ifelse(is.na(gems$F4A_DISP_FECES), gems$F7_DISP_FECES, gems$F4A_DISP_FECES)
child.toilet <- ifelse(disposal==3,1,0)

# Family sanitation
sanitation <- ifelse(is.na(gems$F4A_FAC_WASTE), gems$F7_FAC_WASTE, gems$F4A_FAC_WASTE) 
# Ordinal from flush to ventilated pit to pit to none
sanitation.type <- ifelse(sanitation==1|sanitation==4,1,ifelse(sanitation==2|sanitation==7,2,ifelse(sanitation==3,3,4)))
table(sanitation, sanitation.type)

# Breastfeeding (0=No, 1=Partial, 2=Exclusive)
breast.feed <- ifelse(gems$F4A_BREASTFED=="", gems$F7_BREASTFED, gems$F4A_BREASTFED)
breast.fed <- ifelse(gems$case==1, breast.feed-2, breast.feed)
breast.fed <- ifelse(breast.fed==3,0,breast.fed)

# Gender
gender <- ifelse(is.na(gems$F2_GENDER),gems$F6_GENDER, gems$F2_GENDER)

# Join to GEMS data
covariates <- data.frame(breast.fed, child.toilet, disposal, floor, floor.type, haz, 
                         improved, num.children, num.people, primary.education, sanitation,
                         sanitation.type, water.main, water.type, gender)

gems.covariates <- data.frame(gems, covariates)

### Clean up some of the pathogens from GEMS ###
### Primary GEMS Results
# Global (all) campylobacter sub-species
gems$F16_CAMPY_JEJUNI_COLI <- with(gems, F16_CAMPY_JEJUNI+F16_CAMPY_COLI)
gems$F16_CAMPY_JEJUNI_COLI[gems$F16_CAMPY_JEJUNI_COLI==2] <- 1
# Combine Norovirus I and II
gems$F19_NOROVIRUS <- gems$F19_NORO_GII
# ESTA is a marker for heat-stable (ST) and ELTB for heat-labile (LT)
gems$F17_ETEC <- gems$F17_RESULT_ESTA + gems$F17_RESULT_ELTB
gems$F17_ETEC[gems$F17_ETEC==2] <- 1
# There are two variables for Adenovirus
# ADENO4041 is a subset of ADENOVIRUS positive samples
gems$F18_RES_ADENO4041[gems$F18_RES_ADENO4041==3] <- 0
# EAEC is AATA, AAIC, or Both
gems$F17_EAEC <- gems$F17_RESULT_AATA + gems$F17_RESULT_AAIC
gems$F17_EAEC[gems$F17_EAEC==2] <- 1
# tEPEC is BFPA+
gems$F17_tEPEC <- gems$F17_RESULT_BFPA

###### Save formatted file for further analyses ########
write.csv(gems, "filepath")

