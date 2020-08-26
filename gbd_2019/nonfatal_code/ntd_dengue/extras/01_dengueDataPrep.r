########################################################
########################################################

##	Prep R

require(lme4)
library(readstata13, lib = "~/packages")

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

##	Load shared function

source(sprintf(FILEPATH",prefix))
source(sprintf(FILEPATH",prefix))
source(sprintf(FILEPATH",prefix))
source(sprintf(FILEPATH",prefix))
source(sprintf(FILEPATH",prefix))


##  Load data

DENV <- read.csv(sprintf("FILEPATH",prefix))
DENV <- DENV[grep("case", DENV$source_type, ignore.case = TRUE),]



## Clean up age-specific data points (model is based on all-age, both-sex data)

AllAge <- which(DENV$age_start == 0 & DENV$age_end == 99)
DENVAA <- DENV[AllAge,]
DENVNAA <- DENV[-AllAge,]

## Check for partitioning age bins, re-aggregate them and drop everything else.
Keep <- {}
ULoc <- unique(DENVNAA$location_id)
UYears <- unique(DENVNAA$year_start[DENVNAA$year_start == DENVNAA$year_end])

for (tmploc in ULoc){
  for (tmpyear in UYears){
    tmprows <- which(DENVNAA$location_id == tmploc & DENVNAA$year_start == tmpyear)
    UGender <- unique(DENVNAA$sex[tmprows])
    if (length(tmprows)){
      for (tmpGender in UGender){
        tmpnewrows <- which(DENVNAA$location_id == tmploc & DENVNAA$year_start == tmpyear & DENVNAA$sex == tmpGender)
        if (length(tmpnewrows) > 1){
          tmpAges <- as.vector(sapply(tmpnewrows,function(x)c(DENVNAA$age_start[x],DENVNAA$age_end[x])))
          tmpMaxGap <- max(diff(tmpAges)[seq(2,length(tmpAges)-1,by=2)])
          if (tmpAges[1] == 0 & tmpAges[length(tmpAges)] == 99 & tmpMaxGap <= 1){
            Keep <- c(Keep, tmpnewrows[1])
            DENVNAA$age_start[tmpnewrows[1]] <- 0
            DENVNAA$age_end[tmpnewrows[1]] <- 99
            DENVNAA$cases[tmpnewrows[1]] <- sum(DENVNAA$cases[tmpnewrows])
            DENVNAA$sample_size[tmpnewrows[1]] <- sum(DENVNAA$sample_size[tmpnewrows])
            DENVNAA$mean[tmpnewrows[1]] <- DENVNAA$cases[tmpnewrows[1]] / DENVNAA$sample_size[tmpnewrows[1]]
            DENVNAA$lower[tmpnewrows[1]] <- NA
            DENVNAA$upper[tmpnewrows[1]] <- NA
            DENVNAA$standard_error[tmpnewrows[1]] <- NA
            DENVNAA$effective_sample_size[tmpnewrows[1]] <- NA
          }
        }
      }
    }
  }
}

DENVAA <- rbind(DENVAA,DENVNAA[Keep,])

# Pull just those that are all age and all sex combined

AllAgeSex <- which(DENVAA$sex == "Both")
DENVAAS <- DENVAA[AllAgeSex,]

## Where we have all-age, sex-sepcific data points for a location/year, combine them to be both sex and drop all other points

DENVNAAS <- DENVAA[-AllAgeSex,]

HasBoth <- {}
UYears <- unique(DENVNAAS$year_start[DENVNAAS$year_start == DENVNAAS$year_end])
ULoc <- unique(DENVNAAS$location_id)

for (tmploc in ULoc){
  for (tmpyear in UYears){
    tmpmatch <- which(DENVNAAS$year_start == tmpyear & DENVNAAS$location_id == tmploc)
    if (length(tmpmatch)){
      tmpmale <- which(DENVNAAS$sex[tmpmatch] == "Male")
      tmpfemale <- which(DENVNAAS$sex[tmpmatch] == "Female")
      if (length(tmpmale) == 1 & length(tmpfemale) == 1){
        HasBoth <- c(HasBoth,tmpmatch[1])
        DENVNAAS$sex[tmpmatch[1]] <- "Both"
        DENVNAAS$cases[tmpmatch[1]] <- sum(DENVNAAS$cases[tmpmatch])
        DENVNAAS$sample_size[tmpmatch[1]] <- sum(DENVNAAS$sample_size[tmpmatch])
        DENVNAAS$mean[tmpmatch[1]] <- DENVNAAS$cases[tmpmatch[1]] / DENVNAAS$sample_size[tmpmatch[1]]
        DENVNAAS$lower[tmpmatch[1]] <- NA
        DENVNAAS$upper[tmpmatch[1]] <- NA
        DENVNAAS$standard_error[tmpmatch[1]] <- NA
        DENVNAAS$effective_sample_size[tmpmatch[1]] <- NA
      }
    }
  }
}

FinalDENV <- rbind(DENVAAS,DENVNAAS[HasBoth,])


ULoc <- unique(FinalDENV$location_id)
UYears <- unique(FinalDENV$year_start[FinalDENV$year_start == FinalDENV$year_end])

for (tmploc in ULoc){
  for (tmpyear in UYears){
    tmpmatch <- which(FinalDENV$year_start == tmpyear & FinalDENV$location_id == tmploc)
    if (length(tmpmatch) > 1){
      FinalDENV <- FinalDENV[-head(tmpmatch,-1),]
    }
  }
}


MexData <- read.dta13(sprintf("FILEPATH",prefix))
MexData$mean <- MexData$cases / MexData$sample_size
# This one is already there...
MexData <- MexData[-which(MexData$location_id == 4648 & MexData$year_id == 2002),]

IndData <- read.dta13(sprintf("FILEPATH",prefix))



DENVPAHO2016 <- read.csv(sprintf("FILEPATH",prefix))
DENVPAHO2016 <- DENVPAHO2016[-grep("severe", DENVPAHO2016$case_name, ignore.case = TRUE),]
Toss <- which(DENVPAHO2016$age_start != 0 | DENVPAHO2016$age_end != 99 | DENVPAHO2016$case_name == "DHF cases" | DENVPAHO2016$year_start < 2016)
DENVPAHO2016 <- DENVPAHO2016[-Toss,]
DENVASEAN2016 <- read.csv(sprintf("FILEPATH",prefix))
Toss <- which(DENVASEAN2016$age_start != 0 | DENVASEAN2016$age_end != 99 | DENVASEAN2016$case_name == "DHF cases" | DENVASEAN2016$year_start < 2016)
DENVASEAN2016 <- DENVASEAN2016[-Toss,]

DENV2017 <- read.csv(sprintf("FILEPATH",prefix))
DENV2017 <- DENV2017[grep("case", DENV2017$source_type, ignore.case = TRUE),]
DENV2017 <- DENV2017[grep("incidence", DENV2017$measure, ignore.case = TRUE),]
TossAge <- which(DENV2017$age_start != 0 | DENV2017$age_end != 99)
DENV2017 <- DENV2017[-TossAge,]
TossSev <- grep("sev", DENV2017$severity, ignore.case = TRUE)
DENV2017 <- DENV2017[-TossSev,]
TossWarning <- grep("war", DENV2017$severity, ignore.case = TRUE)
DENV2017 <- DENV2017[-TossWarning,]
# Fix China
FixChina <- which(DENV2017$ihme_loc_id == "CHN")
NewRow <- DENV2017[FixChina[1],]
NewRow$cases <- sum(DENV2017$cases[FixChina])
DENV2017 <- DENV2017[-FixChina,]
DENV2017 <- rbind(DENV2017,NewRow)
# Delete France
DENV2017 <- DENV2017[-which(DENV2017$location_id == 80),]
# Other Severity clean
DENV2017 <- DENV2017[grep("any", DENV2017$severity, ignore.case = TRUE),]
# Just "both sex"
DENV2017 <- DENV2017[grep("both", DENV2017$sex, ignore.case = TRUE),]

####
#### Grabbing MAX of those with multiple (may confound a few places with both suspected and confirmed)
####
MultipleEntries <- names(which(table(DENV2017$ihme_loc_id) > 1))
for (i in 1:length(MultipleEntries)){
  tmplocs <- which(DENV2017$ihme_loc_id == MultipleEntries[i])
  maxrow <- which.max(DENV2017$cases[tmplocs])
  tmprow <- DENV2017[tmplocs[maxrow],]
  DENV2017 <- DENV2017[-tmplocs,]
  DENV2017 <- rbind(DENV2017,tmprow)
}

##########
########## Combine all data
##########

tmplocation_id <- c(FinalDENV$location_id, MexData$location_id, IndData$location_id, DENVPAHO2016$location_id, DENVASEAN2016$location_id, DENV2017$location_id)
tmpyear <- c(FinalDENV$year_start, MexData$year_id, IndData$year_start, DENVPAHO2016$year_start, DENVASEAN2016$year_start, DENV2017$year_start)
tmpcases <- c(FinalDENV$cases, MexData$cases, IndData$cases, DENVPAHO2016$cases, DENVASEAN2016$cases, DENV2017$cases)
tmplocation_name <- c(FinalDENV$location_name, MexData$location_name, IndData$location_name, DENVPAHO2016$location_name, DENVASEAN2016$location_name, DENV2017$location_name)
tmpsample_size <- c(FinalDENV$sample_size, MexData$sample_size, IndData$sample_size, DENVPAHO2016$sample_size, DENVASEAN2016$sample_size, DENV2017$sample_size)
tmpmean <- c(FinalDENV$mean, MexData$mean, IndData$mean, DENVPAHO2016$mean, DENVASEAN2016$mean, DENV2017$mean)




## Bring in and merge covariate data
# HAQI = ADDRESS
haqi <- get_covariate_estimates(covariate_id = ADDRESS)
# SDI = ADDRESS
sdi <- get_covariate_estimates(covariate_id = ADDRESS)
# dengue_prob = ADDRESS
dengue_prob <- get_covariate_estimates(covariate_id = 247)

tmphaqi <- rep(NA,length(tmplocation_id))
tmpsdi <- rep(NA,length(tmplocation_id))
tmpDENV_p <- rep(NA,length(tmplocation_id))

for (i in 1:length(tmplocation_id)){
  # HAQI
  tmpmatch <- which(haqi$location_id == tmplocation_id[i] & haqi$year_id == tmpyear[i])
  tmphaqi[i] <- haqi$mean_value[tmpmatch]
  # SDI
  tmpmatch <- which(sdi$location_id == tmplocation_id[i] & sdi$year_id == tmpyear[i])
  tmpsdi[i] <- sdi$mean_value[tmpmatch]
  # dengue_prob
  tmpmatch <- which(dengue_prob$location_id == tmplocation_id[i] & dengue_prob$year_id == tmpyear[i])
  tmpDENV_p[i] <- dengue_prob$mean_value[tmpmatch]
}

locdat <- get_location_metadata(location_set_id=8)
## Grab location metadata
i <- 1
tmploc <- which(locdat$location_id == tmplocation_id[i])

tmpiso3 <- rep(locdat$ihme_loc_id[tmploc],length(tmplocation_id))
tmpihme_loc_id <- rep(locdat$ihme_loc_id[tmploc],length(tmplocation_id))
tmplocation_type <- rep(locdat$location_type[tmploc],length(tmplocation_id))
tmpcountry_id <- rep(as.numeric(strsplit(locdat$path_to_top_parent[tmploc],",")[[1]][4]),length(tmplocation_id))
tmpsuper_region_id <- rep(locdat$super_region_id[tmploc],length(tmplocation_id))
tmpsuper_region_name <- rep(locdat$super_region_name[tmploc])
tmpregion_id <- rep(locdat$region_id[tmploc],length(tmplocation_id))
tmpregion_name <- rep(locdat$region_name[tmploc],length(tmplocation_id))
tmpis_estimate <- rep(locdat$is_estimate[tmploc],length(tmplocation_id))


for (i in 1:length(tmplocation_id)){
  tmploc <- which(locdat$location_id == tmplocation_id[i])
  if (length(tmploc)){
    tmpiso3[i] <- locdat$ihme_loc_id[tmploc]
    tmpihme_loc_id[i] <- locdat$ihme_loc_id[tmploc]
    tmplocation_type[i] <- locdat$location_type[tmploc]
    tmpcountry_id[i] <- as.numeric(strsplit(locdat$path_to_top_parent[tmploc],",")[[1]][4])
    tmpsuper_region_id[i] <- locdat$super_region_id[tmploc]
    tmpsuper_region_name[i] <- locdat$super_region_name[tmploc]
    tmpregion_id[i] <- locdat$region_id[tmploc]
    tmpregion_name[i] <- locdat$region_name[tmploc]
    tmpis_estimate[i] <- locdat$is_estimate[tmploc]
  } else {
    tmpiso3[i] <- NA
    tmpihme_loc_id[i] <- NA
    tmplocation_type[i] <- NA
    tmpcountry_id[i] <- NA
    tmpsuper_region_id[i] <- NA
    tmpsuper_region_name[i] <- NA
    tmpregion_id[i] <- NA
    tmpregion_name[i] <- NA
    tmpis_estimate[i] <- NA
  }
}




## Grab mortality and population estimates

tmppop <- rep(NA,length(tmplocation_id))
AllNeededPops <- get_population(age_group_id = 22, location_id = tmplocation_id, year_id = tmpyear, sex_id = 3)



pb <- txtProgressBar(min=1,max=length(tmplocation_id),initial=1,style=3)
for (i in 1:length(tmplocation_id)){
  setTxtProgressBar(pb,i)
  tmploc <- which(AllNeededPops$location_id == tmplocation_id[i] & AllNeededPops$year_id == tmpyear[i])
  tmppop[i] <- AllNeededPops$population[tmploc]
}
close(pb)

NewerData <- which(tmpyear > 2015)
tmpsample_size[NewerData] <- tmppop[NewerData]
tmpmean[NewerData] <- tmpcases[NewerData] / tmpsample_size[NewerData]



ULocs <- unique(tmplocation_id)
UYears <- unique(tmpyear)

df3<-get_draws(gbd_id_type="cause_id",gbd_id=ADDRESS,location_id=ULocs, year_id=UYears, sex_id = 1:2, source="codem", status="best", gbd_round_id = 5, num_workers = 30)

draws <- names(df3)[grep("draw", names(df3)) ]

df3[,mean := rowMeans(.SD), .SDcols = draws]
out <- df3[,sum(mean), by=list(location_id, year_id)]


tmpdeath <- rep(NA, length(tmplocation_id))
pb <- txtProgressBar(min=1,max=length(tmplocation_id),initial=1,style=3)
for (i in 1:length(tmplocation_id)){
  setTxtProgressBar(pb,i)
  tmploc <- which(out$location_id == tmplocation_id[i] & out$year_id == tmpyear[i])
  tmpdeath[i] <- as.numeric(out[tmploc,3])
}
close(pb)






## Create some transformed covaraites

DENVData <- data.frame(location_id = tmplocation_id,
                       year_id = tmpyear,
                       cases = tmpcases,
                       location_name = tmplocation_name,
                       sample_size = tmpsample_size,
                       mean = tmpmean,
                       haqi = tmphaqi,
                       sdi = tmpsdi,
                       denge_prob = tmpDENV_p,
                       iso3 = tmpiso3,
                       ihme_loc_id = tmpihme_loc_id,
                       location_type = tmplocation_type,
                       country_id = tmpcountry_id,
                       super_region_id = tmpsuper_region_id,
                       super_region_name = tmpsuper_region_name,
                       region_id = tmpregion_id,
                       region_name = tmpregion_name,
                       is_estimate = tmpis_estimate,
                       denvdeath = tmpdeath)

Keep0 <- which(DENVData$is_estimate == 1 | DENVData$location_name == "admin0")
DENVData0 <- DENVData[Keep0,]


write.csv(DENVData, sprintf("FILEPATH",prefix))


DENVData <- read.csv(sprintf("FILEPATH",prefix))


eEF <- read.dta13(sprintf("FILEPATH",prefix))
eTRR <- read.dta13(sprintf("FILEPATH",prefix))
eID <- read.dta13(sprintf("FILEPATH",prefix))

FullData <- merge(DENVData0, eEF, by = c("year_id","location_id"), all.x = TRUE)
FullData <- merge(DENVData0, eTRR, by = c("year_id","location_id"), all.x = TRUE)
FullData <- merge(DENVData0, eID, by = "year_id", all.x = TRUE)

write.csv(FullData, sprintf("FILEPATH",prefix))

####
###
####

FullData$deathrate <- FullData$denvdeath / FullData$sample_size

FullData$sqrtdenvdeath <- sqrt(FullData$deathrate)
UIso <- unique(FullData$iso3)
FullData$meandeath <- FullData$deathrate
FullData$meansqrtdeath <- FullData$deathrate
for (i in 1:length(UIso)){
  tmplocs <- which(FullData$iso3 == UIso[i])
  FullData$meandeath[tmplocs] <- mean(FullData$deathrate[tmplocs])
  FullData$meansqrtdeath[tmplocs] <- mean(FullData$sqrtdenvdeath[tmplocs])
}

Offset <- 0.0001

FullData$casesM <- FullData$cases
FullData$casesM[which(FullData$denge_prob > 0 & FullData$cases == 0)] <- Offset
FullData$casesM[which(FullData$denge_prob == 0)] <- 0
FullData$yearW <- 5 * round(FullData$year_id / 5)
FullData$yearW[FullData$yearW == 2015] <- 2017
FullData$sampleM <- FullData$sample_size
FullData$meanM <- FullData$casesM / FullData$sampleM
FullData$score <- prcomp(cbind(FullData$denge_prob, scale(FullData$meansqrtdeath)))$x[,1]

CanHazDENV <- FullData[FullData$denge_prob > 0,]

mod0 <- lm(score ~ denge_prob, data = CanHazDENV)

tmpscore <- CanHazDENV$score
tmpscore[abs(mod0$residuals/summary(mod0)$sig) > 3] <- mod0$fitted.values[abs(mod0$residuals/summary(mod0)$sig) > 3]
FullData$scoreM <- FullData$score
FullData$scoreM[FullData$denge_prob > 0] <- tmpscore

write.csv(FullData, sprintf("FILEPATH",prefix))

AgeDist <- read.dta13(sprintf("FILEPATH",prefix))

ef_toan <- read.csv(sprintf("FILEPATH", prefix))


FullData$LinTrend <- 0 * FullData$denge_prob
FullData$logCasesM <- log(FullData$casesM)
FullData$logmeanM <- log(FullData$meanM)

CanHazDENV <- FullData[FullData$denge_prob > 0,]

mod <- lmer(meansqrtdeath ~ year_id + (1|region_name), data=CanHazDENV)

FullData$LinTrend[which(FullData$denge_prob > 0)] <- predict(mod)

CanHazDENV <- FullData[FullData$denge_prob > 0.0001 & !(FullData$denge_prob > 0 & FullData$cases == 0),]
CanHazDENV <- CanHazDENV[-2214,]

CanHazDENV$logLinTrend <- log(CanHazDENV$LinTrend)
CanHazDENV$logscore <- log(CanHazDENV$score+1)

mod2 <- lmer(logmeanM ~ logLinTrend * logscore  + (1|location_id), offset = log(sampleM), data = CanHazDENV)

CanHaz <- unique(CanHazDENV$location_id)


AllDeath <- get_draws(gbd_id_type="cause_id",gbd_id=357,location_id=CanHaz, year_id=1990:2017, sex_id = 1:2, source="codem", status="best", gbd_round_id = 5, num_workers = 30)

draws <- names(AllDeath)[grep("draw", names(AllDeath)) ]

AllDeath[,mean := rowMeans(.SD), .SDcols = draws]
out <- AllDeath[,sum(mean), by=list(location_id, year_id)]
out2 <- AllDeath[,sum(pop), by=list(location_id, year_id)]
out$pop <- out2$V1
out$mean <- out$V1/out$pop
out$sqrtmean <- sqrt(out$mean)

out$denge_prob <- 0 * out$mean
for (i in 1:length(out[,1])){
  tmploc <- which(FullData$location_id == out$location_id[i])[1]
  out$denge_prob[i] <- FullData$denge_prob[tmploc]
}



FullPRData <- out

PRData <- out[,c(6,7)]
names(PRData)[2] <- "meansqrtdeath"
prmod <-prcomp(cbind(FullData$denge_prob, scale(FullData$meansqrtdeath)))

FullPRData$PredScore <- predict(prmod, newdata = PRData)[,1]

AllPlaces <- get_demographics(gbd_team = "cov")
AllPop <- get_population(age_group_id = 22, location_id = CanHaz, year_id = 1990:2017, sex_id = 3)

AllLoc <- get_location_metadata(location_set_id = 8)
AllLoc <- AllLoc[,c( "location_set_version_id","location_set_id","location_id","parent_id","is_estimate","location_name","location_type","super_region_id","super_region_name","region_id",
                     "region_name","ihme_loc_id")]


AllOut <- merge(AllPop,AllLoc, by = "location_id", all = TRUE)

Keep <- which(AllOut$is_estimate == 1 | AllOut$location_type == "admin0")
AllOut <- AllOut[Keep,]
AllOut$countryIso <- substr(AllOut$ihme_loc_id,1,3)

AllOut <- AllOut[complete.cases(AllOut),]

require(mgcv)

mod <- lmer(meansqrtdeath ~ year_id + (1|region_name), data=CanHazDENV)
# mod <- gam(meansqrtdeath ~ s(year_id, k=4) + s(region_name, bs ="re"), data=CanHazDENV)


AllOut$PredTrend <- predict(mod, newdata = data.frame(year_id = AllOut$year_id, region_name = AllOut$region_name))

ADDRESS <- merge(AllOut, FullPRData, by = c("year_id","location_id"))

ADDRESS$logLinTrend <- log(ADDRESS$PredTrend)
ADDRESS$logscore <- log(ADDRESSa$PredScore+1)
ADDRESS$sampleM <- ADDRESS$population

mod2 <- lmer(logmeanM ~ logLinTrend * logscore  + (1|location_id), offset = log(sampleM), data = CanHazDENV)

ADDRESS$predout <- exp(predict(mod2, newdata=BigFakeData))

hm <- which(ADDRESS$location_id == 43916)

plot(ADDRESS$predout[hm], type='p')

write.csv(ADDRESS, sprintf("FILEPATH",prefix))


























