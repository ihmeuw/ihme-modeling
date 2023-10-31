rm(list = ls())
start <- Sys.time()


library("tidyverse")
library("haven")
library("data.table")
library("RODBC")
library("feather")


if (Sys.info()["sysname"]=="Windows") {
  h <- "H:"
  j <- "J:"
} else {
  h <- "~"
  j <- "/snfs1"

}


shared <- odbcConnect("shared")

### ESTABLISH RUNNING CONDITIONS ###
makeFile <- "yes"	# options are yes, no
tempVar <- "dailyTemp"	# options are dailyTemp, tempTmrelDeviation, dailyTempDeviation, dailyHI, dailyWBT
catVar <- "meanTempDegree" # options are loc, meanTempCat, rangeCat, meanTempDegree,meanHIDegree, meanWBTDegree
catVar2 <- "adm1"
ref <- "zoneMean" # default is 0 for deviations & 15_25 for dailyTemp; alternative is maxMort or zoneMean; added zoneMeanHI and zoneMeanWBT
version <- "withZaf" #rrShift" # name to give folder
#version <- "gbd2019" #rrShift" # name to give folder

causeList <- "all"
# causeList <- c("n_all_cause", "n_cirrhosis", "n_ckd", "n_cvd_htn", "n_cvd_ihd", "n_cvd_stroke", "n_diabetes", "n_inj_drowning", "n_inj_fires",
#                "n_inj_homicide", "n_inj_suicide", "n_lri", "n_mental_alcohol", "n_neonatal", "n_nutrition_pem", "n_resp_asthma", "n_resp_copd",
#                "n_sids", "n_tb", "n_uri")  #, "n_ntd_oncho")

#causeList <- c("n_all_cause",	"n_cvd_ihd", "n_inj_drowning")



if (tempVar=="dailyTempDeviation") {
  xtitle <- "Daily mean temperature - local mean (?C)"
} else if (tempVar=="tempTmrelDeviation") {
  xtitle <- "Daily mean temperature - TMREL (?C)"
} else if (tempVar=="dailyTemp") {
  xtitle <- "Daily mean temperature (?C)"
} else if (tempVar=="dailyHI") {
  xtitle <- "Daily heat index (?C)"
} else if (tempVar=="dailyWBT") {
  xtitle <- "Daily wet bulb temperature (?C)"
}



graphDir <- paste0(j, "/FILEPATH/", version)
dir.create(graphDir)
dir.create(paste0(graphDir, "/data"))
dir.create(paste0(graphDir, "/pdf"))
dir.create(paste0(graphDir, "/png"))


### Read in data that includes additional heat indices 
full <- as.data.table(read_feather("/FILEPATH/bra_chl_gtm_mex_nzl_col_usa_zaf_level3_redist_era5_heatindices.feather"))

#quantile(full$mean_wetbulb,probs=c(0.01,0.99),na.rm=TRUE)

subnatLink <- fread(paste0(j, "/FILEPATH/adm2_locid_link.csv"))
natLink <- data.table(location_id = c(72L, 98L, 125L, 128L), loc = c("nzl", "chl", "col", "gtm"))

full <- merge(full, subnatLink, by = c("loc", "zonecode"), all.x = T)

full <- merge(full, natLink, by = "loc", all.x = T)
full[is.na(location_id.x)==T, location_id.x := location_id.y]
full[, location_id.y := NULL]
setnames(full, "location_id.x", "location_id")

full[loc=="zaf", location_id := zonecode]
full <- full[loc!="zaf" | location_id==484, ]


# CREATE THE CATEGORY VARIABLE #
if (catVar=="meanTempCat") {

  full[, meanTempCat := cut(mean_temp, breaks = c(-Inf, seq(10, 25, 2.5), Inf),
                            labels = c("<10", paste(seq(10, 22.5, 2.5), seq(12.4, 24.9, 2.5), sep = "-"), "25+"),
                            right = FALSE)]


  collapseBy <- c(catVar, catVar2, "loc", "zonecode")

} else if (catVar=="meanTempDegree") {
  zoneLimits <- round(fread(paste0("/FILEPATH/zoneLimits.csv")))
  zoneLimits <- data.table(lower=round(6),upper=round(28))
  
  full[, meanTempDegree := as.integer(case_when(mean_temp<=zoneLimits$lower ~ zoneLimits$lower,
                                               mean_temp >= zoneLimits$upper ~ zoneLimits$upper,
                                               mean_temp > zoneLimits$lower & mean_temp < zoneLimits$upper ~ round(mean_temp)))]

  collapseBy <- c(catVar, catVar2, "loc", "zonecode")


} else if (catVar=="meanHIDegree") {
  zoneLimits <- round(fread(paste0("/FILEPATH/zoneLimits.csv")))
  zoneLimits <- data.table(lower=round(6),upper=round(28))
  
  full[, meanHIDegree := as.integer(case_when(mean_heat_index<=zoneLimits$lower ~ zoneLimits$lower,
                                                mean_heat_index >= zoneLimits$upper ~ zoneLimits$upper,
                                                mean_heat_index > zoneLimits$lower & mean_heat_index < zoneLimits$upper ~ round(mean_heat_index)))]
  
  collapseBy <- c(catVar, catVar2, "loc", "zonecode")
  
} else if (catVar=="meanWBTDegree") {
   zoneLimits <- data.table(lower=round(3),upper=round(24))
  
  full[, meanWBTDegree := as.integer(case_when(mean_wetbulb<=zoneLimits$lower ~ zoneLimits$lower,
                                              mean_wetbulb >= zoneLimits$upper ~ zoneLimits$upper,
                                              mean_wetbulb > zoneLimits$lower & mean_wetbulb < zoneLimits$upper ~ round(mean_wetbulb)))]
  
  collapseBy <- c(catVar, catVar2, "loc", "zonecode")
  
  
} else if (catVar=="loc") {

  full <- full %>% rename(locStr = loc)

  locMeanTemp <- full[, mean(temperature, na.rm = TRUE), by = locStr][order(V1), locStr]

  full[, loc := factor(locStr, levels = locMeanTemp)]

  collapseBy <- c(catVar2, "loc", "zonecode")


} else if (catVar=="rangeCat") {

  full[, c("p5", "p95") := .(quantile(temperature, 0.05), quantile(temperature, 0.95)), by = .(loc, zonecode)]
  full[, range95_5 := p95-p5]
  full[, rangeCat := case_when(range95_5<10 ~ floor(range95_5/5)*5, range95_5>=10 & range95_5<=30 ~ floor(range95_5/10)*10, range95_5>30 ~ 30)]

  collapseBy <- c(catVar, catVar2, "loc", "zonecode")
}



# CREATE THE TEMPERATURE VARIABLE #
tempVarCat <- paste0(tempVar, "Cat")

if (tempVar=="dailyTempDeviation") {

  full[, dailyTempDeviation := temp - mean_temp]
  full[, dailyTempDeviationCat := as.integer(case_when(dailyTempDeviation < -8 ~ -8, dailyTempDeviation > 8 ~ 8, dailyTempDeviation > -8 & dailyTempDeviation < 8 ~ round(dailyTempDeviation)))]


} else if (tempVar=="tempTmrelDeviation") {

  full[, tempTmrelDeviation := temp - tmrel]
  full[, tempTmrelDeviationCat := as.integer(case_when(tempTmrelDeviation < -8 ~ -8, tempTmrelDeviation > 8 ~ 8, tempTmrelDeviation > -8 & tempTmrelDeviation < 8 ~ round(tempTmrelDeviation)))]


} else if (tempVar=="dailyTemp") {

  full[,  c('p1', 'p99') := .(round(quantile(temp, 0.01, na.rm=TRUE)), round(quantile(temp, 0.99, na.rm=TRUE))), by = catVar]
  full[, dailyTempCat := as.integer(case_when(temp<=p1 ~ p1, temp >= p99 ~ p99, temp > p1 & temp < p99 ~ round(temp))), by = catVar]
  full[, c('p1', 'p99') := NULL]

} else if (tempVar=="dailyHI") {
  
  full[,  c('p1', 'p99') := .(round(quantile(heat_index, 0.01, na.rm=TRUE)), round(quantile(heat_index, 0.99, na.rm=TRUE))), by = catVar]
  full[, dailyHICat := as.integer(case_when(heat_index<=p1 ~ p1, heat_index >= p99 ~ p99, heat_index > p1 & heat_index < p99 ~ round(heat_index))), by = catVar]
  full[, c('p1', 'p99') := NULL]
  
} else if (tempVar=="dailyWBT") {
  
  full[,  c('p1', 'p99') := .(round(quantile(wetbulb, 0.01, na.rm=TRUE)), round(quantile(wetbulb, 0.99, na.rm=TRUE))), by = catVar]
  full[, dailyWBTCat := as.integer(case_when(wetbulb<=p1 ~ p1, wetbulb >= p99 ~ p99, wetbulb > p1 & wetbulb < p99 ~ round(wetbulb))), by = catVar]
  full[, c('p1', 'p99') := NULL]  
}



# create unique identifier for each admin 1 (need to do this as adm1_id_res is unique within, but not between values of loc)
setorder(full, loc, adm1_id_res)
full[, adm1 := .GRP, by = .(loc, adm1_id_res)]

locLink <- unique(copy(full)[, .(loc, adm1_id_res, adm1, zonecode, location_id)])

full[, collapseId := .GRP, by = collapseBy]

indexVars <- c(catVar, catVar2, tempVarCat)
full[, catId := .GRP, by = indexVars]


# CLEAN UP #
full[, "population" := round(population)]



### REDUCE DATASET SIZE (We need to eliminate unnecessary variables to reduce compuational overhead.
# this block of code creates a list of necessary variables and drops the rest)

# DETERMINE THE CAUSE LIST BASED ON OPTIONS SELECTED ABOVE AND VARIABLES IN THE DATASET #
if (causeList=="all") {
  causeList <- grep("^n_", names(full), value = TRUE)
}


# DETERMINE NUMBER OF DEATHS BY CAUSE AND
# REMOVE ALL CAUSES FOR WHICH THERE ARE NO DEATHS IN THE DATASET (these contribute no information and their removal greatly reduces computational overhead)
totals <- full[, sapply(.SD, function(x) {sum(x, na.rm = TRUE)}), .SDcols= causeList]
causeList <- setdiff(causeList, names(totals)[totals<10])

# DETERMINE WHICH VARIABLES WE NEED TO KEEP
touse <- c("temp", "loc", "location_id", "zonecode", "population", "collapseId", "catId", causeList, paste0(tempVar,"Cat"), catVar)

if (exists("catVar2")==TRUE) {
  touse <- c(touse, catVar2)
}

full <- full[, touse, with = FALSE]

#####################################

### CREATE RATE VARIABLES FROM DEATH COUNTS AND POPULATIONS ###
full[, sub("n_", "rate_", causeList) := lapply(.SD, function(x) { x/population }), .SDcols = causeList]

stubList <- sub("n_", "", causeList)

multiplier <- 0.1
offsets <- sapply(full[,paste0("rate_", stubList), with = FALSE], function(x) {median(x[x>0], na.rm = TRUE) * multiplier})
names(offsets) <- sub("rate_", "", names(offsets))


### COLLAPSE TO CALCUATE RATES BY LOCATION AND CATEGORY VARIABLE ###
setkey(full, collapseId)

full[, paste0("mean_", stubList) := lapply(.SD, function(x) {weighted.mean(x, w = population, na.rm = TRUE)}), by=collapseId, .SDcols=c(paste0("rate_", stubList))]


### CALCUATE RRs BY ADMIN2, RELATIVE TO ADMIN2 MEAN RATE ###
full[, paste0("rr_", stubList) :=lapply(1:length(stubList), function(i)
{(get(paste0("rate_", stubList[i])) + offsets[stubList[i]]) / (get(paste0("mean_", stubList[i])) + offsets[stubList[i]])})]


### COLLAPSE BY ADMIN 1 & TEMP ZONE ###
setkey(full, catId)

# Get population-weigted means of rates, RRs, and mean rates
# (note: this could be done with weighted.mean, but the two-step approach here is an order of magnitude faster)
full[, weight := population/sum(population, na.rm = TRUE), by=catId]
catMeans <- full[, lapply(.SD, function(x) {sum(x*weight, na.rm = TRUE)}), by=c("loc", "catId"), .SDcols=c(paste0("rate_", stubList), paste0("mean_", stubList), paste0("rr_", stubList))]

# Get totals of counts and population
catTotals  <- full[, lapply(.SD, function(x) {sum(x, na.rm = TRUE)}), by=c("loc", "catId"), .SDcols=c(causeList, "population")]

# Get the values of each category variable that correspond to each value of catId
catLabs <- full[, head(.SD, 1), by="catId", .SDcols=c(tempVarCat, catVar, catVar2, "location_id")]

# Merge the above components into a single data frame
catClean <- merge(catLabs,  catMeans,  by="catId", all = TRUE)
catClean <- merge(catClean, catTotals, by=c("catId", "loc"), all = TRUE)

bkup <- copy(catClean)

catClean <- copy(bkup)

### FIND REFERENCE TEMPERATURE FOR EACH LOCATION ###


if (grepl("_", ref)==TRUE) {
  refLimits <- as.numeric(strsplit(ref, split = "_")[[1]])
  catClean[, "ref" := dailyTempCat>=refLimits[1] & dailyTempCat<=refLimits[2]]
  #catClean[, "ref" := dailyHICat>=refLimits[1] & dailyHICat<=refLimits[2]]
  
  refLab <- paste0(refLimits[1], " to ", refLimits[2], "°C")

} else if (ref=="maxMort") {

  # find temperatures in which we have data for all locs by zone & the no. of deaths at each temp in each zone
  refTab <- catClean[, sum(!is.na(population)), by = c(tempVarCat, catVar)]
  refTab <- merge(refTab, catClean[, length(unique(adm1)), by = catVar], by = catVar, all = T)
  refTab <- merge(refTab, catClean[, sum(n_all_cause), by = c(catVar, tempVarCat)], by = c(catVar, tempVarCat), all = T)
  names(refTab)[3:5] <- c("n_adm1", "n_adm1_total", "n_deaths")

  # ensure that there is a common temperature (i.e. one in which all locs have deaths) in each zone
  refTab[, full_check := sum(n_adm1==n_adm1_total), by = catVar]
  if (min(refTab$full_check==0)) warning("----------- TEMP ZONES PRESENT WITH NO COMMON DAILY TEMP!!!! -------------")

  # find the reference temp as the temp with the largest number of deaths, among those temps that are common to all locs in a zone
  refTab[, ref := FALSE][n_adm1==n_adm1_total, ref := max(n_deaths)==n_deaths, by = catVar]
  refTab[, c("n_adm1", "n_adm1_total", "n_deaths", "full_check") := NULL]


  # merge the refs into the RR data
  catClean <- merge(catClean, refTab, by = c(catVar, tempVarCat), all = TRUE)

  setDF(refTab)

  refLab <- "Temperature with largest number of deaths in zone"

} else if (ref == "zoneMean") {
  catClean[, "ref" := dailyTempCat==meanTempDegree]
  refLab <- "Mean temperature in zone"

} else if (ref == "zoneMeanHI") {
  catClean[, "ref" := dailyHICat==meanHIDegree]
  refLab <- "Mean heatindex in zone"
  
} else if (ref == "zoneMeanWBT") {
  catClean[, "ref" := dailyWBTCat==meanWBTDegree]
  refLab <- "Mean wetbulbt temperature in zone"
  
  
} else {
  catClean[, "ref" := dailyTempCat==ref]
  refLab <- paste0(ref, "°C")
}




### DO RR CONVERSIONS & CALCULATE CIs ###


catClean[, refPop := sum(population*as.numeric(ref)), by = c(catVar, catVar2)]
catClean[, paste0("refN_", stubList) := lapply(.SD, function(x) {sum(x*as.numeric(ref))}), by = c(catVar, catVar2), .SDcols = paste0("n_", stubList)]


catClean[, paste0("se_", stubList) :=lapply(1:length(stubList), function(i)
{ sqrt(1/(get(paste0("n_", stubList[i])) + 0.5) - 1/(population + 0.5) + 1/(get(paste0("refN_", stubList[i])) + 0.5) + 1/(refPop + 0.5)) })]


# below is good code for older ref approach but may not be necessary for rolling approach
catClean[, paste0("rrR_", stubList) := lapply(.SD, function(x) {x / sum(x*as.numeric(ref))}), by = c(catVar, catVar2), .SDcols = paste0("rr_", stubList)]

catClean[, paste0("lnRr_", stubList) := lapply(.SD, log), .SDcols = paste0("rrR_", stubList)]


catClean[, paste0("lnRr_", stubList, "_lower") := lapply(1:length(stubList), function(i) {get(paste0("lnRr_", stubList[i])) - (qnorm(0.975) * get(paste0("se_", stubList[i])))})]
catClean[, paste0("lnRr_", stubList, "_upper") := lapply(1:length(stubList), function(i) {get(paste0("lnRr_", stubList[i])) + (qnorm(0.975) * get(paste0("se_", stubList[i])))})]

catClean[, paste0("iVar_", stubList) := lapply(.SD, function(x) {1/(x^2)}), .SDcols = paste0("se_", stubList)]

str(catClean)
nrow(catClean)

#write.csv(catClean, paste0(graphDir, "/data/fullRR_R_", catVar, "_", catVar2, "_", tempVar, "_", zoneLimits$lower, "-", zoneLimits$upper, ".csv"), row.names = FALSE)

write.csv(catClean, paste0("/FILEPATH/fullRR_R_", catVar, "_", catVar2, "_", tempVar, "_", zoneLimits$lower, "-", zoneLimits$upper, ".csv"), row.names = FALSE)

catClean<-fread(paste0("/FILEPATH/fullRR_R_", catVar, "_", catVar2, "_", tempVar, "_", zoneLimits$lower, "-", zoneLimits$upper, ".csv"))
dim(catClean)

setDF(catClean)


### On a cause-by-cause basis, drop locations where the number of deaths in the reference temperature is less than the offset
for (stub in stubList[1]) {
  catClean[eval(parse(text = paste0("catClean$refN_", stub))) < offsets[stub]*catClean$refPop, c(grep(paste0("^lnRr_", stub), names(catClean), value = T), paste0("se_", stub))] <- NA
print(stub)
}

length(stubList)


write.csv(catClean[, c(catVar, catVar2, tempVarCat, "loc", "location_id", grep("^lnRr_", names(catClean), value = T), grep("^se_", names(catClean), value = T))],
          paste0("/FILEPATH/", "/data/mrBrt_R_", catVar, "_", catVar2, "_", tempVar, "_ref", ref, "_", zoneLimits$lower, "-", zoneLimits$upper, ".csv"), row.names = FALSE)


str(catClean)
dim(catClean)

View(catClean)
end <- Sys.time()
difftime(end, start)
