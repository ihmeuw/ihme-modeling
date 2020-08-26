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
tempVar <- "dailyTemp"	   # options are dailyTemp, tempTmrelDeviation, dailyTempDeviation
catVar <- "meanTempDegree" # options are loc, meanTempCat, rangeCat, meanTempDegree
catVar2 <- "adm1"
ref <- "zoneMean"          # default is 0 for deviations & 15_25 for dailyTemp; alternative is maxMort or zoneMean
version <- "era5"          # name to give folder

causeList <- "all"


if (tempVar=="dailyTempDeviation") {
  xtitle <- "Daily mean temperature - local mean (°C)"
} else if (tempVar=="tempTmrelDeviation") {
  xtitle <- "Daily mean temperature - TMREL (°C)"
} else if (tempVar=="dailyTemp") {
  xtitle <- "Daily mean temperature (°C)"
}



graphDir <- paste0(FILEPATH)
dir.create(graphDir)
dir.create(paste0(graphDir, "/data"))
dir.create(paste0(graphDir, "/pdf"))
dir.create(paste0(graphDir, "/png"))


### LOAD DATA FILE AND CREATE TEMPERATURE AND CATEGORY VARIABLES ###
full <- fread(FILEPATH)
setDT(full)

subnatLink <- fread(FILEPATH)
natLink <- data.table(location_id = c(72L, 98L, 125L, 128L), loc = c("nzl", "chl", "col", "gtm"))

full <- merge(full, subnatLink, by = c("loc", "zonecode"), all.x = T)
full <- merge(full, natLink, by = "loc", all.x = T)
full[is.na(location_id.x)==T, location_id.x := location_id.y]
full[, location_id.y := NULL]
setnames(full, "location_id.x", "location_id")


# CREATE THE CATEGORY VARIABLE #
if (catVar=="meanTempCat") {

  full[, meanTempCat := cut(mean_temp, breaks = c(-Inf, seq(10, 25, 2.5), Inf),
                            labels = c("<10", paste(seq(10, 22.5, 2.5), seq(12.4, 24.9, 2.5), sep = "-"), "25+"),
                            right = FALSE)]


  collapseBy <- c(catVar, catVar2, "loc", "zonecode")

} else if (catVar=="meanTempDegree") {
  zoneLimits <- round(fread(FILEPATH))

  full[, meanTempDegree := as.integer(case_when(mean_temp<=zoneLimits$lower ~ zoneLimits$lower,
                                               mean_temp >= zoneLimits$upper ~ zoneLimits$upper,
                                               mean_temp > zoneLimits$lower & mean_temp < zoneLimits$upper ~ round(mean_temp)))]

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

  full[,  c('p1', 'p99') := .(round(quantile(temp, 0.01)), round(quantile(temp, 0.99))), by = catVar]
  full[, dailyTempCat := as.integer(case_when(temp<=p1 ~ p1, temp >= p99 ~ p99, temp > p1 & temp < p99 ~ round(temp))), by = catVar]
  full[, c('p1', 'p99') := NULL]

}



# create unique identifier for each admin 1 (need to do this as adm1_id_res is unique within, but not between values of loc)
setorder(full, loc, adm1_id_res)
full[, adm1 := .GRP, by = .(loc, adm1_id_res)]

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


} else {
  catClean[, "ref" := dailyTempCat==ref]
  refLab <- paste0(ref, "°C")
}




### DO RR CONVERSIONS & CALCULATE CIs ###
catClean[, refPop := sum(population*as.numeric(ref)), by = c(catVar, catVar2)]
catClean[, paste0("refN_", stubList) := lapply(.SD, function(x) {sum(x*as.numeric(ref))}), by = c(catVar, catVar2), .SDcols = paste0("n_", stubList)]


catClean[, paste0("se_", stubList) :=lapply(1:length(stubList), function(i)
{ sqrt(1/(get(paste0("n_", stubList[i])) + 0.5) - 1/(population + 0.5) + 1/(get(paste0("refN_", stubList[i])) + 0.5) + 1/(refPop + 0.5)) })]


catClean[, paste0("rrR_", stubList) := lapply(.SD, function(x) {x / sum(x*as.numeric(ref))}), by = c(catVar, catVar2), .SDcols = paste0("rr_", stubList)]

catClean[, paste0("lnRr_", stubList) := lapply(.SD, log), .SDcols = paste0("rrR_", stubList)]
catClean[, paste0("lnRr_", stubList, "_lower") := lapply(1:length(stubList), function(i) {get(paste0("lnRr_", stubList[i])) - (qnorm(0.975) * get(paste0("se_", stubList[i])))})]
catClean[, paste0("lnRr_", stubList, "_upper") := lapply(1:length(stubList), function(i) {get(paste0("lnRr_", stubList[i])) + (qnorm(0.975) * get(paste0("se_", stubList[i])))})]

catClean[, paste0("iVar_", stubList) := lapply(.SD, function(x) {1/(x^2)}), .SDcols = paste0("se_", stubList)]




### SAVE FULL VERSION OF DATASET ###
write.csv(catClean, FILEPATH, row.names = FALSE)





### PRODUCE AND SAVE REDUCED SUMMARY VERSION OF DATASET (ONLY SUMMARY VARIABLES) ###
setDF(catClean)

for (stub in stubList) {
  catClean[eval(parse(text = paste0("catClean$refN_", stub))) < offsets[stub]*catClean$refPop, c(grep(paste0("^lnRr_", stub), names(catClean), value = T), paste0("se_", stub))] <- NA
}



write.csv(catClean[, c(catVar, catVar2, tempVarCat, "loc", "location_id", grep("^lnRr_", names(catClean), value = T), grep("^se_", names(catClean), value = T))],
          FILEPATH, row.names = FALSE)


end <- Sys.time()
difftime(end, start)



