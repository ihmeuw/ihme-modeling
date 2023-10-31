# Perform data formatting and exclusions

bugs <- FALSE

DoExclusions <- function(data, add.data, order, births, ddm_run_id) {
  if (order == 1) {

    add.data <- add.data[add.data$source != "NATIONAL FERTILITY SURVEY (WFS),  OCTOBER 1974",]
  }
  else if (order == 3) {
    data <- subset(data,
                   !source %in% data$source[grep("(dhs)|(demographic and health)",
                                                 tolower(data$source))] | !iso3 %in% add.data$iso3 |
                     !in.direct == "indirect")
  }
  else if (order == 5) {
    data <- subset(data,
                   !(source %in% data$source[grep("(mics)|(multiple indicator cluster)",
                                                  tolower(data$source))] & iso3 %in% add.data$iso3 &
                       in.direct %in% data$in.direct[grep("(indirect)|(indirectstar)",
                                                          tolower(data$in.direct))]))
    add.data$NID[add.data$iso3 == "JAM" & add.data$source.date == 1999 & add.data$source == "MICS2"] <- 7140
    add.data$NID[add.data$iso3 == "NPL" & add.data$source.date == 2013 & add.data$source == "MICS5"] <- 162317
    add.data$NID[add.data$iso3 == "SSD" & add.data$source.date == 2010 & add.data$source == "MICS4"] <- 32189
    add.data$NID[add.data$iso3 == "SDN" & add.data$source.date == 2010 & add.data$source == "MICS4"] <- 153643
    add.data <- add.data[!(add.data$iso3 == "NGA" & add.data$source.date == 2011 & add.data$source == "MICS4")]
    
  }
  else if (order == 6) {
    data <- subset(data, !(source %in% data$source[grep("(world)|(wolrd)|(egyptian fertility survey)|(ghana fertility survey)|(jamaica fertility survey)|(jamica fertility survey)|(turkey fertility survey)|(WFS)", 
                                                        tolower(data$source))] & iso3 %in% add.data$iso3 & in.direct == "indirect"))
  }
  else if (order == 12) { 
    add.data$source[add.data$source.date < 2003] <- "TLS_subIDN_DHS"
  }
  else if (order == 22) {
    data$iso3.sourcedate <- paste(data$iso3, data$source.date, sep=" ")
    add.data$iso3.sourcedate <- paste(add.data$iso3, add.data$source.date, sep=" ")
    data <- subset(data, !(source %in% data$source[grep("(census)|(conteo)",
                                                        tolower(data$source))] & iso3.sourcedate %in% add.data$iso3.sourcedate &
                             in.direct %in% data$in.direct[grep("(indirect)|(indirect, mac only)",
                                                                tolower(data$in.direct))]))
  }
  else if (order == 34) {
    add.data <- add.data[add.data$iso3 != "PHL" & add.data$source.date != 1993,]
  }
  else if (order == 254) {
    data <- subset(data, !(source %in% data$source[grep("(world)|(wolrd)|(egyptian fertility survey)|(ghana fertility survey)|(jamaica fertility survey)|(jamica fertility survey)|(turkey fertility survey)", tolower(data$source))] & in.direct == "direct"))
  }
  else if (order == 267) {
    add.data <- add.data[add.data$iso3 != "PHL",]
  }
  else if (order == 299) {
    add.data<-subset(add.data,t>1995)
  }
  else if (order == 301) {
    add.data$compiling.entity = "new"
    add.data$data.age = "new"
    add.data$iso3 = add.data$country
    add.data$country[add.data$country=="BGD"]<-"Bangladesh"
    add.data$source = "SRS"
    add.data$source.date = add.data$source.year
    add.data$t = add.data$year
    add.data$in.direct = "NA"
    add.data <- add.data[add.data$t != 2003,]
  }
  else if (order %in% c(303, 311, 315, 316) == TRUE) {
    add.data$in.direct = "hh"
  }
  else if (order == 314) {
    add.data <- add.data[add.data$t != 2007,]
  }
  else if (order == 331) {
    names(add.data) <- gsub("_", ".", names(add.data), fixed=T)
    add.data$Page <- NULL
    if("Extracted.from" %in% colnames(add.data)) add.data$Extracted.from <- NULL
  }
  else if (order == 335) {
    add.data = subset(add.data,
                      !(add.data$source %in% add.data$source[grep("(1995 1% population survey)|(2005 1% population survey)|(calculated using 1982 census)",
                                                                  add.data$source)]))
    add.data$q5 = add.data$v5q0
    add.data$source.date = floor(add.data$year)
    add.data$in.direct = "NA"
    add.data$in.direct[add.data$source =="journal publication using Sample survey on fertility nad birth control in China"] <- "direct"
    add.data$compiling.entity = "new"
    add.data$data.age = "new"
    add.data$country = "China (without Hong Kong and Macao)"
    add.data$t = add.data$year + .25
  }
  else if (order == 356) {
    if (bugs==TRUE){
      data <- AppendData(data, add.data)
    }
  }
  else if (order == 435) { # VR
    print(paste0(ddm_run_id, " used for VR."))
    vr.dir <- paste0("FILEPATH")

    vr <- setDT(read_dta(paste0("FILEPATH"))) 
    pop <- setDT(read_dta(paste0("FILEPATH")))
    all.data <- UpdateVr(vr, pop, births, data)
    data <- all.data$data
    add.data <- all.data$add.data
  }
  else if (order == 436) {
    add.data[, country := NULL]
    setnames(add.data, "year", "t")
    add.data[, ihme.loc.id := iso3]
    lots_of_vr <<- AppendData(data[data$source == "VR",], add.data) 
    add.data <- lots_of_vr[duplicated(lots_of_vr[,c("ihme.loc.id","t")]) == FALSE
                           & lots_of_vr$source == "VR - TransMONEE",]
  }
  else if (order == 437) { # BGD SRS Report
    add.data$ihme.loc.id <- add.data$iso3
    names <- c("iso3","ihme.loc.id","t","q5","source","source.date","in.direct",
               "compiling.entity","data.age","sd.q5","log10.sd.q5", "underlying_NID")
    add.data <- FillMissingCols(names, add.data)
    lots_of_vr <<- FillMissingCols(names, lots_of_vr)
    lots_of_srs <<- rbind(data[data$source == "SRS",], add.data)
    add.data <- lots_of_srs[duplicated(lots_of_srs[, .SD, .SDcols = c("ihme.loc.id","t")]) == FALSE & lots_of_srs$source == "SRS vital registration, BGD Bureau of Statistics",]
  }
  else if (order == 438) { # Nuaru 2002
    add.data$q5 = add.data$v5q0
    add.data$source.date = floor(add.data$year)
    add.data$in.direct = "NA"
    add.data$compiling.entity = "new"
    add.data$data.age = "new"
    add.data$country = "Nauru"
    add.data$t = add.data$year + .25
    add.data$ihme.loc.id <- add.data$iso3
  }
  else if (order == 439){
    matches <- grep("_", unique(add.data$iso3))
    if (length(matches) < length(unique(add.data$iso3))){
      for (loc in unique(add.data$iso3)){
        if (is.na(pmatch("_", loc))){
          add.data$ihme.loc.id[add.data$iso3 == eval(loc)] <- paste0(substr(loc, 1, 3), "_", substr(loc, 4, 100))
          add.data$iso3[add.data$iso3 == eval(loc)] <- substr(loc, 1, 3)
        }
      } 
    }
  }
  else if (order == 440){
    if ("NID" %in% names(add.data) == FALSE){
      add.data$NID <- NA
    }
    add.data$NID[add.data$source.date == 2000 & is.na(add.data$NID)] <- 19571
    add.data$NID[add.data$source.date == 2005 & is.na(add.data$NID)] <- 19557
    add.data$NID[add.data$source.date == 2011 & is.na(add.data$NID)] <- 21301
  }
  return(list("data" = data, "add.data" = add.data))
}

UpdateVr <- function(vr, pop, births, data) {

  cat("\nUpdating VR...\n")
  vr <- vr[!(grepl("IND_", ihme_loc_id) &
               deaths_source %in% c("SRS", "SRS_REPORT")), ]
  vr <- vr[!(grepl("IND", ihme_loc_id) &
               grepl("srs", deaths_source, ignore.case = T) &
               (year >= 2008 & year <= 2019)), ]
  vr <- vr[deaths_nid != 449948]
  vr <- vr[!(ihme_loc_id=="GBR_4749" & year < 1980),]
  pop <- pop[!(grepl("USA_", ihme_loc_id) & pop_source=="USA_CENSUS"),]

  births <- births[sex == "both", .SD, .SDcols = c("ihme_loc_id", "year", "births")]
  ind.srs.births.file <- paste0("FILEPATH")
  births.srs <- fread(ind.srs.births.file)
  names(births.srs) <- c("year", "births")
  births.srs <- births.srs[year > 1992,]
  
  data[iso3 == "CHN", iso3 := "CHN_44533"]
  data[iso3 == "CHN_44533", ihme_loc_id := "CHN_44533"]
  vr[ihme_loc_id == "CHN_44533", country := "China (without Hong Kong and Macao)"]

  # take only both sexes (not males and females) and extract only the necessary age variables for 5q0
  datum_cols_to_add <- c("DATUM0to4", "DATUM1to1", "DATUM2to2", "DATUM3to3", "DATUM4to4")
  datum_cols_to_add <- datum_cols_to_add[!datum_cols_to_add %in% colnames(vr)]
  if (length(datum_cols_to_add) > 0) vr[, (datum_cols_to_add) := NA]

  vr <- vr[sex == "both" & year>=1950, .SD, 
           .SDcols = c("ihme_loc_id","country","year","deaths_source","source_type", "deaths_nid", "deaths_underlying_nid",
             paste("DATUM",seq(0,4),"to",seq(0,4),sep=""),"DATUM1to4","DATUM0to4")]

  vr$orig_source_type <- vr$source_type
  vr$source_type[grepl("VR", vr$source_type)] <- "VR"
  vr$source_type[grepl("SRS", vr$source_type)] <- "SRS"
  vr$source_type[grepl("DSP", vr$source_type)] <- "DSP"
  pop <- pop[sex == "both" & ihme_loc_id != "", .SD, 
             .SDcols = c("ihme_loc_id","year","source_type","c1_0to0","c1_1to4")]
  
  vr <- subset(vr, !(deaths_source=="SRS_LIBRARY" & ihme_loc_id=="BGD"))
  
  # Use finer age groups to get coarser age groups (e.g. 1to4, 0to4)
  vr$DATUM1to4[is.na(vr$DATUM1to4)] <- apply(vr[is.na(DATUM1to4),
                                                .SD,
                                                .SDcols = paste("DATUM",seq(1,4),"to",seq(1,4),sep="")],1,sum)
  vr$DATUM0to4[is.na(vr$DATUM0to4)] <- apply(vr[is.na(vr$DATUM0to4),
                                                .(DATUM0to0, DATUM1to4)],1,sum)
  # merge deaths and population
  vr$id <- paste(vr$ihme_loc_id, vr$source_type, vr$year, sep="_")
  pop$id <- paste(pop$ihme_loc_id, pop$source_type, pop$year, sep="_")
  
  vrpop1 <- merge(vr, pop, by=c("id", "ihme_loc_id", "source_type", "year"))
  vrpop2 <- merge(vr[!vr$id %in% vrpop1$id,
                     .SD,
                     .SDcols = c("country", "ihme_loc_id","year","source_type","deaths_source",
                                 "DATUM0to0","DATUM1to4","DATUM0to4", 
                                 "orig_source_type", "deaths_nid", "deaths_underlying_nid")],
                  pop[pop$source_type == "IHME", .SD, .SDcols = c("ihme_loc_id","year","c1_0to0","c1_1to4")],
                  by = c("ihme_loc_id", "year"), all.x=T)
  vrpop <- rbindlist(list(vrpop1[, .SD, .SDcols = colnames(vrpop2)], vrpop2), use.names = T)
  vrpop <- vrpop[!duplicated(vrpop), ]
  
  # merge in births as well
  vrpop1 <- merge(vrpop[!(vrpop$ihme_loc_id == "IND" &
                            vrpop$source_type == "SRS" & vrpop$year>1992),], births,
                  by=c("ihme_loc_id","year"), all.x=TRUE)
  vrpop2 <- merge(vrpop[(vrpop$ihme_loc_id == "IND" &
                           vrpop$source_type == "SRS" & vrpop$year>1992),], births.srs,
                  by=c("year"), all.x=TRUE)
  vrpop <- rbindlist(list(vrpop1, vrpop2), use.names = T)
  vrpop$source_type[vrpop$ihme_loc_id == "TUR"] <-
    vrpop$orig_source_type[vrpop$ihme_loc_id == "TUR"]
  vrpop$source_type[vrpop$ihme_loc_id == "ZAF"] <-
    vrpop$orig_source_type[vrpop$ihme_loc_id == "ZAF"]
  
  vrpop[, orig_source_type := NULL]

  # insert Thailand births from who
  THA_births <- fread(paste0("FILEPATH"))
  vrpop[year <=2014 & year >=2007 & ihme_loc_id=="THA", births := as.double(THA_births$births)]
  # calculate 5q0
  vr.q5 <- c()
  for(i in 1:nrow(vrpop)) {
    # Extract the ith row of data
    vrI <- c(as.numeric(vrpop[i, .SD, .SDcols = c("DATUM0to0","DATUM1to4")]), NA)
    names(vrI) <- c(0,1,5)
    popI <- c(as.numeric(vrpop[i, .SD, .SDcols = c("c1_0to0","c1_1to4")]), NA)
    names(popI) <- c(0,1,5)
    mortalityRate <- vrI/popI
    
    # First try to use the m -> q, if we can't then use deaths/births
    if(sum(is.na(mortalityRate[c("0","1")])) == 0 & !(vrpop[i, ihme_loc_id] == "THA" & vrpop[i, year] >= 2007 )) {
      qs <- ConvertMortalityRateToProbability(mortalityRate,sex="both")
      q5I <- 1-((1-qs["0"])*(1-qs["1"]))
      vr.q5 <- c(vr.q5, q5I)
    } else {
      # deaths/births
      q5I <- vrpop[i, DATUM0to4] / vrpop[i, births]
      vr.q5 <- c(vr.q5, q5I)
    }
  }
  
  vr.q5 <- as.vector(vr.q5)
  # Conform the new vr 5q0s to the rest of the data
  # Standardize columns for vrpop
  names(vrpop)[names(vrpop) == "deaths_nid"] <- "NID"
  names(vrpop)[names(vrpop) == "deaths_underlying_nid"] <- "underlying_NID"
  add.data <- vrpop
  add.data <- cbind(add.data, vr.q5)
  add.data <- add.data[, .SD, .SDcols = c("ihme_loc_id","year","source_type","vr.q5", "NID", "underlying_NID", "deaths_source")]
  add.data[, source.date := year]
  setnames(add.data, c("ihme_loc_id", "year", "source_type", "vr.q5"), c("ihme.loc.id", "t", "source", "q5"))

  add.data[, (c("data.age", "compiling.entity")) := "new"]
  add.data[, in.direct := NA]

  if(is.null(add.data$NID)) add.data <- cbind(add.data, NID=NA)
  
  add.data[, q5 := q5 * 1000]
  add.data <- unique(add.data)
  add.data[, filename := "FILEPATH"]
  add.data[, source.type := "VR"]

  return(list("data" = data, "add.data" = add.data))
}


FinalDeduplicate <- function(data, locations) {
  data[iso3 == "CHN_44533" & ihme.loc.id == "CHN", ihme.loc.id := "CHN_44533"]
  
  data <- cbind(data, adjust=0)
  local <- locations[, .(ihme_loc_id)]
  local[, iso3 := ihme_loc_id]
  setnames(local, "ihme_loc_id", "ihme.loc.id")
  
  data <- data[!(iso3 %in% c("XIR","XIU")),]
  data[iso3 == "ROM", iso3 := "ROU"]
  
  missings <- data[is.na(ihme.loc.id)]
  missings[, ihme.loc.id := NULL]
  mapped <- data[!is.na(ihme.loc.id)]
  missings <- merge(missings, local, by="iso3", all.x=T)
  missings[is.na(ihme.loc.id), ihme.loc.id := iso3]

  # put the mapped and unmapped together
  data <- rbindlist(list(missings, mapped), use.names = T)
  data[, q5 := as.numeric(q5)]
  
  # Standardize China names
  data <- StandardizeChinaNames(data)
  
  # Deduplicate birth histories & more exclusions
  data <- BirthHistDedupe(data)
  return(data)
}

BirthHistDedupe <- function(data) {
  # Deduplicate birth histories
  
  data[, t := as.numeric(t)]
  data[source == "UNDYB census", source := "undyb census"]
  
  # Clean up data
  data = subset(data, !is.na(t) & !is.na(q5))
  
  # Get rid of duplicates from merge
  data <- unique(data)
  
  data <- data.frame(data)
  data <- data[!duplicated(data),]
  
  return(data)
}

StandardizeChinaNames <- function(data) {
  ##  -------------------------------------------------------------------------
  ##  Standardized China Source Names
  ##  Creating Over-Arching Source Names for clarity in the China 5q0 data
  ##  -------------------------------------------------------------------------
  #  VR - two sources from COD
  data$source[data$source == "WHO" & data$source.date <= 2000 &
                data$ihme.loc.id == "CHN_44533"] <- "VR_MOH_09C"
  data$source[data$source == "MOH_10C" & data$source.date >= 2002 &
                data$ihme.loc.id == "CHN_44533"] <- "VR_MOH_10C"
  
  # 1% Intra-Census Survey
  data$source[data$source == "DC" &
                (data$source.date == 1995 | data$source.date == 2005) &
                data$ihme.loc.id == "CHN_44533"] <- "1% Intra-Census Survey"
  data$source[data$source == "one percent survey 1995"] <- "1% Intra-Census Survey"
  data$in.direct[data$ihme.loc.id == "CHN_44533" &
                   data$source == "1% Intra-Census Survey" & data$in.direct != "indirect"] <- "hh"
  
  # 1 per 1000 Sample Survey on Population Change
  data$source[data$source == "SSPC" & data$ihme.loc.id == "CHN_44533"] <-
    "1 per 1000 Survey on Pop Change"
  data$source[data$source == "china 1 0/00 population sample survey, 1 july 1987"] <-
    "1 per 1000 Survey on Pop Change"
  data$source[data$source == "0/00 sample survey 1990-98"] <-
    "1 per 1000 Survey on Pop Change"
  # all estimates should be marked as hh is not indirects
  data$in.direct[data$source == "1 per 1000 Survey on Pop Change"
                 & data$in.direct == "direct" & data$data.age == "old"] <- "hh"
  data$in.direct[is.na(data$in.direct) &
                   data$source == "1 per 1000 Survey on Pop Change"] <- "hh"
  
  # need to delete duplicates
  data <- data[!(data$source == "1 per 1000 Survey on Pop Change"
                 & data$source.date == "1994" & data$data.age == "old"),]
  data <- data[!(data$source == "1 per 1000 Survey on Pop Change"
                 & data$source.date == "1996" & data$data.age == "old"),]
  data <- data[!(data$source == "1 per 1000 Survey on Pop Change"
                 & data$source.date == "1997" & data$data.age == "old"),]
  data <- data[!(data$source == "1 per 1000 Survey on Pop Change"
                 & data$source.date == "1998" & data$data.age == "old"),]
  
  # Census
  # remove census duplicates
  data <- data[!(data$source == "calculated using 1990 census"),]
  # need to delete duplicate
  data <- data[!(data$source == "census, 1 july 1990"),]
  data$in.direct[is.na(data$in.direct) &
                   data$ihme.loc.id == "CHN_44533" & data$source == "DYB"] <- "hh"
  data$source[data$ihme.loc.id == "CHN_44533" & data$source == "DYB"] <- "census"
  
  # National Maternal and Child Health Surveillance System
  data$source[data$source == "survey on deaths of children under the age of five in china, the national collaborative group for survey of deaths under five"] <- "Maternal and Child Health Surveillance System"
  data$source[data$source == "child and maternal surveillance system 1991-2004"] <-
    "Maternal and Child Health Surveillance System"
  data <- data[!(data$source == "Maternal and Child Health Surveillance System"
                 & data$source.date == "1991" & data$data.age == "old"),]
  data <- data[!(data$source == "Maternal and Child Health Surveillance System"
                 & data$source.date %in% c(1996:2013) & data$data.age == "old"),]
  data$in.direct[data$source == "Maternal and Child Health Surveillance System"
                 & data$in.direct != "indirect"] <- NA
  data$source.date[data$source == "Maternal and Child Health Surveillance System" &
                     data$source.date == "2009"] <-
    data$t[data$source == "Maternal and Child Health Surveillance System" &
             data$source.date == "2009"]
  
  # National Sample Survey on Fertility and Birth Control
  data$source[data$source ==
                "journal publication using Sample survey on fertility nad birth control in China"] <-
    "1 per 1000 Survey on Fertility and Birth Control"
  data$source[data$source ==
                "female fertility in china: a 1 0/00 population survey, 31 june 1982"] <-
    "1 per 1000 Survey on Fertility and Birth Control"
  data$source[data$source == "fertility sampling survey 1992"] <-
    "1 per 1000 Survey on Fertility and Birth Control"
  
  data$in.direct[data$source == "national survey on fertility and birth control 1988"] <-
    "hh"
  data$source[data$source == "national survey on fertility and birth control 1988"] <-
    "1 per 1000 Survey on Fertility and Birth Control"
  data$in.direct[data$source == "1 per 1000 Survey on Fertility and Birth Control" &
                   data$in.direct == "direct"] <- "hh"
  data$t[data$ihme.loc.id == "CHN_44533" &
           data$source == "1 per 1000 Survey on Fertility and Birth Control" &
           data$source.date == "1992"] <- 1991
  
  return(data)
}

shock <- function(data) {
  ## Mortality shocks  
  shock <- rep(F,length(data$t))
  
  shock[data$ihme.loc.id=="ARG" & floor(data$t) == 1957  & data$outlier == 0] <- T
  shock[data$ihme.loc.id=="ARM" & floor(data$t) == 1988 & data$outlier == 0 & data$source == "VR"] <- T
  shock[data$ihme.loc.id=="BGD" & floor(data$t) == 1974 & data$outlier == 0] <- T
  shock[data$ihme.loc.id=="BGD" & floor(data$t) == 1971 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "COG" & (floor(data$t) == 1997 | floor(data$t) == 1998 | floor(data$t) == 1999) & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "JOR" & floor(data$t)== 1967 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "KHM" & floor(data$t)>=1975 & floor(data$t)<=1980 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "LBR" & floor(data$t) == 1990 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "LKA" & floor(data$t) == 1996 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "PAK" & floor(data$t) == 2005 & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "PRT" & (floor(data$t) == 1975 | floor(data$t) == 1976) & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "RWA" & (floor(data$t) == 1994) & data$outlier == 0] <- T
  shock[data$ihme.loc.id == "TJK" & floor(data$t) == 1993 & data$outlier == 0 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "PNG" & floor(data$t) == 1991] <- T
  shock[data$ihme.loc.id == "SLB" & floor(data$t) == 1999] <- T
  shock[data$ihme.loc.id == "KOR" & data$t >= 1950 & data$t < 1954] <- T
  shock[data$ihme.loc.id == "JPN_35426" & floor(data$t) == 2011 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "JPN_35427" & floor(data$t) == 2011 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "JPN_35451" & floor(data$t) == 1995 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "MNE" & floor(data$t) %in% seq(1997,1999)] <- T
  
  
  isShock <- rep(0,length(data$t))
  isShock[shock] <- 1
  
  data <- cbind(data,isShock)
  names(data)[names(data) == "isShock"] <- "shock"
  data$outlier[data$ihme.loc.id == "CHN_44533" & data$source == "1% Intra-Census Survey" & data$source.date == "1995" & data$t == "1992.6"] <- 0
  
  return(data)
}

CleanData <- function(data, locs) {
  
  data = data[order(data$source, data$t),names(data)!="adjust"]
  data = subset(data, !is.na(ihme.loc.id) & !is.na(t) & !is.na(q5))
  
  
  names(data)[names(data) == "t"] <- "year"
  data$q5 <- data$q5/1000
  
  
  # only keep IHME standard countries
  locs <- locs[,c("ihme_loc_id","location_name")]
  names(locs) <- c("ihme.loc.id","location_name")
  data <- merge(data,locs,by="ihme.loc.id")
  data <- data[,c("ihme.loc.id","location_name","year","q5","source",
                  "source.date","in.direct","compiling.entity","data.age",
                  "sd.q5","log10.sd.q5", "NID", "underlying_NID", "filename", "microdata", "shock", "outlier")]
  
  data$in.direct[is.na(data$in.direct)] <- "NA"
  data$year[data$year == floor(data$year) &
              ((data$in.direct != "direct" & data$in.direct != "indirect") |
                 data$data.age != "new")] <-
    data$year[data$year == floor(data$year) & ((data$in.direct != "direct" &
                                                  data$in.direct != "indirect") | data$data.age != "new")] + .5
  data <- data[order(data$ihme.loc.id, data$source, data$source.date,
                     data$in.direct, data$year),]
  
  # keep only data after 1950
  data <- data[data$year >= 1950,]
  
  names(data)[names(data) == "ihme.loc.id"] <- "ihme_loc_id"
  
  return(data)
}

scrub <- function(data) {
  scrub <- rep(F, length(data$t))
  scrub[data$t < 1400] <- T
  scrub.list <- fread(paste0("FILEPATH",na.strings=c(""," ","NA")))
  
  for(i in 1:nrow(scrub.list)){
    line <- scrub.list[i]
    line_na <- line[, !apply(is.na(line), 2, all)]
    line <- line[,line_na, with=FALSE]
    command_start <- "scrub["
    
    if(colnames(line)[1] == "source"){
      str1 <- "tolower(data[['"
      str2 <- "']]) =='"
    } else {
      str1 <- "data[['"
      str2 <- "']] == '"
    }
    
    if (length(colnames(line)) > 1) {
      command <- paste0(command_start, str1,  colnames(line)[1], str2 ,line[[colnames(line)[1]]], "'", " & ")
      for(j in 2:length(colnames(line))){
        if(colnames(line)[j] == "source"){
          str1 <- "tolower(data[['"
          str2 <- "']]) =='"
        } else {
          str1 <- "data[['"
          str2 <- "']] == '"
        }
        if(j == length(colnames(line))){
          command <- paste0(command, str1, colnames(line)[j], str2,line[[colnames(line)[j]]], "'", "] <- T")
        } else {
          command <- paste0(command, str1, colnames(line)[j], str2,line[[colnames(line)[j]]], "'", " & ")
        }
      }
    } else {
      command <- paste0(command_start, str1,  colnames(line)[1], str2 ,line[[colnames(line)[1]]], "'", "] <- T")
    }
    eval(parse(text=command))
  }
  data <- data[!scrub,]
  
  return(data)
}

dirty_scrub <- function(data) {
  scrub <- rep(F, length(data$t)) 
  scrub[data$t < 1400] <- T
  
  scrub[data$country == "Republic of Korea" & tolower(data$source) == "national fertility survey (wfs), october 1974"] <- T
  scrub [data$country == "Niger" & tolower(data$source) == "demographic and health survey 2006 - preliminary"] <- T
  scrub [data$country == "Rwanda" & tolower(data$source) == "preliminary dhs" & data$sourcedate == 2005] <- T
  scrub[data$ihme.loc.id == "ALB" & tolower(data$source) == "dhs preliminary report" & data$source.date=="2008" & data$data.age == "new" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "BEN" & tolower(data$source) == "census february 1992" & data$source.date=="1992" & data$data.age == "new"] <- T  
  scrub[data$ihme.loc.id == "BWA" & tolower(data$source) == "family health survey" & data$source.date=="1988" & data$data.age == "old"] <- T  
  scrub[data$ihme.loc.id == "DZA" & tolower(data$source) == "enquete nationale sur les objectifs" & data$source.date=="1992" & data$data.age == "old"] <- T  
  scrub[data$ihme.loc.id == "TLS" & tolower(data$source) == "census" & data$source.date=="2004" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "RWA" & tolower(data$source) == "rwanda 1978 census" & data$source.date=="1978" & data$data.age == "new"] <- T
  scrub[data$ihme.loc.id == "RWA" & tolower(data$source) == "census 1991" & data$source.date=="1991" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "MAR" & tolower(data$source) == "enquete demographique, 9 may-11 november 1966" & data$source.date=="1966" & data$data.age == "old"] <- T
  scrub[data$country == "Bangladesh" & tolower(data$source) == "srs" & data$t == 2002.5 & data$q5 < 60] <- T
  scrub[data$country == "Haiti" & tolower(data$source) == "enquete haitienne sur la prevalence de la contraception, 1983"] <- T
  scrub[data$country == "Indonesia" & tolower(data$source) == "census, 24 september 1971"] <- T
  scrub[data$country == "Iraq" & tolower(data$source) == "child mortality"] <- T
  scrub[data$country == "Iraq" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Malawi" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Malaysia" & tolower(data$source) == "world fertility survey, august-december 1974"] <- T
  scrub[data$country == "Mongolia" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Namibia" & tolower(data$source) == "census"] <- T
  scrub[data$country == "Nepal" & tolower(data$source) == "contraceptive prevalence survey"] <- T
  scrub[data$country == "Niger" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Ghana" & tolower(data$source) == "multiple indicator cluster survey" & data$source.date == "1984"] <- T
  scrub[data$country == "Gambia" & tolower(data$source) == "census" & data$source.date == "1973"] <- T
  scrub[data$country == "Sri Lanka" & tolower(data$source) == "census, 9 october 1971"] <- T
  scrub[data$country == "Sierra Leone" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Sudan" & tolower(data$source) == "census" & data$source.date == "1983"] <- T
  scrub[data$country == "Sudan" & tolower(data$source) == "census" & data$source.date == "1993"] <- T
  scrub[data$country == "Syrian Arab Republic" & tolower(data$source) == "census, 23 september 1970"] <- T
  scrub[data$country == "Syrian Arab Republic" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Tajikistan" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Thailand" & tolower(data$source) == "census"] <- T
  scrub[data$country == "Timor Leste" & tolower(data$source) == "census"] <- T
  scrub[data$country == "Turkey" & tolower(data$source) == "census" & (data$source.date == "1970" | data$source.date == "1980" | data$source.date== "1985") ] <- T
  scrub[data$country == "Turkmenistan" & tolower(data$source) == "tmd"] <- T
  scrub[data$country == "Uganda" & tolower(data$source) == "undyb census" & (data$source.date == "1991")] <- T
  scrub[data$country == "United Arab Emirates" & tolower(data$source) == "census, 31 december 1975"] <- T
  scrub[data$country == "United Republic of Tanzania" & tolower(data$source) == "census" & data$source.date == "1988"] <- T
  scrub[data$country == "Uruguay" & tolower(data$source) == "census 1985"] <- T
  scrub[data$country == "Uzbekistan" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  scrub[data$country == "Venezuela" & tolower(data$source) == "undyb census" & data$source.date == "1990"] <- T
  scrub[data$country == "Viet Nam" & tolower(data$source) == "undyb census" & data$source.date == "1999"] <- T
  scrub[data$country == "Zambia" & tolower(data$source) == "census" & data$source.date == "1980"] <- T
  scrub[data$ihme.loc.id == "IRN" & tolower(data$source) == "dhs rural"] <-T
  scrub[data$ihme.loc.id == "IRN" & tolower(data$source) == "dhs urban"] <-T
  scrub[data$ihme.loc.id == "AFG" & tolower(data$source) == "census 1979 sample"] <- T
  scrub[data$ihme.loc.id == "SWE" & tolower(data$source) == "undyb census"] <- T
  scrub[data$ihme.loc.id == "MLI" & tolower(data$source) == "census" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "HTI" & tolower(data$source) == "demographic and health survey 2005-2006 - preliminary" & data$source.date == "2005"] <-T     
  scrub[data$ihme.loc.id == "HND" & tolower(data$source) == "encuesta nacional de epidemiolog�a y salud familiar 2001" & data$source.date == "2001"] <-T  
  scrub[data$ihme.loc.id == "IND" & tolower(data$source) == "national family health survey"] <- T                                                      
  scrub[data$ihme.loc.id == "IDN" & tolower(data$source) == "annual national socio-economic survey"] <- T                                        
  scrub[data$ihme.loc.id == "KEN" & tolower(data$source) == "census" & data$source.date == "1989" & data$data.age == "old"] <-T                    
  scrub[data$ihme.loc.id == "LSO" & tolower(data$source) == "census, 12 april 1986" & data$data.age == "old"] <- T                                     
  scrub[data$ihme.loc.id == "LSO" & tolower(data$source) == "demographic and health survey 2004-2005" & data$data.age == "old"] <- T                      
  scrub[tolower(data$source) == "tmd" & data$data.age == "old"] <- T                                                                         
  scrub[data$ihme.loc.id == "MLI" & tolower(data$source) == "census 1987" & data$data.age == "old"] <- T                                                 
  scrub[data$ihme.loc.id == "MLI" & tolower(data$source) == "census, 1-6 december 1976" & data$data.age == "old"] <- T                                  
  scrub[data$ihme.loc.id == "MEX" & tolower(data$source) == "encuesta nacional sobre fecundidad y salud" & data$source.date == "1987" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "MEX" & tolower(data$source) == "encuesta nacional de fecundidad" & data$source.date == "1977" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "MOZ" & tolower(data$source) == "census" & data$source.date == "1997" & data$data.age == "old"] <- T                          
  scrub[data$ihme.loc.id == "IRQ" & tolower(data$source) == "child and maternal mortality survey 1999 (weighted average between the 2)" & data$source.date == "1999" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "ZAF" & tolower(data$source) == "living standard measurement study 1993" & data$source.date == "1993" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "ZAF" & tolower(data$source) == "demographic and health survey 2003 - preliminary" & data$source.date == "2003" & data$data.age == "old"] <- T 
  scrub[data$ihme.loc.id == "LKA" & tolower(data$source) == "registrar" & data$data.age == "old"] <- T                                                
  scrub[data$ihme.loc.id == "LKA" & tolower(data$source) == "demographic and health survey, january-march 1987" & data$data.age == "old"] <- T            
  scrub[data$ihme.loc.id == "TJK" & tolower(data$source) == "living standard measurement study 1999"] <- T                                            
  scrub[data$ihme.loc.id == "TLS" & tolower(data$source) == "ifhs"] <- T                                                                            
  scrub[data$ihme.loc.id == "ZWE" & tolower(data$source) == "demographic and health survey 2005-2006 - preliminary"] <- T                              
  scrub[data$ihme.loc.id == "NGA" & tolower(data$source) == "preliminary dhs" & data$source.date=="2003"] <- T                                   
  scrub[data$ihme.loc.id == "NGA" & tolower(data$source) == "dhs preliminary report" & data$source.date=="2008"] <- T                       
  scrub[data$ihme.loc.id == "BEN" & tolower(data$source) == "census february 1992" & data$source.date == "1992"] <- T                        
  scrub[data$ihme.loc.id == "DZA" & tolower(data$source) == "enquete fecondite" & data$source.date=="1970" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "DZA" & tolower(data$source) == "multiple indicator cluster survey 1995" & data$source.date=="1995" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "BDI" & tolower(data$source) == "census" & data$source.date=="1983" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "GNQ" & tolower(data$source) == "census 1983" & data$source.date=="1983" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "GMB" & tolower(data$source) == "contraceptive prevalence and fertility determinants survey 1990" & data$source.date=="1990" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "IDN" & tolower(data$source) == "census 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "NRU" & tolower(data$source) == "census 1992" & data$source.date=="1992" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "NER" & tolower(data$source) == "census 1988" & data$source.date=="1988" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "NRU" & tolower(data$source) == "census 1992" & data$source.date=="1992" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "MDA" & tolower(data$source) == "multiple indicator cluster survey 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "WSM" & tolower(data$source) == "demographic and vital statistics survey 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "SEN" & tolower(data$source) == "multiple indicator cluster survey 1996" & data$source.date=="1996" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "SEN" & tolower(data$source) == "multiple indicator cluster survey 2000" & data$source.date=="2000" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "SDN" & tolower(data$source) == "safe motherhood survey" & data$source.date=="1999" & data$data.age == "old" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "PNG" & tolower(data$source) == "undyb census" & data$year=="1965.627"]<-T
  scrub[data$ihme.loc.id == "IRN" & tolower(data$source) == "dhs (mohsen)"] <- T
  scrub[data$data.age == "old" & tolower(data$source) %in% c("multiple indicator cluster survey","mics","dhs","DHS","demographic health survey") & data$ihme.loc.id !="AFG" & data$ihme.loc.id != "TKM"] <- T
  scrub[data$ihme.loc.id == "CIV" & tolower(data$source) == "census 1998"] <- T
  scrub[data$ihme.loc.id == "EGY" & tolower(data$source) == "egypt dhs" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "EGY" & tolower(data$source) == "preliminary dhs" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "ECU" & tolower(data$source) == "encuesta demografica y de salud familiar" & data$data.age == "old"] <- T 
  scrub[data$ihme.loc.id == "MDV" & tolower(data$source) == "pv" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "MDV" & tolower(data$source) == "ministry of health vital statistics - vital registration" & data$data.age == "old" & data$t!="1995" & data$t!="1997" & data$t!="1998" & data$t!="1999"] <- T # these also appear to be duplicates of VR. Leaving in the years we don't have VR for
  scrub[data$ihme.loc.id == "CHN_44533" & tolower(data$source) =="calculated using 1990 census" & data$data.age== "new"] <- T
  scrub[data$ihme.loc.id == "CHN_44533" & tolower(data$source) =="census, 1 july 1990" & data$data.age== "old"] <- T
  scrub[data$ihme.loc.id == "SLE" & tolower(data$source) =="dhs preliminary report" & data$source.date=="2008"] <- T
  scrub[data$ihme.loc.id == "MDA" & tolower(data$source) == "preliminary dhs" & data$source.date=="2005"] <- T
  scrub[data$ihme.loc.id == "SDN" & data$source == "pan arab for child development survey"] <- T
  scrub[data$ihme.loc.id == "MRT" & data$source == "mchs"] <- T
  scrub[data$ihme.loc.id == "EGY" & data$source == "eps"] <- T
  scrub[data$ihme.loc.id == "YEM" & data$source == "pap"] <- T
  scrub[data$ihme.loc.id == "DZA" & data$source == "enquete algerienne sur la sante de la mere/enfant"] <- T
  scrub[data$ihme.loc.id == "SYR" & data$source == "maternal and child health survey 1993"] <- T
  scrub[data$ihme.loc.id == "TUN" & data$source == "pap"] <- T
  scrub[data$ihme.loc.id == "LBY" & data$source == "maternal and child health survey 1995"] <- T
  scrub[data$ihme.loc.id == "LBN" & data$source == "maternal and child health survey 1996"] <- T
  scrub[data$ihme.loc.id == "MAR" & data$source == "pap"] <- T
  scrub[data$ihme.loc.id == "CHN_44533" & data$source=="undyb census"  & data$in.direct=="indirect, MAC only"] <- T
  scrub[data$ihme.loc.id == "CHN_44533" & data$source=="child and maternal surveillance system 1991-2004" & (data$source.date=="2000" | data$source.date=="2001" | data$source.date=="2002" | data$source.date=="2003" | data$source.date=="2004")] <- T
  scrub[data$ihme.loc.id == "YEM" & grepl("PAPCHILD", data$source)] <- T 
  scrub[data$ihme.loc.id == "IDN" & data$source=="undyb census" & data$in.direct=="indirect, MAC only" & (data$source.date==1980 | data$source.date==1990)] <- T 
  scrub[data$ihme.loc.id == "CIV" & data$source == "demographic and health survey 2005 - preliminary" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "ARM" & data$source == "demographic and health survey 2005 - preliminary" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "MWI" & data$source == "demographic and health survey 2004" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "RWA" & data$source == "preliminary dhs" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "BRA" & data$source == "pesquisa nacional por amostra de domicilios, october-december 1973"] <- T
  scrub[data$ihme.loc.id == "BRA" & data$source == "pesquisa nacional por amostra de domicilios, october-december 1972"] <- T
  scrub[data$ihme.loc.id == "BRA" & data$source == "pesquisa nacional por amostra de domicilios, 28 november 1976"] <- T
  scrub[(data$ihme.loc.id == "JAM" & data$source == "contraceptive prevalence survey" & data$source.date == "1989")] <- T
  scrub[data$ihme.loc.id == "KHM" & data$source == "census 2008" & data$in.direct == "hh" & data$source.date == "2008"] <- T 
  scrub[data$ihme.loc.id == "MOZ" & data$source == "CDC RHS 2001" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "MOZ" & grepl("CDC-RHS", data$source) & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "TON" & data$source == "SURVEY"] <- T
  scrub[data$source == "GLOBAL FUND 2008" & data$in.direct == "direct" & data$ihme.loc.id %in% c("BFA", "ETH", "ZMB")] <- T
  scrub[data$source == "Ethiopia 2007 Census" & data$source.date == "2007" & data$in.direct == "hh"] <- T
  scrub[data$ihme.loc.id == "ARE" & data$source == "gulf family health survey 1995" & data$data.age=="old"] <- T
  scrub[data$ihme.loc.id %in% c("BHR","QAT","SAU") & data$source == "gfh" & data$data.age=="old"] <- T
  scrub[data$ihme.loc.id == "OMN" & data$source == "Oman Family Health Survey (1995)" & data$data.age=="new"] <- T
  scrub[data$ihme.loc.id == "IND" & data$source == "SRS vital registration, Bhat 1998"] <- T  
  scrub[data$ihme.loc.id == "IND" & data$source == "India SRS" & floor(data$t) %in% c(1992, 1993, 1994, 1995)] <- T 
  scrub[data$ihme.loc.id == "IND" & data$source == "SRS" & data$source.date == 2007 & data$in.direct == "na"] <- T
  scrub[data$ihme.loc.id == "IND" & data$source == "SRS" & data$t == 1996.5] <- T
  scrub[data$ihme.loc.id == "MWI" & data$source == "CENSUS" & floor(data$t) == 1998 & round(data$q5) == 222] <- T
  scrub[data$ihme.loc.id == "NIC" & data$source.date == "1985" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "SAU" & data$source.date == "2006" & data$source == "Population Bulletin 2007"] <- T
  scrub[data$source == "INED via Tabutin (1991)"] <- T 
  scrub[data$source == "INED"] <- T 
  scrub[data$ihme.loc.id == "KHM" & data$source == "DHS Report" & substr(data$source.date, 1, 4)=="2010" & data$in.direct == "direct"] <- T 
  scrub[data$ihme.loc.id == "MWI" & data$source == "DHS Report" & substr(data$source.date, 1, 4)=="2010" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "BOL" & data$source == "DHS preliminary report" & data$source.date == "2008" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id %in% c("PRY", "SLV", "UKR") & data$source == "cdc rhs statcompiler"] <- T 
  scrub[data$ihme.loc.id == "PHL" & data$source == "DHS preliminary report" & data$source.date == "2008" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "TLS" & data$source == "DHS final report" & data$source.date == "2003" & data$in.direct == "direct"] <- T
  scrub[data$ihme.loc.id == "PAK" & data$source == "living standards survey" & data$source.date == "1991" & data$in.direct == "indirect"] <- T
  scrub[data$ihme.loc.id == "MNG" & data$source == "STATYB"] <- T
  scrub[data$ihme.loc.id == "LKA" & data$source == "DHS Summary of Findings Report" & data$source.date == "1993"] <- T
  scrub[data$ihme.loc.id == "CHN_44533" & data$source %in% c("VR_MOH_09C", "VR_MOH_10C")] <- T 
  scrub[data$ihme.loc.id == "CHL" & data$source != "VR"] <- T
  scrub[data$ihme.loc.id == "COD" & data$source == "average of east and west from coghlan"] <- T 
  scrub[data$ihme.loc.id == "VUT" & data$source == "reproductive, maternal and child health"] <- T 
  scrub[data$ihme.loc.id == "SLV" & data$source == "ehs" & data$data.age == "old"] <- T 
  scrub[data$ihme.loc.id == "BGD" & data$source == "population growth estimation experiment 1962-1965" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "DZA" & data$source == "enquete demographique" & data$data.age == "old"] <- T
  scrub[data$ihme.loc.id == "PSE" & data$source == "palestine bureau of statistics survey" & data$source.date == "1995-1999"] <- T
  scrub[data$ihme.loc.id == "SLV" & data$source == "fsl"] <- T
  scrub[data$ihme.loc.id == "ETH" & data$source == "ethiopia demographic survey"] <- T 
  scrub[data$ihme.loc.id == "LSO" & data$source == "rural household consumption and expenditure survey, march 1968-may 1969"] <- T 
  scrub[data$ihme.loc.id == "LBR" & data$source == "undyb census" & data$source.date == "1971"] <- T
  scrub[data$ihme.loc.id == "LBR" & data$source == "SURVEY" & data$source.date == "1970"] <- T
  scrub[data$ihme.loc.id == "MAR" & grepl("PAPFAM", data$source)] <- T
  scrub[data$ihme.loc.id == "QAT" & data$source == "child health survey"] <- T
  scrub[data$ihme.loc.id == "PNG" & data$source == "DHS Preliminary report, via Alan" & data$source.date == 2006] <- T
  scrub[data$ihme.loc.id == "CHN_44533" & data$source == "1 per 1000 Survey on Pop Change" & data$data.age=="old"] <- T
  scrub[data$source == "DSS"] <- T
  scrub[data$ihme.loc.id == "TUR" & data$data.age == "old" & data$source == "census 2000"] <- T
  scrub[data$ihme.loc.id == "OMN" & data$source == "VR" & data$t <= 2004] <- T
  scrub[data$ihme.loc.id == "ZAF" & data$source == "Community Survey, IPUMS"] <- T
  scrub[data$ihme.loc.id == "ZAF" & data$source == "SURVEY" & data$source.date == "2007"] <- T
  scrub[data$ihme.loc.id == "YEM" & data$source == "Yemen Annual Statistical Report" & data$source.date == "2007" & data$t == 2006.5] <- T 
  scrub[data$ihme.loc.id == "YEM" & data$source == "dhs statcompiler" & data$source.date == "1997"] <- T   
  scrub[data$ihme.loc.id == "JOR" & data$source == "epi/cdd and child mortality survey 1988" & data$source.date == 1988 & data$in.direct == "indirect" & data$data.age == "old"] <- T   
  scrub[data$source == "DSP" & data$ihme.loc.id != "CHN_44533" & data$t < 1996] <- T
  scrub[data$source == "ZAF_OCT_HH_SURVEY_1993-95_1997-98"] <- T
  scrub[grepl("ZAF_OCT_HH_SURVEY", data$source)] <- T
  scrub[data$ihme.loc.id == "BOL" & data$source == "ENCUESTA DEMOGRAFICA NACIONAL 1975"] <- T
  scrub[data$source == "China MCHS" & data$filename!= "EST_CHN_PROV_MCHS_2013_2017_v5q0.dta"] <- T
  scrub[data$ihme.loc.id=="CHN_44533" & data$source=="VR" & data$t == 2015] <- T
  
  data <- data[!scrub,]
  
  scre <- read.csv(paste0("FILEPATH"))
  scre$ihme.loc.id <- scre$iso3
  scre$microdata <- NA
  scre$underlying_NID <- NA
  scre$filename <- NA
  scre$NID <- NA
  scre <- scre[,names(scre) != "scrub.reason"]
  
  scre <- as.data.table(scre)
  scre[,microdata:= ifelse(grepl("IPUMS", source), 1, NA)]
  scre[,microdata:= ifelse(grepl("VR", source) | grepl("SURVEY", source), 1, microdata)]
  scre[,microdata:= ifelse(is.na(microdata), 0, microdata)]
  scre <- as.data.frame(scre)
  numbertobeaddedback <- nrow(scre)
  scre <- scre[!(scre$ihme.loc.id=="ZAF" & scre$source == "SURVEY" & scre$source.date == 2007),]
  nrow(scre) == numbertobeaddedback - 1
  scre$country <- NULL
  data <- rbind(data,scre)
  return(data)
}

kill <- function(data) {
  old_locs <- fread(
    paste0(get_path("5q0_process_inputs"),"FILEPATH"),
    stringsAsFactors = F)
  old_locs$local_id_2013[is.na(old_locs$local_id_2013)] <-
    old_locs$ihme_loc_id[is.na(old_locs$local_id_2013)]
  old_locs <- old_locs[, c("ihme_loc_id", "local_id_2013")]
  names(old_locs) <- c("ihme.loc.id_replacement","ihme.loc.id")
  old_locs <- unique(old_locs)
  old_locs[, ihme.loc.id := ifelse(ihme.loc.id == "", ihme.loc.id_replacement, ihme.loc.id)]
  
  data <- data.table(data)
  new_data <- data[!(ihme.loc.id %in% unique(old_locs$ihme.loc.id))]
  old_data <- data[(ihme.loc.id %in% unique(old_locs$ihme.loc.id))]
  old_data <- merge(old_data, old_locs,by="ihme.loc.id", all.x=TRUE)
  old_data$ihme.loc.id <- NULL
  names(old_data)[names(old_data) == 'ihme.loc.id_replacement'] <- 'ihme.loc.id'
  data <- data.frame(rbind(new_data, old_data))
  #########################################################################################
  # Identify outliers. These will be plotted, but grayed out and not included in the analysis
  
  kill0 <- c()
  for (i in 1:length(unique(data$ihme.loc.id))) {
    indir <- (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect" & !data$data.age == "new") | (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect, MAC only")
    
    indir[is.na(indir)] <- FALSE
    if( sum(indir) != 0 ) {
      i.survs <- unique(paste(data$source[indir],data$source.date[indir]))
      for (j in 1:length(i.survs)) {
        indir2 <- paste(data$source,data$source.date)==i.survs[j] & data$ihme.loc.id==unique(data$ihme.loc.id)[i]
        mark = unique(which(indir &indir2))                                                                                      
        if (length(mark)>0) point1 <- mark[data$t[mark]==max(data$t[mark])]               #Find the last two 
        if (length(mark)>1) point2 <- mark[data$t[mark]==max(data$t[mark[mark!=point1]])]  #observations
        
        in.direct.nomissing <- data$in.direct[point2]
        if(is.na(in.direct.nomissing)) in.direct.nomissing <- ""
        
        if (data$compiling.entity[point2]=="u" | data$source[point2] == "undyb census" | in.direct.nomissing == "indirect, MAC only") kill0 <- c(kill0,point1)                                         #Only drop most recent if source is UNICEF or source is undyb census
        else kill0 <- c(kill0,point1,point2)                                                                 #concatenate 
      }
    }
  }
  kill <- rep(F,length(data$q5))                                                        
  kill[kill0] <- T  
  
  kill[data$ihme.loc.id == "MLI" & tolower(data$source) == "census"] <- T
  kill[data$ihme.loc.id == "MDG" & tolower(data$source) == "enquete demographique, 9 may-11 november 1966"] <- T
  kill[data$ihme.loc.id == "MDG" & tolower(data$source) == "madagascar demographic survey"] <- T
  kill[data$country == "Ghana" & tolower(data$source) == "census, ipums"] <- T
  kill[data$country == "Ghana" & tolower(data$source) == "dhs" & data$source.date == "2007" & data$in.direct == "indirect"] <- T
  kill[data$country == "Ghana" & tolower(data$source) == "multiple indicator cluster survey"] <- T
  kill[data$country == "Chad" & tolower(data$source) == "survey" & data$source.date == "1963"] <- T 
  kill[data$country == "Greece" & tolower(data$source) == "census, ipums"] <- T
  kill[data$country == "Guinea-Bissau" & tolower(data$source) == "vr"] <- T
  kill[data$country == "Indonesia" & tolower(data$source) == "undyb census" & data$source.date == "1990"] <- T
  kill[data$country == "Iraq" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Iraq" & tolower(data$source) == "undyb census" & data$source.date == "1987"])] <- T
  kill[data$country == "Kazakhstan" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Kazakhstan" & tolower(data$source) == "undyb census" & data$source.date == "1989"])] <- T
  kill[data$country == "Kazakhstan" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Kazakhstan" & tolower(data$source) == "undyb census" & data$source.date == "1999"])] <- T
  kill[data$country == "Papua New Guinea" & tolower(data$source) == "undyb census" & data$t == min(data$t[data$country == "Papua New Guinea" & tolower(data$source) == "undyb census"])] <- T
  kill[data$country == "Philippines" & tolower(data$source) == "undyb census" & data$source.date == "1990"] <- T
  kill[data$country == "Poland" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Portugal" & tolower(data$source) == "wfs"] <- T
  kill[data$country == "Romania" & tolower(data$source) == "undyb census" & data$source.date == 1966] <- T
  kill[data$ihme.loc.id == "RUS" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "RUS" & tolower(data$source) == "undyb census"])] <- T
  kill[data$country == "Latvia" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Latvia" & tolower(data$source) == "undyb census"])] <- T
  kill[data$ihme.loc.id == "LBY" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "LBY" & tolower(data$source) == "undyb census"])] <- T
  kill[data$ihme.loc.id == "LBY" & tolower(data$source) == "maternal and child health survey 1995"] <- T
  kill[data$country == "Kuwait" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Kuwait" & tolower(data$source) == "undyb census" & data$source.date == "1975"])] <- T
  kill[data$country == "Kuwait" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Kuwait" & tolower(data$source) == "undyb census" & data$source.date == "1980"])] <- T
  kill[data$country == "Solomon Islands" & tolower(data$source) == "census 1999" & data$t == max(data$t[data$country == "Solomon Islands" & tolower(data$source) == "census 1999"])] <- T
  kill[data$country == "Tonga" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Tonga" & tolower(data$source) == "undyb census"])] <- T
  kill[data$country == "Tonga" & tolower(data$source) == "vr" & data$t == "1957.5"] <- T
  kill[data$country == "French Guiana" & tolower(data$source) == "vr" & data$t == "1979.5"] <- T
  kill[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$source.date == "1966"])] <- T
  kill[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$source.date == "1971"])] <- T
  kill[data$country == "Afghanistan" & (tolower(data$source) == "multiple indicator cluster survey" | tolower(data$source) == "census 1979 sample")] <- T
  kill[data$country == "Algeria" & (tolower(data$source) == "vr" | tolower(data$source) == "survey") & data$t < 1970] <- T
  kill[data$country == "Algeria" & tolower(data$source) == "census 1998" & data$t == max(data$t[data$country=="Algeria" & tolower(data$source)=="census 1998"])] <- T
  kill[data$country == "Angola" & tolower(data$source) == "vr"] <- T
  kill[data$country == "Argentina" & tolower(data$source) == "census, ipums"] <- T
  kill[data$country == "Azerbaijan" & tolower(tolower(data$source)) == "mics2" & data$source.date == 2000] <- T
  kill[data$country == "Bahamas" & tolower(data$source) == "vr" & data$source.date == 2002.5] <- T
  kill[data$country == "Bangladesh" & tolower(data$source) == "retrospective survey of fertility and mortality, 1974" & data$t == max(data$t[data$country=="Bangladesh" & tolower(data$source)=="retrospective survey of fertility and mortality, 1974"])] <- T
  kill[data$country == "Bangladesh" & (tolower(data$source) == "vr") & data$t < 1983] <- T
  kill[data$country == "Belarus" & tolower(data$source) == "census, ipums" & data$source.date == 1999] <- T
  kill[data$country == "Belarus" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country=="Belarus" & tolower(data$source)=="undyb census" & data$source.date == 1989])] <- T
  kill[data$country == "Belgium" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Bhutan" & tolower(data$source) == "census"] <- T
  kill[data$country == "Bolivia" & tolower(data$source) == "encuesta demografica nacional  1980" & data$t == max(data$t[data$country=="Bolivia" & tolower(data$source)=="encuesta demografica nacional  1980" & data$source.date == 1980])] <- T
  kill[data$country == "Bolivia" & tolower(data$source) == "undyb census" & data$source.date == 1992] <- T
  kill[data$country == "Brazil" & tolower(data$source) == "undyb census" & data$source.date == 1950 & data$t == max(data$t[data$country=="Brazil" & tolower(data$source)=="undyb census" & data$source.date == 1950])] <- T
  kill[data$country == "Bulgaria" & tolower(data$source) == "undyb census" & data$source.date == 1975] <- T
  kill[data$country == "Burkina Faso" & tolower(data$source) == "undyb census" & data$source.date == 1985] <- T
  kill[data$country == "Burundi" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country=="Burundi" & tolower(data$source)=="undyb census" & data$source.date == 1990])] <- T
  kill[data$country == "Chile" & tolower(data$source) == "census, ipums" & (data$source.date == 1982 | data$source.date == 1992 | data$source.date==2002)] <- T
  kill[data$country == "Colombia" & tolower(data$source) == "vr" & (data$t == 1979.5 | data$source.date == 1980.5)] <- T
  kill[data$country == "Czech Rep." & tolower(data$source) == "undyb census" & data$t > 1986] <- T  
  kill[data$country == "Dominican Republic" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country=="Dominican Republic" & tolower(data$source)=="undyb census" & data$source.date == 1970])] <- T
  kill[data$ihme.loc.id == "EGY" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "EGY" & tolower(data$source)=="undyb census" & data$source.date == 1976])] <- T
  kill[data$country == "Estonia" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country=="Estonia" & tolower(data$source)=="undyb census" & data$source.date == 1989])] <- T
  kill[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$source.date == "1966"])] <- T
  kill[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census" & data$source.date == "1971"])] <- T
  kill[data$country == "Sudan" & tolower(data$source) == "census" & data$t == max(data$t[data$country == "Sudan" & tolower(data$source) == "census" & data$source.date==1973])] <- T
  kill[data$country == "Sweden" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Switzerland" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Syrian Arab Republic" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Syrian Arab Republic" & tolower(data$source) == "undyb census" & data$source.date == "1994"])] <- T
  kill[data$country == "Syrian Arab Republic" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Syrian Arab Republic" & tolower(data$source) == "undyb census" & data$source.date == "1970"])] <- T
  kill[data$country == "Tajikistan" & tolower(data$source) == "lsms" & data$source.date == "1999" & data$t == max(data$t[data$country == "Tajikistan" & tolower(data$source) == "lsms" & data$source.date == "1999"])] <- T
  kill[data$country == "Tajikistan" & tolower(data$source) == "demographic survey 2002" & data$t == max(data$t[data$country == "Tajikistan" & tolower(data$source) == "demographic survey 2002"])] <- T
  kill[data$country == "Togo" & tolower(data$source) == "enquete demographique, march-april 1971" & data$t == max(data$t[data$country == "Togo" & tolower(data$source) == "enquete demographique, march-april 1971"])] <- T
  kill[data$country == "Tonga" & tolower(data$source) == "undyb census" & data$source.date == "1966"] <- T
  kill[data$country == "Tunisia" & tolower(data$source) == "census, 8 may 1975" & data$t == max(data$t[data$country == "Tunisia" & tolower(data$source) == "census, 8 may 1975"])] <- T
  kill[data$country == "Ukraine" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Ukraine" & tolower(data$source) == "undyb census" & data$source.date == "1989"])] <- T
  kill[data$country == "Ukraine" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Ukraine" & tolower(data$source) == "undyb census" & data$source.date== "2001"])] <- T
  kill[data$country == "Vanuatu" & tolower(data$source) == "census 1989" & data$t == max(data$t[data$country == "Vanuatu" & tolower(data$source) == "census 1989"])] <- T
  kill[data$country == "Zambia" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Zambia" & tolower(data$source) == "undyb census" & data$source.date == "1990"])] <- T
  kill[data$country == "Zimbabwe" & tolower(data$source) == "vr"] <- T
  kill[data$country == "Zimbabwe" & tolower(data$source) == "census, 16 august 1982" & data$t == max(data$t[data$country == "Zimbabwe" & tolower(data$source) == "census, 16 august 1982"])] <- T
  kill[data$country == "Zimbabwe" & tolower(data$source) == "census, 20 march 1969" & data$t == max(data$t[data$country == "Zimbabwe" & tolower(data$source) == "census, 20 march 1969"])] <- T
  kill[data$country == "Czech Republic" & tolower(data$source) == "undyb census" & round(data$t,1) == 1989.6] <- T
  kill[data$country == "Czech Republic" & tolower(data$source) == "undyb census" & round(data$t,1) == 1987.9] <- T
  kill[data$country == "Uruguay" & tolower(data$source) == "census, ipums"] <- T
  kill[data$country == "Uganda" & tolower(data$source) == "census, ipums" & data$source.date == 1991] <- T
  kill[data$country == "South Africa" & tolower(data$source) == "census, ipums" & data$source.date == "2001"] <- T
  kill[data$country == "Rwanda" & data$source.date > "1995" &  data$in.direct == "indirect" ] <- T
  kill[data$country == "Romania" & tolower(data$source) == "undyb census" & data$source.date == "1992"] <- T
  kill[data$country == "Philippines" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Paraguay" & tolower(data$source) == "undyb census" & data$source.date == "1982"] <- T
  kill[data$country == "Papua New Guinea" & tolower(data$source) == "vr"] <- T
  kill[data$country == "Panama" & tolower(data$source) == "census, ipums" & data$source.date == "1960"] <- T
  kill[data$country == "Nepal" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Mali" & tolower(data$source) == "census 1987"] <- T
  kill[data$country == "Mali" & tolower(data$source) == "census" & round(data$t,1) == 1987.5] <- T
  kill[data$country == "Malawi" & tolower(data$source) == "undyb census" & data$source.date == "1987"] <- T
  kill[data$country == "Madagascar" & tolower(data$source) == "vr"] <- T
  kill[data$ihme.loc.id == "PRK" & data$t == 1993] <- T
  kill[data$country == "Indonesia" & tolower(data$source) == "survey"] <- T
  kill[data$country == "India" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Hong Kong, China" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Haiti" & tolower(data$source) == "enquete demographique a passages repetes, august 1971-november 1973"] <- T
  kill[data$country == "Haiti" & tolower(data$source) == "census, 31 august 1971"] <- T
  kill[data$country == "Haiti" & (tolower(data$source) == "vr" | tolower(data$source) == "survey")] <- T
  kill[data$country == "Ethiopia" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Czech Republic" & tolower(data$source) == "undyb census"] <- T
  kill[data$country == "Cuba" & tolower(data$source) == "encuesta nacoinal de fecundidad, november-december 1987"] <- T
  kill[data$country == "Comoros" & tolower(data$source) == "mics2"] <- T
  kill[data$country == "Georgia" & tolower(data$source) == "vr" & round(data$t,1) == 2003.5] <- T
  kill[data$country == "Mozambique" & tolower(data$source) == "census" & round(data$t,1) == 1997.5] <- T
  kill[data$country == "Nepal" & tolower(data$source) == "census" & round(data$t,1) == 2001.5] <- T
  kill[data$country == "Zimbabwe" & tolower(data$source) == "census" & round(data$t,1) == 2002.5] <- T
  kill[data$country == "Ghana" & tolower(data$source) == "vr"] <- T
  kill[data$country == "India" & (tolower(data$source) == "vr") ] <- T
  kill[data$ihme.loc.id == "IRN" & (tolower(data$source) == "vr") & round(data$t,1) < 1989] <- T
  kill[data$country == "Iraq" & tolower(data$source) == "vr" & round(data$t,1) < 1980] <- T
  kill[data$country == "Kenya" & tolower(data$source) == "vr"] <- T
  kill[data$ihme.loc.id == "VNM" & tolower(data$source) == "2007 population change and family planning survey" & data$t == max(data$t[data$ihme.loc.id=="VNM" & tolower(data$source)=="2007 population change and family planning survey" & data$source.date == "2007"])] <- T
  kill[data$ihme.loc.id == "MOZ" & tolower(data$source) == "census" & data$source.date == "1970"] <- T
  kill[data$ihme.loc.id == "MHL" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id=="MHL" & tolower(data$source)=="undyb census" & data$source.date == "1999"])] <- T
  kill[data$ihme.loc.id == "BHS" & tolower(data$source) == "vr" & data$t == "2002.5"] <- T
  kill[data$ihme.loc.id == "ARE" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id=="ARE" & tolower(data$source)=="undyb census" & data$source.date == "1975"])] <- T
  kill[data$ihme.loc.id == "BRB" & tolower(data$source) == "vr" & floor(data$t) == 2003] <- T
  kill[data$ihme.loc.id == "LBR" & tolower(data$source) == "survey"] <- T
  kill[data$ihme.loc.id == "BDI" & tolower(data$source) == "survey"] <- T
  kill[data$ihme.loc.id == "MAR" & tolower(data$source) == "survey"] <- T
  kill[data$ihme.loc.id == "TCD" & tolower(data$source) == "dhs" & floor(data$t) == 1969] <- T
  kill[data$ihme.loc.id == "TCD" & tolower(data$source) == "dhs" & floor(data$t) == 1970] <- T
  kill[data$ihme.loc.id == "NRU" & data$source.date == "2007" & data$t == "1995"] <- T
  kill[data$ihme.loc.id == "MOZ" & tolower(data$source) == "survey"] <- T
  kill[data$ihme.loc.id == "TZA" & tolower(data$source) == "survey"] <- T
  kill[data$ihme.loc.id == "NPL" & tolower(data$source) == "survey"] <- T
  kill[data$ihme.loc.id == "MMR" & tolower(data$source) == "vr" & data$t < 1980] <- T
  kill[data$ihme.loc.id == "NAM" & tolower(data$source) == "census plus imr from statistics bureau"] <- T
  kill[data$ihme.loc.id == "IRN" & data$t == 1999 & tolower(data$source) == "iran dhs 2000"] <- T
  kill[data$ihme.loc.id == "BRA" & tolower(data$source) == "lsms"] <- T
  kill[data$ihme.loc.id == "GNQ" & tolower(data$source) == "vr"] <- T
  kill[data$ihme.loc.id == "LBY" & tolower(data$source) == "maternal and child health survey 1995" & data$data.age == "old"] <-T
  kill[data$ihme.loc.id == "BOL" & data$t < 1980 & data$q5 > 400] <- T
  kill[data$ihme.loc.id == "MOZ" & tolower(data$source) == "vr" & data$q5 < 50] <- T
  kill[data$ihme.loc.id == "TGO" & tolower(data$source) == "vr" & data$q5 < 30] <- T
  kill[data$ihme.loc.id == "COL" & tolower(data$source) == "vr" & data$t == 1979] <- T
  kill[data$ihme.loc.id == "CYP" & tolower(data$source) =="vr" & data$source.date == "1974" ] <- T
  kill[data$ihme.loc.id == "CYP" & tolower(data$source) =="vr" & data$source.date == "1975" ] <- T
  kill[data$ihme.loc.id == "CYP" & tolower(data$source) =="vr" & data$source.date == "1976" ] <- T
  kill[data$ihme.loc.id == "BGD" & tolower(data$source) =="retrospective survey of fertility and mortality, 1974" & data$source.date == "1974" ] <- T
  kill[data$ihme.loc.id == "CAF" & data$source =="CENSUS" & data$source.date=="1988"] <-T
  kill[data$ihme.loc.id == "COM" & tolower(data$source) =="census" & data$source.date=="1958"] <-T
  kill[data$ihme.loc.id == "ERI" & tolower(data$source) == "dhs" & data$source.date == "1995" & data$in.direct=="direct"] <-T
  kill[data$ihme.loc.id == "ERI" & tolower(data$source) == "dhs" & data$source.date == "2002" & data$in.direct=="direct"] <-T
  kill[data$ihme.loc.id == "DJI" & tolower(data$source) == "papfam" & data$in.direct=="direct"] <-T
  kill[data$ihme.loc.id == "MDG" & tolower(data$source) == "census" & data$source.date == "1993"] <-T 
  kill[data$ihme.loc.id == "JOR" & tolower(data$source) == "vr" & (data$source.date == "1967" | data$source.date == "1968" | data$source.date == "1969" | data$source.date == "1970" | data$source.date == "1971" | data$source.date == "1972" | data$source.date == "1973" | data$source.date == "1974" | data$source.date == "1975" | data$source.date == "1976" | data$source.date == "1977" | data$source.date == "1978" | data$source.date == "1979" | data$source.date == "1980")] <- T 
  kill[data$ihme.loc.id == "ZAF" & tolower(data$source) == "vr" & data$t <= 1990] <-T 
  kill[data$ihme.loc.id == "LKA" & tolower(data$source) == "vr" & data$t==2005] <- T
  kill[data$ihme.loc.id == "IDN" & data$source == "Population Census 2000"] <- T
  kill[data$ihme.loc.id == "VNM" & tolower(data$source) == "2007 population change and family planning survey"] <-T
  kill[data$ihme.loc.id == "THA" & tolower(data$source) == "vr" & (data$t==1997 | data$t==1998)] <-T
  kill[data$ihme.loc.id == "BMU" & tolower(data$source) == "undyb census"] <- T
  kill[data$ihme.loc.id == "SDN" & grepl("PAPCHILD", data$source) & data$in.direct=="hh"] <- T 
  kill[data$ihme.loc.id == "SRB" & data$source == "MICS3"] <- T 
  kill[data$ihme.loc.id == "KHM" & data$source == "SURVEY" & data$t == 1959] <- T
  kill[data$ihme.loc.id=="IND" & data$source=="DLHS2" & data$in.direct=="hh"] <- T
  kill[data$ihme.loc.id=="IND" & data$source=="DLHS3" & data$in.direct!="indirect"] <- T
  kill[data$ihme.loc.id == "HKG" & tolower(data$source) == "undyb census"] <- T
  kill[data$ihme.loc.id == "PRY" & data$source == "VR" & floor(data$t) == 1950] <- T
  kill[data$ihme.loc.id == "ARG" & tolower(data$source) == "life tables"] <- T
  kill[data$ihme.loc.id == "UKR" & data$source=="DHS" & data$t>= 2005 & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "UKR" & data$source=="MICS3"] <- T
  kill[data$ihme.loc.id == "TLS" & data$source=="DHS" & (floor(data$t)== 1974 | floor(data$t) == 1975) & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "LBN" & data$source == "PAPCHILD" & (data$source.date == "1967.25" | data$source.date == "1969.25")] <- T
  kill[data$ihme.loc.id == "RWA" & data$source == "DHS 2005" & data$source.date == "2005"] <- T
  kill[data$ihme.loc.id == "CPV" & data$source=="VR" & (data$t==1969 | data$t==1971)] <- T
  kill[data$ihme.loc.id == "COG" & data$source=="DHS" & data$in.direct=="direct" & floor(data$t)==1978] <- T
  kill[data$ihme.loc.id == "COG" & data$source=="DHS" & data$in.direct=="direct" & floor(data$t)==1979] <- T
  kill[data$ihme.loc.id == "GUY" & data$source=="MICS2" & data$in.direct=="indirect"] <- T
  kill[data$ihme.loc.id == "MDA" & data$source=="CDC-RHS" & data$in.direct=="indirect"] <- T
  kill[data$ihme.loc.id == "SYC" & data$source=="VR" & data$t==1961] <- T
  kill[data$ihme.loc.id == "DMA" & data$source=="VR" & (data$t==1969 | data$t==1963)] <- T 
  kill[(data$ihme.loc.id == "OMN" & data$source == "1975 Survey of 5 Towns" & data$source.date == 1975 & data$in.direct == "indirect") & (data$t == 1973 | data$t == 1972)] <- T
  kill[(data$ihme.loc.id == "OMN" & data$source == "11 towns surveys reported in ESCWA (1981)" & data$source.date == 1981 & data$in.direct == "indirect") & (data$t == 1975 | data$t == 1976)] <- T
  kill[(data$ihme.loc.id == "OMN" & data$source == "Oman Child Health Survey (OCHS) 1988-89" & data$source.date == 1988 & data$in.direct == "indirect") & data$t == 1986] <- T
  kill[(data$ihme.loc.id == "OMN" & data$source == "1993 Population census" & data$source.date == 1993 & data$in.direct == "indirect") & data$t == 1992] <- T
  kill[(data$ihme.loc.id == "ROU" & data$source == "LSMS")] <- T 
  kill[(data$ihme.loc.id == "COG" & data$source == "AIS" & data$source.date == "2009")] <- T
  kill[data$ihme.loc.id == "PHL" & data$source == "Census, IPUMS" & data$source.date == 1990 & data$in.direct == "indirect"] <- T
  kill[data$ihme.loc.id == "CIV" & data$source == "CENSUS" & data$source.date == 1998] <- T
  kill[data$ihme.loc.id == "CZE" & data$source == "CDC RHS" & data$in.direct== "direct"] <- T
  kill[data$ihme.loc.id == "CZE" & data$source == "CDC-RHS" & data$in.direct== "indirect"] <- T
  kill[data$ihme.loc.id == "CZE" & data$source == "undyb census"] <- T
  kill[data$ihme.loc.id == "BGR" & data$source == "undyb census" & data$in.direct== "indirect, MAC only" & data$source.date==1965] <- T
  kill[data$ihme.loc.id == "MAR" & grepl("DHS SP", data$source)] <- T
  kill[data$ihme.loc.id == "AGO" & data$source == "2008 Integrated Survey on Population Welfare" & data$in.direct=="na" & data$source.date==2008] <- T
  kill[data$q5 == 0 & !data$ihme.loc.id %in% c("AND", "BMU")] <- T
  kill[data$ihme.loc.id == "NGA" & data$source == "MICS3" & data$in.direct == "indirect" & data$source.date == "2007"] <- T
  kill[data$ihme.loc.id == "MOZ" & data$source == "AIS" & data$in.direct == "indirect" & data$source.date == "2009"] <- T
  kill[data$ihme.loc.id == "CHN_44533" & data$source == "VR_MOH_09C" | data$source == "VR_MOH_10C"] <- T
  kill[data$ihme.loc.id == "CHN_44533" & (data$source == "census" | data$source == "Census, IPUMS") & data$in.direct == "indirect" & (data$source.date == "1990" | data$source.date == "2000")] <- T
  kill[data$ihme.loc.id == "MRT" & data$source == "MICS3"] <- T
  kill[data$ihme.loc.id == "GTM" & data$source == "CDC-RHS" & data$source.date == "1978"] <- T
  kill[data$ihme.loc.id == "TLS" & data$source == "DHS" & data$in.direct == "indirect" & data$source.date == "1997"] <- T
  kill[data$ihme.loc.id == "CHN_44533" & data$source == "1 per 1000 Survey on Pop Change" & data$source.date == "2003"] <- T
  kill[data$ihme.loc.id == "NGA" & data$source == "world fertility survey" & data$in.direct == "indirect" & data$source.date == "1981"] <- T 
  kill[data$ihme.loc.id == "NPL" & data$source == "Census, IPUMS" & data$in.direct == "indirect" & data$source.date == "2001"] <- T
  kill[data$ihme.loc.id == "NPL" & data$source == "CENSUS"] <- T
  kill[data$ihme.loc.id == "PAK" & data$source == "Housing, Economic, Demographic Characteristics Survey, IPUMS" & data$in.direct == "indirect" & data$source.date == "1973"] <- T 
  kill[data$ihme.loc.id == "PER" & data$source == "Census, IPUMS" & data$in.direct == "indirect" & (data$source.date == "1993" | data$source.date == "2007")] <- T
  kill[data$ihme.loc.id == "THA" & data$source == "Census, IPUMS" & data$in.direct == "indirect" & (data$source.date == "1980" | data$source.date == "1990" | data$source.date == "2000")] <- T
  kill[data$ihme.loc.id == "KHM" & data$source == "Census, IPUMS" & data$in.direct == "indirect" & data$source.date == "2008"] <- T
  kill[data$ihme.loc.id == "KOR" & data$source == "WFS"] <- T
  kill[data$ihme.loc.id == "KOR" & data$source == "VR" & data$t < 1999] <- T
  kill[data$ihme.loc.id == "EGY" & data$source == "VR" & data$t >= 1954 & data$t <= 1965] <- T
  kill[data$source == "GLOBAL FUND 2008" & data$ihme.loc.id != "HTI"] <- T
  kill[data$source == "Census, IPUMS" & data$ihme.loc.id == "JAM" & data$source.date == 2001 & data$in.direct == "indirect"] <- T
  kill[data$ihme.loc.id == "KWT" & data$source == "Gulf Family Health Survey Report" & data$source.date == 1996] <- T
  kill[data$ihme.loc.id == "GIN" & data$source == "CENSUS" & data$source.date == 1983 & data$q5 < 84] <- T
  kill[data$ihme.loc.id == "SDN" & data$source == "CENSUS" & data$source.date == 2008 & data$q5 > 26] <- T
  kill[data$ihme.loc.id == "IND" & data$source == "India 2001 Census"] <- T
  kill[data$ihme.loc.id == "SYR" & data$source == "VR" & data$t <= 1980] <- T 
  kill[data$ihme.loc.id == "AFG" & grepl("DHS SP", data$source)] <- T
  kill[data$ihme.loc.id == "CMR" & data$source == "CENSUS" & data$source.date == "1987"] <- T
  kill[data$ihme.loc.id == "BGD" & data$source == "SRS" & data$source.date %in% c("1980","1981","1982")] <- T
  kill[data$ihme.loc.id == "BGD" & data$source == "SRS vital registration, BGD Bureau of Statistics" & data$source.date == "1983"] <- T
  kill[data$ihme.loc.id == "PAK" & data$source == "SRS" & data$t <= 1980] <- T
  kill[data$ihme.loc.id == "MUS" & data$t <= 1956 & data$q5 <= 10] <- T
  kill[data$ihme.loc.id == "OMN" & data$t <= 2004 & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "PAN" & data$source == "Census, IPUMS" & data$source.date == "1980"] <- T
  kill[data$ihme.loc.id == "PAN" & data$source == "undyb censys" & data$source.date == "1981"] <- T
  kill[data$ihme.loc.id == "GEO" & data$source.date=="1993" & data$source == "VR - TransMONEE"] <- T
  kill[data$ihme.loc.id == "IRN" & data$source.date=="1991" & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "TON" & data$source.date=="1957" & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "GTM" & data$source.date=="1981" & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "LBR" & data$source == "MIS" & data$in.direct == "direct" & data$t > 2005] <- T
  kill[data$ihme.loc.id == "BFA" & data$source == "CENSUS" & data$source.date %in% c("1984","1995","2005")] <- T
  kill[data$ihme.loc.id == "MAR" & data$source == "PAPCHILD" & data$in.direct == "hh" & data$source.date == "1997"] <- T
  kill[data$ihme.loc.id == "JAM" & data$source == "VR" & data$t == 2006] <- T 
  kill[data$ihme.loc.id == "CHN_44533" & data$source ==  "Census" & data$source.date == 2010] <- T
  kill[data$ihme.loc.id == "IDN" & data$source == "IFLS" & data$in.direct == "direct" & data$source.date == "1985.41662597656"] <- T
  kill[data$ihme.loc.id == "IDN" & data$source == "SUSENAS" & is.na(data$in.direct) & data$source.date == 2006] <- T 
  kill[data$ihme.loc.id == "IDN" & data$source == "SUPAS" & data$in.direct == "direct"] <- T # & data$t <= 1978] <- T
  kill[data$ihme.loc.id == "IDN" & data$source == "2000_CENS_SURVEY" & data$t == 1999] <- T
  kill[data$ihme.loc.id == "TUR" & data$source == "TUR_DHS" & data$t < 1983] <- T
  kill[data$ihme.loc.id == "TUR" & data$source == "turkey population and health survey" & data$t == 1984.8] <- T
  kill[data$ihme.loc.id == "WSM" & tolower(data$source) == "spc demographic and health survey 1999" & data$in.direct=="direct"] <-F
  
  ##########################
  kill <- rep(F,length(kill))
  for (i in 1:length(unique(data$ihme.loc.id))) {
    indir <- (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect" & !data$data.age == "new") | (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect, MAC only")
    
    indir[is.na(indir)] <- FALSE
    if( sum(indir) != 0 ) {
      i.survs <- unique(paste(data$source[indir],data$source.date[indir]))
      for (j in 1:length(i.survs)) {
        indir2 <- paste(data$source,data$source.date)==i.survs[j] & data$ihme.loc.id==unique(data$ihme.loc.id)[i]
        mark = unique(which(indir &indir2))                                                                                      
        if (length(mark)>0) point1 <- mark[data$t[mark]==max(data$t[mark])]
        if (length(mark)>1) point2 <- mark[data$t[mark]==max(data$t[mark[mark!=point1]])]
        
        in.direct.nomissing <- data$in.direct[point2]
        if(is.na(in.direct.nomissing)) in.direct.nomissing <- ""
        
        if (data$compiling.entity[point2]=="u" | data$source[point2] == "undyb census" | in.direct.nomissing == "indirect, MAC only") kill0 <- c(kill0,point1)                                         #Only drop most recent if source is UNICEF or source is undyb census
        else kill0 <- c(kill0,point1,point2)                                                                 #concatenate 
      }
    }
  }                                                        
  kill[kill0] <- T 
  kill[data$ihme.loc.id == "CZE" & grepl("census|RHS", data$source)] <- T
  kill[data$ihme.loc.id == "BOL" & data$source == "ENCUESTA DEMOGRAFICA NACIONAL 1975"] <- T
  kill[data$ihme.loc.id == "BRA" & data$source == "LSMS"] <- T
  kill[data$ihme.loc.id == "CPV" & data$source == "census 2000" & data$t < 2000] <- T
  kill[data$ihme.loc.id == "CAF" & data$source == "census" & floor(data$t) == 1987] <- T
  kill[data$ihme.loc.id == "CAF" & data$source == "undyb census" & floor(data$t) == 1972] <- T
  kill[data$country == "Latvia" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country == "Latvia" & tolower(data$source) == "undyb census"])] <- T
  kill[data$country == "Estonia" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$country=="Estonia" & tolower(data$source)=="undyb census" & data$source.date == 1989])] <- T
  kill[data$ihme.loc.id == "RUS" & tolower(data$source) == "undyb census" & data$t == max(data$t[data$ihme.loc.id == "RUS" & tolower(data$source) == "undyb census"])] <- T
  kill[data$ihme.loc.id == "AFG" & grepl("DHS SP", data$source)] <- T
  kill[data$ihme.loc.id == "PNG" & floor(data$t) == 1965 & data$source == "undyb census"] <- T
  kill[data$ihme.loc.id == "KOR" & floor(data$t) == 1983 & data$q5 > 80] <- T
  kill[data$ihme.loc.id == "DOM" & data$t < 1962 & data$source == "DHS" & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "IRN" & data$source == "Iran household survey" & data$q5 > 300] <- T
  kill[data$ihme.loc.id == "IRN" & data$source == "Iran household survey 2000" & data$q5 > 300] <- T
  kill[data$ihme.loc.id == "YEM" & data$source == "DHS" & data$in.direct == "direct" & data$t < 1966 & data$t > 1964] <- T
  kill[data$ihme.loc.id == "BTN" & data$q5 >300 & data$source == "demographic sample survey 1984"] <- T
  kill[data$ihme.loc.id == "SDN" & data$t == 2008 & data$source == "CENSUS"] <- T
  kill[data$ihme.loc.id == "BLR" & data$source == "Census, IPUMS"] <- T
  kill[data$ihme.loc.id == "KOR" & data$source == "VR" & data$t < 1970] <- T
  kill[data$ihme.loc.id == "COG" & (floor(data$t) == 1978 | floor(data$t) == 1979)] <- T
  kill[data$ihme.loc.id == "RWA" & data$q5 > 250 & data$t > 1994 & data$t < 1997] <- T
  kill[data$ihme.loc.id == "TON" & data$source == "VR" & data$t == 1957] <- T
  kill[data$ihme.loc.id == "TCD" & data$source == "DHS" & data$t <1971] <- T
  kill[data$ihme.loc.id == "SDN" & data$source == "Census, IPUMS" & data$source.date == 2008 & data$t <1990] <- T
  kill[data$ihme.loc.id == "TON" & data$source == "undyb census" & data$source.date == 1966] <- T
  kill[data$ihme.loc.id == "PAK" & grepl("DHS", data$source) & data$t < 1969 & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "DJI" & grepl("PAPFAM", data$source) & data$in.direct == "direct" & data$q5 > 150] <-T
  kill[data$ihme.loc.id == "GEO" & grepl("VR", data$source) & data$t == 1993] <- T
  kill[data$source == "HOUSEHOLD"] <- T
  data$in.direct[data$in.direct == "" | data$in.direct == "NULL" | grepl("na", data$in.direct, ignore.case = T)] <- NA  
  data$in.direct[data$source == "indirect"] <- "indirect"
  data$in.direct[data$in.direct == "null"] <- NA
  kill[(is.na(data$in.direct) & !grepl("VR",data$source) & !grepl("SRS",data$source) & !grepl("DSP",data$source) & !(data$ihme.loc.id %in% c("IND","CHN_44533")) & !data$ihme.loc.id == "PRK") | (data$in.direct == "hh" & !(data$ihme.loc.id %in% c("CHN_44533","IND")) & !data$source == "PAK_demographic_survey")] <- T 
  kill[(grepl("CHN_",data$ihme.loc.id) | grepl("IND_",data$ihme.loc.id)) & !(data$ihme.loc.id == "CHN_44533") & !grepl("indirect",data$in.direct)] <- F
  kill[data$ihme.loc.id == "ZAF" & data$source == "SURVEY" & floor(data$t) == 2006]<- F
  kill[data$ihme.loc.id == "ZAF" & grepl("Rapid Mortality",data$source)] <- F
  kill[data$ihme.loc.id == "ZAF_482" & data$source == "CENSUS" & (data$t == 2006 | data$t == 2000)]<- F
  kill[data$ihme.loc.id == "TON" & data$t == 2006 & data$source == "CENSUS"] <- F
  kill[data$ihme.loc.id == "SSD" & data$source == "SSD_HH_HEALTH_SURVEY_2010_32189"] <- T
  kill[data$ihme.loc.id == "KAZ" & grepl("DHS", data$source) & data$in.direct == "direct" & data$t > 1996 & data$t < 1998] <- T
  kill[data$ihme.loc.id == "SRB" & data$source == "MICS3"] <- T
  kill[data$ihme.loc.id == "NGA" & data$source == "NGA_DHS_1999_report20555"] <- T
  kill[data$ihme.loc.id == "NIC" & grepl("vr",data$source, ignore.case = T) & data$t < 1988] <- T
  kill[data$ihme.loc.id == "SAU" & data$t > 2000] <- F
  kill[data$ihme.loc.id== "MOZ" & grepl("DHS", data$source) & data$in.direct == "direct" & data$t < 1976] <- T
  kill[data$ihme.loc.id == "MLI" & data$source == "MLI DHS Preliminary Report 77388"] <- T 
  kill[data$ihme.loc.id == "AGO" & data$source == "AGO_MICS1report"] <- T
  kill[data$ihme.loc.id == "TJK" & data$source == "LSMS" & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "HTI" & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "ZAF" & data$source == "Community Survey, IPUMS" & data$source.date == 2007] <- T
  kill[data$ihme.loc.id == "ZAF" & data$source == "DHS final report"] <- T
  kill[data$ihme.loc.id == "OMN" & data$source == "VR" & (data$t %in% c(2001, 2003))] <- T
  kill[data$ihme.loc.id == "CHN_499" & data$source == "CENSUS" & data$t == 1982] <- T
  kill[data$ihme.loc.id == "PRK" & data$t < 2000] <- T
  kill[data$ihme.loc.id == "CHN_493" & data$source == "CENSUS" & data$t == 1982] <- T
  kill[data$ihme.loc.id == "MEX" & data$source == "Census" & data$source.date == 2010] <- T
  kill[data$ihme.loc.id == "TON" & data$source == "VR" & (data$t == 1990 | data$t == 1983)] <- T
  kill[data$ihme.loc.id == "IRQ" & data$source == "VR" & data$t < 1970] <- T
  kill[data$ihme.loc.id == "TKM" & data$source == "VR - TransMONEE" & data$t >=1999] <- T
  kill[data$ihme.loc.id == "CUB" & !(data$source %in% c("VR", "undyb census"))] <- T
  kill[data$ihme.loc.id == "JPN" & data$t == 2011] <- T
  kill[data$ihme.loc.id == "JPN_35426" & floor(data$t) == 2011 & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "JPN_35427" & floor(data$t) == 2011 & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "JPN_35451" & floor(data$t) == 1995 & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "CRI" & data$source == "133760#CRI_CENSUS_2011" & data$t < 2010 & data$t > 1990] <- T
  kill[data$ihme.loc.id == "ZAF" & data$source == "CENSUS" & data$t ==2011] <- T
  kill[data$ihme.loc.id == "ZAF" & data$source == "CENSUS" & data$source.date == 2001] <- T
  kill[data$ihme.loc.id == "ZAF" & data$source == "Census, IPUMS" & data$source.date == 2001] <- T
  kill[data$ihme.loc.id == "BWA" & data$source == "22125#BWA_FHS_2007_2008" & data$in.direct == "indirect" & data$t >2006] <- T
  kill[data$ihme.loc.id == "BOL" & data$source == "VR" & data$t >= 2000 & data$t <= 2005] <- T
  kill[data$ihme.loc.id == "CHN_501" & data$t == 2000 & grepl("MCHS", data$source)] <- T
  kill[data$ihme.loc.id == "CHN_517" & data$t < 2004 & grepl("DSP",data$source)] <- T
  kill[grepl("KEN_", data$ihme.loc.id) & data$q5 == 0.0000000] <- T
  kill[grepl("IND_", data$ihme.loc.id) & data$q5 == 0.0000000] <- T
  kill[grepl("BRA_", data$ihme.loc.id) & data$q5 == 0.0000000] <- T
  kill[data$ihme.loc.id %in% c("BRA_4770", "BRA_4750", "BRA_4752", "BRA_4771", "BRA_4763", "BRA_4753", "BRA_4776") & data$source.date < 2005 & grepl("PNAD",data$source)] <- T		 
  kill[grepl("KEN", data$ihme.loc.id) & data$in.direct == "indirect" & grepl("133219#KEN_AIS_2007", data$source) & data$source.date == 2012] <- T
  kill[data$ihme.loc.id == "TJK" & data$source == "LSMS 2007" & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "URY" & data$source == "VR" & data$source.date == 2010] <- T
  kill[grepl("CHN_", data$ihme.loc.id) & !(data$ihme.loc.id %in% c("CHN_354", "CHN_361")) & data$source %in% c("FFPS", "DC")] <- T
  kill[data$ihme.loc.id == "FSM" & data$source == "138577#FSM_CENSUS_1994" & data$source.date == 1994 & data$t < 1979 & data$q5 < 22] <- T
  kill[grepl("IND", data$ihme.loc.id) & data$source == "HOUSEHOLD_DHS_1998-1999"] <- T
  kill[data$ihme.loc.id == "NGA" & data$source == "VR"] <- T
  kill[data$ihme.loc.id == "BGD" & data$source == "MICS3" & data$source.date == 2009] <- T
  kill[grepl("BRA", data$ihme.loc.id) & (data$source == "DHS 1986" | (data$source == "DHS" & data$source.date == 1986))] <- T  
  kill[data$ihme.loc.id == "ZAF_490" & grepl("DHS", data$source) & data$in.direct == "direct"] <- T
  kill[data$ihme.loc.id == "PAK" & data$source == "9924#PAK 1981 Census Report" & data$source.date == 1981 & data$t >= 1979] <- T
  kill[data$ihme.loc.id=="BIH" & data$source== "MICS4"] <- T
  kill[data$ihme.loc.id=="KEN_35629" & data$source.date== 2003.91662597656] <- T
  kill[grepl("SAU_", data$ihme.loc.id) & data$source %in% c("CENSUS", "HOUSEHOLD")] <- F
  kill[grepl("ZAF", data$ihme.loc.id) & data$source=="CENSUS"] <- F
  kill[data$ihme.loc.id=="GRC" & data$source=="Census, IPUMS"] <- T
  kill[data$ihme.loc.id=="BFA" & data$source == "MIS 2014"] <- T
  kill[data$ihme.loc.id=="MNP" & data$source == "VR" & data$t==1989] <- T
  kill[data$ihme.loc.id=="IND_43903" & data$source == "IND_DLHS" & data$source.date==2003] <- T
  kill[data$ihme.loc.id=="IND_4842" & data$source == "IND_DLHS_2007_STATE_23258" & data$source.date==2008] <- T
  kill[data$ihme.loc.id=="IND_4854" & data$source == "IND_DLHS" & data$source.date==2003] <- T
  kill[data$ihme.loc.id=="IND_4864" & data$source == "IND_DHS_1992_1993_STATE_19787" & data$source.date==1992] <- T
  kill[data$ihme.loc.id=="IND_4869" & data$source == "IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[grepl("SWE", data$ihme.loc.id) & data$t==2011] <- T
  kill[data$ihme.loc.id=="ARE" & data$source == "13224#ARE_CHS_1987_1988" & floor(data$t)==1983] <- T
  kill[data$ihme.loc.id=="KEN_35620" & data$source=="DHS 2003" & data$source.date == 1990.25] <- T
  kill[data$ihme.loc.id=="IND_43885" & data$source=="IND_DLHS" & data$source.date==2003] <- T
  kill[data$ihme.loc.id=="IND_43909" & data$source=="India_DLHS3_urban_rural" & data$source.date==2008] <- T
  kill[data$ihme.loc.id=="IND_43921" & data$source=="IND_DLHS" & data$source.date==2003] <- T
  kill[data$ihme.loc.id=="IND_4842"  & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[data$ihme.loc.id=="IND_4849" & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[data$ihme.loc.id=="IND_4850" & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[data$ihme.loc.id=="IND_4864" & data$source=="IND_HDS_2004_STATE_26919" & data$source.date==2005] <- T
  kill[data$ihme.loc.id=="IND_43942" & data$source=="165390_IND_DLHS4_2012_2014"] <- T
  kill[data$ihme.loc.id=="IND_43900" & data$source=="5291#IND Census 1981" & data$source.date==2011 & data$q5>500] <- T
  kill[data$ihme.loc.id=="IND_43919" & data$source=="5291#IND Census 1981" & data$source.date==2011 & data$q5 > 500] <- T
  kill[data$ihme.loc.id=="DOM" & data$source=="VR" & data$t %in% c(2011, 2012)] <- T
  kill[data$ihme.loc.id=="LKA" & data$source=="VR" & data$t==2009] <- T
  kill[data$ihme.loc.id=="KEN_35656" & data$source== "DHS 1993"] <- T
  kill[data$ihme.loc.id=="GBR" & data$t==2000] <- T 
  kill[data$ihme.loc.id=="SRB" &  data$source == "VR" & (data$t %in% seq(1998, 2007))] <- T
  kill[data$ihme.loc.id=="MLI" & data$source=="MIS 2015" & data$t>2011 & data$in.direct=="direct" ] <- T
  kill[data$ihme.loc.id=="GBR" & data$t>=2014] <- T
  kill[data$q5 > 500 | data$q5 < 0] <- T
  kill[data$ihme.loc.id=="PRK"] <- T
  kill[data$ihme.loc.id=="NER" & data$source=="VR"] <- T
  kill[data$ihme.loc.id=="IRN" & data$source=="Iran household survey 2000"] <- T
  kill[data$ihme.loc.id=="SRB" & data$source=="CENSUS"] <- F
  kill[data$ihme.loc.id=="MNE" & data$source=="CENSUS"] <- F
  kill[grepl("CHN_",data$ihme.loc.id) & data$source=="VR" & data$t==2015] <- T
  kill[data$ihme.loc.id=="SAU_44543" & data$t < 2010] <- T
  kill[data$ihme.loc.id=="COG" & data$source=="VR" & data$t==2009] <- T
  kill[data$ihme.loc.id=="CHN_519" & data$t==2015 & data$source=="DSP"] <- T
  kill[data$ihme.loc.id=="KEN_35646" & data$source=="KEN_DHS_subnat" & data$source.date==2014] <- T
  kill[data$ihme.loc.id=="KEN_44796" & data$source=="KEN_DHS_oldprovince" & data$source.date==2014] <- T
  kill[data$ihme.loc.id=="LBN" & data$source=="VR" & data$t %in% c(2009, 2010)] <- T
  kill[data$ihme.loc.id=="IND_43875" & data$source=="165390_IND_DLHS4_2012_2014" & data$source.date==2013] <- T
  kill[data$ihme.loc.id=="MNE" & data$source=="CENSUS"] <- T
  kill[data$ihme.loc.id=="IRN" & data$source=="VR" & data$t==1991] <- T
  kill[data$ihme.loc.id=="IRN" & data$in.direct=="direct"] <- F
  kill[data$ihme.loc.id=="BWA" & data$source=="VR"] <- T
  kill[data$ihme.loc.id %in% c("ALB") & data$source=="VR" & data$t==2013] <- T
  kill[data$ihme.loc.id %in% c("BHR") & data$source=="VR" & data$t==2013] <- T
  kill[data$ihme.loc.id %in% c("HRV") & data$source=="VR" & data$t==2014] <- T
  kill[data$ihme.loc.id %in% c("TKM") & data$source=="VR" & ((data$t > 1999 & data$t < 2012) | data$t > 2013) ] <- T 
  kill[data$ihme.loc.id=="PSE" & data$source=="VR" & data$t < 2008] <- T
  kill[data$ihme.loc.id=="MDG" & data$source=="VR" & data$t < 2000 & data$t > 1980] <- T
  kill[data$ihme.loc.id=="MOZ" & data$source=="VR" & data$t==2001] <- T
  kill[data$ihme.loc.id=="GHA" & data$source=="VR" & data$t %in% c(2000, 2007)] <- T
  kill[data$ihme.loc.id=="MLI" & data$source=="VR"  & data$t %in% c(1981, 1984)] <- T
  kill[data$ihme.loc.id %in% c("IND_43874", "IND_43884", "IND_43893", "IND_43894") & data$source=="VR" & data$t < 1999] <- T
  
  
  ######################  
  
  outlier <- rep(0,length(data$t))
  outlier[kill] <- 1
  
  data <- cbind(data,outlier)  
  return(data)
}
indiaFix <- function(data) {
  nrow_data <- nrow(data)
  ind <- data.table(data[data$ihme_loc_id %in% c("IND_4871", "IND_4841"), ])
  nrow <- nrow(ind)
  
  expected_row_ap = nrow(ind[ind$ihme_loc_id == "IND_4841"]) 
  expected_row_telegana = nrow(ind[ind$ihme_loc_id == "IND_4871"]) 
  expected_row_old_ap = 0
  
  sources_case1 <- c("IND_DLHS_2007_STATE_23258", "IND_HDS_2004_STATE_26919", "157050#IND 2015-2016 DHS Report", 
                     "IND_SRS_1988_2018",
                     "IND DHS 2015 report", "DHS")
  ind1 <- ind[(source == "IND_DLHS" & source.date ==2003) | (source == "HDS 2005" & source.date >=1990) | source %in% sources_case1]
  
  ind1[, year_id := floor(year)]
  
  # reading in population file
  ind_pop <- get_mort_outputs("population", "estimate", locations = c("IND_4871", "IND_4841"), sex_id = 3, age_group_id = 1)
  setnames(ind_pop, "mean", "pop")
  ind_pop <- ind_pop[,.(ihme_loc_id, year_id, pop)]
  ind1 <- merge(ind1, ind_pop, by=c("ihme_loc_id", "year_id"), all.x=T)
  
  ## converting to mx space and pop-weighting
  ind1[, m5 := (-log(1-q5))/5]
  ind1[, m5 := m5 * pop]
  ind1[, source.date := as.numeric(source.date)]
  
  key_vars <- c("year_id","source", "in.direct", "compiling.entity", "data.age", "outlier", "shock", "NID", "filename", "microdata")
  for(i in key_vars){
    if(!is.character(ind1$i)) {
      ind1[, (i) := as.character(get(i))]
    }
  }
  
  setkeyv(ind1, key_vars)
  
  ind1 <- ind1[, .(year = mean(year), source.date = mean(source.date), pop = sum(pop), m5 = sum(m5)), by = key(ind1)]
  ind1[, m5 := m5/pop]
  
  ## converting back to qx space
  ind1[,q5 := 1- exp(-5 * m5)]
  
  ## Creating fake ihme_loc_ids and location names
  ind1[, location_name := "Old Andhra Pradesh"]
  ind1[, ihme_loc_id := "IND_44849"]
  
  ## Dropping and recreating relevant variables
  ind1[, c("pop", "m5", "year_id") := NULL]
  ind1[, sd.q5 := NA]
  ind1[, log10.sd.q5:= NA]
  ind1[, underlying_NID := NA]
  
  ## checking to make sure the number of rows is expected
  ## in this case, we create old Andhra Pradesh and no change made to New AP and Telengana data
  expected_row_old_ap = nrow(ind1) 
  
  ## Appending the pop weighted and aggregated old AP onto the main data set
  ind <- rbind(ind, ind1, use.names=T)
  
  expected_row_ap = expected_row_ap - nrow(ind[(source == "HDS 2005" & source.date < 1990)])
  ind <- ind[!(source == "HDS 2005" & source.date < 1990)] 
  
  sources_case2 <- c("5291#IND Census 1981", "60372#IND_CENSUS_2011", "DHS 1992", "DHS 1998", "DHS 2005", "IND_Census_2001_STATE_5314", "IND_DHS_1992_1993_STATE_19787",
                     "IND_DHS_1998_STATE_19950", "IND_DHS_2005_STATE_19963", "IND_IHDS_2011_2013")
  
  # replace AP with old AP
  ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999), ihme_loc_id := "IND_44849"]
  ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999), location_name := "Old Andhra Pradesh"]
  
  expected_row_ap = expected_row_ap - nrow(ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999)]) 
  expected_row_old_ap = expected_row_old_ap + nrow(ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999)]) 
  
  expected_row_telegana = expected_row_telegana -  nrow(ind[(source == "SRS" & location_name == "Telangana")])
  ind <- ind[!(source == "SRS" & location_name == "Telangana")]
  
  ind[source == "SRS", ihme_loc_id := "IND_44849"]
  ind[source == "SRS", location_name := "Old Andhra Pradesh"]
  
  expected_row_ap = expected_row_ap - nrow(ind[source=="SRS"]) 
  expected_row_old_ap = expected_row_old_ap + nrow(ind[source=="SRS"]) 
  
  expected_nrow_ind = expected_row_telegana + expected_row_ap + expected_row_old_ap
  if(nrow(ind) != expected_nrow_ind) stop("You have an unexpected total number of India AP/Telangana/old AP data points at the end of the AP fix")
  if(nrow(ind[ihme_loc_id == "IND_4871"]) != expected_row_telegana) stop("you have an unexpected number of Telangana data points at the end of AP fix")
  if(nrow(ind[ihme_loc_id == "IND_4841"]) != expected_row_ap) stop("you have an unexpected number of new AP data points at the end of AP fix")
  if(nrow(ind[ihme_loc_id == "IND_44849"]) != expected_row_old_ap ) stop("you have an unexpected number of old AP data points at the end of AP fix")
  
  data <- data[!data$ihme_loc_id %in% c("IND_4871", "IND_4841"),]
  data_fixed <- rbind(data, ind, use.names=T)
  if(nrow(data_fixed)!= nrow_data - nrow + expected_nrow_ind) stop ("after appending to data, somehow you have an unexpected number of rows post AP/Telangana fix")
  
  data_fixed <- data_fixed[!((data_fixed$ihme_loc_id %in% c("IND_43902", "IND_43872", "IND_43908", "IND_43938") & data_fixed$source=="SRS") & data_fixed$year < 2014 ),]
  
  # set SRS reports to "SRS"
  data_fixed[data_fixed$source %in% c("IND_SRS_1988_2018")]$compiling.entity <- "new"
  data_fixed[data_fixed$source %in% c("IND_SRS_1988_2018")]$in.direct <- NA
  data_fixed[data_fixed$source %in% c("IND_SRS_1988_2018")]$source <- "SRS"
  
  return(data_fixed)
}