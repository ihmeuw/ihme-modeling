#############################################################################################
## DoExclusions
#############################################################################################

DoExclusions <- function(data, add.data, order, births, ddm_run_id) {
  if (order == 1) {    # RANDOM SOURCES (703-710)
    temp <- subset(data, iso3=="BGD")
    temp2 <- subset(temp, source != "retrospective survey of fertility and mortality 1974")
    data <- rbind(temp2, subset(data,iso3!="BGD"))
    add.data <- add.data[add.data$source != "NATIONAL FERTILITY SURVEY (WFS),  OCTOBER 1974",]
  }
  else if (order == 2) {    # CDC-RHS (716-757)
    temp <- subset(data, iso3=="GEO")
    temp2 <- subset(temp, source!="reproductive maternal and child health")
    data <- rbind(temp2, subset(data, iso3!= "GEO"))
    
    temp <- subset(data, iso3=="ALB")
    temp2 <- subset(temp, source!="rhs-cdc")
    data <- rbind(temp2, subset(data, iso3!= "ALB"))
    
    temp <- subset(data, iso3=="CPV")
    temp2 <- subset(temp, source!="dsr")
    data <- rbind(temp2, subset(data, iso3!= "CPV"))
    
    temp <- subset(data, iso3=="ECU")
    temp2 <- subset(temp, source!="eds")
    data <- rbind(temp2, subset(data, iso3!= "ECU"))
    
    temp <- subset(data, iso3=="SLV")
    temp2 <- subset(temp, source!="ens")
    data <- rbind(temp2, subset(data, iso3!= "SLV"))
    
    temp <- subset(data, iso3=="GTM")
    temp2 <- subset(temp, source!="rhs-cdc")
    data <- rbind(temp2, subset(data, iso3!= "GTM"))
    
    temp <- subset(data, iso3=="HND")
    temp2 <- subset(temp,
                    source!="encuesta nacional de epidemiolog�a y salud familiar 1996")
    data <- rbind(temp2, subset(data, iso3!= "HND"))
    
    temp <- subset(data, iso3=="NIC")
    temp2 <- subset(temp, source!="rhs-cdc")
    data <- rbind(temp2, subset(data, iso3!= "NIC"))
    
    temp <- subset(data, iso3=="PRY")
    temp2 <- subset(temp, source!="endsr")
    temp2 <- subset(temp2, source!="rhs-cdc")
    data <- rbind(temp2, subset(data, iso3!= "PRY"))
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
  else if (order == 254) { # DHS & WFS
    data <- subset(data, !(source %in% data$source[grep("(world)|(wolrd)|(egyptian fertility survey)|(ghana fertility survey)|(jamaica fertility survey)|(jamica fertility survey)|(turkey fertility survey)", tolower(data$source))] & in.direct == "direct"))
  }
  else if (order == 267) {   # DHS in-depth, 2847
    add.data <- add.data[add.data$iso3 != "PHL",]
  }
  else if (order == 299) {    # INDIA: SRS 1990-1997, 3106
    add.data<-subset(add.data,t>1995)
  }
  else if (order == 300) { # INDIA: SRS 1983-1995, 3115-6      
    add.data<-add.data[,c(1,2,4,5:9)]
    names(add.data)[2:3]<-c("t","q5")
  }
  else if (order == 301) { # BANGLADESH: SRS 2001-2003, 3128-3136
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
  else if (order == 314) {   # BANGLADESH: SRS, 3248
    add.data <- add.data[add.data$t != 2007,]
  }
  else if (order == 331) {  # Gulf Family surveys
    names(add.data) <- gsub("_", ".", names(add.data), fixed=T)
    add.data$Extracted.from <- add.data$Page <- NULL
  }
  else if (order == 335) { # CHINA - 5q0s from published government census
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
      data <- AppendData(data, add.data)  # Bug in CaC: Data appended 2x
    }
  }
  else if (order == 435) { # VR
    if (Sys.info()[1] == 'Windows') {
      path <- quote(win_path)
    } else {
      path <- quote(lin_path)
    }
    library(plyr)
    print(paste0(ddm_run_id, " used for VR."))
    vr.dir <- "FILEPATH"
    pop.dir <- db.dataloc[source_type == "POP", eval(path)]
    vr <- read_dta(paste(vr.dir, "/d00_compiled_deaths_v", ddm_run_id, ".dta", sep="")) 
    pop <- read_dta(paste(vr.dir, "/d09_denominators_v", ddm_run_id, ".dta", sep="")) 
    all.data <- UpdateVr(vr, pop, births, data)
    data <- all.data$data
    add.data <- all.data$add.data
    vr.test <<- add.data
    add.data$filename <- "d00_compiled_deaths.dta"
    add.data$source.type <- "VR"
  }
  else if (order == 436) {  # TransMONEE
    add.data <- add.data[,colnames(add.data) != "country"]
    names(add.data) <- sub("year","t",names(add.data))
    add.data$ihme.loc.id <- add.data$iso3
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
    add.data <- lots_of_srs[duplicated(lots_of_srs[,c("ihme.loc.id","t")]) == FALSE & lots_of_srs$source == "SRS vital registration, BGD Bureau of Statistics",]
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
  data <- setDF(data)
  return(list("data" = data, "add.data" = add.data))
}

UpdateVr <- function(vr, pop, births, data) {
  # Uses globals set by set.global.Directories
  # Imports hard-coded filepath of Thai births
  # Returns VR as add.data, plus edited data 

  cat("\nUpdating VR...\n")
  vr <- vr[!(grepl("IND_", vr$ihme_loc_id) &
               vr$deaths_source %in% c("SRS", "SRS_REPORT")),]
  vr <- vr[!(vr$ihme_loc_id=="GBR_4749" & vr$year < 1980),]
  pop <- pop[!(grepl("USA_", pop$ihme_loc_id) & pop$pop_source=="USA_CENSUS"),]

  births <- births[births$sex == "both",c("ihme_loc_id", "year", "births")]
  ind.srs.births.file <- "FILEPATH/IND_SRS_births_Back_calculated.csv")  
  births.srs <- read.csv(ind.srs.births.file)
  names(births.srs) <- c("year", "births")
  births.srs <- births.srs[births.srs$year > 1992,]
    
  # take only both sexes (not males and females) and extract only the necessary age variables for 5q0
  vr <- vr[vr$sex == "both" & vr$year>=1950,
           c("ihme_loc_id","country","year","deaths_source","source_type", "deaths_nid", "deaths_underlying_nid",
             paste("DATUM",seq(0,4),"to",seq(0,4),sep=""),"DATUM1to4","DATUM0to4")]
  vr$orig_source_type <- vr$source_type
  vr$source_type[grepl("VR", vr$source_type)] <- "VR"
  vr$source_type[grepl("SRS", vr$source_type)] <- "SRS"
  vr$source_type[grepl("DSP", vr$source_type)] <- "DSP"
  pop <- pop[pop$sex == "both" & pop$ihme_loc_id != "",
             c("ihme_loc_id","year","source_type","c1_0to0","c1_1to4")]
  
  # Use finer age groups to get coarser age groups (e.g. 1to4, 0to4)
  vr$DATUM1to4[is.na(vr$DATUM1to4)] <- apply(vr[is.na(vr$DATUM1to4),
                                                paste("DATUM",seq(1,4),"to",seq(1,4),sep="")],1,sum)
  vr$DATUM0to4[is.na(vr$DATUM0to4)] <- apply(vr[is.na(vr$DATUM0to4),
                                                c("DATUM0to0","DATUM1to4")],1,sum)
  # merge deaths and population
  vr$id <- paste(vr$ihme_loc_id, vr$source_type, vr$year, sep="_")
  pop$id <- paste(pop$ihme_loc_id, pop$source_type, pop$year, sep="_")
  
  vrpop1 <- merge(vr, pop, by=c("id", "ihme_loc_id", "source_type", "year"))
  vrpop2 <- merge(vr[!vr$id %in% vrpop1$id,
                     c("country", "ihme_loc_id","year","source_type","deaths_source",
                       "DATUM0to0","DATUM1to4","DATUM0to4", "orig_source_type", "deaths_nid", "deaths_underlying_nid")],
                  pop[pop$source_type=="IHME",c("ihme_loc_id","year","c1_0to0","c1_1to4")],
                  by=c("ihme_loc_id","year"),all.x=T)
  vrpop <- rbind(vrpop1[,names(vrpop2)], vrpop2)
  vrpop <- vrpop[!duplicated(vrpop),]
  
  # merge in births
  vrpop1 <- merge(vrpop[!(vrpop$ihme_loc_id == "IND" &
                            vrpop$source_type == "SRS" & vrpop$year>1992),], births,
                  by=c("ihme_loc_id","year"), all.x=TRUE)
  vrpop2 <- merge(vrpop[(vrpop$ihme_loc_id == "IND" &
                           vrpop$source_type == "SRS" & vrpop$year>1992),], births.srs,
                  by=c("year"), all.x=TRUE)
  vrpop <- rbind(vrpop1, vrpop2)
 
 
  vrpop <- vrpop[,names(vrpop) != "orig_source_type"]
  
  # calculate 5q0
  vr.q5 <- c()
  for(i in 1:nrow(vrpop)) {
    # Extract the ith row of data
    vrI <- c(as.numeric(vrpop[i,c("DATUM0to0","DATUM1to4")]),NA)
    names(vrI) <- c(0,1,5)
    popI <- c(as.numeric(vrpop[i,c("c1_0to0","c1_1to4")]),NA)
    names(popI) <- c(0,1,5)
    mortalityRate <- vrI/popI
    
    # First try to use the m -> q, if we can't then use deaths/births
    if(sum(is.na(mortalityRate[c("0","1")])) == 0 & !(vrpop[i,c("ihme_loc_id")]=="THA" & vrpop[i,c("year")] >= 2007 )) {
      #ax <- CalcMeanPYLbyDead(mortalityRate, "both")
      qs <- ConvertMortalityRateToProbability(mortalityRate,sex="both")
      q5I <- 1-((1-qs["0"])*(1-qs["1"]))
      vr.q5 <- c(vr.q5, q5I)
    } else {
      # deaths/births
      q5I <- vrpop[i,"DATUM0to4"]/vrpop[i,"births"]
      vr.q5 <- c(vr.q5, q5I)
    }
  }
  vr.q5 <- as.vector(vr.q5)
  # Standardize columns for vrpop
  names(vrpop)[names(vrpop) == "deaths_nid"] <- "NID"
  names(vrpop)[names(vrpop) == "deaths_underlying_nid"] <- "underlying_NID"
  add.data <- vrpop
  add.data <- cbind(add.data, vr.q5)
  add.data <- cbind(add.data, vrpop$year)
  add.data <- add.data[,c("ihme_loc_id","year","source_type","vr.q5","vrpop$year", "NID", "underlying_NID", "deaths_source")]
  
  names(add.data) <- c("ihme.loc.id","t","source","q5","source.date", "NID", "underlying_NID", "deaths_source")
  
  add.data <- cbind(add.data,data.age="new")
  add.data <- cbind(add.data,compiling.entity="new")
  add.data <- cbind(add.data,in.direct=NA)
  if(is.null(add.data$NID)) add.data <- cbind(add.data,NID=NA)
  
  add.data$q5 <- 1000*add.data$q5
  add.data <- unique(add.data)
  
  return(list("data" = data, "add.data" = add.data))
}


FinalDeduplicate <- function(data, add.data, locations) {
  # Done after appending all the data
  #
  data <- cbind(data, adjust=0)   
  local <- locations[,c("ihme_loc_id")]
  local$iso3 <- local$ihme_loc_id
  local <- plyr::rename(local, c("ihme_loc_id" = "ihme.loc.id"))
    
  missings <- data[is.na(data$ihme.loc.id),]
  missings$ihme.loc.id <- NULL
  mapped <- data[!is.na(data$ihme.loc.id),]
  missings <- merge(missings, local, by="iso3",all.x=T)
  missings$ihme.loc.id[is.na(missings$ihme.loc.id)] <- missings$iso3[is.na(missings$ihme.loc.id)]
  # put the mapped and unmapped together
  data <- rbind(missings, mapped)
  data$q5 <- as.numeric(data$q5)
  
  # Standardize China names
  data <- StandardizeChinaNames(data)
  
  # Deduplicate birth histories & more exclusions
  data <- BirthHistDedupe(data)
  return(data)
}

BirthHistDedupe <- function(data) {
  # Deduplicate birth histories
  # Lines 4972 - 5021
  # More exclusions in idiosyncratic code location
  #
  data$t <- as.numeric(data$t)
  data$source[data$source == "UNDYB census"] <- "undyb census"
  
  # merge in country names
  data = merge(unique(country.to.iso3[!(is.na(country.to.iso3$iso3)),
                                      c("country","laxo.country", "iso3")]), data, all.y = TRUE,by="iso3")
  
  #colnames(data)[colnames(data) == "
  # Clean up data
  data = subset(data, !is.na(t) & !is.na(q5))
   
  # Get rid of duplicates from merge
  data <- unique(data)
  
  # Get rid of duplicates (duplicates in terms of everyting (iso3, q5, t, source, etc.) but country) from countries with the same iso3 code but different country name
  data <- data[!duplicated(data[-2]),]

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
  
  # Mark the Japanese earthquake sin Iwate and  Miyagi in 2011 and Hyogo 1995  as shocks 
  shock[data$ihme.loc.id == "JPN_35426" & floor(data$t) == 2011 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "JPN_35427" & floor(data$t) == 2011 & data$source == "VR"] <- T
  shock[data$ihme.loc.id == "JPN_35451" & floor(data$t) == 1995 & data$source == "VR"] <- T
  
  # mark shock in MNE for the Kosovo war
  shock[data$ihme.loc.id == "MNE" & floor(data$t) %in% seq(1997,1999)] <- T
  
  isShock <- rep(0,length(data$t))
  isShock[shock] <- 1
  
  data <- cbind(data,isShock)
  names(data)[names(data) == "isShock"] <- "shock"  
  data$outlier[data$ihme.loc.id == "CHN_44533" & data$source == "1% Intra-Census Survey" & data$source.date == "1995" & data$t == "1992.6"] <- 0
  
  return(data)
}

CleanData <- function(data, locs) {
  # CAC Lines 6458 - 6487
  data = data[order(data$country, data$source, data$t),names(data)!="adjust"]
  data = subset(data, !is.na(ihme.loc.id) & !is.na(t) & !is.na(q5))
  data$source[data$source == "tom's vr"] <- rep("tom's vital registration",
                                                sum(data$source == "tom's vr"))
  names(data)[names(data) == "t"] <- "year"
  data$q5 <- data$q5/1000
  
  locs <- locs[,c("ihme_loc_id","location_name")]
  names(locs) <- c("ihme.loc.id","location_name")
  data <- merge(data,locs,by="ihme.loc.id")
  data <- data[,c("ihme.loc.id","location_name","year","q5","source",
                  "source.date","in.direct","compiling.entity","data.age",
                  "sd.q5","log10.sd.q5", "NID", "underlying_NID", "filename", "microdata", "shock", "outlier")]   # remove outlier & shock to apply with database
  
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
 
  data <- data[data$data.age=="new",]
  
  names(data)[names(data) == "ihme.loc.id"] <- "ihme_loc_id"
  
  return(data)
}

scrub <- function(data) {
  scrub <- rep(F, length(data$t))
  scrub[data$t < 1400] <- T
  scrub.list <- fread("FILEPATH/scrub_conditions.csv",na.strings=c(""," ","NA"))  
  
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
  scrub[data$data.age == "old" & tolower(data$source) %in% c("multiple indicator cluster survey","mics","dhs","DHS","demographic health survey") & data$ihme.loc.id !="AFG" & data$ihme.loc.id != "TKM"] <- T

  data <- data[!scrub,]
  
  scre <- read.csv("FILEPATH/scrub_reintroduce.csv")
  scre$ihme.loc.id <- scre$iso3
  scre$country <- NA
  scre$microdata <- NA
  scre$underlying_NID <- NA
  scre$filename <- NA
  scre$NID <- NA
  scre <- scre[,names(scre) != "scrub.reason"]
  
  scre <- as.data.table(scre)
  scre[,microdata:= ifelse(grepl("IPUMS", source), 1, NA)]
  scre[,microdata:= ifelse(grepl("VR", source) | grepl("SURVEY", source), 1, microdata)]
  scre[,microdata:= ifelse(is.na(microdata), 0, microdata)]
  scre <- as.data.frame(scre) #some operations in the later steps only work on data frames. No time to overhaul everything, so this will do for now.
  # do not want the 2007 ZAF community survey in here, because it is calculated on the wrong population.
  numbertobeaddedback <- nrow(scre)
  scre <- scre[!(scre$ihme.loc.id=="ZAF" & scre$source == "SURVEY" & scre$source.date == 2007),]
  nrow(scre) == numbertobeaddedback - 1
  data <- rbind(data,scre)
  return(data)
}

kill <- function(data) {
  old_locs <- fread(
    "FILEPATH/gbd_2017_locs.csv",
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
  
  #This flags indirect calculations based on 15-19 and 20-24 year-old women.
  #For the from="k" data we drop the two most recent, since these do not note which age groups the data is based on.
  #Note that these points are excluded from ALL analysis, but will appear in plots as small gray points.
  kill0 <- c()
  for (i in 1:length(unique(data$ihme.loc.id))) {
    indir <- (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect" & !data$data.age == "new") | (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect, MAC only")
    
    indir[is.na(indir)] <- FALSE
    if( sum(indir) != 0 ) {
      i.survs <- unique(paste(data$source[indir],data$source.date[indir]))            #indirect survey-years in a country (values) (direct points may also be associated with the type)
      for (j in 1:length(i.survs)) {
        indir2 <- paste(data$source,data$source.date)==i.survs[j] & data$ihme.loc.id==unique(data$ihme.loc.id)[i]     #data points associated with the survey-year known to have at least one indirect point in the country of interest
        mark = unique(which(indir &indir2))                                                                                      
        if (length(mark)>0) point1 <- mark[data$t[mark]==max(data$t[mark])]               #Find the last two 
        if (length(mark)>1) point2 <- mark[data$t[mark]==max(data$t[mark[mark!=point1]])]  #observations
        
        in.direct.nomissing <- data$in.direct[point2]
        if(is.na(in.direct.nomissing)) in.direct.nomissing <- ""
        
        if (data$compiling.entity[point2]=="u" | data$source[point2] == "undyb census" | in.direct.nomissing == "indirect, MAC only") kill0 <- c(kill0,point1)                                        
        else kill0 <- c(kill0,point1,point2)                                                                 #concatenate 
      }
    }
  }
  kill <- rep(F,length(data$q5))                                                        
  kill[kill0] <- T  
  kill <- rep(F,length(kill))
  for (i in 1:length(unique(data$ihme.loc.id))) {
    indir <- (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect" & !data$data.age == "new") | (data$ihme.loc.id==unique(data$ihme.loc.id)[i] & data$in.direct=="indirect, MAC only")
    
    indir[is.na(indir)] <- FALSE
    if( sum(indir) != 0 ) {
      i.survs <- unique(paste(data$source[indir],data$source.date[indir]))            #indirect survey-years in a country (values) (direct points may also be associated with the type)
      for (j in 1:length(i.survs)) {
        indir2 <- paste(data$source,data$source.date)==i.survs[j] & data$ihme.loc.id==unique(data$ihme.loc.id)[i]     #data points associated with the survey-year known to have at least one indirect point in the country of interest
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
  kill[kill0] <- T 

  data$in.direct[data$in.direct == "null"] <- NA
  
  ######################  
  outlier <- rep(0,length(data$t))
  outlier[kill] <- 1
  
  data <- cbind(data,outlier)  
  return(data)
}


indiaFix <- function(data) {
  nrow_data <- nrow(data)
  ind <- data.table(data[data$ihme_loc_id %in% c("IND_4871", "IND_4841"), ]) ## Andhra Pradesh and Telangana
  nrow <- nrow(ind)
  
  #initial expected number of rows of present AP,  Telegana and old AP
  expected_row_ap = nrow(ind[ind$ihme_loc_id == "IND_4841"]) 
  expected_row_telegana = nrow(ind[ind$ihme_loc_id == "IND_4871"]) 
  expected_row_old_ap = 0
  
  ## Manually inspected all of the sources, finding that:
  ## Case 1:
  ## AP and Telangana present and differnet in following sources:
  ## IND_DLHS for source.date 2003
  ## IND_DLHS_2007_STATE_23258
  ## IND_HDS_2004_STATE_26919
  ## 157050#IND 2015-2016 DHS Report
  ## HDS 2005, except for source.date < 1990
  
  ## SRS for 2014 and 2015 are included here
  ## In this case, we are going to population weight the AP and Telegana data to create old Andhra Pradesh
  ## And we'll keep the New AP and Telengana data as well
  
  # subsetting to the relevant data points
  sources_case1 <- c("IND_DLHS_2007_STATE_23258", "IND_HDS_2004_STATE_26919", "157050#IND 2015-2016 DHS Report", 
                     "2014 SRS Report", "2015 SRS Report",
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
  
  ## Dropping and recreating relevant variables (I don't think sd.q5 and log10.sd.q5 can be simply averaged, so I'm just making them NA)
  ind1[, c("pop", "m5", "year_id") := NULL]
  ind1[, sd.q5 := NA]
  ind1[, log10.sd.q5:= NA]
  ind1[, underlying_NID := NA]
  
  ## checking to make sure the number of rows is expected
  ## in this case, we create old Andhra Pradesh and no change made to New AP and Telengana data
  expected_row_old_ap = nrow(ind1) 
  
  ## Appending the pop weighted and aggregated old AP onto the main data set
  ind <- rbind(ind, ind1, use.names=T)
  
  ## Case 2:
  ## Only AP present in following sources:
  ## 5291#IND Census 1981
  ## 60372#IND_CENSUS_2011
  ## DHS 1992
  ## DHS 1998
  ## DHS 2005
  ## IND_Census_2001_STATE_5314
  ## IND_DHS_1992_1993_STATE_19787
  ## IND_DHS_1998_STATE_19950
  ## IND_DHS_2005_STATE_19963
  ## IND_DLHS for source.date 1999
  ## HDS 2005 for source.date < 1990
  ## IND_IHDS_2011_2013 
  ## For these sources, simply change new AP to old AP, execpt for HDS 2005 pre-1990, because we're not sure why post 1990 would have both new AP 
  ## Telangana but pre 1990 wouldn't, so we're not sure if it's data for new AP or old AP. For those 2 points, just drop out of the data set
  
  # drop HDS 2005 pre-1990 
  expected_row_ap = expected_row_ap - nrow(ind[(source == "HDS 2005" & source.date < 1990)])
  ind <- ind[!(source == "HDS 2005" & source.date < 1990)] 
  
  sources_case2 <- c("5291#IND Census 1981", "60372#IND_CENSUS_2011", "DHS 1992", "DHS 1998", "DHS 2005", "IND_Census_2001_STATE_5314", "IND_DHS_1992_1993_STATE_19787",
                     "IND_DHS_1998_STATE_19950", "IND_DHS_2005_STATE_19963", "IND_IHDS_2011_2013")
  
  # replace AP with old AP
  ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999), ihme_loc_id := "IND_44849"]
  ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999), location_name := "Old Andhra Pradesh"]
  
  expected_row_ap = expected_row_ap - nrow(ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999)]) 
  expected_row_old_ap = expected_row_old_ap + nrow(ind[(source %in% sources_case2) | (source =="IND_DLHS" & source.date==1999)]) 
  
  ## Case 3:
  ## AP and Telangana present and Telangana the same as AP in:
  ## SRS
  ## For SRS, we drop the Telangana data and use the AP data as old AP
  expected_row_telegana = expected_row_telegana -  nrow(ind[(source == "SRS" & location_name == "Telangana")])
  ind <- ind[!(source == "SRS" & location_name == "Telangana")]
  
  ind[source == "SRS", ihme_loc_id := "IND_44849"]
  ind[source == "SRS", location_name := "Old Andhra Pradesh"]
  
  expected_row_ap = expected_row_ap - nrow(ind[source=="SRS"]) 
  expected_row_old_ap = expected_row_old_ap + nrow(ind[source=="SRS"]) 
  
  
  ## ADD ALL THE CHECKS, append to "data" data frame
  expected_nrow_ind = expected_row_telegana + expected_row_ap + expected_row_old_ap
  if(nrow(ind) != expected_nrow_ind) stop("You have an unexpected total number of India AP/Telangana/old AP data points at the end of the AP fix")
  if(nrow(ind[ihme_loc_id == "IND_4871"]) != expected_row_telegana) stop("you have an unexpected number of Telangana data points at the end of AP fix")
  if(nrow(ind[ihme_loc_id == "IND_4841"]) != expected_row_ap) stop("you have an unexpected number of new AP data points at the end of AP fix")
  if(nrow(ind[ihme_loc_id == "IND_44849"]) != expected_row_old_ap ) stop("you have an unexpected number of old AP data points at the end of AP fix")
  
  data <- data[!data$ihme_loc_id %in% c("IND_4871", "IND_4841"),]
  data_fixed <- rbind(data, ind, use.names=T)
  if(nrow(data_fixed)!= nrow_data - nrow + expected_nrow_ind) stop ("after appending to data, somehow you have an unexpected number of rows post AP/Telangana fix")
  
  # also drop IND urban rural SRS
  data_fixed <- data_fixed[!((data_fixed$ihme_loc_id %in% c("IND_43902", "IND_43872", "IND_43908", "IND_43938") & data_fixed$source=="SRS") & data_fixed$year < 2014 ),]
  
  # set SRS reports to "SRS"
  data_fixed[data_fixed$source %in% c("2014 SRS Report", "2015 SRS Report")]$compiling.entity <- "new"
  data_fixed[data_fixed$source %in% c("2014 SRS Report", "2015 SRS Report")]$in.direct <- NA
  data_fixed[data_fixed$source %in% c("2014 SRS Report", "2015 SRS Report")]$source <- "SRS"
  
  return(data_fixed)
}