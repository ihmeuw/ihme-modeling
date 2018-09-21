## Overwrite prepare_epp_fit in the package
 prepare_epp_fit <- function (pjnz, proj.end = 2015.5) {
    eppd <- read_epp_data(pjnz)
    epp.subp <- read_epp_subpops(pjnz)
    epp.input <- read_epp_input(pjnz)
    epp.subp.input <- fnCreateEPPSubpops(epp.input, epp.subp,
        eppd)
    val <- setNames(vector("list", length(eppd)), names(eppd))
    set.list.attr <- function(obj, attrib, value.lst) mapply(function(set,
        value) {
        attributes(set)[[attrib]] <- value
        set
    }, obj, value.lst)
    val <- set.list.attr(val, "eppd", eppd)
    val <- set.list.attr(val, "likdat", lapply(eppd, fnCreateLikDat,
        anchor.year = epp.input$start.year))
    val <- set.list.attr(val, "eppfp", lapply(epp.subp.input,
        fnCreateEPPFixPar, proj.end = proj.end))
    val <- set.list.attr(val, "country", attr(eppd, "country"))
    val <- set.list.attr(val, "region", names(eppd))
    val <- set.list.attr(val, "epidemic.type", epp.input$epidemic.type)
    return(val)
}





###########################################################################
####  Function to read inputs from Spectrum to EPP (.ep1, .ep3, .ep4)  ####
###########################################################################

read_epp_input <- function(pjnz){

  ## ep1
  ep1file <- grep(".ep1", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, ep1file)
  ep1 <- scan(con, "character", sep="\n")
  close(con)

  country.idx <- which(sapply(ep1, substr, 1, 7) == "COUNTRY")
  firstprojyr.idx <-  which(sapply(ep1, substr, 1, 11) == "FIRSTPROJYR")
  lastprojyr.idx <-  which(sapply(ep1, substr, 1, 10) == "LASTPROJYR")
  popstart.idx <- which(ep1 == "POPSTART")+1
  popend.idx <- which(ep1 == "POPEND")-1

  country <- as.character(read.csv(text=ep1[country.idx], header=FALSE, as.is=TRUE)[2])
  country.code <- as.integer(read.csv(text=ep1[country.idx], header=FALSE)[3])

  start.year <- as.integer(read.csv(text=ep1[firstprojyr.idx], header=FALSE)[2])
  stop.year <- as.integer(read.csv(text=ep1[lastprojyr.idx], header=FALSE)[2])
  epp.pop <- setNames(read.csv(text=ep1[popstart.idx:popend.idx], header=FALSE, as.is=TRUE),
                      c("year", "pop15to49", "pop15", "pop50", "netmigr"))

  ## ep4
  ep4file <- grep(".ep4", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, ep4file)
  ep4 <- scan(con, "character", sep="\n")
  close(con)
  
  cd4lim.idx <- which(sapply(ep4, substr, 1, 12) == "CD4LOWLIMITS")
  lambda.idx <- which(sapply(ep4, substr, 1, 6) == "LAMBDA")
  cd4init.idx <- which(sapply(ep4, substr, 1, 13) == "NEWINFECTSCD4")
  mu.idx <- which(sapply(ep4, substr, 1, 3) == "MU_")
  alpha1.idx <- which(sapply(ep4, substr, 1, 6) == "ALPHA1")
  alpha2.idx <- which(sapply(ep4, substr, 1, 6) == "ALPHA2")
  alpha3.idx <- which(sapply(ep4, substr, 1, 6) == "ALPHA3")
  infectreduc.idx <- which(sapply(ep4, substr, 1, 11) == "INFECTREDUC")
  artstart.idx <- which(ep4 == "ARTSTART")+1
  artend.idx <- which(ep4 == "ARTEND")-1

  cd4lim <- as.integer(read.csv(text=ep4[cd4lim.idx], header=FALSE)[-1])
  cd4init <- as.matrix(read.csv(text=ep4[cd4init.idx], header=FALSE, row.names=1))
  lambda <- as.matrix(read.csv(text=ep4[lambda.idx], header=FALSE, row.names=1))
  mu <- as.matrix(read.csv(text=ep4[mu.idx], header=FALSE, row.names=1))
  alpha1 <- as.matrix(read.csv(text=ep4[alpha1.idx], header=FALSE, row.names=1))
  alpha2 <- as.matrix(read.csv(text=ep4[alpha2.idx], header=FALSE, row.names=1))
  alpha3 <- as.matrix(read.csv(text=ep4[alpha3.idx], header=FALSE, row.names=1))
  infectreduc <- as.numeric(read.csv(text=ep4[infectreduc.idx], header=FALSE)[2])

  epp.art <- setNames(read.csv(text=ep4[artstart.idx:artend.idx], header=FALSE, as.is=TRUE),
                      c("year", "m.isperc", "m.val", "f.isperc", "f.val", "cd4thresh", "m.perc50plus", "f.perc50plus", "perc50plus", "1stto2ndline"))

  specpop.idx <- grep("SPECPOP", ep4)
  art.specpop <- read.csv(text=ep4[specpop.idx], header=FALSE,
                          colClasses=c("NULL", "character", "numeric", "integer"),
                          col.names=c(NA, "specpop", "percelig", "yearelig"))
  art.specpop$percelig <- art.specpop$percelig/100
  

  cd4median.start.idx <- which(ep4 == "CD4MEDIAN_START")+1
  cd4median.end.idx <- which(ep4 == "CD4MEDIAN_END")-1
  if(length(cd4median.start.idx) > 0)
    epp.pop$cd4median <- read.csv(text=ep4[cd4median.start.idx:cd4median.end.idx], header=FALSE, colClasses=c("NULL", "numeric"))[[1]]
  else
    epp.pop$cd4median <- 0
                             
  hivp15yr.start.idx <- which(ep4 == "HIVPOS_15YEAROLDS")+1
  hivp15yr.end.idx <- which(ep4 == "HIVPOS_15YEAROLDS_END")-1
  if(length(hivp15yr.start.idx) > 0)
    epp.pop$hivp15yr <- read.csv(text=ep4[hivp15yr.start.idx:hivp15yr.end.idx], header=FALSE, colClasses=c("NULL", "numeric"))[[1]]
  else
    epp.pop$hivp15yr <- 0

  art15yr.start.idx <- which(ep4 == "HIVPOS_15YEAROLDSART")+1
  art15yr.end.idx <- which(ep4 == "HIVPOS_15YEAROLDSART_END")-1
  if(length(art15yr.start.idx) > 0)
    epp.art$art15yr <- read.csv(text=ep4[art15yr.start.idx:art15yr.end.idx], header=FALSE, colClasses=c("NULL", "numeric"))[[1]]
  else
    epp.art$art15yr <- 0

  artdropout.start.idx <- which(ep4 == "ARTDROPOUTRATE")+1
  artdropout.end.idx <- which(ep4 == "ARTDROPOUTRATE_END")-1
  if(length(artdropout.start.idx) > 0)
    epp.art$artdropout <- read.csv(text=ep4[artdropout.start.idx:artdropout.end.idx], header=FALSE, colClasses=c("NULL", "numeric"))[[1]]
  else
    epp.art$artdropout <- 0

  hivp15yr.cd4dist.idx <- which(ep4 == "HIVPOS15_CD4")+1
  if(length(hivp15yr.cd4dist.idx) > 0)
    hivp15yr.cd4dist <- as.numeric(read.csv(text=ep4[hivp15yr.cd4dist.idx], header=FALSE))
  else
    hivp15yr.cd4dist <- rep(0, length(cd4lim))

  art15yr.cd4dist.idx <- which(ep4 == "HIVPOS15ART_CD4")+1
  if(length(art15yr.cd4dist.idx) > 0)
    art15yr.cd4dist <- as.numeric(read.csv(text=ep4[art15yr.cd4dist.idx], header=FALSE))
  else
    art15yr.cd4dist <- rep(0, length(cd4lim))


  ## Between 2014 and 2015, Spectrum switched from passing CD4 stage duration to passing
  ## CD4 stage progression rate. This change is unmarked in the .ep4 file.  Guess which
  ## is correct based on the first stage duration, assuming first stage should be > 1 year.

  if(mean(lambda[,1]) > 1)
    lambda <- lambda
  else
    lambda <- 1/lambda

  ## XML (for epidemic start year)

  xmlfile <- grep(".xml", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, xmlfile)
  epp.xml <- scan(con, "character", sep="\n")
  close(con)

  if (!require("XML", quietly = TRUE))
    stop("read_epp_input() requires the package 'XML'. Please install it.", call. = FALSE)
      
  obj <- xmlTreeParse(epp.xml)
  r <- xmlRoot(obj)[[1]]

  ## Note: tag "epidemicStartYrVarR" doesn't appear to change...
  ## Use epidemic start from first EPP subpopulation fit
  eppSetChildren.idx <- which(xmlSApply(r, xmlAttrs) == "eppSetChildren")
  eppSet <- r[[eppSetChildren.idx]][[1]][[1]]
  if(length(which(xmlSApply(eppSet, xmlAttrs) == "priorT0vr")) == 0) {
    eppSetChildren.idx2 <- which(xmlSApply(eppSet, xmlAttrs) == "eppSetChildren")
    eppSet2 <- eppSet[[eppSetChildren.idx2]][[1]][[1]]
    epidemic.start <- as.integer(xmlToList(eppSet2[[which(xmlSApply(eppSet2, xmlAttrs) == "priorT0vr")]][[1]]))
  } else {
    epidemic.start <- as.integer(xmlToList(eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "priorT0vr")]][[1]]))
  }

  ## TYP (for epidemic type: GENERALIZED or CONCENTRATED)"
  ep.type.file <- grep(".TYP", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, ep.type.file)
  ep.type <- scan(con, "character", sep="\n")
  close(con)

  ##########
  eppin <- list(start.year       = start.year,
                stop.year        = stop.year,
                epidemic.start   = epidemic.start,
                epp.pop          = epp.pop,
                cd4lowlim        = cd4lim,
                cd4initperc      = cd4init,
                cd4stage.dur     = lambda,
                cd4mort          = mu,
                artmort.less6mos = alpha1,
                artmort.6to12mos = alpha2,
                artmort.after1yr = alpha3,
                infectreduc      = infectreduc,
                epp.art          = epp.art,
                art.specpop      = art.specpop,
                hivp15yr.cd4dist = hivp15yr.cd4dist,
                art15yr.cd4dist  = art15yr.cd4dist,
                epidemic.type    = ep.type)
  class(eppin) <- "eppin"
  attr(eppin, "country") <- country
  attr(eppin, "country.code") <- country.code

  return(eppin)
}


############################################################################
####  Function to read prevalence data used in EPP fitting (from .xml)  ####
############################################################################

read_epp_data <- function(pjnz){

    xmlfile <- grep(".xml", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, xmlfile)
  epp.xml <- scan(con, "character", sep="\n")
  close(con)

  if (!require("XML", quietly = TRUE))
    stop("read_epp_data() requires the package 'XML'. Please install it.", call. = FALSE)
      
  obj <- xmlTreeParse(epp.xml)

  r <- xmlRoot(obj)[[1]]
  eppSetChildren.idx <- which(xmlSApply(r, xmlAttrs) == "eppSetChildren")
  country <- xmlToList(r[[which(xmlSApply(r, xmlAttrs) == "worksetCountry")]][[1]])

  epp.data <- list() # declare list to store output
  attr(epp.data, "country") <- country

  for(eppSet.idx in 1:xmlSize(r[[eppSetChildren.idx]])){
    # eppSet.idx <- 2
    print(eppSet.idx)
    tmp.eppSet <- r[[eppSetChildren.idx]][[eppSet.idx]][[1]]
    n.iter <- 1
    if (xmlSize(xmlSApply(tmp.eppSet, xmlAttrs)) > 0 & xmlSize(which(xmlSApply(tmp.eppSet, xmlAttrs) == "siteNames")) == 0) {
      newSetChildren.idx <- which(xmlSApply(tmp.eppSet, xmlAttrs) == "eppSetChildren")
      n.iter <- xmlSize(tmp.eppSet[[newSetChildren.idx]])
    }
    used.indices <- c()
    if(length(which(xmlSApply(tmp.eppSet, xmlAttrs) == "groupToAssignTo")) > 0) {
      hold <- tmp.eppSet[[which(xmlSApply(tmp.eppSet, xmlAttrs) == "groupToAssignTo")]]
      if(any(xmlSApply(hold, xmlAttrs) == "epp2011.core.sets.ProjectionSet")) {
        if(xmlSize(hold[[which(xmlSApply(hold, xmlAttrs) == "epp2011.core.sets.ProjectionSet")]]) > 1) {
          n.iter <- n.iter + length(which(xmlSApply(tmp.eppSet, xmlAttrs) == "groupToAssignTo"))
        }        
      }
    }
    for (eppSet.idx.2 in 1:n.iter) {
      # eppSet.idx.2 <- 2
      eppSet <- tmp.eppSet
      if (xmlSize(xmlSApply(tmp.eppSet, xmlAttrs)) > 0 & xmlSize(which(xmlSApply(tmp.eppSet, xmlAttrs) == "siteNames")) == 0)
        eppSet <- tmp.eppSet[[newSetChildren.idx]][[eppSet.idx.2]][[1]]

      if (xmlSize(eppSet) == 0) {

        tmp_len <- 0
        tmp_index <- 0
        obj_size <- 0
        while(obj_size == 0) {
          tmp_index <- tmp_index + 1
          if (tmp_index != eppSet.idx & !(tmp_index %in% used.indices)) {
            # print(paste('tmp_index',tmp_index))
            eppSet <- r[[eppSetChildren.idx]][[tmp_index]][[1]]
            tmp_len <- length(which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo"))
            if (tmp_len > 0) {
              tmp_eppSet <- eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo")]]
              set_location <- ifelse(xmlSize(tmp_eppSet) == 1, 1, which(xmlSApply(tmp_eppSet, xmlAttrs) == "epp2011.core.sets.ProjectionSet"))
              tmp_eppSet <- tmp_eppSet[[set_location]]
              obj_size <- xmlSize(tmp_eppSet)
            }
          }
        }
        used.indices <- c(used.indices, tmp_index)

        eppSet <- eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo")]]
        set_location <- ifelse(xmlSize(eppSet) == 1, 1, which(xmlSApply(eppSet, xmlAttrs) == "epp2011.core.sets.ProjectionSet"))
        eppSet <- eppSet[[set_location]]
      }
      if(eppSet.idx.2 > 1) {
        eppSet <- eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo")]]
        set_location <- ifelse(xmlSize(eppSet) == 1, 1, which(xmlSApply(eppSet, xmlAttrs) == "epp2011.core.sets.ProjectionSet"))
        eppSet <- eppSet[[set_location]]
      }
      eppName <- xmlToList(eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "name")]][["string"]])

      ##  ANC data  ##

      siteNames.idx <- which(xmlSApply(eppSet, xmlAttrs) == "siteNames")
      siteSelected.idx <- which(xmlSApply(eppSet, xmlAttrs) == "siteSelected")
      survData.idx <- which(xmlSApply(eppSet, xmlAttrs) == "survData")
      survSampleSizes.idx <- which(xmlSApply(eppSet, xmlAttrs) == "survSampleSizes")

      siteNames <- xmlSApply(eppSet[[siteNames.idx]][[1]], xmlSApply, xmlToList, FALSE)
      siteIdx <- as.numeric(xmlSApply(eppSet[[siteNames.idx]][[1]], xmlAttrs)) ## 0 based

      nsites <- length(siteNames)
      nANCyears <- max(as.integer(xmlSApply(eppSet[[survData.idx]][["array"]][[1]][[1]], xmlAttrs))) + 1

      ## ANC site used
      anc.used <- rep(FALSE, nsites)
      anc.used[as.integer(xmlSApply(eppSet[[siteSelected.idx]][[1]], xmlAttrs)) + 1] <- as.logical(xmlSApply(eppSet[[siteSelected.idx]][[1]], xmlSApply, xmlToList, FALSE))

      ## ANC prevalence
      anc.prev <- matrix(NA, nsites, nANCyears)
      rownames(anc.prev) <- siteNames
      colnames(anc.prev) <- 1985+0:(nANCyears-1)
      for(clinic.idx in 1:nsites){
        clinic <- eppSet[[survData.idx]][["array"]][[clinic.idx]][[1]]
        prev <- as.numeric(xmlSApply(clinic, xmlSApply, xmlToList, FALSE))
        idx <- as.integer(xmlSApply(clinic, xmlAttrs)) + 1
        anc.prev[clinic.idx, idx] <- prev
      }
      anc.prev[is.na(anc.prev)] <- 0.0 ## NOTE: appears that if value is 0.0, the array index is omitted from XML file, might apply elsewhere.
      anc.prev[anc.prev == -1] <- NA
      anc.prev <- anc.prev/100

      ## ANC sample sizes
      anc.n <- matrix(NA, nsites, nANCyears)
      rownames(anc.n) <- siteNames
      colnames(anc.n) <- 1985+0:(nANCyears-1)
      for(clinic.idx in 1:nsites){
        clinic <- eppSet[[survSampleSizes.idx]][["array"]][[clinic.idx]][[1]]
        n <- as.numeric(xmlSApply(clinic, xmlSApply, xmlToList, FALSE))
        idx <- as.integer(xmlSApply(clinic, xmlAttrs)) + 1
        anc.n[clinic.idx, idx] <- n
      }
      anc.n[anc.n == -1] <- NA


      ##  HH surveys  ##

      hhsUsed.idx <- which(xmlSApply(eppSet, xmlAttrs) == "surveyIsUsed")
      hhsHIV.idx <- which(xmlSApply(eppSet, xmlAttrs) == "surveyHIV")
      hhsSampleSize.idx <- which(xmlSApply(eppSet, xmlAttrs) == "surveySampleSize")
      hhsSE.idx <- which(xmlSApply(eppSet, xmlAttrs) == "surveyStandardError")
      hhsYear.idx <- which(xmlSApply(eppSet, xmlAttrs) == "surveyYears")

      nhhs <- max(xmlSize(eppSet[[hhsYear.idx]][[1]]),
                  xmlSize(eppSet[[hhsHIV.idx]][[1]]),
                  xmlSize(eppSet[[hhsSE.idx]][[1]]),
                  xmlSize(eppSet[[hhsSampleSize.idx]][[1]]),
                  xmlSize(eppSet[[hhsUsed.idx]][[1]]))

      hhs <- data.frame(year = rep(NA, nhhs), prev = rep(NA, nhhs), se = rep(NA, nhhs), n = rep(NA, nhhs), used = rep(NA, nhhs))

      hhs$year[as.integer(xmlSApply(eppSet[[hhsYear.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[hhsYear.idx]][[1]], xmlSApply, xmlToList, FALSE))
      hhs$prev[as.integer(xmlSApply(eppSet[[hhsHIV.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[hhsHIV.idx]][[1]], xmlSApply, xmlToList, FALSE))/100
      hhs$se[as.integer(xmlSApply(eppSet[[hhsSE.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[hhsSE.idx]][[1]], xmlSApply, xmlToList, FALSE))/100
      hhs$n[as.integer(xmlSApply(eppSet[[hhsSampleSize.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[hhsSampleSize.idx]][[1]], xmlSApply, xmlToList, FALSE))
      hhs$used[as.integer(xmlSApply(eppSet[[hhsUsed.idx]][[1]], xmlAttrs))+1] <- as.logical(xmlSApply(eppSet[[hhsUsed.idx]][[1]], xmlSApply, xmlToList, FALSE))

      hhs <- subset(hhs, !is.na(prev))

      epp.data[[eppName]] <- list(country=country,
                                  region=eppName,
                                  anc.used=anc.used,
                                  anc.prev=anc.prev,
                                  anc.n=anc.n,
                                  hhs=hhs)
    }
  }
  class(epp.data) <- "eppd"

  return(epp.data)
}


################################################################################
####  Function to read subpopulation sizes used in EPP fitting (from .xml)  ####
################################################################################

read_epp_subpops <- function(pjnz){

  xmlfile <- grep(".xml", unzip(pjnz, list=TRUE)$Name, value=TRUE)
  con <- unz(pjnz, xmlfile)
  epp.xml <- scan(con, "character", sep="\n")
  close(con)

  obj <- xmlTreeParse(epp.xml)
  r <- xmlRoot(obj)[[1]]
  eppSetChildren.idx <- which(xmlSApply(r, xmlAttrs) == "eppSetChildren")
  country <- xmlToList(r[[which(xmlSApply(r, xmlAttrs) == "worksetCountry")]][[1]])

  epp.pops <- list() # declare list to store output
  attr(epp.pops, "country") <- country

  workset.startyear <- as.integer(xmlToList(r[[which(xmlSApply(r, xmlAttrs) == "worksetStartYear")]][[1]]))
  workset.endyear <- as.integer(xmlToList(r[[which(xmlSApply(r, xmlAttrs) == "worksetEndYear")]][[1]]))


  pop15to49.idx <- which(xmlSApply(r, xmlAttrs) == "pop15to49")
  pop15.idx <- which(xmlSApply(r, xmlAttrs) == "pop15")
  pop50.idx <- which(xmlSApply(r, xmlAttrs) == "pop50")
  netMigration.idx <- which(xmlSApply(r, xmlAttrs) == "netMigration")

  epp.pops$total <-  data.frame(year = workset.startyear:workset.endyear,
                                pop15to49 = 0,
                                pop15 = 0,
                                pop50 = 0,
                                netmigr = 0)
  epp.pops$total$pop15to49[as.integer(xmlSApply(r[[pop15to49.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(r[[pop15to49.idx]][[1]], xmlSApply, xmlToList))
  epp.pops$total$pop15[as.integer(xmlSApply(r[[pop15.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(r[[pop15.idx]][[1]], xmlSApply, xmlToList))
  epp.pops$total$pop50[as.integer(xmlSApply(r[[pop50.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(r[[pop50.idx]][[1]], xmlSApply, xmlToList))
  epp.pops$total$netmigr[as.integer(xmlSApply(r[[netMigration.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(r[[netMigration.idx]][[1]], xmlSApply, xmlToList))

  epp.pops$subpops <- list()

  for(eppSet.idx in 1:xmlSize(r[[eppSetChildren.idx]])){

        tmp.eppSet <- r[[eppSetChildren.idx]][[eppSet.idx]][[1]]
    n.iter <- 1
    if (xmlSize(xmlSApply(tmp.eppSet, xmlAttrs)) > 0 & xmlSize(which(xmlSApply(tmp.eppSet, xmlAttrs) == "siteNames")) == 0) {
      newSetChildren.idx <- which(xmlSApply(tmp.eppSet, xmlAttrs) == "eppSetChildren")
      n.iter <- xmlSize(tmp.eppSet[[newSetChildren.idx]])
    }
    used.indices <- c()
    if(length(which(xmlSApply(tmp.eppSet, xmlAttrs) == "groupToAssignTo")) > 0) {
      hold <- tmp.eppSet[[which(xmlSApply(tmp.eppSet, xmlAttrs) == "groupToAssignTo")]]
      if(any(xmlSApply(hold, xmlAttrs) == "epp2011.core.sets.ProjectionSet")) {
        if(xmlSize(hold[[which(xmlSApply(hold, xmlAttrs) == "epp2011.core.sets.ProjectionSet")]]) > 1) {
          n.iter <- n.iter + length(which(xmlSApply(tmp.eppSet, xmlAttrs) == "groupToAssignTo"))
        }        
      }
    }
    for (eppSet.idx.2 in 1:n.iter) {
      # eppSet.idx.2 <- 2
      eppSet <- tmp.eppSet
      if (xmlSize(xmlSApply(tmp.eppSet, xmlAttrs)) > 0 & xmlSize(which(xmlSApply(tmp.eppSet, xmlAttrs) == "siteNames")) == 0)
        eppSet <- tmp.eppSet[[newSetChildren.idx]][[eppSet.idx.2]][[1]]

      if (xmlSize(eppSet) == 0) {

        tmp_len <- 0
        tmp_index <- 0
        obj_size <- 0
        while(obj_size == 0) {
          tmp_index <- tmp_index + 1
          if (tmp_index != eppSet.idx & !(tmp_index %in% used.indices)) {
            # print(paste('tmp_index',tmp_index))
            eppSet <- r[[eppSetChildren.idx]][[tmp_index]][[1]]
            tmp_len <- length(which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo"))
            if (tmp_len > 0) {
              tmp_eppSet <- eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo")]]
              set_location <- ifelse(xmlSize(tmp_eppSet) == 1, 1, which(xmlSApply(tmp_eppSet, xmlAttrs) == "epp2011.core.sets.ProjectionSet"))
              tmp_eppSet <- tmp_eppSet[[set_location]]
              obj_size <- xmlSize(tmp_eppSet)
            }
          }
        }
        used.indices <- c(used.indices, tmp_index)

        eppSet <- eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo")]]
        set_location <- ifelse(xmlSize(eppSet) == 1, 1, which(xmlSApply(eppSet, xmlAttrs) == "epp2011.core.sets.ProjectionSet"))
        eppSet <- eppSet[[set_location]]
      }
      if(eppSet.idx.2 > 1) {
        eppSet <- eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "groupToAssignTo")]]
        set_location <- ifelse(xmlSize(eppSet) == 1, 1, which(xmlSApply(eppSet, xmlAttrs) == "epp2011.core.sets.ProjectionSet"))
        eppSet <- eppSet[[set_location]]
      }
      eppName <- xmlToList(eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "name")]][["string"]])

      pop15to49.idx <- which(xmlSApply(eppSet, xmlAttrs) == "pop15to49")
      pop15.idx <- which(xmlSApply(eppSet, xmlAttrs) == "pop15")
      pop50.idx <- which(xmlSApply(eppSet, xmlAttrs) == "pop50")
      netMigration.idx <- which(xmlSApply(eppSet, xmlAttrs) == "netMigration")

      subp <- data.frame(year = workset.startyear:workset.endyear,
                         pop15to49 = 0,
                         pop15 = 0,
                         pop50 = 0,
                         netmigr = 0)
      subp$pop15to49[as.integer(xmlSApply(eppSet[[pop15to49.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[pop15to49.idx]][[1]], xmlSApply, xmlToList))
      subp$pop15[as.integer(xmlSApply(eppSet[[pop15.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[pop15.idx]][[1]], xmlSApply, xmlToList))
      subp$pop50[as.integer(xmlSApply(eppSet[[pop50.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[pop50.idx]][[1]], xmlSApply, xmlToList))
      subp$netmigr[as.integer(xmlSApply(eppSet[[netMigration.idx]][[1]], xmlAttrs))+1] <- as.numeric(xmlSApply(eppSet[[netMigration.idx]][[1]], xmlSApply, xmlToList))

      epp.pops$subpops[[eppName]] <- subp
      attr(epp.pops$subpops[[eppName]], "epidemic.start") <- as.integer(xmlToList(eppSet[[which(xmlSApply(eppSet, xmlAttrs) == "priorT0vr")]][[1]]))
    }
  }
  class(epp.pops) <- "eppsubp"

  return(epp.pops)
}


#####

fnCreateEPPSubpops <- function(epp.input, epp.subpops, epp.data){

  ## Raise a warning if sum of subpops is more than 1% different from total population
  if(any(abs(1-rowSums(sapply(epp.subpops$subpops, "[[", "pop15to49")) / epp.input$epp.pop$pop15to49) > 0.01))
    warning("Sum of subpopulations does not equal total population")

  ## If national survey data are available, apportion ART according to relative average HH survey prevalence in each subpopulation,
  ## If no HH survey, apportion based on relative mean ANC prevalence
  subpop.dist <- prop.table(sapply(epp.subpops$subpops, "[[", "pop15to49")[epp.subpops$total$year == 2010,])  # population distribution in 2010
  if(nrow(subset(epp.data[[1]]$hhs, used)) != 0){ # HH survey data available
    hhsprev.means <- sapply(lapply(epp.data, function(dat) na.omit(dat$hhs$prev[dat$hhs$used])), mean)
    hhsprev.means[is.na(hhsprev.means)] <- 0  ## in case of NA 
    art.dist <- prop.table(subpop.dist * hhsprev.means)
  } else {  ## no HH survey data
    ## Apportion ART according to relative average ANC prevalence in each subpopulation
    ancprev.means <- sapply(lapply(epp.data, "[[", "anc.prev"), mean, na.rm=TRUE)
    art.dist <- prop.table(subpop.dist * ancprev.means)
  }

  epp.subpop.input <- list()

  for(subpop in names(epp.subpops$subpops)){

    epp.subpop.input[[subpop]] <- epp.input
    epp.subpop.input[[subpop]]$epp.pop <- epp.subpops$subpops[[subpop]]
    epp.subpop.input[[subpop]]$epp.pop$cd4median <- epp.input$epp.pop$cd4median
    epp.subpop.input[[subpop]]$epp.pop$hivp15yr <- epp.input$epp.pop$hivp15yr * art.dist[subpop] # assume distributed same as art.dist (not sure what EPP does)

    epp.art <- epp.input$epp.art
    epp.art$m.val[epp.art$m.isperc == "N"] <- epp.art$m.val[epp.art$m.isperc == "N"] * art.dist[subpop]
    epp.art$f.val[epp.art$f.isperc == "N"] <- epp.art$f.val[epp.art$f.isperc == "N"] * art.dist[subpop]
    epp.art$art15yr <- epp.art$art15yr * art.dist[subpop]

    epp.subpop.input[[subpop]]$epp.art <- epp.art

    if(!is.null(attr(epp.subpops$subpops[[subpop]], "epidemic.start")))
      epp.subpop.input[[subpop]]$epidemic.start <- attr(epp.subpops$subpops[[subpop]], "epidemic.start")
  }

  return(epp.subpop.input)
}
