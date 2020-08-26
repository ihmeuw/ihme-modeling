
library("data.table")
library("feather") 
library("dplyr")
library("rjson") 

source("FILEPATH/get_location_metadata.R")


# set demographics for which to build redistribution table
arg <- commandArgs()[-(1:5)]  # First args are for unix use only
years <- as.numeric(arg[1])
ages <- as.numeric(arg[2])
sexes <- as.numeric(arg[3])
loc_id <- as.integer(arg[4])
code_system_id <- as.integer(arg[5])
gc_source <- arg[6]
description <- arg[7]


if (code_system_id==6) {
  gc_source <- "cod_full"
} 

yearLab <- ifelse(length(years)==1, years, paste0(min(years), "-", max(years)))
ageLab <- ifelse(length(ages)==1, sub("\\.", "_", ages), paste0(sub("\\.", "_", min(ages)), "-", sub("\\.", "_", max(ages))))
sexLab <- ifelse(length(sexes)==1, sexes, paste0(min(sexes), "-", max(sexes)))

age <- ages
sex <- sexes
year <- years

loc_meta_full <- get_location_metadata(location_set_id = 8, gbd_round_id = 6)
loc_meta <- loc_meta_full[location_id==loc_id, c("ihme_loc_id", "location_name", "region_name", "super_region_name", "developed", "path_to_top_parent")]

ihme_loc_id <- as.character(loc_meta[,1])
country <- gsub(" and ", " & ", as.character(loc_meta[,2]))
region <- gsub(" and ", " & ", as.character(loc_meta[,3]))
super_region <- gsub(" and ", " & ", as.character(loc_meta[,4]))
dev_status <- paste0("D", as.character(loc_meta[,5]))

# if this is a subnational get country from the location name of the parent country
if (grepl("_", ihme_loc_id)==T) {
  iso3_temp <- substr(ihme_loc_id, 1, 3)
  country <- gsub(" and ", " & ", as.character(loc_meta_full[ihme_loc_id==iso3_temp, location_name]))
  iso3 <- substr(ihme_loc_id, 1, 3)
  
} else {
  iso3 <- ihme_loc_id
  ihme_loc_id <- paste0(ihme_loc_id, "_", as.character(loc_id))
}


# set directories
if (Sys.info()["sysname"]=="Windows") {
  drive <- "H:"
} else {
  drive <- "~"
}



rdir <- FILEPATH   # set as the root redistribution directory path
pdir <- paste(rdir, "packages", code_system_id, sep = "/")  # use package directory 1 for ICD 10 & 6 for ICD9



pathExtension <- paste("outputs", iso3, description, ihme_loc_id, code_system_id, sep = "/")

cdir <- paste(rdir, pathExtension, sep = "/")
finaldir <- paste0(cdir, "/", yearLab, "_", ageLab, "_", sexLab, "/")
outdir <- paste0("FILEPATH", pathExtension, "/", yearLab, "_", ageLab, "_", sexLab, "/")

dir.create(finaldir)
dir.create(outdir)

# get cause map
icd2gbd <- fread(paste0(rdir, "/inputs/icd10_2_gbd.csv"))[cause_id!=743,][, target_codes := gsub("\\.", "", target_codes)]
gcList <- unique(fread(paste0(rdir, "/inputs/icd10_2_gbd.csv"))[cause_id==743,][, target_codes := gsub("\\.", "", target_codes)])

# get cod data to inform proportional redistribution packages
cod_full <- fread(paste0(rdir, "/datasets/", substr(ihme_loc_id, 1, 3), "_split_codes.csv"))
cod_full[, value := gsub("\\.X", "", value)]
cod_full[, value := gsub("\\.", "", value)]

cod <- cod_full[year_id==year & age==ages & sex_id==sex & location_id==loc_id, ][, c("value", "deaths")]
cod[, deaths := as.numeric(deaths)]
names(cod) <- c("target_codes", "deaths")

cod_full <- cod_full[, c("value", "deaths")]
names(cod_full) <- c("target_codes", "deaths")

if (gc_source!="all") {
  gc_included <- unique(eval(parse(text = paste0(gc_source, "$target_codes"))))
  gc_included <- gc_included[gc_included %in% gcList$target_codes]
} 


# Create master data frame with all combinations of location, age and sex
locMeta <- data.frame(ihme_loc_id = ihme_loc_id, country = country, region = region, super_region = super_region, dev_status = dev_status, stringsAsFactors = F)
master <- merge(locMeta, ages) %>% rename(age = y) %>% merge(years) %>% rename(year_id = y) %>% merge(sexes) %>% rename(sex = y)

# create age data frame with age index -- need later to expand contracted dataframes
ages <- data.frame(age = ages, age_index = 1:length(ages))


# get a list of package ids
pids <- as.integer(sub("package_", "", rjson::fromJSON(file = paste0(pdir, "/_package_list.json"))))

# get redistribution restrictions
cmap <- fread(paste0(pdir, "/cause_map.csv"))[garbage==0 & restrictions!="(age >= 0.0)",][, garbage := NULL]
cmap[, restrictions := gsub(" and ", " & ", restrictions)]

restrict <- data.table(restrictions = unique(cmap$restrictions), use = sapply(unique(cmap$restrictions), function(x) {eval(parse(text = x))}))
badcodes <- as.character(cmap[restrictions %in% restrict[use==F, restrictions], cause])


# save image to diagnose any run failures
save.image(paste0(finaldir, "startingImage.RData"))



#-----------------------------------------------------#
#           DEFINE EXTRACT_PACKAGE FUNCTION           #
#-----------------------------------------------------#

extract_package <- function(step, passthrough, gcListRollIn, cod) {
  
  file = paste0(pdir, "/package_", pids[step], ".json")
  
  # import json file
  pdata <- rjson::fromJSON(file = file)
  

  # get list of garbage codes processed in this packages
  if (country=="United States") {
    gcfilter <- "iran"
  } else if (country=="Iran") {
    gcfilter <- "usarmfor"
  } else {
    gcfilter <- "usarmfor|iran"
  }
  
  gc_in_list <- setdiff(as.character(pdata$garbage_codes), grep(gcfilter, pdata$garbage_codes, value = T))

  # get list of input garbage codes (minus those that have been already ingested by previous packages) & cross with the master, and append expanded passthrough
  gc <- gc_in_list[!(gc_in_list %in% gcListRollIn)]
  
  if (gc_source!="all") {
    gc <- gc[gc %in% gc_included]
  }  
  
  gc <- merge(master, data.frame(gc = gc, stringsAsFactors = F))
  setDT(gc)
  
  gc[, weight_prior := 1]
  gc[, "gc_in" := as.character()]
  
  
  
  # PROCESS DATA THAT HAVE BEEN PASSED ALONG THE REDISTRIBUTION CHAIN FROM PREVIOUS PACKAGES #
  
  # add passthrough codes that will be processed in this package
  setDT(passthrough)
  
  # process that passthrough file if there is one -- we'll first split into rows with codes to process in this package (pt_process),
  # and those to hold aside for processing in later packages (pt_append)
  if (nrow(passthrough)>0) {
    pt_process <- passthrough[gc_out %in% gc_in_list,]
    pt_append  <- passthrough[!(gc_out %in% gc_in_list), ]
    
    # prep the rows with codes to process in this file if there are any
    if (nrow(pt_process)>0) {
      
      names(pt_process) <- sub("^gc_out$", "gc", names(pt_process))
  
      pt_process <- merge(master, pt_process)
        
      # append to gc
      if (nrow(gc)>0) {
        gc <- rbind(gc, pt_process, fill = T)
      }
    }
  }
  
  gc[, index := 1:.N]
  
  if (nrow(gc)>0) {  
    
    # PROCESS WEIGHT GROUP INFORMATION AND DETERMINE WHICH WEIGHT GROUPS APPLY TO THE DEMOGRAPHICS BEING PROCESSED #
    
    # extract weight groups
    wg <- data.frame(do.call(rbind, lapply(pdata$weight_groups, function(x) {cbind(unlist(x[1]), unlist(x[2]))})), row.names = NULL, stringsAsFactors = F)
    names(wg) <- names(pdata$weight_groups[[1]])
    
    # create weight group id
    wg$wg_id <- 1:nrow(wg)
    
    # change weight_group_parameter from SQL to R syntax
    wg[,1] <- gsub(" or ", " | ", gsub(" and ", " & ", wg[,1]))
    
    # assign each row of gc to the correct weight group
    gc[, wg_id := integer()]
    for (i in 1:nrow(wg)) {
      gc[eval(parse(text = wg[i,1])), wg_id := i]
    }
    
    gc <- unique(gc)
    idList <- unique(gc$wg_id)
    
    gc[, c("ihme_loc_id", "country", "dev_status", "super_region", "region", "age", "sex", "year_id") := NULL]
    
    
    setDT(wg)
    
    
    # extract target groups
    extract_targets <- function(x) {
      ## x <- pdata$target_groups[[2]]
      
      # remove garbage codes from target codes
      x$target_codes <- setdiff(x$target_codes, pdata$garbage_codes)
      
      
      # remove exception codes and those specific to locations not being redistributed
      if (length(x) == 3) {
        x$target_codes <- setdiff(x$target_codes, x$except_codes)
      }
      
      x$target_codes <- setdiff(x$target_codes, grep(gcfilter, x$target_codes, value = T))
      x$target_codes <- setdiff(x$target_codes, badcodes)
      
      if (length(x$target_codes)>0) {
        
        # pull the target codes, and their weights
          tmerge <- merge(data.frame(target_codes = x$target_codes, stringsAsFactors = F), data.frame(wg_id = idList, weights = x$weights[idList]))
          tmerge <- data.table(merge(tmerge, cod, by = "target_codes", all.x = T))
          
          tmerge[, deaths := as.numeric(deaths)]
          tmerge[is.na(deaths)==T, deaths := 0]
          
          if (pdata$create_targets==1) {
            tmerge[, deaths := deaths + 0.001]
          }
          
          tmerge[, totalDeaths := sum(deaths), by = "wg_id"][totalDeaths==0, deaths := as.numeric(deaths) + 0.001][, totalDeaths := NULL]
          tmerge[, weights := weights * deaths / sum(deaths), by = "wg_id"][, "deaths" := NULL]
          
       
        tmerge <- tmerge[weights>0 & is.na(weights)==F,]
        
        forcod <- unique(gc[gc %in% cod$target_codes, .(gc, wg_id)])
        if (nrow(forcod)>0) {
          forcod <- merge(tmerge, forcod, by = "wg_id", all = T, allow.cartesian = T)
        }

        tmerge <- merge(tmerge, icd2gbd, by = "target_codes", all.x = T)
        
        # keep rows that match to gbd cause and collapse to get sum of weights
        done <- unique(tmerge[is.na(cause_id)==FALSE, ][, target_codes := NULL][, weights := sum(weights), by = .(wg_id, cause_id)])

        # merge with gc & clean up
        done <- merge(done, gc, by = "wg_id", allow.cartesian = T)
        done[is.na(gc_in)==TRUE, gc_in := as.character(gc)]
        done[, wg_id := NULL]
        
        # keep rows that need to be passed on to later redistribution packages
        topass <- tmerge[is.na(cause_id)==TRUE, ][, cause_id := NULL]
        
        # merge with gc
        topass <- merge(topass, gc, by = "wg_id", allow.cartesian = T)
        topass[is.na(gc_in)==TRUE, gc_in := as.character(gc)]
        topass[, wg_id := NULL]
        topass[, gc_out := as.character(target_codes)][, target_codes := NULL]

        return(list("topass" = topass, "done" = done, "forcod" = forcod))
        
      } 
      
    }
    
    extracted <- lapply(pdata$target_groups, extract_targets)
    extracted <- extracted[unlist(lapply(extracted,length)!=0)]
    
    # if there is only a single weight group then extracted will be a list of data tables, otherwise it will be a list of lists the following code
    # takes different approaches to rbind the contents correctly depending on which situation it is dealing with
    if (length(extracted)>0) {
      if (class(extracted[[1]]) == "list") {
        done <- do.call(rbind, lapply(extracted, function(x) {x$done}))
        topass <-  do.call(rbind, lapply(extracted, function(x) {x$topass}))
        forcod <-  do.call(rbind, lapply(extracted, function(x) {x$forcod}))
        
      } else {
        done <- extracted$done
        topass <- extracted$topass
      }
      
     
      scalers <- rbind(unique(done[, scaler := sum(weights, na.rm = T), by = index][, .(scaler, index)]), unique(topass[, scaler := sum(weights, na.rm = T), by = index][, .(scaler, index)]))
      scalers <- unique(scalers[, scaler := sum(scaler, na.rm = T), by = index])
      
      done <- merge(done[, scaler := NULL], scalers, by = "index", all.x = T)
      done <- done[, weights := weights * weight_prior / scaler][, .(weights, cause_id, gc_in)]
      
      topass <- merge(topass[, scaler := NULL], scalers, by = "index", all.x = T)
      topass <- topass[, weight_prior := weights * weight_prior / scaler][, .(weight_prior, gc_out, gc_in)]  
      
      if (nrow(forcod)>0) {
        forcod <- forcod[is.na(weights)==F, ]  
        forcod[, scaler := sum(weights, na.rm = T), by = c("wg_id", "gc")][, weights := weights/scaler][, c("wg_id", "scaler") := NULL]
        forcod <- unique(forcod[, weights := sum(weights), by = c("target_codes", "gc")])
        
        setnames(forcod, "target_codes", "tc")
        setnames(forcod, "gc", "target_codes")
        
        cod <- merge(cod, forcod, by = "target_codes", all = T, allow.cartesian = T)
        cod[is.na(weights)==F, deaths := deaths * weights]
        cod[is.na(tc)==F, target_codes := tc]
        cod[, c("tc", "weights") := NULL]
        cod <- unique(cod[, deaths := sum(deaths), by = "target_codes"])
        
      }
      

      if (exists("pt_append")) {
        topass <- (rbind(topass, pt_append))
      }
      
      done <- unique(done[, weights := sum(weights), by = .(gc_in, cause_id)])
      topass <- unique(topass[, weight_prior := sum(weight_prior), by = .(gc_in, gc_out)])
      
      gcListRollNew <- unique(c(done$gc_in, topass$gc_in))
    }
  }
  
  
  if (exists("extracted")==F) {
    extracted <- NULL
  }
  
  if (exists("done")==F) {
    done <- data.table() #weights, cause_id, gc_in
  } else if (class(done)=="function") {
    done <- data.table() #weights, cause_id, gc_in
  }
  
  if (exists("gcListRollNew")==F) {
    gcListRollNew <- NULL
  }
  
  if (exists("topass")==F) {
    topass <- data.table()
  }
  
  if (step==1) {
    gcListRollOut <- gcListRollNew
  } else if (nrow(gc)==0 | length(extracted)==0) {
    gcListRollOut <- gcListRollIn
    topass <- passthrough
  } else {  
    gcListRollOut <- unique(c(gcListRollNew, gcListRollIn))
  }

  if (debug==T) {
    write_feather(done, paste0(outdir, "package_", pids[step], "_rd.feather"))
    write_feather(topass, paste0(outdir, "package_", pids[step], "_topass.feather"))
    write_feather(data.frame(gc = gcListRollOut), paste0(outdir, paste("package", pids[step], "gcListRoll.feather", sep = "_")))
  }

  return(list("topass" = topass, "done" = done, "gcRoll" = gcListRollOut, "cod" = cod))
}





#---------------------------------------------------------


start_loop <- Sys.time()

passthrough <- data.table()
gcRoll <- NULL
rdAppend <- data.table()

for (i in 1:(length(pids)-1)) {
  start <- Sys.time()
  cat(pids[i], ": ", i, "of", length(pids))
  
  epout <- extract_package(step = i, passthrough = passthrough, gcListRollIn = gcRoll, cod = cod)
  passthrough <- epout$topass
  gcRoll <- epout$gcRoll
  cod <- epout$cod
  
  if (class(epout$done)!="function") {
    rdAppend <- rbind(rdAppend, epout$done)
  }
  
  end <- Sys.time()
  cat(" ", round(difftime(end, start, units = "mins"), digits = 2), "minutes", "\n")
  
  if (debug==T) {
    final <- rbind(passthrough, rdAppend, fill = T)
    if (nrow(final)>0) {
      final[is.na(weights)==T, weights := weight_prior][, "sum" := sum(weights), by = .(gc_in)]
      print(summary(final$sum))
    } else {
      print("No codes processed yet")
    }
  }
}

end_loop <- Sys.time()
difftime(end_loop, start_loop)


final <- rbind(passthrough, rdAppend, fill = T)
final[is.na(weights)==T, weights := weight_prior][, weight_prior := NULL]

write_feather(final, paste0(finaldir, "prefinal.feather"))


final[, "sum" := sum(weights, na.rm = T), by = .(gc_in)]
write_feather(setNames(data.frame(t(as.numeric(summary(final$sum)))), nm = names(final)), paste0(finaldir, "weightSums.feather"))




## PROCESS FINAL PACKAGE ---------------------------------------------------



cmeta <- fread(paste0(rdir, "/inputs/cause_meta.csv"))[, .(cause_id_3, cause_id, acause_3)]

if (country=="United States") {
  gcfilter <- "iran"
} else if (country=="Iran") {
  gcfilter <- "usarmfor"
} else {
  gcfilter <- "usarmfor|iran"
}



# import json file
pdata <-  rjson::fromJSON(file = paste0(pdir, "/package_", pids[length(pids)], ".json"))

# clean up target codes to remove those not allowed in this demographic group
x <- pdata$target_groups[[1]]
x$target_codes <- setdiff(x$target_codes, grep(gcfilter, x$target_codes, value = T))
x$target_codes <- setdiff(x$target_codes, pdata$garbage_codes)

if (length(x) == 3) {
  x$target_codes <- setdiff(x$target_codes, x$except_codes)
}

x$target_codes <- setdiff(x$target_codes, badcodes)



# pull the target codes, and their weights
tmerge <- data.frame(target_codes = x$target_codes, weights = x$weights[1], stringsAsFactors = F)
tmerge <- data.table(merge(tmerge, cod, by = "target_codes", all.x = T))

tmerge[is.na(deaths)==T, deaths := 0]
tmerge[, totalDeaths := sum(deaths)][totalDeaths==0, deaths := deaths + 0.001][, totalDeaths := NULL]
tmerge[, weights := weights * deaths / sum(deaths)][, "deaths" := NULL]

tmerge <- tmerge[weights>0 & is.na(weights)==F,]

tmerge <- merge(tmerge, icd2gbd, by = "target_codes", all.x = T)
tmerge[is.na(cause_id)==T, cause_id := 743]
tmerge <- unique(tmerge[, target_codes := NULL][, weights := sum(weights), by = .(cause_id)])

rd_last <- merge(data.frame(gc = pdata$garbage_codes, stringsAsFactors = F), tmerge, allow.cartesian = T)
setDT(rd_last)

rd_last <- unique(rd_last[, weights := sum(weights), by = .(gc, cause_id)])


tempRd <- final[is.na(cause_id)==FALSE, ][, c("gc_out", "sum") := NULL]
tempRd <- unique(tempRd[, weights := sum(weights), by = .(gc_in, cause_id)]) 

tempPt <- final[is.na(cause_id)==TRUE, ][, c("cause_id", "sum") := NULL] 
setnames(tempPt, "weights", "weight_prior")
setnames(tempPt, "gc_out", "gc")

gcList <- unique(tempPt$gc_in)

gcMerge <- function(x) {
  mergeTemp <- merge(tempPt[gc_in %in% x,], rd_last, by = "gc", all.x = T)
  mergeTemp[, weights := weights * weight_prior][, `:=` (gc = NULL, weight_prior = NULL)]
  mergeTemp <- mergeTemp[, lapply(.SD, sum), by = .(gc_in, cause_id), .SDcols = "weights"]
  
  return(mergeTemp)
}


start <- Sys.time()
ptMerged <- do.call(rbind, lapply(gcList, gcMerge))
end <- Sys.time()
difftime(end, start)

combined <- rbind(ptMerged, tempRd)

remain <- rd_last[!(gc %in% unique(combined$gc_in)), ]
setnames(remain, "gc", "gc_in")

combined <- rbind(combined, remain)
combined <- unique(combined[, weights := sum(weights), by = c("gc_in", "cause_id")])


write_feather(combined, paste0(finaldir, "final_detailed.feather"))







