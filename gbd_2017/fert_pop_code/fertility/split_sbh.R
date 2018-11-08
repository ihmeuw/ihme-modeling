################################################################################
## **Pulls in Standarized Summary Birth History Data and splits by draw with ASFR Loop 1 for specified model version**
## **Parallelized over prepped files in sbh directory**
################################################################################

## ROOTS AND DIRS

username <- USERNAME
j        <- FILEPATH
h        <- FILEPATH
codedir <- ifelse(Sys.info()[1] == 'Windows', '', paste0(FILEPATH)

## LOAD DEPENDENCIES
source(paste0(codedir, 'shared/primer.R'))
sessionInfo()

## ARGS
if (Sys.info()[1] == 'Windows'|interactive()) {
  sbh_dir <- FILEPATH
  ihmelid <- 
  version <- 
  
} else {
  sbh_dir <- commandArgs(trailingOnly = T)[1]
  ihmelid <- commandArgs(trailingOnly = T)[2]
  version <- commandArgs(trailingOnly = T)[3]
}


plot <- F


## MAIN

    
    locsdf <- readRDS(FILEPATH) # SHOULD SAVE A FLAT FILE AT BEGINNING OF MODEL RUN
    
    ## LOAD CEB DATA
    setwd(paste0(sbh_dir, '/standardized'))
    
    cebfiles <- list.files(getwd(), pattern = paste0(ihmelid, '_yid'))
    
    cebdf <- lapply(cebfiles, readRDS) %>% rbindlist(. , use.names = T, fill = T)
    
    ## LOAD ASFR DRAWS FOR GIVEN LOCATION
    
    drawfiles <- list.files(paste0(FILEPATH),
                            pattern = paste0(ihmelid, '_[[:digit:]]{2}_sim.csv'), recursive = T,
                            full.names = T)
    
    drawfiles <- grep("unraked", drawfiles, invert = T, value = T)
    asfrdf <- mclapply(drawfiles, function(dfile){
      
      retdf <- fread(dfile)
      retdf[, age := as.numeric(gsub('age_', '', tstrsplit(dfile, split = '/')[[13]]))]
      
    }, mc.cores = coreNum()) %>% rbindlist
    

    ## LOAD BOUNDS TO BACKTRANSFORM DRAWS
    asfrdf[, c("year_id", "med_bt") := .(floor(year), val)]
    
    ## RESHAPE DRAWS TO EMPHASIZE COHORT TRENDS
    
    asfrdf <- asfrdf[, c('ihme_loc_id', 'year_id', 'age', 'sim', 'med_bt')]
    asfrdf[, cohort := year_id - age]
    setnames(asfrdf, 'age', 'estage')
    asfrdf <- asfrdf[order(cohort, estage, sim)]
    
    widedf <- dcast(asfrdf, ihme_loc_id + cohort + sim ~ estage, value.var = 'med_bt')
    widedf <- widedf[order(sim, cohort)]
    widedf[, `10` := shift(`15`, 5, type = 'lag'), by = sim][!is.na(`10`), `10` := 0]
    widedf[, `50` := shift(`45`, 5, type = 'lead'), by = sim][!is.na(`50`), `50` := 0]  # CHANGE THIS ONCE MAKING ESTIMATES FOR 50 + 
    
    ## CREATE EXPANDED DATAFRAME TO ADVANCE COHORTS BY SINGLE YEARS
    
    templatedf <- expand.grid(agestart = 10:49, year_id = 1950:2016, sim = 0:999, ihme_loc_id = ihmelid) %>% as.data.table
    templatedf[, cohort := year_id - agestart][, year_id := NULL]
    templatedf <- templatedf[cohort >= 1940] # DROP COHORTS FRONT CENSORED BY PREDICTION INTERVAL (e.g. 1934 and earlier)
    templatedf <- merge(templatedf, widedf, by = c('cohort', 'sim', 'ihme_loc_id'), allow.cartesian = T)
    templatedf[, year_id := cohort + agestart]
    templatedf[, lage := round_any(agestart, 5, floor)][, uprop := (agestart-lage)/5][, lprop := 1 - uprop] #l prefix refers to lower age group contributing to weighted avg for fertility rate, u prefix refers to upper age group
    
    ## TEMP FIX: FOR SITUATION WHERE LPROP = 1 and ESTIMATES FOR UAGE ARE NOT WITHIN 1950-GBDYEAR PREDICTION INTERVAL
    ## ALLOWS THIS VALUE TO NOT BE NA
    for (lbound in seq(10, 45, 5)) templatedf[agestart == lbound & is.na(get(as.character(lbound + 5))), as.character(lbound + 5) := 0]
    
    ## CALCULATE COHORT ASFR AS WEIGHTED AVERAGE OF LOWER AND UPPER BOUNDING AGE GROUPS
    templatedf[lage == 10, val := (lprop * `10`) + (uprop * `15`)]
    templatedf[lage == 15, val := (lprop * `15`) + (uprop * `20`)]
    templatedf[lage == 20, val := (lprop * `20`) + (uprop * `25`)]
    templatedf[lage == 25, val := (lprop * `25`) + (uprop * `30`)]
    templatedf[lage == 30, val := (lprop * `30`) + (uprop * `35`)]
    templatedf[lage == 35, val := (lprop * `35`) + (uprop * `40`)]
    templatedf[lage == 40, val := (lprop * `40`) + (uprop * `45`)]
    templatedf[lage == 45, val := (lprop * `45`) + (uprop * `50`)]
    
    ## DROP VALUES OUTSIDE PREDICTION INTERVAL
    templatedf <- templatedf[!is.na(val)]
    templatedf[, c(as.character(seq(10, 50, 5)), 'lprop', 'uprop') := NULL]
    
    ## CALCULATE CUMULATIVE FERTILITY (E.G. AVG CHILDREN EVER BORN) BY COHORT AT DRAW LEVEL
    templatedf <- templatedf[order(cohort, sim, agestart)]
    templatedf[, cfert := cumsum(val), by = .(cohort, sim)]
    
    ## KEEP ORIGINAL FIVE YEAR AGE GROUPS
    templatedf <- templatedf[agestart %in% seq(15, 45, 5)]
    
    ## Merge ceb and asfr info - needs to merge data onto full cohort fert in past
    #setnames(cebdf, c('sv_age'), c('agestart'))
    
    splitdf <- merge(cebdf, templatedf, by = c("ihme_loc_id","cohort"), allow.cartesian = T) #need to allow cartesian because mapping entire cohort experience from model to 1 datapoint
    splitdf <- splitdf[year_id <= sv_year_id]
    splitdf[, maxage := max(agestart), by = .(cohort, nid, sv_year_id)]
    
    scalardf <- splitdf[agestart == maxage, .(nid, cohort, sv_year_id, sim, sv_cfert, cfert)]
    scalardf[, scalar := sv_cfert/cfert][, c('sv_cfert', 'cfert') := NULL]
    splitdf <- merge(splitdf, scalardf, by = c('nid', 'cohort', 'sv_year_id', 'sim'))
    splitdf[, splitval := scalar * val]
    
    
    collapsedf <- splitdf[, .(val = mean(splitval), var = var(splitval)), by = .(pvid, nid, source, source_type, ihme_loc_id, sv_year_id, year_id, agestart)]
    collapsedf[var < 1e-10 & agestart == 15, var := NA] # 15-19 age group not split so no variance associated with a split
    
    
    ## CLEAN AND SAVE
    collapsedf[, c('age_group_id', 'fmeasid') := .((agestart/5) + 5, 2)][, agestart := NULL]
    collapsedf <- merge(collapsedf, locsdf[, .(location_id, ihme_loc_id)], by = 'ihme_loc_id')
    collapsedf <- collapsedf[order(year_id, age_group_id)]
    collapsedf[, asfmvid := version]
    
    savedfs <- collapsedf[, c('pvid', 'asfmvid', 'nid', 'source', 'source_type', 'location_id', 'sv_year_id' ,'ihme_loc_id',
                             'year_id', 'age_group_id', 'fmeasid', 'val', 'var')] %>% split(., by = c("ihme_loc_id","nid", "sv_year_id"))
    
    assert_values(rbindlist(savedfs), colnames = c('source_type', 'nid', 'ihme_loc_id', 'age_group_id', 'year_id', 'val'), test = 'not_na')
    
    message('SAVING')
    
    lapply(savedfs, function(sdf) {saveRDS(sdf, paste0(FILEPATH))})
    
    if (plot) {
      
      ## GENERATE HEAT MAP FOR DATA VETTING
      compdf <- splitdf[agestart == sv_age, .(sv_cfert = mean(sv_cfert), cfert = mean(cfert)), by = .(pvid, nid, source, type, year_id, ihme_loc_id, cohort)]
      compdf[, sv_age := year_id - cohort]
      compdf[, label := source]
      for (sourcetype in c('census', 'kap', 'lsms', 'mis', 'ais', 'mics', 'dhs')) compdf[tolower(label) %like% sourcetype, label := sourcetype]
      compdf[, label := paste(year_id, label, nid, sep = '_')]
      diffcut <- 40
      compdf[, pctdiff := 100*(sv_cfert-cfert)/cfert][pctdiff <= -diffcut, rpctdiff := -diffcut + (1e-4)][pctdiff >= diffcut, rpctdiff := diffcut - (1e-4)][is.na(rpctdiff), rpctdiff := pctdiff]
      compdf[, sv_age := factor(sv_age, levels = seq(15, 45, 5))]
      
      mypal <- colorRampPalette(brewer.pal(9, 'RdYlBu'))
      heatmap <- ggplot(compdf, aes(x = sv_age, y = label)) + geom_tile(aes(fill = rpctdiff)) + geom_text(data = compdf[abs(pctdiff)> 10], aes(x = sv_age, y = label, label = round(pctdiff, 1))) + 
        scale_fill_gradientn(colors = mypal(100), limits = c(-diffcut, diffcut)) + theme_bw() + labs(x = 'Age at Survey', y = 'Source-year', fill = '% Diff Survey vs Loop 1 cTFR',
                                                                                                     title = paste0(locsdf[ihme_loc_id == unique(compdf$ihme_loc_id), location_name_short], ' (',unique(compdf$ihme_loc_id), ')'),
                                                                                                     subtitle = paste0('SBH Cumulative Fertility Comparison to ASFR va', version)) +
        theme(legend.position = 'bottom')
      
      pdf(FILEPATH), width = 8, height = 8)
      
      print(heatmap)
      
      dev.off()
      
    }

    

    
    
    