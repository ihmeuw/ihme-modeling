####
# USERNAME
# Smoothing marketscan scalars and writing to csv for use in Python
# 2017 March 20
rm(list = ls())

library(data.table)
library(dplyr)
library(foreign)
library(stringr)
library(R.utils)
library(haven)
library(RMySQL)

if (Sys.info()[1] == "Linux") {
    j <- "FILEPATH"
    h <- "FILEPATH"
} else if (Sys.info()[1] == "Windows") {
    j <- "J:"
    h <- "H:"
}

#########################
# define functions
#########################
#shared func
source(paste0("FILEPATH", "FILEPATH"))

# helper function to read a folder of dta files
ms_reader <- function(filepath) {
    dat <- lapply(paste0(filepath, list.files(filepath)), read_dta)
    dat <- rbindlist(dat)
    return(dat)
}

# helper function to read in a folder of csv files
csv_reader <- function(filepath) {
    dat <- lapply(paste0(filepath, list.files(filepath)), fread)
    dat <- rbindlist(dat, fill = TRUE)
    return(dat)
}

# marketscan data has single year ages so create gbd age bins
age_binner <- function(dat){
    # age bin
    agebreaks <- c(0,1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95,125)
    agelabels = c("0","1","5","10","15","20","25","30","35","40",
                 "45","50","55","60","65","70","75","80","85","90", "95")
    setDT(dat)[, age_start := cut(age, breaks = agebreaks, right = FALSE, labels = agelabels)]
    # convert agegroups to numeric
    dat$age_start <- as.numeric(as.character(dat$age_start))
    dat$age_end <- dat$age_start + 4
    dat[age_start == 0, 'age_end'] <- 1
    return(dat)
}

# get a map from bundle id to cause id
loadCauses <- function() {
    db_con = fread(paste0("FILEPATH", "FILEPATH"))

    # Get list of ids and names
    con <- dbConnect(dbDriver("MySQL"),
                     username = db_con$username,
                     password = db_con$pass,
                     host = db_con$host)
    df <- dbGetQuery(con, sprintf("QUERY"))
    dbDisconnect(con)
    return(data.table(df))
}

# get the cause ids beneath a given parent id. this is used for neonatal causes
loadChildCauses <- function(parent_id) {
    db_con = fread(paste0("FILEPATH", "FILEPATH"))

    # Get list of ids and names
    con <- dbConnect(dbDriver("MySQL"),
                     username = db_con$username,
                     password = db_con$pass,
                     host = db_con$host)
    df <- dbGetQuery(con, sprintf("QUERY",
                                  paste0(parent_id, collapse = ",")))
    dbDisconnect(con)
    return(data.table(df))
}


applyRestrictions <- function(df, manually_fix) {
    # refactor a function in hosp_prep.py which applies restrictions from
    # USERNAME's restriction map to our hospital data
    restrict <- fread(paste0("FILEPATH", "FILEPATH"))
    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.  This is necessary, there is a age_start = 0.1 years
    restrict[restrict$yld_age_start < 1, "yld_age_start"] <- 0

    level1 <- restrict[, c('Level1-Bundel ID', 'male',
                            'female', 'yld_age_start', 'yld_age_end'), with = FALSE]
    level2 <- restrict[, c('Level2-Bundel ID', 'male',
                           'female', 'yld_age_start', 'yld_age_end'), with = FALSE]
    level3 <- restrict[, c('Level3-Bundel ID', 'male',
                           'female', 'yld_age_start', 'yld_age_end'), with = FALSE]
    setnames(level1,'Level1-Bundel ID', 'bundle_id')
    setnames(level2,'Level2-Bundel ID', 'bundle_id')
    setnames(level3,'Level3-Bundel ID', 'bundle_id')
    # clean the bundle ids
    level1 <- level1[bundle_id != 0]
    level2 <- level2[bundle_id != 0 & bundle_id != "#USER"]
    level3 <- level3[bundle_id != 0]

    level2$bundle_id <- as.numeric(level2$bundle_id)

    if (manually_fix) {
        # USERNAME's restrictions are at the baby sequela, but we provide corr
        # factors at bundle level. take the broadest restrictions possible
        # for each bundle
        # manually fix the duplicates to continue developing code
        level1[ which(level1$bundle_id == 121), "yld_age_start"] <- 0
        level1[which(level1$bundle_id == 131), c("yld_age_start", "yld_age_end")] <- list(0, 95)
        level1[which(level1$bundle_id == 292), c("male", "female", "yld_age_start", "yld_age_end")] <- list(1, 1, 20, 65)
        level1[which(level1$bundle_id == 409), c("yld_age_start", "yld_age_end")] <- list(15, 95)
        level1[which(level1$bundle_id == 618), c("male", "female")] <- list(1, 1)
        level2[which(level2$bundle_id == 283), "yld_age_start"] <- 0
    }


    level1 <- unique(level1)
    level2 <- unique(level2)
    level3 <- unique(level3)
    # drop the multilevel bundles
    level2 <- level2[bundle_id != 616]
    level3 <- level3[bundle_id != 502]

    level_list = list(level1, level2, level3)
    for (i in 1:3) {
        # merge on restrictions
        pre <- nrow(df)
        df <- merge(df, level_list[[i]], all.x = TRUE, by = 'bundle_id')
        stopifnot(pre == nrow(df))

        case_cols <- c("inp_pri_claims_cases", "inp_pri_indv_cases", 'inp_any_claims_cases', 'inp_any_indv_cases',
                       'inp_otp_any_claims_cases', 'inp_otp_any_indv_cases', 'otp_any_claims_cases', 'otp_any_indv_cases')

        # age and sex restrictions
        df[ which(df$male == 0 & df$sex_id == 1), case_cols] <- NA
        df[ which(df$female == 0 & df$sex_id == 2), case_cols] <- NA
        df[ which(df$age_end < df$yld_age_start), case_cols] <- NA
        df[ which(df$age_start > df$yld_age_end), case_cols] <- NA

        # drop the columns we use to set restrictions
        df[, c('male', 'female', 'yld_age_start', 'yld_age_end') := NULL]
    }
    return(df)

}
########################
# get helper data
########################

# read a df of sex specific causes (bundle ids, ME ids, baby sequela ids)
sex_specific_df <- fread(paste0("FILEPATH", "FILEPATH"))
setnames(sex_specific_df, 'modelable_entity_id', 'me_id')

# get baby seq maternal causes
clean_maps <- fread(paste0("FILEPATH", "FILEPATH"))
# just maternal ME IDs
mat_clean_maps <- clean_maps[clean_maps$me_id %in% c(1555, 10484, 1535, 1543, 3086, 1550, 3085, 10504, 1544)]
# just level 1 ME IDs (baby sequela aren't duplicated)
mat_clean_maps <- mat_clean_maps[level==1]
mat_clean_maps <- unique(mat_clean_maps[, c('me_id', 'nfc_id'), with=FALSE])

# cause_restrictions <- get_cause_metadata(cause_set_id=9)
# cause_restrictions <- cause_restrictions[, c('cause_id', 'male', 'female', 'yld_age_start', 'yld_age_end'), with=FALSE]
# map bundles on cause restrictions
cause_map <- loadCauses()
# cause_restrictions <- merge(cause_restrictions, cause_map, by='cause_id')

# This is the function which calculates correction factors and writes the corr
# factor files to J
create_scalars <- function() {
    # loop over 3 ranks of correction factors (bundle, ME, baby sequela)
    # we're only concerned with bundles at the moment
    # for (cf_rank in c("bundle", "me_id", "baby")) {
    for (cf_rank in c("bundle")) {
        # counter for progress report
        counter <- 0

        # initiate final df to write to J
        final_df = data.table()

        # gives us some flexibility if we want to create corr factors at nfc level
        if (cf_rank == "bundle") {rank_name <- "bundle_id"
        } else if (cf_rank == "baby") {rank_name <- "nfc_id"
        } else {rank_name <- "me_id"}

        # read in data and subset important rows
        if (Sys.info()[1] == "Linux") {
            # cluster data
            df <- csv_reader("FILEPATH")
        }
        if (Sys.info()[1] == "Windows") {
            # local test data
            df <- csv_reader("FILEPATH")
        }

        # remove data from year 2000
        df <- df[year_start != 2000]

        # set otp claims to 0 for outpatient cf except for 3 facility types we want to use
        df[facility_id != 11 & facility_id != 22 & facility_id != 95]$otp_any_claims_cases <- 0

        # age bin and sum to 5 year groups
        df = age_binner(df)

        # this collapses the cases over age groups, sex_id and bundle
        collapser <- function(df) {
            check_inp_pri <- sum(df$inp_pri_claims_cases, na.rm=TRUE)
            df = df[, .(inp_pri_claims_cases = sum(inp_pri_claims_cases, na.rm=TRUE),
                        inp_pri_indv_cases = sum(inp_pri_indv_cases, na.rm=TRUE),
                        inp_any_claims_cases = sum(inp_any_claims_cases, na.rm=TRUE),
                        inp_any_indv_cases = sum(inp_any_indv_cases, na.rm=TRUE),
                        inp_otp_any_claims_cases = sum(inp_otp_any_claims_cases, na.rm=TRUE),
                        inp_otp_any_indv_cases = sum(inp_otp_any_indv_cases, na.rm=TRUE),
                        otp_any_claims_cases = sum(otp_any_claims_cases, na.rm=TRUE),
                        otp_any_indv_cases = sum(otp_any_indv_cases, na.rm=TRUE)),
                    by = .(age_start, sex_id, bundle_id)]
            stopifnot(check_inp_pri == sum(df$inp_pri_claims_cases, na.rm = TRUE))
            return(df)
        }

        df <- collapser(df)

        # map on restrictions
        df <- applyRestrictions(df, manually_fix = TRUE)

        # pool together cases for a select group of bundles
        anence_bundles <- c(610, 612, 614)
        chromo_bundles <- c(436, 437, 438, 439, 638)
        poly_synd_bundles <- c(602, 604, 606, 799)
        cong_bundles <- c(622, 624, 626, 803)
        cong2_bundles <- c(616, 618)

        neonate_bundles <- c(80, 81, 82, 500)

        anence <- df[bundle_id %in% anence_bundles]
        chromo <- df[bundle_id %in% chromo_bundles]
        poly_synd <- df[bundle_id %in% poly_synd_bundles]
        cong <- df[bundle_id %in% cong_bundles]
        cong2 <- df[bundle_id %in% cong2_bundles]
        neonate <- df[bundle_id %in% neonate_bundles]

        anence$bundle_id <- 610
        chromo$bundle_id <- 2000
        poly_synd$bundle_id <- 607
        cong$bundle_id <- 4000
        cong2$bundle_id <- 5000
        neonate$bundle_id <- 6000

        anence <- collapser(anence)
        chromo <- collapser(chromo)
        poly_synd <- collapser(poly_synd)
        cong <- collapser(cong)
        cong2 <- collapser(cong2)
        neonate <- collapser(neonate)

        # get list of bundle ids with the parent neonatal cause
        child_neo_causes <- unique(loadChildCauses(380))  # 380 is the parent cause for neonatal causes
        # get map from neonatal causes to bundle ID
        cause_bundle_map <- cause_map[cause_id %in% child_neo_causes$cause_id]
        # vector of neonatal bundles in the data
        neonate_bundles <- unique(df$bundle_id)[unique(df$bundle_id) %in% cause_bundle_map$bundle_id]

        #anence_buns <- rep(anence_bundles, 1, each=nrow(anence))
        chromo_buns <- rep(c(436, 437, 438, 638), 1, each=nrow(chromo))
        #poly_synd_buns <- rep(poly_synd_bundles, 1, each=nrow(poly_synd))
        cong_buns <- rep(cong_bundles, 1, each=nrow(cong))
        cong2_buns <- rep(cong2_bundles, 1, each=nrow(cong2))
        neonate_buns <- rep(neonate_bundles, 1, each=nrow(neonate))

        #anence <- do.call("rbind", replicate(length(anence_bundles), anence, simplify=FALSE))
        chromo <- do.call("rbind", replicate(length(unique(chromo_buns)), chromo, simplify=FALSE))
        #poly_synd <- do.call("rbind", replicate(length(poly_synd_bundles), poly_synd, simplify=FALSE))
        cong <- do.call("rbind", replicate(length(cong_bundles), cong, simplify=FALSE))
        cong2 <- do.call("rbind", replicate(length(cong2_bundles), cong2, simplify=FALSE))
        neonate <- do.call("rbind", replicate(length(neonate_bundles), neonate, simplify=FALSE))

        #anence$bundle_id <- 610
        chromo$bundle_id <- chromo_buns
        #poly_synd$bundle_id <- poly_synd_buns
        cong$bundle_id <- cong_buns
        cong2$bundle_id <- cong2_buns
        neonate$bundle_id <- neonate_buns

        # remove bundle IDs for pooled causes
        df <- df[!bundle_id %in% c(610, 436, 437, 438, 638, 607, 622, 624, 626, 803, 616, 618, neonate_bundles)]

        df <- rbind(df, anence, chromo, poly_synd, cong, cong2, neonate)
        #df[bundle_id %in% c(610, 612, 614), case_cols] <- anence[,..case_cols]

        # create a vector of sex specific causes at cf_rank level
        sex_specific <- unique(sex_specific_df[[rank_name]])
        sex_specific <- sex_specific[!is.na(sex_specific)]
        # manually add the REI PID to sex specific causes
        if (rank_name == 'bundle_id') {sex_specific = c(sex_specific, 294)}

        for (cause in unique(df[[rank_name]])) {
            # vars to catch unsmoothable cf levels
            all_null  = NULL
            type_null = NULL
            sex_null = NULL
            # # var to show progress complete and # of remaining cause ids
            counter <- counter + 1

            print(paste0(" ", round((counter/length(unique(df[[rank_name]])))*100, 2), "% Complete"))

            # make cartesian product
            dummy_grid <- expand.grid('age_start' = c(0,1,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95),
                                      'sex' = c(1,2))
            df_sub <- df[bundle_id == cause]
            setnames(df_sub, "sex_id", "sex")
            #df_sub$age_start <- as.numeric(as.character(df_sub$age_start))
            df_sub <- merge(dummy_grid, df_sub, all.x = TRUE)
            df_sub <- data.table(df_sub)

            # df to be used when smoothing doesn't work
            cases_df <- copy(df_sub)

            ########################################################
            # process raw ratios
            #######################################################

            # create cf from inpatient primary admis to any dx inpatient individual
            df_sub <- df_sub[, incidence := get("inp_any_indv_cases") / get("inp_pri_claims_cases")]

            # create cf from inpatient primary admis to inp + outpatient all individual
            df_sub <- df_sub[, prevalence := get("inp_otp_any_indv_cases") / get("inp_pri_claims_cases")]

            # create cf for injuries, any inpatient individual / any inpatient admission
            df_sub <- df_sub[, injury_cf := get("inp_any_indv_cases") / get("inp_any_claims_cases")]

            # create cf from admissions to individuals
            df_sub <- df_sub[, indv_cf := get("inp_pri_indv_cases") / get("inp_pri_claims_cases")]

            # These CF are mainly used in diagnostics
            # create cf from primary dx to all dx
            #df_sub <- df_sub[, all_dx_cf := get("ms_inp_indv") / get("primary_inp_indv")]
            # create cf from inpatient to inp + outpatient
            #df_sub <- df_sub[, in2out_cf := get("ms_all") / get("ms_inp_indv")]

            # outpatient corr factors
            df_sub <- df_sub[, outpatient := get("otp_any_indv_cases") / get("otp_any_claims_cases")]

            # reshape long
            df_sub <- melt(df_sub[, c("sex", "age_start", "incidence", "prevalence", "injury_cf", "indv_cf", "outpatient"), with = FALSE],
                                 id.vars = c("sex", "age_start"),
                                 measure.vars = c("incidence", "prevalence", "injury_cf", "indv_cf", "outpatient"),
                                 variable.name = "type",
                                 value.name = "value")

            # create the smoothed value column which will be filled later
            df_sub$smoothed_value <- as.numeric(NA)

            # loop over correction level and sex id
            # smooth each one of four correction factors - incidence, prevalence, injury_cf and individual
            for (corr_level in c("incidence", "prevalence", "injury_cf", "indv_cf", "outpatient")) {
                for (sex_id in c(1,2)) {
                    # maternal cause restrictions
                    if ( cause %in% c(79, 646, 74, 75, 423, 77, 422, 667, 76) & rank_name == "bundle_id" |
                         cause %in% c(1555, 10484, 1535, 1543, 3086, 1550, 3085, 10504, 1544) & rank_name == "me_id" |
                         cause %in% mat_clean_maps$nfc_id & rank_name == "nfc_id") {
                        if (sex_id == 1) {next}
                        # set age restrictions
                        # df_sub$value[df_sub$age_start < 10] <- NA
                        # df_sub$value[df_sub$age_start > 54] <- NA
                        # df_sub$value[df_sub$sex == 1] <- 0

                        # smooth values
                        dat = df_sub[type == corr_level & sex == sex_id]
                        dat$value[is.infinite(dat$value)] <- NA
                        smoothobj <- loess(log(value) ~ age_start,
                                           data = dat,
                                           span = 0.6)

                        # add smoothed values to df_sub
                        df_sub[type == corr_level & sex == 2, smoothed_value := exp(predict(smoothobj, dat))]
                        # dat$smoothed_value <- predict(smoothobj, dat)
                        #dat <- smoothScalar(df, cf, span=0.6)
                        #inc <- rbind(inc, dat, fill=TRUE)

                        if (sex_id == 2) {
                            # fix the tails of the predictions
                            # if the loess prediction ends at the last insample datapoint assign that value up to age start 95
                            max_age = max(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
                            upper_val <- df_sub$smoothed_value[df_sub$age_start==max_age & df_sub$sex==sex_id & df_sub$type==corr_level]
                            #prev$smoothed_value[is.na(prev$smoothed_value)& prev$sex==sex] <- upper_val
                            df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start>max_age & df_sub$type==corr_level] <- upper_val

                            # again for min age
                            min_age <- min(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
                            lower_val <- df_sub$smoothed_value[df_sub$age_start==min_age & df_sub$sex==sex_id & df_sub$type==corr_level]
                            df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start<min_age & df_sub$type==corr_level] <- lower_val
                        }

                        # re-set age restrictions
                        # df_sub$smoothed_value[df_sub$age_start < 10] <- NA
                        # df_sub$smoothed_value[df_sub$age_start > 54] <- NA
                        # df_sub$smoothed_value[df_sub$sex == 1] <- NA
                    } else {
                        # create loess model for subset of data
                        dat <- df_sub[type == corr_level & sex == sex_id]
                        dat$value[is.infinite(dat$value)] <- NA
                        # loess will break with too few data points
                        if (sum(!is.na(dat$value)) <= 2) {
                            all_null <- cause
                            type_null <- c(type_null, corr_level)
                            sex_null <- c(sex_null, sex_id)
                            # if (!cause %in% sex_specific) {
                            #     # append to a vector if not sex specific
                            #     all_null <- cause
                            #     type_null <- c(type_null, corr_level)
                            # }
                            next
                        }
                        else if (sum(!is.na(dat$value)) <= 5) {
                            smoothobj <- loess(log(value) ~ age_start, data = dat, span = 1)
                            # add loess smoothed values to df_sub
                            df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
                        }
                        else if (sum(!is.na(dat$value)) <= 10) {
                            smoothobj <- loess(log(value) ~ age_start, data = dat, span = .75)
                            # add loess smoothed values to df_sub
                            df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
                        } else {
                            smoothobj <- loess(log(value) ~ age_start,
                                               data = dat,
                                               span = 0.5)
                            # add loess smoothed values to df_sub
                            df_sub[type == corr_level & sex == sex_id, smoothed_value := exp(predict(smoothobj, dat))]
                        }
                        # fix the tails of the predictions
                        # if the loess prediction ends at the last insample datapoint assign that value up to age start 95
                        max_age = max(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$type == corr_level])
                        upper_val <- df_sub$smoothed_value[df_sub$age_start == max_age & df_sub$sex == sex_id & df_sub$type == corr_level]
                        #prev$smoothed_value[is.na(prev$smoothed_value)& prev$sex==sex] <- upper_val
                        df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex == sex_id & df_sub$age_start > max_age & df_sub$type == corr_level] <- upper_val

                        # again for min age
                        min_age <- min(df_sub$age_start[!is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$type==corr_level])
                        lower_val <- df_sub$smoothed_value[df_sub$age_start==min_age & df_sub$sex==sex_id & df_sub$type==corr_level]
                        df_sub$smoothed_value[is.na(df_sub$smoothed_value) & df_sub$sex==sex_id & df_sub$age_start<min_age & df_sub$type==corr_level] <- lower_val

                    } # else statement (after maternal cause smoothing)
                } # sex id
            } # corr level

            if (cause %in% all_null) {
                # take a weighted average if not able to smooth and not a sex specific cause
                # this should be exceedingly rare b/c we're smoothing values with only 2 observations
                for (asex in sex_null) {
                    if ("incidence" %in% type_null) {
                        df_sub[sex == asex & type == "incidence"]$smoothed_value <- sum(cases_df[sex==asex]$inp_any_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)
                        #df_sub[sex==asex & type=="incidence"]$smoothed_value <- sum(cases_df[sex==asex]$inp_any_indv_cases, na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)
                    }
                    if ("prevalence" %in% type_null) {
                        df_sub[sex==asex & type=="prevalence"]$smoothed_value <- sum(cases_df[sex==asex]$inp_otp_any_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)
                        #df_sub[sex==2 & type=="prevalence"]$smoothed_value <- sum(cases_df[sex==2]$inp_otp_any_indv_cases, na.rm=TRUE) / sum(cases_df[sex==2]$inp_pri_claims_cases, na.rm=TRUE)
                    }
                    if ("injury_cf" %in% type_null) {
                        df_sub[sex==asex & type=="injury_cf"]$smoothed_value <- sum(cases_df[sex==asex]$inp_any_indv_cases[!is.na(cases_df[sex==asex]$inp_any_claims_cases) & cases_df[sex==asex]$inp_any_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_any_claims_cases, na.rm=TRUE)
                        #df_sub[sex==2 & type=="injury_cf"]$smoothed_value <- sum(cases_df[sex==2]$inp_any_indv_cases, na.rm=TRUE) / sum(cases_df[sex==2]$inp_any_claims_cases, na.rm=TRUE)
                    }
                    if ("indv_cf" %in% type_null) {
                        df_sub[sex == asex & type == "indv_cf"]$smoothed_value <- sum(cases_df[sex==asex]$inp_pri_indv_cases[!is.na(cases_df[sex==asex]$inp_pri_claims_cases) & cases_df[sex==asex]$inp_pri_claims_cases != 0], na.rm=TRUE) / sum(cases_df[sex==asex]$inp_pri_claims_cases, na.rm=TRUE)
                        #df_sub[sex==2 & type=="indv_cf"]$smoothed_value <- sum(cases_df[sex==2]$inp_pri_indv_cases, na.rm=TRUE) / sum(cases_df[sex==2]$inp_pri_claims_cases, na.rm=TRUE)
                    }
                    if ("outpatient" %in% type_null) {
                        df_sub[sex == asex & type == "outpatient"]$smoothed_value <-
                            sum(cases_df[sex == asex]$otp_any_indv_cases[!is.na(cases_df[sex == asex]$otp_any_claims_cases) &
                            cases_df[sex == asex]$otp_any_claims_cases != 0], na.rm = TRUE) /
                            sum(cases_df[sex == asex]$otp_any_claims_cases, na.rm = TRUE)
                    }
                } # for asex in sex_null
            } # if cause in all_null

            # injury correction factor should never go above 1
            df_sub[type == "injury_cf" & smoothed_value > 1]$smoothed_value <- 1
            df_sub[type == "indv_cf" & smoothed_value > 1]$smoothed_value <- 1
            # outpatient correction factor shouldn't smooth above 1
            df_sub[type == "outpatient" & smoothed_value > 1]$smoothed_value <- 1

            # some values are infinite due to dividing a number by zero
            df_sub <- df_sub[is.infinite(smoothed_value), smoothed_value := NA]
            # some values are NaN due to dividing zero by zero
            #df <- df[is.na(smoothed_value), smoothed_value := 1]
            # some smoothed values are negative
            #df_sub$smoothed_value[df_sub$smoothed_value < 0] <- 1

            # df_sub$measure = run_measure
            df_sub[[rank_name]] = cause
            # rbind back together
            final_df = rbind(final_df, df_sub)

        } # end cause loop

        # re apply restrictions
        df = applyRestrictions(df, manually_fix = TRUE)

        # Write data
        write.csv(final_df, paste0("FILEPATH", "FILEPATH", rank_name, ".csv"), row.names = FALSE)
        # write backup file
        write.csv(final_df, paste0("FILEPATH", "FILEPATH", as.character(Sys.Date()), "_smoothed_by_", rank_name, ".csv"), row.names = FALSE)

        if (cf_rank == "bundle") {
           # reshape wide to make applying CF in python easier
           wide_df <- data.table::dcast(final_df, sex+age_start+bundle_id~type, value.var = 'smoothed_value')
           wide_df <- wide_df[, c("sex", "age_start", "bundle_id", "indv_cf", "incidence", "prevalence", "injury_cf", "outpatient"), with = FALSE]
           write.csv(wide_df, paste0("FILEPATH", "FILEPATH", rank_name, ".csv"), row.names = FALSE)
        }

    } # end loop for cf_rank
} # end function



#########################
# run everything
#########################
create_scalars()
