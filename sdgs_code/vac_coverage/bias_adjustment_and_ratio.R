###########################################################
### Purpose: Prep vaccination for ST-GPR		
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr, parallel, readxl, ggplot2, boot, lme4, pscl, purrr, splines, binom)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
}

## Initialize
source(paste0(j, 'FILEPATH/init.r'))

## Source
source(db_tools)

## Vaccine Introduction and Admin Coverage
vacc.intro <- paste0(data_root, "FILEPATH/vaccine_intro.rds")
vacc.admin <- paste0(data_root, "FILEPATH/who_admin.rds")
vacc.whosurvey <- paste0(data_root, "FILEPATH/who_survey.rds")
vacc.survey <- paste0(data_root, "FILEPATH")
vacc.outliers <- paste0(code_root, "FILEPATH/vaccination.csv")
vacc.lit <- paste0(data_root, "FILEPATH/vaccine_lit.rds")
vacc.schedule <- paste0(data_root, "FILEPATH/vaccine_schedule.rds")

## Other reference
me.db <- paste0(code_root, "/reference/me_db.csv") %>% fread
locs <- get_location_hierarchy(location_set_version_id)[level>=3]
year.est.start <- year_start
year.est.end <- year_end

## Suppress messages on readxl
read_excel <-  function(...) {
  quiet_read <- purrr::quietly(readxl::read_excel)
  out <- quiet_read(...)
  if(length(c(out[["warnings"]], out[["messages"]])) == 0)
    return(out[["result"]])
  else readxl::read_excel(...)
}

###########################################################################################################################
# SECTION 1: Prep tabulated survey data
###########################################################################################################################

load.survey <- function(folder) {
  ## Find most recent file in folder
  files <- list.files(folder, full.names=TRUE)
  files <- files[!grepl("failed", files)]
  file <- files[order(file.mtime(files), decreasing=TRUE)][1]
  print(paste0("Opening ", file))
  return(fread(file))
}


prep.survey <- function() {
  df <- load.survey(vacc.survey)
  ## Clean
  old <- c('var', 'mean', 'age_start')
  new <- c('me_name', 'data', 'age_year')
  setnames(df, old, new)
  ## Variance
  df <- df[, variance := standard_error^2]
  ## Age groups
  df <- df[, age_group_id := 22]
  ## Years
  df <- df[, year_id := floor((year_start+year_end)/2)]
  ## Center around birth year
  df <- df[, year_id := year_id - age_year]
  ## Clean me_name
  df <- df[me_name == "bcg1", me_name := "bcg"]
  df <- df[me_name == "yfv1", me_name := "yfv"]
  df <- df[, me_name := paste0("vacc_", me_name)]
  ## Remap some ihme_loc_ids
  df <- df[ihme_loc_id == "ALG", ihme_loc_id := "DZA"]
  df <- df[ihme_loc_id == "PAL", ihme_loc_id := "PSE"]
  ## Drop age_year == 0
  df <- df[age_year != 0]
  df <- df[, cv_survey := 1]
  ## Drop messed up locations
  drop.locs <- df[!(ihme_loc_id %in% locs$ihme_loc_id)]$ihme_loc_id %>% unique
  if (length(drop.locs)>0 ) {
    print(paste0("UNMAPPED LOCATIONS (DROPPING): ", toString(drop.locs)))
    df <- df[!(ihme_loc_id %in% drop.locs)]
  }
  ## If mean==0/1 use Wilson Interval Method (https://en.wikipedia.org/wiki/Binomial_proportion_confidence_interval#Wilson_score_interval)
  if (nrow(df[data %in% c(0,1)]) > 0) {
    df.w <- df[data %in% c(0,1)]
    sample_size <- df.w$sample_size
    n <- ifelse(df.w$data==0, 0, sample_size)
    ci <- binom.confint(n, sample_size, conf.level = 0.95, methods = "wilson")
    se <- (ci$upper - ci$lower)/3.92
    variance <- se^2 * 2.25 ## Inflate by design effect according to DHS official/unocfficial rule of thumb (http://userforum.dhsprogram.com/index.php?t=msg&goto=3450&)
    df[data %in% c(0,1)]$variance <- variance
  }
  ## If design effect is unreasonably small, assume 2.25 and readjust
  df <- df[design_effect < 1, variance := variance *  2.25/design_effect]
  df <- df[design_effect < 1, design_effect := 2.25]
  return(df)
}

###########################################################################################################################
# SECTION 4: Outlier data
###########################################################################################################################

outlier.data <- function(df) {
  ## Get outlier frame
  df.o <- fread(vacc.outliers) %>% unique
  df.o <- df.o[, outlier := 1]
  ## Set conditions
  cond <- list(
              list('me_name==""', c("nid")),
              list('batch_outlier==1 & me_name != ""', c("me_name", "nid")),
              list('batch_outlier==0', c("me_name", "nid", "ihme_loc_id", "year_id"))
          )
  ## Loop through and outlier
  for (i in cond) {
    condition <- i[[1]]
    vars <- i[[2]]
    ## Subset
    o.sub <- df.o[eval(parse(text=condition)), (vars), with=FALSE]
    o.sub <- o.sub[, outlier := 1]
    ## Merge
    df <- merge(df, o.sub, by=vars, all.x=TRUE)
    ## Set outlier
    df <- df[outlier==1, cv_outlier := data]
    ## Clean
    df <- df[outlier==1, data := NA]
    df <- df[, outlier := NULL]
  }
  return(df)
}

###########################################################################################################################
# SECTION 5: Clean data
###########################################################################################################################

clean.data <- function(df) {
  ## Get list of cvs
  cvs <- grep("cv_", names(df), value=TRUE)
  ## Subset to ones that are binary
  i <- lapply(cvs, function(x) length(setdiff(unique(df[!is.na(get(x))][[x]]), c(0,1)))==0) %>% unlist
  cvs <- cvs[i]
  ## Harmonize cv_* terms that are binary
  for (var in cvs) df[is.na(get(var)), (var) := 0]
  ## Clean year_id
  df <- df[, year_id := as.numeric(as.character(year_id))]
  ## Clean sample_size
  df <- df[, sample_size := as.numeric(sample_size)]
  ## Offset coverage
  df <- df[data<=0, data := 0.001]
  df <- df[data>=1, data := 0.999]
  ## Age group and sex
  df <- df[, `:=` (age_group_id=22, sex_id=3)]
  ## Bundle
  df <- df[, bundle := "vaccination"]
  ## Order
  df <- df[order(me_name, ihme_loc_id, year_id)]
  return(df)
}

###########################################################################################################################
# SECTION 6: Calculate out administrative bias
###########################################################################################################################

## For each country/vaccine, run a individual regressions to estimate
## a country/vaccine shift, running a spline model + a dummy term for if the data
## is from WHO/UNICEF Admin or not

calc.shift <- function(df, me, loc, graph=FALSE) {
  ## Subset
  df <- df[me_name==me & ihme_loc_id==loc]
  ## If any admin data
  if (nrow(df[cv_admin==1])>1) {
  #------------------------------
    ## Set knots based on how many data points there are
    n <- nrow(df[!is.na(data) & cv_admin == 0])
    n.knots <- ifelse(n < 5, 2, ifelse(n >=5 & n < 10, 3, 5))
    ## Formula
    formula <- as.formula(paste0("logit(data) ~ cv_admin + ns(year_id, knots=", n.knots, ")"))
    mod <- lm(formula, data=df)
    ## Extract shift
    ## Set cv_admin = 0 and predict
    df <- df[cv_admin==1, admin := 1]
    df <- df[, cv_admin := 0]
    df <- df[, predict := predict(mod, newdata=df) %>% inv.logit]
    ## Calculate mean shift as the administrative bias
    df <- df[admin==1, shift := mean(predict-data, na.rm=TRUE)]
    shift <- unique(df[admin==1]$shift)
    ## Graph
    if (graph) {
      df <- df[, year_id := as.numeric(as.character(year_id))]
      df <- df[order(year_id)]
      df <- df[admin==1, newadmin := data + shift]
      df <- df[, newadmin := ifelse(newadmin <0, 0, newadmin)]
      p <- ggplot(df) +
        geom_point(aes(y=data, x=year_id, color='Survey')) +
        geom_point(data=df[admin==1], aes(y=data, x=year_id, color='Original Admin')) +
        geom_point(data=df[admin==1], aes(y=newadmin, x=year_id, color='Adjusted Admin'), shape=2) +
        geom_ribbon(data=df[admin==1], aes(ymax=data, ymin=newadmin, x=year_id), alpha=0.1) +
        scale_color_manual(values= c("Survey"="Black", "Original Admin"="Red", "Adjusted Admin"="Blue")) +
        ylab("Coverage") + xlab("Year") +
        theme_bw()+
        theme(axis.title=element_text(),
              plot.title=element_text(size=10),
              strip.text=element_text(size=12, face ="bold"),
              strip.background=element_blank(),
              axis.text.x = element_text(size = 9),
              legend.position = "right",
              legend.title = element_blank(),
              legend.background = element_blank(),
              legend.key = element_blank()
        ) +
        ggtitle(loc)
      print(p)
    } else {
      return(list(ihme_loc_id=loc, me_name=me, cv_admin_bias=shift))
    }
  #-------------------------------
  } else {
    return(list(ihme_loc_id=loc, me_name=me, cv_admin_bias=0))
  }
}

## Function to run through each vacc/country combo and get admin shift
est.admin_bias <- function(df, exceptions) {
  ## Get list of all vacc/country combo
  vacc.country <- df[me_name %in% c("vacc_mcv1", "vacc_dpt3", "vacc_bcg", "vacc_polio3"), .(me_name, ihme_loc_id)] %>% unique
  ## Loop through shifts
  print("Calculating administrative bias across vaccinations")
  bias <- lapply(1:nrow(vacc.country), function(x) {
      me <- vacc.country$me_name[[x]]
      loc <- vacc.country$ihme_loc_id[[x]]
      calc.shift(df, me, loc)
  }) %>% rbindlist
  ## Merge onto df
  bias <- bias[, cv_admin := 1]
  df <- merge(df, bias, by=c("ihme_loc_id", "me_name", "cv_admin"), all.x=TRUE)
  ## Shift administrative data and save preshifted value
  print("Shifting administrative estimates")
  df <- df[cv_admin==1 & !is.na(cv_admin_bias) & !(ihme_loc_id %in% exceptions), cv_admin_orig := data]
  df <- df[cv_admin==1 & !is.na(cv_admin_bias) & !(ihme_loc_id %in% exceptions), data := data + cv_admin_bias]
  return(df)
}

temp.outlier <- function(df) {
  ## EMERGENCY

  ## Systematically outlier WHO_WHS
  df <- df[which(grepl("WHO_WHS", survey_name) & !(nid %in% c(21535))), cv_outlier := data]
  df <- df[which(grepl("WHO_WHS", survey_name) & !(nid %in% c(21535))), data := NA]
  
  ## Systematically outlier SUSENAS
  df <- df[which(grepl("SUSENAS", survey_name)), cv_outlier := data]
  df <- df[which(grepl("SUSENAS", survey_name)), data := NA]
  
  ## Outlier IND/HMIS FOR DPT3
  df <- df[which(grepl("IND/HMIS", survey_name) & me_name=="vacc_dpt3"), cv_outlier := data]
  df <- df[which(grepl("IND/HMIS", survey_name) & me_name=="vacc_dpt3"), data := NA]
  
  ## Outlier MEX INEGI
  df <- df[which(grepl("MEX/INEGI", survey_name) & me_name=="vacc_dpt3"), cv_outlier := data]
  df <- df[which(grepl("MEX/INEGI", survey_name) & me_name=="vacc_dpt3"), data := NA]
  
  ## For RUS/LMS keep only 1 y/o
  df <- df[grepl("RUS/LONGITUDINAL", survey_name) & age_year != 2, cv_outlier := data]
  df <- df[grepl("RUS/LONGITUDINAL", survey_name) & age_year != 2, data := NA]
  
  ## Outlier 1 y/o for MCV from cv_survey==1
  df <- df[cv_survey==1 & age_year==1 & me_name=="vacc_mcv1", cv_outlier := data]
  df <- df[cv_survey==1 & age_year==1 & me_name=="vacc_mcv1", data := NA]
  
}

set.admin_variance <- function(df) {
  ## By country, year, vaccination, count how many non-admin points
  df <- df[, n_survey := length(data[!is.na(data) & cv_survey==1]), by=c("ihme_loc_id", "year_id", "me_name")]
  df <- df[n_survey > 0 & cv_admin==1, variance := data * (1-data)/10]
  df <- df[n_survey==0 & cv_admin==1, variance := data * (1-data)/50]
}

###########################################################################################################################
# SECTION 7: Adjust introduction frame on if data is present prior to official introduction date
###########################################################################################################################

adjust.intro_frame <- function(df) {
  ## Readjust cv_intro_years for modeling utility
  df <- df[, cv_intro_years := ifelse((year_id-(cv_intro-1))>=0, year_id-(cv_intro-1), 0)]
  return(df)
}

###########################################################################################################################
# SECTION 9: Set ROTAC Based on Schedule
###########################################################################################################################

make.rotac <- function(df) {
  sf <- readRDS(vacc.schedule)[, .(ihme_loc_id, doses)]
  ## Cap doses at 3, if doses = 0, set to 2
  sf <- sf[doses > 3, doses := 3]; sf <- sf[doses == 0, doses := 2]
  sf <- sf[, me_name := paste0("vacc_rota", doses)]; sf$doses <- NULL
  sf <- sf[, rota_flag := 1]
  ## Merge on
  df <- merge(df, sf, by=c("ihme_loc_id", "me_name"), all.x=TRUE)
  ## Locations with dose information
  locs.dose <- df[!is.na(rota_flag)]$ihme_loc_id %>% unique
  ## Locations that have introduced rota
  df.intro <- readRDS(vacc.intro)
  locs.intro <- df.intro[grepl("rota", me_name) & cv_intro < 9999]$ihme_loc_id %>% unique
  ## For locations that have no dose information but have introduced rota, assume 2 doses for now
  locs.assume <- setdiff(locs.intro, locs.dose)
  ## Pull out new frame that will be the mapped rotac data
  df.rotac <- df[(rota_flag == 1 | (me_name == "vacc_rota2" & ihme_loc_id %in% locs.assume)) & !is.na(data)]
  df.rotac <- df.rotac[, me_name := "vacc_rotac"]
  ## Append back onto df
  df <- rbind(df, df.rotac)
  ## Clean
  df <- df[, rota_flag := NULL]
  return(df)
}


###########################################################################################################################
# SECTION 10: Create unique id
###########################################################################################################################

create.id <- function(df) {
  
## Create id for cleanliness
cols.id <- c('nid', 'ihme_loc_id', 'year_start', 'year_end', 'year_id', 'survey_name', 'survey_module', 'file_path')
df <- df[, cv_id := .GRP, by=cols.id]
## Look for duplicates
df <- df[, dupe := lapply(.SD, function(x) length(x)-1), .SDcols='cv_id', by=c('cv_id', 'me_name')]
## Drop duplicates from last year from india
ndupe <- nrow(df[which(dupe>0)])
print(paste0("Dropping duplicates by cv_id : ", ndupe, " rows"))
df <- df[!which(dupe > 0)]
## Check for remaining duplicates
if (max(df$dupe) >0 ) stop("Duplicates in meta")
df$dupe <- NULL

return(df)
}

###########################################################################################################################
# SECTION 10: Create ratios for modeling
###########################################################################################################################

make.ratios <- function(df, me) {
  ## Setup
  vaccs <- unlist(strsplit(me, "_"))[2:3]
  num <- paste0("vacc_", vaccs[1])
  denom <- paste0("vacc_", vaccs[2])
  ## Combine frames
  cols <- c("ihme_loc_id", "year_id", "age_group_id", "sex_id", "nid", "cv_id", "cv_admin_orig", "data", "variance", "sample_size")
  df.num <- df[me_name==num, cols, with=F]; df.num$x <- "num"
  df.denom <- df[me_name==denom, cols, with=F]; df.denom$x <- "denom"
  ## Make sure that ratio calculated between admin data is based on pre-bias shift
  df.num <- df.num[!is.na(cv_admin_orig), data := cv_admin_orig]
  df.num$cv_admin_orig <- NULL
  df.denom <- df.denom[!is.na(cv_admin_orig), data := cv_admin_orig]
  df.denom$cv_admin_orig <- NULL
  cols <- setdiff(cols, "cv_admin_orig")
  df.r <- rbind(df.num, df.denom) %>% dcast(., ihme_loc_id + year_id + age_group_id + sex_id + nid + cv_id ~ x, value.var=c("data", "variance", "sample_size"))
  ## Calculate ratios, variance 
  df.r <- df.r[, data := data_num/data_denom]
  df.r <- df.r[, data := ifelse(data >=1, 0.999, data)]
  df.r <- df.r[!is.na(data)]
  df.r <- df.r[, variance := data_num^2/data_denom^2 * (variance_num/data_num^2 + variance_denom/data_denom^2)] ## (http://www.stat.cmu.edu/~hseltman/files/ratio.pdf)
  df.r <- df.r[, sample_size := data * (1 - data) / variance] ## Effective sample size from above variance calculation
  ## Clean
  df.r <- df.r[, cols, with=F]
  df.r <- df.r[, me_name := me]
  return(df.r)
}

###########################################################################################################################
# SECTION 12: Split meta
###########################################################################################################################

split.meta <- function(df) {
  
  ## Create id for cleanliness
  cols.id <- c('nid', 'ihme_loc_id', 'year_start', 'year_end', 'year_id', 'survey_name', 'survey_module', 'file_path')
  cols.meta <- c(cols.id, grep("cv_", names(df), value=TRUE))
  cols.data <- c('me_name', 'data', 'variance', 'sample_size')
  
  ## Split sets
  meta <- df[, cols.meta, with=FALSE] %>% unique
  df.mod <- df[, c("ihme_loc_id", "year_id", "age_group_id", "sex_id", "nid",  "cv_id", "cv_intro", "cv_intro_years",  cols.data), with=FALSE]
  
  return(list(meta=meta, df.mod=df.mod, df=df))
}

##########################################################################################################################
# PREP VACCINATION COVERAGE
#1.) Load tabulated survey data
#2.) Combine WHO Survey Estimates (removing duplicates)
#3.) Add in administrative data
#4.) Outlier data
#5.) Clean data (harmonize cv_* cols, make sure year_id is numeric, offset)
#6.) Calculate out administrative bias
#7.) Set introduction frame
#8.) Clean data and create ratios for multidose
###########################################################################################################################

## 1.) Load data
df.survey <- prep.survey() ## Microdata
df.admin <- readRDS(vacc.admin) ## WHO Admin
df.lit <- readRDS(vacc.lit) ## Lit data
df.lit <- df.lit[, cv_lit := 1]
df <- rbind(df.survey, df.admin, df.lit, fill=TRUE)
df <- df[, year_id := as.numeric(as.character(year_id))]

## 2.) Outlier data using database
df <- outlier.data(df)

## 3.) Other outliers
df <- temp.outlier(df)

## Floor sample size at 20
df <- df[-which(sample_size<20)]

## 5.) Clean data
df <- clean.data(df)

## 6.) Calculate out administrative bias
df <- est.admin_bias(df, exceptions=c("ETH", "UKR"))

## 5.) Clean data
df <- clean.data(df)

## Set admin variance based on survey data availability by country-year-vaccine
df <- set.admin_variance(df)

## Set lit variance where we have sample_size
df <- df[cv_lit==1 & !is.na(sample_size), variance := data*(1-data)/sample_size]

## Set lit variance where we dont have sample_size
df <- df[cv_lit==1 & is.na(variance), variance := data*(1-data)/100]

## Set sample_size based off of variance
df <- df[!is.na(data) & is.na(sample_size), sample_size := data * (1-data) / variance]

## 8.) Clean and save full set
df <- clean.data(df)

## 9.) Set rotac using the scheduled doses
df <- make.rotac(df)

## 10.) Create id to match and 
df <- create.id(df)

## 11.) Make ratios to model out
df <- df[me_name == "vacc_full_sub", me_name := "vacc_fullsub"]
df <- df[me_name == "vacc_dpt3_on_time", me_name := "vacc_dpt3time"]

ratios <- c("vacc_hib3_dpt3_ratio", "vacc_pcv3_dpt3_ratio", "vacc_rotac_dpt3_ratio",
            "vacc_dpt3_dpt1_ratio", "vacc_full_dpt3_ratio", "vacc_fullsub_dpt3_ratio")
df.ratios <- lapply(ratios, function(x) make.ratios(df, x)) %>% rbindlist
df <- rbind(df, df.ratios, fill=TRUE, use.names=TRUE)

## 12.) Set introduction frame
df.intro <- readRDS(vacc.intro)
df <- merge(df, df.intro, by=c("ihme_loc_id", "year_id", "me_name"), all=TRUE)
df <- adjust.intro_frame(df)

df <- clean.data(df)

## 13.) Split meta
split <- split.meta(df)
df <- split$df.mod
df.full <- split$df
meta <- split$meta

#--------------------------------------------
subset <- c("vacc_dpt3", "vacc_mcv1", "vacc_bcg", "vacc_polio3", "vacc_dpt3time", ratios)
df <- df[me_name %in% subset]

## Save full
saveRDS(df.full, paste0(data_root, "FILEPATH/vaccination.rds"))

## Save Meta
saveRDS(meta, paste0(data_root, "FILEPATH/vaccination_meta.rds"))

## Map location_id
df <- merge(df, locs[, .(ihme_loc_id, location_id)], by='ihme_loc_id', all.x=TRUE)
if (nrow(df[is.na(location_id)])>0) stop("Unmapped locations")

## Save
for (i in subset) {
  df.sub <- df[me_name==i] %>% copy
  write.csv(df.sub, paste0(data_root, "FILEPATH", i, ".csv"), row.names=F, na="")
}


















