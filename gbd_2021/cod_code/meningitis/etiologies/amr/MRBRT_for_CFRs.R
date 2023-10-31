##################################################
## Project: Antimicrobial Resistance - Generalizable MRBRT CFR Models
## Script purpose: Calculate case fatality rates for
##                 infectious syndromes by pathogen
## Date: 11/2/2020
## Author: Lucien Swetschinski
##################################################
##### R Initialization and Functions #####
hdrive <- '/ihme/homes/lswets/'
setwd(hdrive)
rm(list = ls())
pacman::p_load(data.table, openxlsx, dplyr, ggplot2, gridExtra, argparse)
library(reticulate)
library(crosswalk, lib.loc = "/ihme/code/mscm/R/packages/")
library(mrbrt001, lib.loc = "/ihme/code/mscm/R/packages/")
library(reshape2)
library(stringr)
source("/ihme/cc_resources/libraries/current/r/get_age_spans.R")

# function to convert logits to probabilities
logit2prob <- function(logit){
  odds <- exp(logit)
  prob <- odds / (1 + odds)
  return(prob)
}

# function for one hot encoding
one_hot_encode <- function(df, col) {
  stopifnot(is.data.table(df))
  stopifnot(class(df[, get(col)]) == 'factor')
  
  # Create a copy of the df to work with
  df_copy <- copy(df)
  df_copy[, uniq_row := seq(1, .N)]
  one_hot <- as.data.table(dcast(
    df_copy, formula(paste0("uniq_row ~ ", col)),
    length
  ))
  stopifnot(ncol(one_hot) == length(unique(df[, get(col)])) + 1)
  stopifnot(unique(
    one_hot[, rowSums(.SD),
            .SDcols = names(one_hot)[names(one_hot) != "uniq_row"]]
  ) == 1)
  
  reference_level <- names(one_hot)[names(one_hot) != "uniq_row"][1]
  one_hot[, c("uniq_row", reference_level) := NULL]
  names(one_hot) <- sapply(names(one_hot), function(x) {paste0(col, x)})
  df <- cbind(df, one_hot)
  return(list("df" = df, "cols" = names(one_hot)))
}

get_mode <- function(x){
  return(names(sort(table(x), decreasing = T, na.last = T)[1]))
}

unencode_col <- function(df, col) {
  encoded_cols = inputdata$generated_cols$covariate[inputdata$generated_cols$origcol == col]
  df[rowSums(df[, encoded_cols]) == 0, col] <- paste0(col, covs$reference[covs$covariate == col])
  df[is.na(eval(parse(text = (paste0("df$", col))))), col] <- 
    names(df[is.na(eval(parse(text = (paste0("df$", col))))), encoded_cols])[max.col(
      df[is.na(eval(parse(text = (paste0("df$", col))))), encoded_cols])]
  df[,col] <- sub(col, '', eval(parse(text = (paste0("df$", col)))))
  return (df)
}

wilson_std_err <- function(cf, sample_size){
  cf_component = cf * (1 - cf)
  return(
    (sqrt((cf_component / sample_size) + ((1.96**2) / (4 * sample_size ** 2))) / (1 + 1.96**2 / sample_size))
  )
}

agg_ages <- function (df, agg_ages, outcol = 'agg_age'){
  ages = get_age_spans()
  agesindf = unique(df$age_group_id)
  for (age_id in agg_ages) {
    start = ages$age_group_years_start[ages$age_group_id == age_id]
    end = ages$age_group_years_end[ages$age_group_id == age_id]
    subids = ages$age_group_id[ages$age_group_years_start >= start & ages$age_group_years_end <= end]
    df[df$age_group_id %in% subids, outcol] <- age_id
    agesindf = agesindf[!agesindf %in% subids]
  }
  if (length(agesindf) > 0){
    warning(paste0('There are age group ids (', agesindf, ') that cannot be nicely mapped to the specified aggregate ages'))
  }
  return (df)
}

year_categorizer <- function(df, yrcol, yrcat_string){
  yrcats = lapply(strsplit(as.character(yrcat_string), ", "), as.numeric)[[1]]
  yrcats = c(min(df[, yrcol]), yrcats, max(df[, yrcol])+1)
  df$yrcat <- cut(df[[yrcol]], breaks = yrcats, right = FALSE, dig.lab = 4)
  df$yrcat <- gsub("\\[", "", df$yrcat)
  df$yrcat <- gsub("\\)", "", df$yrcat)
  df$yrcat <- gsub(",", "_", df$yrcat)
  df$yrcat <- gsub(max(df[, yrcol])+1, max(df[, yrcol]), df$yrcat)
  
  return (df)
}

#### Set Synd and Model ####
usermods <- c()
model_phase <- "PCV_and_HIB_updates"
useRE <- FALSE
takedraws <- TRUE
flu_rsv <- TRUE

if (is.na(usermods)){
  mods <- c('blood_stream_infectious-intercept_adult2', 'blood_stream_infectious-interaction_adult2',
            'blood_stream_infectious-other_adult', 'blood_stream_infectious-intercept_neonatal2',
            'blood_stream_infectious-other_neonatal', 'bone_joint_infection-intercept2',
            'bone_joint_infection-other', 'cns_infectious-interaction2',
            'cns_infectious-intercept2', 'cns_infectious-other', 
            'diarrhea-intercept2','diarrhea-other', 
            'peritoneal_and_intra_abdomen_infectious-intercept2',
            'peritoneal_and_intra_abdomen_infectious-other', 'respiratory_infectious-intercept_comm2',
            'respiratory_infectious-other_comm2', 
            'respiratory_infectious-intercept_hosp2', 'respiratory_infectious-other_hosp',
            'skin_infectious-intercept2', 'skin_infectious-other', 
            'uti_plus-intercept_comm2', 'uti_plus-other_comm', 
            'uti_plus-intercept_hosp2', 'uti_plus-other_hosp') 
} else {
  mods <- usermods
}

# loop over each model
for (mod in mods){
  inf_synd <- str_split(mod, "[-]")[[1]][1]
  model_id <- str_split(mod, "[-]")[[1]][2]
  
  ##### Model Initialization and Functions #####
  subset <- ifelse(grepl('comm', model_id) == TRUE, 'community',
                   ifelse(grepl('hosp', model_id) == TRUE, 'hospital',
                          ifelse(grepl('adult', model_id) == TRUE, 'adult',
                                 ifelse(grepl('neonatal', model_id) == TRUE, 'neonatal', NA))))
  mntintermed <- paste0('/mnt/team/amr/priv/intermediate_files/03b_cfr/', inf_synd, '/')
  mntresult <- paste0('/mnt/team/amr/priv/model_results/03b_cfr/', inf_synd, '/')
  if (is.na(subset)){
    drawpath <- paste0(mntresult, "draws/")
  } else {
    drawpath <- paste0(mntresult, "draws/", subset, "/")
  }
  if (is.na(subset)){
    outpath <- paste0('/mnt/team/amr/priv/model_results/03b_cfr/diagnostics/', model_phase, '/', inf_synd, '/')
  } else {
    outpath <- paste0('/mnt/team/amr/priv/model_results/03b_cfr/diagnostics/', model_phase, '/', inf_synd, '/', subset, '/')
  }
  diagplotpath <- paste0(outpath, 'diagnostic_plots/')
  outpathdetail <- paste0(outpath, inf_synd, '_', model_id, '/')
  
  #if there's no directory output path, create one
  if(!dir.exists(outpath)){
    dir.create(outpath, recursive = TRUE)
  }
  
  if(!dir.exists(diagplotpath)){
    dir.create(diagplotpath, recursive = TRUE)
  }
  
  if(!dir.exists(outpathdetail)){
    dir.create(outpathdetail, recursive = TRUE)
  }
  
  # Read in global parameters and subset to model
  globparams <- read.xlsx("amr/modelling/3b_cfr/config/infectious_synd_CFR_by_pathogen_config.xlsx", sheet = 1)
  globparams <- globparams[globparams$infectious_syndrome == inf_synd & globparams$model_id == model_id,]
  # Read in covariates and subset to model
  covs <- read.xlsx("amr/modelling/3b_cfr/config/infectious_synd_CFR_by_pathogen_config.xlsx", sheet = 2)
  covs <- covs[covs$infectious_syndrome == inf_synd & covs$model_id == model_id,]
  
  
  # determine the evaluation type:
  # complete - generate estimates for each pathogen estimated in the model
  # partial - generate estimates for only some of the pathogens estimated in the model,
  # perhaps because we are leveraging data for data rich pathogens to estimate 
  # data sparse pathogens but have a more sophisticated model for the data rich pathogens
  # other - a catch-all model for all pathogens without the same pathogen names
  # as those pathogens in the data
  determine_eval_type <- function(global_parameters){
    if (is.na(global_parameters$eval_pathogens) |
        identical(unique(str_split(global_parameters$eval_pathogens, ", ")[[1]]), 
                  unique(str_split(global_parameters$model_pathogens, ", ")[[1]]))){
      eval_type <- 'complete'
    }
    
    if (identical(unique(str_split(global_parameters$eval_pathogens, ", ")[[1]]), 
                  unique(str_split(global_parameters$model_pathogens, ", ")[[1]])) == FALSE &
        global_parameters$eval_pathogens != 'other' &
        !is.na(global_parameters$eval_pathogens)){
      eval_type <- 'partial'
    }
    
    if (!is.na(global_parameters$eval_pathogens) &
        global_parameters$eval_pathogens == 'other'){
      eval_type <- 'other'
    }
    
    if (!exists('eval_type')){
      stop("Cannot detect evaluation type. Check that global parameters are properly entered/configurated")
    }
    
    return (eval_type)    
  }
  
  eval_type <- determine_eval_type(globparams)
  
  ##### Process Data #####
  cfrdat <- as.data.table(read.csv(file = paste0(mntintermed, "cfr_data_vaxadd.csv"), stringsAsFactors = FALSE))
  
  hospds <- c("AUT_HDD", "BRA_SIH", "CAN_DAD", "ITA_HID", "MEX_SAEH", "NZL_NMDS", "USA_NHDS", "USA_SID")
  
  # convert hai/cai designation for hospital data, CAI if infectious_syndrome = main_diagnosis
  # HAI otherwise
  cfrdat$hosp[cfrdat$source %in% hospds] <- case_when(cfrdat$infectious_syndrome[cfrdat$source %in% hospds] == cfrdat$main_diagnosis[cfrdat$source %in% hospds] ~ 'community',
                                                      TRUE ~ 'hospital')
  if (flu_rsv & inf_synd == 'respiratory_infectious'){
    cfrdat <- cfrdat[cfrdat$pathogen != 'flu',]
    cfrdat$hosp[cfrdat$pathogen == 'rsv'] <- 'community'
    
    flu_pneumo <- read.csv(file = paste0(mntintermed, "flu_pneumonia_data_split_jan25.csv"), stringsAsFactors = FALSE)
    cfrdat <- rbind(cfrdat, flu_pneumo, fill = TRUE)
    
    # fill NA PCV and Hib cols from flu data
    cfrdat$PCV3_coverage_prop[is.na(cfrdat$PCV3_coverage_prop)] <- 0
    cfrdat$Hib3_coverage_prop[is.na(cfrdat$Hib3_coverage_prop)] <- 0
  }
  
  cfrdat <- agg_ages(cfrdat, lapply(strsplit(globparams$age_categories, ", "), as.numeric)[[1]], outcol = 'age_group_id')
  
  if(!globparams$hosp & !globparams$by_sex){
    cfrsum <- cfrdat %>% group_by_at(c('source', 'location_id', 'year_id', 'pathogen', 'age_group_id', 'ICU',
                                       covs$covariate[covs$covariate_type == 'continuous'])) %>%
      summarize(
        cases = sum(cases),
        deaths = sum(deaths)
      )
  } else if (globparams$hosp & !globparams$by_sex) {
    cfrsum <- cfrdat %>% group_by_at(c('source', 'location_id', 'year_id', 'pathogen',
                                       'age_group_id', 'hosp', 'ICU',
                                       covs$covariate[covs$covariate_type == 'continuous'])) %>%
      summarize(
        cases = sum(cases),
        deaths = sum(deaths)
      )
  } else if (!globparams$hosp & globparams$by_sex) {
    cfrsum <- cfrdat %>% group_by_at(c('source', 'location_id', 'year_id', 'pathogen',
                                       'age_group_id', 'sex_id', 'ICU',
                                       covs$covariate[covs$covariate_type == 'continuous'])) %>%
      summarize(
        cases = sum(cases),
        deaths = sum(deaths)
      )
  } else if (globparams$hosp & globparams$by_sex) {
    cfrsum <- cfrdat %>% group_by_at(c('source', 'location_id', 'year_id', 'pathogen',
                                       'age_group_id', 'hosp', 'sex_id', 'ICU',
                                       covs$covariate[covs$covariate_type == 'continuous'])) %>%
      summarize(
        cases = sum(cases),
        deaths = sum(deaths)
      )
  }
  
  cfrsum$deaths <- round(cfrsum$deaths, 7)
  cfrsum$cases <- round(cfrsum$cases, 7)
  cfrsum$deaths[cfrsum$deaths > cfrsum$cases] <- cfrsum$cases[cfrsum$deaths > cfrsum$cases]
    
  # remove entries with <5 cases and 0 deaths
  table(cfrsum$pathogen[cfrsum$cases < 5 & cfrsum$deaths == 0])
  cfrsumclip <- cfrsum[!(cfrsum$cases < 5 & cfrsum$deaths == 0),]
  
  cfradj <- cfrsumclip[!round(cfrsumclip$cases, 2) == 0,]
  
  # add small amount to deaths when deaths=0, subtract small amount from deaths when deaths=cases
  # prevents -Inf and Inf logits  
  cfradj$deaths <- case_when(cfradj$deaths == 0 ~ cfradj$cases/100,
                             cfradj$deaths == cfradj$cases ~ cfradj$cases - cfradj$cases/100,
                             TRUE ~ cfradj$deaths)
  
  # drop negative deaths, occur when cases and deaths both very small and equal (b/c of age splitting)
  # after subtracting the offset, deaths become negative
  cfrsumadj <- as.data.table(cfradj)
    
  # calculate CFR and SE using Wilson's interval
  cfrsumadj[, cfr := deaths/cases]
  
  cfrsumadj$cfr_se <- wilson_std_err(cfrsumadj$cfr, cfrsumadj$cases)
  
  cfrsumadj[, cfr_se_binom := sqrt(((deaths/cases)*(1-(deaths/cases)))/(cases))]
  
  cfrsumadj <- as.data.frame(cfrsumadj)
  
  # for each row, calculate proportion of pathogen cases represented by that row
  # for plot, below
  cfrsumadj <- cfrsumadj %>% group_by(pathogen) %>%
    mutate(propadmit = cases/sum(cases))
  
  # use delta method to get logit standard errors
  cfrsumadj <- as.data.table(cfrsumadj) %>% 
    cbind(delta_transform(cfrsumadj$cfr, cfrsumadj$cfr_se_binom, "linear_to_logit"))
  
  # format crude age and year dummies
  cfrsumadj <- as.data.table(cfrsumadj)
  cfrsumadj$age_group_id <- factor(cfrsumadj$age_group_id, levels = strsplit(globparams$age_categories, ", ")[[1]])
  cfrsumadj$yrcat <- as.factor(cfrsumadj$yrcat)
  cfrsumadj$yrcat <- as.factor(cfrsumadj$yrcat)
  cfrsumadj$ICU <- as.factor(cfrsumadj$ICU)
  cfrsumadj$hosp[cfrsumadj$source == 'inicc'] <- 'unknown'
  cfrsumadj$hosp <- as.factor(cfrsumadj$hosp)
  
  inputdata <- cfrsumadj
  inputdata <- inputdata[inputdata$pathogen %in% str_split(globparams$model_pathogens, ", ")[[1]],]
  
  inputdata$pathogen <- factor(inputdata$pathogen)
  inputdata$study_id <- inputdata[[globparams$study_id]]
  
  # zero out all instances of a covariate that should not apply to certain pathogens
  if (length(which(!is.na(covs$specific_pathogen))) > 0){
    for (cov in covs$covariate[!is.na(covs$specific_pathogen)]){
      pathogens <- str_split(covs$specific_pathogen[covs$covariate == cov], ',')[[1]]
      inputdata[!inputdata$pathogen %in% pathogens, cov] <- 0
    }
  }
  
  if (globparams$hosp & !is.na(globparams$hosp_val)){
    inputdata <- inputdata[inputdata$hosp %in% c(globparams$hosp_val, 'unknown'),]
  }
  
  if (globparams$by_sex & !is.na(globparams$sex_val)){
    inputdata <- inputdata[inputdata$sex_id == globparams$sex_val,]
  }
  
  if (!is.na(globparams$age_subset)){
    inputdata <- inputdata[inputdata$age_group_id %in% str_split(globparams$age_subset, ", ")[[1]],]
  }
  
  # spot fix ICU hospital data to type 'mixed
  inputdata$ICU[inputdata$ICU == ''] <- 'mixed'
  inputdata$ICU <- factor(inputdata$ICU)
  
  # drop India data for CNS, has underreporting
  if (inf_synd == 'cns_infectious'){
    inputdata <- inputdata[!inputdata$study_id %in% c('373299', '424258'),]
  }
  
  npaths <- length(unique(inputdata$pathogen))
  nages <- length(unique(inputdata$age_group_id))
  if (grepl('interaction', model_id) == TRUE){
    nparams <- nages-1 + 1 + 1 + (npaths-1)*2
  } else if (grepl('intercept', model_id) == TRUE){
    nparams <- nages-1 + 1 + 1 + npaths-1
  } else if (grepl('other', model_id) == TRUE){
    nparams <- nages-1 + 1 + 1
  }
  #####
  
  print(paste0('N Samples: ', nrow(inputdata)))
  print(paste0('N Params: ', nparams))
  ##### Input Data Diagnostics ##### 
  
  datasummary <- group_by(inputdata, pathogen) %>%
    summarize(
      cases = sum(cases),
      datapoints = length(pathogen),
      datapoints_lo_SD = length(pathogen[sd_logit < 2]),
      datapoints_lo_haq = length(pathogen[haqi <= 0.7]),
      datapoints_lo_haq_lo_SD = length(pathogen[haqi <= 0.7 & sd_logit < 2]),
      cfr = sum(deaths)/sum(cases))
  
  if(eval_type != 'other'){
    ggplot(inputdata[inputdata$pathogen %in% str_split(globparams$eval_pathogens, ", ")[[1]],]) +
      geom_point(aes(x = haqi, y = cfr, color = pathogen, size = propadmit), alpha = 0.5) +
      scale_size(guide = 'none') +
      geom_smooth(aes(x = haqi, y = cfr, weight = 1/sd_logit), size = 0.5, alpha = 0.8, method = "lm", se = FALSE) +
      lims(y = c(0, 1)) +
      theme(legend.position = "none") +
      facet_wrap(~pathogen)
    ggsave(paste0(diagplotpath, model_id, '_INPUTDATA.png'), height = 9, width = 14)
  }
  
  if(eval_type != 'other'){
    for (categorical in covs$covariate[covs$covariate_type == 'category' & covs$covariate != 'pathogen']){
      cfrplot <- ggplot(inputdata) +
        geom_boxplot(aes(x = eval(parse(text = paste(categorical))), y = cfr)) +
        xlab(paste(categorical))
      haqiplot <- ggplot(inputdata) +
        geom_boxplot(aes(x = eval(parse(text = paste(categorical))), y = haqi)) +
        xlab(paste(categorical))
      ggsave(paste0(diagplotpath, model_id, categorical, '_HAQICFR_association.png'), arrangeGrob(cfrplot, haqiplot, nrow = 1),
             height = 9, width = 16)
    }
  }
  
  if (grepl('interaction', model_id) & length(datasummary$pathogen[datasummary$datapoints_lo_haq_lo_SD < 10])){
    stop(paste0("Pathogen has too few high confidence data points to run in interaction: ",
                    as.character(datasummary$pathogen[datasummary$datapoints_lo_haq_lo_SD < 10]), '\n'))
  }
  
  if (grepl('intercept', model_id) & length(datasummary$pathogen[datasummary$datapoints_lo_SD < 10])){
    warning(paste0("Pathogen has few high confidence datapoints, consider modelling as 'other': ",
                as.character(datasummary$pathogen[datasummary$datapoints_lo_SD < 10]), '\n'))
  }
  
  if (any(grepl('DETERMINE_FROM_DATA', covs$args))){
    determine_cols <- covs$covariate[grepl('DETERMINE_FROM_DATA', covs$args)]
    for (determine_col in determine_cols){
      mod <- lm(cfr ~ eval(parse(text = paste(determine_col))), weights = 1/cfr_se, data = inputdata)
      prior_est <- as.numeric(mod$coefficients['eval(parse(text = paste(determine_col)))'])
      covs$args[covs$covariate == determine_col] <- gsub('DETERMINE_FROM_DATA',
                                                         paste0('array(c(', prior_est, ', 0.05))'),
                                                         covs$args[covs$covariate == determine_col])
    }
  }
  
  if (grepl('intercept', model_id)){
    if (is.na(subset)){
      write.csv(datasummary, 
                file = paste0(outpath, inf_synd, '_datasummary.csv'), row.names = FALSE)
    } else {
      write.csv(datasummary, 
                file = paste0(outpath, inf_synd, '_', subset, '_datasummary.csv'), row.names = FALSE)
    }
  }
  
  ##### Generalized code #####
  
  # track columns generated in one hot encoding/interaction so they can be called in the future
  generated_cols <- as.data.frame(matrix(nrow = 0, ncol = 2))
  colnames(generated_cols) <- c("generatedcol", "origcol")
  
  # track everything that interacts with pathogen, which is necessary for the plotting step
  pathogen_ints <- as.data.frame(matrix(nrow = 0, ncol = 3))
  colnames(pathogen_ints) <- c("pathogen", "intcol", "interacted")
  
  encode_and_interact <- function(df, #data to encode/interact
                                  covariates #subset covariate data from config file
  ) {
    # One hot encode factor variables
    for(categorical in covariates$covariate[covariates$covariate_type == "category"]){
      print(paste("One hot encoding", categorical))
      # if there is a reference level specified, use
      if(!is.na(covariates$reference[covariates$covariate == categorical])){
        df[, categorical] <- relevel(eval(parse(text = (paste0("df$", categorical)))), 
                                     ref = covariates$reference[covariates$covariate == categorical])
      }
      # if there is no reference level specified, use the most common entry from the data
      else{
        df[, categorical] <- relevel(eval(parse(text = (paste0("df$", categorical)))), 
                                     ref = get_mode(eval(parse(text = (paste0("df$", categorical))))))
      }
      encoded <- one_hot_encode(df, categorical)
      df <- encoded$df
      
      # track columns added by one hot encoding thru the generated_cols df
      addcols <- as.data.frame(matrix(nrow = length(encoded$cols), ncol = 2))
      colnames(addcols) <- c("generatedcol", "origcol")
      addcols$generatedcol <- encoded$cols
      addcols$origcol <- categorical
      generated_cols <- bind_rows(generated_cols, addcols)
    }
    
    # create categorial:continuous interaction inputs in the data
    for(catcontint in covariates$covariate[covariates$covariate_type == "catcont_interaction"]){
      categorical <- str_split(catcontint, ":")[[1]][1]
      continuous <- str_split(catcontint, ":")[[1]][2]
      categories <- levels(eval(parse(text = (paste0("df$", categorical)))))
      if(!is.na(covariates$reference[covariates$covariate == categorical])){
        categories <- categories[!categories == covariates$reference[covariates$covariate == categorical]]
      }
      else{
        categories <- categories[!categories == get_mode(eval(parse(text = (paste0("df$", categorical)))))]
      }
      
      # for each category that isn't the reference, multiply the one-hot encoded values by the 
      # continuous interacted column and store results as new column
      for(category in categories){
        df[, paste0(category, "_", continuous)] <- 
          eval(parse(text = (paste0("df$", categorical, category)))) * eval(parse(text = (paste0("df$", continuous))))
      }
      
      # track columns added by interacting categorical and continuous variables thru the generated_cols df
      addcols <- as.data.frame(matrix(nrow = length(categories), ncol = 2))
      colnames(addcols) <- c("generatedcol", "origcol")
      addcols$generatedcol <- paste0(categories, "_", continuous)
      addcols$origcol <- catcontint
      generated_cols <- bind_rows(generated_cols, addcols)
      
      # track everything that interacts with pathogen, which is necessary for the plotting step
      pathintcols <- as.data.frame(matrix(nrow = length(categories), ncol = 3))
      colnames(pathintcols) <- c("pathogen", "intcol", "interacted")
      pathintcols$pathogen <- categories
      pathintcols$intcol <- paste0(categories, "_", continuous)
      pathintcols$interacted <- continuous
      pathogen_ints <- bind_rows(pathogen_ints, pathintcols)
    }
    
    return(list("data" = df, "generated_cols" = generated_cols, "pathogen_ints" = pathogen_ints))
    
  }
  
  inputdata <- encode_and_interact(inputdata, covariates = covs)
  
  # get all columns in model ready form
  inputdata$generated_cols <- merge(inputdata$generated_cols, covs[, c("covariate", "covariate_type")], by.x = "origcol", by.y = "covariate")
  inputdata$generated_cols <- rename(inputdata$generated_cols, covariate = generatedcol)
  
  modelcols <- bind_rows(covs[covs$covariate_type %in% c("intercept", "continuous"), c("covariate", "covariate_type", "args")],
                     inputdata$generated_cols[, c("covariate", "covariate_type")])
  
  # if there is an intercept and we should use a random effect, designate that for the model
  if (sum(modelcols$covariate == 'intercept') == 1 & globparams$use_re){
    modelcols$modelform[modelcols$covariate == 'intercept'] <- 
      paste0("LinearCovModel(\"", modelcols$covariate[modelcols$covariate == 'intercept'],
             "\", use_re = ", globparams$use_re, ", prior_gamma_gaussian = array(c(0, 0.001)))")
  } else {
    modelcols$modelform <- NA
  }
  
  modelcols$modelform[is.na(modelcols$modelform) & is.na(modelcols$args)] <- 
    paste0("LinearCovModel(\"", modelcols$covariate[is.na(modelcols$modelform) & is.na(modelcols$args)], "\")")
  
  modelcols$modelform[is.na(modelcols$modelform) & !is.na(modelcols$args)] <- 
    paste0("LinearCovModel(\"", modelcols$covariate[is.na(modelcols$modelform) & !is.na(modelcols$args)], "\", ", 
           modelcols$args[is.na(modelcols$modelform) & !is.na(modelcols$args)], ")")
  
  # Initialize MRData object
  mrbrtdata <- MRData()
  
  mrbrtdata$load_df(
    data = inputdata$data,  col_obs = "mean_logit", col_obs_se = "sd_logit",
    col_covs = modelcols$covariate[!modelcols$covariate %in% "intercept"],
    col_study_id = 'study_id')
  
  # specify MR BRT model structure
  modelchunk <- paste("    ", modelcols$modelform, collapse = ",\n")
  
  mrbrtschema <- paste0(
    "MRBRT(
    data = mrbrtdata,
    cov_models = list(\n",
    modelchunk,
    "))"
    )

  # initialize MR BRT model structure and fit the model
  mrbrt_mod <- eval(parse(text = paste(mrbrtschema)))
  mrbrt_mod$fit_model(inner_print_level = 5L, inner_max_iter = 50000L)
  
  #generate beta summary
  mrbrt_results <- as.data.frame(matrix(nrow = length(names(mrbrt_mod$fe_soln)), ncol = 2))
  colnames(mrbrt_results) <- c('covariate', 'beta')
  mrbrt_results$covariate <- names(mrbrt_mod$fe_soln)
  mrbrt_results$beta <- unlist(mrbrt_mod$fe_soln, use.names = FALSE)
  write.csv(mrbrt_results, file = paste0(outpath, 'betas_', inf_synd, '_', model_id, '.csv'), row.names = FALSE)
  
  # plot variables of interest
  plotvars <- str_split(globparams$plot_vars, ", ")[[1]]
  
  if (eval_type == 'other'){
    reference_pathogen <- 'other'
  } else {
    reference_pathogen <- ifelse(!is.na(covs$reference[covs$covariate == "pathogen"]),
                                 covs$reference[covs$covariate == "pathogen"],
                                 get_mode(inputdata$data$pathogen))
  }
  
  for (plotvar in plotvars){
    # for each plotting variable, generate a prediction template with 100 increments from minimum
    # to maximum of the continuous plotting variable of interest
    bugframe <- as.data.frame(matrix(ncol = 1, nrow = 100))
    colnames(bugframe) <- plotvar
    if (plotvar == 'haqi'){
      bugframe[, plotvar] <- seq(0.2, max(eval(parse(text = paste0("inputdata$data$", plotvar)))),
                                 length.out = 100)
    } else{
    bugframe[, plotvar] <- seq(min(eval(parse(text = paste0("inputdata$data$", plotvar)))),
                    max(eval(parse(text = paste0("inputdata$data$", plotvar)))),
                    length.out = 100)
    }
    # set the pathogen for this prediction template as the reference pathogen
    bugframe$pathogen <- reference_pathogen
    
    # then add a filler "ceteris paribus" value for all other covariates in the model,
    # 0 for all categorical/interaction covariates, the median for all continuous covariates
    for (modcol in modelcols$covariate[!modelcols$covariate %in% c("intercept", plotvar)]){
      if(modelcols$covariate_type[modelcols$covariate == modcol] == "continuous"){
        bugframe[, modcol] <- median(eval(parse(text = paste0("inputdata$data$", modcol))))
      }
      else{
        bugframe[, modcol] <- 0
      }
    }
    
    frame_temp <- bugframe
    
    # if we facet by anything, duplicate this prediction template for each category of the
    # facet (setting that category's dummy to 1)
    if (!is.na(globparams$facet)){
      facetcols = inputdata$generated_cols$covariate[inputdata$generated_cols$origcol == globparams$facet]
      for (facet in facetcols){
        bugframe_app <- frame_temp
        bugframe_app[, facet] <- 1
        bugframe <- bind_rows(bugframe, bugframe_app)
      }
      frame_temp <- bugframe
    }
    
    # for each bug analyzed, duplicate the prediction template, setting that bug's dummy to 1
    # and all associated pathogen-interactions to the value of the continuous covariate
    # this procedure is only done when the evaluated pathogen is not 'other'
    if (eval_type != 'other'){
      for (bug in unique(inputdata$data$pathogen[!inputdata$data$pathogen == reference_pathogen])){
        bugframe_app <- frame_temp
        bugframe_app[, paste0("pathogen", bug)] <- 1
        bugframe_app$pathogen <- bug
        
        if (length(inputdata$pathogen_ints$intcol[inputdata$pathogen_ints$pathogen == bug & inputdata$pathogen_ints$interacted == plotvar] > 0)){
          interactcol <- inputdata$pathogen_ints$intcol[inputdata$pathogen_ints$pathogen == bug & inputdata$pathogen_ints$interacted == plotvar]
          bugframe_app[, interactcol] <- bugframe_app[, plotvar]
        }
        bugframe <- bind_rows(bugframe, bugframe_app)
      }
    }
    
    if(globparams$pred_for_study){
      bugframe$study_id <- globparams$study_to_pred
    } else {
      bugframe$study_id <- "NA"
    }
     
    
    # configure prediction template in MRData format
    plotmrbrt_dat <- MRData()
    if (useRE){
      plotmrbrt_dat$load_df(
        data = bugframe,
        col_covs = modelcols$covariate[!modelcols$covariate %in% "intercept"],
        col_study_id = 'study_id')
    } else {
      plotmrbrt_dat$load_df(
        data = bugframe,
        col_covs = modelcols$covariate[!modelcols$covariate %in% "intercept"])
    }
    
    plotmrbrt <- plotmrbrt_dat$to_df()
    
    # generate predictions
    plotmrbrt$predict <- logit2prob(as.vector(mrbrt_mod$predict(data = plotmrbrt_dat, 
                                                                predict_for_study = useRE)))
    
    # plot without facetting
    if (is.na(globparams$facet)){
      
      # if pathogen is a covariate, unencode that column, otherwise set pathogen to 'other'
      if (eval_type == 'other'){
        plotmrbrt$pathogen <- 'other'
      } else {
        plotmrbrt <- unencode_col(plotmrbrt, 'pathogen')
      }
      
      ggplot(plotmrbrt) +
        geom_line(aes(x = eval(parse(text = paste(plotvar))), y = predict, color = pathogen), size = 2, alpha = 0.7) +
        guides(color=guide_legend(nrow=ceiling(length(unique(inputdata$data$pathogen))/4),byrow=TRUE)) +
        labs(y = "Case Fatality Rate", title = paste0("Case Fatality Rate : ", plotvar, " for ", inf_synd, " pathogens"),
             x = plotvar) +
        theme_bw() +
        theme(axis.title = element_text(size =15), axis.text = element_text(size =13),
              legend.text = element_text(size = 15), legend.position = ("bottom"),
              legend.title = element_blank())
      ggsave(filename = paste0(outpathdetail, "unfaceted_plot_of_", plotvar, ".png"), height = 9, width = 12)
      
      # save point predictions
      pointpreds <- plotmrbrt[, c("pathogen", plotvar, "predict")]
      pointpreds$model <- globparams$model_id
      write.csv(pointpreds, file = paste0(outpathdetail, "point_predictions_by_", plotvar, ".csv"), row.names = FALSE)
    }
    
    # plot with facets two ways, facet by facet, facet by pathogen
    if (!is.na(globparams$facet)){
      facet <- globparams$facet
      
      # if pathogen is a covariate, unencode that column, otherwise set pathogen to 'other'
      if (eval_type == 'other'){
        plotmrbrt$pathogen <- 'other'
      } else {
        plotmrbrt <- unencode_col(plotmrbrt, 'pathogen')
      }
      plotmrbrt <- unencode_col(plotmrbrt, facet)
      
      ggplot(plotmrbrt) +
        geom_line(aes(x = eval(parse(text = paste(plotvar))), y = predict, color = pathogen), size = 2, alpha = 0.7) +
        guides(color=guide_legend(nrow=2,byrow=TRUE)) +
        labs(y = "Case Fatality Rate", title = paste0("Case Fatality Rate : ", plotvar, " for ", inf_synd, " pathogens"),
             x = plotvar) +
        theme_bw() +
        theme(axis.title = element_text(size =15), axis.text = element_text(size =13),
              legend.text = element_text(size = 15), legend.position = ("bottom"),
              legend.title = element_blank()) +
        facet_wrap(~eval(parse(text = paste(facet))))
      ggsave(filename = paste0(outpathdetail, plotvar, "_faceted_plot_by_", globparams$facet, ".png"), height = 9, width = 12)
      
      ggplot(plotmrbrt) +
        geom_line(aes(x = eval(parse(text = paste(plotvar))), y = predict, color = eval(parse(text = paste(facet)))), size = 2, alpha = 0.7) +
        guides(color=guide_legend(nrow=2,byrow=TRUE)) +
        labs(y = "Case Fatality Rate", title = paste0("Case Fatality Rate : ", plotvar, " for ", inf_synd, " pathogens"),
             x = plotvar) +
        theme_bw() +
        theme(axis.title = element_text(size =15), axis.text = element_text(size =13),
              legend.text = element_text(size = 15), legend.position = ("bottom"),
              legend.title = element_blank()) +
        facet_wrap(~pathogen)
      ggsave(filename = paste0(outpathdetail, plotvar, "_faceted_plot_by_pathogen.png"), height = 9, width = 12)
      
      # save point predictions
      pointpreds <- plotmrbrt[, c("pathogen", plotvar, facet, "predict")]
      pointpreds$model <- globparams$model_id
      write.csv(pointpreds, file = paste0(outpathdetail, "point_predictions_by_", plotvar, ".csv"), row.names = FALSE)
    }
  }
  ##### Prediction Diagnostics #####
  predhaqi <- mrbrtdata$to_df()
  predhaqi$pred <- mrbrt_mod$predict(data = mrbrtdata, 
                                 predict_for_study = TRUE)
  predhaqi <- unencode_col(predhaqi, 'pathogen')
  
  predhaqi$cfr_pred <- logit2prob(predhaqi$pred)
  if(eval_type != 'other'){
    ggplot(predhaqi[predhaqi$pathogen %in% str_split(globparams$eval_pathogens, ", ")[[1]],]) +
      geom_point(aes(x = haqi, y = cfr_pred, color = pathogen), alpha = 0.5) +
      scale_size(guide = 'none') +
      geom_smooth(aes(x = haqi, y = cfr_pred, weight = 1/obs_se), size = 0.5, alpha = 0.8, method = "lm", se = FALSE) +
      lims(y = c(0, 1)) +
      theme(legend.position = "none") +
      facet_wrap(~pathogen)
    ggsave(paste0(diagplotpath, model_id, '_PREDICTIONS.png'), height = 9, width = 14)
  }
  
##### Point Predictions and Draws for Pathogen Distribution ####
pred_template <- read.csv(file = paste0(mntintermed, inf_synd, '_', model_id, "/prediction_template.csv"))

if (globparams$hosp & !is.na(globparams$hosp_val)){
  pred_template <- pred_template[pred_template$hosp  == globparams$hosp_val,]
}
  
# zero out all instances of a covariate that should not apply to certain pathogens
if (length(which(!is.na(covs$specific_pathogen))) > 0){
  for (cov in covs$covariate[!is.na(covs$specific_pathogen)]){
    pathogens <- str_split(covs$specific_pathogen[covs$covariate == cov], ',')[[1]]
    pred_template[!pred_template$pathogen %in% pathogens, cov] <- 0
  }
}

pred_template$age_group_id <- factor(pred_template$age_group_id)

if ("ICU" %in% covs$covariate){
  ICU <- c('ICU_only')
  fakeICU <- data.frame(ICU)
  pred_template <- bind_rows(pred_template, fakeICU)
  pred_template$ICU <- factor(pred_template$ICU)
} 

pred_template <- as.data.table(pred_template)

# if the evaluation pathogens are the same as the model pathogens, (i.e. eval_pathogens is blank)
# can just encode_and_interact
if (eval_type %in% c('complete', 'other')){
  pred <- encode_and_interact(df = pred_template, covariates = covs)
  
  # encoded NA columns are generated for each categorical variable, isolate those to drop
  nacols <- pred$generated_cols$generatedcol[!pred$generated_cols$generatedcol %in% modelcols$covariate]
  
  if ("ICU" %in% covs$covariate){
    pred$data <- pred$data[pred$data$ICU == 'mixed',]
  }
  
  if (length(nacols) > 0){
    pred$data <- pred$data[, (nacols):=NULL]
  }
}

if(eval_type == 'partial'){
  misspaths <- as.data.frame(matrix(ncol = 1, 
                                    nrow = length(unique(inputdata$data$pathogen)[!unique(inputdata$data$pathogen) %in% unique(pred_template$pathogen)])))
  colnames(misspaths) <- 'pathogen'
  misspaths$pathogen <- 
    unique(inputdata$data$pathogen)[!unique(inputdata$data$pathogen) %in% unique(pred_template$pathogen)]
  pred_template <- bind_rows(pred_template, misspaths)
  pred_template$pathogen <- factor(pred_template$pathogen)
  
  pred <- encode_and_interact(df = pred_template, covariates = covs)
  
  # encoded NA columns are generated for each categorical variable, isolate those to drop
  nacols <- pred$generated_cols$generatedcol[!pred$generated_cols$generatedcol %in% modelcols$covariate]
  
  pred$data <- pred$data[pred$data$pathogen %in% str_split(globparams$eval_pathogens, ", ")[[1]],]
  pred$data <- pred$data[, (nacols):=NULL]
  
  if ("ICU" %in% covs$covariate){
    pred$data$ICUICU_only <- 0
  } 
}

if ("hosp" %in% covs$covariate){
  pred$data$hospunknown <- 0
}

predtempcols <- c('age_group_id', 'year_id', 'location_id', 'sex_id', modelcols$covariate[!modelcols$covariate %in% "intercept"])
pred$data$age_group_id <- as.numeric(as.character(pred$data$age_group_id))

# configure prediction template in MRData format
predmrbrt_dat <- MRData()
if (useRE){
  predmrbrt_dat$load_df(
    data = pred$data,
    col_covs = predtempcols,
    col_study_id = globparams$study_id)
} else {
  predmrbrt_dat$load_df(
    data = pred$data,
    col_covs = predtempcols)
}

predmrbrt <- predmrbrt_dat$to_df()


predmrbrt$predict <- logit2prob(as.vector(mrbrt_mod$predict(data = predmrbrt_dat, 
                                                            predict_for_study = useRE)))

if (eval_type %in% c('complete', 'partial')){
  predmrbrt <- unencode_col(predmrbrt, 'pathogen')
}
if (eval_type == 'other'){
  predmrbrt$pathogen <- 'other'
}
if ("hosp" %in% covs$covariate & is.na(globparams$hosp_val)){
  predmrbrt <- unencode_col(predmrbrt, 'hosp')
}
if (globparams$hosp & !is.na(globparams$hosp_val)){
  predmrbrt$hosp <- globparams$hosp_val
} 

predmrbrt <- predmrbrt[, colnames(predmrbrt) %in% c("location_id", "year_id", "age_group_id", "sex_id", "pathogen", "hosp", "predict")]

if(!dir.exists(path = paste0(mntresult, 'point_predictions/'))){
  dir.create(path = paste0(mntresult, 'point_predictions/'), recursive = TRUE)
}

write.csv(predmrbrt, file = paste0(mntresult, 'point_predictions/', inf_synd, '_', model_id, '_point_predictions.csv'),
          row.names = FALSE)

if(!is.na(globparams$draws) & globparams$draws > 0 & takedraws){
  
  #if there's no directory for infectious syndrome draws, create one
  if(!dir.exists(drawpath)){
    dir.create(drawpath, recursive = TRUE)
  }
  
  # create draws
  #n_samples <- as.integer(globparams$draws)
  n_samples <- as.integer(globparams$draws)
  samples <- mrbrt_mod$sample_soln(sample_size = n_samples)
  
  if (grepl('prior_beta_gaussian', mrbrtschema) == TRUE){
    beta_samps_to_use <- mrbrt001::core$other_sampling$sample_simple_lme_beta(n_samples, mrbrt_mod)
  } else {
    beta_samps_to_use <- samples[[1]]
  }
 
  draws <- mrbrt_mod$create_draws(
    data = predmrbrt_dat,
    beta_samples = beta_samps_to_use,
    gamma_samples = samples[[2]],
    random_study = useRE)
  
  draws <- logit2prob(draws)
  pred_w_draws <- cbind(predmrbrt[, colnames(predmrbrt) %in% c("location_id", "year_id", "age_group_id", "sex_id", "pathogen", "hosp")],
                        draws)
  
  colnames(pred_w_draws)[grepl("[0-9]", colnames(pred_w_draws)) == TRUE] <-
    gsub("^[A-Z]*", "draw_", colnames(pred_w_draws)[grepl("[0-9]", colnames(pred_w_draws)) == TRUE])
  pred_w_draws$pathogen <- as.character(pred_w_draws$pathogen)
  
  # write out separate draw file for each evaluated pathogen
  for (pathogen in unique(pred_w_draws$pathogen)){
    writeout <- pred_w_draws[pred_w_draws$pathogen == pathogen,]
    write.csv(writeout,
    file = paste0(drawpath, pathogen, "_draws.csv"),
    row.names = FALSE)
  }
  
  plotdraws <- mrbrt_mod$create_draws(
    data = plotmrbrt_dat,
    beta_samples = beta_samps_to_use,
    gamma_samples = samples[[2]],
    random_study = useRE )
  
  plotmrbrt$pred_lo <- logit2prob(apply(plotdraws, 1, function(x) quantile(x, 0.025)))
  plotmrbrt$pred_hi <- logit2prob(apply(plotdraws, 1, function(x) quantile(x, 0.975)))
  plotdrawsdf <- plotmrbrt[plotmrbrt$pathogen %in% str_split(globparams$eval_pathogens, ", ")[[1]],]
  
  if(!dir.exists(paste0(diagplotpath, "draws/"))){
    dir.create(paste0(diagplotpath, "draws/"), recursive = TRUE)
  }
  
  ggplot(plotdrawsdf) +
    geom_line(aes(x = haqi, y = predict, color = pathogen), size = 2, alpha = 0.7) +
    geom_ribbon(aes(x = haqi, ymin = pred_lo, ymax = pred_hi, fill = pathogen), alpha = 0.25) +
    guides(color=guide_legend(nrow=2,byrow=TRUE)) +
    labs(y = "Case Fatality Rate", x = 'HAQI', title = paste0("Case Fatality Rate : HAQI for ", inf_synd, " pathogens")) +
    theme_bw() +
    theme(axis.title = element_text(size =15), axis.text = element_text(size =13),
          legend.text = element_text(size = 15), legend.position = ("bottom"),
          legend.title = element_blank()) +
    facet_wrap(~pathogen)
  ggsave(paste0(diagplotpath, "draws/", model_id, '_draws.png'), height = 9, width = 14)
}
}
