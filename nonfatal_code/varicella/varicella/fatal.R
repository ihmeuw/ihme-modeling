#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: PART ONE - Prepare negative binomial regression of diphtheria cases for input into codcorrect
#          PART TWO - Replace modeled estimates with CODEm model for data-rich countries
#          PART THREE - Format for CodCorrect and save results to database
#          PART FOUR - Run DisMod model for CFR
#          PART FIVE - Calculate nonfatal outcomes from mortality
#          PART SIX - Format for COMO and save results to database
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, rhdf5, plyr,data.table, parallel, dplyr)
if (Sys.info()["sysname"] == "Linux") {
  require(mvtnorm, lib="FILEPATH")
} else { 
  pacman::p_load(mvtnorm)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause <- "varicella"
cause_id <- 342
me_id <- 1440
ages <- c(2:20, 30:32, 235) ## age group early neonatal to 95+

### make folders on cluster
cl.death.dir <- file.path("FILEPATH")                                              
dir.create(cl.death.dir, recursive = T)

cl.version.dir <- file.path("FILEPATH")                                              
dir.create(file.path(cl.version.dir), recursive = T)

### directories
home <- file.path(j_root, "FILEPATH")  
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
dir.create(j.version.dir.inputs, recursive = T)
dir.create(j.version.dir.logs, recursive = T)

### save description of model run
write.table(description, file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### load shared functions
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_covariate_estimates.R") %>% source
file.path(j_root, "FILEPATH/get_envelope.R") %>% source
file.path(j_root, "FILEPATH/get_cod_data.R") %>% source
file.path(j_root, "FILEPATH/get_draws.R") %>% source

### reading .h5 files
read_block_names <- function(str){
  # Use string splitting to get the column names of a value block
  split_vec <- strsplit(str, "'")[[1]]
  N <- length(split_vec)
  split_vec[seq(from=2, to=N, by=2)]
}
value_blocks <- function(attrib){
  # get all the value block names from teh attributes of an hdf5 table
  grep(glob2rx("values_block_*_kind"), names(attrib), value=TRUE)
}
h5_value_col_names <- function(h5File, key){
  # return a list where each element are the column names of the different
  # value block types i.e. float, int, string
  attrib <- h5readAttributes(h5File, paste0(key, "/table/"))
  value_block_names <- value_blocks(attrib)
  nam <- lapply(value_block_names, function(x) read_block_names(attrib[[x]]))
  names(nam) <- gsub("_kind", "", value_block_names)
  nam
}
mat_to_df <- function(mat, mat_names){
  # pytables hdf5 saves values as matrices when more than one column exists 
  # We need to transpose it then apply the names
  if (length(dim(mat)) == 1){
    mat <- matrix(data=mat, nrow=length(mat), ncol=length(mat_names))
  }
  else{
    mat <- t(mat)
  }
  df <- as.data.frame(mat)
  names(df) <- mat_names
  df
}
read_hdf5_table <- function(h5File, key){
  # read in the indices  values as well as the value blocks
  # only indices values may be strings
  data_list <- h5read(h5File, paste0(key, "/table/"), compoundAsDataFrame=F)
  data_value_names <- h5_value_col_names(h5File, key)
  indices <- setdiff(names(data_list), c("index", names(data_value_names)))
  df_index <- data.frame(data_list[indices])
  df_values <- lapply(names(data_value_names), function(x) 
    mat_to_df(data_list[[x]], data_value_names[[x]]))
  do.call(cbind, c(list(df_index), df_values))
}
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: COD NEG BIN REGRESSION #################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=4, location_set_id=22) ## id=22 is from covariates team, id=9 is from epi
locations <- subset(locations, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", "level", "location_type", "parent_id"))
pop_locs <- unique(locations$location_id)


if (SAVE_FATAL == "yes") {
  
  ### pop_env
  # get envelope
  envelope <- get_envelope(location_id=pop_locs, sex_id=1:2, age_group_id=ages, year_id=1980:2016, with_hiv=0)
  colnames(envelope)[colnames(envelope)=="mean"] <- "envelope"
  envelope <- subset(envelope, select=c("location_id", "year_id", "age_group_id", "sex_id", "envelope"))
  # get population data
  population <- get_population(location_id=pop_locs, sex_id=1:2, age_group_id=ages, year_id=1980:2016)
  population <- subset(population, select=c("location_id", "year_id", "age_group_id", "sex_id", "population"))
  # bring together
  pop_env <- merge(population, envelope, by=c("location_id", "year_id", "age_group_id", "sex_id"))
  
  ### get cod data
  cod <- get_cod_data(cause_id=342)
  cod <- subset(cod, select=c("location_id", "year", "age_group_id", "sex", "sample_size", "cf"))
  colnames(cod)[colnames(cod)=="sex"] <- "sex_id"
  colnames(cod)[colnames(cod)=="year"] <- "year_id"
  cod <- subset(cod, year_id >= 1980 & age_group_id %in% ages & !is.na(cf) & sample_size != 0)
  
  if (HAQI_or_HSA == "HSA") {
    
    # covariate: HSA, covariate_id=208, covariate_name_short="health_system_access_capped"
    hsa <- get_covariate_estimates(covariate_name_short="health_system_access_capped")
    hsa <- hsa[hsa$year_id >= 1980, ]
    colnames(hsa)[colnames(hsa)=="mean_value"] <- "health"
    hsa <- subset(hsa, select=c("location_id", "year_id", "health"))
    
    # bring together all covariates
    covariates <- hsa
    
  } else if (HAQI_or_HSA =="HAQI") {
    
    # covariate: HSA, covariate_id=208, covariate_name_short="health_system_access_capped"
    haqi <- get_covariate_estimates(covariate_name_short="haqi")
    haqi <- haqi[haqi$year_id >= 1980, ]
    haqi$health <- haqi$mean_value / 100
    haqi <- subset(haqi, select=c("location_id", "year_id", "health"))
    
    # bring together all covariates
    covariates <- haqi
    
  }
  
  # prep data for regression
  regress <- merge(cod, covariates, by=c("location_id", "year_id"), all.x=TRUE)
  regress <- merge(regress, pop_env, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE)
  
  # calculate deaths
  regress$deaths <- regress$cf * regress$envelope
  
  # replace deaths with 0 if deaths < 0.5
  regress[regress$deaths < 0.5, "deaths"] <- 0
  
  # drop outliers (cf greater than 99th percentile)
  cf_99 <- quantile(regress$cf, 0.99)
  
  # make ages into levels
  regress$age_group_id <- as.factor(regress$age_group_id)
  
  # save file for reference
  write.csv(regress, file.path(j.version.dir.inputs, "inputs_for_nbreg_COD.csv"), row.names=FALSE)
  #***********************************************************************************************************************
  
  
  #----NEG BIN MODEL------------------------------------------------------------------------------------------------------
  ### run negative binomial regression
  theta = 1/4.42503
  GLM <- glm.nb(deaths ~ health + age_group_id + offset(log(population)), init.theta=theta, data=regress)
  summary(GLM)
  
  # save log
  capture.output(summary(GLM), file = file.path(j.version.dir.logs, "log_deaths_nbreg.txt"), type="output")
  #*********************************************************************************************************************** 
  
  
  #----DRAWS--------------------------------------------------------------------------------------------------------------
  set.seed(0311)
  
  ### predict out for all country-year-age-sex
  pred_death <- merge(population, covariates, by=c("year_id", "location_id"))
  N <- length(pred_death$year_id)
  
  ### 1000 draws for uncertainty
  # coefficient matrix
  coefmat <- c(coef(GLM))
  names(coefmat)[1] <- "constant"
  names(coefmat) <- paste("b", names(coefmat), sep = "_")
  coefmat <- matrix(unlist(coefmat), ncol=24, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))
  
  # covariance matrix
  vcovmat <- vcov(GLM)
  
  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
  betas <- t(betadraws)
  
  betas <- betas %>% data.frame
  colnames(betas) <- paste0("beta_draw_", 0:999)
  
  # betas for age group
  betas_age <- betas
  colnames(betas_age) <- paste0("age_draw_", 0:999)
  betas_age$age_group_id <- c(NA, NA, 3:20, 30:32, 235)
  
  # merge together predictions with age draws by age_group_id
  pred_death <- merge(pred_death, betas_age, by="age_group_id", all.x=TRUE)
  
  # create draws of disperion parameter
  alphas <- 1 / exp(rnorm(1000, mean=GLM$theta, sd=GLM$SE.theta))
  
  if (GAMMA_EPSILON == "with") {
    for (draw in 0:999) { 
      b0 <- betas[1, ][[paste0("beta_draw_", draw)]]
      b1 <- betas[2, ][[paste0("beta_draw_", draw)]]
      age.fe <- pred_death[[paste0("age_draw_", draw)]]
      age.fe <- ifelse(is.na(age.fe), 0, age.fe)
      alpha <- alphas[draw + 1]
      pred_death[, paste0("death_draw_", draw)] <- rgamma( length(pred_death$age_group_id), scale=(alpha * 
                                                                                                     exp( b0 + b1*pred_death[["health"]] + age.fe ) * 
                                                                                                     pred_death[["population"]] ), 
                                                           shape=(1 / alpha) )
    }
  } else if (GAMMA_EPSILON == "without") {
    for (draw in 0:999) { 
      b0 <- betas[1, ][[paste0("beta_draw_", draw)]]
      b1 <- betas[2, ][[paste0("beta_draw_", draw)]]
      age.fe <- pred_death[[paste0("age_draw_", draw)]]
      age.fe <- ifelse(is.na(age.fe), 0, age.fe)
      alpha <- alphas[draw + 1]
      pred_death[, paste0("death_draw_", draw)] <- exp( b0 + b1*pred_death[["health"]] + age.fe ) * 
        pred_death[["population"]] 
    }
  }
  
  # save results
  pred_death <- pred_death %>% data.frame
  pred_death_save <- subset(pred_death, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                                 colnames(pred_death)[grepl("death_draw_", colnames(pred_death))]))
  colnames(pred_death_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
  
  if (WRITE_FILES == "yes") {
    write.csv(pred_death_save, file.path(j.version.dir, paste0("01_death_draws.csv")), row.names=FALSE)
  }
  #*********************************************************************************************************************** 
  
  
  ########################################################################################################################
  ##### PART TWO: COMBINE CODEm DEATHS ###################################################################################
  ########################################################################################################################
  
  
  #----COMBINE------------------------------------------------------------------------------------------------------------
  ### read CODEm COD results for data-rich countries
  cod_M <- read_hdf5_table(file.path("FILEPATH", acause, male_CODEm_version, paste0("draws/deaths_", "male", ".h5")), key="data") #212141
  cod_F <- read_hdf5_table(file.path("FILEPATH", acause, female_CODEm_version, paste0("draws/deaths_", "female", ".h5")), key="data") #212147
  
  # combine M/F CODEm results
  cod_DR_M <- subset(cod_M, select=c("location_id", "year_id", "age_group_id", "sex_id", colnames(cod_M)[grep("draw_", colnames(cod_M))]))
  cod_DR_F <- subset(cod_F, select=c("location_id", "year_id", "age_group_id", "sex_id", colnames(cod_F)[grep("draw_", colnames(cod_F))]))
  cod_DR <- rbind(cod_DR_M, cod_DR_F)
  cod_DR <- cod_DR[cod_DR$age_group_id %in% ages, ]
  
  data_rich <- unique(cod_DR$location_id)
  split_deaths_glb <- pred_death_save[!pred_death_save$location_id %in% data_rich, ]
  colnames(split_deaths_glb) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
  split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)
  
  # save results
  if (WRITE_FILES == "yes") {
    write.csv(split_deaths_hyb, file.path(j.version.dir, "02_death_draws_with_codem_data_rich.csv"), row.names=FALSE)
  }
  #***********************************************************************************************************************
  
  
  ########################################################################################################################
  ##### PART THREE: FORMAT FOR CODCORRECT ################################################################################
  ########################################################################################################################
  
  
  #----SAVE RESULTS-------------------------------------------------------------------------------------------------------
  ### format for codcorrect
  ids <- unique(split_deaths_hyb$location_id)
  years <- unique(split_deaths_hyb$year_id)
  sexes <- c(1,2)
  
  split_deaths_hyb <- split_deaths_hyb %>% data.table
  
  # save .csv for location-year-sex
  write <- function(x,y,z) { write.csv(split_deaths_hyb[split_deaths_hyb$location_id==x & split_deaths_hyb$year_id==y & split_deaths_hyb$sex_id==z, ], 
                                       file.path(cl.death.dir, paste0(x, "_", y, "_", z, ".csv")), row.names=FALSE) }
  for (location_id in ids) {
    for (year in years) {
      for (sex in sexes) {
        write(x=location_id, y=year, z=sex)
      }
    }
  }
  
  print(paste0("death draws saved in ", cl.death.dir))
  #*********************************************************************************************************************** 
  
}