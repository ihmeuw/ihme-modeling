## This is the Master file for models to be run off


## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~ SETUP ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# some bash commands to clean up system and pull any new code
rm(list=ls())

## Set repo location and indicator group
repo            <- <<<< FILEPATH REDACTED >>>>>
indicator_group <- 'u5m'    # for filepath identification
indicator       <- 'died'   # for filepath identification
preprocessing   <- FALSE    # preprocessing will run if true. Only needed if new raw data are added
CBHSBH          <- FALSE    # dont need to run it unless have new data
Regions         <- 'africa' # runs on full africa, not regionally
dovalidation    <- TRUE     # Doing holdouts on this run?

setwd(repo)

# save some other important drive locations
root           <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")
package_lib    <- <<<< FILEPATH REDACTED >>>>> # where packages are stored
datadrive      <- <<<< FILEPATH REDACTED >>>>> # where data are stored
sharedir       <- <<<< FILEPATH REDACTED >>>>> # where we will write output


## Load libraries and  MBG project functions.
.libPaths(package_lib)
package_list<-c('rgeos'  , 'data.table','raster'    ,'rgdal'  ,'dplyr',
                'INLA'   ,'seegSDM'    ,'seegMBG'   ,'dismo'  ,'gbm',
                'foreign','parallel'   ,'doParallel','grid'   ,'gridExtra',
                'pacman' ,'gtools'     ,'glmnet'    ,'ggplot2','RMySQL')
for(package in package_list)
  library(package, lib.loc = package_lib, character.only=TRUE)
for(funk in list.files(recursive=TRUE,pattern='functions'))
  source(funk)


## Read config file and save all parameters in memory
config <- load_config(repo            = repo,
                      indicator_group = indicator_group,
                      indicator       = indicator)
slots      <- as.numeric(slots)
inla_cores <- as.numeric(inla_cores)

## Create run date in correct format
run_date <- make_time_stamp(TRUE)

## Create directory structure for this model run
create_dirs(indicator_group = indicator_group,
            indicator       = indicator)

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~ Mortality Data Preprocessing ~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Do not need to be run evertime, just if there is new data.
if(preprocessing){

  # combine as one dataset
  dfc <- fread(paste0(sharedir,'/input/mortality_cbh_w2.csv'))
  dfs <- fread(paste0(sharedir,'/input/mortality_sbh_w3.csv'))
  df  <- rbind(dfs,dfc)

  # use bias ratios where available
  df <- apply_haidong_bias_ratios(data=df)

  # resample any polygon identified data into points
  df <- resample_polygons(data=df,unique_vars = 'age_bin',cores=50,use_1k_popraster=FALSE,density=0.01,shp_path=<<<< FILEPATH REDACTED >>>>>)

  save_dated_input_data(d=df)
}


## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~ Load up data and make validation folds ~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Bring in simple africa polygon
load('<<<< FILEPATH REDACTED >>>>>/simple_africa_polygon.RData')

# bring in the dataset for all africa
df <- load_input_data(indicator   = indicator,
                      simple      = simple_polygon,
                      removeyemen = TRUE)
df <- subset(df, age %in% 1:4)

# save prepped data for shiny diagnostics
md <- prep_data_for_shiny(data=df)
save_data_for_shiny(df=md,var='died',indicator='died',year_var='year')

# get gauls and region ids
df <- merge_with_ihme_loc(df)

# run holdouts
yr_col     <- 'year'
temp_strat <- 'prop'
long_col   <- 'longitude'
lat_col    <- 'latitude'
n_folds    <- 5
all_strat <- get_strat_combos(data=as.data.frame(df), strat_cols=c("region","age"))
if(dovalidation){
  stratum_ad2 <- setup_fold_u5m(spat_strat='poly',admin_level=2)
  stratum_ad1 <- setup_fold_u5m(spat_strat='poly',admin_level=1)
}

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~  MBG  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## This is where we begin to do things by region and age bin
valloopvars <- expand.grid(Regions,1:4,1:n_folds,c('ad1','ad2'),'full') # or null
fulloopvars <- expand.grid(Regions,1:4,0,'',c('full'))

# toggle full for final run or vall if doing validation
loopvars    <- rbind(valloopvars)

# some final run options
intstrat  <- 'ccd'  # INLA Integration strategy
SKIPSTACK <- FALSE  # if using pre-run stackers

# save environment to be picked up after the job is qsubbed out
rm(list=c('test','age','holdout','reg','use_base_covariates','modnames','simple_polygon','i','vallevel','modeltype'))
save.image(paste0('<<<< FILEPATH REDACTED >>>>>y/pre_run_tempimage_', run_date,'.RData'))

# submit model jobs as parallel scripts
# note: this qsubs out jobs to run ./model/mbg_bybin.R by age where the meat of the modelling takes place
for(i in 1:nrow(loopvars)) {
  age        <- loopvars[i,2]
  reg        <- as.character(loopvars[i,1])
  holdout    <- loopvars[i,3]
  vallevel   <- as.character(loopvars[i,4])
  modeltype  <- as.character(loopvars[i,5])
  if(vallevel=='') vallevel <- 'NA'

  message(paste(age,reg,holdout,vallevel,modeltype))

  # this function makes the qsub command for the system call
  qsub <- make_qsub(  age        = age,
                      reg        = reg,
                      vallevel   = vallevel,
                      proj       = 'proj_geospatial_u5m',
                      saveimage  = FALSE,
                      test       = FALSE,               # quick test run
                      code       = 'mbg_bybin',         # wrapper file (This is the main modeling script getting called)
                      log_location = 'sharedir',        # to save logs in outputs folder do 'sharedir'
                      memory     = 10,
                      cores      = 25,                  # actually slots on qsub, not cores for inla
                      holdout    = holdout,             # holdout number (0 for full runs)
                      use_base_covariates=FALSE,        # use raw covariates in model in addition to stacker
                      test_pv_on_child_models=FALSE,    # in order to make the plot of plots
                      constrain_children_0_inf=FALSE,   # when stacking, do we constrain children models
                      child_cv_folds=5,                 # when stacking, do we constrain children models
                      fit_stack_cv=TRUE,                # fit stacker on cross validated children (T), or full pred (F)
                      shell = 'r_shell2.sh',
                      hard_set_sbh_wgt = FALSE,
                      pct_sbh_wgt = 100,                # if want to set wgt on all sbh data (if not due FALSE, 100)
                      modeltype=modeltype,
                      geo_nodes=FALSE)
    system(qsub)
 }

# wait for all models to finish before continuing
waitformodelstofinishu5m()

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~ ~~~~~~~~~~~~~~~~~~~~~~ Post-Estimation ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# In parallel by region,do post estimation using u5mpostest(), only pull holdout=0
postestlist <- mclapply(Regions,u5mpostest,subnational_condsim=TRUE, mc.cores=length(Regions))

# unlist x to get all the bits that were done in parallel out
for(i in 1:length(postestlist))
  for(n in names(postestlist[[i]]))
    assign(n,postestlist[[i]][[n]])

#############################
## save combined raked and raw outputs
for(group in c('child','neonatal')){
  message(group)

  # combine regions raster
  um <- unname(mget(grep(sprintf('*_mean_unraked_%s_raster',group), ls(), value = TRUE)))
  m  <- unname(mget(grep(sprintf('*_mean_raked_%s_raster',group), ls(), value = TRUE)))
  m2 <- unname(mget(grep(sprintf('*_range_raked_%s_raster',group), ls(), value = TRUE)))
  m3 <- unname(mget(grep(sprintf('*_lower_raked_%s_raster',group), ls(), value = TRUE)))
  m4 <- unname(mget(grep(sprintf('*_upper_raked_%s_raster',group), ls(), value = TRUE)))
  if(length(m)>1){
    um <- do.call(raster::merge,um)
    m  <- do.call(raster::merge,m)
    m2 <- do.call(raster::merge,m2)
    m3 <- do.call(raster::merge,m3)
    m4 <- do.call(raster::merge,m4)
  } else {
    um <- um[[1]]
    m  <- m[[1]]
    m2 <- m2[[1]]
    m3 <- m3[[1]]
    m4 <- m4[[1]]
  }

  # save them
  save_post_est(um, 'raster',sprintf('mean_%s_unraked_stack',group))
  save_post_est(m, 'raster',sprintf('mean_%s_raked_stack',group))
  save_post_est(m2, 'raster',sprintf('range_%s_raked_stack',group))
  save_post_est(m3, 'raster',sprintf('lower_%s_raked_stack',group))
  save_post_est(m4, 'raster',sprintf('upper_%s_raked_stack',group))

  # also save as 4 layers for convenience
  for(i in 1:4){
    save_post_est(m[[i]],  'raster',sprintf('mean_%s_raked_%i' ,group,c(2000,2005,2010,2015)[i]))
    save_post_est(m2[[i]], 'raster',sprintf('range_%s_raked_%i',group,c(2000,2005,2010,2015)[i]))
    save_post_est(m3[[i]], 'raster',sprintf('lower_%s_raked_%i',group,c(2000,2005,2010,2015)[i]))
    save_post_est(m4[[i]], 'raster',sprintf('upper_%s_raked_%i',group,c(2000,2005,2010,2015)[i]))
  }

  # save aggregated estimates
  ad0 = do.call(rbind,
        mget(grep(sprintf('*_%s_adm0_geo',group), ls(), value = TRUE)))
  ad1 = do.call(rbind,
        mget(grep(sprintf('cond_sim_raked_adm1_%s_*',group), ls(), value = TRUE)))
  ad2 = do.call(rbind,
        mget(grep(sprintf('cond_sim_raked_adm2_%s_*',group), ls(), value = TRUE)))
  ad1u = do.call(rbind,
        mget(grep(sprintf('cond_sim_unraked_adm1_%s_*',group), ls(), value = TRUE)))
  ad2u = do.call(rbind,
        mget(grep(sprintf('cond_sim_unraked_adm2_%s_*',group), ls(), value = TRUE)))

  save_post_est(ad0,'csv',sprintf('%s_adm0_geo'  ,group))
  save_post_est(ad1,'csv',sprintf('%s_adm1_raked',group))
  save_post_est(ad2,'csv',sprintf('%s_adm2_raked',group))
  save_post_est(ad1u,'csv',sprintf('%s_adm1_unraked',group))
  save_post_est(ad2u,'csv',sprintf('%s_adm2_unraked',group))
}

# summary tables for adm1 and adm2 estimates
make_adm_summary_tables()

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~ Model-Validation Summaries ~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
if(dovalidation){

    # gets done for qt ad1 and ad2
    for(lvl in c('ad1','ad2')){

      message(lvl)

      # load
      fs <- list.files(path=sprintf('%s/output/%s',sharedir,run_date),pattern='validation_bin')
      fs <- fs[grep(paste0(lvl,'.csv'),fs)]
      oosval <-list()
      for(f in fs)
        oosval[[length(oosval)+1]] <- fread(sprintf('%s/output/%s/%s',sharedir,run_date,f))

      # make on big data tables
      draws <- do.call(rbind,oosval)

      # save summary validation table for this model.
      val_summary <- make_summary_validation_table(agg_over=c('region','age','oos'),lv=lvl)
      save_post_est(val_summary,'csv',paste0('validation_summary_table',lvl))
    }
 }

# some final cleanup
cleanup_inla_scratch(run_date=run_date)
