## This script is submitting from run_mbg_died.R for each region-age bin taking in various testing related parameters

## GET SET UP FROM THE QSUB CALL and Print to log inputs
R.Version()

# assign arguments from qsub an object name
if(Sys.info()[['nodename']]!='db04'){
  reg                      <- as.character(commandArgs()[4])
  age                      <- as.numeric(commandArgs()[5])
  run_date                 <- as.character(commandArgs()[6])
  test                     <- as.character(commandArgs()[7])
  holdout                  <- as.numeric(commandArgs()[8])
  use_base_covariates      <- as.numeric(commandArgs()[11])
  test_pv_on_child_models  <- as.numeric(commandArgs()[12])
  constrain_children_0_inf <- as.numeric(commandArgs()[13])
  child_cv_folds           <- as.numeric(commandArgs()[14])
  fit_stack_cv             <- as.numeric(commandArgs()[15])
  modeltype                <- as.character(commandArgs()[16])
  vallevel                 <- as.character(commandArgs()[17])
  pct_sbh_wgt              <- as.numeric(commandArgs()[18])
  hard_set_sbh_wgt         <- as.numeric(commandArgs()[19])
}

# print options for this run to logs
message("options for this run:\n")
  message(paste('run_date:',run_date))
  message(paste('test:',test))
  message(paste('holdout:',(holdout)))
  message(paste('use_base_covariates:',(use_base_covariates)))
  message(paste('test_pv_on_child_models:',(test_pv_on_child_models)))
  message(paste('constrain_children_0_inf:',(constrain_children_0_inf)))
  message(paste('child_cv_folds:',(child_cv_folds)))
  message(paste('fit_stack_cv:',(fit_stack_cv)))
  message(paste('vallevel:',(vallevel)))
  message(paste('pct_sbh_wgt:',(pct_sbh_wgt)))
  message(paste('hard_set_sbh_wgt:',(hard_set_sbh_wgt)))
  message(paste('modeltype:',modeltype))

# extra set ups for various modelling options
vallevel = gsub('\r','',as.character(vallevel))
if(modeltype=='null'){
  nullmodel=TRUE
  linearonly=FALSE
  nm='nullmodel'
}
if(modeltype=='full'){
  nullmodel=FALSE
  linearonly=FALSE
  nm=''
}
if(modeltype=='linearonly'){
  nullmodel=FALSE
  linearonly=TRUE
  use_base_covariates=TRUE
  nm='linearonly'
}

# load the data image that we saved in the launch script before qsub
load(paste0('<<<< FILEPATH REDACTED >>>>>/pre_run_tempimage_', run_date,'.RData'))
pathaddin = paste0('_bin',age,'_',reg,'_',holdout,nm,vallevel)

# print for logging purposes
message(paste('pathaddin:',pathaddin))

# load packages and custom functions
package_lib <- <<<< FILEPATH REDACTED >>>>>
.libPaths(package_lib)
for(package in package_list) {
  message(paste0('loading ',package))
  library(package, lib.loc = package_lib, character.only=TRUE)
}
setwd(repo)
for(funk in list.files(recursive=TRUE,pattern='functions')){
    message(funk)
    source(funk)
}

# load the config info again
config <- load_config(repo            = repo,
                      indicator_group = indicator_group,
                      indicator       = indicator)

## ~~~~~~~~~~~~~~~~~~~~~~~~ Prep MBG inputs ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Load simple polygon template to model over
gaul_list           <-  get_gaul_codes(reg)
simple_polygon_list <-  load_simple_polygon(gaul_list = gaul_list,
                                            buffer    = 1)
subset_shape     <- simple_polygon_list[[1]]
simple_polygon   <- simple_polygon_list[[2]]
message(paste0('Simple Polygon Class:',class(simple_polygon)))

## Build simple raster to project model and crop covariates quickly
raster_list   <- build_simple_raster_pop(subset_shape)
simple_raster <- raster_list[['simple_raster']]
pop_raster    <- raster_list[['pop_raster']]

# save the template for covenience for later on
if(!file.exists(paste0('<<<< FILEPATH REDACTED >>>>>/simple_raster',reg,'.RData')))
  save(simple_raster,file=paste0('<<<< FILEPATH REDACTED >>>>>/simple_raster',reg,'.RData'))

## Load input data and make sure it is cropped to modeling area
## Load input data based on stratification and holdout, OR pull in data as normal and run with the whole dataset if holdout == 0.
message(paste('testing vallevel',vallevel,'class',class(vallevel)))
if(holdout!=0) {
  df <- as.data.table(get(paste0('stratum_',vallevel))[[paste('region',reg,'_age',age,sep='__')]])
  df <- df[fold != holdout, ]
}
if(holdout==0) {
  df <- load_input_data(indicator     = indicator,
                          simple      = simple_polygon,
                          agebin      = age,
                          removeyemen = TRUE,
                          pathaddin   = pathaddin)
}

# patch fix for newer inla version
if(grepl("geos", Sys.info()[4])) INLA:::inla.dynload.workaround()

##################################################################
## Survey drops as described in methods appendix
df <- subset(df,nid!=227133)
df <- subset(df,!nid%in%c(133731,91508,218035,203663,203654,203664))
df <- subset(df, !(df$nid==74393 & df$location_code == c(2203)))
df <- subset(df, !(df$nid==32189 & df$location_code %in% c(2751,2747)))
############################################################

# incorporate SBH weights into Poly-resample weights
df$sbh_wgt[df$sbh_wgt>1]=1
df[,weight := weight*sbh_wgt]

# build spatial mesh
mesh_s <- build_space_mesh(d           = df,
                           simple      = simple_polygon,
                           max_edge    = mesh_s_max_edge,
                           mesh_offset = mesh_s_offset)

# Build temporal mesh
mesh_t <- build_time_mesh()

# Load covariates and crop. Keep minimal for null model
if(nullmodel) {
 all_fixed_effects <- fixed_effects <- 'rates'
 cov_layers<-load_and_crop_covariates('rates', simple_polygon, agebin=age)
}


############ SKIPSTACK, set TRUE if using previous stacking objects to save time
if(SKIPSTACK==FALSE){


# if not 'null' (intercept only) model
if(!nullmodel) {

  # Pull covariate rasters
  cov_layers <- load_and_crop_covariates(fixed_effects  = fixed_effects,
                                         simple_polygon = simple_polygon,
                                         agebin         = age )


  ## Combine all the covariate layers we want to use and combine our fixed effects formula
  all_cov_layers <- all_cov_layers_orig <- c(cov_layers)
  all_fixed_effects <- fixed_effects

  # remove rates from stacking fit
  all_fixed_effects_child <- gsub(" \\+ rates","",all_fixed_effects)
  all_cov_layers[[which(names(all_cov_layers)%in%'rates')]] <- NULL

} else{
  all_cov_layers_orig <- cov_layers
}

# see if need to drop rates from things (config option). Do not include rates as cov after March 2017
if(!as.logical(use_rate_cov) & !nullmodel){
  message('DROPPING RATES AS COMMANDED BY CONFIG')
  cov_layers[[which(names(cov_layers)%in%'rates')]] <- NULL
  all_cov_layers_orig[[which(names(all_cov_layers_orig)%in%'rates')]] <- NULL
  fixed_effects <- gsub(" \\+ rates","",fixed_effects)
  all_fixed_effects <- gsub(" \\+ rates","",all_fixed_effects)
}

# make sure its transformed
message('Transform a few things')
if('rates' %in% names(all_cov_layers_orig))
    values(all_cov_layers_orig[['rates']])=qlogis(as.matrix(all_cov_layers_orig[['rates']]))

################## STACKING ########################

# parse out list of covs
if(length(all_fixed_effects)==1 & !nullmodel){
  the_covs <-  unlist(tstrsplit(all_fixed_effects_child,"\\+"))
}

if(!nullmodel){
  #copy the dataset to avoid unintended namespace conflicts
  the_data <- copy(df)

  #shuffle the data into five folds for within stacking cross validation
  the_data <- the_data[sample(nrow(the_data)),]
  the_data[,fold_id := cut(seq(1,nrow(the_data)),breaks=child_cv_folds,labels=FALSE)]

  # extract covariates  to the points and subset data where its missing covariate values
  the_data[,row_id := 1:nrow(the_data)]
  cs <- TRUE
  cs_covs <- extract_covariates(the_data,
                                all_cov_layers,
                                period_var = 'year',
                                id_col = 'row_id',
                                return_only_results = TRUE,
                                centre_scale = cs)
  if(cs){ # centre scaling
    the_data = cbind(the_data, cs_covs[[1]])
    covs_cs_df = cs_covs[[2]]
  } else {
    the_data = cbind(the_data, cs_covs)
  }
  the_covs <- trimws(the_covs)
  the_data <- na.omit(the_data, c(the_covs)) # drop NA covariate values
  wgt      <- the_data$weight

  # define child model names
  modnames <- as.vector(eval(parse(text=child_stacker_model_names)))
  years    <- c(2000,2005,2010,2015)

  # blank data table to be rbinded later
  d       <- data.table()

  # print out the modnames
  message(paste(modnames,collapse=', '))

  # save the data now for importance weighting
  save_post_est(the_data,'csv', paste0('df_going_into_stacking',pathaddin))

  # run stacking for each year
  for(y in years){
    print(y)

    # subset the data from current iterating year only
    sub_the_data = subset(the_data,year==y)

    # run all child models, separated out to a different script
    message('CHILD MODELS')
    source('./model/run_stacker_models.R')

    # rename results by year
    assign(paste0('stacked_results',y),stacked_results)
    assign(paste0('stacked_rasters',y),stacked_rasters)

    # add the estimated stacked results as a column in the data
    sub_the_data =cbind(sub_the_data, stacked_results=stacked_results[[1]])

    # rbind to add this years data to the full dataset
    library(gtools)
    d <- smartbind(d,sub_the_data)
  }

  # copy the data table
  d2 <- data.table(d)

  # combine all years stacked rasters together
  stacked_rasters <- list()
  rasnames        <- c()
  for(n in c('stacked_results',modnames)){

    # if testing, some children  may not have converged. If so, flexibly ignore them
    if(any(  is.null(stacked_rasters2000[[n]]),
             is.null(stacked_rasters2005[[n]]),
             is.null(stacked_rasters2010[[n]]),
             is.null(stacked_rasters2015[[n]]) ))  next

    rasnames <- c(rasnames,n)

    stacked_rasters[[n]]=
              brick(stacked_rasters2000[[n]][[1]],
                    stacked_rasters2005[[n]][[1]],
                    stacked_rasters2010[[n]][[1]],
                    stacked_rasters2015[[n]][[1]])
  }

  # remove if any didnt make it into modnames
  # logit transform child model
  for(cl in colnames(d2)){
    if(length(grep('_pred',cl)==1)){
      message(cl)
      d2[[cl]] <- qlogis(d2[[cl]])
      if(length(grep(paste0(rasnames,collapse='|'),cl))!=1)
        d2[[cl]] <- NULL
    }
  }

  #copy things back over to df
  df <- copy(d2)

  #remove the cov columns from the data frame (they'll get added back in later)
  for(cl in c('stacked_results',the_covs)) df[[cl]]=NULL

  # set all fixed effects for the geo model to be the children models
  if(!use_base_covariates){
    if(!as.logical(use_rate_cov)){
        final_all_fixed_effects <- paste0(names(stacked_rasters[2:length(stacked_rasters)]), collapse = ' + ')
    } else {
      final_all_fixed_effects <- paste0(paste0(names(stacked_rasters[2:length(stacked_rasters)]), collapse = ' + '),' + rates')
    }
  } else {
    if(linearonly == 1){
      final_all_fixed_effects <-  paste0(all_fixed_effects)
    } else {
      final_all_fixed_effects <- paste0(paste0(names(stacked_rasters[2:length(stacked_rasters)]),collapse = ' + '),' + ', all_fixed_effects)
    }
  }

  # all rasters togethers
  full_raster_list = c(unlist(stacked_rasters),unlist(all_cov_layers_orig))
  full_raster_list[['stacked_results']]<-NULL # remove since doing stacking of children within inla

  # subset the raster list to match only the final child model
  if(!use_base_covariates) {
    if(!as.logical(use_rate_cov)){
      usable_rasters = full_raster_list[c(modnames)]
    } else {
      usable_rasters = full_raster_list[c(modnames,'rates')]
     }
  } else {
    usable_rasters = full_raster_list
  }

  # logit transform models rasters so they are in logit space (same as response) when stacked their coef will be more interpretable
  for(r in names(usable_rasters)){
    if(r %in% modnames)
     values(usable_rasters[[r]])<-qlogis(as.matrix(usable_rasters[[r]]))
  }


} else { # close !nullmodel conditional
  final_all_fixed_effects <- fixed_effects
  usable_rasters <- all_cov_layers_orig
  modnames <- 'NONE'
}

# save prepped inputs before heading into modelling
save_mbg_input(indicator             = indicator,
                   indicator_group   = indicator_group,
                   df                = df,
                   simple_raster     = simple_raster,
                   mesh_ s           = mesh_s,
                   mesh_t            = mesh_t,
                   cov_list          = usable_rasters,
                   run_date          = run_date,
                   pathaddin         = pathaddin,
                   child_model_names = modnames,
                   centre_scale      = FALSE,
                   all_fixed_effects = final_all_fixed_effects)

# load them back in
load(paste0('<<<< FILEPATH REDACTED >>>>>/', indicator_group, '/', indicator, '/model_image_history/', run_date, pathaddin,'.RData'))


 ####### END SKIPSTACK, if skipping, load an old environment ################
} else {

  if(vallevel!='NA'){
    message('Loading Val data from previous run on 2017_05_20_18_47_03')
    load(paste0('<<<< FILEPATH REDACTED >>>>>/', indicator_group, '/', indicator, '/model_image_history/2017_05_20_18_47_03', pathaddin,'.RData'))
  } else {
    message('Loading full data from previous run on 2017_05_20_01_10_49')
    load(paste0('<<<< FILEPATH REDACTED >>>>>/', indicator_group, '/', indicator, '/model_image_history/2017_05_20_01_10_49', pathaddin,'.RData'))
  }

  # if skipstack need to reset some objects
  final_all_fixed_effects <- all_fixed_effects
  cs       <- 1
  modnames <- c('gam','gbm','ridge','enet','lasso')
  mesh_s   <- build_space_mesh(d           = df,
                               simple      = simple_polygon,
                               max_edge    = mesh_s_max_edge,
                               mesh_offset = mesh_s_offset)
  usable_rasters <- cov_list

}

# set some things up for a nullmodel if selected
if(nullmodel) cs ,- 0
if(nullmodel & !as.logical(use_rate_cov) ){
  final_all_fixed_effects <- 'NONE'
}


if(!nullmodel){

  #for stacking, overwrite the columns matching the model_names so that we can make inla into our stacker
  if(fit_stack_cv) child_fit_on = '_cv_pred' else child_fit_on = '_full_pred'
  df = rbind(df[,paste0(modnames) := lapply(modnames, function(x) get(paste0(x,child_fit_on)))])

  # check for strong correlation in stackers (>0.99) and chuck if there is
  cr <- melt(cor(cbind(df[,modnames,with=FALSE])))
  save_post_est(cr,'csv', paste0('stacker_corr',pathaddin))

  # get pairs of high correlation
  cr <- cr[cr$value > 0.99 & cr$Var1!=cr$Var2,]
  cr <- cr[duplicated(cr$value),]
  pref <- data.table(Var1  = c('enet','ridge','lasso','gam','gbm'),
                     order = 1:5)
  cr <- merge(cr,pref,by='Var1')
  cr <- cr[order(cr$order),]
  modstodrop <- character(0)
  i=1
  message('Dropping Collinear Models')
  while(sum(cr$value>0.99)){
    modstodrop[i]<-as.character(cr[1,1])
    cr<-subset(cr,Var1!=modstodrop[i])
    cr<-subset(cr,Var2!= modstodrop[i])
    i=i+1
  }

  # stacker mods identified for drop due to high correlation
  message('Dropping'); message(paste(modstodrop,collapse=', '))

 # make sure objects needed for modeling have responded to the drop
 if(length(modstodrop)>0){
   message(paste0('Old formula = ',final_all_fixed_effects))
   for(i in 1:length(modstodrop)) {
     final_all_fixed_effects <- gsub(paste0(' \\+ ',modstodrop[i]),'',final_all_fixed_effects)
   }
   message(paste0('New formula = ',final_all_fixed_effects))
   message(paste0('Old rasterlist = ',paste0(names(usable_rasters),collapse=', ')))
   usable_rasters <- usable_rasters[which(!names(usable_rasters)%in%modstodrop)]
   message(paste0('New rasterlist = ',paste0(names(usable_rasters),collapse=', ')))

   modnames <- modnames[which(!modnames %in% modstodrop)]
 }
}

## ~~~~~~~~~~~~~~~~~~~~~~~~ Run MBG model ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Generate MBG formula for INLA call, formula may change based on input args
interact_children_by_year <- 0          # set testing option to false
if( final_all_fixed_effects == 'NONE'){ # null no rate model
      mbg_formula <- build_mbg_formula(fixed_effects = 'int',
                                          int        = FALSE,
                                          add_nugget = as.logical(use_inla_nugget))
} else {
  if(constrain_children_0_inf){
    mbg_formula <- build_mbg_formula(fixed_effects = final_all_fixed_effects,
                                    positive_constrained_variables = modnames,
                                    add_nugget = as.logical(use_inla_nugget))
  } else {
    if(interact_children_by_year){
          mbg_formula <- build_mbg_formula(fixed_effects = final_all_fixed_effects,
                                           interact_with_year = modnames,
                                           add_nugget = as.logical(use_inla_nugget))
    } else {
      mbg_formula <- build_mbg_formula(fixed_effects = final_all_fixed_effects,
                                          add_nugget = as.logical(use_inla_nugget))
    }
  }
}

## Create SPDE INLA stack
if(!nullmodel){
  input_data <- build_mbg_data_stack(df            = df,
                                     fixed_effects = final_all_fixed_effects,
                                     mesh_s        = mesh_s,
                                     mesh_t        = mesh_t,
                                     exclude_cs    = c(modnames,'rates'),
                                     usematernnew  = FALSE)
} else {
  input_data <- build_mbg_data_stack(df            = df,
                                     fixed_effects = final_all_fixed_effects,
                                     mesh_s        = mesh_s,
                                     mesh_t        = mesh_t,
                                     exclude_cs    = c('rates'),
                                     usematernnew  = FALSE)
}
stacked_input <- input_data[[1]]
spde          <- input_data[[2]]
cs_df         <- input_data[[3]]

## Generate other inputs necessary
outcome <- df[[indicator]] # N+_i - event obs in cluster
N       <- df$N            # N_i  - total obs in cluster
weights <- df$weight

## Fit MBG model
if(grepl("geos", Sys.info()[4])) INLA:::inla.dynload.workaround()
model_fit <- fit_mbg(indicator_family    = indicator_family,
                     stack.obs           = stacked_input,
                     spde                = spde,
                     cov                 = outcome,
                     N                   = N,
                     int_prior_mn        = intercept_prior,
                     int_prior_prec      = 0.001,
                     f_mbg               = mbg_formula,
                     run_date            = run_date,
                     keep_inla_files     = keep_inla_files,
                     cores               = inla_cores,
                     verbose_output      = TRUE,
                     wgts                = weights,
                     intstrat = intstrat)

# save fit object
save_post_est(model_fit,'rdata', paste0('model_fit',pathaddin))

# Run predict_mbg on chunks of 50 samples (to avoid memory issues)
max_chunk <- 50
samples   <- as.numeric(samples)

message(paste0('Doing prection of chunks size ',max_chunk,' until ',samples,' draws are reached.'))

# Create vector of chunk sizes
chunks <- rep(max_chunk, samples %/% max_chunk)
if (samples %% max_chunk > 0) chunks <- c(chunks, samples %% max_chunk)

pm <- lapply(chunks, function(samp) {
  chunk <- predict_mbg(res_fit       = model_fit,
                       cs_df         = cs_df,
                       mesh_s        = mesh_s,
                       mesh_t        = mesh_t,
                       cov_list      = usable_rasters,
                       samples       = samp,
                       simple_raster = simple_raster,
                       transform     = 'inverse-logit')
  return(chunk[[3]])
})

# make some prediction objects
cell_pred <- do.call(cbind, pm)
pred_mean <- rowMeans(cell_pred)
mean_ras  <- insertRaster(simple_raster,matrix(pred_mean,ncol = 4))
names(mean_ras) <- paste0('period_', 1:4)

## Save MBG outputs in standard outputs folder structure
save_mbg_preds(config       = config,
               time_stamp   = time_stamp,
               run_date     = run_date,
               mean_ras     = mean_ras,
               sd_ras       = NULL, #sd_ras,
               res_fit      = model_fit,
               cell_pred    = cell_pred,
               df           = df,
               pathaddin    = pathaddin)

## for holdout data, aggregate prediction over vallelvel
if(holdout != 0){
  message('Doing out of sample predictive validity over aggregates')

  aggval <- aggregate_validation(holdoutlist         = get(paste0('stratum_',vallevel)),
                                 cell_draws_filename = paste0('%s_cell_draws_eb_bin%i_%s_%i',nm,'%s.RData'),
                                 years               = c(2000,2005,2010,2015),
                                 indicator_group     = indicator_group,
                                 indicator           = indicator,
                                 run_date            = run_date,
                                 reg                 = reg,
                                 holdout             = holdout,
                                 vallevel            = vallevel,
                                 sbh_wgt             = 'sbh_wgt',
                                 addl_strat          = c('age' = age),
                                 iter                = 1)

  # get validation metrics
  draws <- fit_stats_ho_id(draws       = aggval,
                           draw_prefix = 'phat_',
                           observed    = 'p')
  draws$covered = NULL

  save_post_est(draws,'csv', paste0('validation',pathaddin))
  save_post_est(aggval,'csv',paste0('aggval',    pathaddin))

}

# write a little note showing this is done. helps with waitformodelstofinishu5m(
write.table(1,sprintf('%s/output/%s/fin%s',sharedir,run_date,pathaddin))
