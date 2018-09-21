
  #GAM
  message('GAM')
  if("gam" %in% modnames){
  gam=  try(
            fit_gam_child_model(df = sub_the_data, #data frame
                           model_name = 'gam', #identifier for the child model-- needs to be consistent through the steps
                           fold_id_col = 'fold_id',
                           covariates = all_fixed_effects_child, #rhs formula
                           additional_terms = NULL, #'year', #column(s) in df that should be included in the fit. Ideally, there is a raster companion to the column. These columns are not splined
                           weight_column = 'weight', #column in the data frame specifying weights
                           bam = FALSE, #should mgcv::bam be called rather than gam?
                           spline_args = list(bs = 'ts', k = 3), #spline arguments to be applied to the columns specified by the covariates argument
                           auto_model_select = TRUE, #should the function override bam calls in low data situations (boosts probability of convergence)
                           indicator = indicator, #outcome variable column
                           indicator_family = 'binomial', #family of the outcome. Binomial and Gaussian are implemented.
                           cores = 5) #number of compute cores available
                    )
    if( class(gam)=='try-error' | is.null(gam) ){
      message('BRT FAILED SO EXCLUDING IT FROM CHILD MODEL LIST')
      modnames <- modnames[modnames != 'gam']
      child_stacker_models <- gsub('gam\\[\\[2\\]\\], ','',child_stacker_models)
    }
  }


  #BRT
  set.seed(123)
  message('BRT')
  if("gbm" %in% modnames){
    gbm =  try(
            fit_gbm_child_model(df = sub_the_data, #dataset to be passed. Should have covariates in the table via extract_covariates
                        model_name = 'gbm', #name of the model. Serves as the prefix for the outputs and interfaces with later steps
                        indicator = indicator,
                        indicator_family = indicator_family,
                        covariates = paste0(all_fixed_effects_child), #,' + ', cat_effects),
                        fold_id_col = 'fold_id',
                        weight_column             = 'weight', #a column of weights. Needs to be a seperate object. Has not been test as of 11/2
                        tc                 = 3,
                        lr                 = 0.005,
                        bf                 = 0.75,
                        cores              = 5)
                    )
    if( class(gbm)=='try-error' | is.null(gbm) ){
      message('BRT FAILED SO EXCLUDING IT FROM CHILD MODEL LIST')
      modnames <- modnames[modnames != 'gbm']
      child_stacker_models <- gsub('gbm\\[\\[2\\]\\], ','',child_stacker_models)
    }
  }

  #fit some nets
  #lasso
  message('LASSO')
  if("lasso" %in% modnames)
  lasso = fit_glmnet_child_model(df = sub_the_data,
                                 model_name = 'lasso',
                                 fold_id_col = 'fold_id',
                                 covariates = all_fixed_effects_child,
                                 additional_terms = NULL,
                                 weight_column = 'weight',
                                 alpha = 1, #The elasticnet mixing parameter, with 0≤α≤ 1. 0 is ridge, 1 is lasso
                                 indicator = indicator,
                                 indicator_family = 'binomial',
                                 cores = 5)

  #ridge
  message('RIDGE')
  if('ridge' %in% modnames){
  ridge = try(
        fit_glmnet_child_model(df = sub_the_data,
                                model_name = 'ridge',
                                fold_id_col = 'fold_id',
                                covariates = all_fixed_effects_child,
                                additional_terms = NULL,
                                weight_column = 'weight',
                                alpha = 0,
                                indicator = indicator,
                                indicator_family = 'binomial',
                                cores = 5)
                    )
    if( class(ridge)=='try-error' | is.null(ridge) ){
      message('BRT FAILED SO EXCLUDING IT FROM CHILD MODEL LIST')
      modnames <- modnames[modnames != 'ridge']
      child_stacker_models <- gsub('ridge\\[\\[2\\]\\], ','',child_stacker_models)
    }
  }

  #enet
  message('ENET')
  if('enet' %in% modnames){
  enet = try(
      fit_glmnet_child_model(df = sub_the_data,
                                model_name = 'enet',
                                fold_id_col = 'fold_id',
                                covariates = all_fixed_effects_child,
                                additional_terms = NULL,
                                weight_column = 'weight',
                                alpha = .5,
                                indicator = indicator,
                                indicator_family = 'binomial',
                                cores = 5)
                    )
    if( class(enet)=='try-error' | is.null(enet) ){
      message('BRT FAILED SO EXCLUDING IT FROM CHILD MODEL LIST')
      modnames <- modnames[modnames != 'enet']
      child_stacker_models <- gsub('enet\\[\\[2\\]\\], ','',child_stacker_models)
    }
  }

  #combine the children models
  message('\nadding child models predictions to data frame')
  for(i in 1:length(modnames)){
    message(modnames[i])
    sub_the_data = cbind(sub_the_data, get(modnames[i])[[1]])
  }

  #fit stacker
  message('\nfitting gam stacker')
  stacked_results = gam_stacker(sub_the_data, #the dataset in data table format
                                model_names=modnames, #prefixes of the models to be stacked
                                weight_column='weight',
                                bam = F, #whether or not bam should be used
                                spline_args = list(bs = 'ts', k = 3),
                                indicator = indicator, #the indicator of analysis
                                indicator_family = indicator_family,
                                cores = 50)


  #return the stacked rasters
  message('\nmaking stacked rasters')
  stacked_rasters = make_stack_rasters(covariate_layers = all_cov_layers, #raster layers and bricks
                                        period = which(years==y), #period of analysis, NULL returns all periods
                                        child_models = eval(parse(text=child_stacker_models)), #model objects fitted to full data
                                        stacker_model = stacked_results[[2]],
                                        indicator_family = 'binomial',
                                        return_children = TRUE,
                                        centre_scale_df = covs_cs_df)


  # plot the stacker surfaces
  pdf(sprintf('%s/output/%s/stacking_rasters_%s_%i_%i_%i.pdf',sharedir,run_date,reg,age,holdout,y))
  for(i in 1:length(stacked_rasters))
    plot(stacked_rasters[[i]],main=names(stacked_rasters[[i]]))
  dev.off()

  # save surfaces and fit objects
  stacked_model_objects = list(gam=gam,gbm=gbm,lasso=lasso,ridge=ridge,enet=enet)

  save(stacked_rasters,file=sprintf('%s/output/%s/stacking_rasters_%s_%i_%i%s_%i.RData',sharedir,run_date,reg,age,holdout,vallevel,y))
  save(stacked_results,file=sprintf('%s/output/%s/stacking_results_%s_%i_%i%s_%i.RData',sharedir,run_date,reg,age,holdout,vallevel,y))
  save(stacked_model_objects,file=sprintf('%s/output/%s/stacking_objects_%s_%i_%i%s_%i.RData',sharedir,run_date,reg,age,holdout,vallevel,y))
