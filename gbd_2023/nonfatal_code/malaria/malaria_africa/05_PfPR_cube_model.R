# ---- Library imports ----

library("raster")
library("rgdal")
library("zoo")
library("INLA")


# ---- Function definitions ----


#' Operations for handling the full INLA model call
#'
#' Abstracted operations for handling data prior to the INLA model call.
#' Mainly split for abstractions related to switching between a full model run
#' and a validation run.
#'
#' @param cfg Parsed configuration file data
#' @return NULL
run_inla_full <- function(cfg) {
  
  ########################################## Load PfPR point file  #####################################################
  pr_point_table <- read.csv(cfg$pr_point_with_cov_file) # the file produced in the previous step that contains the PR points linked to all covariates, intervnetions, and baseline PR
  
  # Read in the admin raster
  admin_raster                      <- raster(cfg$admin_raster_file) #load Africa raster
  NAvalue(admin_raster)             <- (-9999)
  
  #Make the cnn raster is necessary for each country to have an iid
  cnn                               <- renumber_gaul(admin_raster)  # Create an admin raster with countries renumbered to be 1:55
  
  ########################################### Prepare data for the model ###############################################
  
  # Transform the pr_point_table and add interaction terms - could move to step 1 as this is just a table modification
  pr_point_table$PfPr_logit  <- emplogit(pr_point_table$PfPr, pr_point_table$Nexamined)
  pr_point_table$interact    <- (-interact(pr_point_table$itnavg4, pr_point_table$bpr))
  pr_point_table$irsinteract <- (-pr_point_table$irs)
  pr_point_table$actinteract <- (-pr_point_table$act)
  pr_point_table$smcinteract <- (-pr_point_table$smc)
  pr_point_table$bpr_emp     <- emplogit(pr_point_table$bpr, 1000)
  
  # The formula which will be used in INLA model later
  form                    <- as.formula(paste0('FILEPATH', paste('Pf_V',1:20, sep='', collapse='+'), '+ bpr_emp + interact + irsinteract + actinteract + smcinteract'))
  design_matrix           <- model.matrix(form, data=as.data.frame(pr_point_table))
  colnames(design_matrix) <- paste0('V', 1:ncol(design_matrix)) # Creates V1.V26
  
  # Coverting to xyz cordinates and adding it to the pr_point_table
  xyz            <- ll_to_xyz(pr_point_table[,c('longitude', 'latitude')])
  pr_point_table <- cbind(pr_point_table, xyz)
  comb           <- rbind(pr_point_table[,c('longitude', 'latitude')])
  xyz            <- as.data.frame(ll_to_xyz(comb))
  un             <- paste(xyz$x, xyz$y, xyz$z, sep=':')
  dup            <- !duplicated(un)
  
  # change from max_edge being passed from config to derived here
  max_edge <- cfg$inla$inla_range / 10 * pi / 180
  
  
  ## Make the mesh 
  mesh <- inla.mesh.2d(loc       = cbind(xyz[dup,'x'], xyz[dup,'y'], xyz[dup,'z']),
                       cutoff    = cfg$inla$cutoff_val * max_edge,          
                       min.angle = cfg$inla$min_angle_val,          # The smallest allowed triangle angle. One or two values. (Default=21)
                       max.edge  = c(1,5) * max_edge,               # The largest allowed triangle edge length. One or two values.
                       crs       = inla.CRS("sphere"))              
  
  # Building the SPDE model on the mesh
  prior_range0  <- cfg$inla$inla_range   
  prior_sigmau0 <- log(1.1) 
  spde <- inla.spde2.pcmatern(mesh, 
                              prior.range = c(prior_range0 * (pi / 180), 0.1),  # p(spatial range < prior.range0)
                              prior.sigma = c(prior_sigmau0, 0.01))             # p(sigma > prior.sigma.u0)
  
  # parameter of the process,
  pr_point_table$gyear                                                       <- pr_point_table$year
  pr_point_table$gyear[pr_point_table$gyear < cfg$constant$first_model_year] <- cfg$constant$first_model_year # collapse 1995-1999 to 2000
  

  last_model_year <- cfg$constant$last_model_year
  max_pr_point_year <- max(pr_point_table$gyear, na.rm = TRUE)
  if (max_pr_point_year < cfg$constant$last_model_year) {
    print(paste0("The last model year is higher than the highest PR point year, so set it back to ", max_pr_point_year))
    last_model_year <- max_pr_point_year
  }
  print(paste0("Last model year is ", last_model_year))
  mesh1d          <- inla.mesh.1d(seq(cfg$constant$first_model_year, last_model_year, by = cfg$inla$by_val), interval=c(cfg$constant$first_model_year, last_model_year), degree = cfg$inla$degree_val, boundary=c('free'))
  est_cov         <- as.list(as.data.frame(design_matrix))
  iid             <- list(iid=raster::extract(cnn,cbind(pr_point_table$longitude, pr_point_table$latitude)))
  
  # Projection matrix that projects the spatially continuous Gaussian random field from the observations to the mesh nodes.
  A_est           <- inla.spde.make.A(mesh, loc=cbind(pr_point_table[,'x'], pr_point_table[,'y'], pr_point_table[,'z']), group=pr_point_table[,'gyear'], group.mesh=mesh1d)
  
  #  Generate the index set for the SPDE model.This creates a list with vector s equal to 1:spde$n.spde,
  #  and vectors s.group and s.repl that have all elements equal to 1s and length given by the number of mesh vertices.
  field_indices   <- inla.spde.make.index("field", n.spde=mesh$n, n.group=mesh1d$m)
  
  # Stack with pr_point_table for estimation and prediction
  stack_est       <- inla.stack(data          = list(response=pr_point_table$PfPr_logit), # list of pr_point_table vectors
                                A             = list(A_est, 1),                           # list of projection matrices
                                effects       = list(c(field_indices), c(est_cov,iid)),   # list with fixed and random effects
                                tag           = "est",
                                remove.unused = TRUE,
                                compress      = TRUE)
  
  ## prior on rho
  # P(cor > 0) = 0.99
  h_spec <- list(rho = list(prior = 'pc.cor1', param = c(cfg$constant$h_spec_low, cfg$constant$h_spec_high)))  
  
  # PC prior on the precision of AR(1)
  # P(sigma > sigma0) = alpha
  prec_prior <- list(prior = 'pc.prec', param = c(cfg$constant$prec_prior_high, cfg$constant$prec_prior_low))
  
  formula         <- as.formula(paste(
    paste("response ~ -1 + "),
    paste("f(field, model=spde, group=field.group, control.group=list(model='ar1', hyper=h_spec))+", sep=''),
    paste("f(iid, model='iid') +", sep=""),
    paste(paste('V', 1:(ncol(design_matrix)), sep=''), collapse='+'),
    sep=''))
  
  ########################################### Run the INLA model ###############################################
  #### Call INLA and get results ####
  mod_pred        <- inla(formula,
                          data              = inla.stack.data(stack_est),
                          family            = 'gaussian',
                          ################ PRIORS ##########################
                          control.fixed     = list(mean = list(default = 0), prec = list(default = 0.001)),
                          control.family    = list(hyper = list(prec = prec_prior)),
                          ######## END PRIORS ADDITIONS ####################
                          
                          ##################################################
                          # control.mode     = list(theta         = thetac,
                          #                         restart       = TRUE,
                          #                         fixed         = FALSE),
                          ##################################################
                          control.predictor = list(A             = inla.stack.A(stack_est),
                                                   compute       = TRUE,
                                                   quantiles     = NULL),
                          control.compute   = list(cpo           = TRUE,
                                                   dic           = TRUE,
                                                   config        = TRUE),
                          keep              = FALSE,
                          verbose           = TRUE,
                          #num.threads       = 5,
                          control.inla      = list(strategy      = 'gaussian',
                                                   int.strategy  = 'eb',
                                                   verbose       = TRUE,
                                                   fast          = TRUE,
                                                   dz            = 1,
                                                   step.factor   = 1,
                                                   stupid.search = FALSE))
  
  ## Produce and/or write out the model diagnostics and plots
  save.image(file=paste0('FILEPATH/INLA.model.rdata'))   # Save all the variables so we can check output objects
  save(mod_pred, A_est, pr_point_table, cnn, mesh, mesh1d, file=FILEPATH)
  
  # For scatter plots, RMSE, and pseudo R^2
  index         <- inla.stack.index(stack_est,"est")$data
  pred_logit    <- mod_pred$summary.fitted.values$mean[index]
  # pred_logit    <- mod_pred$summary.linear.predictor$mean[index] # yields identical results to $summary.fitted.values
  pred          <- exp(pred_logit)/(1 + exp(pred_logit)) # convert from logit form
  residuals     <- pr_point_table$pf_pr - pred
  MSE           <- mean(residuals^2)
  RMSE          <- mean((residuals^2)^0.5)
  mean_fit      <- mean(pred)
  rr            <- pr_point_table$pf_pr - rep(mean_fit, length(pred))
  pseudo_R2     <- 1 - (sum(residuals^2) / sum(rr^2))
  spde_res2     <- inla.spde2.result(mod_pred, "field", spde)          # for the GC distance plot
  range         <- spde_res2$marginals.range.nominal$range.nominal.1   # for the GC distance plot
  pit           <- mod_pred$cpo$pit
  uniquant      <- (1:length(pit))/(length(pit)+1)
  palette       <- hcl.colors(30, palette = "inferno")
  
  # Create a copy of the mesh object
  mesh_swap <- mesh
  
  # Swap z and x coordinates in the 'loc' matrix of mesh_swap (this turns around the 3D mesh)
  temp <- mesh_swap$loc[, 3]
  mesh_swap$loc[, 3] <- mesh_swap$loc[, 1]
  mesh_swap$loc[, 1] <- temp
  
  # Write the model summary in a text file
  summary.filename <- paste0(FILEPATH, '/model_summary.txt')
  cat('INLA Model Summary:\n', file=summary.filename)
  capture.output(summary(mod_pred), file=summary.filename, append=TRUE)
  
  # Open a PDF for writing outputs
  pdf(file = paste0(FILEPATH, "/diagnostics.pdf"), onefile = TRUE)
  plot(pr_point_table$PfPr_logit, pred_logit, main='In Sample Validation of Logit PfPR', xlab='Measured', ylab='Modelled', pch='·')
  smoothScatter(pr_point_table$PfPr_logit, pred_logit, main='In Sample Validation of Logit PfPR', xlab='Measured', ylab='Modelled', colramp = colorRampPalette(palette), pch='·')
  plot(pr_point_table$pf_pr, pred, main='In Sample Validation of PfPR', xlab='Measured', ylab='Modelled', pch='·')
  text(0.1, 0.95, paste('Pseudo R^2 = ', round(pseudo_R2,3), sep=''), cex = .8)
  text(0.1, 0.9, paste('RMSE = ', round(RMSE,3), sep=''), cex = .8)
  text(0.1, 0.85, paste('MSE = ', round(MSE,3), sep=''), cex = .8)
  abline(0,1, col='green')
  smoothScatter(pr_point_table$pf_pr, pred, main='In Sample Validation of PfPR', xlab='Measured', ylab='Modelled',  pch='·')
  text(0.1, 0.95, paste('Pseudo R^2 = ', round(pseudo_R2,3), sep=''), cex = .8)
  text(0.1, 0.9, paste('RMSE = ', round(RMSE,3), sep=''), cex = .8)
  text(0.1, 0.85, paste('MSE = ', round(MSE,3), sep=''), cex = .8)
  abline(0,1, col='green')
  hist(residuals, xlim=c(-1,1), xlab='PfPR Residual', main='Histogram of PfPR Residuals')
  plot(gc_dist(range[,1]), range[,2], main='GC Distance', xlab='Range 1', ylab='Range 2')
  plot(log(uniquant/(1-uniquant)), log(sort(pit)/(1-sort(pit))), main='PIT Distribution - Logit Scale', xlab='Uniform Quantiles', ylab='Sorted PIT Values')
  plot(uniquant, sort(pit), main='PIT Distribution', xlab='Uniform Quantiles', ylab='Sorted PIT Values')
  hist(pit, breaks=8, main="Histogram of PIT",xlab="PIT",freq =FALSE) # uniformly distibuted denote better fit # ***
  abline(h=1,col="red")
  plot(pred, pit, main='Residuals vs. Fitted', xlab='Posterior fitted means', ylab='PIT', pch='·')
  smoothScatter(pred, pit, main='Residuals vs. Fitted', xlab='Posterior fitted means', ylab='PIT',colramp = colorRampPalette(palette), pch='·')
  plot(mesh_swap, draw.edges = TRUE, edge.color = "black", lwd = 0.4, main = "") # Plotting the mesh
  title(main = "Mesh Visualization: z and y coordinates")
  
  # Close the PDF graphics device
  dev.off()
  
  return(invisible(NULL))
}


#' Operations for handling the validation INLA model call
#'
#' Identical to the full run with these exceptions:
#' (1) Additional 'crossfold' input parameter (i.e., an integer from 1 to 10)
#' (2) Code to PREDICTABLY subset the PR point table in a way that the holdout data can be resonstituted later
#' (3) Makes holdout sets without repeats (i.e., each point is held out only once across the 10 iterations)
#' (4) It does this for 10% subsets using random indies, countries, surveys, and years.
#'
#' @param cfg Parsed configuration file data
#' @return NULL
run_inla_validation <- function(cfg) {
  
########################################## Load PfPR point file  #####################################################
  
  pr_point_table <- read.csv(FILEPATH) # the file produced in the previous step that contains the PR points linked to all covariates, intervnetions, and baseline PR
  
################################ Predictably Group the PfPR point table  to support subsetting #######################
  
  #create index column 
  pr_point_table$random_selection <- 1:nrow(pr_point_table)
  
  # create a vector of holdout alternatives
  holdout_names <- c("random_selection","country_id", "dhs_survey_id", "year_start")
  
  set.seed(120)
  # Loop over the four variables for holdout
  for (i in 1:length(holdout_names)) {
    
    j <- holdout_names[i]
    
    # Add a new 'holdout' column. One for holdout, the other for crossfolds
    pr_point_table$hold <- 0
    pr_point_table$holdout <- 0
    
    # Convert the variable to a factor and extract values
    pr_point_table$hold <- as.factor(pr_point_table[[j]])
    
    # Get unique subsetting values
    row_indices <- unique(pr_point_table$hold)
    
    # Randomly reorder the row numbers
    row_indices <- sample(row_indices)
    
    # Calculate sample size
    sample_size <- floor(length(row_indices) * 0.1)
    
    # List to store samples
    samples <- vector("list", 10)
    
    # Loop to create 10 sequential samples
    for (crossfold in 1:10) {
      # Calculate the starting and ending indices for the current sample
      start_index <- (crossfold - 1) * sample_size + 1
      end_index <- crossfold * sample_size
      
      # Extract the current sample from the vector
      holdout_indices <- row_indices[start_index:end_index]
      
      pr_point_table$holdout[pr_point_table$hold %in% holdout_indices] <- crossfold
      
      samples[[crossfold]] <- holdout_indices
    
    }
    # #######################printing the subsetted indices for each variable & crossfolds to a file####################
    
    # # Open the text file for writing
    # if(j!= "random_selection"){
    holdout_summary <- paste0(FILEPATH, '/cross-validation-holdouts_summary.txt')
    
    # Print the samples
    for (crossfold in 1:10) {
      if (j != "random_selection") {
        sample_text <- paste('Holdout', crossfold, j, ':', samples[[crossfold]], "\n")
        cat(sample_text, file = holdout_summary, append = TRUE, "\n")
        cat("\n")
      }
    }
  ########################################## Load and prep admin raster ################################################
  
  # Read in the admin raster
  admin_raster                      <- raster(FILEPATH) #load Africa raster
  NAvalue(admin_raster)             <- (-9999)
  
  # A legacy hack to temporarily set the GAUL for Rep. of Congo to the GAUL for DRC
  admin_raster[admin_raster == 59]  <- 68     # modify the gaul number
  
  #Make the cnn raster is necessary for each country to have an iid
  cnn                               <- renumber_gaul(admin_raster)  # Create an admin raster with countries renumbered to be 1:55
  
  # Reload load raster to remove Rep. of Congo fix
  admin_raster                      <- raster(FILEPATH)
  
  ########################################### Prepare data for the model ###############################################
  
  # Transform the pr_point_table and add interaction terms - could move to step 1 as this is just a table modification
  pr_point_table$PfPr_logit  <- emplogit(pr_point_table$PfPr, pr_point_table$Nexamined)
  pr_point_table$interact    <- (-interact(pr_point_table$itnavg4, pr_point_table$bpr))
  pr_point_table$irsinteract <- (-pr_point_table$irs)
  pr_point_table$actinteract <- (-pr_point_table$act)
  pr_point_table$smcinteract <- (-pr_point_table$smc)
  pr_point_table$bpr_emp     <- emplogit(pr_point_table$bpr, 1000)
  
  # The formula which will be used in INLA model later
  form                    <- as.formula(paste0('FILEPATH', paste('Pf_V',1:20, sep='', collapse='+'), '+ bpr_emp + interact + irsinteract + actinteract + smcinteract'))
  design_matrix           <- model.matrix(form, data=as.data.frame(pr_point_table))
  colnames(design_matrix) <- paste0('V', 1:ncol(design_matrix)) # Creates V1..V26
  
  # Coverting to xyz cordinates and adding it to the pr_point_table
  xyz            <- ll_to_xyz(pr_point_table[,c('longitude', 'latitude')])
  pr_point_table <- cbind(pr_point_table, xyz)
  comb           <- rbind(pr_point_table[,c('longitude', 'latitude')])
  xyz            <- as.data.frame(ll_to_xyz(comb))
  un             <- paste(xyz$x, xyz$y, xyz$z, sep=':')
  dup            <- !duplicated(un)
  
  # change from max_edge being passed from config to derived here
  max_edge <- cfg$inla$inla_range / 10 * pi / 180
  
  
  ## Make the mesh 
  mesh <- inla.mesh.2d(loc       = cbind(xyz[dup,'x'], xyz[dup,'y'], xyz[dup,'z']),
                       cutoff    = cfg$inla$cutoff_val * max_edge,          
                       min.angle = cfg$inla$min_angle_val,          # The smallest allowed triangle angle. One or two values. (Default=21)
                       max.edge  = c(1,5) * max_edge,               # The largest allowed triangle edge length. One or two values.
                       crs       = inla.CRS("sphere"))              
  
  # Building the SPDE model on the mesh
  prior_range0  <- cfg$inla$inla_range   
  prior_sigmau0 <- log(1.1) 
  spde <- inla.spde2.pcmatern(mesh, 
                              prior.range = c(prior_range0 * (pi / 180), 0.1),  # p(spatial range < prior.range0)
                              prior.sigma = c(prior_sigmau0, 0.01))             # p(sigma > prior.sigma.u0)
  
  # parameter of the process,
  pr_point_table$gyear                                                       <- pr_point_table$year
  pr_point_table$gyear[pr_point_table$gyear < cfg$constant$first_model_year] <- cfg$constant$first_model_year # collapse 1995-1999 to 2000
  
  #INLA:::inla.binary.install() # Linux only, potential replacemnt for previous line if that one stops working
  mesh1d          <- inla.mesh.1d(seq(cfg$constant$first_model_year, last_model_year, by = cfg$inla$by_val), interval=c(cfg$constant$first_model_year, last_model_year), degree = cfg$inla$degree_val, boundary=c('free'))
  est_cov         <- as.list(as.data.frame(design_matrix))
  iid             <- list(iid=raster::extract(cnn,cbind(pr_point_table$longitude, pr_point_table$latitude)))
  
  # Projection matrix that projects the spatially continuous Gaussian random field from the observations to the mesh nodes.
  A_est           <- inla.spde.make.A(mesh, loc=cbind(pr_point_table[,'x'], pr_point_table[,'y'], pr_point_table[,'z']), group=pr_point_table[,'gyear'], group.mesh=mesh1d)
  
  #  Generate the index set for the SPDE model.This creates a list with vector s equal to 1:spde$n.spde,
  #  and vectors s.group and s.repl that have all elements equal to 1s and length given by the number of mesh vertices.
  field_indices   <- inla.spde.make.index("field", n.spde=mesh$n, n.group=mesh1d$m)
  
  ########################################### Run the INLA model for the crossfolds ####################################
  full_pr_point_table <- pr_point_table
  
  for (crossfold in 1:10){
    # crossfold <- 1 # for testing
    if (crossfold > 1) { pr_point_table <- full_pr_point_table }
    
    # Set the response values in the rows to be held out to NA. The model will still make predictions for these rows.
    pr_point_table$PfPr_logit[pr_point_table$holdout == crossfold] <- NA
    
    # Stack with pr_point_table for estimation and prediction
    stack_est       <- inla.stack(data          = list(response=pr_point_table$PfPr_logit), # list of pr_point_table vectors
                                  A             = list(A_est, 1),                           # list of projection matrices
                                  effects       = list(c(field_indices), c(est_cov,iid)),   # list with fixed and random effects
                                  tag           = "est",
                                  remove.unused = TRUE,
                                  compress      = TRUE)
    ## prior on rho
    # P(cor > 0) = 0.99
    h_spec <- list(rho = list(prior = 'pc.cor1', param = c(cfg$constant$h_spec_low, cfg$constant$h_spec_high)))  
    
    # PC prior on the precision of AR(1)
    # P(sigma > sigma0) = alpha
    prec_prior <- list(prior = 'pc.prec', param = c(cfg$constant$prec_prior_high, cfg$constant$prec_prior_low))
    
    formula         <- as.formula(paste(
      paste("response ~ -1 + "),
      # paste("f(field, model=spde, group=field.group, control.group=list(model='ar1'))+", sep=''),
      paste("f(field, model=spde, group=field.group, control.group=list(model='ar1', hyper=h_spec))+", sep=''),
      paste("f(iid, model='iid') +", sep=""),
      paste(paste('V', 1:(ncol(design_matrix)), sep=''), collapse='+'),
      sep=''))
    
    #### Call INLA and save results ####
    mod_pred        <- inla(formula,
                            data              = inla.stack.data(stack_est),
                            family            = 'gaussian',
                            ################ PRIORS ##########################
                            control.fixed     = list(mean = list(default = 0), prec = list(default = 0.001)),
                            control.family    = list(hyper = list(prec = prec_prior)),
                            ######## END PRIORS ADDITIONS ####################
                            ##################################################
                            # control.mode     = list(theta         = thetac,
                            #                         restart       = TRUE,
                            #                         fixed         = FALSE),
                            ##################################################
                            control.predictor = list(A             = inla.stack.A(stack_est),
                                                     compute       = TRUE,
                                                     quantiles     = NULL),
                            control.compute   = list(cpo           = TRUE,
                                                     dic           = TRUE,
                                                     config        = TRUE),
                            keep              = FALSE,
                            verbose           = TRUE,
                            #num.threads       = 5,
                            control.inla      = list(strategy      = 'gaussian',
                                                     int.strategy  = 'eb',
                                                     verbose       = TRUE,
                                                     fast          = TRUE,
                                                     dz            = 1,
                                                     step.factor   = 1,
                                                     stupid.search = FALSE))
    
    # Save just the variables needed for step 4
    save(mod_pred, A_est, pr_point_table, cnn, mesh, mesh1d, file=paste(FILEPATH, '/INLA.model.params.cross-validation_K',crossfold,'_',j,'.rdata', sep=''))
    save.image(file=paste0(FILEPATH,'/INLA.model.rdata'))   # Save all the variables so we can check output objects
    
    # For scatter plots, RMSE, and pseudo R^2
    index         <- inla.stack.index(stack_est,"est")$data
    pred_logit    <- mod_pred$summary.fitted.values$mean[index]
    pred          <- exp(pred_logit)/(1 + exp(pred_logit)) # convert from logit form
    pred_in       <- pred[-holdout_indices]
    pred_out      <- pred[holdout_indices]
    residuals     <- pr_point_table$pf_pr - pred
    residuals_in  <- residuals[-holdout_indices]
    residuals_out <- residuals[holdout_indices]
    
    # Calculate the stats for the in and out of sample
    holdout_indices <- which(pr_point_table$holdout == crossfold)
    MSE_in          <- mean(residuals_in^2)
    MSE_out         <- mean(residuals_out^2)
    RMSE_in         <- mean((residuals_in^2)^0.5)
    RMSE_out        <- mean((residuals_out^2)^0.5)  
    mean_fit_in     <- mean(pred[-holdout_indices])
    mean_fit_out    <- mean(pred[holdout_indices])    
    rr_in           <- pr_point_table$pf_pr[-holdout_indices] - rep(mean_fit_in, length(pred_in))
    rr_out          <- pr_point_table$pf_pr[holdout_indices] - rep(mean_fit_out, length(pred_out))
    pseudo_R2_in    <- 1 - (sum(residuals_in^2) / sum(rr_in^2))
    pseudo_R2_out   <- 1 - (sum(residuals_out^2) / sum(rr_out^2))
    
    # Make a new data frame of columns needed for plotting to facillitate grouping
    # Note that I get the PfPr_logit from the FULL table (i.e., without the values turned to NA) so that I can plot out of sample points
    plot_table           <- as.data.frame(cbind(full_pr_point_table$PfPr_logit, pred_logit, pr_point_table$pf_pr, pred, residuals))
    colnames(plot_table) <- c('PfPr_logit', 'pred_logit', 'pf_pr', 'pred', 'residuals') # retain names for consistency
    
    # Add a and populate a group column
    plot_table$group                  <- 'in_sample' 
    plot_table$group[holdout_indices] <- 'out_of_sample'
    
    # Write the model summary in a text file
    summary.filename <- paste0(FILEPATH, '/model_summary.cross-validation.K',crossfold,'_',j,'.txt')
    cat('INLA Model Summary:\n', file=summary.filename)
    capture.output(summary(mod_pred), file=summary.filename, append=TRUE)
    
    # Define the color palette for the smoothScatter plots
    palette <- hcl.colors(30, palette = "inferno")
    
    # Open a PDF for writing outputs
    pdf(file = paste0(FILEPATH, '/diagnostics.cross-validation.K',crossfold,'_',j,'.pdf'), onefile = TRUE)
    attach(plot_table)
    plot(PfPr_logit, pred_logit, main='Validation of Logit PfPR', xlab='Measured', ylab='Modelled', pch='·', col = factor(group))
    smoothScatter(PfPr_logit, pred_logit, main='Validation of Logit PfPR', xlab='Measured', ylab='Modelled', colramp = colorRampPalette(palette), pch='·')
    plot(pf_pr, pred, main='Validation of PfPR', xlab='Measured', ylab='Modelled', pch='·',col = factor(group))
    text(0.05, 0.95, paste('Pseudo R^2 (in sample) = ', round(pseudo_R2_in,3), '        ', 'Pseudo R^2 (out of sample) = ', round(pseudo_R2_out,3), sep=''), cex = .5, adj = c(0,0))
    text(0.05, 0.90, paste('RMSE (in sample) = ', round(RMSE_in,3), '        ', 'RMSE (out of sample) = ', round(RMSE_out,3), sep=''), cex = .5, adj = c(0,0))
    text(0.05, 0.85, paste('MSE (in sample) = ', round(MSE_in,3), '        ', 'MSE (out of sample) = ', round(MSE_out,3), sep=''), cex = .5, adj = c(0,0))
    abline(0,1, col='green')
    smoothScatter(pf_pr, pred, main='In Sample Validation of PfPR', xlab='Measured', ylab='Modelled',  pch='·')
    text(0.05, 0.95, paste('Pseudo R^2 (in sample) = ', round(pseudo_R2_in,3), '        ', 'Pseudo R^2 (out of sample) = ', round(pseudo_R2_out,3), sep=''), cex = .5, adj = c(0,0))
    text(0.05, 0.90, paste('RMSE (in sample) = ', round(RMSE_in,3), '        ', 'RMSE (out of sample) = ', round(RMSE_out,3), sep=''), cex = .5, adj = c(0,0))
    text(0.05, 0.85, paste('MSE (in sample) = ', round(MSE_in,3), '        ', 'MSE (out of sample) = ', round(MSE_out,3), sep=''), cex = .5, adj = c(0,0))
    abline(0,1, col='green')
    hist(residuals, xlim=c(-1,1), xlab='All PfPR Residual', main='Histogram of PfPR Residuals')
    # Close the PDF graphics device
    dev.off()
    
    # Retain the predicted values for all crossfolds in a combined table   
    if (crossfold == 1) {
      crossfold_table <- as.data.frame(cbind(plot_table$PfPr_logit, plot_table$pf_pr, pr_point_table$holdout, plot_table$pred_logit, plot_table$pred))
      colnames(crossfold_table) <- c('PfPr_logit','pf_pr', 'holdout')
    } else {
      crossfold_table <- cbind(crossfold_table, plot_table$pred_logit, plot_table$pred)
    }
    colnames(crossfold_table)[(ncol(crossfold_table)-1)] <- paste0('pred_logit_', crossfold)
    colnames(crossfold_table)[ncol(crossfold_table)]     <- paste0('pred_', crossfold)  
  }
  
  
  # Write out the holdout validation table 
  crossfold.file <- paste0(FILEPATH, '/cross-validation.all_',j,'.predictions.csv')
  write.csv(crossfold_table, crossfold.file)
  
  }
  return(invisible(NULL))
  
}


# ---- Main function definition ----


#' Step 02 -- Main function
#'
#' Essentially a wrapper to check existence of IO paths and to switch
#' operations depending on whether the run is a full run or a validation run.
#'
#' @param cfg Parsed configuration file data
#' @return NULL
run_inla_model <- function(cfg) {
  
  
  time_start <- as.numeric(Sys.time())
  cat("Starting step 02\n")
  
  
  # ---- Source required functions ----
  
  
  source(FILEPATH)
  
  
  # ---- Validate file and directory existence ----
  
  
  input_files <- c(
    cfg$admin_raster_file,
    cfg$functions_source_file,
    cfg$pr_point_with_cov_file)
  
  # Check that all input files exist
  sapply(input_files, FUN = check_exists_file)
  
  # Ensure that the output directories exist
  Map(
    ensure_directory,
    c(
      # Config value is "FILEPATH/basename.ext", so get only dirname
      dirname(cfg$model_file),
      # Cross-validation plot dir -- also captures `plot_path`
      paste0(FILEPATH, "/cross_validation")
    )
  )

  
  # ---- Operations ----
  
  if (cfg$inla_run_validation) {
    run_inla_validation(cfg)
  } else {
    run_inla_full(cfg)
  }
  
  cat(sprintf(
    "Step 02 complete in %.2f minutes\n",
    (as.numeric(Sys.time()) - time_start) / 60
  ))
  
  return(invisible(NULL))
  
}


# ---- Entrypoint ----


if (sys.nframe() == 0) {
  
  tryCatch(source("FILEPATH/read_config.R"), error = stop)
  
  args <- commandArgs(trailingOnly = TRUE)
  
  config_file_path <- "FILEPATH/config_full_cube.toml"
   
  if (length(args) == 0) {
    print("No configuration file provided - using default")
  } else if (length(args) > 1) {
    stop("Too many arguments provided")
  } else {
    config_file_path <- args[[1]]
  }
  
  cfg <- read_config(config_file_path)
  
  run_inla_model(cfg)
  
}

