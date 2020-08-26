jobnum <- commandArgs()[3]
opt_type <- commandArgs()[4]

in_dir <- 'FILEPATH'
out_dir <- 'FILEPATH'

.libPaths('FILEPATH')
library(seegSDM)
library(gbm)
source('FILEPATH')    # eco-niche stats


#################################################################
## CONSTRUCT PYTHON SYSTEM COMMAND TO RUN 'optimizers.py'

run_optimizerPy <- function(funcs.file_path,
                            funcs.file,
                            bounds.file_path,
                            bounds.file,
                            data.file_path,
                            data.loc,
                            optimizer,
                            learner,
                            cv_folds,
                            n_calls,
                            jobnum)
{
  system(
    paste0('FILEPATH ', 
           funcs.file_path, funcs.file,
           bounds.file_path, bounds.file, 
           data.file_path, data.loc,
           optimizer, 
           learner,
           cv_folds,
           n_calls,
           jobnum)
  )
}
#################################################################

# read in covariates and data
dat_all <- read.csv('FILEPATH'))
covs <- brick('FILEPATH')
covs <- dropLayer(covs, 6)    # drop gecon

## MAKE .CSV OF OPTIMIZED HPARS

# split to make OoS hold-out data
indx <- sample(nrow(dat_all), 0.70*nrow(dat_all))
data_train <- dat_all[indx,]
data_test <- dat_all[-indx,]
write.csv(data_train, file = 'FILEPATH', row.names=F)

# run optimization in python via system
run_optimizerPy(funcs.file_path <- 'FILEPATH'),
                funcs.file <- 'optimizers.py ',
                bounds.file_path <- 'FILEPATH'),
                bounds.file <- 'space_bounds.csv ',
                data.file_path <- out_dir,
                data.loc <-paste0('/FILEPATH', ' '),
                optimizer <- paste0(opt_type, ' '),
                learner <- 'brtR ',
                cv_folds <- '10 ',
                n_calls <- '150 ',
                jobnum <- jobnum)

# delete extraneous file
file.remove(paste0('FILEPATH/data_train_', jobnum, '.csv'))

## RUN BRT MODEL

# define weighting function (bernoulli)
wt.func <- function(PA) ifelse(PA == 1, 1, sum(PA) / sum(1 - PA))
wt.train <- wt.func(data_train[,1])
wt.test <- wt.func(data_test[,1])

# load optimized hyperparameter values
par <- read.csv('FILEPATH'))

# run model fit on the training data
model_train <- gbm(data_train[,1]~.,
                   distribution = 'bernoulli',
                   data = data_train[,5:ncol(data_train)], 
                   n.trees = par$n.trees, 
                   interaction.depth = par$interaction.depth,
                   shrinkage = par$shrinkage,
                   n.minobsinnode = par$n.minobsinnode,
                   weights = wt.train,
                   bag.fraction = 0.75,
                   train.fraction = 0.70,
                   verbose = F)

# get fit statistics for the training data
train.pred <- predict.gbm(model_train,
                          data_train[,5:ncol(data_train)], 
                          n.trees = model_train$n.trees, 
                          type = 'response')
stats_train <- calcStats(data.frame(data_train[,1], train.pred))

# run model fit on the testing data
model_test <- gbm(data_test[,1]~.,
                  distribution = 'bernoulli',
                  data = data_test[,5:ncol(data_test)], 
                  n.trees = par$n.trees, 
                  interaction.depth = par$interaction.depth,
                  shrinkage = par$shrinkage,
                  n.minobsinnode = par$n.minobsinnode,
                  weights = wt.test,
                  bag.fraction = 0.75,
                  train.fraction = 0.70,
                  verbose = F)

# get fit statistics for the test data
test.pred <- predict.gbm(model_test,
                         data_test[,5:ncol(data_test)], 
                         n.trees = model_test$n.trees, 
                         type = 'response')
stats_test <- calcStats(data.frame(data_test[,1], test.pred))

save(stats_test, file = 'FILEPATH')
save(stats_train, file = 'FILEPATH')

# run final model
wt <- wt.func(dat_all[,1])
model <- gbm(dat_all[,1]~.,
             distribution = 'bernoulli',
             data = dat_all[,5:ncol(dat_all)], 
             n.trees = par$n.trees, 
             interaction.depth = par$interaction.depth,
             shrinkage = par$shrinkage,
             n.minobsinnode = par$n.minobsinnode,
             weights = wt,
             bag.fraction = 0.75,
             train.fraction = 0.70,
             verbose = F)

## final model results:
# effect curves
effects <- lapply(1:length(5:ncol(dat_all)),
                  function(i) {
                    plot(model, i, return.grid = TRUE)
                  })

# get relative influence
relinf <- summary(model, plotit = FALSE)

# get prediction raster
pred.raster <- predict(covs, model, type = 'response', n.trees = model$n.trees)

# get coordinates
coords <- data_train[,2:3]

# format like seegSDM::runBRT() result
model.list <- list(model = model,
                   effects = effects,
                   relinf = relinf,
                   pred = pred.raster,
                   coords = coords)
save(model.list, file = 'FILEPATH')

# get fit statistics
gbm.pred <- predict.gbm(model,
                        dat_all[,5:ncol(data_train)], 
                        n.trees = model_train$n.trees, 
                        type = 'response')
stats <- calcStats(data.frame(dat_all[,1], gbm.pred))

# save fit statistics and prediction raster
save(stats, file = 'FILEPATH')
preds <- writeRaster(pred.raster, 
                     'FILEPATH', 
                     format = 'GTiff')

