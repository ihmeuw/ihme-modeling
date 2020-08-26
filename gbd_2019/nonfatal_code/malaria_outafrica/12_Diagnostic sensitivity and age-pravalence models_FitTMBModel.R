# ----------------------------------------------------------- #
# Fit TMB spatial Binomial regression
#   of indian malaria risk
# For GBD analysis
#
#
# Author: *****
# Date: 2016-10-26
# Requires:
#   TMB devtools::install_github(ADDRESS)
#   INLA mesh (ADDRESS)
#   INLA ADDRESS
#   Covariate data (../data)
#   Response data (../data)
# ----------------------------------------------------------- #


# ----------------------------------------------------------- #
# Table of contents:
#   Read in data and define parameters
#   Fit model
#   Diagnostic plots
# ----------------------------------------------------------- #

#setwd('FILEPATH')
#setwd('FILEPATH')

#setwd('FILEPATH')

# load('FILEPATH/indiaMesh.RData')
# useiso3 = c('IND', 'BGD', 'LKA')
# usecountry = c('India', 'BanglUSER', 'Sri Lanka')

#'@param runname Character giving the name for this model run. Is used to write out models etc.
#'@param mesh The INLA mesh used.
#'@param useiso3 Vector of ISO3 codes for countries to use API data for
#'@param useiso3_points Vector of ISO3 codes for countries to use point data for. DUSERts to same as API data.
#'@param subnatpath Relative or absolute path (character) to a csv file
#'                  containing the subnational API case data .
#'@param natpath Relative or absolute path (character) to a csv file
#'             containing the national API case data.
#'@param natcovspath Path to a csv containing the national covariate data.
#'@param its length 2 numeric vector giving the number of iterations for preliminary models with soft
#'           spatial prior (first element) and the final model (second element).
#'@param mediumPrior DUSERt (FALSE) runs a soft prior and full strength prior model.
#'                   If TRUE, run 3 soft prior models with increasing strength of spatial prior.
#'@param soft Length two vector giving the strength of spatial prior
#'            (effect of prior is soft[i] * negative log likelihood).
#'            Second element should almost always be 1 (full strength spatial prior).
#'@param medsoft Length two numeric. If mediumPrior = TRUE, these two values give the strength of
#'               the spatial prior for the additional two runs. Should adhere to:
#'               soft[1] < medsoft[1] < medsoft[2] < soft[2].
#'@param usePointData If FALSE, effect of pointdata likelihood is set to 0. Probably safer than setting
#'                    usecountry = NULL or something which will probably cause errors.
#'@param use_nationalcovars If FALSE, effect of national covariates is set to 0 (by fixing parameters at 0).
#'@param addPseudo dataframe in the same format as the csv defined by subnatpath. This can be used
#'                 to add any data, but is particularly aimed at adding pseudo absence subnational
#'                 API data points. Because lots of areas with API = 0 aren't measured, this might be
#'                 able to help deal with various issues.
#'@param startingvalues A vector (probably from opt$par of a previous run) giving starting values.
#'                      These aren't priors, just a (hopefully) good place to start the optimisation.
#'@param usepolygonyears If NULL, use all years. If a numeric vector ONLY use polygon API data from the
#'                       years defined in the vector. e.g. c(2012, 2013) will only use API data (national
#'                       and subnational) for the years 2012 and 2013.
#'@param filterpolygons A data frame defining API data to filter out. Each column name should match a
#'                      column in subnatpath. Each row gives a set of values to filter out for those
#'                      column names. NB, combinations of values are not created for you. e.g.
#'                      data.frame(shape_file_id = 91312476, year = 2015) will filter out all rows where
#'                      shape_file_id is 91312476 and year is 2015. If given
#'                      data.frame(shape_file_id = c(100, 101), year = c(2015, 2016)), data rows with
#'                      shape_fileid of 100 and year of 2016 would be LEFT IN, not filtered out. To remove
#'                      all data from 2015, 2016 and shape_file_ids 100 and 101, make a 4 row data.frame
#'                      giving all combinations.




fitmodel <- function(runname = 'india-test',
                     mesh = india.mesh,
                     useiso3 = c('IND', 'BGD', 'LKA'),
                     useiso3_points = useiso3,
                     subnatpath = 'FILEPATH',
                     natpath = 'FILEPATH',
                     natcovspath = 'FILEPATH',
                     pointcasespath = 'FILEPATH',
                     pointcovspath = 'FILEPATH',
                     its = c(400, 1000),
                     mediumPrior = FALSE,
                     soft = c(0.0001, 1),
                     medsoft = c(0.1, 0.2),
                     usePointData = FALSE,
                     use_nationalcovars = TRUE,
                     addPseudo = NULL,
                     startingValues = NULL,
                     usepolygonyears = NULL,
                     filterpolygons = NULL){


  dir.create(paste0('FILEPATH', runname), showWarnings = FALSE)
  #setwd('FILEPATH')

  library(INLA)
  library(splines)
  library(TMB)

  # install_github('ADDRESS') but only needed for plotting.
  library(INLAutils)

  library(dplyr)
  library(magrittr)
  library(reshape2)
  library(readr)
  library(lubridate)

  #library(readxl)


  source('FunctionsForTesting.R')


  set.seed(1298)

  # ----------------------------------------------------------- #
  # Read in covariate data.
  # ----------------------------------------------------------- #

  if(!exists('shapecovsfullread')){
    shapecovsfullread <- read_csv('FILEPATH') %>%
                           rename(shape_file = area_id)
  }

  pointcovs <- suppressMessages(read_csv(pointcovspath))

  #
  # length(unique(polygon_data$shape_file_id)) == length(unique(shapecovsfull$shape_file))
  # nrow(pointcases) == nrow(pointcovs)
  #

  # Read in case data and split years into blocks


  # ----------------------------------------------------------- #
  # Read in polygon case data and define likelihood types.
  # ----------------------------------------------------------- #

  subnational_data <- suppressMessages(read_csv(subnatpath) %>% mutate(api_sd_pf = NA))
  national_data <- suppressMessages(read_csv(natpath))
  polygon_data <- merge(subnational_data, national_data, all = TRUE)
  #polygon_data <- mutate(polygon_data, year = year(start_dt))

  if(!is.null(usepolygonyears)){
    polygon_data <- polygon_data %>%
                      filter(year %in% usepolygonyears)
  }

  # Filter polygons by flexible data.frame
  if(!is.null(filterpolygons)){
    polygonsrowstofilter <- list()
    for(i in 1:NROW(filterpolygons)){
      polygonsrowstofilter[[i]] <- which(apply(polygon_data[, names(filterpolygons), drop = FALSE], 1, function(x) all(x == filterpolygons[i, ])))
    }
    polygonsrowstofilter <- do.call(c, polygonsrowstofilter)
    polygonsrowstofilter <- na.omit(unique(polygonsrowstofilter))
    polygon_data <- polygon_data[-polygonsrowstofilter, ]
    message(paste('Filtering', length(polygonsrowstofilter), 'polygons'))
  }

  # ----------------------------------------------------------- #
  # Clean, wrangle etc.
  # ----------------------------------------------------------- #

  # Subset to correct countries only

  polygon_data <- polygon_data %>% filter(iso_3_code %in% useiso3)

  polygon_data <- polygon_data %>%
                    filter(pop_at_risk_pf > 0)

  message('Number of API points over 620: ', sum(polygon_data$api_mean_pf > 620, na.rm = TRUE))
  polygon_data$api_mean_pf[polygon_data$api_mean_pf > 620] <- 620
  #polygon_data <- polygon_data %>%


  # Remove NAs in case data (filter natioanlpoints first...)
  message('Number of NA API points : ', sum(is.na(polygon_data$api_mean_pf), na.rm = TRUE))

  polygon_data <- polygon_data %>%
    filter(!is.na(api_mean_pf))


  # Add pseudo 0s
  if(!is.null(addPseudo)){
    polygon_data <- rbind(polygon_data, addPseudo)
  }



  shapecovsfull <- shapecovsfullread %>% filter(shape_file %in% polygon_data$shape_file_id)

  # ----------------------------------------------------------- #
  # Read in point case data.
  # ----------------------------------------------------------- #

  pointcases <- suppressMessages(read_csv(pointcasespath))

  usepoints <- which(pointcases$iso_3_code %in% useiso3_points &
                       pointcases %>% dplyr::select(examined, positive) %>% complete.cases &
                       pointcases$year_start >= 1980)

  if(NROW(usepoints) == 0) stop('Zero points selected. This messes up building splines.')

  pointcases <- pointcases[usepoints, ]
  pointcovs <- pointcovs[usepoints, ]


  # Convert -9999 to NA, and remove non complete data.
  # GPW_2013 is a population layer, so shouldn't be used anyway.
  # No NAs in india point covs data.

  for(i in seq_len(ncol(shapecovsfull))){
    replacewithnas <- which(shapecovsfull[ , i] == -9999)
    shapecovsfull[replacewithnas, i] <- NA
    replacewithnas <- which(shapecovsfull[ , i] == 9999)
    shapecovsfull[replacewithnas, i] <- NA
  }

  # ignoring GP_2013, remove NA rows.
  shapecovsfull <- shapecovsfull[shapecovsfull %>% dplyr::select(-GPW_2013, -Pf_limits_MBGW2) %>% complete.cases, ]


  for(i in seq_len(ncol(pointcovs))){
    replacewithnas <- which(pointcovs[ , i] == -9999)
    pointcovs[replacewithnas, i] <- NA
    replacewithnas <- which(pointcovs[ , i] == 9999)
    pointcovs[replacewithnas, i] <- NA

  }


  # ignoring GP_2013, remove NA rows.
  missingpoints <- pointcovs %>% dplyr::select(-GPW_2013, -Pf_limits_MBGW2) %>% complete.cases
  pointcovs <- pointcovs[missingpoints, ]
  pointcases <- pointcases[missingpoints, ]


  # Remove case data that has no covariates (small islands)
  message('Number of polygons with no covariate data: ', sum(!polygon_data$shape_file_id %in% shapecovsfull$shape_file))
  polygon_data <- polygon_data %>% filter(shape_file_id %in% shapecovsfull$shape_file)

  # Reduce shapecovs to only shapes in cases
  shapecovs <- shapecovsfull %>% filter(shape_file %in% polygon_data$shape_file_id)


  DescribePolygon_data(polygon_data, useiso3)

  dim(shapecovs)

  # Create covariates only data and scale it. Small for now.



  shapecovs_scaled <- shapecovs %>%
    dplyr::select(-FrankenPop_2010_5km, -GPW_2013, -Pf_limits_MBGW2, -shape_file, -cellid, -x, -y) %>%
    scale

  covsscale <- data_frame(center = attr(shapecovs_scaled, "scaled:center"),
                          scale = attr(shapecovs_scaled, "scaled:scale"))

  # Apply scaling factors from shape covs scaling.
  pointcovs_scaled <- pointcovs %>%
    dplyr::select(-FrankenPop_2010_5km, -Pf_limits_MBGW2, -GPW_2013) %>%
    sweep(2, covsscale$center, `-`) %>%
    sweep(2, covsscale$scale, `/`) %>%
    as.matrix




  # ----------------------------------------------------------- #
  # Make national covariate data and match subsetted case data.
  # ----------------------------------------------------------- #

  nationalcovs <- suppressMessages(read_csv(natcovspath))

  # Hack. Use Comoros values for mayotte
  if('MYT' %in% polygon_data$iso_3_code){
    # Duplicate comoros as mayotte
    mayotte <- nationalcovs[nationalcovs$COUNTRY_ID == 'COM', ]
    mayotte$COUNTRY_ID <- 'MYT'
    mayotte$NAME <- 'Mayotte'
    # mayotte$GAUL_CODE <- NA
    nationalcovs <- rbind(nationalcovs, mayotte) %>% arrange(year, COUNTRY_ID)
  }

  # Convert shape matrix to data frames
  nationalcovs[, 5:ncol(nationalcovs)] <- scale(nationalcovs[, 5:ncol(nationalcovs)] )

  baselines <- nationalcovs %>%
    dplyr::filter(year == 2015) %>%
    dplyr::select(drug_covar:ncol(nationalcovs))

  baselinelong <- do.call(rbind, replicate(36, baselines, simplify = FALSE))


  nationalcovs[, 5:ncol(nationalcovs)] <- nationalcovs[, 5:ncol(nationalcovs)] - baselinelong

  # Morocco is duplicated for some reason. Take first of each.
  nationalcovs <- nationalcovs[!duplicated(subset(nationalcovs, select = c(NAME, year))), ]


  nationalshapecovs_scaled <- polygon_data %>%
    left_join(nationalcovs, by = c('iso_3_code' = 'COUNTRY_ID', 'year')) %>%
    dplyr::select(drug_covar:`WB_public_exp_covar$WB_public_exp_covar`) %>%
    as.matrix


  nationalpointcovs_scaled <- pointcases %>%
    left_join(nationalcovs, by = c('iso_3_code' = 'COUNTRY_ID', 'year_start' = 'year')) %>%
    dplyr::select(drug_covar:`WB_public_exp_covar$WB_public_exp_covar`) %>%
    as.matrix




  # Create  in startendindex matrix.
  startendindex <- lapply(unique(shapecovs$shape_file), function(x) range(which(shapecovs$shape_file == x))) %>% do.call(rbind, .)

  whichindices <- match(polygon_data$shape_file_id, unique(shapecovs$shape_file))

  startendindex <- startendindex[whichindices, ] - 1L


  # ----------------------------------------------------------- #
  # define likelihoods for shape data.
  # ----------------------------------------------------------- #


  # these are in units of per 1000 PYO

  # Create empty vectors: Indicator for whether to use normal or gamma likelihood, mean and sd of likelihood distributions.
  polygon_normal_likelihoods <- rep(NA_integer_, nrow(polygon_data))
  polygon_mean_incs <-          rep(NA, nrow(polygon_data))
  polygon_sd_incs <-            rep(NA, nrow(polygon_data))

  # Find the smallest, non zero sd. This is used for cases where sd == 0
  minsd <- na.omit((polygon_data$api_high_pf - polygon_data$api_low_pf)/(2 * 1.96))
  minsd <- min(minsd[minsd > 0])

  for (i in 1:nrow(polygon_data)) {
    if (polygon_data$api_mean_pf[i] > 0) {
      polygon_normal_likelihoods[i] <- 1L
      polygon_mean_incs[i] <- polygon_data$api_mean_pf[i]
      if(polygon_data$api_high_pf[i] != polygon_data$api_low_pf[i]){
        polygon_sd_incs[i] <- (polygon_data$api_high_pf[i] - polygon_data$api_low_pf[i])/(2 * 1.96)
      } else {
        polygon_sd_incs[i] <- minsd
      }
    } else {
      polygon_normal_likelihoods[i] <- 0L
      polygon_mean_incs[i] <- min(polygon_data$api_mean_pf[polygon_data$api_mean_pf > 0]) # make dependant on polygon.
      polygon_sd_incs[i] <- minsd
    }
  }

  polygon_sd_incs[!is.na(polygon_data$api_sd_pf) & polygon_data$api_sd_pf != 0] <- polygon_data$api_sd_pf[!is.na(polygon_data$api_sd_pf) & polygon_data$api_sd_pf != 0]

  ## check no standard deviations for normal likelihoods are zero

  min(polygon_sd_incs[polygon_normal_likelihoods == 1])




  # ----------------------------------------------------------- #
  # Read in india mesh
  # ----------------------------------------------------------- #
  #load('FILEPATH', verbose = TRUE)


  spde <- (inla.spde2.matern(mesh, alpha = 2)$param.inla)[c("M0", "M1", "M2")]
  Apix <- inla.mesh.project(mesh, loc = as.matrix(shapecovs[, c('x', 'y')]))$A
  Apoint <- inla.mesh.project(mesh, loc = as.matrix(pointcases[, c('longitude', 'latitude')]))$A


  # ----------------------------------------------------------- #
  # Build spline
  # ----------------------------------------------------------- #

  # Create spline that smooths over 4 spatial fields
  n_y <- length(1980:2016)
  temporal_spline_covariate <- bs(seq(0, 1, length.out = n_y), degree = 3, knots = c(0.1, 0.2, 0.3))
  temporal_spline_covariate[round(n_y * 0.4) : n_y, 4] <- temporal_spline_covariate[round(n_y * 0.4), 4]
  temporal_spline_covariate <- temporal_spline_covariate[n_y:1, 1:4] # this ensures spline goes through zero at row corresponding to year 2016 (to avoid degeneracy with baseline 2016 field)

  # Create shape data splines (ncases x 4)
  time_spline_shape <- temporal_spline_covariate[polygon_data$year - 1980 + 1, ]
  # Create point data splines (ncases x 4)
  time_spline_point <- temporal_spline_covariate[pointcases$year_start - 1980 + 1, ]



  # Create national level temporal spline
  # We don't have any data for 2016, so this year 2016 is forces to revert to mean, causing uptick.
  # So we don't want a node at 2016.
  temporal_spline_covariate_national <- bs(seq(0, 1, length.out = n_y + 1), degree = 4,
                                           knots = c(0.1, 0.6, 0.7, 0.8))
  temporal_spline_covariate_national <- temporal_spline_covariate_national[(n_y + 1):2, ]
  nat_spline_coefs <- ncol(temporal_spline_covariate_national)
  n_spline_slopes <- nat_spline_coefs * length(useiso3)

  # Aim is for a 7 x nCountries columns.
  # If a row is 1999, second country, all columns other than 8:14 will be zero. 8:14 will be 1999 row of temporal_spline_covariate_national
  countryindexshape <- factor(polygon_data$iso_3_code, levels = sort(useiso3))
  countryindexpoint <- factor(pointcases$iso_3_code, levels = sort(useiso3))
  countryindexshape <- as.numeric(countryindexshape)
  countryindexpoint <- as.numeric(countryindexpoint)



  time_spline_national_point <- matrix(0, nrow = nrow(pointcases), ncol = n_spline_slopes)
  for(r in 1:nrow(time_spline_national_point)){
    colstart <- countryindexpoint[r] * nat_spline_coefs - ncol(temporal_spline_covariate_national) + 1
    colend <- colstart + nat_spline_coefs - 1
    time_spline_national_point[r, colstart:colend] <-
      temporal_spline_covariate_national[pointcases$year_start[r] - 1979, ]
  }

  time_spline_national_shape <- matrix(0, nrow = nrow(polygon_data), ncol = n_spline_slopes)
  for(r in 1:nrow(time_spline_national_shape)){
    colstart <- countryindexshape[r] * nat_spline_coefs - ncol(temporal_spline_covariate_national) + 1
    colend <- colstart + nat_spline_coefs - 1
    time_spline_national_shape[r, colstart:colend] <-
      temporal_spline_covariate_national[polygon_data$year[r] - 1979, ]
  }


  # ----------------------------------------------------------- #
  # Collect data and fit model
  # ----------------------------------------------------------- #



  # Compile and load the model
  compile("multilikelihoodmodel.cpp")
  #compile("multilikelihoodmodel.cpp", "-O1 -g", DLLFLAGS="")

  dyn.load(dynlib("multilikelihoodmodel"))

  n_s <- nrow(spde$M0)

  if(is.null(startingValues)){

    # Hacky collect together some slopes start values from previous runs.
    # Slope params. Just put in if statement to avoid annoyance if I want to subset columns
    if(ncol(pointcovs_scaled) == 20){
      slopestart <- c(0.1, 0.2, 0, 0, 0.2, -0.1, -0.3, -0.7, 0.2, 0.2, -0.2, 0.4,
                      -0.1, 0.2, -0.01, 0.01, -0.5, 0.4, -0.4, -0.2)
    } else {
      slopestart <- rep(0.0, ncol(pointcovs_scaled))
    }


    nationalsplines <-  get_spline_starting_values(n_spline_slopes,
                                                   useiso3,
                                                   polygon_data,
                                                   nat_spline_coefs,
                                                   time_spline_national_shape,
                                                   temporal_spline_covariate_national,
                                                   plot = FALSE)



    # Use starting slope values from a previous analysis.
    parameters <- list(intercept = -2,
                       slope = slopestart,
                       nationalslope = rep(0.00, ncol(nationalshapecovs_scaled)),
                       nationalsplines = nationalsplines,
                       # hyperparameters
                       log_tau = 2,
                       log_kappa = -4,
                       nodemean = rep(1.000, n_s),
                       spline_nodes_one = rep(0.000, n_s),
                       spline_nodes_two = rep(0.000, n_s),
                       spline_nodes_three = rep(0.000, n_s),
                       spline_nodes_four = rep(0.000, n_s),
                       log_tau_spline = 2,
                       log_kappa_spline = -4,
                       mean_spline_coeffs = rep(0.0, 4))
  } else {
    parameters <- startingValues
  }

  if(usePointData){
    pointlike <- 1
  } else {
    pointlike <- 0
  }

  data <- list(x = shapecovs_scaled,
              nationalcovs = nationalshapecovs_scaled,
              xpop = shapecovs$FrankenPop_2010_5km,
              Apixel = Apix,
              Apoint = Apoint,
              spde = spde,
              time_spline_cov = time_spline_shape,
              time_spline_national = time_spline_national_shape,
              startendindex = startendindex,
              polygon_normal_likelihoods = polygon_normal_likelihoods,
              polygon_mean_API = polygon_mean_incs,
              polygon_sd_API = polygon_sd_incs,
              pointx = pointcovs_scaled,
              nationalpointcovs = nationalpointcovs_scaled,
              pointcases = pointcases$positive,
              pointtested = pointcases$examined,
              point_time_spline_cov = time_spline_point,
              weights = ifelse(polygon_data$admin_level == 'ADMIN0', 10, 1),
              point_time_spline_national = time_spline_national_point,
              softprior = soft[1], # Soften prior on field to get good starting values.
              prev_inc_par = c(2.616, -3.596, 1.594),
              pointlike = pointlike)

  # function to test for NAs etc.
  testdata(data, parameters)

  map <- list()
  if(!use_nationalcovars){
    map$nationalslope <- factor(rep(NA, ncol(nationalshapecovs_scaled)))
  }

  # First run
  obj <- MakeADFun(
    data = data,
    parameters = parameters,
    DLL = "multilikelihoodmodel",
    map = map
  )

  if(!exists('its')) its <- 10

  opt <- nlminb(obj$par, obj$fn, obj$gr, control = list(iter.max = its[1], eval.max = 2*its[1], trace = 100))

  xx <- obj$report(opt$par)

  save(obj, file = paste0('FILEPATH', runname, '/unfittedmodel.RData'))
  save(opt, file = paste0('FILEPATH', runname, '/modelweak.RData'))
  save(xx, file = paste0('FILEPATH', runname, '/reportweak.RData'))

  # Second run

  if(mediumPrior){
    # Middle run
    data$softprior <- medsoft[1]

    # Reset parameter list and readd nationalslope if needed
    parameters <- split(opt$par, names(opt$par))

    if(!use_nationalcovars){
      parameters$nationalslope <- rep(0.00, ncol(nationalshapecovs_scaled))
    }

    obj <- MakeADFun(
      data = data,
      parameters = parameters,
      DLL = "multilikelihoodmodel",
      map = map)

    opt2 <- nlminb(obj$par, obj$fn, obj$gr, control = list(iter.max = its[1], eval.max = 2*its[1], trace = 100))

    xxfinal <- obj$report(opt$par)

    save(obj, file = paste0('FILEPATH', runname, '/unfittedmidmodel.RData'))
    save(opt2, file = paste0('FILEPATH/', runname, '/modelmid.RData'))
    save(xxfinal, file = paste0('FILEPATH/', runname, '/reportmid.RData'))


    # Middle run
    data$softprior <-  medsoft[2]

    # Reset parameter list and readd nationalslope if needed
    parameters <- split(opt$par, names(opt$par))

    if(!use_nationalcovars){
      parameters$nationalslope <- rep(0.00, ncol(nationalshapecovs_scaled))
    }

    obj <- MakeADFun(
      data = data,
      parameters = parameters,
      DLL = "multilikelihoodmodel",
      map = map)

    opt <- nlminb(obj$par, obj$fn, obj$gr, control = list(iter.max = its[1], eval.max = 2*its[1], trace = 100))

    xxfinal <- obj$report(opt$par)

    save(obj, file = paste0('FILEPATH', runname, '/unfittedmidmodel.RData'))
    save(opt, file = paste0('FILEPATH', runname, '/modelmid.RData'))
    save(xxfinal, file = paste0('FILEPATH', runname, '/reportmid.RData'))

  }

  # Final run
  data$softprior <- soft[2]

  parameters <- split(opt$par, names(opt$par))

  if(!use_nationalcovars){
    parameters$nationalslope <- rep(0.00, ncol(nationalshapecovs_scaled))
  }

  obj <- MakeADFun(
    data = data,
    parameters = parameters,
    DLL = "multilikelihoodmodel",
    map = map)

  opt <- nlminb(obj$par, obj$fn, obj$gr, control = list(iter.max = its[2], eval.max = 2*its[2], trace = 100))

  xxfinal <- obj$report(opt$par)

  save(obj, file = paste0('FILEPATH', runname, '/unfittedfinalmodel.RData'))
  save(opt, file = paste0('FILEPATH', runname, '/modelfinal.RData'))
  save(xxfinal, file = paste0('FILEPATH', runname, '/reportfinal.RData'))

  write(paste(Sys.time(), runname), 'runlist.txt', append = TRUE)

  # Now run hamilton sampler.
  # source('HamiltonionSampler.R')
  #
  # samples <- HamiltonionSampler(obj, opt$par, optimiterations = 100, samples = 1000, keep = 0.1, plot = FALSE)
  # save(samples, file = paste0('intermediate-data/', runname, '/parametersamples.RData'))
  #

}


get_spline_starting_values <- function(n_spline_slopes,
                                       useiso3,
                                       polygon_data,
                                       nat_spline_coefs,
                                       time_spline_national_shape,
                                       temporal_spline_covariate_national,
                                       plot = FALSE){

  starting_spline_values <- rep(0, n_spline_slopes)
  allpreds <- data.frame()

  for(i in 1:length(useiso3)){
    which_rows_iso <- polygon_data$iso_3_code == sort(unique(polygon_data$iso_3_code))[i]
    which_rows_admin <- polygon_data$admin_level == 'ADMIN0'
    which_rows <- which_rows_iso & which_rows_admin

    polygons <- polygon_data[which_rows, ]

    colstart <- i * nat_spline_coefs - ncol(temporal_spline_covariate_national) + 1
    colend <- colstart + nat_spline_coefs - 1

    splines <- time_spline_national_shape[which_rows, colstart:colend]

    d <- data.frame(api_mean_pf = polygons$api_mean_pf, splines)

    m <- lm(api_mean_pf ~ 0 + ., d)

    starting_spline_values[colstart:colend] <- m$coefficients


    if(plot){

      smooth <- as.vector(m$coefficients %*% t(as.matrix(d[, 2:ncol(d)])))

      preds <- data.frame(smooth_api = smooth,
                          year = 1980:2015,
                          iso_3_code = sort(unique(polygon_data$iso_3_code))[i])

      allpreds <- rbind(allpreds, preds)

    }

  }

  if(plot){
    p <- ggplot(allpreds, aes(year, smooth_api)) +
          geom_line() +
          facet_wrap(~ iso_3_code, scale = 'free_y') +
          geom_point(data = polygon_data %>% filter(admin_level == 'ADMIN0'), aes(y = api_mean_pf))
    print(p)
  }

  return(starting_spline_values)

}

