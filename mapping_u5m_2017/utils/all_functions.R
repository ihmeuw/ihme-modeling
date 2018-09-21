


##################################################################################################
##################################################################################################
## ALL FUNCTIONS
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################
##################################################################################################






##################################################################################################
##################################################################################################
## COVARIATE FUNCTIONS
##################################################################################################
##################################################################################################


## Load and crop covariates to the modeled area
load_and_crop_covariates <- function(fixed_effects, simple_polygon, agebin=1) {

  # get selected covs
  selected_covs <- strsplit(fixed_effects," ")
  selected_covs <- selected_covs[[1]][selected_covs[[1]] != "+"]

  # Hard-code central directories
  root <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")
  central_cov_dir <- <<<< FILEPATH REDACTED >>>>>
  central_tv_covs <- c('mss','msw','unrakedmss','unrakedmsw','sevwaste','sevstunt','matedu_yrs','unrakedmatedu_yrs','wocba','evi','lights_new','LST_day','total_pop','rates','malaria','fertility','fertility_infill','fertility_smooth','urban_rural', 'land_cover', 'LST_avg', 'gpcp_precip', 'aridity_cruts','malaria_pfpr')
  central_ntv_covs <- c('access','irrigation','LF','LF_vector','reservoirs','aridity','elevation','annual_precip','PET','dist_rivers_lakes','dist_rivers_only','lat','lon','latlon')

  # Load all temporally-varying covariates
  evi             <- brick(paste0(central_cov_dir, 'EVI_stack.tif'))
  lights_new      <- brick(paste0(central_cov_dir, 'NTL_stack.tif'))
  LST_day         <- brick(paste0(central_cov_dir, 'LST_day_stack.tif'))
  total_pop       <- brick(paste0(central_cov_dir, 'WorldPop_allStages_stack.tif'))
  urban_rural     <- brick(paste0(central_cov_dir, 'GHS_settlement_model_stack.tif'))
  land_cover      <- brick(paste0(central_cov_dir, 'landCover_stack.tif'))
  LST_avg         <- brick(paste0(central_cov_dir, 'LST_avg_stack.tif'))
  gpcp_precip     <- brick(paste0(central_cov_dir, 'GPCP_precip_stack.tif'))
  aridity_cruts   <- brick(paste0(central_cov_dir, 'cruts_ard_stack.tif'))

  # Load all temporally-nonvarying covariates
  # Human/Cultural Synoptic Rasters
  access          <- brick(paste0(central_cov_dir, 'synoptic_humCul_stack.tif'))$synoptic_humCul_stack.1
  irrigation      <- brick(paste0(central_cov_dir, 'synoptic_humCul_stack.tif'))$synoptic_humCul_stack.2
  LF              <- brick(paste0(central_cov_dir, 'synoptic_humCul_stack.tif'))$synoptic_humCul_stack.3
  LF_vector       <- brick(paste0(central_cov_dir, 'synoptic_humCul_stack.tif'))$synoptic_humCul_stack.4
  reservoirs      <- brick(paste0(central_cov_dir, 'synoptic_humCul_stack.tif'))$synoptic_humCul_stack.5
  # Environmental/Physical Synoptic Rasters
  aridity           <- brick(paste0(central_cov_dir, 'synoptic_envPhy_stack.tif'))$synoptic_envPhy_stack.1
  dist_rivers_lakes <- brick(paste0(central_cov_dir, 'synoptic_envPhy_stack.tif'))$synoptic_envPhy_stack.2
  dist_rivers_only  <- brick(paste0(central_cov_dir, 'synoptic_envPhy_stack.tif'))$synoptic_envPhy_stack.3
  elevation         <- brick(paste0(central_cov_dir, 'synoptic_envPhy_stack.tif'))$synoptic_envPhy_stack.4
  annual_precip     <- brick(paste0(central_cov_dir, 'synoptic_envPhy_stack.tif'))$synoptic_envPhy_stack.5
  PET               <- brick(paste0(central_cov_dir, 'synoptic_envPhy_stack.tif'))$synoptic_envPhy_stack.6

  lat     <- raster(paste0(central_cov_dir, 'lat.tif'))
  lon     <- raster(paste0(central_cov_dir, 'lon.tif'))
  latlon  <- lat*lon


  # some u5m additions
  malaria         <- brick(paste0(central_cov_dir, 'malaria_infant_death_rate_stack.tif'))
  if('malaria' %in% selected_covs)  values(malaria)=log(as.matrix(malaria)+.01)
  fertility         <- brick(paste0(central_cov_dir, 'fertility_stack.tif'))
  fertility_smooth         <- brick(paste0(central_cov_dir, 'fertility_smooth_stack.tif'))
  fertility_infill         <- brick(paste0(central_cov_dir, 'fertility_infill_stack.tif'))

  mss         <- brick(paste0(central_cov_dir, 'mss_stack.tif'))
  msw         <- brick(paste0(central_cov_dir, 'msw_stack.tif'))
  unrakedmss  <- brick(paste0(central_cov_dir, 'unrakedmss_stack.tif')) #'unrakedmss_stack.tif'))
  unrakedmsw  <- brick(paste0(central_cov_dir, 'unrakedmsw_stack.tif')) #'unrakedmsw_stack.tif'))
  sevstunt    <- brick(paste0(central_cov_dir, 'ss_stack.tif')) #'sevstunt_stack.tif'))
  sevwaste    <- brick(paste0(central_cov_dir, 'sw_stack.tif')) #'sevwaste_stack.tif'))

  wocba       <- brick(paste0(central_cov_dir, 'WOCBA_stack.tif'))
  malaria_pfpr       <- brick(paste0(central_cov_dir, 'malaria_pfpr_stack.tif'))

  matedu_yrs         <- brick(paste0(central_cov_dir, 'matedu_yrs_stack.tif')) #'matedu_yrs_stack.tif'))
  unrakedmatedu_yrs  <- brick(paste0(central_cov_dir, 'unrakedmatedu_yrs_stack.tif')) #'unrakedmatedu_yrs_stack.tif'))


    u5m_dir <-paste0(root,'/temp/geospatial/U5M_africa/')
    load(paste0(u5m_dir,'data/raw/covariates/national_mr_m0.Rdata'))
    load(paste0(u5m_dir,'data/raw/covariates/national_mr_m1_11.Rdata'))
    load(paste0(u5m_dir,'data/raw/covariates/national_mr_2q1.Rdata'))
    load(paste0(u5m_dir,'data/raw/covariates/national_mr_2q3.Rdata'))
    load(paste0(u5m_dir,'data/raw/covariates/national_mr_5q0.Rdata'))
    rates = get(paste0('rates_',agebin))
    names(rates)=paste0('rates.',1:4)
  if('rates' %in% selected_covs)  rates=extend(rates,extent(-180, 180, -90, 90),keepres=TRUE)

  # Add names to layers
  names(access)         <- "access"
  names(irrigation)     <- "irrigation"
  names(LF)             <- "LF"
  names(LF_vector)      <- "LF_vector"
  names(reservoirs)     <- "reservoirs"
  names(aridity)        <- "aridity"
  names(elevation)      <- "elevation"
  names(annual_precip)  <- "annual_precip"
  names(PET)            <- "PET"
  names(dist_rivers_only)  <- "dist_rivers_only"
  names(dist_rivers_lakes) <- "dist_rivers_lakes"
  names(lat)            <- "lat"
  names(lon)  <- "lon"
  names(latlon) <- "latlon"

  # time varying covariates with 4 period names
  for(c in central_tv_covs){
    tmp=get(c)
    names(tmp)=rep(paste0(c,'.',1:4))
    assign(c,tmp)
  }

  # Construct list of covariates .
  num_covs <- length(selected_covs)
  lcovs <- list()
  for(i in 1:num_covs) {
    message(selected_covs[i])
    this_cov <- selected_covs[i]
    # Add if it is from the temporally non-varying list.
    if(this_cov %in% central_ntv_covs) {
      lcovs[[i]] <- get(this_cov)
    }
    # Add if it is from the temporally varying list.
    if(this_cov %in% central_tv_covs) {
      lcovs[[i]] <- get(this_cov)
    }
    names(lcovs)[i] <- this_cov
  }

  # Make sure covariate layers line up with raster we are modeling over
  for(l in 1:length(lcovs)) {
    message(names(lcovs)[l])
    print(lcovs[[l]])
    lcovs[[l]]  <- crop(lcovs[[l]], extent(simple_polygon))
    print(lcovs[[l]])
    lcovs[[l]]  <- setExtent(lcovs[[l]], simple_polygon) #, keepres=TRUE, snap=TRUE)
    print(lcovs[[l]])
    lcovs[[l]]  <- mask(lcovs[[l]], simple_polygon)
  }

  return(lcovs)

}


##################################################################################################
##################################################################################################
## HOLDOUT FUNCTIONS
##################################################################################################
##################################################################################################


## USAGE NOTES:

## 1) all of these options takes in data that is already cleaned and processed
##
## 2) if you specify strata with little data, things may break but it will hopefully warn you
##
## 3) this is still a work in progress. I know some things won't work
##
## 4) source this whole file then proceed to make folds
##
## 5) I would suggest temp_strat="prop"
##    and spat_strat="poly" or "qt"
##
## 6) make_folds() returns a named list. each item is a data strata and will have a folds
##    column in it. the name of the list element tells you the strata

##  here are some examples which I've tested - may take a while (30-60min?):

#df <- fread('FILEPATH/fully_processed.csv',
#            stringsAsFactors = FALSE)
## clean the df
#df$long <- as.numeric(as.character(gsub(",", "", df$long)))
#df$lat  <- as.numeric(as.character(gsub(",", "", df$lat)))
#df <- df[-which(df$lat > 90), ]
#data <- df
#data <- data[-which(data$year == 2011), ]
#data <- data[, fold:= NULL]
#dim(data)

## with quad_tree
#system.time(
#stratum_qt <- make_folds(data = data, n_folds = 5, spat_strat = 'qt',
#                         temp_strat = "prop", strat_cols = "age_bin",
#                         ts = 1e5, mb = 10, lat_col = 'lat', long_col = 'long',
#                         ss_col = 'exposed', yr_col = 'year')
#)
#str(stratum_qt)

## pdf("C:/Users/azimmer/Documents/U5M_africa/Holdouts/qt_tests.pdf")
## for(i in 1:length(stratum_qt)){
##   library(scales)
##   d  <- stratum_qt[[i]]
##   for(yr in sort(unique(d$year))){
##     r <- which(d$year == yr)
##     x <- d$long[r]
##     y <- d$lat[ r]
##     col <- d$fold[r]
##     main <- paste0("Strata: ", names(stratum_qt)[i], " Year: ", yr)
##     plot(x, y, col = alpha(col, alpha = 0.25), pch = ".", main = main)
##   }
## }
## dev.off()

## ## with ad2
## stratum_ad2 <- make_folds(data = data, n_folds = 5, spat_strat = 'poly',
##                           temp_strat = "prop", strat_cols = "age_bin",
##                           admin_shps=<<<< FILEPATH REDACTED >>>>>,
##                           shape_ident="gaul_code",
##                           admin_raster=<<<< FILEPATH REDACTED >>>>>,
##                           mask_shape=<<<< FILEPATH REDACTED >>>>>,
##                           mask_raster=<<<< FILEPATH REDACTED >>>>>,
##                           lat_col = 'lat', long_col = 'long',
##                           ss_col = 'exposed', yr_col = 'year')

## ad2 <- shapefile(<<<< FILEPATH REDACTED >>>>>)
## pdf(<<<< FILEPATH REDACTED >>>>>)
## for(i in 1:length(stratum_ad2)){
##   library(scales)
##   d  <- stratum_ad2[[i]]
##   for(yr in sort(unique(d$year))){
##     r <- which(d$year == yr)
##     x <- d$long[r]
##     y <- d$lat[ r]
##     col <- d$fold[r]
##     main <- paste0("Strata: ", names(stratum_ad2)[i], " Year: ", yr)
##     plot(x, y, col = alpha(col, alpha = 0.25), pch = ".", main = main)
##     plot(ad2, add = T)
##   }
## }
## dev.off()


#######################
#######################
### HOLDOUT OPTIONS ### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#######################
#######################


####################
## TOTALLY RANDOM ##
####################

## 1) As it sounds, totally random points across space and time


##############
## IN SPACE ##
##############

## 1) random in space (this is just totally random with one timepoint)
## 2) small 'tesselations' aggregated up until certain population is reached
##    a) with quadtree
##    b) NOT DONE: with weighted k-means (weights inversely proportional to sample size)
## 3) by admin2
## 4) countries
## 5) larger regions (e.g. East Africa, West Africa, ...)


##############
## IN  TIME ##
##############

## 1) random in time
## 2) proportional to data amount in the period
## 3) full years, randomly selected across the duration of data
## 4) build up different datasets as if we were moving chronologically in time
##    i.e. first set is only yr 1, second set is yrs 1 & 2, third set is yrs 1, 2, 3 ...


##############
## IN   S-T ##
##############

## all the combos of the space time sets!


#####################
#####################
### CODING STARTS ### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#####################
#####################

## ## load packages
## library(raster)
## library(seegSDM)
## library(seegMBG)
## library(rgdal)
## library(rgeos)
## library(foreach)
## library(doParallel)
## library(data.table)

## ## codeloc
## j="J:"
## codeloc = <<<< FILEPATH REDACTED >>>>>

## ## load miscellaneous functions
## source(paste0(codeloc,'code/functions.R'))

####################
## TOTALLY RANDOM ##
####################

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## makes folds completely at random across all data
##
## INPUTS:
## data: cleaned data.table/frame to be split into folds
## n_folds: number of folds
## strat_cols: vector of column string names to
##    stratify over when making folds. if NULL, holdout
##    sets are made across the entire data structure
##
## OUTPUTS: 2 item list
## 1st item: 1 by nrow(data) vector containing integers identifying folds
## 2nd item: matrix of stratification combinations used to make holdouts
##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
rand_folds <- function(data,
                       n_folds=5,
                       strat_cols=NULL,
                       ...){

  ## make fold vectors stratifying by specified columns
  if(!is.null(strat_cols)){

    ## get different stratum
    all_strat <- get_strat_combos(data=data, strat_cols=strat_cols)

    ## make a vector to identify folds and assign completely at random by strata
    fold_vec <- rep(NA, nrow(data))

    for(strat in 1:nrow(all_strat)){

      strata <- as.data.frame(all_strat[strat, ])
      colnames(strata) <- colnames(all_strat)

      ## get rows in current strata
      strat_rows <- get_strat_rows(data=data,
                                   strata=strata)

      ## assign fold numbers uniformly (with rounding)
      fold_s  <- cut(seq(1, sum(strat_rows)),
                     breaks = n_folds, labels = 1:n_folds)
      fold_s <- as.numeric(as.character(fold_s))

      ## randomize the numbers within the strata
      fold_vec[which(strat_rows==1)]  <- sample(fold_s)
    }

    ## check to make sure we got all rows
    if(sum(is.na(fold_vec) > 0)){
      message("Warning! Check why some data rows didn't get assigned to folds")
    }

  }else{ ## make folds across all data
    fold_vec <- sample(cut(seq(1, nrow(data)),
                           breaks = n_folds, labels = 1:n_folds))
  }


  ## and return a list containing
  ## 1) vector of folds
  ## 2) matrix containing all stratum
  if(is.null(strat_cols)){
    return(list(folds   = fold_vec,
                stratum = NA))
  }else{
    return(list(folds   = fold_vec,
                stratum = all_strat))
  }
}

## just a little testing
## f <- rand_folds(data=data
## d <- cbind(data, f[[1]])
## d1 <- d[which(d[,1]==1),]
## d2 <- d[which(d[,1]==2),]
## d3 <- d[which(d[,3]==3),]
## table(d1[,4])
## table(d2[,4])
## table(d3[,4])


## f <- rand_folds(data=data, strat_cols=c("A", "B"))
## d <- cbind(data, f[[1]])
## for(i in 1:nrow(f[[2]])){
##     message("On strata:  ", f[[2]][i,])

##     rows <- which(d[,1]==f[[2]][i, 1] &
##                   d[,2]==f[[2]][i, 2])

##     print(table(d[rows,4]))
## }

##############
## IN SPACE ##
##############

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## (1) random in space.
## this is the same as totally random with one time point
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##


##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## (2) small aggregates in space - QUADTREE
##
## INPUTS:
##
## OUTPUTS:
##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
quadtree_folds <- function(xy, ## 2 col matrix of xy locs
                           ss, ## vector of sample size at each loc - if all 1s, it aggregates by # of points
                           mb, ## minimum allowed pts in a quadtree region
                           ts, ## target sample size in each region
                           n_folds, ## number of folds,
                           plot_fn  = NULL,  ## if true plots data and quad tree must be .png
                           plot_shp = NULL,  ## to add shapefile outlines to plot
                           save_qt  = TRUE,  ## if desired, can save quadtree regions to shapefiles
                           ...,
                           t_fold = 1, ## if multiple t_folds. to save shapefiles
                           stratum = 1 ## for multiple stratum to save shapefiles
#                           pathaddin = ""   ## file path addin to specifiy specifis of model run
#                           run_date = run_date
                           ){

  ## this function segregates data into spatial regions. it splits
  ## data down until the target sample size is reached or until a
  ## minimum allowable number of points are in the bin

  ## it then breaks the data into folds by ensuring that all data in
  ## any one spatial region stays together and such that the sample
  ## size sums in each fold are relatively similar

  ## points at the same place pose a problem because you can't split them
  ## so, first we make a subset of the data using only unique xy combos
  ## quadtree is run on the unique list and then we buid back up to
  ## the full dataset and make folds

  ## make data.table
  library(data.table)
  dt_xy <- data.table(cbind(xy, ss))

  ## round to 5 decimal points to ensure matching
  cols <- names(dt_xy)[1:2]
  dt_xy[, (cols) := round(.SD, 5), .SDcols=cols]

  ## griyo by lat and long combos
  dt_xy <- dt_xy[, loc_id := .GRP, by = c('long','lat')]

  ## unique xy locations by group and sum ss in groups
  un_xy <- dt_xy[, list(long=mean(long), lat=mean(lat), ss=sum(ss)), by=c('loc_id')]

  ## to allow for deeper recursions in quadtree
  options(expressions=500000)

  ## get quad-tree regions on unique locations and ss sum at those locations
  system.time(qt <- quadtree_ct(xy=as.matrix(un_xy[, .(long, lat)]),
                                ss=as.numeric(un_xy[, ss]),
                                target_ss=ts,
                                min_in_bin=1,
                                rand = T))

  ## in order to match these back to all the points, we collect locations and ids
  ## this can take a little while...
  ids <- id(qt)
  ids <- na.omit(ids) ## qt alg adds a few NA rows sometimes...

  ## now we match ids to our original dataset
  dt_xy[, row:=1:nrow(dt_xy) ]
  setkey(dt_xy, long, lat)
  dt_xy[, qt_id:= -1.0]
  system.time(
    for(i in 1:nrow(ids)){
      ## this next line is to fix weird dropping 0 issues resulting in non-matches
      loc <- data.table(long = round(ids[i, 2], 6), lat = round(ids[i, 3], 6))
      dt_xy[.(loc), qt_id:=ids[i, 1] ]
    }
  )

  ## now we stratify - selecting by qt_id and ensuring sum of ss in
  ## folds is close to even across folds
  if(length(unique(dt_xy[, qt_id])) < n_folds){
    message("Warning: Fewer quadtree sections than n_folds!! Things may break. Either increase data amount or decrease ts")
  }

  cts_in_qt <- dt_xy[, sum(ss), by = qt_id]
  fold_vec <- make_folds_by_poly(cts_in_polys = as.matrix(cts_in_qt),
                                 pt_poly_map  = as.numeric(dt_xy[,qt_id]),
                                 n_folds      = n_folds)

  ## now we 'unsort' dt_xy to make it match the orde of the original data
  dt_xy[, fold_vec := fold_vec]
  setkey(dt_xy, row)
  fold_vec <- dt_xy[, fold_vec]
  ho_id    <- dt_xy[, qt_id]

  ## plot if desired
  if(!is.null(plot_fn)){
    xylim <- cbind(x=c(min(xy[,1]), max(xy[,1])), y=c(min(xy[,2]), max(xy[,2])))
    png(paste0(plot_fn), width=4000, height=4000)
    title <- paste0("Quadtree with threshold of ", ts)
    if(!is.null(plot_shp)){
      plot(plot_shp, , xlab="x", ylab="y", main=title)
    }else{
      plot(xylim, type="n", xlab="x", ylab="y", main=title)
    }
    lines(qt, xylim, col="Gray")
    cols=hsv(fold_vec/n_folds, 1, 1)
    library(scales)
    points(dt_xy[,.(long, lat)], col=alpha(cols, alpha=0.5), pch=16, cex=2)
    dev.off()
  }

  ## save quadtree rectangles to shapefile if desired
  if(save_qt){
#    if(time_stamp==TRUE) output_dir <- <<<< FILEPATH REDACTED >>>>>
#    if(time_stamp==FALSE) output_dir <- <<<< FILEPATH REDACTED >>>>>
#    dir.create(output_dir, showWarnings = FALSE)
    output_dir <- <<<< FILEPATH REDACTED >>>>>

    xylim <- cbind(x=c(min(un_xy[,2]), max(un_xy[,2])), y=c(min(un_xy[,3]), max(un_xy[,3])))
    polys <- cell(qt, xylim)
    polys_attr <- data.frame(id=unique(polys$id))
    library(shapefiles)
    polys_shapefile <- convert.to.shapefile(polys, polys_attr, "id", 5)
    write.shapefile(polys_shapefile, paste0(output_dir, '/', 'spat_holdout_stratum_', stratum, '_t_fold', t_fold), arcgis=TRUE)
  }

  return(cbind(fold_vec, ho_id))
}

## ## example
## df <- fread(<<<< FILEPATH REDACTED >>>>>,
##            stringsAsFactors = FALSE)
## clean the df
## df$long <- as.numeric(as.character(gsub(",", "", df$long)))
## df$lat  <- as.numeric(as.character(gsub(",", "", df$lat)))
## df <- df[-which(df$lat > 90), ]
## data <- df
## shp_full <- plot_shp <- shapefile(<<<< FILEPATH REDACTED >>>>>)
## xy <- data[,c('long', 'lat')]
## ss <- data$exposed
## ts <- 1e6
## plot_fn <- 'quadtree.png'

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## (2) small aggregates in space - WEIGHTED K-MEANS
## not yet (or ever?) implemented. quadtree looks pretty good
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##


##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## (3) admin2 in space
##
## INPUTS:
##
## OUTPUTS:
##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
ad2_folds <- function(admin_shps,
                      shape_ident="gaul_code",
                      admin_raster,
                      ss=1,
                      xy,
                      n_folds,
                      mask_shape,
                      mask_raster,
                      ...){
  ## this function takes in admin2 (or any mutually exclusive and
  ## collectively exhaustive) shapefiles covering the domain of your
  ## data and splits your data into folds of approximately equal
  ## sample size using admin2 units to split the data

  ## admin_shps: file location of all pertinent shapefiles to use when folding (.grd)
  ## shape_ident: string identifying data col in shapefile used to uniquely identify polygons
  ## admin_raster: file location of associated raster for admin_shps (.shp)
  ## data: complete dataset that you want to fold
  ## strat_cols: vector of column string names to
  ##    stratify over when making folds. if NULL, holdout
  ##    sets are made across the entire data structure
  ## ss: vector of sample sizes for each row. if <1>, assumes all have ss=1
  ## xy: matrix of xy coordinates
  ## n_folds: number of folds to make
  ## mask_shape: shapefile file location for boundary of area to be folded
  ## mask_raster: raster to project mask_shape onto

  library(raster)

  ## make a mask for ther region we care about
  mask <- rasterize(shapefile(mask_shape), raster(mask_raster))*0

  ## get raster cells in mask
  cell_idx <- cellIdx(mask)

  ## load raster and shapefile for admin units
  rast      <- raster(admin_shps)
  rast_cell <- extract(rast, cell_idx)
  shp_full  <- shapefile(admin_raster)
  shp       <- shp_full@data[c('name', 'gaul_code')]
  ## plot(shp_full, col=1:length(shp_full))

  ## match raster polys to gaul
  rast_code <- match(rast_cell, shp$gaul_code)
  rast_code <- shp$gaul_code[rast_code]

  ## get number of datapoints in shapefiles
  shp_cts <- get_sample_counts(ss          = ss,
                               xy          = xy,
                               shapes      = shp_full,
                               shape_ident = shape_ident)
  pt_shp_map  <- shp_cts$pt_poly_map
  cts_in_shps <- shp_cts$ct_mat

  ## make the folds by using the polygons (ad2 units) as the

  ## sampling unit to divide the data into the folds while keeping a
  ## similar sample size in each fold
  fold_vec <- make_folds_by_poly(cts_in_polys = cts_in_shps,
                                 pt_poly_map  = pt_shp_map,
                                 n_folds      = n_folds)
  ho_id <- pt_shp_map



  return(cbind(fold_vec,
               ho_id))
}

## ## example
## df <- read.csv(<<<< FILEPATH REDACTED >>>>>,
##                stringsAsFactors = FALSE)

## df$long <- as.numeric(as.character(gsub(",", "", df$long)))
## df$lat  <- as.numeric(as.character(gsub(",", "", df$lat)))
## df <- df[-which(df$lat>90), ]
## data <- df

## shp_full <- shapefile(<<<< FILEPATH REDACTED >>>>>)
## folds <- ad2_folds(admin_shps=<<<< FILEPATH REDACTED >>>>>,
##                   shape_ident="gaul_code",
##                   admin_raster=<<<< FILEPATH REDACTED >>>>>,
##                   data=df,
##                   strat_cols=NULL,
##                   ss=data$exposed,
##                   xy=cbind(data$long, data$lat)
##                   n_folds=5,
##                   mask_shape=<<<< FILEPATH REDACTED >>>>>,
##                   mask_raster=<<<< FILEPATH REDACTED >>>>>)

## library(scales)
## cols <- folds
## cols[which(cols==1)] <- "cyan"
## cols[which(cols==2)] <- "red"
## cols[which(cols==3)] <- "blue"
## cols[which(cols==4)] <- "green"
## cols[which(cols==5)] <- "magenta"

## png("~/check_folds_plot.png", width=1080, height=1080)
## plot(shp_full)
## points(df$long, df$lat, col=alpha(cols, alpha=0.01), pch=16)
## dev.off()


##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## (4) countries in space
##
## INPUTS:
##
## OUTPUTS:
##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
ct_folds <- function(xy,   ## xy location matrix
                     ct,   ## country vec
                     ss=1, ## sample size vec (or 1 if equi-ss)
                     n_folds=5,
                     ...){

  if(length(unique(yr) < n_folds)){
    message("Too many folds for too few countries! Expand your horizons")
    stop()
  }

  if(length(ss)==1) ss <- rep(1, nrow(xy))

  ## first we find the sample size in each of the countries
  library(data.table)
  dt <- data.table(long = xy[,1],
                   lat  = xy[,2],
                   ss   = ss,
                   ct   = ct)

  ## get sample size totals in each country
  cts_in_ct <- dt[, sum(ss), by=ct]

  ## make the folds
  fold_vec <- make_folds_by_poly(cts_in_polys = as.data.frame(cts_in_ct),
                                 pt_poly_map  = as.character(dt[,ct]),
                                 n_folds      = n_folds)


  return(fold_vec)
}

## ## example
## df <- read.csv(<<<< FILEPATH REDACTED >>>>>,
##                stringsAsFactors = FALSE)
## shp_full <- shapefile(<<<< FILEPATH REDACTED >>>>>)

## df$long <- as.numeric(as.character(gsub(",", "", df$long)))
## df$lat  <- as.numeric(as.character(gsub(",", "", df$lat)))
## df$country <- gsub(pattern='Guinea-Bissau\\"',replacement="Guinea-Bissau", df$country)
## df <- df[-which(df$lat>90), ]
## data <- df

## xy <- data[,c('long', 'lat')]
## ss <- data$exposed
## ct <- data$country
## folds <- ct_folds(xy, ct, ss)
## plot(shp_full)
## library(scales)
## points(xy, col=alpha(folds, alpha=0.25), pch=".")

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## (4) random folds in space
##
## INPUTS:
##
## OUTPUTS:
##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
rand_s_folds <- function(xy,   ## xy location matrix
                         ss=1, ## sample size vec (or 1 if equi-ss)
                         n_folds=5,
                         ...){

  if(length(ss) < n_folds){
    message("Too many folds for too few countries! Expand your horizons")
    stop()
  }

  if(length(ss) != nrow(xy)){
    message("length of ss and nrow(xy) must match!")
    stop()
  }

  if(length(ss)==1) ss <- rep(1, nrow(xy))

  total.ct <- sum(ss)
  max.fold.ct <- ceiling(total.ct/n_folds)

  ## make a vector to store folds
  fold.vec <- numeric(nrow(xy))

  ## randomize the order of the rows
  rand.ord <- sample(1:nrow(xy))

  ## randomly decide if the first one will include the final poly
  flip <- sample(0:1, 1)
  if(flip==0){
    include.final <- rep(0:1, ceiling(n_folds/2))
  }else{
    include.final <- rep(1:0, ceiling(n_folds/2))
  }

  fold.sums <- rep(NA, n_folds)
  start.ind <- 1
  stop.ind  <- 1
  for(fold in 1:(n_folds-1)){

    ## check threshhold
    while(sum(ss[rand.ord[start.ind:stop.ind]]) < max.fold.ct){
      stop.ind <- stop.ind + 1
    }

    ## check if final row is included
    if(include.final[fold]==1){
      stop.ind <- stop.ind - 1
    }

    ## identify the selected rows as being in this fold
    fold.vec[rand.ord[start.ind:stop.ind]] <- fold

    ## record total in fold
    total.ct[fold] <- sum(ss[rand.ord[start.ind:stop.ind]])

    ## adjust indices
    start.ind <- stop.ind + 1
    stop.ind  <- start.ind + 1
  }

  ## the last fold is everything else
  stop.ind <- nrow(xy)
  fold.vec[rand.ord[start.ind:stop.ind]] <- n_folds
  total.ct[n_folds] <- sum(ss[rand.ord[start.ind:stop.ind]])

  ## print ss sums in folds
  message("The sum in each different fold is: \n")
  for(i in 1:n_folds){
    message(total.ct[i])
  }

  return(fold.vec)
}


##############
## IN  TIME ##
##############

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## 1) random in time - already done with completely random
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##


##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## 2) proportional to data amount in the period
## already done. this is equivalent to stratifying by time the
## stratified holdouts can then be recombined across time to yield
## proportional to data in the time period
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
proptime_folds <- function(yr, n_folds = NULL, ss=NULL, ...){

  ## so all we have to do is return a 'fold vector' with integers for unique years

  yrf  <- as.factor(yr)
  fold_vec <- as.numeric(yrf)

  fold_list <- list(NULL)
  for(i in sort(unique(fold_vec))){
    fold_list[[i]] <- which(fold_vec == i)
  }
  return(fold_list)

}


##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## 3) full years, randomly selected across the duration of data
##
## INPUTS:
##
## OUTPUTS:
##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
yr_folds <- function(xy,   ## xy location matrix
                     yr,   ## yr vec
                     ss=1, ## sample size vec (or 1 if equi-ss)
                     n_folds=5,
                     ...){

  if(length(unique(yr) < n_folds)){
    message("Too many folds for too few years! Try again in a decade")
    stop()
  }

  if(length(ss)==1) ss <- rep(1, nrow(xy))

  ## first we find the sample size in each of the countries
  library(data.table)
  dt <- data.table(long = xy[,1],
                   lat  = xy[,2],
                   ss   = ss,
                   yr   = yr)

  ## get sample size totals in each country
  cts_in_yr <- dt[, sum(ss), by=ct]

  ## make the folds
  fold_vec <- make_folds_by_poly(cts_in_polys = as.data.frame(cts_in_yr),
                                 pt_poly_map  = as.character(dt[,ct]),
                                 n_folds      = n_folds)

  fold_list <- list(NULL)
  for(i in sort(unique(fold_vec))){
    fold_list[[i]] <- which(fold_vec == i)
  }
  return(fold_list)
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## 4) build up different datasets as if we were moving chronologically in time
##    e.g. 2000. 2000 & 2005. 2000, 2005 & 2010. ...
##
## INPUTS:
## yr: 1 by #datapt vector containing the year for each datapt
##
## OUTPUTS: list with as many entries as unique years
## each item in the list contains all data rows that should be in that fold
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
chrono_folds <- function(yr, ss=NULL, n_folds=NULL, ...){

  ## get unique years
  yrs <- sort(unique(yr))

  ## make the list
  chronos <- list(NULL)
  for(y in yrs){
    chronos[[which(yrs==y)]] <- which(yr <= y)
  }

  return(chronos)
}

###################
## IN SPACE-TIME ##
###################
## first we obey the temporal choice, then split in space
## currently, these are all combos of space holdout funcs and time holdout funcs.


##############################
## OVERALL WRAPPER FUNCTION ##
##############################
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
##
## STEPS:
## 1st: subset data by strata if specified
## 2nd: fold by temporal strategy if chosen
## 3rd: fold by spatial  strategy if chosen
##
## INPUTS:
##
## data: full dataset to split into test/train
## n_folds: number of folds in test/train splits
## spat_strat: spatial  holdout strategy. one of: c('rand', 'poly', 'qt', 'ct')
## temp_strat: temporal holdout strategy. one of: c('rand', 'prop', 'yr', 'chrono')
## seed: RNG seed in case you'd like to be able to recreate folds
##
## OUTPUTS: 2 item list
## 1st item: 1 by nrow(data) vector containing integers identifying folds
## 2nd item: matrix of stratification combinations used to make holdouts
##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
make_folds <- function(data,
                       n_folds,
                       spat_strat = 'rand',
                       temp_strat = 'rand',
                       spte_strat =  NULL,
                       save.file = <<<< FILEPATH REDACTED >>>>>), ## place to save the final object from the function
                       ...,
    #                   long_col,   ## needed for any spat_strat
    #                   lat_col,    ## needed for any spat_strat
    #                   ss_col,     ## needed for most strats
    #                   yr_col,     ## needed for most temp_strats
    #                   ct_col,     ## needed for ct spat_strat
                       strat_cols, ## needed for stratifying
                       seed
                       ){

  ## setup for testing purposes
  ## n_folds = 5
  ## spat_strat = "qt"
  ## temp_strat = "prop"
  ## spte_strat = NULL
  ## strat_cols = "age_bin"
  ## yr_col = "yr"
  ## ss_col = "exposed"
  ## lat_col = "lat"
  ## long_col = "long"
  ## ts = 1000
  ## mb = 5


  ## ## make a dataset
  ## n = 10000
  ## data = data.frame(lat  = runif(n, 10, 30),
  ##                   long = runif(n, 0, 40),
  ##                   yr   = sample(rep(c(1995, 2000, 2005, 2010), n / 4), n),
  ##                   age_bin = sample(1:4, n, replace = T),
  ##                   exposed = sample(25:35, n, replace = T))

  ##~#####################
  ## Prepare for battle ##
  ##~#####################

  ## for some reason, functions that work on data.frames don't all work on data.tables
  data <- as.data.frame(data)

  message('Identifying folds for validation')

  if(!is.null(spte_strat)){
    message("B/C you've chosen a space-time strategy, any input into either spat_strat or temp_strat strategies will be ignored")
    spat_strat=NA
    temp_strat=NA
  }

  if(temp_strat == "yr" |
     temp_strat == "chrono"){
    message("B/C you've chosen temp_strat==('yr'|'chrono'), I've set spat_strat to be NA")
    spat_strat=NA
  }

  message(paste("You've selected:",
                paste0("spat_strat = ", spat_strat),
                paste0("temp_strat = ", temp_strat),
                paste0("spte_strat = ", spte_strat),
                sep="\n"))

  ## check for unused arguments and give warning if any won't be used
  params <- list(...)
  optional_params <- c('ts', 'mb', 'plot_fn', 'plot_shp', 'shp_fn',
                       'admin_shps', 'shape_ident', 'admin_raster',
                       'ss_col', 'mask_shape', 'mask_raster',
                       'long_col', 'lat_col', 'yr_col', 'ts_vec')
  unused_params <- setdiff(names(params),optional_params)
  if(length(unused_params)){
    stop('You entered unused parameters! ', paste(unused_params,collapse = ', '))
  }

  ## set the seed if desired
  if(!(missing(seed))) set.seed(seed)


  ##~##############################
  ## FIRST, split data by strata ##
  ##~##############################
  print("Making stratum")

  if(!missing(strat_cols)){

    ## get different stratum
    all_strat <- get_strat_combos(data=data, strat_cols=strat_cols)

    ## make a list. each element is a different strata of data
    stratum <- list(NULL)
    for(s in 1:nrow(all_strat)){

      ## get all combos
      strata <- as.data.frame(all_strat[s, ])
      colnames(strata) <- colnames(all_strat)

      ## get rows in current strata
      strat_rows <- get_strat_rows(data=data,
                                   strata=strata)

      ## enter the data strata into the list
      stratum[[s]] <- data[strat_rows, ]
    }
  }else{
    stratum[[1]] <- data
  }

  m1 <- paste0("Length of data is: ", nrow(data))
  m2 <- paste0("Length of data in all strata is: ", sum(unlist(lapply(stratum, nrow))))
  print(m1)
  print(m2)
  print("These should be the same number. If not, maybe you have NAs to clean?")

  ##~#############################
  ## SECOND, make folds in time ##
  ##~#############################

  print("Making time folds")

  if(!is.na(temp_strat)){
    ## we have a dictionary of temporal methods to call from
    t_dict <- list('rand'  = NULL,
                   'prop'  = proptime_folds,
                   'yr'    = yr_folds,
                   'chrono'= chrono_folds
                   )

    ## match the choice to the function and run it
    t_fun   <- t_dict[[temp_strat]]
    if(!is.null(t_fun)){
      t_stratum <- lapply(1:length(stratum),
                          function(x){
                            yr <- stratum[[x]][[yr_col]]
                            ss <- stratum[[x]][[ss_col]]
                            ## get the fold indices
                            t_folds <- t_fun(yr=yr, ss=ss,
                                             n_folds=n_folds,
                                             ...)
                            ## make & return the subsetted datasets
                            lapply(1:length(t_folds),
                                   function(y){
                                     stratum[[x]][t_folds[[y]], ]
                                   })

                          })
    }else{
      t_stratum <- list(NULL)
      for(i in 1:length(stratum)){
        t_stratum[[i]]  <- stratum[[i]]
      }
    }
  }else{
    t_stratum <- list(NULL)
    for(i in 1:length(stratum)){
      t_stratum[[i]]  <- stratum[[i]]
    }
  }

  ## identify time folds in each stratum and unlist back to stratum
  stratum  <- NULL
  for(i in 1:length(t_stratum)){
    for(j in 1:length(t_stratum[[i]])){
      t_stratum[[i]][[j]] <- cbind(t_stratum[[i]][[j]],
                                   rep(j, nrow(t_stratum[[i]][[j]])))
      colnames(t_stratum[[i]][[j]])[ncol(t_stratum[[i]][[j]])] <- "t_fold"
    }
    stratum[[i]] <- do.call(rbind, t_stratum[[i]])
  }

  m1 <- paste0("Length of data is: ", nrow(data))
  m2 <- paste0("Length of data in all strata is: ", sum(unlist(lapply(stratum, nrow))))
  print(m1)
  print(m2)
  print("These should be the same number. If not, maybe you have NAs to clean?")


  ##~#############################
  ## THIRD, make folds in space ##
  ##~#############################

  print("Making space folds")

  if(!is.na(spat_strat)){
    ## we have a dictionary of spatial methods to call from
    s_dict <- list('rand' = rand_s_folds,
                   'poly' = ad2_folds,
                   'qt'   = quadtree_folds,
                   'ct'   = ct_folds
                   )

    ## match the choice to the function and run it on the double
    ## list of data
    s_fun <- s_dict[[spat_strat]]
    if(!is.null(s_fun)){
      ## TODO parallelize
      s_stratum <- lapply(1:length(stratum),
                          function(x){

                            message(paste0("Making folds for stratum: ", x))
                            ## first col is fold second col is holdout ID
                            s_fold_hoid <- matrix(ncol = 2, nrow = nrow(stratum[[x]]))

                            lapply(1:max(stratum[[x]][['t_fold']]),
                                   function(y){

                                     message(paste0("On temp fold: ", y))
                                     t_fold_r <- which(stratum[[x]][['t_fold']] == y)
                                     xy <- cbind(stratum[[x]][t_fold_r,
                                                              long_col],
                                                 stratum[[x]][t_fold_r,
                                                              lat_col])
                                     colnames(xy) <- c("long", "lat")
                                     ss <- stratum[[x]][t_fold_r, ss_col]

                                     ## IF 'ts_vec' has been input, as opposed to just 'ts', we need to reassign that here
                                     extra.args <- list(...)
                                     if(!is.null(extra.args[['ts_vec']])){
                                       extra.args[['ts']] <- extra.args[['ts_vec']][x]
                                       extra.args[['ts_vec']] <- NULL
                                     }
                                     all.args <- c(list('xy' = xy,
                                                        'ss' = ss,
                                                        'n_folds' = n_folds,
                                                        'stratum' = x,
                                                        't_folds' = y),
                                                   extra.args)

                                     ## get the fold indices
                                     s_folds <- do.call(s_fun, all.args)



                                     s_fold_hoid[t_fold_r, ]  <- s_folds

                                                      #...)

                                   })
                          })




      ## unpack fold indices back to stratum
      for(i in 1:length(stratum)){
        fold_hoid <- matrix(ncol = 2, nrow = nrow(stratum[[i]]))

        for(j in sort(unique(stratum[[i]][['t_fold']]))){
          t_fold_r <- which(stratum[[i]][['t_fold']] == j)
          fold_hoid[t_fold_r, ] <- s_stratum[[i]][[j]]
          colnames(fold_hoid) <- c("fold", "ho_id")
        }
        stratum[[i]] <- cbind(stratum[[i]], fold_hoid)
      }
    }
  }

  m1 <- paste0("Length of data is: ", nrow(data))
  m2 <- paste0("Length of data in all strata is: ", sum(unlist(lapply(stratum, nrow))))
  print(m1)
  print(m2)

  ##~######################################################
  ## FOURTH, if not the others, make folds in space-time ##
  ##~######################################################

  if(!is.null(spte_strat)){

    ## we have a dictionary of space-time methods to call from
    st_dict <- list()

    ## match the choice to the function and run it
    st_fun   <- st_dict[[spte_strat]]
    st_folds <- st_fun(xy=xy, yr=yr, ss=ss, n_folds=n_folds, ...)
  }

  ##~###########################################
  ## Lastly, if completely random is selected ##
  ##~###########################################



  ## ############################
  ## FINISH w/ post-processing ##
  ## ############################

  ## recombine temporal stratification if needed (i.e. if )

  ## check it.
  ## table(data$fold,data$age_bin,dnn=list('fold','age_bin'))

  ## ## clean it up
  ## data=data[order(as.numeric(row.names(data))),]
  ## data$elig=NULL

  ## ## save to disk
  ## write.csv(data,
  ##           file = <<<< FILEPATH REDACTED >>>>>,
  ##           row.names = FALSE)

  ## name the folds
  names(stratum) <- name_strata(all_strat = all_strat)

  ## save a copy of stratum
  saveRDS(file = save.file, object = stratum)

  ## return
  return(stratum)

}

#######################
## UTILITY FUNCTIONS ##
#######################

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## stratification functions
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##

get_strat_combos <- function(data=data,
                             strat_cols=strat_cols,
                             ...){

  ## this function creates all unique combos that we need to stratify over when making folds
  ## e.g. if gender and age_bin vectors are specified this function
  ## will return a list with all unique combos of age_bin and gender

  ## get all unique items from  each column
  unique_list <- list(NULL)
  for (i in 1:length(strat_cols)){
    unique_list[[i]] <- sort(unique(data[, strat_cols[i]]))
  }

  ## make a dataframe of all combos and return it
  all_combos <- expand.grid(unique_list)
  colnames(all_combos) <- strat_cols
  return(all_combos)

}

get_strat_rows <- function(data=data,
                           strata,
                           ...){

  ## this function returns all the rows in a strata

  if(length(strata) < 1){
    message("Need to identify some strata!")
    stop()
  }

  ## loop through and intersect all rows we want
  good_rows <- data[, colnames(strata)[1] ] == strata[[1]]

  if(length(strata) > 1){
    for(c in 2:ncol(strata)){
      tmp_rows <- data[, colnames(strata)[c] ] == strata[[c]]
      good_rows <- good_rows * tmp_rows ## intersect them
    }
  }

  good_rows <- which(good_rows == 1)

  return(good_rows)
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## shapefile counts
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
get_sample_counts <- function(ss = 1,
                              xy,
                              shapes,
                              shape_ident   = "gaul_code",
                              ...){
  ## this function takes in data and relevant shapefiles and returns
  ## how many of the datapoints falls into each of the shapefiles as
  ## well as the mapping vector between points and the shape_ident
  ## of the polygons

  ## data: full dataset
  ## ss: vector of sample sizes in each row. if <1> it assumes all have ss=1
  ## xy: matrix of xy coordinates
  ## shapes: all relevant loaded shapefiles (e.g loaded visa raster::shapefiles() )
  ## shape_ident: in a spatial polygons dataframe, there is data associated with each
  ##    entry. this variable identifies which of these data cols you'd like to use to
  ##    refer to the different polygons in the SPDF. if using admin2, leave as "gaul_code"
  ##    but, if you make your own set of shapes, you may want to select another col

  library(sp)

  ## grab relevant cols
  if(length(ss) == 1){
    data <- cbind(xy, rep(1, nrow(nrow))) ## samplesize
  }else{
    data <- cbind(xy, ss)
  }
  colnames(data) <- c("long", "lat", "ss")

  ## make sure all relevant cols are truly numeric
  for(i in 1:3){
    data[, i] <- as.numeric(as.character(data[, i]))
  }

  data <- as.data.frame(data)

  ## prepare the data to extract points into the shapefile
  coordinates(data) <- ~long+lat
  proj4string(data) <- proj4string(shapes)

  ## find which shapes the data falls into
  ## this takes the longest
  row_shapes <- over(data, shapes)
  row_shapes <- row_shapes[, shape_ident]

  ## WARNING! If some of your pts don't fit into the poly shapes
  ## they get randomly assigned to folds at the end
  if(sum(is.na(row_shapes)) > 0 ){
    message("Warning!! Some of your pts don't fall into any of the polygon shapes!!")
    message("They will get randomly assigned to folds")
    png("~/pts_outside_polys.png", width=1080, height=1080)
    plot(shapes)
    points(data[which(is.na(row_shapes)), ],col="red", pch=3)
    dev.off()
  }

  ## find the sample size in each shape
  all_shapes <- sort(unique(shapes@data[,shape_ident]))
  n_shapes   <- length(all_shapes)
  samp_size_shape <- cbind(all_shapes, rep(NA, n_shapes))
  for(i in 1:n_shapes){
    shape_id  <- samp_size_shape[i, 1]
    data_rows <- which(row_shapes==shape_id)
    samp_size_shape[i, 2] <- sum(data$ss[data_rows])
  }
  colnames(samp_size_shape) <- c(shape_ident, "count")

  return(list(ct_mat=samp_size_shape,
              pt_poly_map=row_shapes))
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## make approximate equal sample size folds by subsets
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
make_folds_by_poly <- function(cts_in_polys,
                               pt_poly_map,
                               n_folds,
                               ...){
  ## cts_in_polys:matrix containing two cols. 1st col is shape_ident
  ##    which identifies polygons in shapefile. 2nd col is count in shape
  ## pt_poly_map: vector as long as dataframe. each entry contains
  ##    the map from data to polygon shape_ident

  ## this function randomizes admin unit order and then sequentially
  ## adds to each fold until the sample size in the fold reaches
  ## 1/n_folds of the total count

  ## to make sure that the last one isn't much smaller, half of the
  ## folds take out the last poly that pushed them over the
  ## allowable count

  ## randomize the order

  if(n_folds > nrow(cts_in_polys)){
    message("You have too little data somewhere to split into ", n_folds, " folds")
    message("Check the sample size in each strata, and each strata after time holdouts")
    stop()
  }

  rand_ord <- sample(1:nrow(cts_in_polys))

  ## randomly decide if the first one will include the final poly
  flip <- sample(0:1, 1)
  if(flip==0){
    include_final <- rep(0:1, ceiling(n_folds/2))
  }else{
    include_final <- rep(1:0, ceiling(n_folds/2))
  }

  ## get the sample size threshhold in each poly
  total_ct <- sum(cts_in_polys[,2])
  max_fold_ct <- ceiling(total_ct/n_folds)

  ## add polys to folds
  fold_sums <- rep(NA, n_folds)
  start.ind <- 1
  stop.ind  <- 1
  for(fold in 1:(n_folds-1)){

    ## check threshhold
    while(sum(cts_in_polys[rand_ord[start.ind:stop.ind], 2]) < max_fold_ct){
      stop.ind <- stop.ind + 1
    }

    ## check if final poly is included
    if(include_final[fold]==1){
      stop.ind <- stop.ind - 1
    }

    ## store all the polys in the fold
    assign(paste0('polys_in_fold_', fold),
           cts_in_polys[rand_ord[start.ind:stop.ind], 1])

    ## record total in fold
    total_ct[fold] <- sum(cts_in_polys[rand_ord[start.ind:stop.ind], 2])

    ## adjust indices
    start.ind <- stop.ind + 1
    stop.ind  <- start.ind + 1
  }

  ## and the last fold is everything else
  stop.ind <- length(rand_ord)
  fold <- n_folds
  assign(paste0('polys_in_fold_', fold),
         cts_in_polys[rand_ord[start.ind:stop.ind], 1])
  total_ct[fold] <- sum(cts_in_polys[rand_ord[start.ind:stop.ind], 2])

  message("The sum in each different fold is: \n")
  for(i in 1:n_folds){
    message(total_ct[i])
  }

  ## now we have the polys in different folds. we make a
  ## vector for which data rows are in the folds
  fold_vec <- rep(NA, length(pt_poly_map))
  for(fold in 1:n_folds){
    fold_rows <- which(pt_poly_map %in% get( (paste0('polys_in_fold_', fold) )))
    fold_vec[fold_rows] <- fold
  }

  ## lastly, some of the points may not have fallen in the shapefiles
  ## for the moment they get randomly assigned to folds
  if(sum(is.na(pt_poly_map)) > 0){
    last_folds <- cut(seq(1, sum(is.na(pt_poly_map))),
                      breaks = n_folds, labels = 1:n_folds)
    last_folds <- sample(as.numeric(as.character(last_folds)))
    fold_vec[which(is.na(pt_poly_map))] <- last_folds
  }

  ## return a vector placing each data row in a fold
  return(fold_vec)

}


## make strata names for final list of holdout datasets
name_strata  <-  function(all_strat){
  ## takes in a matrix containing combinations of all strata
  ##      (e.g. the output from get_strat_combos() )
  ## returns a vector containing the names of the strata
  ## NAMING CONVENTION: stratcol1__strata1___stratcol2__strata2___...


  as <- all_strat
  strat_names <- paste(colnames(as)[1], as[, 1], sep = "__")
  if(ncol(as) > 1){
    for(i in 2:ncol(as)){
      strat_names <- paste(strat_names,
                           paste(colnames(as)[i], as[, i], sep = "__"),
                           sep = "___")
    }
  }

  return(strat_names)
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## quadtree functions ##
##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~##
## hacked from:
## http://gis.stackexchange.com/questions/31236/how-can-i-generate-irregular-grid-containing-minimum-n-points

## quadtree by points
quadtree <- function(xy, k=1) {
  d = dim(xy)[2]
  quad <- function(xy, i, id=1) {
    if (nrow(xy) < k*d) {
      rv = list(id=id, value=xy)
      class(rv) <- "quadtree.leaf"
    }
    else {
      q0 <- (1 + runif(1,min=-1/2,max=1/2)/dim(xy)[1])/2 # Random quantile near the median
      x0 <- quantile(xy[,i], q0)
      j <- i %% d + 1 # (Works for octrees, too...)
      rv <- list(index=i, threshold=x0,
                 lower=quad(xy[xy[,i] <= x0, ], j, id*2),
                 upper=quad(xy[xy[,i] > x0, ], j, id*2+1))
      class(rv) <- "quadtree"
    }
    return(rv)
  }
  quad(xy, 1)
}

## my hacked version. quadtree by sum of value at each point.
## is ss=1 for all points you get back to original quadtree
quadtree_ct <- function(xy, ss, target_ss, min_in_bin=5, rand = T) {
  ## this function quadtrees by sample size
  ## rand introduces some randomness to the "median"
  print(paste0("Aiming for ts between: ", target_ss, " and ", 2 * target_ss))
  d = dim(xy)[2]
  quad <- function(xy, i, id=1, ss, target_ss) {
    if (sum(ss) < target_ss * 2 | length(xy)/2 <= min_in_bin){
      rv = list(id=id, value=xy)
      class(rv) <- "quadtree.leaf"
    }
    else {
      if(rand){
        q0 <- (1 + runif(1,min=-1/10,max=1/10)/dim(xy)[1])/2 # Random quantile near the median
      }else{
        q0 <- 1 / 2 ## no randomness, just the median
      }
      x0 <- quantile(xy[,i], q0)
      j <- i %% d + 1 # (Works for octrees, too...)
      rv <- list(index=i, threshold=x0,
                 lower=quad(xy[xy[,i] <= x0, ], j, id*2,  ss[xy[,i] <= x0], target_ss),
                 upper=quad(xy[xy[,i] > x0, ], j, id*2+1, ss[xy[,i] >  x0], target_ss))
      class(rv) <- "quadtree"
    }
    return(rv)
  }
  quad(xy=xy, i=1, id=1, ss=ss, target_ss=target_ss)
}

## plotting functions
points.quadtree <- function(q, alpha=0.1, ...) {
  points(q$lower, alpha, ...); points(q$upper, alpha, ...)
}

points.quadtree.leaf <- function(q, alpha=0.1, ...) {
  library(scales)
  points(q$value, col=alpha(q$id, alpha=alpha), ...)
}

lines.quadtree <- function(q, xylim, ...) {
  i <- q$index
  j <- 3 - q$index
  clip <- function(xylim.clip, i, upper) {
    if (upper) xylim.clip[1, i] <- max(q$threshold, xylim.clip[1,i]) else
      xylim.clip[2,i] <- min(q$threshold, xylim.clip[2,i])
    xylim.clip
  }
  if(q$threshold > xylim[1,i]) lines(q$lower, clip(xylim, i, FALSE), ...)
  if(q$threshold < xylim[2,i]) lines(q$upper, clip(xylim, i, TRUE), ...)
  xlim <- xylim[, j]
  xy <- cbind(c(q$threshold, q$threshold), xlim)
  lines(xy[, order(i:j)],  ...)
}
lines.quadtree.leaf <- function(q, xylim, ...) {} # Nothing to do at leaves!


## get cell boundaries to make shapefiles
cell <- function(q, xylim, ...) {
  if (class(q)=="quadtree") f <- cell.quadtree else f <- cell.quadtree.leaf
  f(q, xylim, ...)
}
cell.quadtree <- function(q, xylim, ...) {
  i <- q$index
  j <- 3 - q$index
  clip <- function(xylim_clip, i, upper) {
    if(upper){
      xylim_clip[1, i] <- max(q$threshold, xylim_clip[1,i])
    }else{
      xylim_clip[2,i] <- min(q$threshold, xylim_clip[2,i])
    }
    xylim_clip
  }
  d <- data.frame(id=NULL, x=NULL, y=NULL)
  if(q$threshold > xylim[1,i]) d <- cell(q$lower, clip(xylim, i, FALSE), ...)
  if(q$threshold < xylim[2,i]) d <- rbind(d, cell(q$upper, clip(xylim, i, TRUE), ...))
  d
}
cell.quadtree.leaf <- function(q, xylim) {
  data.frame(id = q$id,
             x = c(xylim[1,1], xylim[2,1], xylim[2,1], xylim[1,1], xylim[1,1]),
             y = c(xylim[1,2], xylim[1,2], xylim[2,2], xylim[2,2], xylim[1,2]))
}


## get ids for each point
id <- function(q, ...){
  d <- data.frame(id=NULL, x=NULL, y=NULL)
  if (class(q)=="quadtree") f <- id.quadtree else f <- id.quadtree.leaf
  f(q, xylim, ...)
}
id.quadtree <- function(q, ...) {
  rbind(id(q$lower), id(q$upper))
}
id.quadtree.leaf <- function(q, ...) {
  ## print(q$id) ## for debugging
  if(length(q$value)==0){
    data.frame(id = q$id,
               x  = NA,
               y  = NA)
  }else if(length(q$value)==2){
    data.frame(id = q$id,
               x  = q$value[1],
               y  = q$value[2])
  }else{
    data.frame(id = q$id,
               x  = q$value[, 1],
               y  = q$value[, 2])
  }
}

## quadtree example
## n <- 25000         # Points per cluster
## n_centers <- 40  # Number of cluster centers
## sd <- 1/2        # Standard deviation of each cluster
## set.seed(17)
## centers <- matrix(runif(n_centers*2, min=c(-90, 30), max=c(-75, 40)), ncol=2, byrow=TRUE)
## xy <- matrix(apply(centers, 1, function(x) rnorm(n*2, mean=x, sd=sd)), ncol=2, byrow=TRUE)
## k <- 5
## system.time(qt <- quadtree(xy, k))
##
## xylim <- cbind(x=c(min(xy[,1]), max(xy[,1])), y=c(min(xy[,2]), max(xy[,2])))
## plot(xylim, type="n", xlab="x", ylab="y", main="Quadtree")
## lines(qt, xylim, col="Gray")
## points(qt, pch=16, cex=0.5)

## quadtree_ct example
## ## test it out on some simulated data with different sample sizes at each point
## n <- 20          # Points per cluster
## n_centers <- 10  # Number of cluster centers
## sd <- 1/2        # Standard deviation of each cluster
## set.seed(17)
## centers <- matrix(runif(n_centers*2, min=c(-90, 30), max=c(-75, 40)), ncol=2, byrow=TRUE)
## xy <- matrix(apply(centers, 1, function(x) rnorm(n*2, mean=x, sd=sd)), ncol=2, byrow=TRUE)
## ss <- c(rep(1, n*n_centers/2), rep(5, n*n_centers/2))
## n <- 1           # Points per cluster
## n_centers <- 5   # Number of cluster centers
## sd <- 1/2        # Standard deviation of each cluster
## centers <- matrix(runif(n_centers*2, min=c(-90, 30), max=c(-75, 40)), ncol=2, byrow=TRUE)
## xy <- rbind(xy,
##             matrix(apply(centers, 1, function(x) rnorm(n*2, mean=x, sd=sd)), ncol=2, byrow=TRUE))
## ss <- c(ss, rep(20, n*n_centers))
## k <- 5
## system.time(qt <- quadtree_ct(xy, ss, 20, min_in_bin=1))

## ## plot
## xylim <- cbind(x=c(min(xy[,1]), max(xy[,1])), y=c(min(xy[,2]), max(xy[,2])))
## png("qt_test.png")
## plot(xylim, type="n", xlab="x", ylab="y", main="Quadtree w/ max SS = 20")
## lines(qt, xylim, col="Gray")
## points(qt, pch=16, cex=0.5, alpha=0.9)
## points(xy, col=as.factor(ss), pch=16)
## legend('bottomright', legend=c('1', '5', '20'), col=1:3, pch=16)
## dev.off()


## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## done


## example for discussion only. can delete later
## t1 <- function(t1, t2, t11, t12, ...){...}
## t2 <- function(t1, t2, t21, t22, ...){...}

## s1 <- function(s1, s2, s11, s12, ...){...}
## s2 <- function(s1, s2, s21, s22, ...){...}

## wrapper <- function(t_fun, s_fun, t1, t2, s1, s2, ...){
##   ## t_fun: either '1' or '2'
##   ## s_fun: either '1' or '2'

##   ## dictionary to match choice to function
##   t_dict <- list('1' = t1,
##                  '2' = t2)
##   t_use  <- s_dict[[t_fun]]
##   ## run t function
##   t_res  <- t_use(t1, t2, ...)

##   ## dictionary to match choice to function
##   s_dict <- list('1' = s1,
##                  '2' = s2)
##   s_use  <- s_dict[[s_fun]]
##   ## run s function
##   s_res  <- s_use(s1, s2, ...)
## }


#sneaky rename :)
rename_aarons_shapefiles<-function(len=(length(Regions)*4),prefix){
  for(i in 1:len)
   for(f in list.files(paste0(sharedir,'/output/',run_date,'/'),pattern= paste0("spat_holdout_stratum_",i,"_t")))
    file.rename(paste0(sharedir,'/output/',run_date,'/',f),paste0(sharedir,'/output/',run_date,'/',prefix,'_',gsub(paste0("spat_holdout_stratum_",i),names(stratum_qt)[[i]],f)))
}


## simulate some data
## n <- 500
## yr <- c(2000, 2005, 2010, 2015)
## xy <- matrix(runif(n * length(yr) * 2), ncol = 2)
## ss <- sample(1:100, size = n * length(yr), replace = TRUE)
## age <- sample(1:4, size=n*length(yr), replace=TRUE)
## yrs <- sample(yr, size = n * length(yr), replace = TRUE)
## df <- as.data.frame(cbind(xy, ss, yrs, age))
## colnames(df) <- c("longitude", "latitude", "ss", "year", "age")
## ss_col   = "ss"  ;     yr_col = 'year'     ; spat_strat = 'rand'; temp_strat = 'prop'
## long_col = 'longitude'; lat_col = 'latitude'; n_folds = 5; strat_cols = 'age'
## folds = make_folds(data = df, n_folds = 5, spat_strat = 'rand', temp_strat = 'prop',
##                    long_col = 'longitude', lat_col = 'latitude', strat_cols = 'age')




##################################################################################################
##################################################################################################
## MBG FUNCTIONS
##################################################################################################
##################################################################################################


## Logit functions
logit <- function(x) {
  log(x/(1-x))
}
invlogit <- function(x) {
  exp(x)/(1+exp(x))
}

save_mbg_input <- function(indicator = indicator, indicator_group = indicator_group, df = df , simple_raster = simple_raster, mesh_s = mesh_s,
                           mesh_t = mesh_t, cov_list = all_cov_layers, run_date = NULL, pathaddin="",
                           child_model_names = NULL, all_fixed_effects = NULL, period_map = NULL,centre_scale = T) {

  just_covs <- extract_covariates(df, cov_list, return_only_results = T, centre_scale = centre_scale, period_var = 'year', period_map = period_map)
  if(centre_scale==TRUE){
    just_covs <- just_covs$covs
  }
  just_covs <- just_covs[, year := NULL]
  just_covs <- just_covs[, period_id := NULL]
  df = cbind(df, just_covs)

  #create a period column
  if(is.null(period_map)) {
    period_map <- make_period_map(c(2000,2005,2010,2015))
  }
  df[, period := NULL]
  setnames(period_map, 'data_period', 'year')
  setnames(period_map, 'period_id', 'period')
  df = merge(df, period_map, by = 'year', sort =F)

  ## Now that we've extracted covariate values to our data in the buffer zone, clip cov list to simple_raster instead of simple_polygon
  ##    (simple_raster is area we actually want to model over)
  for(l in 1:length(cov_list)) {
    cov_list[[l]]  <- crop(cov_list[[l]], extent(simple_raster))
    cov_list[[l]]  <- setExtent(cov_list[[l]], simple_raster)
    cov_list[[l]]  <- mask(cov_list[[l]], simple_raster)
  }

  ## Save all inputs
  to_save <- c("df", "simple_raster", "mesh_s", "mesh_t", 'cov_list', 'child_model_names', 'all_fixed_effects')
  save(list = to_save, file = <<<< FILEPATH REDACTED >>>>>)
  message(paste0('All inputs saved to <<<< FILEPATH REDACTED >>>>>'))

}

transform_theta <- function(theta) {(exp(theta)-1) / (1+exp(theta))}

test_rho_priors <- function(mean,sd) {
  prec <- (1/sd)^2
  message(paste0('theta1_prior_prec: ', round(prec, 2)))
  lower <- transform_theta(mean - (1.96*sd))
  upper <- transform_theta(mean + (1.96*sd))
  message(paste0('95% range in Rho prior: ', round(transform_theta(mean), 2), ' (', round(lower, 2), ' - ', round(upper, 2), ')'))
}



## BUILD THE FORMULA USED IN INLA
build_mbg_formula <- function(fixed_effects,
                              positive_constrained_variables=NULL,
                              interact_with_year=NULL,
                              int=TRUE,
                              nullmodel=FALSE,
                              add_nugget=FALSE) {

  # Set up model equation
  if(int==TRUE) intercept='+int' else intercept =''
  f_null  <- formula(paste0('covered~-1',intercept))

  if(!is.null(interact_with_year)){
    for(iwy in interact_with_year){
      fixed_effects <- gsub(iwy,paste0(iwy,'* factor(period)'),fixed_effects)
    }
    if(!is.null(positive_constrained_variables))
      stop("Cannot Both Constrain and Interact, one must be NULL.")
  }


  if(!is.null(positive_constrained_variables)){
    if(!all(positive_constrained_variables %in% strsplit(fixed_effects,' \\+ ')[[1]]))
      stop('Not all your positive_constrained_variables match fixed_effects')
    for(pcv in positive_constrained_variables){
      v <- sprintf("f(%s, model='clinear',range=c(0,Inf),initial=0)",pcv)
      fixed_effects <- gsub(pcv,v,fixed_effects)
    }
  }

  f_nugget <- ~ f(IID.ID, model='iid')

  f_space <- ~ f(
    space,
    model = spde,
    group = space.group,
    control.group = list(model = 'ar1'))

  if(!nullmodel){
    f_lin <- reformulate(fixed_effects)
    f_mbg = f_null + f_lin + f_space
    if(add_nugget==TRUE) f_mbg = f_null + f_lin + f_space + f_nugget
  } else {
    f_mbg = f_null + f_space
  }
  message(f_mbg)
  return(f_mbg)
}


# spde priors function
local.inla.spde2.matern.new = function(mesh, alpha=2, prior.pc.rho,
                                       prior.pc.sig)
{
  # Call inla.spde2.matern with range and standard deviationparametrization
  d = INLA:::inla.ifelse(inherits(mesh, "inla.mesh"), 2, 1)
  nu = alpha-d/2
  kappa0 = log(8*nu)/2
  tau0   = 0.5*(lgamma(nu)-lgamma(nu+d/2)-d/2*log(4*pi))-nu*kappa0
  spde   = inla.spde2.matern(mesh = mesh,
                             B.tau   = cbind(tau0,   nu,  -1),
                             B.kappa = cbind(kappa0, -1, 0))

  # Change prior information
  param = c(prior.pc.rho, prior.pc.sig)
  spde$f$hyper.default$theta1$prior = "pcspdega"
  spde$f$hyper.default$theta1$param = param
  spde$f$hyper.default$theta1$initial = log(prior.pc.rho[1])+1
  spde$f$hyper.default$theta2$initial = log(prior.pc.sig[1])-1

  # End and return
  return(invisible(spde))
}

build_mbg_data_stack <- function(df, fixed_effects, mesh_s, mesh_t,exclude_cs='',usematernnew=F,sig0=0.5,rho0=0.3) {

  # construct an SPDE model with a Matern kernel
  message('Building SPDE...')
  if(usematernnew){
    #rho0 is typical range, sig0 typical sd
    spde = local.inla.spde2.matern.new(mesh = mesh_s,
                                       prior.pc.rho = c(rho0, 0.5),
                                       prior.pc.sig = c(sig0, 0.5),
                                       alpha        = 2)
  } else {
    spde <- inla.spde2.matern(mesh = mesh_s,  alpha = 2)
  }

  # Projector Matrix
  A <- inla.spde.make.A(
    mesh = mesh_s,
    loc = as.matrix(df[, c('longitude', 'latitude'),with=F]),
    group = df$period,
    group.mesh = mesh_t
  )
  space = inla.spde.make.index("space",
                               n.spde = spde$n.spde,
                               n.group = mesh_t$m)


  #find cov indices
  if(fixed_effects!="NONE"){
    f_lin <- reformulate(fixed_effects)
    message('Indexing covariates...')
    covs_indices <- unique(c(match(all.vars(f_lin), colnames(df))))

    # make design matrix, center the scaling
    design_matrix <- data.frame(int = 1,
                                df[,covs_indices,with=F])


    cs_df <- getCentreScale(design_matrix, exclude = c('int',exclude_cs))


    design_matrix <- centreScale(design_matrix,
                                 df = cs_df)
  } else{
    design_matrix <- data.frame(int = rep(1,nrow(df)))
    cs_df <- getCentreScale(design_matrix, exclude = c('int','rates'))
  }

  # construct a 'stack' object for observed data
  cov=df[[indicator]] # N+_i
  N=df$N                 # N_i

  message('Stacking data...')
  stack.obs <- inla.stack(
    data = list(covered = cov),
    A = list(A, 1),
    effects = list(space,
                   design_matrix),
    tag = 'est'
  )

  return_list <- list(stack.obs, spde, cs_df)

  return(return_list)

}

fit_mbg <- function(indicator_family, stack.obs, spde, cov, N, int_prior_mn, int_prior_prec=1, f_mbg, run_date, keep_inla_files, cores,verbose_output=FALSE,wgts=0,intstrat='eb') {

  if(wgts[1]==0) wgts=rep(1,length(N))
  # Check if user has allocated less cores in their qsub than they specified in their config file
  cores_available <- Sys.getenv("NSLOTS")

  # ~~~~~~~~~~~~~~~~
  # fit the model
  # enable weights
  inla.setOption("enable.inla.argument.weights", TRUE)

  message('Fitting INLA model')

  # set a prior variance of 1.96 on the intercept as this is
  # roughly as flat as possible on logit scale without >1 inflection

  # code just to fit the model (not predict)
  inla_working_dir <- <<<< FILEPATH REDACTED >>>>>
  dir.create(inla_working_dir)
  if(indicator_family=='binomial') {
    system.time(
      res_fit <- inla(f_mbg,
                      data = inla.stack.data(stack.obs),
                      control.predictor = list(A = inla.stack.A(stack.obs),
                                               link = 1,
                                               compute = FALSE),
                      control.fixed = list(expand.factor.strategy = 'inla',
                                           prec.intercept = int_prior_prec,
                                           mean.intercept = int_prior_mn),
                      control.compute = list(dic = TRUE,
                                             cpo = TRUE,
                                             config = TRUE),
                      control.inla = list(int.strategy = intstrat, h = 1e-3, tolerance = 1e-6),
                      family = 'binomial',
                      num.threads = cores,
                      Ntrials = N,
                      verbose = verbose_output,
                      working.directory = inla_working_dir,
                      weights = wgts,
                      keep = TRUE)
    )
  }
  if(indicator_family=='gaussian') {
    system.time(
      res_fit <- inla(f_mbg,
                      data = inla.stack.data(stack.obs),
                      control.predictor = list(A = inla.stack.A(stack.obs),
                                               compute = FALSE),
                      control.fixed = list(expand.factor.strategy = 'inla',
                                           prec.intercept = 1,
                                           mean.intercept = int_prior_mn),
                      control.compute = list(dic = TRUE,
                                             cpo = TRUE,
                                             config = TRUE),
                      control.inla = list(int.strategy = intstrat, h = 1e-3, tolerance = 1e-6),
                      family = 'Gaussian',
                      num.threads = cores,
                      verbose = TRUE,
                      working.directory = inla_working_dir,
                      weights = wgts,
                      scale = N,
                      keep = TRUE)
    )
  }

  return(res_fit)

}

predict_mbg <- function(res_fit, cs_df, mesh_s, mesh_t, cov_list, samples, simple_raster, transform) {

  ## Now that we've extracted covariate values to our data in the buffer zone, clip cov list to simple_raster instead of simple_polygon
  ##    (simple_raster is area we actually want to model over)
  for(l in 1:length(cov_list)) {
    cov_list[[l]]  <- crop(cov_list[[l]], extent(simple_raster))
    cov_list[[l]]  <- setExtent(cov_list[[l]], simple_raster)
    cov_list[[l]]  <- mask(cov_list[[l]], simple_raster)
  }

  #updated by DCCASEY on 11-21-2016


  message('Making predictions')

  # number of samples
  n_draws <- samples

  # dummy raster
  template <- simple_raster

  cell_idx <- seegSDM:::notMissingIdx(template)

  # sample from posterior over latents
  suppressWarnings(draws <- inla.posterior.sample(n_draws, res_fit))

  # get parameter names
  par_names <- rownames(draws[[1]]$latent)

  # index to spatial field and linear coefficient samples
  s_idx <- grep('^space.*', par_names) ## sapce-time random effects
  l_idx <- match(sprintf('%s.1', res_fit$names.fixed), ## main effects
                 par_names)
  if(mean(is.na(l_idx)) == 1) l_idx <- match(sprintf('%s', res_fit$names.fixed), par_names) ## fix for different INLA version naming conventions

  # get samples as matrices
  pred_s <- sapply(draws, function (x)
    x$latent[s_idx])
  pred_l <- sapply(draws, function (x)
    x$latent[l_idx])
  if(length(l_idx)==1) pred_l=t(as.matrix(pred_l))
  rownames(pred_l) <- res_fit$names.fixed
  ## if we fit with a nugget, we also need to take draws of the nugget precision
  if(length(grep('^IID.ID.*', par_names)) > 0){
    pred_n <- sapply(draws, function(x)
      x$hyperpar[[4]]) ## this gets the precision for the nugget
  }else{
    pred_n <- NULL
  }

  # get coordinates of cells to predict to
  coords <- xyFromCell(template, seegSDM:::notMissingIdx(template))

  # ~~~~~~~~~~~~~~
  # project spatial effect

  # make predictions for all periods

  # get predictor matrix between spatial nodes and prediction locations
  #nperiod <- mesh_t$n
  nperiod <- max(mesh_t$loc)

  # replicate coordinates and years
  coords_periods <- do.call(rbind,
                            replicate(nperiod,
                                      coords,
                                      simplify = FALSE))

  groups_periods <- rep(1:nperiod,
                        each = nrow(coords))

  # Projector matrix
  A.pred <- inla.spde.make.A(
    mesh = mesh_s,
    loc = coords_periods,
    group = groups_periods,
    group.mesh = mesh_t
  )

  # get samples of s for all cells
  cell_s <- A.pred %*% pred_s
  cell_s <- as.matrix(cell_s)

  # remove out temporally varying covariates for now, will deal with them later
  #split the effects names by varying or time varying
  tvnames = pars =c()
  for(i in 1:length(cov_list)){
    if(dim(cov_list[[i]])[3]!=1) tvnames[length(tvnames)+1]= names(cov_list)[i]
    if(dim(cov_list[[i]])[3]==1) pars[length(pars)+1]= names(cov_list)[i]
  }

  #now split the covariates themselves (not just the names)
  # process covariates

  #start by ensuring the same mask and cell size
  cov_list = setNames(lapply(cov_list, function(x) mask(resample(x,template),template)),names(cov_list))

  #extract the covariates
  vals = data.table(do.call(cbind, lapply(cov_list, function(x) raster::extract(x,coords))))

  #create the int column
  vals[,int:= 1]

  #reshape long
  vals[,id:= 1:nrow(vals)] #create an id to ensure that melt doesn't sort things

  #convert the time varying names into meltable values
  tv_cov_colnames = grep(paste(tvnames, collapse = "|"), names(vals), value = T) #unlisted
  tv_cov_colist = lapply(tvnames, function(x) grep(x, names(vals), value = T))

  vals = melt(vals, id.vars = c('id','int',pars), measure = tv_cov_colist, value.name = tvnames, variable.factor =F)

  #fix the names
  #melt returns different values of variable based on if its reshaping 1 or 2+ columns.
  #enforce that it must end with the numeric after the period
  vals[,variable:= as.numeric(substring(variable,regexpr("(\\.[0-9]+)$", variable)[1]+1))]

  #Tests suggest this is not needed anymore/as a carry over. Keep just in case
  #if(length(tv_cov_colist)==1){
  #  setnames(vals,paste0(tvnames[[1]],'1'), tvnames)
  #}

  #keep only columns that were part of the regression
  vals = vals[,mget(rownames(pred_l))]

  #centreScale the values
  vals = centreScale(vals,cs_df) #happens for all periods at once

  #create the grid of fixed effects
  cell_l =as.matrix(vals) %*% pred_l

  ## add on nugget effect if applicable
  if(!is.null(pred_n)){
    cell_n <- sapply(pred_n, function(x){
      rnorm(n = nrow(cell_l), sd = 1 / sqrt(x), mean = 0)
    })
    cell_l <- cell_l + cell_n ## add on the nugget/noise to fixed effects field
  }

  # project model uncertainty from node-level sd
  cell_l_sd <-  apply(cell_l, 1, sd)

  node_s_sd <- apply(pred_s, 1, sd)
  cell_s_sd <- as.matrix(A.pred %*% node_s_sd)
  # combine to get cell-level sd

  # project to cells - ASSUMES INDEPENDENCE!
  cell_sd <- sqrt(cell_l_sd ^ 2 + cell_s_sd ^ 2)

  # ~~~~~~~~~~~~~~
  # combine and summarise them
  cell_all <- cell_l + cell_s

  # get predictive draws on probability scale
  if(transform=='inverse-logit') { cell_pred <- plogis(as.matrix(cell_all))
  } else cell_pred <- eval(parse(text=transform))

  # get prediction mean (integrated probability)
  pred_mean <- rowMeans(cell_pred)

  # create multi-band rasters for each of these metrics
  # each band giving the values for a different period
  mean_ras <- insertRaster(template,
                           matrix(pred_mean,
                                  ncol = nperiod))

  sd_ras <- insertRaster(template,
                         matrix(cell_sd,
                                ncol = nperiod))

  names(mean_ras) <-
    names(sd_ras) <-
    paste0('period_', 1:nperiod)

  return_list <- list(mean_ras, sd_ras, cell_pred)

  return(return_list)

}


## Save predictions (post inla and prediction run)
save_mbg_preds <- function(config, time_stamp, run_date, mean_ras, sd_ras, res_fit, cell_pred, df ,pathaddin="") {

  if(time_stamp==TRUE) output_dir <- <<<< FILEPATH REDACTED >>>>>
  if(time_stamp==FALSE) output_dir <- <<<< FILEPATH REDACTED >>>>>
  dir.create(output_dir, showWarnings = FALSE)

  # Save log of config file
  write.csv(config, paste0(output_dir,'/config.csv'), row.names = FALSE)

  if (!is.null(mean_ras)) {
    writeRaster(
      mean_ras,
      file = (paste0(output_dir, '/', indicator,'_prediction_eb',pathaddin)),
      overwrite = TRUE
    )
  }

  if (!is.null(sd_ras)) {
    # latent sd
    writeRaster(
      sd_ras,
      file = (paste0(output_dir, '/', indicator,'_sd_eb',pathaddin)),
      overwrite = TRUE
    )
  }

  # save model
  save(res_fit,
       file = (paste0(output_dir, '/', indicator,'_model_eb',pathaddin,'.RData')))
  # save draws (with compression) to recombine later
  save(
    cell_pred,
    file = (paste0(output_dir, '/', indicator,'_cell_draws_eb',pathaddin,'.RData')),
    compress = TRUE
  )
  # save training data
  write.csv(
    df,
    file = (paste0(output_dir, '/', indicator,'_trainingdata',pathaddin)),
    row.names = FALSE
  )

}


##################################################################################################
##################################################################################################
## MISC FUNCTIONS
##################################################################################################
##################################################################################################



pullupstream<-function(){
  system('cd <<<< FILEPATH REDACTED >>>>>\ngit pull upstream develop')
}

# miscellaneous functions
lstype<-function(type='closure'){
    inlist<-ls(.GlobalEnv)
    if (type=='function') type <-'closure'
    typelist<-sapply(sapply(inlist,get),typeof)
    return(names(typelist[typelist==type]))
}
# to listen in on parallel processes in R: http://stackoverflow.com/questions/10903787/how-can-i-print-when-using-dopar
Log <- function(text, ...) {
  msg <- sprintf(paste0(as.character(Sys.time()), ": ", text, "\n"), ...)
  cat(msg)
  write.socket(log.socket, msg)
}



  waitformodelstofinishu5m <- function(sleeptime=100,
                                    path =  <<<< FILEPATH REDACTED >>>>>,
                                    rd   = run_date,
                                    lv   = loopvars,
                                    showfiles = TRUE){
      pattern = 'fin_' # paste0(indicator,'_model')
      while(length(list.files(path=path, pattern=pattern))!=nrow(lv)){


            message('\n====================================================================================')
            message(sprintf('=====================      Run Date: %s      ======================',rd))
            message(paste0('\nAt ',Sys.time(),' .... ',length(list.files(path=path, pattern=pattern))),' Models have written output.')
            if(showfiles){
              message('\nCurrently missing models:')
              for(i in 1:nrow(lv))
                if(length(list.files(path=path,pattern=paste0(pattern,'_eb_bin',lv[i,2],'_',lv[i,1],'_',lv[i,3])))==0)
                  message(paste('Age Bin =',lv[i,2],'| Region =',lv[i,1],'| Holdout =',lv[i,3]))
            }
            message('\nFuthermore, this many are still running on cluster:')
            system("qstat | grep job_ | wc | awk '{print $1}'")
            message('\nThe following are still running on cluster')
            system("jobnames")
            message('\n====================================================================================')
            message('====================================================================================')
            message("\n")
            Sys.sleep(sleeptime)
      }
}


# make a system qsub call for this model
make_qsub <- function(user = Sys.info()['user'],
                      cores = slots,
                      memory = 100,
                      proj  = 'proj_geospatial',
                      ig = indicator_group,
                      repo = repo,
                      indic = indicator,
                      reg = "test",
                      age = 0,
                      vallevel = '',
                      hard_set_sbh_wgt = TRUE,
                      pct_sbh_wgt = 100,
                      rd = run_date,
                      log_location = 'sgeoutput',
                      code,
                      saveimage=FALSE,
                      test=FALSE,
                      holdout = 0,
                      use_base_covariates=FALSE,
                      test_pv_on_child_models=TRUE,
                      constrain_children_0_inf=FALSE,
                      child_cv_folds=5,
                      fit_stack_cv=TRUE,
                      shell = "r_shell.sh",
                      modeltype = 'full',
                      geo_nodes = FALSE){

   if(test) t=1 else t=0
   if(use_base_covariates) b=1 else b=0
   if(test_pv_on_child_models) pvc=1 else pvc=0
   if(constrain_children_0_inf) cc0i=1 else cc0i=0
   if(fit_stack_cv) fscv=1 else fscv=0
   if(hard_set_sbh_wgt) hssw=1 else hssw=0

   if(saveimage==TRUE)  save.image(<<<< FILEPATH REDACTED >>>>>)

    dir.create(sprintf('%s/output/%s',sharedir,rd))

   if(log_location=='sgeoutput')
     logloc = sprintf('/share/temp/sgeoutput/%s',user)
   if(log_location=='sharedir'){
     logloc = sprintf('%s/output/%s',sharedir,rd)
     dir.create(sprintf('%s/errors',logloc))
     dir.create(sprintf('%s/output',logloc))
   }

  ## do we want to submit to geo nodes? if so, there are a few tweaks
  if(geo_nodes == TRUE){
    shell     <- "r_shell_geos.sh"   ## for the correct path to R
    proj      <- "proj_geo_nodes"    ## correct proj for geos nodes
    node.flag <- "-l geos_node=TRUE" ## send the job to geo nodes
  }else{
    node.flag <- "" ## don't do anything special
  }

  return(sprintf("qsub -e %s/errors -o %s/output -cwd -l mem_free=%iG -pe multi_slot %i -P %s %s -N job_%s_%i_%i%s %s/%s %s/%s.R %s %i %s %i %i %s %s %i %i %i %i %i %s %s %i %i",
                 logloc,logloc,memory,cores,proj,node.flag,reg,age,holdout,vallevel,ig,shell,ig,code,reg,age,rd,t,holdout,indic,ig,b,pvc,cc0i,child_cv_folds,fscv,modeltype,vallevel,pct_sbh_wgt,hssw))

}


cellIdx <- function (x) which(!is.na(getValues(x[[1]])))
insertRaster <- function (raster, new_vals, idx = NULL) {


  # calculate cell index if not provided
  if (is.null(idx)) idx <- cellIdx(raster)

  # check the index makes superficial sense
  stopifnot(length(idx) == nrow(new_vals))
  stopifnot(max(idx) <= ncell(raster))

  # create results raster
  n <- ncol(new_vals)
  raster_new <- raster::brick(replicate(n,
                                        raster[[1]],
                                        simplify = FALSE))
  names(raster_new) <- colnames(new_vals)

  # update the values
  for(i in 1:n) {
    raster_new[[i]][idx] <- new_vals[, i]
  }

  return (raster_new)

}
condSim <- function (vals, weights = NULL, group = NULL, fun = NULL, ...) {
  # given a matrix of pixel-level prevalence samples `prev`
  # where each rows are pixels and columns are draws, a vector
  # of corresponding pixel populations `pop`, and an optional pixel
  # grouping factor `group`, return draws for the total deaths in each
  # group, or overall if groups are not specified

  # get dimensions of vals
  ncell <- nrow(vals)
  ndraw <- ncol(vals)

  # capture function as a string
  fun_string <- deparse(substitute(fun))

  # check fun accepts a

  # check dimensions of weights and group, set to 1 if not specified
  if (is.null(weights)) {
    weights <- rep(1, ncell)
  } else {
    if (length(weights) != ncell) {
      stop (sprintf('number of elements in weights (%i) not equal to number of cells in vals (%i)',
                    length(weights),
                    ncell))
    }
  }

  if (is.null(group)) {
    group <- rep(1, length(weights))
  } else {
    if (length(group) != ncell) {
      stop (sprintf('number of elements in group (%i) not equal to number of cells in vals (%i)',
                    length(group),
                    ncell))
    }
  }

  # otherwise, get the levels in group and create a matrix of results
  levels <- unique(na.omit(group))
  nlevel <- length(levels)

  ans <- matrix(NA,
                ncol = ndraw,
                nrow = nlevel)
  rownames(ans) <- levels

  # loop through levels in group, getting the results
  for (lvl in 1:nlevel) {

    # get an index o pixels in the level
    idx <- which(group == levels[lvl])

    # by default, calculate a weighted sum
    if (is.null(fun)) {

      # get draws and add to results
      # exception for if area has 1 cell, transpose matrix so it conforms (RB)
      if(all(dim(t(vals[idx, ]))==c(1,ndraw))){
        ans[lvl, ] <- weights[idx] %*% t(vals[idx, ])
      } else {
        ans[lvl, ] <- weights[idx] %*% vals[idx, ]
      }

    } else {

      # otherwise, apply function to each column
      ans[lvl, ] <- apply(vals[idx, ], 2, fun, weights = weights[idx], ...)

    }

  }

  # if only one level, make this a vector
  if (nlevel == 1) ans <- as.vector(ans)

  # return result
  return (ans)

}


reportTime <- function () {

  # get start time stored in options
  start <- options()$start

  # if it exists
  if (!is.null(start)) {

    # get elapsed time
    now <- Sys.time()
    diff <- difftime(now, start)

    # format nicely and report
    diff_string <- capture.output(print(diff, digits = 3))
    diff_string <- gsub('Time difference of', 'Time elapsed:', diff_string)
    message (diff_string)
  }
}


subtractYears <- function (Date, years) {
  # given a vector of dates in Date format,
  # and a number of years (positive numeric, can be non-integer)
  # subtract the required number of years and return

  stopifnot(years >= 0)
  stopifnot(inherits(Date, 'Date'))

  n_Date <- length(Date)
  n_years <- length(years)

  if (n_years != n_Date) {
    if (n_years == 1) {
      years <- rep(years, n_Date)
    } else {
      stop('Date and years have different lengths')
    }
  }

  Date <- as.POSIXlt(Date)

  month <- round(12 * years %% 1)
  year <- floor(years)

  Date$year <- Date$year - year
  Date$mon <- Date$mon - month

  Date <- as.Date(Date)

  return (Date)

}

toYear <- function(Date) format(Date, '%Y')

targetYear <- function (period, period_end, width = 60) {
  # get the target year, based on the period, period size (in months) and end
  # date of the final period

  # convert date to cmc
  period_end <- Date2cmc(period_end)

  # get number of months preceeding
  months <- width / 2 + width * (period - 1)

  # subtract
  cmc <- period_end - months + 1

  # format as a year
  ans <- toYear(cmc2Date(cmc))

  return (ans)

}

# convert strings or numerics to a sequential numeric index
# pass any number of vectors of the same length, get a numeric ID for the
# unique ones
idx <- function (...) {
  list <- list(...)

  # get their lengths
  lengths <- lapply(list, length)
  if (!isTRUE(do.call(all.equal, lengths))) {
    stop('vectors do not appear to have the same length')
  }
  # combine them
  x <- do.call(paste, list)
  #get numeric index
  match(x, unique(x))
}


# given the admin level, a GAUL code and the admin1 and 2 shapefiles,
# return an SPDF with the relevant area
getPoly <- function (level, code, admin1, admin2) {

  # get admin level
  if (level == 1) {
    admin <- admin1
  } else {
    admin <- admin2
  }

  # find the reight area
  idx <- match(code, admin$GAUL_CODE)

  # if it's valid
  if (length(idx) == 1) {
    ans <- admin[idx, ]
  } else {
    # handle errors
    warning (paste0("something's wrong on row ", i))
    ans <- NULL
  }

  # return result
  return (ans)
}

# functions to determine proportion of women in the given age group
matAgeRate <- function (age_group,
                        groups = c('15-19', '20-24', '25-29', '30-34')) {
  # given a vector `age_group` reporting the age group to which mothers belong,
  # return the proportion falling in each of the age groups in `groups`.

  # check it's a character vector
  stopifnot(class(age_group) == 'character')

  # count the number in all age groups
  counts <- table(age_group)

  # add on any groups not represented a 0s
  missing_groups <- which(!(groups %in% names(counts)))

  if (length(missing_groups) > 0) {
    dummy_counts <- rep(0, length(missing_groups))
    names(dummy_counts) <- groups[missing_groups]
    counts <- c(counts, dummy_counts)
  }

  # get proportions
  props <- counts / sum(counts)

  # find the ones we want
  idx_keep <- match(groups, names(props))

  # and return these
  return (props[idx_keep])

}


# functions to determine proportion of women in the given age group
matAgeParRate <- function (ceb,
                           age_group,
                           groups = c('15-19', '20-24', '25-29', '30-34')) {
  # given vectors `ceb` and `age_group` reporting the number of children ever
  # born to mothers and the age groups to which they belong,
  # return the proportion of births falling in each of the age groups in
  # `groups`.

  # check it's a character vector
  stopifnot(class(ceb) %in% c('numeric', 'integer'))
  stopifnot(class(age_group) == 'character')

  # count the number of births in all age groups
  counts <- tapply(ceb, age_group, sum)

  # add on any groups not represented a 0s
  missing_groups <- which(!(groups %in% names(counts)))

  if (length(missing_groups) > 0) {
    dummy_counts <- rep(0, length(missing_groups))
    names(dummy_counts) <- groups[missing_groups]
    counts <- c(counts, dummy_counts)
  }

  # get proportions
  props <- counts / sum(counts)

  # find the ones we want
  idx_keep <- match(groups, names(props))

  # and return these
  return (props[idx_keep])

}

# function to tabulate maternal age rate
tabMatAgeRate <- function (age_group,
                           cluster_id,
                           groups = c('15-19', '20-24', '25-29', '30-34')){
  # given a vector `age_group` reporting the age group to which mothers belong,
  # and a vector `cluster_id` giving the cluster to which each mother belongs
  # return a matrix - with number of rows equal to the number of unique elements
  # in `cluster_id` and number of columns equal to the length of `groups` -
  # giving the proportion falling in each of the age groups in `groups`.

  # get the grouped data as a list
  ans <- tapply(age_group,
                cluster_id,
                matAgeRate,
                groups)

  # combine into a matrix
  ans <- do.call(rbind, ans)

  # and return
  return (ans)

}


# function to tabulate maternal age rate
tabMatAgeParRate <- function (ceb,
                              age_group,
                              cluster_id,
                              groups = c('15-19', '20-24', '25-29', '30-34')){
  # given vectors `ceb` and `age_group` reporting the number of children ever
  # born to mothers and the age groups to which they belong,
  # return a matrix - with number of rows equal to the number of unique elements
  # in `cluster_id` and number of columns equal to the length of `groups` -
  # giving the proportion of births falling in each of the age groups in
  # `groups`.

  # get unique cluster ids
  cluster_ids <- unique(cluster_id)

  # create dummy results matrix
  ans <- matrix(NA,
                nrow = length(cluster_ids),
                ncol = length(groups))

  rownames(ans) <- cluster_ids
  colnames(ans) <- groups

  # loop through the cluster ids calculating the rates
  for (i in 1:length(cluster_ids)) {

    # get index for the cluster
    idx_cluster <- which(cluster_id == cluster_ids[i])

    ans[i, ] <- matAgeParRate(ceb = ceb[idx_cluster],
                              age_group = age_group[idx_cluster],
                              groups = groups)

  }

  # and return
  return (ans)

}

# get predictions from predictive INLA glm models
predGLM <- function (result,
                     intercept = '(Intercept)',
                     fixed_continuous = NULL,
                     fixed_group = NULL,
                     random_group = 'cluster_id') {

  # starting means and variances
  pred_mean <- pred_var <- 0

  # add intercept terms
  if (!is.null(intercept)) {
    pred_mean <- pred_mean + result$summary.fixed[intercept, 'mean']
    pred_var <- pred_var + result$summary.fixed[intercept, 'sd'] ^ 2
  }

  # add continuous fixed effects terms
  if (!is.null(fixed_continuous)) {
    for (name in fixed_continuous) {

      # get coefficients
      coef_mean <- result$summary.fixed[name, 'mean']
      coef_var <- result$summary.fixed[name, 'sd'] ^ 2

      # get covariate
      cov <- result$model.matrix[, name]

      pred_mean <- pred_mean + cov * coef_mean
      pred_var <- pred_var + (cov ^ 2) * coef_var

    }
  }

  # add discrete fixed effects terms
  if (!is.null(fixed_group)) {
    for (name in fixed_group) {

      # find group members
      idx <- grep(sprintf('^%s*', name), rownames(result$summary.fixed))

      # get clean names
      names <- rownames(result$summary.fixed)[idx]
      names_clean <- gsub(name, '', names)

      # get coefficients
      coef_mean <- result$summary.fixed[idx, 'mean']
      coef_var <- result$summary.fixed[idx, 'sd'] ^ 2

      # get covariate
      cov <- result$model.matrix[, names]

      pred_mean <- pred_mean + as.vector(cov %*% coef_mean)
      pred_var <- pred_var + as.vector((cov ^ 2) %*% coef_var)

    }
  }

  # add discrete random effects terms
  if (!is.null(random_group)) {
    for (name in random_group) {

      # get coefficients
      coef_level <- result$summary.random[[name]][, 'ID']
      coef_mean <- result$summary.random[[name]][, 'mean']
      coef_var <- result$summary.random[[name]][, 'sd'] ^ 2

      # get covariate
      cov <- result$.args$data[, name]

      # match up the levels
      length(cov)
      length(coef_level)
      idx <- match(cov, coef_level)

      pred_mean <- pred_mean + coef_mean[idx]
      pred_var <- pred_var + coef_var[idx]

    }
  }

  # return result
  ans <- data.frame(mean = pred_mean,
                    sd = sqrt(pred_var))

  return (ans)

}

getCols <- function (df, period = 1) {
  # subset results matrix
  df[, grep(sprintf('^p%s_', period),
            colnames(df))]
}

rnormMatrix <- function (n, mean, sd) {
  # sample random normals with matrix parameters
  # returna an array as a result

  # coerce to matrix
  mean <- as.matrix(mean)
  sd <- as.matrix(sd)

  # get & check dimensions
  ncol <- ncol(mean)
  nrow <- nrow(mean)
  stopifnot(ncol(sd) == ncol)
  stopifnot(nrow(sd) == nrow)

  # convert to vector
  mean <- as.vector(mean)
  sd <- as.vector(sd)

  # sample
  draws <- rnorm(n * length(mean), mean, sd)

  # reshape
  ans <- array(draws, dim = c(nrow, ncol, n))

  return (ans)
}

accumulate <- function (mean, sd, months = c(1, 11, 24, 24), nsample = 1000) {
  # given matrices of means and standard deviations of
  # logit component death probabilities, each column giving
  # consecutive and adjacent age bins and rows giving the records,
  # calculate the logit of the cumulative mortality probability across
  # the age bins by Monte Carlo sampling

  # generate random draws from these logits
  draws <- rnormMatrix(nsample, mean, sd)

  # convert to draws of survival probabilities
  draws_p <- 1 - plogis(draws)

  # raise to power of number of months per bin
  for (i in 1:dim(draws_p)[2]) {
    draws_p[, i, ] <- draws_p[, i, ] ^ months[i]
  }

  # loop through age bins accumulating them
  for (i in 2:dim(draws_p)[2]) {
    draws_p[, i, ] <- draws_p[, i, ] * draws_p[, i - 1, ]
  }

  # convert back to logit mortality probabilities
  draws_l <- qlogis(1 - draws_p)

  # calculate the means and standard deviations of these logits
  l_mean <- apply(draws_l, c(1, 2), mean)
  l_sd <- apply(draws_l, c(1, 2), sd)

  # return as a list
  return (list(mean = l_mean,
               sd = l_sd))

}


# functions to centre and scale matrices, columnwise

getCentreScale <- function (x, exclude = NULL, na.rm = TRUE) {
  # get dataframe of centreing and scaling values to convert x
  # to the standard normal. exclude is an optional character vector
  # giving column names to exclude from scaling

  # get means and SDs for all columns
  df <- data.frame(name = colnames(x),
                   mean = colMeans(x, na.rm = na.rm),
                   sd = apply(x, 2, sd, na.rm = na.rm))
  rownames(df) <- NULL

  # replace any zero standard deviations with 1
  # to avoid divide-by-zero errors
  df$sd[df$sd == 0] <- 1

  # if any named covariates are to be excluded, set mean to 0 and sd to 1
  if (!is.null(exclude)) {
    idx <- match(exclude, df$name)
    df$mean[idx] <- 0
    df$sd[idx] <- 1
  }

  return (df)
}

centreScale <- function (x, df, inverse = FALSE) {
  # apply pre-calculated centreing/scaling to matrix x,
  # with fixed dataframe of means/sds df
  # or uncentre/unscale if inverse = TRUE

  # get the centreing/scaling dataframe if not available
  if (is.null(df))
    df <- getCentreScale(x)

  # get index to match up values with column names
  names <- colnames(x)
  idx <- match(names, df$name)

  if (any(is.na(idx))) {
    stop ('could not match up column names with the values in df')
  }

  df <- df[idx, ]

  # apply transformations
  if (!inverse) {
    # move to standard normal

    # centre
    x <- sweep(x, MARGIN = 2, STATS = df$mean, FUN = '-')
    # scale
    x <- sweep(x, MARGIN = 2, STATS = df$sd, FUN = '/')

  } else {
    # inverse case, move from standard normal to original

    # unscale
    x <- sweep(x, MARGIN = 2, STATS = df$sd, FUN = '*')
    # uncentre
    x <- sweep(x, MARGIN = 2, STATS = df$mean, FUN = '+')

  }

  return (x)

}

loc <- function () {
  # detect location based on username

  user <- system('echo $USER',
                 intern = TRUE)

  # use this to work out our location
  location <- switch(user,
                     <USERNAME REDACTED> = 'oxford',
                     <USERNAME REDACTED> = 'seattle',
                     <USERNAME REDACTED> = 'seattle')

  return (location)

}

startLog <- function(file = 'full_run.log') {
  # create a logfile and start logging
  con <- file(file)
  sink(con,
       split = TRUE)
  sink(con,
       type = 'message')

  # report session info
  message(sprintf('# starting log at %s\n', Sys.time()))
  message('# session info:\n')
  print(sessionInfo())
  message('\n# run log:\n')
}

stopLog <- function() {
  # report session info
  message('\n# session info:\n')
  print(sessionInfo())
  message(sprintf('\n# stopping log at %s', Sys.time()))
  # stop logging to the logfile
  sink()
  sink(type = 'message')
}

list2paths <- function (x) {

  # get node and branch names
  nodes <- unlist(x, use.names = FALSE)
  branches <- unlist(getBranches(x),
                     use.names = FALSE)

  # append the node name
  paths <- file.path(branches, nodes)

  # remove extra slashes
  paths <- gsub('/+', '/', paths)

  paths
}

getBranches <- function(x, parent = "") {
  # loop through levels of a tree-like list,
  # flattening names of levels into
  # slash-separated character vector

  # get element names
  names <- names(x)

  # if unnamed, repeat the parent names
  if (is.null(names)) {
    names <- rep.int(parent, length(x))
  } else{
    names <- paste0(parent, names)
  }

  # if there are more branches ahead
  if (is.list(x)) {

    # add a slash
    names <- paste0(names, "/")

    # define a function to loop through them
    getTwigs <- function(i) {
      getBranches(x[[i]], names[i])
    }

    # get the names from these
    names <- lapply(1:length(x), getTwigs)

  }

  return (names)

}

unpack <- function (tmp) {
  # unpack a nested list where the outer list is unnamed, but each inner
  # list contains a single named element.
  # Assign this element to the parent environment and delete the list
  # it is called on fromt he parent ennvironment

  # get name of list in calling environment
  tmp_name <- deparse(substitute(tmp))

  # get calling environment
  pf <- parent.frame()

  # unpack into a single list
  tmp <- unlist(tmp, recursive = FALSE)

  # loop through assigning to calling environment
  for (i in 1:length(tmp)) {
    assign(names(tmp)[i], tmp[[i]], envir = pf)
  }

  # remove object from calling environment
  rm(list = tmp_name, envir = pf)

  # return nothing
  return (invisible(NULL))
}

prepLines <- function (rate,
                       year,
                       ylab = '',
                       title = '',
                       xlim = c(1999, 2016),
                       ylim = c(0, max(rate)),
                       line_years = c(2000, 2005, 2010, 2015)) {
  # set up the base plot for the custom line chart

  plot(rate ~ year,
       type = 'n',
       ylab = '',
       xlab = '',
       axes = FALSE,
       xlim = xlim,
       ylim = ylim)

  for (ly in line_years) {
    lines(x = rep(ly, 2),
          y = ylim,
          lwd = 3,
          lty = 3,
          col = grey(0.8))
  }

  axis(1,
       lty = 0,
       col.axis= grey(0.4),
       line = -1)

  axis(2,
       las = 2,
       cex.axis = 0.8,
       col = grey(0.4),
       col.axis= grey(0.4))

  title(main = title,
        col.main = grey(0.35),
        cex.main = 1.2,
        line = 0.5)

  title(ylab = ylab,
        col.lab = grey(0.4),
        cex.lab = 1.2)
}

addLines <- function (rate,
                      year,
                      country,
                      countries = NULL,
                      col = grey(0.7),
                      size = 1,
                      border = grey(0.4)) {

  # given vectors: rate (y axis), year (x axis)
  # and country (grouping factor), make a nice line
  # plot with lollipop-like likes with border colour
  # 'border' and fill colour 'col' (repeated if length one)
  # If 'countries' is NULL, all countries are plotted,
  # otherwise only those named in this character vector

  # check inputs
  stopifnot(all.equal(length(rate),
                      length(year),
                      length(country)))


  # sort countries
  all_countries <- sort(unique(country))
  if (is.null(countries)) {
    countries <- all_countries
  } else {
    stopifnot(all(countries %in% all_countries))
  }
  n_ctry <- length(countries)

  # expand col and bg if needed
  if (length(col) == 1) {
    col <- rep(col, n_ctry)
  } else {
    stopifnot(length(col) == n_ctry)
  }

  # loop through countries
  for (i in 1:n_ctry) {

    ctry <- countries[i]

    idx_ctry <- which(country == ctry)

    # dark grey outline
    lines(rate[idx_ctry] ~ year[idx_ctry],
          col = border,
          lwd = 7.5 * size)
    points(rate[idx_ctry] ~ year[idx_ctry],
           col = border,
           cex = 1 * size,
           pch = 16)

    # coloured foreground
    lines(rate[idx_ctry] ~ year[idx_ctry],
          col = col[i],
          lwd = 6 * size)

    points(rate[idx_ctry] ~ year[idx_ctry],
           col = col[i],
           pch = 16,
           cex = 0.85 * size)

  }
}

addLabels <- function (rate,
                       year,
                       country,
                       countries = NULL,
                       col = grey(0.7),
                       gap = diff(range(rate)) / 60,
                       cex = 0.7,
                       adj = 0,
                       xpd = NA,
                       ...) {

  # add country names on RHS
  # arguments as before, with gap to define spacing between labels.
  # dots are passed to text
  require (plotrix)

  # check inputs
  stopifnot(all.equal(length(rate),
                      length(year),
                      length(country)))

  # sort countries
  all_countries <- sort(unique(country))
  if (is.null(countries)) {
    countries <- all_countries
  } else {
    stopifnot(all(countries %in% all_countries))
  }
  n_ctry <- length(countries)

  # expand col and bg if needed
  if (length(col) == 1) {
    col <- rep(col, n_ctry)
  } else {
    stopifnot(length(col) == n_ctry)
  }

  # keep only those for latest year
  max_year <- max(as.numeric(year))
  year_idx <- which(year == max_year)
  rate <- rate[year_idx]
  year <- year[year_idx]
  country <- country[year_idx]

  # keep only those for countries requires
  ctry_idx <- which(country %in% countries)
  rate <- rate[ctry_idx]
  year <- year[ctry_idx]
  country <- country[ctry_idx]

  # order by countries
  ctry_o <- match(countries, country)
  rate <- rate[ctry_o]
  year <- year[ctry_o]
  country <- country[ctry_o]

  # get a good gap
  y_pos <- plotrix::spreadout(rate, gap)
  text(x = max_year + 1,
       y = y_pos,
       labels = country,
       col = col,
       cex = cex,
       adj = adj,
       xpd = xpd,
       ...)

}


splitGeoNames <- function (geo) {
  # split country/years (in format 'country_year') out of rownames
  # of a geostatistical conditional simulation object and add as columns
  splits <- strsplit(rownames(geo), split = '_')
  ctry <- sapply(splits, '[', 1)
  year <- sapply(splits, '[', 2)
  geo <- data.frame(iso3 = ctry,
                    year = year,
                    geo,
                    stringsAsFactors = FALSE)
  rownames(geo) <- NULL
  return (geo)
}

getEst <- function (df, iso3, year) {
  # for each dataframe of estimates, line up the country
  # and year, add object name as prefix to the column nmes and
  # return as a dataframe

  # get object name
  prefix <- deparse(substitute(df))

  # get index
  iso3_year_target <- paste(iso3, year, sep = '_')
  iso3_year_df <- paste(df$iso3, df$year, sep = '_')
  idx <- match (iso3_year_target, iso3_year_df)

  # subset, add prefix to column names and return
  df <- df[idx, -(1:2)]
  colnames(df) <- paste(prefix, colnames(df), sep = '_')
  return (df)
}

elogit <- function (y, n) log ( (y + 0.5) / (n - y + 0.5) )

## Combine input data and covariates layers for models run by region
## Necessary for saving to Shiny tool
## Save "covs", "tv_*", and "df" to new combined snapshot in model_image_history
combine_region_image_history <- function(indicator, indicator_group, run_date, fixed_effects) {


  load(<<<< FILEPATH REDACTED >>>>>)
  new_cov_list <- list()
  for(cov in names(cov_list)[!grepl("gaul_code", names(cov_list))]) {
    pull_raster_covs <- function(region) {
      load(<<<< FILEPATH REDACTED >>>>>)
      cov_raster <- cov_list[[cov]]
      return(cov_raster)
    }
    region_layers <- lapply(Regions, pull_raster_covs)
    combined_layers <- do.call(raster::merge, region_layers)
    names(combined_layers) <- gsub("layer", cov, names(combined_layers))
    if(length(names(combined_layers))==1) new_cov_list[[cov]] <- combined_layers
    if(length(names(combined_layers))!=1) assign(paste0('tv_', cov), combined_layers)
  }
  #cov_list <- do.call(raster::brick, new_cov_list)
  cov_list <- brick(new_cov_list)

  # Combine input data
  pull_df <- function(region) {
    load(<<<< FILEPATH REDACTED >>>>>)
    return(df)
  }
  df <- lapply(Regions, pull_df)
  df <- do.call(rbind.fill, df)

  covs <- cov_list

  save(list = c('df','covs',grep('^tv_*', ls(), value = TRUE)), file = <<<< FILEPATH REDACTED >>>>>)

}


      cirange = function(x){
          z=quantile(x,probs=c(.025,.975),na.rm=T)
          return(z[2]-z[1])
      }
      lower = function(x) quantile(x,probs=.025,na.rm=T)
      upper = function(x) quantile(x,probs=.975,na.rm=T)

cleanup_inla_scratch <- function(run_date) {

  if(keep_inla_files==FALSE) {

    # Clean up INLA intermediate directories unless user has specified to keep them.
    inla_working_dir <- <<<< FILEPATH REDACTED >>>>>
    inla_working_dirs <- list.dirs(inla_working_dir, recursive = FALSE)
    inla_working_dirs <- inla_working_dirs[grepl(run_date, inla_working_dirs)]
    for(inla_dir in inla_working_dirs) {
      unlink(inla_dir, recursive=TRUE)
    }

  }

  if(keep_inla_files==TRUE) {

    message('Keeping INLA intermediate files because keep_inla_files==TRUE in config.')
    message(paste0('Files stored here: <<<< FILEPATH REDACTED >>>>>'))

  }

}


##################################################################################################
##################################################################################################
## POINT POLYGON FUNCTIONS
##################################################################################################
##################################################################################################


#################################################################################
### Takes a dataset with polygon records in specific format. Returns them as points
### using k-means resampling (getPoints function from seegMBG)
## Inputs:
    # data: a data frame or data table with the following columns: cluster_id, exposed,
    #       <indicator>, latitude, longitude, shapefile, location code. Location_code refers to
    #       the shapefile polygon that the data. Data should alreaday be aggregated up
    #       to the smallest geog unit (if this is point, then lat long should have
    #       values). If lat long dont have values then shapefile and location_id should
    # shp_path: directory where all shapefiles are saved. Function will look in this
    #           directory and match on the shapefiles column
    # ignore_warnings: if TRUE, and there are polys that are not found the function will
    #                  warn but not stop.
    # cores: how many cores to use for parallel processing bits
    # unique_vars: character values of additional names that uniquely ID rows in data
    #              for example "age_bin" for u5m data. Defaults to NULL
    # density: sampling density in points per pixel (per square km for
    #          1km raster). supplies the n argument in getPoints().
    #          default is 0.001
    # perpixel: see ?getPoints
    # prob:     see ?getPoints

## Outputs: data table with polygon data as resampled points
## Notes: Right now assumes all africa data
#################################################################################

resample_polygons  <-   function(data,
                                 shp_path = <<<< FILEPATH REDACTED >>>>>,
                                 ignore_warnings = TRUE,
                                 cores           = as.numeric(slots),
                                 indic           = indicator,
                                 unique_vars     = NULL,
                                 density         = 0.001,
                                 perpixel        = TRUE,
                                 prob            = TRUE,
                                 use_1k_popraster= TRUE)
                                 {

  ############################
  # Confirm everything is set up
  require(doParallel); require(plyr); require(snow); require(doSNOW)
  data = data.frame(data) # df not dt for this one

  # TODO, add a check to see that all necessary columns are there.

  # change lat long to latiude longitude if needed
  #data = rename(data,c('lat'='latitude','long'='longitude'))
  names(data)[names(data)=="lat"] <- "latitude"
  names(data)[names(data)=="long"] <- "longitude"

  # find records with shapefiles
  data$shapefile[!is.na(data$longitude)&!is.na(data$latitude)]=NA
  noloc=rep(FALSE,nrow(data))
  noloc[data$shapefile==""&!is.na(data$shapefile)]=TRUE

  # remove any spatially unidentifiable data
  message(paste('Dropping',sum(noloc),'of',nrow(data),'rows of data due to no spatial identifiers (lat, long, shapefile info missing)\n'))
  data=data[!noloc,]

  # keep index of only polygon data
  shp_idx <- which(!is.na(data$shapefile))
  message(paste(length(shp_idx),'of',nrow(data),'rows of data are polygon assigned\n'))

  # hotfix for a weird yemen thing
  data$shapefile[data$shapefile=="matched to GADM admin 1 shapefile"&data$country=="Yemen"]="YEM_adm1_GADM"

  # identify all shapefiles from the dataset
  all_shapes <- unique(data$shapefile[shp_idx])
  message(paste(length(all_shapes),'unique shapefiles found in data.\n'))

  # check they're in the directory
  message(paste0('Checking shapefile directory (',shp_path,') for matches.. '))
  if (!all(paste0(all_shapes, '.shp') %in% list.files(shp_path))){
    message('Missing the following shapefiles:')
    print(all_shapes[!(paste0(all_shapes, '.shp') %in% list.files(shp_path))])
    if(!ignore_warnings) stop('Function breaking because not all shapefiles are a match. Please')
  }  else {
    message('All shapefiles in data match a shapefile by name in the directory.\n')
  }

  ############################
  # HOTFIX FOR ONE SHAPEFILE WITH WEIRD GAULS
  data$location_code[data$shapefile == "GRED_Zambia" & !is.na(data$shapefile)]=gsub('894.2006.','',data$location_code[data$shapefile == "GRED_Zambia" & !is.na(data$shapefile)])

  # load and extract all polygons - now in parallel

  # get unique shapefiles/ location codes
  shapes   <- unique(data[, c('shapefile', 'location_code')])
  shapes   <- shapes[!is.na(shapes$shapefile), ]
  n_shapes <- nrow(shapes)

  # sort by shapefile name (faster if they're all clumped together)
  o <- order(shapes$shapefile)
  shapes <- shapes[o, ]

  # empty, named list to store polygons
  polys <- list()
  polys[[n_shapes]] <- NA
  names(polys) <- paste(shapes$shapefile, shapes$location_code, sep = '__')

  # null first shapefile
  shape <- ''

  # report to the user
  message('Extracting all polygons from shapefiles on disk -- in parallel.\n')
  message(sprintf('extracting %i polygons', n_shapes))


  # vector of shapefiles
  shapefiles <- unique(shapes$shapefile)

  # run in parallel by shapefile

  getpolys <- function(i){
    # pull shapefile
    shape <- shapefiles[i]
    message(paste0("Working on shapefile: ", shape))
    shp   <- shapefile(paste0(shp_path, '/', shape, '.shp'))

    # HOTFIX for zambia shapefile with funny gaul codes
    if(shape == "GRED_Zambia"){
      shp@data$GAUL_CODE = gsub('894.2006.','',shp@data$GAUL_CODE)
    }

    # get location codes as numeric, not factor
    loc_codes <- unique(shapes[shapes$shapefile == shape,]$location_code) %>%
          as.character  %>% as.numeric

    polys_subset <- list()

    for (j in 1:length(loc_codes)) {
      code <- loc_codes[j]

      if (code %in% as.character(shp$GAUL_CODE)) {
        poly <- shp[as.character(shp$GAUL_CODE) == code, ]
      } else{
        warning(sprintf('GAUL code: %s not found in shapefile: %s',code,shape))
        poly <- NULL
      }

      poly_name <- paste0(shape, "__", code)
      polys_subset[[poly_name]] <- poly

    }

    return(polys_subset)
  }

  polys1 <- mclapply(1:length(shapefiles),getpolys,mc.cores=cores)


  polys <- unlist(polys1) # get to single-level list

  # find ones that didn't work
  bad_records <- which(sapply(polys, class) == 'NULL')
  if(length(bad_records)!=0){
    warning(sprintf('%i polygons could not be found:', length(bad_records)))
    print(names(bad_records))
    if(!ignore_warnings) {
      stop('Since ignore_warnings==FALSE, the function is now breaking. Please fix your bad records or set ignore_warnings==TRUE to drop them.\n')
    } else {
      warning('Since you have set ignore_warnings=TRUE, the function will drop all bad records and continue. Note: this is NOT recommended.\n')
      data = data[!paste(data$shapefile, data$location_code, sep = '__') %in%
                   names(bad_records),]
    }
  }

  ######################################
  ##
  message('In parallel, generating integration points for each shapefile and expanding records to integrate. \n')

  # get unique sets of shapefiles and location codes (bins optional)
  d       <- data[, c(unique_vars, 'shapefile', 'location_code')]
  pars    <- unique(d)
  pars    <- pars[!is.na(pars$shapefile), ]
  n_chunk <- nrow(pars)

  # get row indices
  message('Getting indices and chunking out data by polygon in parallel.\n')
  dx      <- trim(apply(d,   1,paste,collapse='--'))
  px      <- trim(apply(pars,1,paste,collapse='--'))
  indices <- mclapply(1:n_chunk,function(x){unname(which(dx==px[x]))},mc.cores=cores)

  chunks <- mclapply(1:n_chunk,function(x){data[indices[[x]],]},mc.cores=cores)

  # grab population raster
  message('Loading Population Raster.\n')
  suppressMessages(suppressWarnings(load_simple_polygon(gaul_list = get_gaul_codes('africa'),buffer=0.4,subset_only=TRUE)))
  raster_list<-suppressMessages(suppressWarnings(build_simple_raster_pop(subset_shape)))
  if(use_1k_popraster){
    popraster <- disaggregate(raster_list[['pop_raster']],5) # needs to be 1km for density 0.001
  #popraster=brick('/share/geospatial/pop_density/africa_1km_pop.tif')
  } else {
    popraster = raster_list[['pop_raster']]
  }

  # get points in parallel
  message('Running getPoints() on each polygon in parallel.\n')
  getPointsWrapper <- function(x){
    poly_name <- paste(pars[x,c('shapefile', 'location_code')], collapse = '__')
    poly <- polys[[poly_name]]
    message(poly_name)
    # NOTE: simply resampling on 2010 population data for now for convenience. Could redo by year, but wouldnt make much of difference anyway.
    if (is.null(poly)) {
      # make a dummy, empty points dataframe
      points <- data.frame(longitude = NA, latitude  = NA, weight    = NA)[0, ]
    } else {
      points <- try(
              getPoints(shape    = poly,
                        raster   = popraster[[3]],
                        n        = density,
                        perpixel = perpixel,
                        prob     = prob) )
      if(inherits(points,'try-error')){
        points <- data.frame(longitude = NA, latitude  = NA, weight    = NA)[0, ]
      } else {
        colnames(points) <- c('longitude', 'latitude', 'weight')
      }
    }
    return(points)
  }
  chunk_points <- mclapply(1:n_chunk,getPointsWrapper,mc.cores=cores)

  # duplicate records, and add their integration points
  message('Duplicating polygon records for each of their new integration points.\n')
  getNewChunksWrapper <-function(x){
    # data for this chunk (shapefile/age bin combo)
    chunk  <- chunks[[x]]
    points <- chunk_points[[x]]

   if (nrow(points) == 0) {
      new_chunk <- chunk[0, ]
      warning(paste('Chunk',x,'missing spatial info and will be dropped.'))
    } else {

      dupRecords <- function(j) {
        record <- chunk[j, , drop = FALSE]       # pull out the record
        # drop columns also in "points"
        record <- record[, !(colnames(record) %in% colnames(points))]
        # faster than rbind / replicate
        record_dup <- cbind(record, points, row.names = NULL)

        return(record_dup)
      }

      duped_records <- lapply(1:nrow(chunk), dupRecords)

      # append the duplicated data
      new_chunk <- rbindlist(duped_records)
    }
    return(new_chunk)
  }

  new_chunks <- mclapply(1:n_chunk,getNewChunksWrapper,mc.cores=cores)

  # remove old chunks from data
  message('Finishing up..\n')
  idx_all <- unlist(indices)
  data <- data[-idx_all, ]

  if(nrow(data)==0) data[1,]=NA

  new_chunks_all <- rbindlist(new_chunks, fill = TRUE)
  new_chunks_all$pseudocluster = TRUE
    data$pseudocluster= FALSE
    data$weight= 1
    data <- rbind(data, new_chunks_all)

  # count rows with no lat/longs and remove them
  no_ll <- which(is.na(data$latitude) | is.na(data$longitude))
  message(paste('Dropping',length(no_ll),'rows with missing spatial information.'))
  if(length(no_ll) != 0) data <- data[-no_ll, ]

  # last minute renames to fit broader naming convention
  if(indic=='died')
    data <- rename(data, c('exposed'='N',
                           'age_bin'='age'))

  data <- data.table(data)
  return(data)

}


##################################################################################################
##################################################################################################
## PREP FUNCTIONS
##################################################################################################
##################################################################################################


## Make time stamp in standardized format.
  make_time_stamp <- function(time_stamp) {

    run_date <- gsub("-","_",Sys.time())
    run_date <- gsub(":","_",run_date)
    run_date <- gsub(" ","_",run_date)

    if(time_stamp==FALSE) run_date <- 'scratch'

    return(run_date)

  }

## Load parameters from config file into memory
#   Arguments:
#     repo            = Location where you've cloned "mbg" repository.
#     indicator_group = Category of indicator, i.e. "education"
#     indicator       = Specific outcome to be modeled within indicator category, i.e. "edu_0"
  load_config <- function(repo, indicator_group, indicator, config_name=NULL, post_est_only=FALSE, run_date = '') {

    if(is.null(config_name)) {
      ## If new model run, pull config from /share repo
      if(post_est_only==FALSE) config <- fread(paste0(repo, indicator_group, '/config_', indicator, '.csv'), header=FALSE)
      ## If running analysis on existing model, use config from that model's outputs folder
      if(post_est_only==TRUE) config <- <<<< FILEPATH REDACTED >>>>>
    }
    if(!is.null(config_name)) config <- fread(paste0(repo, indicator_group, '/', config_name, '.csv'), header=FALSE)
    for(param in config[, V1]) {
      assign(param, config[V1==param, V2], envir=globalenv())
    }

    return(config)

  }

## Create directory structure
#   Arguments:
#     indicator_group = Category of indicator, i.e. "education"
#     indicator       = Specific outcome to be modeled within indicator category, i.e. "edu_0"
  create_dirs <- function(indicator_group, indicator) {

    dir.create(<<<< FILEPATH REDACTED >>>>>)
    dir.create(<<<< FILEPATH REDACTED >>>>>)

    indicator_dir <- <<<< FILEPATH REDACTED >>>>>

    for(dir in c('output','model_image_history')) {
      dir.create(paste0(indicator_dir,'/',dir), showWarnings = FALSE)
    }

  }

## Make template rasters (pull from central analysis folders managed by Lucas based off the area to model specified in config)
#   Arguments:
#     simple = Single polygon that defines boundaries of the entire area you want to model over.
#   Returns: Empty raster over modeling area. To be used for cropping covariates quickly and projecting model.
  get_template_raster <- function(simple) {

    message('Creating rasters of admin units')
    root <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")

    # Centrally controlled folder of analysis shapefiles
    analysis_raster_dir <- <<<< FILEPATH REDACTED >>>>>

    # Load empty raster for largest analysis area, mask/crop to selected analysis area
    template_raster <- raster(paste0(analysis_raster_dir, 'stage2_analysis.tif'))
    template_raster <- mask(crop(template_raster,simple),simple)

    return(template_raster)

  }

## Load input data from required location
#   Arguments:
#     indicator = Specific outcome to be modeled within indicator category, i.e. "edu_0"
#     simple    = Single polygon that defines boundaries of the entire area you want to model over.
#   Returns: Input data subset to modeling area.
  load_input_data <- function(indicator, simple, agebin = 0, removeyemen = FALSE, pathaddin = "", withdate=FALSE, date='', years='five_year',range=5, update_run_date = FALSE, withtag=FALSE, datatag='', use_share=FALSE) {

    if(withdate){
      if(date=='')
        rd=run_date
      if(date!='')
        rd=date
    } else {
      rd = run_date
    }

    # Load input data by indicator
    root <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")
    if(use_share==FALSE) load_dir <- paste0(root,'<<<< FILEPATH REDACTED >>>>>')
    if(use_share==TRUE) load_dir <- <<<< FILEPATH REDACTED >>>>>
    if(!withdate & !withtag) d = read.csv(paste0(load_dir, indicator, '.csv'))
    if(withtag) d = read.csv(paste0(load_dir, indicator, datatag, '.csv'))
    if(withdate) d = read.csv(paste0(root,'<<<< FILEPATH REDACTED >>>>>/',rd,'/', indicator, '.csv'))
    d$latitude  = as.numeric(as.character(d$latitude))
    d$longitude = as.numeric(as.character(d$longitude))
    message(nrow(d))
    d=d[d$latitude<=90,]
    d=d[d$latitude>=-90,]
    d=d[d$longitude<=180,]
    d=d[d$longitude>=-180,]
    d <- subset(d, !is.na(latitude))
    d <- subset(d, latitude!=0)
    message(nrow(d))

    # Check for necessary columns
    if(!(indicator %in% names(d))) stop(paste0("Your input data does not contain a column for your indicator: ", indicator))

    # Subset to within modeling area
    coordinates(d) <- c("longitude", "latitude")
    proj4string(d) <- proj4string(simple)
    d$keep <- !is.na(over(d, as(simple, "SpatialPolygons")))
    message(paste0(round(mean(d$keep), 2)*100, '% of input data in specified template'))
    d <- d[d$keep==TRUE,]
    d <- as.data.table(d)

    if(agebin!=0) d = d[age==agebin,]
    if(removeyemen) d = d[country!='Yemen' & country!='YEM',]
    if(years=='five_year') {
      d <- d[year >= 1998 & year <= 2002, year := 2000]
      d <- d[year >= 2003 & year <= 2007, year := 2005]
      d <- d[year >= 2008 & year <= 2012, year := 2010]
      d <- d[year >= 2013 & year <= 2017, year := 2015]
    }
    if(years=='annual') {
      d <- d[year >= 1998 & year <= 2000, year := 2000]
    }
    d <- subset(d, year >= 1998)

    # Save a copy
    if(update_run_date == TRUE) {
      if(dir.exists(<<<< FILEPATH REDACTED >>>>>) == TRUE) {
        existing_dir <- <<<< FILEPATH REDACTED >>>>>
        new_try <- existing_dir
        index <- 0
        while(dir.exists(new_try)) {
          index <- index + 1
          new_try <- paste0(existing_dir, '_', index)
        }
        run_date <- paste0(run_date, '_', index)
        dir.create(new_try, showWarnings = FALSE)
        run_date_dir <- new_try
      }
      if(dir.exists(<<<< FILEPATH REDACTED >>>>>) == FALSE) {
        run_date_dir <- <<<< FILEPATH REDACTED >>>>>
        dir.create(run_date_dir, showWarnings = FALSE)
      }
      write.csv(d, file=paste0(run_date_dir, "/input_data", pathaddin, ".csv"))
      return(list(d, run_date))
    }

    if(update_run_date == FALSE) {
      run_date_dir <- <<<< FILEPATH REDACTED >>>>>
      dir.create(run_date_dir, showWarnings = FALSE)
      write.csv(d, file=<<<< FILEPATH REDACTED >>>>>)
      return(d)
    }

  }

## Read in shapefile and dissolve to single polygon for creating one big mesh (no admin1 boundaries)
#   gaul_list = any
load_simple_polygon <- function(gaul_list, buffer, subset_only = FALSE,returnsubsetshape=FALSE, makeplots = F) {

  # Check for common gaul lists so we can query premade simple polygons
  if(identical(gaul_list, c(29, 42, 45, 47, 50, 66, 90, 94, 106, 105, 144, 155, 159, 181, 182, 214, 217, 221, 243))) {
    message("Your GAUL list matches WSSA, loading premade simple_polygon and assigning subset_shape to global environment...")
    load(<<<< FILEPATH REDACTED >>>>>)
    assign("subset_shape", subset_shape, envir=globalenv())
    if(makeplots) plot(spoly_spdf)
    if(makeplots) plot(subset_shape, add = TRUE, border = grey(0.5))
    return(list(subset_shape=subset_shape,spoly_spdf=spoly_spdf))
  }
  if(identical(gaul_list, c(8,49,59,68,76,89))) {
    message("Your GAUL list matches CSSA, loading premade simple_polygon and assigning subset_shape to global environment...")
    load(<<<< FILEPATH REDACTED >>>>>)
    assign("subset_shape", subset_shape, envir=globalenv())
    if(makeplots) plot(spoly_spdf)
    if(makeplots) plot(subset_shape, add = TRUE, border = grey(0.5))
    return(list(subset_shape=subset_shape,spoly_spdf=spoly_spdf))
  }
  if(identical(gaul_list, c(43,58,70,77,79,133,150,152,170,205,226,74,257,253,270))) {
    message("Your GAUL list matches ESSA, loading premade simple_polygon and assigning subset_shape to global environment...")
    load(<<<< FILEPATH REDACTED >>>>>)
    assign("subset_shape", subset_shape, envir=globalenv())
    if(makeplots) plot(spoly_spdf)
    if(makeplots) plot(subset_shape, add = TRUE, border = grey(0.5))
    return(list(subset_shape=subset_shape,spoly_spdf=spoly_spdf))
  }
  if(identical(gaul_list, c(4,40762,40765,145,169,6,248))) {
    message("Your GAUL list matches NAME, loading premade simple_polygon and assigning subset_shape to global environment...")
    load(<<<< FILEPATH REDACTED >>>>>)
    assign("subset_shape", subset_shape, envir=globalenv())
    if(makeplots) plot(spoly_spdf)
    if(makeplots) plot(subset_shape, add = TRUE, border = grey(0.5))
    return(list(subset_shape=subset_shape,spoly_spdf=spoly_spdf))
  }
  if(identical(gaul_list, c(35,142,172,227,235,271))) {
    message("Your GAUL list matches SSSA, loading premade simple_polygon and assigning subset_shape to global environment...")
    load(<<<< FILEPATH REDACTED >>>>>)
    assign("subset_shape", subset_shape, envir=globalenv())
    if(makeplots) plot(spoly_spdf)
    if(makeplots) plot(subset_shape, add = TRUE, border = grey(0.5))
    return(list(subset_shape=subset_shape,spoly_spdf=spoly_spdf))
  }
if(identical(gaul_list, c(4,6,8,29,35,42,43,45,47,49,50,58,59,66,68,70,
                            74,76,77,79,89,90,94,95,105,106,142,144,145,150,
                            152,155,159,169,170,172,181,182,205,214,217,221,
                            226,235,243,248,253,268,270,271,40762,40765,
                            227,257,133,269))) {
    message("Your GAUL list matches AFRICA, loading premade simple_polygon and assigning subset_shape to global environment...")
    load(<<<< FILEPATH REDACTED >>>>>)
    assign("subset_shape", subset_shape, envir=globalenv())
    if(makeplots) plot(spoly_spdf)
    if(makeplots) plot(subset_shape, add = TRUE, border = grey(0.5))
    return(list(subset_shape=subset_shape,spoly_spdf=spoly_spdf))
  }
  else {
    # count the vertices of a SpatialPolygons object, with one feature
    vertices <- function(x) sum(sapply(x@polygons[[1]]@Polygons, function(y) nrow(y@coords)))

    # ~~~~~~~~~~~~~~~~~
    # load data
      message("Opening master shapefile...")
      master_shape <- shapefile(paste0(root,"<<<< FILEPATH REDACTED >>>>>"))
      subset_shape <- master_shape[master_shape@data$GAUL_CODE %in% gaul_list, ]

    if(subset_only==TRUE) {

      return(list(subset_shape=subset_shape,spoly_spdf=NULL))

    }

    if(subset_only==FALSE) {

      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

      message('Making a super-low-complexity map outline for INLA mesh creation')

      message(paste0('Full polygon vertices: ', vertices(subset_shape)))

      # and to the coastline
      af <- gUnaryUnion(subset_shape)

      # simplify this
      af_simple <- gSimplify(af, tol = buffer, topologyPreserve = TRUE)

      # Remove tiny features
      # Get all sub polygons and their areas
      polys <- af_simple@polygons[[1]]@Polygons
      areas <- sapply(polys, function(x) x@area)

      # If there is more than one sub polygon, remove the ditzels (many single-country subsets are a single polygon,
      #   like Uganda, which would break these few lines)
      if(length(areas)>1) {
        # find top 5% by area
        big_idx <- which(areas > quantile(areas, 0.95))

        # convert back into a spatialPolygons object
        spoly <- SpatialPolygons(list(Polygons(polys[big_idx], ID = 1)))
      }
      if(length(areas)==1) {
        spoly <- af_simple
      }

      # Buffer slightly to simply and for masking rasters
      spoly <- gBuffer(spoly, width = buffer)

      # simplify again to reduce vertex count
      spoly <- gSimplify(spoly, tol = buffer, topologyPreserve = TRUE)

      message(paste0('Simplified vertices: ', vertices(spoly)))

      # plot to check it encloses all of the important bits
      if(makeplots) plot(spoly)
      if(makeplots) plot(af, add = TRUE, border = grey(0.5))

      # turn into an SPDF
      spoly_spdf <- SpatialPolygonsDataFrame(spoly,
                                             data = data.frame(ID = 1),
                                             match.ID = FALSE)

      # add projection information
      projection(spoly_spdf) <- projection(master_shape)

      return(list(subset_shape=subset_shape,spoly_spdf=spoly_spdf))

    }
  }
}

## Create spatial mesh (pull from central analysis folders managed by Lucas based off the area to model specified in config)
#   d = Data table with columns "latitude" and "longitude".
#   simple = Single polygon to create mesh over.
#   max_edge = Largest allowed triangle edge length (see INLA's inla.mesh.2d documentation).
#   mesh_offset = One or two values, for an inner and an optional outer extension distance (see INLA's inla.mesh.2d documentation).
build_space_mesh <- function(d, simple, max_edge, mesh_offset) {
  message(paste0('Creating spatial mesh, max edge parameter: ', max_edge))
    max.edge <- eval(parse(text=max_edge))
    mesh_offset <- eval(parse(text=mesh_offset))
    mesh_s <- inla.mesh.2d(
      boundary = inla.sp2segment(simple),
      loc = cbind(d$longitude,d$latitude),
      max.edge = max.edge,
      offset = mesh_offset,
      cutoff = max.edge[1]
    )

    plot(mesh_s,asp=1);points(d$longitude,d$latitude,col=d$year)

    return(mesh_s)
}

## Create temporal mesh (defaulting to the four period U5M approach for now, come back and make more flexible later)
build_time_mesh <- function(periods=1:4) {
  mesh_t <- inla.mesh.1d(
    loc = c(periods),
    degree = 1,
    boundary = rep("free", 2)
  )
  return(mesh_t)
}

## Load list of raster inputs (pop and simple)
build_simple_raster_pop <- function(subset_shape,u5m=FALSE) {

  if(u5m==FALSE){
    master_pop <- brick('<<<< FILEPATH REDACTED >>>>>/WorldPop_total_global_stack.tif') #WorldPop_allStages_stack.tif')
  } else {
    master_pop <- brick(raster('<<<< FILEPATH REDACTED >>>>>/worldpop_a0004t_5y_2000_00_00.tif'),
    raster('<<<< FILEPATH REDACTED >>>>>/worldpop_a0004t_5y_2005_00_00.tif'),
    raster('<<<< FILEPATH REDACTED >>>>>/worldpop_a0004t_5y_2010_00_00.tif'),
    raster('<<<< FILEPATH REDACTED >>>>>/worldpop_a0004t_5y_2015_00_00.tif'))
  }

  cropped_pop <- crop(master_pop, extent(subset_shape), snap="out")
  ## Fix rasterize
  initial_raster <- rasterize(subset_shape, cropped_pop, field = 'GAUL_CODE')
  if(length(subset_shape[!subset_shape$GAUL_CODE%in%unique(initial_raster),])!=0) {
    rasterized_shape <- merge(rasterize(subset_shape[!subset_shape$GAUL_CODE%in%unique(initial_raster),], cropped_pop, field = 'GAUL_CODE'), initial_raster)
  }
  if(length(subset_shape[!subset_shape$GAUL_CODE%in%unique(initial_raster),])==0) {
    rasterized_shape <- initial_raster
  }
  #rasterized_shape <- rasterize(subset_shape, cropped_pop, field='GAUL_CODE')
  masked_pop <- mask(x=cropped_pop, mask=rasterized_shape)

  raster_list <- list()
  raster_list[['simple_raster']] <- rasterized_shape
  raster_list[['pop_raster']] <- masked_pop

  return(raster_list)

}

## Grab lists of GAUL codes
get_gaul_codes <- function(gauls) {

  # Inputs: gauls = vector of continents, regions, stages, or countries
  #                 **now also accepting combinations**
  # Outputs: vector of gaul codes

  gaul_list <- NULL

  # Set up list of gaul_codes
   gaul_ref <- list()
   gaul_ref[["eastern_europe"]] <- c(165,254,26,147,140,78)
   gaul_ref[["middle_east"]] <- c(117,1,187,137,267,141,201,215,249,118,130,238,21,255)
   gaul_ref[["latin_america"]] <- c(71,72,11,63,99,123,246,20,209,211,24,108,200,258,191,75,180,162,111,103,61,28,30,57,107,73,194,233,263,195,37,33)
   gaul_ref[["se_asia"]] <- c(147296,67,147295,171,44,264,139,153,240,196,116,242)
   gaul_ref[["south_asia"]] <- c(23,31,115,175,188)
   gaul_ref[["africa"]] <- c(4,6,8,29,35,42,43,45,47,49,50,58,59,66,68,70,
                             74,76,77,79,89,90,94,95,105,106,142,144,145,150,
                             152,155,159,169,170,172,181,182,205,214,217,221,
                             226,235,243,248,253,268,270,271,40762,40765,
                             227,257,133,269)
   gaul_ref[["stage1"]] <- c(170,152,205,226,270,133,150,74,257,253,271,79,77,70,
                             58,43,89,76,50,214,59,68,45,49,8,169,6,40765,4,172,235,
                             142,227,35,243,221,217,182,181,159,155,144,105,90,106,94,
                             47,66,42,29,72,108,180,111,103,195,33,239,132,167,67,171,
                             44,264,139,153,240,196,116,242,175,188,115,117,231,31,23,
                             1,267,269,91,118,130,238,3,192,262,225,157,135)
   gaul_ref[["stage2"]] <- c(145,75,162,28,57,107,73,261,138,147295,154,187,215,249,19,
                             255,254,34,170,152,205,226,270,133,150,74,257,253,271,79,77,
                             70,58,43,89,76,50,214,59,68,45,49,8,169,6,40765,4,172,235,
                             142,227,35,243,221,217,182,181,159,155,144,105,90,106,94,
                             47,66,42,29,72,108,180,111,103,195,33,239,132,167,67,171,
                             44,264,139,153,240,196,116,242,175,188,115,117,231,31,23,
                             1,267,269,91,118,130,238,3,192,262,225,157,135)
   gaul_ref[["cssa"]]   <- c(8,49,59,68,76,89)
   gaul_ref[["cssa_diarrhea"]]   <- c(49,59,68,76,89) # Removed Angola
   gaul_ref[["sssa_diarrhea"]]   <- c(8,35,142,172,227,271) # Added Angola, removed Swaziland
   gaul_ref[["essa_diarrhea"]]   <- c(43,58,70,77,79,133,150,152,170,205,226,74,235,257,253,270) # Added Swaziland

   gaul_ref[["cssa_diarrhea2"]]   <- c(49,45,50,59,89,76)
   gaul_ref[["essa_diarrhea2"]]   <- c(43,58,70,77,79,133,150,152,170,205,226,74,235,257,253,270,6) # Added Swaziland
   gaul_ref[["name_diarrhea2"]]   <- c(4,40762,145,169,248) # no yemen for now,269)
   gaul_ref[["sssa_diarrhea2"]]   <- c(8,35,142,172,227,271) # Added Angola, removed Swaziland
   gaul_ref[["wssa_diarrhea2"]]   <- c(29,42,47,66,90,94,106,105,144,155,159,181,182,214,217,221,243)

   gaul_ref[["essa_edu"]]   <- c(226,79,74,6,77,70)
   gaul_ref[["cssa_edu"]]   <- c(182,43,58,133,150,152,170,205,235,257,253,270,49,45,59,89,76,68,8) # Added Swaziland
   gaul_ref[["name_edu"]]   <- c(4,40762,145,169,248) # no yemen for now,269)
   gaul_ref[["sssa_edu"]]   <- c(35,142,172,227,271) # Added Angola, removed Swaziland
   gaul_ref[["wssa_edu"]]   <- c(29,42,47,66,90,94,106,105,144,155,159,181,214,217,221,243,50)

   gaul_ref[["essa_edu2"]]   <- c(43,58,70,77,79,133,150,152,170,205,226,74,257,253,270)
   gaul_ref[["name_edu2"]]   <- c(4,40762,145,169,6,248) # no yemen for now,269)
   gaul_ref[["sssa_edu2"]]   <- c(35,142,172,227,235,271)
   gaul_ref[["wssa_edu2"]]   <- c(29,42,45,47,50,66,90,94,106,105,144,155,159,181,182,214,217,221,243)
   gaul_ref[["cssa_edu2"]]   <- c(8,49,59,68,76,89)

   gaul_ref[["essa"]]   <- c(43,58,70,77,79,133,150,152,170,205,226,74,257,253,270)
   gaul_ref[["name"]]   <- c(4,40762,40765,145,169,6,248) # no yemen for now,269)
   gaul_ref[["namelite"]] <- c(169,6,40765) # remove tunisia, algeria and libya from mesh
   gaul_ref[["sssa"]]   <- c(35,142,172,227,235,271)
   gaul_ref[["wssa"]]   <- c(29,42,45,47,50,66,90,94,106,105,144,155,159,181,182,214,217,221,243)
   gaul_ref[["cessa"]]  <- c(8,49,59,68,76,89,43,58,70,77,79,133,150,152,170,205,226,74,257,253,270)
   gaul_ref[["cwssa"]]  <- c(8,49,59,68,76,89,29,42,45,47,50,66,90,94,106,105,144,155,159,181,182,214,217,221,243)

   gaul_ref[["cessa2"]]  <- c(49,59,68,76,89,43,58,70,77,79,133,150,205,226,74,257,253,5) # remove AGO, ZMB, MWI, MOZ, add cameroon
   gaul_ref[["sssa2"]]    <- c(35,142,172,227,235,271,8,270,152,170) # add AGO, ZMB, MWI, MOZ

   gaul_ref[["cssa_cam"]]   <- c(8,49,59,68,76,89,45) # added cameroon
   gaul_ref[["wssa_nocam"]]   <- c(29,42,47,50,66,90,94,106,105,144,155,159,181,182,214,217,221,243) # removed cameroon

   gaul_ref[["name_hi"]] <- c(4,40765,145,169,248,40762) # No Sudan
   gaul_ref[["essa_hi"]] <- c(133,270) # Zambia and Kenya
   gaul_ref[["essa_lo"]] <- c(43,58,70,77,79,150,152,170,205,226,74,257,253,6) # Contains Sudan
   gaul_ref[["cssa_hi"]] <- c(59,76,89) # GNQ, COG, and Gabon
   gaul_ref[["cssa_lo"]] <- c(8,49,68)
   gaul_ref[["wssa_hi"]] <- c(94) #Ghana
   gaul_ref[["wssa_lo"]] <- c(29,42,45,47,50,66,90,106,105,144,155,181,182,214,217,221,243) # no Ghana
   gaul_ref[["sssa_hi"]] <- c(35,172,227) # Botswana, namibia, south africa
   gaul_ref[["sssa_lo"]] <- c(142,235,271) # LSO, Zimbabwe, Swaziland
   gaul_ref[["mwi"]] <- 152
   gaul_ref[["nga"]] <- 182
   gaul_ref[["egy"]] <- 40765
   gaul_ref[["gha"]] <- 94
   gaul_ref[["cod"]] <- 68
   gaul_ref[["zaf"]] <- 227
   gaul_ref[["essa_hilo"]] <- c(133,270,43,58,70,77,79,150,152,170,205,226,74,257,253,6,142,235,271) # Added LSO, ZWE, swaziland, and SDN

  # Start to build gaul list by matching reference list above
   gaul_list <- lapply(gauls, function(x) {gaul_ref[[x]]})
   names(gaul_list) <- gauls

  # Try to bring in additional gaul codes with gaul_convert
   gaul_list <- lapply(names(gaul_list), function(x) {

     gl <- c()
     if (is.null(gaul_list[[x]]) == T) {
       try(gl <- gaul_convert(x, from = "iso3"))
     } else {
       gl <- gaul_list[[x]]
     }

     # Message if this didn't work
     if (is.null(gl) == T) {
       message(paste0("/nUnable to find a match for the following: ", x))
       message("Check your input vector to ensure valid region names / country codes")
     }
     return(gl)
   })

  # Rearrange gaul_list to be a single vector
   gaul_list <- unlist(gaul_list)

  # Check for duplicates & drop if needed
   if (length(gaul_list) != length(unique(gaul_list))) {
     message("Duplicate gaul codes found - your input regions may overlap.")
     message("Dropping duplicates...")
   }
  gaul_list <- unique(gaul_list)
  return(gaul_list)

}

get_gaul_codes_subnat <- function(gaul_list, admin_level) {

  hierarchy <- read.dbf(paste0("<<<< FILEPATH REDACTED >>>>>", admin_level, "/g2015_2014_", admin_level, "/g2015_2014_", admin_level, ".dbf"))
  hierarchy <- as.data.table(hierarchy)
  adm0_name <- grep("ADM0_CODE", names(hierarchy), value = T)
  names(hierarchy)[names(hierarchy)==adm0_name] <- "ADM0_CODE"
  hierarchy <- hierarchy[ADM0_CODE %in% gaul_list,]
  adm_specific_name <- grep(paste0('ADM', admin_level, '_CODE'), names(hierarchy), value = T)
  adm_list <- unique(hierarchy[[adm_specific_name]])
  return(adm_list)

}


add_gauls_regions <- function(df, simple_raster) {

  # Extract GAUL_CODE from simple_raster using lat/longs
  df$GAUL_CODE <- extract(simple_raster, df[ , c('longitude', 'latitude'), with=F])

  # Add names of regions by GAUL_CODE
  for(r in Regions) {
    df <- df[GAUL_CODE %in% get_gaul_codes(r), region := r]
  }

  ##Check that merge of GAUL_CODE worked properly
  df <- df[!is.na(region), good_records := 1]
  message(paste0(length(df$good_records) - length(df[good_records==1, N]), ' out of ', length(df$good_records), ' could not have GAUL/region extracted properly. Probably coastal points, need to fix.'))
  df <- df[good_records==1, ]

}

gaul_convert <- function(countries, from = "iso3") {

  # Purpose: Convert a vector of countries to vector of GAUL codes
  # Inputs:
  #         countries: vector of countries in format below
  #         from:      format of input country list.
  #                       accepted formats: "name", "iso2", "iso3", or "undp"
  #                       default format is iso3
  # Outputs: a vector of gaul codes

  # load reference table

  if (Sys.info()["sysname"] == "Linux") {
    j_root <- "/home/j/"
  } else {
    j_root <- "J:/"
  }

  table_file <- <<<< FILEPATH REDACTED >>>>>
  gaul_table <- read.csv(table_file) %>% data.table

  # convert input & output to lower case for easier matching
  lowercase_cols <- c("short_name", "official_name", "iso3", "iso2", "uni", "undp")
  gaul_table[, (lowercase_cols) := lapply(.SD, tolower), .SDcols = lowercase_cols,]
  countries <- tolower(countries)

  if(from == "iso3") {
    gaul_code <- sapply(countries, function(x) gaul_table[iso3 == x, gaul]) %>% as.numeric

  } else if(from == "iso2") {
    gaul_code <- sapply(countries, function(x) gaul_table[iso2 == x, gaul]) %>% as.numeric

  } else if(from == "name") {

    countries <- tolower(countries)
    gaul_code <- sapply(countries, function(x) gaul_table[short_name == x, gaul]) %>% as.numeric

    #check to see if this matched all of the provided items in the vector; use partial / fuzzy matching if not
    if(length(gaul_code[!is.na(gaul_code)]) > 0) {

      # Create a table to fill in
      table_matching <- cbind(countries, gaul_code) %>% as.data.table
      names(table_matching) <- c("country", "gaul_code")
      table_matching$gaul_code <- as.numeric(table_matching$gaul_code)

      approx_matched <- table_matching[is.na(gaul_code), country]

      # Indicate that approximate matching took place

      message("\nWarning: not all country names provided were found in the lookup table.")
      message("Attempting to match names provided with those in lookup table.")
      message(paste0("Approximate matching used for: ", paste(approx_matched, collapse = ', '), "\n"))

      approx_match <- function(country) {
        gaul_code <- gaul_table[grep(country, gaul_table$short_name),]$gaul
        if (length(gaul_code) == 0) {
          gaul_code <- NA
        }
        return(as.numeric(gaul_code))
        # Could fill in other matching here if desired
      }

      # Try approximate matching
      table_matching[is.na(gaul_code)]$gaul_code <- sapply(table_matching[is.na(gaul_code)]$country,approx_match)

      not_matched <- table_matching[is.na(gaul_code)]$country

      # Error checking
      if(length(not_matched) > 0) {
        message(paste0("Warning: some countries could not be matched:\n", paste(not_matched, collapse=', ')))
      }
        gaul_code <- table_matching$gaul_code %>% as.numeric
    }

  } else {
    # Error catching for non-supported country type
    stop("\nPlease enter a valid country code type \nOptions: name, iso2, iso3")
  }

  if(length(gaul_code[is.na(gaul_code)]) == 0){
    return(gaul_code)
  } else {
    # Error catching for failure to match all country codes
    stop(paste0("\nMatches not found for all country codes in input list.\n",
                "Please check your input values"))
  }
}

## Make map of period indices to run any time periods in your data (like annual instead of 5-year)
  make_period_map <- function(modeling_periods) {
    data_period <- sort(modeling_periods)
    period_ids <- seq(data_period)
    period_map <- as.data.table(data_period)
    period_map <- period_map[, period_id := period_ids]
    return(period_map)
  }

  interpolate_gbd <- function(gbd) {
    new_gbd <- list()
    for(this_year in c(2000:2015)) {
      if(this_year %in% 2000:2004) copied_data <- gbd[year == 2000, ]
      if(this_year %in% 2005:2009) copied_data <- gbd[year == 2005, ]
      if(this_year %in% 2010:2014) copied_data <- gbd[year == 2010, ]
      if(this_year %in% 2015:2015) copied_data <- gbd[year == 2015, ]
      copied_data <- copied_data[, year := this_year]
      new_gbd[[as.character(this_year)]] <- copied_data
    }
    new_gbd <- rbindlist(new_gbd)
    new_gbd <- new_gbd[order(name, year)]
    new_gbd <- new_gbd[!(year %in% c(2000,2005,2010,2015)), mean := NA]
    library(zoo)
    for(country in unique(new_gbd[, name])) {
      new_gbd <- new_gbd[name == country, mean := na.approx(new_gbd[name == country, mean])]
    }
    return(new_gbd)
  }

make_regions_map <- function(regions, subset_shape) {
  col_list <- c('#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#ffff33','#a65628','#f781bf','#999999')
  i <- 1
  plot(subset_shape)
  for(reg in regions) {
    shapes <- subset_shape[subset_shape$GAUL_CODE %in% get_gaul_codes(reg), ]
    plot(shapes, add=TRUE, col=col_list[i])
    i <- i + 1
  }
}


##################################################################################################
##################################################################################################
## seegMBG TRANSFORM FUNCTIONS
##################################################################################################
##################################################################################################


#' @name pcaTrans
#' @rdname pcaTrans
#'
#' @title PCA transform a \code{Raster*} object
#'
#' @description Given a matrix of point coordinates and a Raster* object of
#'  covariates, return a \code{Raster*} object of the same size with the layers
#'  giving principal components from a PCA rotation based on the data values
#'  at the coordinates.
#'
#' @param coords a two-column matrix or dataframe giving the location about
#' which to carry out the PCA analysis
#'
#' @param covs a \code{Raster*} object containing covariates to rotate
#'
#' @return a \code{Raster*} object with the same extent, resolutiona and number
#'  of layers as \code{covs} but with each layer giving the location on a
#'  different principal component axis.
#'
#' @family transform
#'
#' @export
#' @import raster
#'
pcaTrans <- function(coords, covs) {


  # get covariate data at point locations
  vals <- extract(covs, coords)
  vals <- na.omit(vals)

  # do pca analysis
  pca <- prcomp(vals, retx = FALSE, center = TRUE, scale. = TRUE)

  # find non-missing cells
  cell_idx <- cellIdx(covs)

  # extract covariate values
  vals <- raster::extract(covs, cell_idx)

  # convert to a data.frame
  vals <- data.frame(vals)

  # get PCA predictions
  vals_trans <- predict(pca, vals)

  # set new raster values
  trans_ras <- insertRaster(raster = covs,
                            new_vals = vals_trans,
                            idx = cell_idx)
  return (trans_ras)

}



#' @name gamTrans
#' @rdname gamTrans
#'
#' @title Carry out a model-based covariate transformation using a GAM
#'
#' @description Define an optimal set of univariate covariate
#'  transformations of a set of model covariates by fitting a generalised
#'  additive model with univariate smoothers to data, and then using the
#'  smoothers to spline-transform the covariates.
#'  This makes use of the\code{type = 'terms'} argument in
#'  \code{\link{predict.gam}}.
#'  This function also makes use of
#'
#' @param coords a two-column matrix of coordinates of records
#'
#' @param response an object acting as thge response object in the GAM
#'  model (e.g. a vector of counts, or a matrix for binomial data)
#'
#' @param covs a \code{Raster*} object giving the spatial covariates
#'  for the main part of the model
#'
#' @param family the distribution family for the gam
#'
#' @param condition an optional vector of 1s and 0s of the same length as
#'  the number of records in \code{coords} and \code{response} and stating
#'  whether the record should also be modelled using covariates in
#'  \code{condition_covs} (1 if so and 0 if not). This enables the construction
#'  of slightly more complex models, such as those with an explicitly modelled
#'  observation process. This is achieved by passing \code{condition} to the
#'  \code{by} argument in \code{mgcv::s} when fitting smooths for
#'  the condition covariates, as well as adding the condition as an intercept.
#'
#' @param condition_covs an optional \code{Raster*} object giving the spatial covariates
#'  for the conditional part of the model
#'
#' @param extra_terms an optional formula object (of the form \code{~ s(x, k = 2)}
#'  or similar which can be concatenated onto the model formula)
#'  specifying further model components (in \code{extra_data}) not provided in
#'  the spatial covariates.
#'
#' @param extra_data an optional dataframe giving the covariates referred to in
#'  \code{extra_terms}
#'
#' @param bam whether to fit the model using \code{mgcv::bam} (the default),
#'  otherwise \code{mgcv::gam} is used instead
#'
#' @param s_args a named list of additional arguments to pass to the smoother on
#'   each covariate. For example, this may include the smoother type (\code{bs})
#'   or the basis dimension (\code{k}). See \code{\link[mgcv]{s}} for the list
#'   of available arguments.
#'
#' @param predict whether to transform the rasters after fitting the model.
#'  If set to \code{FALSE} this can enable model tweaking before the final
#'  transformations are applied, without the computational cost of prediction
#'
#' @param \dots other arguments to be passed to \code{mgcv::bam} or
#'  \code{mgcv::gam}
#'
#' @return a three-element named list containing:
#'  \itemize{
#'    \item{model}{the fitted \code{bam} or \code{gam} model object}
#'    \item{trans}{if \code{predict = TRUE} a \code{Raster*} object of the
#'     same extent, resolution and number of layers as \code{covs}, but with
#'     the values of each layer having been optimally spline-transformed.
#'     Otherwise \code{NULL}}
#'    \item{trans_cond}{if \code{predict = TRUE} and \code{condition} is not
#'     \code{NULL} a \code{Raster*} object of the same extent, resolution and
#'     number of layers as \code{condition_covs}, but with the values of each layer
#'     having been optimally spline-transformed. Otherwise \code{NULL}}
#'  }
#'
#' @export
#' @import mgcv
#' @import raster
#'
gamTrans <- function(coords,
                     response,
                     covs,
                     family = gaussian,
                     condition = NULL,
                     condition_covs = NULL,
                     extra_terms = NULL,
                     extra_data = NULL,
                     bam = TRUE,
                     s_args = list(),
                     predict = TRUE,
                     ...) {

  # test to see if covs are passed as a rasterbrick or a list
  # an RBrick indicates there are no temporal covariates
  # a list indicates either a mix, or only temporal
  # within the list RLayers are non-temporal, RBricks are temporally varying
  if(inherits(covs,'list')) {
    temporal = TRUE
  } else {
    temporal = FALSE
  }

  # run a test to see that we have years in the extra_data if we have temporal covs
  if(temporal & is.null(extra_data))
    stop('You have temporally-varying covariates, but not temporally varying data. Please include in extra data argument.')

  # test we have the same number of periods in data as in all temporal covs
  if(temporal & !is.null(extra_data)){
    for(rb in covs) {
      if(class(rb)=="RasterBrick"&dim(rb)[3]!=length(unique(extra_data$year)))
        stop('One of your temporally-varying bricks does not have the correct number of layers. Assumes year is the temporal variable in your extra_data.')
    }
  }


  # whether there's a conditional bit
  cond <- !is.null(condition)

  stopifnot(inherits(extra_terms, 'formula'))

  # check inputs
  stopifnot(inherits(covs, 'Raster')|inherits(covs, 'list'))
  if (cond)
    stopifnot(inherits(condition_covs, 'Raster'))

  # add 'cond_' onto the conditional covariate names to prevent naming conflicts
  # with the disease model
  if (cond)
    names(condition_covs) <- paste0('cond_', names(condition_covs))

  # get covariate names
  cov_names <- names(covs)
  if (cond)
    cond_names <- names(condition_covs)


  # ~~~~~~~~~~~~~
  # build formula

  cov_terms_string <- paste(sprintf('s(%s, %s)',
                                    cov_names,
                                    parseArgsS(s_args)),
                            collapse = ' + ')

  # cov_terms <- reformulate(cov_terms_string)
  # f <- response ~ 1
  # f <- f + cov_terms

  # if required, add extra terms
  if (!is.null(extra_terms))
    cov_terms_string=paste(cov_terms_string,'+',as.character(extra_terms)[2])


  f<-formula(paste('response~1+',cov_terms_string))


  # if required, add conditional terms
  if (cond) {
    cond_terms_string <- paste(sprintf('s(%s, %s, by = condition)',
                                       cond_names,
                                       parseArgsS(s_args)),
                               collapse = ' + ')

    cond_terms <- reformulate(cond_terms_string)

    f <- f + cond_terms + ~ condition_intercept
  }

  # assign any objects in the arguments of l into this environment
  # so they can be accessed by gam/bam
  if (length(s_args) > 0) {
    for (i in 1:length(s_args))
      assign(names(s_args)[i], s_args[[i]])
  }

  # ~~~~~~~~~~~~~
  # get training data

  # extraction for temporally varying and non-temporally varing covariates is slightly different
  message('Extracting covariates at data locations')
  if(temporal){
    # split apart temporally varying and non temporally varying covariates
    nT_covs=T_covs=list()
    for(c in names(covs)){
      if(class(covs[[c]])=="RasterLayer")
        nT_covs[[c]]=covs[[c]]
      if(class(covs[[c]])=="RasterBrick")
        T_covs[[c]]=covs[[c]]
    }
    time.varying.cov.names=names(T_covs)
    # initiate
    if (!is.null(extra_data)){
      data <- extra_data
    } else {
      stop('you have temporal covariates with no year extra data terms.')
    }


    # begin data with nontemporally varying because that is easy
    if(length(nT_covs)>0){
      nT_covs=brick(nT_covs)
      crs(nT_covs)="+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"
      data <- data.frame(cbind(data,extract(nT_covs, coords)))
    }


    # Note, we expect rasterbricks to go from earliest to latest period order, it will match with watch we get in extra_data to do this
    periods=sort(unique(data$year))
    tnames=c()
    for(n in names(T_covs)){
      names(T_covs[[n]])=paste0(n,periods)
      tnames=c(tnames,names(T_covs[[n]]))
      data <- data.frame(cbind(data,extract(T_covs[[n]], coords)))
    }

    # match years of time varying covariates and data points, then clean up the data frame
    # assumes a 4 year time period (years, in most cases 2000,2005,2010,2015)
    for(n in tnames)
      data[,n][data$year!= as.numeric(substr(n,nchar(n)-3,nchar(n)))]=NA
    for(n in names(T_covs))
      data[,n]=rowSums(data[,paste0(n,periods)],na.rm=T)

    data=data[,!names(data) %in% tnames]

    # save all covs in a brick, will need them later
    covs<-brick(covs)
  }



  # extract covariates if have no temporally varying
  if(!temporal) {

    data <- data.frame(extract(covs, coords))

    if (!is.null(extra_data)){
      data <- extra_data
    }
  }


  # optionally combine this with the conditional and extra data
  if (cond)
    data <- cbind(data,
                  data.frame(extract(condition_covs,
                                     coords)),
                  condition_intercept = condition,
                  condition)




  # find any missing values and remove corresponding rows
  rem_idx <- badRows(data)
  data <- data[!rem_idx, ]

  if (is.vector(response)) {
    response <- response[!rem_idx]
  } else {
    response <- response[!rem_idx, ]
  }



  message('Fitting GAM')
  # fit the model
  if (bam) {
    m <- mgcv::bam(f, data = data, family = family)
  } else {
    m <- mgcv::gam(f, data = data, family = family)
  }



  # ~~~~~~~~~~
  # optionally apply transformations

  if (predict) {

    # find index for non-missing cells
    cell_idx <- cellIdx(covs)



    # transform the main covariates

    # extract covariate values
    message('Extracting Covariates at all cells (takes a few minutes)')
    vals <- raster::extract(covs, cell_idx)


    # convert to a data.frame
    vals <- data.frame(vals)

    # add on the extra data
    vals <- cbind(vals, extra_data[rep(1, nrow(vals)), , drop = FALSE])

    # optionally add on dummy data for the condition intercept and variables
    if (cond) {
      cond_data <- data.frame(matrix(0,
                                     nrow = 1,
                                     ncol = length(cond_names)))
      names(cond_data) <- cond_names
      vals <- cbind(vals,
                    condition = 0,
                    condition_intercept = 0,
                    cond_data)
    }



    # get the transformations of these values
    message('Grab Transformed Values')

    if( temporal ) {

      # need to account for time-varying variables here first, predict with
      # them in roating order
      # loop through the years of covariates to transform them.
      valslist = list()
      for(p in 1:length(periods)){ # 1:4
        tmp=vals
        for(n in names(covs)){  # loop through all covariate names
          if(length(grep(paste0('.',p),n))>0){    # find ones that have the period number (ie 1-4, not in 2000-2015 format) in them
            names(tmp)[names(tmp)==n]=substr(n,1,nchar(n)-2)
          }
          if(length(grep(paste(paste0('.',1:4),collapse='|'),n))>0){           # drop all variables not in the time bin
            tmp[,n]=NULL
          }
        }
        valslist[[length(valslist)+1]]=data.frame(tmp)

      }
      # so now we have a list of suitable data frames with which to predict out transformations

      # thow a check for prediction frame names matching those in the estimation data used

      # predict them out
      vals_trans=list()
      for(p in 1:length(periods)){ # 1:4

        # quick fix, BAD BAD, Get lucas to make sure we have no NA values
        for(n in names(T_covs)){
          if(length(valslist[[p]][,n][is.na(valslist[[p]][,n])])!=0) {
            message(paste0(n, ': ', length(valslist[[p]][,n][is.na(valslist[[p]][,n])]), ' rows missing!'))
            message('...filling in with nearby average. Explore why you have NAs in this covariate.')
            valslist[[p]][,n][is.na(valslist[[p]][,n])]=mean(valslist[[p]][,n],na.rm=T) # do the ones around it..
          }
        }
        for(n in names(nT_covs)){
          if(length(valslist[[p]][,n][is.na(valslist[[p]][,n])])!=0) {
            message(paste0(n, ': ', length(valslist[[p]][,n][is.na(valslist[[p]][,n])]), ' rows missing!'))
            valslist[[p]][,n][is.na(valslist[[p]][,n])]=mean(valslist[[p]][,n],na.rm=T) # do the ones around it..
            message('...filling in with nearby average. Explore why you have NAs in this covariate.')
          }
        }


        message(paste0('Predicting Transform for Time Period ',p))

        # vals_trans[[p]] <- predict(m,
        #                            newdata = valslist[[p]],
        #                            type = 'terms',exclude='s(fertility)')
        vals_trans[[p]] <- predict(m,
                                   newdata = valslist[[p]],
                                   type = 'terms')

        # format the names
        colnames(vals_trans[[p]]) <- gsub(')', '', colnames(vals_trans[[p]]))
        colnames(vals_trans[[p]]) <- gsub('^s\\(', '', colnames(vals_trans[[p]]))
      }
      message('Prediction Complete')

      # now explode back out the temporally varying covariates and prep them to be returned in the same format they came in
      # in other words a list with RLs and RBs..
      message('Raster Brick non temporally varying covariates')
      nT_vals_trans=as.matrix(vals_trans[[1]][,colnames(vals_trans[[1]]) %in% names(nT_covs)]) # first remove all temporals
      nT_vals_trans=insertRaster(raster = covs,
                                 new_vals = nT_vals_trans,
                                 idx = cell_idx)


      # save temporals in their own lists, to be bricked
      message('Raster Brick and List temporally varying covariates')
      T_vals_trans=list()
      for(n in names(T_covs)){
        print(n)
        temp=data.frame(id=1:nrow(vals_trans[[1]]))
        for(p in 1:length(periods)){
          temp[,paste0(n,'.',p)]=vals_trans[[p]][,n]
        }
        temp$id=NULL
        assign(n,temp)
        T_vals_trans[[n]]=insertRaster(raster = covs,
                                       new_vals = as.matrix(get(n)),
                                       idx = cell_idx)
      }


      # set new raster values
      trans_ras <- list(nT_vals_trans=nT_vals_trans,T_vals_trans=T_vals_trans)









    } else { # if not temporal
      vals_trans <- predict(m,
                            newdata = vals,
                            type = 'terms')
      # find any condition terms
      cond_terms_idx <- grep('):condition$', colnames(vals_trans))

      # remove the condition ones and the condition index
      if (length(cond_terms_idx) > 0) {
        vals_trans <- vals_trans[, -cond_terms_idx]
      }
      vals_trans <- vals_trans[, !(colnames(vals_trans) %in% c('condition', 'condition_intercept'))]

      # format the names
      colnames(vals_trans) <- gsub(')', '', colnames(vals_trans))
      colnames(vals_trans) <- gsub('^s\\(', '', colnames(vals_trans))


      # keep only the names that are in the raster
      # gets rid of year and age bin
      vals_trans <- vals_trans[, colnames(vals_trans) %in% cov_names]

      # set new raster values
      trans_ras <- insertRaster(raster = covs,
                                new_vals = vals_trans,
                                idx = cell_idx)
    }


    # optionally apply the transformation to the conditional terms

    if (cond) {

      # remove the previous variables
      rm(vals, vals_trans)

      # extract covariate values
      vals <- raster::extract(condition_covs, cell_idx)

      # convert to a data.frame
      vals <- data.frame(vals)

      # add on the extra data
      vals <- cbind(vals, extra_data[rep(1, nrow(vals)), ])

      # add on dummy data for the main variables
      main_data <- data.frame(matrix(0,
                                     nrow = 1,
                                     ncol = length(cov_names)))

      names(main_data) <- cov_names

      vals <- cbind(vals,
                    condition = 1,
                    condition_intercept = 0,
                    main_data)

      # get the transformations of these values
      vals_trans <- predict(m,
                            newdata = vals,
                            type = 'terms')

      # keep only the condition ones
      vals_trans <- vals_trans[, grep('):condition$', colnames(vals_trans))]

      # format the names
      colnames(vals_trans) <- gsub('):condition$', '', colnames(vals_trans))
      colnames(vals_trans) <- gsub('^s\\(', '', colnames(vals_trans))

      # keep only the names that are in the raster
      vals_trans <- vals_trans[, colnames(vals_trans) %in% cond_names]

      # remove the `cond_` bit
      colnames(vals_trans) <- gsub('^cond_', '', colnames(vals_trans))

      # set new raster values
      trans_cond_ras <- insertRaster(raster = condition_covs,
                                     new_vals = vals_trans,
                                     idx = cell_idx)

    } else { # if not cond
      trans_cond_ras <- NULL
    }

  } else {    # if not poredicting, set these to NULL

    trans_ras <- trans_cond_ras <- NULL

  }

  # return the three components
  return (list(model = m,
               trans = trans_ras,
               trans_cond = trans_cond_ras))

}


addQuotes <- function (x) {
  # If x is a character string, add (escaped) quotation marks
  if (is.character(x)) {
    x <- sprintf('\"%s\"', x)
  }
  return (x)
}

parseArgsS <- function(l) {
  # parse a list of additional arguments to smoothers in gamTrans
  stopifnot(is.list(l))
  l_string <- paste(names(l),
                    lapply(l, addQuotes),
                    sep = ' = ',
                    collapse = ', ')
  return (l_string)
}


##################################################################################################
##################################################################################################
## STACKING FUNCTIONS
##################################################################################################
##################################################################################################


#Extract Covariates
#About: Reads in a data table (with latitude and longitude) and a list of rasters/bricks/stacks and extracts the values at the points
#df: a data table. Should have columns named latitude and longitude at a miniumum
#covariate list: a list object of rasters-like formats (e.g. layer, brick, stack) or just one of that raster-like object
                  #unlike some of the other MBG covariate functions, this extract_covariates function should be able to treat
                  #raster bricks as a de facto list-- only if 1 brick is passed the function doesn't automatically assume time varying)
                  #instead, that is implied from the layer names within the brick.
                  #For best use, provide a list OR a single brick when trying to analyze multiple sets of covariates
#reconcile_timevarying: If true, the functio n enforces period matching. For example, if evi is in 4 periods, there will be one EVI column
                        #where the values of the column are period/time/year specific. Period in this case is usually identified by
                        # a .#### at the end of the layer name (usually within a brick). As of 11/2, this is .1, .2, .3 ,.4 but it should
                        # work for something like .1990, .2000, etc. more testing is needed
#period_var: is there a column in the passed data frame designating the time period that matches the rasters
#return_only_results: return only the covariate columns? (better for cbinding)
extract_covariates = function(df, covariate_list, id_col = NULL, reconcile_timevarying = T, period_var = NULL, return_only_results = F, centre_scale = F, period_map = NULL){
  #Load libraryd packages. Should be redundent

  library('raster')
  library('data.table')
  df = copy(df) #data table does weird scoping things. Do this just in case

  #create an id column
  if(is.null(id_col)){
    df[,rrrr := 1:nrow(df)]
    id_col = 'rrrr'
  }

  # fill in standard 5-year period map if it's missing
  if(is.null(period_map)) period_map <- make_period_map(modeling_periods = c(2000,2005,2010,2015))

  #if covariate list is not actually a list, convert it to a list
  if(class(covariate_list) != 'list'){

    #if a brick or a stack, extract the basename
    if(class(covariate_list) %in% c('RasterBrick', 'RasterStack')){
      #get the list of brick names
      #find the prefixes, remove the .### at the end of the brick
      brick_names = unique(gsub("(\\.[0-9]+)$","",names(covariate_list)))

      #convert the brick into a list the rest of the function expects (e.g. time varying vs. single)
      #assuming I've done this properly, this will convert the raster brick into a list where items in the list are seperated by the prefix
      #(assumes a .### at the end of the name in the raster brick object refers to the period)

      covariate_list = setNames(lapply(brick_names, function(x) covariate_list[[grep(x, names(covariate_list), value =T )]]),brick_names)


    }

  }

  #rename the raster bricks
  #borrowed from some of the covariate functions
  #converts the time varying covariate names into something that places nicer with the extract function
  tv_cov_names = c()
  for(lll in names(covariate_list)){
    if(class(covariate_list[[lll]]) == 'RasterBrick'){
      tv_cov_names = append(tv_cov_names, lll)
      names(covariate_list[[lll]]) = paste0('XXX',1:length(names(covariate_list[[lll]]))) #use XXX as a place holder
    }

  }



  #extract the rasters by points
  locations = SpatialPoints(coords = as.matrix(df[,.(longitude,latitude)]), proj4string = CRS(proj4string(covariate_list[[1]])))

  cov_values = as.data.frame(lapply(covariate_list, function(x) extract(x, locations)))

  #fix the names of the time varying covariates
  names(cov_values) = sub('XXX', '', names(cov_values))

  #right now 4 periods are assumed-- make more flexible later

  #reconcile time varying covariates
  #start by converting the year variable into periods
  #if period is empty -- infer
  if(is.null(period_var)){

    year_map = as.data.frame(sort(unique(df[,year])))
    year_map$period_hold = 1:nrow(year_map)
    names(year_map) = c('year','period_hold')

    df = merge(df, year_map, by = 'year', sort =F) #sorting screws up things relative to cov values
    period_var = 'period_hold'
  }

  #sort the dataset by rid

  setorderv(df, cols = c(id_col))


  if(return_only_results){
    df = df[, c(id_col, 'longitude', 'latitude', period_var), with = F]
  }

  #combine the dataset with the covariate extractions
  df = cbind(df, cov_values)

  #reshape to fill out the columns-- these just get the tv cov names in a way that is needed for two different efforts
  tv_cov_colnames = grep(paste(tv_cov_names, collapse = "|"), names(df), value = T) #unlisted
  tv_cov_colist = lapply(tv_cov_names, function(x) grep(x, names(df), value = T))

  #Keep only where period of the data matches the period of the covariate for time varying covariates
  if(reconcile_timevarying){
    #reshapes the time varying covariates long
    df = melt(df, id.vars = names(df)[!names(df) %in% tv_cov_colnames], measure = tv_cov_colist, value.name = tv_cov_names, variable.factor =F)

    #melt returns different values of variable based on if its reshaping 1 or 2+ columns.
    #enforce that it must end with the numeric after the period
    df <- df[,variable:= as.numeric(substring(variable,regexpr("(\\.[0-9]+)$", variable)[1]+1))]

    #keep only where the periods match
    df <- merge(df, period_map, by.x = period_var, by.y = 'data_period')
    df = df[period_id == variable, ]

    #clean up
    df = df[,variable := NULL]

  }

  #clean up
  if(period_var=='period_hold'){
    df = df[,period_hold := NULL]
  }

#centre_scale the results
  if(centre_scale){
    design_matrix = data.frame(df[,names(covariate_list), with =F])
    cs_df <- getCentreScale(design_matrix)
    design_matrix <- centreScale(design_matrix, df = cs_df)

    #replace the df columns with the design matrix
    df[, names(covariate_list) := NULL]
    df = cbind(df, design_matrix)

  }

  df <- df[, year := NULL]

  #return the data frame with the year-location specific covariates
  if(return_only_results & reconcile_timevarying){
    df = df[, names(df)[!names(df) %in% c('latitude','longitude')], with = F]
  }

  #sort df just to be sure
  setorderv(df, cols = c(id_col))
  if(id_col == 'rrrr') df[,rrrr := NULL]

  if(centre_scale){
    return(list(covs = df, cs_df = cs_df))
  } else{
    return(df)
  }
}




#for parsing gams string functions from the seeg stuff
parseArgsS <- function(l) {
  # parse a list of additional arguments to smoothers in gamTrans
  stopifnot(is.list(l))
  l_string <- paste(names(l),
                    lapply(l, addQuotes),
                    sep = ' = ',
                    collapse = ', ')
  return (l_string)
}

#fit gam: a function to fit a gam or bam
#df: data table with the outcome/indicator and some covariates already extracted. This is different than the gam_cov functions
#covariates: a  vector of covariate names and/or formula in the style of all_fixed_effects (e.g. cov1 + cov2 + cov3)
#additional terms: a vector or single character of column names to be included in the model fit
#weight_column: in df, is there a column that specifies the observation weights?
#bam: should bam be used rather than gam?
#spline args: additional arguments to be sent to the spline call of gam/bam
#auto_model_select: if true, it overwrites BAM instructions to fit a GAM on smaller (N<2000) datasets-- helps with convergence
#indicator: name of the column of the DV
#indicator_family: model family
#cores: # of cores are available for use
fit_gam = function(df, covariates = all_fixed_effects, additional_terms = NULL, weight_column = NULL, bam = F, spline_args = list(), auto_model_select =F, indicator, indicator_family = 'binomial', cores = 1){
  library(mgcv)
  library(data.table)
  #also requires seeg

  df = copy(df) #in case data table scoping gets wonky

  #check to see if the gam formula is prespecified. if not, make it
  #check to see if its a vector of characters or a psudo-formula
  covariates = format_covariates(covariates) #additional terms is handled below

  #remove binary vars from the splined versions and set as additional terms
  n_uniq_vals = unlist(setNames(df[,lapply(covariates, function(x) uniqueN(get(x)))], covariates)) #this needs to return a named vector
  binary_vars = names(n_uniq_vals[n_uniq_vals<=2])
  covariates = covariates[!covariates %in% binary_vars]

  additional_terms = c(additional_terms,binary_vars)
  additional_terms = additional_terms[!(is.null(additional_terms) | is.na(additional_terms))]

  #set response variable (stolen from GAM trans)
  if(indicator_family=="binomial") response <- cbind(events = df[, get(indicator)], trials = df[, N] - df[, get(indicator)])
  if(indicator_family=="gaussian") response <- cbind(outcome = df[, get(indicator)])

  #sort out the formula body
  f_bod = paste(paste0('s(',covariates,', ',parseArgsS(spline_args),')'),collapse = " + ")
  gam_formula = paste0("response ~ 1 + ", f_bod)
  if(length(additional_terms)>0) gam_formula = paste0(gam_formula,' + ', paste(additional_terms, collapse = " + "))

  gam_formula = as.formula(gam_formula) #final model formula

  # format weights
  if(!is.null(weight_column)){
    df[,data_weight := get(weight_column)]
  } else{
    df[,data_weight := 1]
  }
  weight_column = 'data_weight'

  #fit the gam
  message(paste0('Fitting GAM/BAM with spline args of: ', names(spline_args)[1],'=',spline_args[1],' ', names(spline_args)[2],'=', spline_args[2]))
  if(bam){
    if(auto_model_select ==T & nrow(df)<2000){
      model = mgcv::gam(gam_formula, data = df, family = indicator_family, weights = df[,get(weight_column)], control = list(nthreads = as.numeric(cores)))
    } else{
      model = mgcv::bam(gam_formula, data = df, family = indicator_family, weights = df[,get(weight_column)], nthreads = as.numeric(cores), discrete = T)
    }
  } else{
    model = mgcv::gam(gam_formula, data = df, family = indicator_family, weights = df[,get(weight_column)], control = list(nthreads = as.numeric(cores)))
  }

  #return the gam object
  return(model)

}

#a wrapper function for fitting gam/bam models in the stacking framework. It'll run 1+k times internally
#arguments are the same as fit gam above.
#basically a wrapper function for the fit_gam function
fit_gam_child_model = function(df, model_name = 'gam', fold_id_col = 'fold_id', covariates = all_fixed_effects,
                                additional_terms = NULL, weight_column = NULL, bam =F, spline_args = list(bs = 'ts', k = 3),
                                auto_model_select =T, indicator = indicator, indicator_family = 'binomial', cores = 1){
  library(mgcv)
  #remove scoping surprises
  df = copy(df)

  #start by fitting the full gam
  message('Fitting the Full GAM model')
  full_model = fit_gam(df,
                      covariates = covariates,
                      additional_terms = additional_terms,
                      weight_column = weight_column,
                      bam = bam,
                      spline_args = spline_args,
                      auto_model_select =auto_model_select,
                      indicator = indicator,
                      indicator_family = indicator_family,
                      cores = cores)

  #add a name to the game object
  full_model$model_name = model_name

  #fit the child/cv gams
  message("Fitting baby gams")
  #fit the child/cv rfs
  folds = unique(df[,get(fold_id_col)])


  for(fff in folds){
    baby_model = fit_gam(df = df[get(fold_id_col) != fff,],
                       covariates=covariates,
                       additional_terms = additional_terms,
                       bam = bam,
                       weight_column = weight_column,
                       spline_args = spline_args,
                       auto_model_select = auto_model_select,
                       indicator = indicator,
                       indicator_family = indicator_family,
                       cores = cores)

    #fill in the data
    df[get(fold_id_col)==fff, paste0(model_name,'_cv_pred') := predict(baby_model, df[get(fold_id_col)==fff,],type = 'response')]
  }

  #predict using full model fit earlier
  df[,paste0(model_name,'_full_pred') := predict(full_model,df,type = 'response')]

  #return a subset of the columns. Full pred denotes the fit from the full model. CV pred is the OOS stuff
  suffixes = c('_full_pred', '_cv_pred')
  return_cols = paste0(model_name, suffixes)
  return(setNames(list(df[,return_cols,with = F], full_model),c('dataset',paste0(model_name))))
}


#adapted from R's BRT covs function-- took away the year specific bit. GBMs==BRTs
#df: a data table (post extract covariates)
#covariates: rhs of formula specifying the covariates/columns to be used to help fit the model
#weight_column: column in the data table that specifies the weight
#tc: tree complexity
#lr: learning rate
#bf: bag fraction
#indicator: dependant variable.
#indicator_family: Binomial models assume N as the # of trials and are actually modelled with poission with N as the offset
fit_gbm= function(df, covariates = all_fixed_effects, additional_terms = NULL, weight_column = NULL, tc = 4, lr = .005, bf = .75, indicator, indicator_family = 'binomial'){


  library(dismo)

  #check to see if its a vector of characters or a psudo-formula
  covariates = format_covariates(add_additional_terms(covariates,additional_terms))

  df = copy(df)

  #format weights
  if(!is.null(weight_column)){
    df[,data_weight := get(weight_column)]
  } else{
    df[,data_weight := 1]
  }
  weight_column = 'data_weight' #specify the column

  # set up poisson outcome structure for binomial and poisson data
  if(indicator_family %in% c('binomial','poisson'))  {
    indicator_family = 'poisson'
    offset=log(df[,N])
    df[,pre_round := get(indicator)]
    df[,paste0(indicator) := round(pre_round,0)]
  } else offset = NULL

  message(paste('Fitting GBM/BRT with tc:',tc,'lr:',lr,'bf:',bf))

    mod <- try(
      gbm.step(data             = df,
               gbm.y            = indicator,
               gbm.x            = covariates,
               offset           = offset,
               family           = indicator_family,
               site.weights     = df[,get(weight_column)],
               tree.complexity  = tc,
               learning.rate    = lr,
               bag.fraction     = bf,
               silent           = T),silent=TRUE)
    if(is.null(mod)){
      message('First BRT attempt failed. Lowering Learning Rate by 1/10')
      mod <- try(
        gbm.step(data             = df,
                 gbm.y            = indicator,
                 gbm.x            = covariates,
                 offset           = offset,
                 family           = indicator_family,
                 site.weights     = df[,get(weight_column)],
                 tree.complexity  = tc,
                 learning.rate    = lr*.1,
                 bag.fraction     = bf,
                 silent           = T))
    }
    if(is.null(mod)){
      message('Second BRT attempt failed. Lowering Original Learning rate by 1/1000 AGAIN')
      mod <- try(
        gbm.step(data             = df,
                 gbm.y            = indicator,
                 gbm.x            = covariates,
                 offset           = offset,
                 family           = indicator_family,
                 site.weights     = df[,get(weight_column)],
                 tree.complexity  = tc,
                 learning.rate    = lr*.001,
                 bag.fraction     = bf,
                 silent           = T))
    }
    if(is.null(mod)){
      message('Third BRT attempt failed. Slow learn plus low tree complexity')
      mod <- try(
        gbm.step(data             = df,
                 gbm.y            = indicator,
                 gbm.x            = covariates,
                 offset           = offset,
                 family           = indicator_family,
                 site.weights     = df[,get(weight_column)],
                 tree.complexity  = 2,
                 learning.rate    = lr*.001,
                 bag.fraction     = bf,
                 silent           = T))
    }

    if(is.null(mod)) stop('ALL BRT ATTEMPTS FAILED')

    return(mod)
}

#a function to fit the GBMs for stacking
#basically a wrapper function for the fit_gam function
#model_name = what do you want the full fit model to be called upon the return. Must sync with subsquent functions
fit_gbm_child_model = function(df, model_name = 'gbm', fold_id_col = 'fold_id', covariates = all_fixed_effects, additional_terms = NULL, weight_column = NULL,
                                tc = 4, lr = 0.005, bf = 0.75, indicator = indicator, indicator_family = indicator_family, cores = 1){

  library(parallel)

  #prevent df scoping
  df = copy(df)

  #fit the baby trees in parallel
  folds = unique(df[,get(fold_id_col)])

  message('Fitting baby gbm models in parallel')
  baby_models = mclapply(folds, function(fff)
            fit_gbm(df = df[get(fold_id_col) != fff,],
            covariates = covariates,
            additional_terms   = additional_terms,
            weight_column      = weight_column,
            tc                 = tc,
            lr                 = lr,
            bf                 = bf,
            indicator          = indicator,
            indicator_family   = indicator_family), mc.cores = cores)


  for(fff in folds){
    #use the data fit on K-1 of the folds to fit on the help out fold
    df[get(fold_id_col)==fff, paste0(model_name,'_cv_pred') := predict(baby_models[[fff]], df[get(fold_id_col)==fff,], n.trees=baby_models[[fff]]$gbm.call$best.trees,type='response')]
  }


  #fit GBM
  message('Fitting Full GBM')
  full_model = fit_gbm(df = df,
           covariates = covariates,
           additional_terms   = additional_terms,
           weight_column      = weight_column,
           tc                 = tc,
           lr                 = lr,
           bf                 = bf,
           indicator          = indicator,
           indicator_family   = indicator_family)


  #add a model name slot
  full_model$model_name = model_name

  #predict the main BRT
  df[,paste0(model_name,'_full_pred') := predict(full_model,df,n.trees=full_model$gbm.call$best.trees,type = 'response')]

  suffixes = c('_full_pred', '_cv_pred')
  return_cols = paste0(model_name, suffixes)
  return(setNames(list(df[,return_cols,with = F], full_model),c('dataset',paste0(model_name))))

}


#fetch covariate layer
#given a raster-like object and a period, returns the appropriate layer, assuming chronological order
fetch_covariate_layer = function(ras, period = 1){
  if(class(ras) == 'RasterBrick' | class(ras) == "RasterStack"){
    return(ras[[period]])
  } else{
    return(ras)
  }
}

#create a stacked prediction for each period and child model. Function returns a list of raster objects. First object is a brick of stacked results. Additional objects refer to the child models
#covariate_layers: list of raster objects that consititue the covs. Must share names with the DT that the models were fit on
#period: what period should be used. 1:N -- as of 11/2 we assume 4
#child_models: a list of model objects from the child models
#stacker_model: the model object from the stacker
#return children: return the prediction rasters from the child models as well
#todo: allow dataframes of constants to be passed
produce_stack_rasters = function(covariate_layers = all_cov_layers, #raster layers and bricks
                                 period = 1, #period of analysis
                                 child_models = list(), #gam model fitted to full data
                                 stacker_model = stacker,
                                 indicator_family = 'binomial',
                                 return_children = F,
                                 centre_scale_df = NULL){ #stacker model

  message(paste0('The Period is ', period))

  #fetch the covariates appropriate for the period
  period_covs =brick(lapply(covariate_layers, function(x) fetch_covariate_layer(x,period)))

  #create constants -- only flexible for year/period
  year = data.frame(year = (1995 +period*5))

  #predict the various models. This is super strict with variable names (and beware scoping issues with the names)
  #brick the results
  stacker_predictors = brick(lapply(child_models, function(x) predict_model_raster(x, period_covs, constants = year, indicator_family = indicator_family, centre_scale_df = centre_scale_df)))

  #take the rasters of the stacker predictors and predict using stacker
  #to: reuse the predict_model_raster function and add glm functionality to it
  stacked_ras = predict_model_raster(model_call = stacker_model, covariate_layers = stacker_predictors, indicator_family = indicator_family)


  #return the results
  if(return_children){
    return(list(setNames(stacked_ras,'stacked_results'), stacker_predictors))
  }else{
    stacked_ras = setNames(stacked_ras,'stacked_results')
    return(stacked_ras)
  }
}


#Wrapper function for produce stacked rasters
#Runs all (or a subset of periods) and formats the results.
make_stack_rasters = function(covariate_layers = all_cov_layers, #raster layers and bricks
                              period = NULL, #period of analysis, NULL returns all periods
                              child_models = list(), #gam model fitted to full data
                              stacker_model = stacked_results[[2]],
                              indicator_family = 'binomial',
                              return_children = F,
                              rd = run_date,
                              re = reg,
                              ind_gp = indicator_group,
                              ind = indicator,
                              ho = holdout,
                              centre_scale_df = NULL){

  ## first, save child_models for use in get.cov.wts in post_estiamtion if not other places too
  save(child_models, file=<<<< FILEPATH REDACTED >>>>>)

  if(is.null(period)){
    period = 1:4
  }

  res = lapply(period, function(the_period) produce_stack_rasters(
              covariate_layers = covariate_layers, #raster layers and bricks. Covariate rasters essentially
              period = the_period, #period of analysis. Fed in from the lapply
              child_models = child_models, #a list of model objects for the child learners
              stacker_model = stacker_model, #the model object of the stacker
              indicator_family= indicator_family,
              return_children = return_children,
              centre_scale_df = centre_scale_df))

  stacked_rasters = brick(lapply(1:length(res), function(x) res[[x]][[1]]))



  if(return_children){
    cms = lapply(1:length(res), function(x) res[[x]][[2]])
    cms = lapply(1:dim(cms[[1]])[[3]], function(x) brick(sapply(cms,'[[', x)))

    ret_obj = unlist(list(stacked_rasters,cms))
  } else{
    ret_obj = list(stacked_rasters)
  }

  #set the names of the return object
  ret_obj_names = unlist(lapply(ret_obj, function(x) unique(gsub("(\\.[0-9]+)$","",names(x)))))

  #return!
  return(setNames(ret_obj, ret_obj_names))
}

#predict model raster: given a model object and some covariates, predict out the results in raster form
#model call: the model object you want to create rasters from
#covariate_layers: a list of raster-like objects named identically to the models fit on the tabular data
#constants: do any constants need to be fed to the prediction step?
#indicator_family: model family. Specifies what sorts of transformations need doing
#to do: add a transform switch
predict_model_raster = function(model_call, covariate_layers, constants = NULL, indicator_family = 'binomial', centre_scale_df = NULL){
  message(paste0('predicting out:', model_call$model_name))
  #convert the raster objects into a named matrix
  dm = as.data.frame(stack(covariate_layers),xy =T)

  #apply centre scaling
  if(!is.null(centre_scale_df)){
    cs_dm = centreScale(dm[,names(covariate_layers)], df = centre_scale_df)
    dm = cbind(dm[,c('x','y')], cs_dm)
  }

  orig_names = names(dm)

  #add constants(if present) to the raster
  if(!is.null(constants)){
    dm = cbind(dm, constants)
  }
  #create a template
  dm$rid = 1:nrow(dm)
  template = dm[,c('rid','x','y')]

  #drop rows with NA data
  dm = na.omit(dm)


  #if a gam or a bam
  if(inherits(model_call, 'gam') | inherits(model_call, 'bam')){

    ret_obj = predict(model_call, newdata =dm, type = 'response')

  } else if(inherits(model_call, 'gbm')){

    ret_obj = predict(model_call, newdata=dm, n.trees = model_call$gbm.call$best.trees, type = 'response')

  } else if(inherits(model_call, 'glmnet')){


    dm_n = names(dm)
    dm = as.matrix(dm)
    colnames(dm) = dm_n

    ret_obj = predict(model_call, newx = data.matrix(dm[,rownames(model_call$beta)]), s=model_call$cv_1se_lambda, type = 'link')

    #backtransform into probability/percentage space
    if( indicator_family=='binomial'){
      ret_obj = invlogit(ret_obj)
    }


    #return dm to its data frame form
    dm = data.frame(dm)
    colnames(ret_obj) = 'ret_obj'
  } else if(inherits(model_call, 'randomForest')){

    ret_obj = predict(model_call, newdata=dm, type = 'response')

    #back transform if binomial
    if( indicator_family=='binomial'){
        ret_obj = invlogit(ret_obj)
    }

  } else if(inherits(model_call, 'earth')){
    ret_obj = predict(model_call, newdata=dm, type = 'response')
    colnames(ret_obj) ='ret_obj'
  }

  #convert back to a raster
  ret_obj = cbind(data.frame(rid = dm[,c('rid')]), ret_obj = ret_obj)

  #restore to former glory
  ret_obj= merge(template, ret_obj, by = 'rid', all.x =T)
  setorder(ret_obj, rid)
  ret_obj = rasterFromXYZ(ret_obj[,c('x','y','ret_obj')], res = res(covariate_layers[[1]]), crs=crs(covariate_layers[[1]]))

  #return the object
  return(setNames(ret_obj,model_call$model_name))
}


#Stack models using GAM/BAM
#df: the data frame
#indicator: indicator of analysis
#indicator_family: what is the analytical model of the family
#bam: should bam be used?
#spline_args: spline parameters passed to GAM/BAM function
gam_stacker = function(df, #the dataset in data table
                       model_names=c('gam','gbm'),
                       weight_column = NULL, #prefixes of the models to be stacked
                       bam = F, #whether or not bam should be used
                       spline_args = list(bs = 'ts', k = 3),
                       indicator = indicator, #the indicator of analysis
                       indicator_family = indicator_family,
                       cores = 1){
  ##start function##

  #copy dataset to avoid weird data table scoping issues
  df = copy(df)

  #format the outcome variable depending on the family
  if(indicator_family == 'binomial'){
    df[, failures := N-get(indicator)]
    outcome = df[, .(get(indicator),failures)]
    names(outcome)[1] = indicator
  } else{

    outcome = df[,.(get(indicator))]
    names(outcome)[1] = indicator
  }

  outcome = as.matrix(outcome)

  #format the child model results into the glm format
  #create new columns to hold the cv results, which are then replaced with the full results upon prediction
  df[, (model_names) := lapply(model_names, function(mn) get(paste0(mn,'_cv_pred')))]

  #Fit the gam as the stacker
  stacker = fit_gam(df = df,
            covariates = paste(model_names, collapse = ' + '),
            additional_terms = NULL,
            weight_column = NULL,
            bam = bam,
            spline_args = spline_args,
            auto_model_select =T,
            indicator = indicator,
            indicator_family = indicator_family,
            cores = cores)

  stacker$model_name = 'gam_stacker'

  #predict the results as fit from the crossvalidated stuff
  df[,stacked_cv_pred := predict(stacker, df, type = 'response')]

  #overwrite the columns to work on the full fit child modules
  df[, (model_names) := lapply(model_names, function(mn) get(paste0(mn,'_full_pred')))]
  df[,stacked_pred := predict(stacker, df, type = 'response')]

  #return the dataframe and the stacker model
  return(setNames(list(df[,stacked_pred], stacker),c('dataset','stacker_model'))) #[,.(stacked_pred)]

}


#Take the emperical logit
emplogit = function(success, N, epsilon = NULL) {
  #http://stats.stackexchange.com/questions/109702/empirical-logit-transformation-on-percentage-data
  #squeeze in the edges
  tform = success/N

  #if epsilon is null, follow the instructions from the attached link
  if(is.null(epsilon)){
    epsilon = min(tform[tform >0 & tform <1])/2
  }

  tform[tform==0] = tform[tform == 0] + epsilon
  tform[tform==1] = tform[tform==1] - epsilon
  tform = log(tform/(1-tform))

  return(tform)
}

#fit penalized regressions(lasso, elastic net, ridge)
#df: the data, in data table format
#covariates: string in formula notation denoting the covariates
#additional_terms: other covariates to include
#alpha: alpha parameter for glmnet calls
#cores: how many cores are available
#offset: is there an offset, valid only for poission models.
fit_glmnet = function(df, covariates = all_fixed_effects, additional_terms = NULL, weight_column = NULL, alpha = 1, indicator, indicator_family = 'binomial', parallel = F){

  library(glmnet)
  library(doParallel)
  df = copy(df)

  #add additional terms if requested
  the_covs = format_covariates(add_additional_terms(covariates,additional_terms))

  #format weights
  if(!is.null(weight_column)){
    data_weights = df[,get(weight_column)]
  } else {
    data_weights = rep(1,nrow(df))
  }


  #create outcome object
  if(indicator_family == 'binomial'){
    not_indicator = df[,N] - df[,get(indicator)]
    response_var = cbind(not_indicator, outcome = df[,get(indicator)])
  } else if (indicator_family == 'poisson') {
    if(!is.null(offset)){
      stop('fit_glmnet does not currently work for offset poissons')
    } else {
      response_var = df[,get(indicator)]
    }
  } else {
    response_var = df[,get(indicator)]
  }


  #create design matrix
  dm = as.matrix(df[,the_covs, with = F])
  colnames(dm) = the_covs

  message(paste0('Fitting glmnet with alpha: ', alpha))
  cv_res = cv.glmnet(x = dm , y= response_var, family = indicator_family, alpha = alpha, weights = data_weights, parallel = parallel)

  #fit full model using selected lambdas
  model = glmnet(x = dm , y= response_var, family = indicator_family, lambda = cv_res$lambda, alpha = alpha, weights = data_weights)

  #preserve the cv_1se_lambda
  model$cv_1se_lambda = cv_res$lambda.1se

  return(model)

}

#Fit a glmnet child model
#df: the data in data table format
#model_name: what do you want to call this?
#covariates: formula of the fixed effects
#fold_id_col: what is the column iding the fold
#additional_terms: constants, other covarites. Usually only used for year and other non-raster covariates.
#indicator_family: analyitical family
#indicator: what indicator
#cores: how many cores can be used?
#alpha: paramter for glmnet
fit_glmnet_child_model = function(df,model_name = 'glmnet', fold_id_col = 'fold_id', covariates = all_fixed_effects, additional_terms = NULL, weight_column = NULL, alpha = 1, indicator = indicator, indicator_family = 'binomial', cores = 1){
  library(glmnet)
  library(doParallel)
  df = copy(df)
  message('Fitting the Full GLMNET')


  the_covs = format_covariates(add_additional_terms(covariates, additional_terms))

  #start the cluster
  if(cores>1) registerDoParallel(cores = cores)

  #fit the full model
  full_model = fit_glmnet(df,
                     covariates = covariates,
                     additional_terms = additional_terms,
                     weight_column = weight_column,
                     alpha = alpha,
                     indicator = indicator,
                     indicator_family = indicator_family,
                     parallel = cores>1)

  full_model$model_name = model_name

  #fit the child/cv rfs
  folds = unique(df[,get(fold_id_col)])


  for(fff in folds){
    baby_model = fit_glmnet(df[get(fold_id_col) != fff,],
                       covariates = covariates,
                       additional_terms = additional_terms,
                       weight_column = weight_column,
                       alpha = alpha,
                       indicator = indicator,
                       indicator_family = indicator_family,
                       parallel = cores>1)

    new_data = df[get(fold_id_col)==fff,the_covs, with = F]

    n_nd = names(new_data)
    new_data = as.matrix(new_data)
    names(new_data) = n_nd

    df[get(fold_id_col)==fff, paste0(model_name,'_cv_pred') := predict(baby_model, newx = new_data, s = baby_model$cv_1se_lambda, type = 'link')]

  }

  #stop the cluster just in case
  stopImplicitCluster()

  #predict using full model fit earlier
  new_data = df[,the_covs, with = F]

  n_nd = names(new_data)
  new_data = as.matrix(new_data)
  names(new_data) = n_nd
  df[,paste0(model_name,'_full_pred') := predict(full_model,newx = new_data, s = full_model$cv_1se_lambda, type = 'link')]

  #return a subset of the columns. Full pred denotes the fit from the full model. CV pred is the OOS stuff
  suffixes = c('_full_pred', '_cv_pred')
  return_cols = paste0(model_name, suffixes)

  #if binomial, undo the logit
  if( indicator_family=='binomial'){
    df[,return_cols[1] := invlogit(get(return_cols[1]))]
    df[,return_cols[2] := invlogit(get(return_cols[2]))]
  }


  #set up with for the return call
  return(setNames(list(df[,return_cols,with = F], full_model),c('dataset',paste0(model_name))))
}


add_additional_terms= function(fes, add_terms = NULL){
  new_list = paste(unlist(sapply(c(fes, add_terms), function(x) format_covariates(x))), collapse = ' + ')
  return(new_list)
}


##################################################################################################
##################################################################################################
## VALIDATION FUNCTIONS
##################################################################################################
##################################################################################################



##################################################################################################
##################################################################################################
## Overview:  Will search saved results and model objects to produce m draws of estimates
#             for each holdout area. Note this is written as a unit (ie for one reg, holdout),
#             an will likely be run in a loop or apply function
#
## Inputs:
#          indicator_group: you know
#          indicator: oh you know
#          run_date: you definitely know
#          reg: string, region abbreviation, or just 'africa'
#          addl_strat: ie c('age'=1), make age=0 for others
#          holdout: holdout number
#
## Outputs: A table with one row per area-holdout, with p as estimated from data
#           and m draws of p_hat for each area
##################################################################################################
##################################################################################################

aggregate_validation <- function( holdoutlist = stratum_qt,
                                  cell_draws_filename = '%s_cell_draws_eb_bin%i_%s_%i%s.RData',
                                  years = c(2000,2005,2010,2015),
                                  indicator_group,
                                  indicator,
                                  run_date,
                                  reg,
                                  holdout,
                                  vallevel = "",
                                  addl_strat = c('age'=0),
                                  sbh_wgt = NULL,
                                  returnaggonly=TRUE,
                                  iter=i){

  require(raster); require(data.table)

  ## Load data
  datdir <- <<<< FILEPATH REDACTED >>>>>

  # cell draws
  cdfile <- sprintf(paste0(datdir,cell_draws_filename),indicator,addl_strat,reg,holdout,vallevel)
  #message(paste0('Loading cell draws from ',cdfile))
  load(cdfile) # loads cell pred

  # holdout data
  d          <- data.frame(holdoutlist[[sprintf('region__%s___%s__%i',reg,names(addl_strat),addl_strat)]])
  if(holdout!=0) {
    d_oos      <- d[d$fold==holdout,]
  } else {
    d_oos      <- d
    d_oos$fold = 0
  }
  # Acct for weights
  d_oos$indic_orig <-   d_oos[,indicator]
  if(is.null(sbh_wgt)){
    d_oos$exposure       <- d_oos$N*d_oos$weight
    d_oos[,indicator]    <- d_oos[,indicator] *d_oos$weight
  } else { # either a variable set (char) or numeric auto down weight
    if(class(sbh_wgt)=='character'){
      d_oos$exposure     <- d_oos$N*d_oos$weight*d_oos[,sbh_wgt]
      d_oos[,indicator]  <- d_oos$died*d_oos$weight*d_oos[,sbh_wgt]
    }
    if(class(sbh_wgt)=='numeric'){
      d_oos$exposure      <- d_oos$N*d_oos$weight*sbh_wgt
      d_oos[,indicator]   <- d_oos[,indicator] *d_oos$weight*sbh_wgt
    }
  }


  draws=dim(cell_pred)[2]

  # load simple raster
  load(paste0('<<<< FILEPATH REDACTED >>>>>/simple_raster',reg,'.RData'))

  # for each draw in cell_draws, pull the p
  r_list = list()
  for(i in 1:draws){
    r_list[[length(r_list)+1]] <- insertRaster(simple_raster, matrix(cell_pred[,i],ncol = length(years)))
  }

  # extract probabilities from draw rasters
  ycol = match(d_oos$year,years)
  for(i in 1:draws){
    # extract at locations
    t=raster::extract(r_list[[i]],cbind(d_oos$longitude,d_oos$latitude))
    # match year and put into df
    d_oos[,paste0(indicator,'hat_',i)]=      sapply(seq_along(ycol), function(x){t[x,ycol[x]]}) * d_oos$exposure
    d_oos[,paste0(indicator,'hat_full_',i)]= sapply(seq_along(ycol), function(x){t[x,ycol[x]]}) * d_oos$N

  }


  # get binomial draws for each point-draw, and estimate the coverage based on X%ci
  x <- matrix(NA,ncol=draws,nrow=nrow(d_oos))
  for(i in 1:draws){
    x[,i] <- rbinom(nrow(d_oos),
                    size=round(d_oos$N,0),
                    prob=d_oos[,paste0(indicator,'hat_full_',i)]/d_oos$N)
  }
  for(c in c(95)){
    coverage = c/100
    li=apply(x,1,quantile,p=(1-coverage)/2,na.rm=T)
    ui=apply(x,1,quantile,p=coverage+(1-coverage)/2,na.rm=T)
    d_oos[,paste0('clusters_covered_',c)] = d_oos[,'indic_orig']>=li & d_oos[,'indic_orig'] <= ui
  }

  # collapse into areas
  d_oos[,names(addl_strat)]<-addl_strat
  d_oos$total_clusters=1
  res <- d_oos[c(names(addl_strat),'fold','ho_id','year',indicator,'exposure',paste0('clusters_covered_',c(95)),'total_clusters',paste0(indicator,'hat_',1:draws))]
  f   <- as.formula(paste('.~ho_id+year+fold',names(addl_strat),sep='+'))
  resfull <- copy(res)
  res   <- aggregate(f,data=res,FUN=sum)

  # transform back to probability
  res$p = res[,indicator]/res$exposure
  res$region = reg
  res$oos = res$fold != 0
  for(i in 1:draws){
    res[,paste0('phat_',i)]=res[,paste0(indicator,'hat_',i)]/res$exposure
    res[,paste0(indicator,'hat_',i)]=NULL
  }
  res[,indicator]=NULL

  message(sprintf('%i is finished in function as %s',iter,paste0(class(res),collapse=' ')))


  ## return results table
  if(returnaggonly==TRUE){
    return(data.table(res))
  } else {
    # transform back to probability
    resfull$p = resfull[,indicator]/resfull$exposure
    resfull$region = reg
    resfull$oos = resfull$fold != 0
    for(i in 1:draws){
      resfull[,paste0('phat_',i)]=resfull[,paste0(indicator,'hat_',i)]/resfull$exposure
      resfull[,paste0(indicator,'hat_',i)]=NULL
    }
    resfull[,indicator]=NULL
    return(list(agg=data.table(res),cluster=data.table(resfull)))
  }

}



##################################################################################################
##################################################################################################
## U5M SPECIFIC FUNCTIONS
##################################################################################################
##################################################################################################



load_u5m_data <- function(file,dir = datadrive){
  d<-fread(paste0(dir,toupper(file),'.csv'),stringsAsFactors = FALSE)
  message(sprintf('Numbers of records read in: %s = %i',file,nrow(d)))
  return(d)
}

# redistribute ages where only the year of death was given (center them)
redistibute_ages <-function(data=cbh){
  # set to numeric
  data$age_of_death_number=as.numeric(data$age_of_death_number)

  data$child_age_at_death_months[as.character(data$age_of_death_units)=='Years' & !is.na(data$age_of_death_units)]=
    data$age_of_death_number[as.character(data$age_of_death_units)=='Years' & !is.na(data$age_of_death_units)]*12+6

  data$child_age_at_death_months[as.character(data$age_of_death_units)=='Days' & !is.na(data$age_of_death_units) ] =
    floor(data$age_of_death_number[as.character(data$age_of_death_units)=='Days' & !is.na(data$age_of_death_units) ]/30)

  data$child_age_at_death_months[as.character(data$age_of_death_units)=='Months' & !is.na(data$age_of_death_units) & is.na(data$child_age_at_death_months)] =
    data$age_of_death_number[as.character(data$age_of_death_units)=='Months' & !is.na(data$age_of_death_units) & is.na(data$child_age_at_death_months)]

  data$child_age_at_death_months[data$child_age_at_death_months<1&data$child_age_at_death_months>0&!is.na(data$child_age_at_death_months)]=0
  data$child_age_at_death_months[data$child_age_at_death_months>1&data$child_age_at_death_months<2&!is.na(data$child_age_at_death_months)]=1
  data$child_age_at_death_months[data$child_age_at_death_months>2&data$child_age_at_death_months<3&!is.na(data$child_age_at_death_months)]=2

  return(data)
}


###
apply_haidong_bias_ratios <- function(data=df){
  # load data
  hd <- <<<< FILEPATH REDACTED >>>>>
  hd <- subset(hd,!is.na(nid)) # adam filled in best he could, should be pretty much all there
  hd$nid = as.numeric(hd$nid)
  #categorize as cbh or sbh
  hd$data_type <- ""
  hd$data_type[grep(" indirect",hd$source1)]="sbh"
  hd$data_type[grep(" direct",hd$source1)]="cbh"

  # deal with years, shift for merging
  hd$year = hd$year+0.5

  # collapse rates to five year averages
  hd<-hd[,.(rat=mean(rat)),.(year=cut(year,c(1997.9,2002.9,2007.9,2012.9,2017.9),label=c(2000,2005,2010,2015)),data_type,nid)]
  hd$year = as.numeric(as.character(hd$year))

  # drop years not in the study period
  hd <- hd[!is.na(year),]

  # make 5-yearly estimates of 5q0 from each nid/dt/yr
  data[,age_bin:=as.numeric(age_bin)]
  mon <- data.table(m=c(1,11,24,24),age_bin=1:4)
  data<-merge(data,mon,by='age_bin')
  ages <- data[,.(N=sum(exposed,na.rm=T),died=sum(died,na.rm=T)),
                  by=.(nid,data_type,year,age_bin,m)]
  ages[,mr:=died/N]
  ages$m=ages$N=ages$died=NULL
  a1 <- ages[age_bin==1,]; setnames(a1,old='mr',new='mr1'); a1$age_bin=NULL
  a2 <- ages[age_bin==2,]; setnames(a2,old='mr',new='mr2'); a2$age_bin=NULL
  a3 <- ages[age_bin==3,]; setnames(a3,old='mr',new='mr3'); a3$age_bin=NULL
  a4 <- ages[age_bin==4,]; setnames(a4,old='mr',new='mr4'); a4$age_bin=NULL
  ages <- merge(a1,a2,by=c('nid','data_type','year'))
  ages <- merge(ages,a3,by=c('nid','data_type','year'))
  ages <- merge(ages,a4,by=c('nid','data_type','year'))
  ages[,q5r:=(1-(1-mr1)*(1-mr2)^11*(1-mr3)^24*(1-mr4)^24)]

  # merge with hd ratio
  d <- merge(ages,hd, by=c('nid','data_type','year'),all.x=TRUE)
  d$rat[is.na(d$rat)]=1 # make ratio = 1 where no haidong data (21%)

  # get the uniroot to solve for the ratio ( so we get a monthly ratio )
  d[,func := paste0('function(X) (1-(1-',mr1,'*X)*(1-',mr2,'*X)^11*(1-',mr3,'*X)^24*(1-',mr4,'*X)^24)-',q5r,'*',rat)]
  d[,mrat:=uniroot(eval(parse(text=func)),interval=c(0,2))$root]
  for(i in 1:nrow(d)) d$mrat[i] = uniroot(eval(parse(text=d$func[i])),interval=c(0,5))
  d$mrat = unlist(d$mrat)

  d$mrat[d$rat==1]=1 # precision fix
  d$mrat[d$mrat==0]=1 # one rogue obs

  d <- d[,c('nid','data_type','year','mrat')]
  setnames(d,old='mrat',new='rat')

  data = merge(data,d,by=c('nid','data_type','year'),all.x=TRUE)
  data$rat[is.na(data$rat)]=1

  # apply ratios to deaths
  data$died = data$died/data$exposed*data$rat*data$exposed
  return(data)
}

#
save_dated_input_data <- function(d     = data,
                                  indic = indicator,
                                  rd    = run_date){

  loc1 =paste0('<<<< FILEPATH REDACTED >>>>>/input_data/')
  loc2 =paste0('<<<< FILEPATH REDACTED >>>>>/input_data/dated/',rd,'/')
  dir.create(loc2)

  #fwrite(d,paste0(loc1,indic,'.csv'))
  #fwrite(d,paste0(loc2,indic,'.csv'))
  write.csv(d,paste0(loc1,indic,'.csv'))
  write.csv(d,paste0(loc2,indic,'.csv'))
}


merge_with_ihme_loc <-function(d,re=Regions){
  message(nrow(d))
  gaul_to_loc_id <- fread("<<<< FILEPATH REDACTED >>>>>/gaul_to_loc_id.csv")
  d <- d[, ihme_lc_id := as.character(country)]

  d$ihme_lc_id[d$ihme_lc_id=='Gambia']='GMB'
  d$ihme_lc_id[d$ihme_lc_id=='CotedIvoire']="CIV"
  d$ihme_lc_id[d$ihme_lc_id=='DR Congo']="COD"
  d$ihme_lc_id[d$ihme_lc_id=='Guinea-Bissau\"\"']='GNB'
  d$ihme_lc_id[d$ihme_lc_id=="BurkinaFaso"]=  'BFA'
  d$ihme_lc_id[d$ihme_lc_id=="DRCongo"]=      'COD'
  d$ihme_lc_id[d$ihme_lc_id=="Guinea Bissau"]='GNB'
  d$ihme_lc_id[d$ihme_lc_id=='Guinea-Bissau']='GNB'
  d$ihme_lc_id[d$ihme_lc_id=="SierraLeone"]=  'SLE'
  d$ihme_lc_id[d$ihme_lc_id=="Guinea-Bissau\""]='GNB'
  d$ihme_lc_id[d$ihme_lc_id=="Angola"]='AGO'
  d$ihme_lc_id[d$ihme_lc_id=="Burundi"]='BDI'
  d$ihme_lc_id[d$ihme_lc_id=="Central African Republic"]='CAF'
  d$ihme_lc_id[d$ihme_lc_id=="Burkina Faso"]='BFA'
  d$ihme_lc_id[d$ihme_lc_id=="Cameroon"]='CMR'
  d$ihme_lc_id[d$ihme_lc_id=="DR Congo"]='COD'
  d$ihme_lc_id[d$ihme_lc_id=="Congo"]='COG'
  d$ihme_lc_id[d$ihme_lc_id=="Comoros"]='COM'
  d$ihme_lc_id[d$ihme_lc_id=="Djibouti"]='DJI'
  d$ihme_lc_id[d$ihme_lc_id=="Egypt"]='EGY'
  d$ihme_lc_id[d$ihme_lc_id=="Ethiopia"]='ETH'
  d$ihme_lc_id[d$ihme_lc_id=="Ghana"]='GHA'
  d$ihme_lc_id[d$ihme_lc_id=="Gambia"]='GMB'
  d$ihme_lc_id[d$ihme_lc_id=="Equatorial Guinea"]='GNQ'
  d$ihme_lc_id[d$ihme_lc_id=="Kenya"]='KEN'
  d$ihme_lc_id[d$ihme_lc_id=="Liberia"]='LBR'
  d$ihme_lc_id[d$ihme_lc_id=="Lesotho"]='LSO'
  d$ihme_lc_id[d$ihme_lc_id=="Morocco"]='MOR'
  d$ihme_lc_id[d$ihme_lc_id=="Madagascar"]='MDG'
  d$ihme_lc_id[d$ihme_lc_id=="Mali"]='MLI'
  d$ihme_lc_id[d$ihme_lc_id=="Mozambique"]='MOZ'
  d$ihme_lc_id[d$ihme_lc_id=="Mauritania"]='MRT'
  d$ihme_lc_id[d$ihme_lc_id=="Malawi"]='MWI'
  d$ihme_lc_id[d$ihme_lc_id=="Niger"]='NER'
  d$ihme_lc_id[d$ihme_lc_id=="Nigeria"]='NGA'
  d$ihme_lc_id[d$ihme_lc_id=="Rwanda"]='RWA'
  d$ihme_lc_id[d$ihme_lc_id=="Sudan"]='SDN'
  d$ihme_lc_id[d$ihme_lc_id=="Senegal"]='SEN'
  d$ihme_lc_id[d$ihme_lc_id=="Sierra Leone"]='SLE'
  d$ihme_lc_id[d$ihme_lc_id=="South Sudan"]='SSD'
  d$ihme_lc_id[d$ihme_lc_id=="Sao Tome and Principe"]='STP'
  d$ihme_lc_id[d$ihme_lc_id=="Swaziland"]='SWZ'
  d$ihme_lc_id[d$ihme_lc_id=="Togo"]='TGO'
  d$ihme_lc_id[d$ihme_lc_id=="Tanzania"]='TZA'
  d$ihme_lc_id[d$ihme_lc_id=="Uganda"]='UGA'
  d$ihme_lc_id[d$ihme_lc_id=="South Africa"]='ZAF'
  d$ihme_lc_id[d$ihme_lc_id=="Zambia"]='ZMB'
  d$ihme_lc_id[d$ihme_lc_id=="Zimbabwe"]='ZWE'
  d$ihme_lc_id[d$ihme_lc_id=="Botswana"]='BWA'
  d$ihme_lc_id[d$ihme_lc_id=="Chad"]='TCD'
  d$ihme_lc_id[d$ihme_lc_id=="Benin"]='BEN'
  d$ihme_lc_id[d$ihme_lc_id=="Guinea"]='GIN'
  d$ihme_lc_id[d$ihme_lc_id=="Namibia"]='NAM'
  d$ihme_lc_id[d$ihme_lc_id=="Gabon"]='GAB'
  d$ihme_lc_id[d$ihme_lc_id=="Eritrea"]='ERI'
  d$ihme_lc_id[d$ihme_lc_id=="Somalia"]='SOM'

  d <- merge(d, gaul_to_loc_id, by='ihme_lc_id',all.x=T)

  message(nrow(d))
  for(r in re) {
    d <- d[GAUL_CODE %in% get_gaul_codes(r), region := r]
  }
  return(d)
}






# U5m post est to be done accross regions
u5mpostest <- function(reg=r,subnational_condsim=FALSE,other=''){


  message(reg)

  outputlist = list()
  .libPaths(package_lib)
  for(package in package_list)
    require(package, lib.loc = package_lib, character.only=TRUE)
  for(funk in list.files(recursive=TRUE,pattern='functions')){
      if(length(grep('central',funk))!=0|length(grep('u5m',funk))!=0)
          source(funk) #message(funk) #
    }
  # Combine cell_pred age groups in each region
  cell_pred=combine_agebins(reg=reg,u5m=TRUE,other=other)


  ## get aggregated estimates for all admin0. Aggregate to level you rake to
  message('loading data')
  simple_polygon <- load_simple_polygon(gaul_list = get_gaul_codes(reg), buffer = 1,returnsubsetshape=TRUE)
  subset_shape   <- simple_polygon[['subset_shape']]
  simple_polygon <- simple_polygon[['spoly_spdf']]

  raster_list    <<- build_simple_raster_pop(subset_shape,u5m=TRUE)
  simple_raster  <<- raster_list[['simple_raster']]
  pop_raster     <<- raster_list[['pop_raster']]
  pop_wts_adm0 <- make_population_weights(admin_level   = 0,
                                          simple_raster = simple_raster,
                                          pop_raster    = pop_raster,
                                          gaul_list     = get_gaul_codes(reg))

  # for both child and neonatal
  for(group in c('child','neonatal')){
    message(group)
    # draw level cond sim at national level
    message('National Cond Sim')
    cond_sim_draw_adm0 <- make_condSim(admin_level   = 0,
                                      pop_wts_object = pop_wts_adm0,
                                      cell_pred      = cell_pred[[sprintf('%s_all',group)]],
                                      gaul_list      = get_gaul_codes(reg),
                                      summarize      = FALSE)

    # save some raw country estimates to compare with GBD in a plot later on
    cond_sim_raw_adm0 <- apply(cond_sim_draw_adm0   , 1, mean)
    adm0_geo =cbind(mean=cond_sim_raw_adm0,
                    lower=apply(cond_sim_draw_adm0, 1, quantile, probs=.025),
                    upper=apply(cond_sim_draw_adm0, 1, quantile, probs=.975))
    outputlist[[sprintf('%s_%s_adm0_geo',reg,group)]] = data.table(split_geo_names(adm0_geo),adm0_geo)

    # get gbd estimates for raking
    gbd <- load_u5m_gbd(gaul_list = get_gaul_codes(reg), age_group = group, exact=FALSE)

    ## Get raking factors
    rf   <- calc_raking_factors(agg_geo_est = cond_sim_raw_adm0,
                                rake_to     = gbd)



    raked_cell_pred <- rake_predictions(raking_factors = rf,
                                        pop_wts_object = pop_wts_adm0,
                                        cell_pred      = cell_pred[[sprintf('%s_all',group)]])

    ## summarize raked predictions for each cell
    mean_unraked_raster <- make_cell_pred_summary( draw_level_cell_pred = cell_pred[[sprintf('%s_all',group)]],
                                                 mask                 = simple_raster,
                                                 return_as_raster     = TRUE,
                                                 summary_stat         = 'mean')

    mean_raked_raster <- make_cell_pred_summary( draw_level_cell_pred = raked_cell_pred,
                                                 mask                 = simple_raster,
                                                 return_as_raster     = TRUE,
                                                 summary_stat         = 'mean')

    range_raked_raster <- make_cell_pred_summary( draw_level_cell_pred = raked_cell_pred,
                                                 mask                 = simple_raster,
                                                 return_as_raster     = TRUE,
                                                 summary_stat         = 'cirange')

    lower_raked_raster <- make_cell_pred_summary( draw_level_cell_pred = raked_cell_pred,
                                                 mask                 = simple_raster,
                                                 return_as_raster     = TRUE,
                                                 summary_stat         = 'lower')
    upper_raked_raster <- make_cell_pred_summary( draw_level_cell_pred = raked_cell_pred,
                                                 mask                 = simple_raster,
                                                 return_as_raster     = TRUE,
                                                 summary_stat         = 'upper')

    if(subnational_condsim){
      for(ad in 1:2){
         pop_wts <- make_population_weights(admin_level   = ad,
                                            simple_raster = simple_raster,
                                            pop_raster    = pop_raster,
                                            gaul_list     = get_gaul_codes(reg))

         condsim <-    make_condSim(admin_level   = ad,
                                    pop_wts_object = pop_wts,
                                    cell_pred      = raked_cell_pred,
                                    gaul_list      = get_gaul_codes(reg),
                                    summarize      = FALSE)

         condsim2 <-    make_condSim(admin_level   = ad,
                                    pop_wts_object = pop_wts,
                                    cell_pred      = cell_pred[[sprintf('%s_all',group)]],
                                    gaul_list      = get_gaul_codes(reg),
                                    summarize      = FALSE)

        condsim <- data.table(cbind(split_geo_names(as.matrix(condsim)),mean=unname(condsim) ))
        condsim2 <- data.table(cbind(split_geo_names(as.matrix(condsim2)),mean=unname(condsim2) ))
        outputlist[[paste0('cond_sim_raked_adm',ad,'_',group,'_',reg)]]=condsim
        outputlist[[paste0('cond_sim_unraked_adm',ad,'_',group,'_',reg)]]=condsim2

        rm(condsim);rm(condsim2)

      }
    }
    outputlist[[sprintf('%s_mean_unraked_%s_raster',reg,group)]]=mean_unraked_raster
    outputlist[[sprintf('%s_mean_raked_%s_raster',reg,group)]]=mean_raked_raster
    outputlist[[sprintf('%s_range_raked_%s_raster',reg,group)]]=range_raked_raster
    outputlist[[sprintf('%s_lower_raked_%s_raster',reg,group)]]=lower_raked_raster
    outputlist[[sprintf('%s_upper_raked_%s_raster',reg,group)]]=upper_raked_raster
    save_post_est(raked_cell_pred, 'rdata',sprintf('%s_raked_%s_cell_pred',reg,group))
    save_post_est(rf, 'csv',sprintf('%s_%s_rf',reg,group))

    }
  return(outputlist)

}










#
launchAVpar <- function(i){

      message(sprintf('%i has launched',i))

      Sys.sleep(abs(rnorm(1,5,2)))

      x=   try(  aggregate_validation(holdoutlist           = stratum_qt,
                                cell_draws_filename   = '%s_cell_draws_eb_bin%i_%s_%i.RData',
                                years                 = c(2000,2005,2010,2015),
                                indicator_group       = indicator_group,
                                indicator             = indicator,
                                run_date              = run_date,
                                reg                   = as.character(loopvars[i,1]),
                                holdout               = loopvars[i,3],
                                addl_strat            = c('age'=loopvars[i,2]),
                                iter                  = i)  )

    if(inherits(x,'try-error')) message(x)

    message(sprintf('%i is finished as %s',i,paste0(class(x),collapse=' ')))

    return(x)
}
#



  make_summary_validation_table <- function(  agg_over    =  c('region','age','oos'),dr=as.numeric(samples) ,lv, weighted=TRUE, plot=TRUE){

    require(boot)

    # make data to work with
    d = subset(draws, ho_id!=0)
    d$mean_phat = apply(d[,paste0('phat_',1:dr),with=FALSE],1,mean)
    weighted.rmse <- function(error, w) sqrt( sum(  (w/sum(w)) * ((error)^2) ) )

    # choose to weight on SS or not.
    if(weighted==TRUE){
      d$weight = d$exposure
    } else {
      d$weight = 1
    }

    res<- data.table(cbind(d[,.(me          = weighted.mean(error,w=weight)),      by=agg_over],
                           mean_phat   = d[,.(mean_phat   = weighted.mean(mean_phat,w=weight)),  by=agg_over]$mean_phat,
                           rmse        = d[,.(rmse        = weighted.rmse(error,w=weight)),      by=agg_over]$rmse,
                           avg_exp     = d[,.(avg_exp     = median(exposure)),                   by=agg_over]$avg_exp,
                           cor         = d[,.(cor         = corr(cbind(p,mean_phat), w=weight )),by=agg_over]$cor,
                           coverage_95 = d[,.(coverage_95 = sum(clusters_covered_95*weight)/sum(total_clusters*weight)) ,  by=agg_over]$coverage_95))

    # plots
    if(plot==TRUE){
      pdf(paste0(sharedir,'/output/',run_date,'/oos_plots_',lv,'.pdf'))
        for(g in 1:4){
          tmp = subset(d,age==g)
          gg=ggplot(tmp,aes(y=p,x=mean_phat))+
            geom_abline(intercept=0,slope=1,colour='red')+
            geom_point(size=1)+
            theme_bw()+
            ylab('Data Estimate') +
            xlab('Out of Sample Mean Prediction')+
            facet_wrap(~year,ncol=2)+
            theme(strip.background = element_rect(fill="white"))+
            ggtitle(paste0('Age bin ',g))
          plot(gg)
        }
        dev.off()
    }

    return(res)
  }










#
## ~~~~~~~~~~~ make table of model results ~~~~~~~~~~~~~~~~~
model_fit_table <- function(lv=loopvars[loopvars[,3]==0,],rd=run_date,nullmodel=''){
  # load models
  require(INLA)
  message(sprintf('Pulling together table for %s models',rd))
  tlist=list()
  for(i in 1:nrow(lv)){
    message(sprintf('%i of %i loopvars. %s %i',i,nrow(lv),lv[i,1],lv[i,2]))
    f=sprintf('%s/output/%s/died_model_eb_bin%i_%s_0%sNA.RData',sharedir,rd,lv[i,2],lv[i,1],nullmodel)
    if(!file.exists(f)){
      message('FAILED TO OPEN')
    } else {
      load(f)
      tlist[[sprintf('%s_%i',lv[i,1],lv[i,2])]]=rbind(summary(res_fit)$fixed[,1:6],summary(res_fit)$hyperpar)
    }
  }
  return(tlist)
}

# readable smoothness and variance from the model fit table
tr.params<-function(params.list){

  range.res<- var.res<-rho.res<-matrix(ncol=4, nrow=4)
  colnames(range.res)<-colnames(var.res) <- colnames(rho.res) <-c("cessa", "wssa", "name", "sssa")
  rownames(range.res)<-rownames(var.res) <- rownames(rho.res)<-as.character(1:4)

  for(i in 1:length(params.list)){
    res.name=names(params.list)[i]
    c <- which(colnames(range.res)==strsplit(res.name, "_")[[1]][1])
    r <- which(rownames(range.res)==strsplit(res.name, "_")[[1]][2])

    params.mat=params.list[[i]]

    nr=nrow(params.mat)
    tau=exp(params.mat[(nr-2),1])
    kappa=exp(params.mat[(nr-1),1])
    range=sqrt(8)/kappa
    nom.var=1/(4*pi*kappa^2*tau^2)

    range.res[r, c]=range
    var.res[r, c]=nom.var
    rho.res[r,c]=params.mat[nr,1]

  }

   return(list("range"=range.res,
               "nominal.variance"=var.res,
               "rho"=rho.res))
}


# ~~~ load training data

load_train_data <- function(rd=run_date){
  require(gtools)

  fs <- list.files(path=<<<< FILEPATH REDACTED >>>>>,pattern='died_training')
  d  <- data.table()

  for(f in fs)
    if(length(grep('_0NA',f))>0)
      d <- smartbind(d,fread(<<<< FILEPATH REDACTED >>>>>))
  d <- data.table(d)

  return(d)
}


#rewrite save_mbg_input so its less annoying
save_mbg_input_new <- function(indicator = indicator, indicator_group = indicator_group, df = df , simple_raster = simple_raster, mesh_s = mesh_s,
                           mesh_t = mesh_t, cov_list = all_cov_layers, run_date = NULL, pathaddin="",
                           child_model_names = NULL, all_fixed_effects = NULL) {

    df = cbind(df, extract_covariates(df, cov_list, return_only_results = T))

    #create a period column
    year_map = as.data.frame(sort(unique(df[,year])))
    year_map$period = 1:nrow(year_map)
    names(year_map)[1] = 'year'
    df = merge(df, year_map, by = 'year', sort =F)

    if(child_model_names!='NONE'){
        to_save <- c("df", "simple_raster", "mesh_s", "mesh_t", 'cov_list', 'child_model_names', 'all_fixed_effects')
    } else {
        to_save <- c("df", "simple_raster", "mesh_s", "mesh_t", 'cov_list', 'all_fixed_effects')

    }

    save(list = to_save, file = <<<< FILEPATH REDACTED >>>>>)
    message(paste0('All inputs saved to <<<< FILEPATH REDACTED >>>>>'))

}



# setup validation bits to run
setup_fold_u5m <- function( spat_strat,  # qt or poly
                            data = df,
                            n_folds = 5,
                            admin_level=2, # 1 or 2 (only if poly)
                            ss_col   = 'exp',
                            yr_col = 'year'  ,
                            temp_strat = 'prop',
                            long_col = 'longitude',
                            lat_col = 'latitude',
                            strat_cols=c('region','age')) {


  if(spat_strat=='poly'){
    message(sprintf('spat_strat set to %s with admin level %i chosen',spat_strat,admin_level))

    message('\n Loading in necessary spatial layers')
      admin_shps   = sprintf('<<<< FILEPATH REDACTED >>>>>/ad%i_raster.grd',admin_level)
      shape_ident  = "gaul_code"
      admin_raster = sprintf('<<<< FILEPATH REDACTED >>>>>/africa_ad%i.shp',admin_level)
      mask_shape   = '<<<< FILEPATH REDACTED >>>>>/africa_simple.shp'
      mask_raster  = '<<<< FILEPATH REDACTED >>>>>/ad0_raster'
      prefix=paste0('poly_admin',admin_level)

    message('\n Running make folds, this takes a while.')
      stratum_qt <- make_folds(data       = data,
                               n_folds    = n_folds,
                               spat_strat = spat_strat,
                               temp_strat = temp_strat,
                               strat_cols = strat_cols,
                               admin_shps = admin_shps,
                               admin_raster = admin_raster,
                               mask_shape = mask_shape,
                               mask_raster = mask_raster,
                               seed       = 123445)



  }

  if(spat_strat=='qt'){
    message(sprintf('spat_strat set to %s',spat_strat))

    message('assigning ts to all strat combos. Auto setting to 200 for age 1 and 600 for the rest')
      all_strat <- get_strat_combos(data=as.data.frame(data), strat_cols=strat_cols)
      ts_vec = c(rep(200, 4), rep(600, 12)) ## make a vector of ts per strata
      prefix='qt'

    message('\n Running make folds, this takes a while.')
      stratum_qt <- make_folds(data       = data,
                             n_folds    = n_folds,
                             spat_strat = spat_strat,
                             temp_strat = temp_strat,
                             strat_cols = strat_cols,
                             ts_vec     = ts_vec,
                             mb         = 1,
                             seed       = 123445)


  }
  if(spat_strat=='rand'){
    message(sprintf('spat_strat set to %s',spat_strat))


    all_strat <- get_strat_combos(data=as.data.frame(data), strat_cols=strat_cols)
    #ts_vec = c(rep(200, 4), rep(600, 12)) ## make a vector of ts per strata
      prefix='rand'

    message('\n Running make folds, this takes a while.')
      stratum_qt <- make_folds(data       = data,
                             n_folds    = n_folds,
                             spat_strat = spat_strat,
                             temp_strat = temp_strat,
                             strat_cols = strat_cols,
                             seed       = 123445)


  }

  save(stratum_qt,file=<<<< FILEPATH REDACTED >>>>>)
  #rename_aarons_shapefiles(prefix = prefix)

  return(stratum_qt)
}




make_adm_summary_tables <- function(run_date,getdraws=FALSE,sr=NULL){

  if(getdraws==TRUE & is.null(sr)) stop('Need simple raster if want to return draws')

  # bring in admin data
  a1shp <- shapefile(paste0("<<<< FILEPATH REDACTED >>>>>/admin", 1, "/g2015_2014_", 1, "/g2015_2014_", 1, ".shp"))
  a2shp <- shapefile(paste0("<<<< FILEPATH REDACTED >>>>>/admin", 2, "/g2015_2014_", 2, "/g2015_2014_", 2, ".shp"))
  a1info <- data.table(unique(a1shp@data))[,c('ADM0_NAME','ADM1_NAME','ADM1_CODE'),with=FALSE]
  a2info <- data.table(unique(a2shp@data))[,c('ADM0_NAME','ADM1_NAME','ADM2_NAME','ADM2_CODE'),with=FALSE]


  for(group in c('neonatal','child')){
    message(group)
    # bring in saved adm results
    ad1 <- fread(<<<< FILEPATH REDACTED >>>>>)
    ad2 <- fread(<<<< FILEPATH REDACTED >>>>>)

    # clean them up a bit
    ad1$name = as.numeric(ad1$name); ad2$name = as.numeric(ad2$name)
    ad1 <- merge(ad1,a1info,by.x='name',by.y='ADM1_CODE',all.x=T)
    ad2 <- merge(ad2,a2info,by.x='name',by.y='ADM2_CODE',all.x=T)

    ad1$V1 <- ad2$V1 <- NULL
    names(ad1)[names(ad1) == "name"] = "gaul"
    names(ad2)[names(ad2) == "name"] = "gaul"

    draws1<-ad1[, lapply(.SD,as.numeric), .SDcols = grep("V", names(ad1))]
    draws2<-ad2[, lapply(.SD,as.numeric), .SDcols = grep("V", names(ad2))]

    summarize <- function(draws)
      res<-data.table(mean=apply(draws,1,mean),t(apply(draws,1,quantile,probs=c(.025,.975))))

    # clean up draws table to ready it for rbinding later
    if(getdraws==TRUE){
      i=1
      for(n in names(ad1) ){
        if(grepl('V',n)){
          setnames(ad1,n,paste0('draw',i))
          setnames(ad2,n,paste0('draw',i))
          i=i+1
        }
      }
      setcolorder(ad1, c( "ADM0_NAME", "ADM1_NAME", "gaul", "year" , paste0('draw',1:(i-1))))
      setcolorder(ad2, c( "ADM0_NAME", "ADM1_NAME", "ADM2_NAME", "gaul", "year" , paste0('draw',1:(i-1))))
    } else {
      ad1 = data.table(ad1[,c('ADM0_NAME','ADM1_NAME','gaul','year')],summarize(draws1))
      ad2 = data.table(ad2[,c('ADM0_NAME','ADM1_NAME','ADM2_NAME','gaul','year')],summarize(draws2))

      ad1 <- ad1[order(ADM0_NAME,ADM1_NAME)]
      ad2 <- ad2[order(ADM0_NAME,ADM1_NAME,ADM2_NAME)]
    }

    ## small districts got dropped, use their centroids to fill in the gaps
    # search for those missed
    a2missing <- a2info[ADM0_NAME%in%unique(ad2$ADM0_NAME) & (!ADM2_CODE%in%unique(ad2$gaul))]
    a1missing <- a1info[ADM0_NAME%in%unique(ad1$ADM0_NAME) & (!ADM1_CODE%in%unique(ad1$gaul))]

    # bring in results rasters to be used to fill in gaps later
    for(measure in c('mean','upper','lower'))
      assign(sprintf('r_%s',measure),brick(sprintf('%s/output/%s/died_%s_%s_raked_stack.tif',sharedir,run_date,measure,group)))

    # If I am going to want draws, need full cell pred files
    if(getdraws == TRUE){
      message('Loading Cell Pred File, Takes a while')
      load(<<<< FILEPATH REDACTED >>>>>)
      cp <-x
    }

    # get centroids of ads that were missing
    fillgaps <- function(gcode,shp,missing,ids,gd) {
      require(rgeos); require(stringr)
      a2misspoly <- merge(shp,missing,by=c(ids,gcode),all.x=FALSE)
      a2misscent <- gCentroid(a2misspoly,byid=TRUE)

      if(gd == FALSE){
        stop('NOT SET UP TO FILL ALL GAPS ON NOT DRAW LEVEL YET, SET ARG getdraws=TRUE')

        mean   <- data.table(extract(r_mean,a2misscent));  names(mean)  <- c('mean2000','mean2005','mean2010','mean2015')
        lower  <- data.table(extract(r_lower,a2misscent)); names(lower) <- c('lower2000','lower2005','lower2010','lower2015')
        upper  <- data.table(extract(r_upper,a2misscent)); names(upper) <- c('upper2000','upper2005','upper2010','upper2015')
        a2missdat <- data.table(cbind(extract(a2misspoly,a2misscent),mean,lower,upper))
        m1<-melt(a2missdat, id.vars = c(ids,gcode),
                        measure.vars = c('mean2000','mean2005','mean2010','mean2015'))
        setnames(m1,"value","mean");  setnames(m1,"variable","year"); m1[,year:=as.numeric(str_sub(year, start= -4))]
        m2<-melt(a2missdat, id.vars = c(ids,gcode),
                        measure.vars = c('lower2000','lower2005','lower2010','lower2015'))
        setnames(m2,"value","2.5%");  setnames(m2,"variable","year"); m2[,year:=as.numeric(str_sub(year, start= -4))]
        m3<-melt(a2missdat, id.vars = c(ids,gcode),
                        measure.vars = c('upper2000', 'upper2005','upper2010','upper2015'))
        setnames(m3,"value","97.5%");  setnames(m3,"variable","year"); m3[,year:=as.numeric(str_sub(year, start= -4))]
        m1<-na.omit(m1);    m2<-na.omit(m2);    m3<-na.omit(m3)

        m <- merge(m1,m2,by=c(ids,gcode,'year'),all=T)
        m <- merge(m, m3,by=c(ids,gcode,'year'),all=T)
        setnames(m,gcode,"gaul")


      } else { # if want draws, need to work the cell pred files

        message('Extracting draw rasters, takes some time')
        m <- data.table(a2misspoly@data)[, c(ids,gcode),with=FALSE]
        m <- m[rep(seq_len(nrow(m)), length=(nrow(m)*4)),]
        m$year <- rep(c(2000,2005,2010,2015),each=(nrow(m)/4))
        pb <- txtProgressBar(style = 3)
        for(d in 1:ncol(cp)){
          setTxtProgressBar(pb, d / ncol(cp))
          r <- insertRaster(sr, matrix(cp[,d],ncol = 4))

          # 1) First attempt: use centroid
          amissdat <- data.table(cbind(extract(a2misspoly,a2misscent),extract(r , a2misscent)))

          # 2) some may be missing still at centroid, try to get at least one pixel
          miss2Idx <- which(is.na(amissdat$layer.1))
          stillmiss <- data.table(na.omit(a2misspoly@data[miss2Idx,]))[,c(ids,gcode),with=FALSE]
          amissdat2 <- extract(r , merge(shp,stillmiss,by=c(ids,gcode),all.x=FALSE))
          for(i in 1:length(amissdat2)){
            if(!all(is.na(amissdat2[[i]]))){
              amissdat2[[i]]=amissdat2[[i]][min(which(!is.na(amissdat2[[i]][,1]))),]
            } else {
              amissdat2[[i]]=amissdat2[[i]][1,]
            }
            amissdat$layer.1[miss2Idx[i]]=amissdat2[[i]][1]
            amissdat$layer.2[miss2Idx[i]]=amissdat2[[i]][2]
            amissdat$layer.3[miss2Idx[i]]=amissdat2[[i]][3]
            amissdat$layer.4[miss2Idx[i]]=amissdat2[[i]][4]
          }

          amissdat<-melt(amissdat, id.vars = c(ids,gcode),
                                   measure.vars = c('layer.1','layer.2','layer.3','layer.4'))
          m[[paste0('draw',d)]] = amissdat$value

        }
        close(pb)
        m<-na.omit(m);
        setnames(m,gcode,"gaul")
      }

    return(m)
    }

    ad2 <-rbind(ad2,fillgaps(ids=c('ADM0_NAME','ADM1_NAME','ADM2_NAME'),gcode='ADM2_CODE',shp=a2shp,missing=a2missing,gd=getdraws))
    # NONE IN ADM1 ad1 <-rbind(ad1,fillgaps(ids=c('ADM0_NAME','ADM1_NAME'),gcode='ADM1_CODE',shp=a1shp,missing=a1missing,gd=getdraws))

    # sort some final names
    for(x in c('ad2','ad1')){
      xx <- get(x)
      xx$ADM1_NAME[xx$ADM0_NAME=='Abyei']='Abyei'
      xx$ADM1_NAME[xx$ADM0_NAME=="Hala'ib triangle"]="Hala'ib triangle"
      xx$ADM1_NAME[xx$ADM0_NAME=="Ilemi triangle"]="Ilemi triangle"
      xx$ADM1_NAME[xx$ADM0_NAME=="Ma'tan al-Sarra"]="Ma'tan al-Sarra"
      xx$ADM0_NAME[xx$ADM1_NAME=='Abyei']='Sudan'
      xx$ADM0_NAME[xx$ADM1_NAME=="Hala'ib triangle"]='Egypt'
      xx$ADM0_NAME[xx$ADM1_NAME=="Ilemi triangle"]='Kenya'
      xx$ADM0_NAME[xx$ADM1_NAME=="Ma'tan al-Sarra"]='Libya'
      xx <- subset(xx,!ADM0_NAME %in% c('Algeria','Cape Verde','Comoros','Sao Tome and Principe','Libya','Tunisia'))
      assign(x,xx)
    }

    # order before saving
    ad1 <- ad1[order(ADM0_NAME,ADM1_NAME)]
    ad2 <- ad2[order(ADM0_NAME,ADM1_NAME,ADM2_NAME)]

    if(getdraws==TRUE){
      save_post_est(ad1,'csv',sprintf('%s_admin1_draws',group))
      save_post_est(ad2,'csv',sprintf('%s_admin2_draws',group))
    } else {
      save_post_est(ad1,'csv',sprintf('%s_admin1_summary_table',group))
      save_post_est(ad2,'csv',sprintf('%s_admin2_summary_table',group))
    }
  }
}










# this is me beign lazy
countrytoiso <- function(md){
          #  deal with mix of country name and ISO
          md$country[md$country=="BurkinaFaso"]=  'BFA'
          md$country[md$country=="Burkina Faso"]=  'BFA'
          md$country[md$country=="DRCongo"]=      'COD'
          md$country[md$country=="Guinea Bissau"]='GNB'
          md$country[md$country=='Guinea-Bissau']='GNB'
          md$country[md$country=="SierraLeone"]=  'SLE'
          md$country[md$country=="Guinea-Bissau\""]='GNB'
          md$country[md$country=="Angola"]='AGO'
          md$country[md$country=="Burundi"]='BDI'
          md$country[md$country=="Central African Republic"]='CAF'
          md$country[md$country=="Burkina Faso"]='BFA'
          md$country[md$country=="Cameroon"]='CMR'
          md$country[md$country=="DR Congo"]='COD'
          md$country[md$country=="Congo"]='COG'
          md$country[md$country=="Comoros"]='COM'
          md$country[md$country=="Djibouti"]='DJI'
          md$country[md$country=="Egypt"]='EGY'
          md$country[md$country=="Ethiopia"]='ETH'
          md$country[md$country=="Ghana"]='GHA'
          md$country[md$country=="Gambia"]='GMB'
          md$country[md$country=="Equatorial Guinea"]='GNQ'
          md$country[md$country=="Kenya"]='KEN'
          md$country[md$country=="Liberia"]='LBR'
          md$country[md$country=="Lesotho"]='LSO'
          md$country[md$country=="Morocco"]='MOR'
          md$country[md$country=="Madagascar"]='MDG'
          md$country[md$country=="Mali"]='MLI'
          md$country[md$country=="Mozambique"]='MOZ'
          md$country[md$country=="Mauritania"]='MRT'
          md$country[md$country=="Malawi"]='MWI'
          md$country[md$country=="Niger"]='NER'
          md$country[md$country=="Nigeria"]='NGA'
          md$country[md$country=="Rwanda"]='RWA'
          md$country[md$country=="Sudan"]='SDN'
          md$country[md$country=="Senegal"]='SEN'
          md$country[md$country=="Sierra Leone"]='SLE'
          md$country[md$country=="South Sudan"]='SSD'
          md$country[md$country=="Sao Tome and Principe"]='STP'
          md$country[md$country=="Swaziland"]='SWZ'
          md$country[md$country=="Togo"]='TGO'
          md$country[md$country=="Tanzania"]='TZA'
          md$country[md$country=="Uganda"]='UGA'
          md$country[md$country=="South Africa"]='ZAF'
          md$country[md$country=="Zambia"]='ZMB'
          md$country[md$country=="Zimbabwe"]='ZWE'
          md$country[md$country=="Botswana"]='BWA'
          md$country[md$country=="Chad"]='TCD'
          md$country[md$country=="Somalia"]='SOM'
          md$country[md$country=="Guinea"]='GIN'
          md$country[md$country=="Benin"]='BEN'
          md$country[md$country=="CotedIvoire"]='CIV'
          md$country[md$country=="Namibia"]='NAM'
          md$country[md$country=="Eritrea"]='ERI'
          md$country[md$country=="Yemen"]='YEM'
          md$country[md$country=="Gabon"]='GAB'
          md$country[md$country=="MOR"]='MAR'

return(md)
}



prep_data_for_shiny=function(data=df){
  md <- copy(df)
  md <- subset(md,!nid%in%c(31142,133731,91508,218035,203663,203654,203664,256243, 256201, 256365,227133)) # Surveys that drop later

  md <- md[, point := as.numeric(!is.na(latitude) & !is.na(longitude))]
  md <- md[, died  := died/N]
  md <- md[, N     := N*sbh_wgt*weight]
  md[,age_bin := as.numeric(age)]
  md <- md[, svy_id     := nid]

  # make an age bin 5 just for the plot
  md[,s:=(1-died)^m]
  md5 <- md[,.(died=1-prod(s),N=sum(N),age_bin=5,sbh_wgt=prod(sbh_wgt),m=60,s=prod(s)),
              by=.(year,cluster_id,longitude,latitude,shapefile,location_code,nid,source,country,data_type,point)]
  md[,died:=1-(1-died)^m]
  md <- data.table(smartbind(md,md5))
  md$shapefile = NA
  md$location_code = NA

  require(stringr)
  return(md)
}




save_data_for_shiny <- function(df,
                                      var,
                                      year_var,
                                      indicator) {


    # 1. Pull in country table ----------------------------------------------

    # Pull in list of regions from GBD
    country_table <- data.table(get_location_hierarchy(41))[, .(ihme_loc_id,
                                                                level,
                                                                location_name,
                                                                location_name_short,
                                                                region_name)]

    # For now, subset to countries only
    # Note: could change this for subnationals if desired!
    # May be helpful if looking at a country for which we have tons of data (e.g. India)

    country_table <- country_table[level == 3]

    # 2. Prep input data table for merge -------------------------------------

    df <- as.data.table(df)  # in case passed a data frame

    # Check if latitude and longitude are numeric, if not make the user fix themselves rather than dropping stuff in the conversion
    if(!is.numeric(df$latitude) | !is.numeric(df$longitude)) {
      stop("Latitude and longitude not both numeric, please fix.")
    }

    # Rename the year variable to something standard
    setnames(df, year_var, "year_var")

    # Recode NA attribution for shapefile
    df[shapefile == "", shapefile := NA]

    # Generate pointpoly variable
    df[!is.na(latitude), pointpoly := "Point"]
    df[!is.na(shapefile), pointpoly := "Polygon"]
    df <- df[!is.na(pointpoly)]

    # 3. Merge on country identifiers ---------------------------------------

    setnames(country_table, "ihme_loc_id", "country")

    # Merge in country table
    df <- merge(df, country_table, by = "country", all = T)

    # Drop non-matched countries & notify user
    message(paste0("\nDropping ", nrow(df[is.na(country)]),
                   " rows without matches in GBD country table."))
    message(paste0("Countries affected: ", unique(df[is.na(country)]$location_name)))
    df <- df[!is.na(country)]

    # 4. Subset data for analysis -------------------------------------------

    # Split off a point & polygon data set for use later
    df[, outcome := get(var)]
    df_point <- df[pointpoly == "Point"]
    df_poly  <- df[pointpoly == "Polygon"]


    setnames(df, var, indicator)
    write.csv(df, <<<< FILEPATH REDACTED >>>>>)

}





# include weights in seegMBG::periodTabulate()
periodTabulate_w=function (age_death, birth_int, cluster_id, pw=NULL,windows_lower = c(0,
    1, 3, 6, 12, 24, 36, 48), windows_upper = c(0, 2, 5, 11,
    23, 35, 47, 59), nperiod = 1, period = 60, period_end = NULL,
    interview_dates = NULL, method = c("monthly", "direct"),
    cohorts = c("one", "three"), inclusion = c("enter", "exit",
        "both", "either"), mortality = c("bin", "monthly"), delay = NULL,
    verbose = TRUE, n_cores = 1)
{
    if (is.null(period_end))
        interview_dates <- rep(NA, length(age_death))
    if (n_cores > 1) {
        message(sprintf("running periodTabulate on %s cores",
            n_cores))
        stopifnot(length(birth_int) == length(age_death))
        stopifnot(length(cluster_id) == length(age_death))
        clusters <- unique(cluster_id)
        indices <- parallel::splitIndices(length(clusters), n_cores)
        data_all <- data.frame(age_death, birth_int, cluster_id,
            interview_dates)
        data_chunks <- lapply(indices, function(i, dat, clusters) {
            cluster_group <- clusters[i]
            idx <- which(dat$cluster_id %in% cluster_group)
            dat[idx, ]
        }, data_all, clusters)
        parfun <- function(dat, ...) {
            periodTabulate(age_death = dat$age_death, birth_int = dat$birth_int,
                cluster_id = dat$cluster_id, interview_dates = dat$interview_dates,
                ...)
        }
        on.exit(sfStop())
        sfInit(parallel = TRUE, cpus = n_cores)
        sfLibrary("seegMBG", character.only = TRUE)
        ans_list <- sfLapply(data_chunks, parfun, windows_lower = windows_lower,
            windows_upper = windows_upper, nperiod = nperiod,
            period = period, period_end = period_end, method = method,
            cohorts = cohorts, inclusion = inclusion, mortality = mortality,
            delay = delay, verbose = verbose, n_cores = 1)
        ans <- do.call(rbind, ans_list)
        return(ans)
    }
    method <- match.arg(method)
    cohorts <- match.arg(cohorts)
    inclusion <- match.arg(inclusion)
    mortality <- match.arg(mortality)
    if (is.null(delay)) {
        delay <- switch(cohorts, one = 0, three = max(windows_upper -
            windows_lower))
    }
    if (!is.null(period_end)) {
        if (is.null(interview_dates) || (class(interview_dates) !=
            "Date")) {
            stop("if period_end is being used, interview_dates must also be specified, as a vector of class Date")
        }
        if (length(period_end) != 1 | class(period_end) != "Date") {
            stop("period_end must be a Date object of length one")
        }
    }
    cohort_names <- switch(cohorts, one = "B", three = c("A",
        "B", "C"))
    clusters <- unique(cluster_id)
    n <- length(age_death)
    nw <- length(windows_lower)
    np <- nperiod
    ncl <- length(clusters)
    periods <- rep(period, n)
    delays <- rep(delay, n)
    stopifnot(length(period) == 1)
    stopifnot(length(delay) == 1)
    stopifnot(length(periods) == n)
    stopifnot(length(delays) == n)
    stopifnot(length(birth_int) == n)
    stopifnot(length(cluster_id) == n)
    stopifnot(length(windows_upper) == nw)
    if (any(windows_upper[-1] <= windows_lower[-nw])) {
        stop("windows_upper and windows_lower appear to overlap")
    }
    ans <- data.frame(cluster_id = rep(clusters, each = nw *
        np), exposed = rep(NA, ncl * nw * np), died = 0, period = rep(rep(1:np,
        each = nw), ncl), age_bin = rep(1:nw, ncl * np))
    for (p in 1:np) {
        if (verbose & np > 1)
            message(paste("\nprocessing period", p))
        if (method == "monthly") {
            for (w in 1:nw) {
                new_windows <- seq(windows_lower[w], windows_upper[w],
                  by = 1)
                n_nw <- length(new_windows)
                res_tmp <- periodTabulate_w(pw=pw,age_death = age_death,
                  birth_int = birth_int, cluster_id = cluster_id,
                  windows_lower = new_windows, windows_upper = new_windows,
                  nperiod = 1, period = period, period_end = period_end,
                  interview_dates = interview_dates, method = "direct",
                  cohorts = cohorts, inclusion = inclusion, mortality = mortality,
                  delay = delay + period * (p - 1), verbose = verbose)
                exposed_mnth <- tapply(res_tmp$exposed, res_tmp$cluster_id,
                  sum)
                died_mnth <- tapply(res_tmp$died, res_tmp$cluster_id,
                  sum)
                if (mortality == "bin") {
                  exposed_per <- exposed_mnth/n_nw
                  rate <- 1 - (1 - (died_mnth/(exposed_mnth)))^n_nw
                  died_per <- rate * exposed_per
                  exposed_per[exposed_mnth == 0] <- 0
                  died_per[exposed_mnth == 0] <- 0
                  exposed_res <- exposed_per
                  died_res <- died_per
                }
                else {
                  exposed_res <- exposed_mnth
                  died_res <- died_mnth
                }
                idx_insert <- which(ans$period == p & ans$age_bin ==
                  w)
                ans$exposed[idx_insert] <- exposed_res
                ans$died[idx_insert] <- died_res
            }
        }
        else if (method == "direct") {
            if (!is.null(period_end)) {
                period_end <- Date2cmc(period_end)
                interview_date <- Date2cmc(interview_dates)
                delays <- delays + interview_date - period_end
            }
            upper_mat <- t(expand(windows_upper, n))
            lower_mat <- t(expand(windows_lower, n))
            age_range_mat <- upper_mat - lower_mat + 1
            age_death <- expand(age_death, nw)
            birth_int <- expand(birth_int, nw)
            delay_mat <- expand(delays, nw)
            period_mat <- expand(periods, nw)
            extra_delay_mat <- period_mat * (p - 1)
            deaths <- exposed <- 0
            for (cohort in cohort_names) {
                if (verbose & length(cohort_names) > 1) {
                  message(paste("processing cohort", cohort))
                }
                start_offset <- switch(inclusion, enter = -age_range_mat,
                  exit = 0, both = 0, either = -age_range_mat)
                end_offset <- switch(inclusion, enter = 0, exit = age_range_mat,
                  both = 0, either = age_range_mat)
                start_mat <- switch(cohort, B = upper_mat, A = period_mat +
                  lower_mat, C = lower_mat)
                end_mat <- switch(cohort, B = period_mat + lower_mat,
                  A = period_mat + upper_mat, C = upper_mat)
                start_mat <- start_mat + delay_mat + extra_delay_mat
                end_mat <- end_mat + delay_mat + extra_delay_mat
                start_mat <- start_mat + start_offset
                end_mat <- end_mat + end_offset
                trunc_mat <- start_mat - delay_mat
                exposed_cohort <- (birth_int < end_mat & birth_int >=
                  start_mat & age_death >= lower_mat & birth_int >=
                  trunc_mat)
                deaths_cohort <- (exposed_cohort > 0 & age_death <=
                  upper_mat)
                weight <- ifelse(cohort == "B", 1, 0.5)
                exposed <- exposed + exposed_cohort * weight
                deaths <- deaths + deaths_cohort * weight
            }
            stopifnot(all(exposed <= 2))
            stopifnot(all(deaths <= 2))
      # incorporate pweights for deaths count
      if(!is.null(pw)){
        we=exposed*pw
        de=deaths*pw
        exposed_wgt_agg <- aggMatrix(we, cluster_id)
        deaths_wgt_agg  <- aggMatrix(de,  cluster_id)

        weighted_q <- deaths_wgt_agg/exposed_wgt_agg

        exposed_agg <- aggMatrix(exposed, cluster_id)
        deaths_agg <- exposed_agg*weighted_q
        deaths_agg[is.na(deaths_agg)]=0
      } else {
        exposed_agg <- aggMatrix(exposed, cluster_id)
        deaths_agg  <- aggMatrix(deaths,  cluster_id)
      }
            ans$exposed[ans$period == p] <- as.vector(t(exposed_agg))
            ans$died[ans$period == p] <- as.vector(t(deaths_agg))
        }
    }
    return(ans)
}
