# function file

## Make time stamp in standardized format.
make_time_stamp <- function(time_stamp) {
  
  run_date <- gsub("-","_",Sys.time())
  run_date <- gsub(":","_",run_date)
  run_date <- gsub(" ","_",run_date)
  
  if(time_stamp==FALSE) run_date <- 'scratch'
  
  return(run_date)
  
}

firstDay <- function (year, month) {
  # given a year and month, return a Date object of the first day of that month
  date_string <- paste(year,
                       month,
                       '01',
                       sep = '-')
  
  date <- as.Date (date_string)
  
  return (date)
  
}

lastDay <- function (year, month) {
  # given a year and month, return a Aate object of the last day of that month
  next_month <- ifelse(month == 12,
                       1,
                       month + 1)
  
  next_year <- ifelse(month == 12,
                      year + 1,
                      year)
  
  next_date_string <- paste(next_year,
                            next_month,
                            '01',
                            sep = '-')
  next_date <- as.Date(next_date_string)
  date <- next_date - 1
  
  return (date)
}

sentenceCase <- function (text) {
  # given a vector of text strings `text`, convert to sentence case
  
  # convert all to lower case
  text <- tolower(text)
  
  # split at spaces
  text_list <- strsplit(text,
                        ' ')
  
  text_list <- lapply(text_list,
                      function(x) {
                        x[1] <- paste(toupper(substring(x[1], 1, 1)),
                                      substring(x[1], 2),
                                      sep = "")
                        x <- paste(x, collapse = ' ')
                        return(x)
                      })
  
  text_vector <- unlist(text_list)
  
  return (text_vector)
  
}

# define functions
firstTwo <- function (text) {
  # given a vector of text strings `text` subset each to only the first two
  # words (bits separated by spaces) and return this as a vector.
  text <- as.character(text)
  text_list <- strsplit(text, ' ')
  text_list <- lapply(text_list, '[', 1:2)
  text_list <- lapply(text_list, paste, collapse = ' ')
  text_vector <- unlist(text_list)
  return (text_vector)
}

rasterizeSpecies <- function(species,
                             shape,
                             raster,
                             buffer = NULL,
                             folder = 'bats/range_buff/') {
  
  # first buffer, then rasterize IUCN range map for species
  shp <- shape[shape$BINOMIAL == species, ]
  
  if (!is.null(buffer)) {
    # convert buffer from kilometres to decimal degrees, assuming at the equator
    buffer <- buffer / 111.32
    
    # buffer by this amount
    shp <- gBuffer(shp,
                   width = buffer)
  }
  
  # rasterize the shapefile 
  tmp <- rasterize(shp,
                   raster,
                   field = 1,
                   background = 0,
                   fun = 'first')
  
  writeRaster(tmp,
              filename = paste0('~/Z/zhi/ebola/',
                                folder,
                                '/',
                                gsub(' ', '_', species)),
              format = 'GTiff',
              overwrite = TRUE)
  
  rm(tmp)
  
  return (NULL)
}

tidySpecies <- function (filename, template) {
  # load a raster if it contains any of the species' range,
  # mask and resave it, else delete it
  tmp <- raster(filename)
  if (!is.na(maxValue(tmp)) && maxValue(tmp) == 1) {
    tmp <- mask(tmp,
                template)
    
    writeRaster(tmp,
                file = filename,
                overwrite = TRUE)
    
  } else {
    
    rm(tmp)
    
    file.remove(filename)
    
  }
  
  return (NULL)
  
}

subsamplePolys <- function (data, ...) {
  # given a presence-background dataset, with multiple rows for some of the
  # occurrence records, subset it so that there's only one randomly selected
  # point from each polygon and then take a bootstrap it using `subsample`.
  # Dots argument is passed to subsample.
  
  # index for background records (outbreak id = 0)
  bg_idx <- data$outbreak_id == 0
  
  # subset to get occurrence section only
  occ <- data[!bg_idx, ]
  
  # get the different outbreaks
  u <- unique(occ$outbreak_id)
  
  # loop through, picking an index for each based on the number available
  occ_idx <- sapply(u,
                    function (id, occ) {
                      idx <- which(occ$outbreak_id == id)
                      sample(idx, 1)
                    },
                    occ)
  
  # get the subsetted dataset
  dat <- rbind(occ[occ_idx, ],
               data[bg_idx, ])
  
  # randomly subsample the dataset
  ans <- subsample(dat,
                   n = nrow(dat),
                   ...)
  
  # remove the outbreak ID column
  ans <- ans[, -which(names(ans) == 'outbreak_id')]
  
  return (ans)
}

makeUniform <- function (SPDF) {
  pref <- substitute(SPDF)  #just putting the file name in front.
  newSPDF <- spChFIDs(SPDF,
                      as.character(paste(pref,
                                         rownames(as(SPDF,
                                                     "data.frame")),
                                         sep = "_")))
  return (newSPDF)
}

summarizeStats <- function (path) {
  
  # load validation stats
  stats <- read.csv(paste0(path, 'stats.csv'),
                    row.names = 1)
  
  auc  <- c(as.character(round(mean(stats$auc,
                                    na.rm = TRUE),
                               2)),
            as.character(round(sd(stats$auc,
                                  na.rm = TRUE),
                               2)))
  
  # load relative influence stats
  relinf <- read.csv(paste0(path,
                            'relative_influence.csv'),
                     stringsAsFactors = FALSE)
  
  ans <- c(auc_mean = auc[1],
           auc_sd = auc[2],
           cov1 = relinf[1, 1],
           relinf1 = as.character(round(relinf[1, 2],
                                        1)),
           cov2 = relinf[2, 1],
           relinf2 = as.character(round(relinf[2, 2],
                                        1)),
           cov3 = relinf[3, 1],
           relinf3 = as.character(round(relinf[3, 2],
                                        1)),
           cov4 = relinf[4, 1],
           reliUSER = as.character(round(relinf[4, 2],
                                        1)),
           cov5 = relinf[5, 1],
           relinf5 = as.character(round(relinf[5, 2],
                                        1)))
  
  return (ans)
  
}

thresholdRisk <- function (risk_raster,
                           occ,
                           proportion = 1) {
  
  # extract risk values for the occurrence data
  occ_risk <- extract(risk_raster[[1]],
                      occ[, c('long', 'lat')])
  
  # remove any missing data
  occ_risk <- na.omit(occ_risk)
  
  # get the relevant quantile
  thresh <- quantile(occ_risk,
                     1 - proportion,
                     na.rm = TRUE)
  
  # classify the raster
  at_risk_raster <- risk_raster > thresh
  
  # return this
  return (at_risk_raster)
  
}

# rewriting sunsample to include sample weights
subsamplenew <- function (data,
                       n,
                       colname,
                       minimum = c(5, 5),
                       prescol = 1,
                       replace = FALSE,
                       max_tries = 10) {
  # get a random subset of 'n' records from 'data', ensuring that there
  # are at least 'minimum[1]' presences and 'minimum[2]' absences.
  # assumes by dUSERt that presence/absence code is in column 1 ('prescol')
  OK <- FALSE
  tries <- 0
  
  # until criteria are met or tried enough times
  while (!OK & tries < max_tries) {
    # take a subsample
    sub <- data[sample(1:nrow(data), n, replace = replace, prob = dat_all$prevalence), ]
    # count presences and absences
    #npres <- sum(sub[, prescol]) 
    #nabs <- sum(1 - sub[, prescol])
    npres <- nrow(sub[sub[, prescol] >= 1,])
    nabs <- nrow(sub[sub[, prescol] == 0,])
    # if they are greater than or equal to the minimum, criteria are met
    if (npres >= minimum[1] & nabs >= minimum[2]) {
      OK <- TRUE
    }
    tries <- tries + 1
  }
  
  # if the number of tries maxed out, throw an error
  if (tries >= max_tries) {
    stop (paste('Stopped after',
                max_tries,
                'attempts,
                try changing the minimum number of presence and absence points'))
  }
  
  return (sub)
  }
