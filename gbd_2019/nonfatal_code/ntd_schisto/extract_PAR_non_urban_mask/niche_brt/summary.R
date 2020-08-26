.libPaths('FILEPATH')

library(seegSDM)

source('FILEPATH')    # eco-niche stats

# read in covariates and data
dat_all <- read.csv('FILEPATH')
covs <- brick('FILEPATH')
covs <- dropLayer(covs, c(6, 11:13))

# make lists of all files in output directories
path <- 'FILEPAHT'
model_files <- list.files('FILEPATH'), full.names = TRUE)
model_list <- c()
stats_files <- list.files('FILEPATH'), full.names = TRUE)
stats_list <- c()
preds_files <- list.files(path = 'FILEPATH', full.names = TRUE)
for (i in 1:length(model_files)){
  model_list[[i]] <- get(load(model_files[[i]]))
  stats_list[[i]] <- get(load(stats_files[[i]]))
}

# make lists from .Rdata files
preds_list <- lapply(preds_files, raster)
preds_list <- stack(preds_list)

# zoom to disease extent
zoom<- c(abs(min(dat_all$longitude))/180 + 0.1,
         abs(max(dat_all$longitude))/180 + 0.1,
         abs(min(dat_all$latitude))/180 + 0.1,
         abs(max(dat_all$latitude))/180 + 0.1)
         
# get prediction raster and save it as GeoTiff
preds_sry <- combinePreds(preds_list)
names(preds_sry) <- c('mean', 'median', 'lowerCI', 'upperCI')
writeRaster(preds_sry,
            file = 'FILEPATH',
            format = 'GTiff',
            overwrite=T)

# verification histograms
dat0.pts <- dat_all[dat_all$PA==0, 2:3]
dat0.preds <- extract(preds_sry, dat0.pts)
png('FILEPATH')
hist(dat0.preds, breaks = 20)
dev.off()

dat1.pts <- dat_all[dat_all$PA==1, 2:3]
dat1.preds <- extract(preds_sry, dat1.pts)
png('FILEPATH')
hist(dat1.preds, breaks = 20)
dev.off()

# save matrix of summary statistics
stats <- do.call('rbind', stats_list)
write.csv(stats, file = 'FILEPATH')

# get and save relative influence scores of covariates; make a box plot
relinf <- getRelInf(model_list, plot=F) #for gbm fitting, use summary(gbm.fit$finalModel)
write.csv(relinf, file = 'FILEPATH')

# plot the risk map
ext <- zoom*extent(preds_sry)
png(paste0(path, 'oncho_risk.png'),
    width = 3000,
    height = 2000,
    pointsize = 30)
par(oma = rep(0, 4),
    mar = c(0, 0, 0, 2))
plot(preds_sry[[1]], axes=F, box=F, xlim=c(ext[1],ext[2]), ylim=c(ext[3],ext[4]))
points(dat_all[dat_all$PA == 1, 2:3],
       pch = 16,
       cex = 0.25,
       col = 'blue')
dev.off()

# calculate uncertainty in the predictions; write mean prediction and uncertainty rasters as GeoTiffs
preds_sry$uncertainty <- preds_sry[[4]] - preds_sry[[3]]
writeRaster(preds_sry$mean, file = 'FILEPATH', format = 'GTiff')
writeRaster(preds_sry$uncertainty, file = 'FILEPATH', format = 'GTiff')

## PLOT EFFECT CURVES

# get the covariate mean effects; make a plot of each mean effect curve
effect <- getEffectPlots(model_list, plot=T)

# get the order of plots (all except relinf)! - customize list to covs of interest
order <- match(rownames(relinf), names(covs))

# make lists of x axis labels and titles
short_names<-names(effect)[order]
units <- c('mm/day', #GPCP
           'index', #arid
           'index', #TCB
           'meters', #GLOB_DEM_MOSAIC
           'index', #EVI
           'index', #TCW
           'meters', #minDIst
           'deg C', #LST_DAY
           'urban/rural') #GHS_settlment
units.df <- data.frame(units)
rownames(units.df) <- short_names

# initialize plot file
png('FILEPATH',
    width = 5000,
    height = 3000,
    pointsize = 60)
# set up grid of plots
layout(matrix(c(1:9),3,byrow=T))
# loop through plots
for (i in 1:length(effect)) {
  # extract summary stats
  df <- effect[[order[i]]][, 1:4]
  # pick y axis
  if (i %% 5 == 1) {
    ylab = 'marginal effect'
  } else {
    ylab = ''
  }
  # set up empty plotting region
  plot(df[, 2] ~ df[, 1],
       type = 'n',
       ylim = c(-3.5, 2.5),
       ylab = '',
       xlab = '')
  # add the 95% CIs
  polygon(x = c(df[, 1], rev(df[, 1])),
          y = c(df[, 3], rev(df[, 4])),
          border = NA,
          col = grey(0.7))
  # add the mean line
  lines(df[, 2] ~ df[, 1],
        lwd = 5,
        col = grey(0.2))
  # y axis lable (only on left hand column)
  title(ylab = ylab,
        cex.lab = 1.2,
        col.lab = grey(0.3),
        xpd = NA,
        line = 2.5)
  # x-axis label
  j = which(rownames(units.df)==short_names[i])
  title(xlab = units[j],
        cex.lab = 1.2,
        col.lab = grey(0.3),
        line = 2.5)
  # title
  title(main = short_names[i],
        line = 1.5,
        cex.main = 1.2)
  # relative contribution inset
  mtext(text = round(relinf[i, 1] / 100, 2),
        side = 3,
        line = -2,
        adj = 0.07,
        col = grey(0.5))
}
dev.off()

## RASTER DATA PLOTTING
ext <- zoom*extent(covs[[1]])
png('FILEPATH',
    width = 5000,
    height = 3000,
    pointsize = 60)
layout(matrix(c(1:9),3,byrow=T))
for (i in 1:length(names(covs))){
  # png('test.png')
  plot(covs[[order[i]]], xlim=c(ext[1],ext[2]), ylim=c(ext[3],ext[4]))
  title(main = short_names[i])
  points(dat_all[dat_all$PA==1, 2:3], pch=16, cex=0.10, col='blue')
  points(dat_all[dat_all$PA==0, 2:3], pch=16, cex=0.10, col='red')
} 
dev.off()

## HISTOGRAM PLOTTING
dat.pts <- dat_all[dat_all$PA==1, 6:ncol(dat_all)]
png('FILEPATH',
    width = 5000,
    height = 3000,
    pointsize = 60)
layout(matrix(c(1:9),3,byrow=T))
for (i in 1:length(dat.pts)){
  # png('test.png')
  hist(dat.pts[[i]], breaks=20, main=names(dat.pts[i]))
}
dev.off()
