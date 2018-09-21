### SPECIFIC RESULTS CALLED OUT IN PAPER

run_date = "2017_05_20_09_44_46"
output <- paste0('<<<< FILEPATH REDACTED >>>>>/',run_date,'/')

processing = FALSE

  if(processing) {
  # load admin rasters
  load_simple_polygon(gaul_list = get_gaul_codes('africa'), buffer = 0.4, subset_only = TRUE)
  raster_list    <- build_simple_raster_pop(subset_shape) # slow
  simple_raster  <- raster_list[['simple_raster']]

  # load output data objects
  load(paste0('<<<< FILEPATH REDACTED >>>>>/',run_date,'/died_africa_raked_child_cell_pred.RData'))
  cell_pred_c <-x
  load(paste0('<<<< FILEPATH REDACTED >>>>>/',run_date,'/died_africa_raked_neonatal_cell_pred.RData'))
  cell_pred_n <-x
  rm(x)

  ad0rast <- load_admin_raster(admin_level=0,simple_raster=simple_raster)
  ad0.vec <- rep(as.vector(ad0rast)[cellIdx(ad0rast)],4)
  ad1rast <- load_admin_raster(admin_level=1,simple_raster=simple_raster)
  ad1.vec <- rep(as.vector(ad1rast)[cellIdx(ad0rast)],4)
  ad2rast <- load_admin_raster(admin_level=2,simple_raster=simple_raster)
  ad2.vec <- rep(as.vector(ad2rast)[cellIdx(ad0rast)],4)

  gaul_to_loc_id <- fread("/<<<< FILEPATH REDACTED >>>>>/gaul_to_loc_id.csv")

  ad0n.vec <- gaul_to_loc_id$loc_name[match(ad0.vec,gaul_to_loc_id$GAUL_CODE)]
  ad0n.vec.orig <- ad0n.vec
  yr.vec   <- rep(1:4,each=length(ad0.vec)/4)

  pop <-brick(raster('<<<< FILEPATH REDACTED >>>>>/africa_pop_0005TotalAdj_2000_5km.tif'),
                raster('<<<< FILEPATH REDACTED >>>>>/africa_pop_0005TotalAdj_2005_5km.tif'),
                raster('<<<< FILEPATH REDACTED >>>>>africa_pop_0005TotalAdj_2010_5km.tif'),
                raster('<<<< FILEPATH REDACTED >>>>>/africa_pop_0005TotalAdj_2015_5km.tif'))
  pop   <- crop(pop,ad0rast)
  p2000 <- as.vector(pop[[1]])[cellIdx(ad0rast)]
  p2000[is.na(p2000)]<-0
  p2015 <- as.vector(pop[[4]])[cellIdx(ad0rast)]
  p2015[is.na(p2015)]<-0

  # grab admin units in draw space
  # remove unwanted territories from cell stuff
  goodcells <- which(!ad0n.vec.orig %in% c('Tunisia','Libya','Sao Tome and Principe','Algeria','Comoros','Cape Verde','Yemen'))
  cell_pred_c = cell_pred_c[goodcells,]
  cell_pred_n = cell_pred_n[goodcells,]
  ad0.vec     = ad0.vec[goodcells]
  ad1.vec     = ad1.vec[goodcells]
  ad2.vec     = ad2.vec[goodcells]
  ad0n.vec    = ad0n.vec[goodcells]
  yr.vec      = yr.vec[goodcells]

  p2000 = p2000[goodcells[1:(length(goodcells)/4)]]
  p2015 = p2015[goodcells[1:(length(goodcells)/4)]]

  # get year specific ones
  cpc2000 = cell_pred_c[yr.vec==1,]
  cpc2015 = cell_pred_c[yr.vec==4,]
  cpn2000 = cell_pred_n[yr.vec==1,]
  cpn2015 = cell_pred_n[yr.vec==4,]

  c2000.mean = apply(cpc2000,1,mean)
  c2015.mean = apply(cpc2015,1,mean)
  n2000.mean = apply(cpn2000,1,mean)
  n2015.mean = apply(cpn2015,1,mean)

  # get draw level roc stuff
  mdgc <- log(cpc2015/cpc2000)/15
  sdgc <- log(0.025  /cpc2015)/15
  mdgc.mean <- apply(mdgc,1,mean)
  sdgc.mean <- apply(sdgc,1,mean)
  mdgn <- log(cpn2015/cpn2000)/15
  sdgn <- log(0.012  /cpn2015)/15
  mdgn.mean <- apply(mdgn,1,mean)
  sdgn.mean <- apply(sdgn,1,mean)

  # get full training data
  dat <- fread(paste0('<<<< FILEPATH REDACTED >>>>>/',run_date,'/full_training_data.csv'))


  # get draw level admin 1 and admin 2 results
  ad2d_n <- fread(paste0(output,'died_neonatal_admin2_draws.csv'))
  ad1d_n <- fread(paste0(output,'died_neonatal_admin1_draws.csv'))
  ad2d_c <- fread(paste0(output,'died_child_admin2_draws.csv'))
  ad1d_c <- fread(paste0(output,'died_child_admin1_draws.csv'))

  # numeric
  for(vn in c('year',paste0('draw',1:1000))){
    ad2d_n[[vn]]=as.numeric(ad2d_n[[vn]])
    ad1d_n[[vn]]=as.numeric(ad1d_n[[vn]])
    ad2d_c[[vn]]=as.numeric(ad2d_c[[vn]])
    ad1d_c[[vn]]=as.numeric(ad1d_c[[vn]])
  }

  # calculate S/M Dev goals
  for(y in c(2000,2015))  for(a in 1:2) for(g in c('n','c'))
    assign(paste0('ad',a,'d_',g,'.',y),subset(get(paste0('ad',a,'d_',g)),year==y))

  ad2n.sdg <- log(0.012/(ad2d_n.2015[,7:1006]))/15
  ad2c.sdg <- log(0.025/(ad2d_c.2015[,7:1006]))/15
  ad1n.sdg <- log(0.012/(ad1d_n.2015[,6:1005]))/15
  ad1c.sdg <- log(0.025/(ad1d_c.2015[,6:1005]))/15

  ad2n.mdg <- log((ad2d_n.2015[,7:1006])/(ad2d_n.2000[,7:1006]))/15
  ad2c.mdg <- log((ad2d_c.2015[,7:1006])/(ad2d_c.2000[,7:1006]))/15
  ad1n.mdg <- log((ad1d_n.2015[,6:1005])/(ad1d_n.2000[,6:1005]))/15
  ad1c.mdg <- log((ad1d_c.2015[,6:1005])/(ad1d_c.2000[,6:1005]))/15

  # summarize
  for(n in c('ad2n.sdg','ad2c.sdg','ad2n.mdg','ad2c.mdg'))
  assign(paste0(n,'.summ'), cbind(ad2d_n.2015[,2:5],
                        data.table(mean=apply(get(n),1,mean),
                                   lci =apply(get(n),1,quantile,probs=0.025),
                                   uci =apply(get(n),1,quantile,probs=0.975))) )
  for(n in c('ad1n.sdg','ad1c.sdg','ad1n.mdg','ad1c.mdg'))
  assign(paste0(n,'.summ'), cbind(ad1d_n.2015[,2:4],
                        data.table(mean=apply(get(n),1,mean),
                                   lci =apply(get(n),1,quantile,probs=0.025),
                                   uci =apply(get(n),1,quantile,probs=0.975))) )

  for(n in c('ad2d_n','ad2d_c'))
  assign(paste0(n,'.summ'), cbind(ad2d_n[,2:6],
                        data.table(mean=apply(get(n)[,7:1006],1,mean),
                                   lci =apply(get(n)[,7:1006],1,quantile,probs=0.025),
                                   uci =apply(get(n)[,7:1006],1,quantile,probs=0.975))) )
  for(n in c('ad1d_n','ad1d_c'))
  assign(paste0(n,'.summ'), cbind(ad1d_n[,2:5],
                        data.table(mean=apply(get(n)[,6:1005],1,mean),
                                   lci =apply(get(n)[,6:1005],1,quantile,probs=0.025),
                                   uci =apply(get(n)[,6:1005],1,quantile,probs=0.975))) )

  save_post_est(ad1d_c.summ,'csv',sprintf('%s_admin1_summary_table','child'))
  save_post_est(ad1d_n.summ,'csv',sprintf('%s_admin1_summary_table','neonatal'))
  save_post_est(ad2d_c.summ,'csv',sprintf('%s_admin2_summary_table','child'))
  save_post_est(ad2d_n.summ,'csv',sprintf('%s_admin2_summary_table','neonatal'))

  # get GBD Data
  gbd_c  <- load_u5m_gbd(gaul_list = get_gaul_codes('africa'), getci = TRUE, age_group='child')
  gbd_n  <- load_u5m_gbd(gaul_list = get_gaul_codes('africa'), getci = TRUE, age_group='neonatal')
  rf_n   <- fread(paste0(output,'died_africa_neonatal_rf.csv'))
  rf_c   <- fread(paste0(output,'died_africa_child_rf.csv'))
  rf_c   <- merge(rf_c,gaul_to_loc_id,by.x='name',by.y='GAUL_CODE')
  rf_n   <- merge(rf_n,gaul_to_loc_id,by.x='name',by.y='GAUL_CODE')
  gbd_c  <- merge(gbd_c,gaul_to_loc_id,by.x='name',by.y='GAUL_CODE')
  gbd_n  <- merge(gbd_n,gaul_to_loc_id,by.x='name',by.y='GAUL_CODE')


  # save workspace
  save.image(paste0('<<<< FILEPATH REDACTED >>>>>/',run_date,'/prepped_for_plugging.RData'))

} else { # if not processing, just load the processed outputs here
  load(paste0('<<<< FILEPATH REDACTED >>>>>/',run_date,'/prepped_for_plugging.RData'))
}


## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## Yet attaining SDG3·2 remains a very ambitious goal for most of Africa; the majority of the continent needs to achieve a ___% or faster decline in under-5 mortality per year
quantile(apply(sdgc,2,weighted.median,w=p2015),probs=c(0.025,0.50,.975))
weighted.median(sdgc.mean,p2015)
  # 6.740291

## We assembled __ geographically-resolved household survey
  length(unique(dat$nid))
  # 235

## data were extracted together with cluster geographic coordinates (GPS); there were ___ such points
  dp <- subset(dat, pseudocluster == FALSE)
  nrow(unique(cbind(dp$cluster_id,dp$nid)))
  # 59,279

  dp <- subset(dat, pseudocluster == TRUE)
  nrow(unique(cbind(dp$cluster_id,dp$nid)))
  # 6,111 unique polygon

### Subnational areas in countries such as _, _, and _ recorded some of the largest decreases in child mortality rates since 2000, positioning them well to achieve SDG targets by or prior to 2030.
  head(ad2c.mdg.summ[order(mean)],50)
  # Botswana, Rwanda, Ethiopia

## We achieved this by calculating the ratio of the population-weighted posterior mean national-level estimate from our analysis to posterior mean national estimates from GBD, and then multiplying each cell in the posterior sample by this ratio. The median for these ratios was ___
  rf_c = subset(rf_c, !loc_nm_sh %in% c('Tunisia','Libya','Sao Tome Principe','Algeria','Comoros','Cape Verde'))
  rf_n = subset(rf_n, !loc_nm_sh %in% c('Tunisia','Libya','Sao Tome Principe','Algeria','Comoros','Cape Verde'))

  quantile(rf_n$raking_factor, probs = c(.05,.1,.25,.5,.75,.9,.95))
  quantile(rf_c$raking_factor, probs = c(.05,.1,.25,.5,.75,.9,.95))
  rf_c[raking_factor == max(raking_factor)]
  rf_c[raking_factor == min(raking_factor)]
  rf_n[raking_factor == max(raking_factor)]
  rf_n[raking_factor == min(raking_factor)]


## In 2000, most of sub-Saharan Africa recorded under-5 mortality rates exceeding ___ deaths per 1,000 livebirths
  weighted.median(c2000.mean[!ad0n.vec[yr.vec==1] %in% c('Morocco','Egypt')],p2000[!ad0n.vec[yr.vec==1] %in% c('Morocco','Egypt')])
  # 137.9782
  weighted.median(c2000.mean,p2000)
  # 133.3649


## By 2015, half of Africa had under-5 mortality rates below XX deaths per 1,000 livebirths,
  weighted.median(c2015.mean[!ad0n.vec[yr.vec==4] %in% c('Morocco','Egypt')],p2015[!ad0n.vec[yr.vec==4] %in% c('Morocco','Egypt')])
  # sub saharan : 71.975


## 2000: large swathes of xx, xx, and xx, along with other countries in xx Africa, surpassed 200 deaths per 1,000 livebirths.
  tmp <- subset(ad2d_c.summ,year==2000 & mean > .2)
  table(tmp$ADM0_NAME) # 22 countries with ad2

  dt <- data.table(m=c2000.mean,p=p2000,a=ad0n.vec[yr.vec==1])
  dt[,gt200  := m > 0.200]
  dt[,tp  := sum(p),by=a]
  dt[,pp  := sum(p),by=.(a,gt200)]
  dt <- subset(dt, gt200==TRUE)
  dt[,pct := pp/tp]
  dt <- unique(dt[,c('a','pct')])
  dt[order(-pct)]
  # Niger 83%, Sierra Leone 64%, Mali 53%, Nigeria 41%, Chad 36%

## Nonetheless, a number of localities, including __ __ __ still faced under-5 mortality rates higher than 180 per 1,000 livebirths in 2015.
  subset(ad1d_c.summ,year==2015)[order(-mean),][1:15, ]
  subset(ad2d_c.summ,year==2015)[order(-mean),][mean>.17]   # mean above leve
  subset(ad2d_c.summ,year==2015)[order(-mean),][lci>.180] # certain above level

  table(subset(ad2d_c.summ,year==2015)[order(-mean),][mean>.17]$ADM0_NAME)
  # Burkina Faso, CAF, Mali, Nigeria, Chad

## Further, sizeable within-country inequalities remained. For instance, Nigeria had a national under-5 mortality rate of ________ per 1,000 livebirths, yet at the local government area level (LGA, equivalent to district), under-5 mortality rates ranged from ________ deaths per 1,000 livebirths in the ______ LGA of ______ to ______ deaths per 1,000 livebirths in the ______-.
  gbd_c[loc_name=="Nigeria"] # these are for exact years from GBD2016 envelope

  ad2d_c.summ[ADM0_NAME=="Nigeria"&year==2015][mean==min(ad2d_c.summ[ADM0_NAME=="Nigeria"&year==2015]$mean)]
  ad2d_c.summ[ADM0_NAME=="Nigeria"&year==2015][mean==max(ad2d_c.summ[ADM0_NAME=="Nigeria"&year==2015]$mean)]

  ad1d_c.summ[ADM0_NAME=="Nigeria"&year==2015][mean==min(ad1d_c.summ[ADM0_NAME=="Nigeria"&year==2015]$mean)]
  ad1d_c.summ[ADM0_NAME=="Nigeria"&year==2015][mean==max(ad1d_c.summ[ADM0_NAME=="Nigeria"&year==2015]$mean)]


## In 2015, ___ had one of the widest gaps between districts, ranging from XX deaths per 1,000 livebirths in __ in the ___ region to ___deaths per 1,000 livebirths in ___.

  ad2_n_tmp <- subset(ad2d_n.summ, year == 2015)
  ad2.min <- aggregate(mean ~ ADM0_NAME, ad2_n_tmp, min)
  colnames(ad2.min)[2] <- c("min")
  ad2.max <- aggregate(mean ~ ADM0_NAME, ad2_n_tmp, max)
  colnames(ad2.max)[2] <- c("max")
  ad2.range <- data.table(merge(ad2.min, ad2.max))
  ad2.range[, min:=as.numeric(min) ]
  ad2.range[, max:=as.numeric(max) ]
  ad2.range[, range:=(max - min) ]

  ## get countries with biggest range
  head(ad2.range[order(ad2.range[, 4], decreasing = T), ])
  #  THE TOP 6 ARE NIGERA, MALI, COTE D IVOIRE, CHAD, SOUTH SUDAN, SUDAN

  # look at some specifics
  ct <- "Côte d'Ivoire"
  ct.dat <- (subset(ad2_n_tmp, ADM0_NAME == ct))
  ct.dat[which.max(ct.dat[, mean]), ] ## ad2 with largest rate in ct
  ct.dat[which.min(ct.dat[, mean]), ] ## ad2 with smallest rate in ct

  ct <- "Nigeria"
  ct.dat <- (subset(ad2_n_tmp, ADM0_NAME == ct))
  ct.dat[which.max(ct.dat[, mean]), ] ## ad2 with largest rate in ct
  ct.dat[which.min(ct.dat[, mean]), ] ## ad2 with smallest rate in ct

  ct <- "Chad"
  ct.dat <- (subset(ad2_n_tmp, ADM0_NAME == ct ) )
  ct.dat[which.max(ct.dat[, mean]), ] ## ad2 with largest rate in ct
  ct.dat[which.min(ct.dat[, mean]), ] ## ad2 with smallest rate in ct

  ct <- "Sudan"
  ct.dat <- (subset(ad2_n_tmp, ADM0_NAME == ct) )
  ct.dat[which.max(ct.dat[, mean]), ] ## ad2 with largest rate in ct
  ct.dat[which.min(ct.dat[, mean]), ] ## ad2 with smallest rate in ct



## ___, __, and ___ all had districts experiencing neonatal mortality rates above 60 deaths per 1,000 livebirths.
  ###
  ad2d_n.summ[year==2015&mean>.05,]
  table(ad2d_n.summ[year==2015&mean>.05,]$ADM0_NAME)
  # Nigeria, Cote dIvoire, Mali .. Change to 50




## In 2015,XXX achieved the SDG3·2 target for under-5 mortality, at ___ deaths and ___ deaths per 1,000 livebirths, respectively; still, approximately __% of each country’s population lived in areas above this threshold. Beyond these two North African countries,

  # child and pop vectors: ch2015, pop2015
  # admin vector: ad0n.vec
  dt <- data.table(m=c2015.mean,p=p2015,a=ad0n.vec[yr.vec==4])
  dt[,sdgmet := m<25/1000]
  dt=na.omit(dt)
  dt[,tp := sum(p),by=a]
  dt[,pw := p/tp]

  dtt = subset(dt, a %in% c("Morocco","Botswana","Egypt"))
  dtt[,.(pctpop = sum(pw)),by=.(a,sdgmet)]
  # 25% for MAR EGYPT,  botswanna is 100%

  # pixels meeting target
  unique(dt$a[dt$sdgmet==TRUE])


#### From 2000 to 2015, many countries saw under-5 mortality decrease more than 4·4% per year (Figure 5A), a rate that exceeded the pace of progress established under MDG4 (ie, a two-thirds reduction by 2015); these countries included XXX

  #### GBD estimates for country level rates of decline
  gb=subset(gbd_c, !loc_nm_sh %in% c('Tunisia','Libya','Sao Tome Principe','Algeria','Comoros','Cape Verde','Yemen'))

  gb <- subset(gb, year %in% c(2000,2015))
  gb <- gb[,c('loc_name','mean','year'),with=FALSE]
  gb <- reshape(gb,
                timevar = "year",
                idvar = c("loc_name"),
                direction = "wide")
  gb[,roc := log(mean.2015/mean.2000)/15]
  gb[roc < -0.044][order(roc)]
  ## TOP FEW ARE BOT RWA ETH ANG LIB EQ GUINEA ZAMBIA MALAWI SENEGAL


## Several other countries, such as XXX, had a mixture of annualised rates of decline between 2·0% and 4·4%, as well as those that exceeded a 4·4% decrease per year.
  dt <- data.table(r=mdgc.mean ,p00=p2000,p15=p2015,a=ad0n.vec[yr.vec==4])
  dt <- na.omit(dt)
  dt <- dt[,.(min=min(r),max=max(r)),by=a]
  dt[,rng:=max-min]
  dt[order(-rng)]

  dt[min < -0.044 & max < -0.02][order(max)]
    # 22 fit this criteria

### By contrast, areas throughout XXX experienced much slower gains, recording annualised rates of change between an increase of 0·2% and decrease of 3·0% since 2000.
  dt[order(-max)]
  dt[order(-min)]
  # MALI, CAF, CAMEROON,  (-0.36 and 4.5)

    tmp <- ad2c.mdg.summ
    tmp[,exceed := mean < -0.044]
    tmp=tmp[,.(exceed = sum(exceed)),by=ADM0_NAME]
    tmp[exceed==0]


## Nigeria posted some of the widest disparities in terms of progress, with annualised rates spanning from XX to XX

  # get year specific ones
  roc.nga <- na.omit(mdgc[ad0n.vec[yr.vec==4]=="Nigeria",])
  roc.nga  <- data.table(mean=apply(roc.nga,1,mean),l=apply(roc.nga,1,quantile,probs=.025),u=apply(roc.nga,1,quantile,probs=.975))
  roc.nga[mean==min(mean)]
  roc.nga[mean==max(mean)]
  # -0.7 [-1.6, 0.2]
  # -5.00 [-5.89, -4.12]

## Nonetheless, only __ out of the 46 countries had districts that, based on current trajectories, could reduce under-5 mortality rates to 25 deaths per 1,000 livebirths or lower by 2030.

  trajectory.2030  <- exp(ad2c.mdg*15)*ad2d_c.2015[,7:1006]
  willmeetsdg.2030 <- trajectory.2030 < 0.025

  probabilitymeetsdg <- cbind(ad2c.mdg.summ[,1:4],data.table(mean=apply(willmeetsdg.2030,1,mean)))

  ##
  sum(probabilitymeetsdg[,.(s=sum(mean>0.025)),by=ADM0_NAME]$s>0) # District with > 2.5% probability of meeting it
  sum(probabilitymeetsdg[,.(s=sum(mean>0.500)),by=ADM0_NAME]$s>0) # District with > 50%  probability of meeting it
  sum(probabilitymeetsdg[,.(s=sum(mean>0.975)),by=ADM0_NAME]$s>0) # District with > 97.5% probability of meeting it


## Instead, to reach that SDG3·2 threshold by 2030, the majority of Africa must at XX the pace at which their under-5 mortality rates fell between 2000 and 2015 (Figure 5C).

  # do in draw space so each draw is a % of the population
  tmp <- (sdgc < mdgc*2)*p2015
  gc  <- !is.na(tmp[,1])
  mustdouble     <- na.omit(data.table(m=apply(tmp[gc,] , 1, mean),
                                       l=apply(tmp[gc,] , 1, quantile, probs=0.025),
                                       u=apply(tmp[gc,] , 1, quantile, probs=0.975),
                                       p=p2015[gc],
                                       a= ad0n.vec[yr.vec==1][gc]))
  mustdouble[,.(mean =sum(m)/sum(p),
                upper=sum(u)/sum(p),
                lower=sum(l)/sum(p))]
#        mean     upper     lower
#1: 0.4459468 0.6078043 0.2837088

  # 44.59%



## Within some countries, particularly in XXX, under-5 mortality rates must fall more than 10% per year through 2030 to achieve SDG3·2.
  dt <- na.omit(data.table(roc=sdgc.mean,a= ad0n.vec[yr.vec==1],p=p2015))
  dt <- dt[,bigroc := roc < -0.10,]
  dt <- dt[,tp := sum(p),by=a]
  dt <- dt[,tpbig := sum(p),by=.(a,bigroc)]
  dt <- subset(dt[,bigpct := tpbig/tp],bigroc==TRUE)
  unique(dt[,c('a','bigpct'),with=FALSE])[order(-bigpct)]
  # CAF, MALI, SIERRA LEONE, NIGER, CHAD, BURKINA FASO, ALL MORE THAN 50% of population

## By 2015, nearly all geographies saw under-5 mortality rates fall since 2000, with areas in XX recording particularly dramatic reductions.
  head(ad1c.mdg.summ[order(mean)],30)
  # BWA, RWA, ETH, ANGOLA, UGANDA, SENEGAL

  head(ad2c.mdg.summ[order(mean)],30)
  # BWA, RWA, ETH, ANGOLA, UGANDA, SENEGAL


## Yet in XX, more than half of the population lives in regions with under-5 mortality rates exceeding 120 deaths per 1,000 livebirths.
  dt <- na.omit(data.table(c=c2015.mean, a=ad0n.vec[yr.vec==1], p=p2015))
  dt <- dt[,.(mr = weighted.median(c,p)),by=a]
  dt[order(mr)]
  # CHAD, SIERRA LEONE, MALI, CAF.

## especially as more than XX% of the continent’s children live in areas where annual declines in under-5 mortality must exceed the pace of reductions they achieved from 2000 to 2015 in order to meet the SDG3.2 target by 2030.
  dt <- na.omit(data.table(mdg=mdgc.mean,sdg=sdgc.mean,a= ad0n.vec[yr.vec==1],p=p2015))

  dt$mustincreasepace <- dt$sdg < dt$mdg
  sum((dt$mustincreasepace) * dt$p)/sum(dt$p)
  ## 75% (0.7492627)

  # Another way to look at it: > 50% probability of needing to increase
  mi <-   na.omit(data.table(mi=apply(mdgc > sdgc,1,mean),a= ad0n.vec[yr.vec==1],p=p2015))
  mi$mi50 = mi$mi > .5
  sum((mi$mi50) * dt$p)/sum(dt$p)
  ## 75% same result (0.7492239)

## From 2000 to 2015, only X% of districts recorded an annualised rate exceeding 4·4%, whereas X% of these geographies will need to at least match their current pace to make the SDG3·2 target for under-5 mortality a reality.
  mean(ad2c.mdg.summ$mean < -0.044,na.rm=T)    # 26% ( 0.2623126 )
  mean(ad2c.mdg.summ$mean[ad2c.mdg.summ$mean < -0.044] > ad2c.sdg.summ$mean[ad2c.mdg.summ$mean < -0.044])  # 60%


## Of particular concern are the areas encompassing nearly X% of Africa’s population that must double the MDG4 rate – or more – to reach 25 deaths per 1,000 livebirths by 2030, a pace that has been unprecedented in the last few decades.
  dt = na.omit(data.table(mustdoubleMDG = sdgc.mean < -0.088, p = p2015))
  sum(dt$mustdoubleMDG*dt$p)/sum(dt$p)
  ## 26.6% (0.2642302)

















############# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
####### SI stuff


## Grab hyperpriors
    load(paste0('<<<< FILEPATH REDACTED >>>>>.RData'))
    mesh_s <- build_space_mesh(d = df,simple = simple_polygon,max_edge=c(0.35, 5),mesh_offset=c(1,5))
    param2.matern.orig(mesh_s)


##






#
