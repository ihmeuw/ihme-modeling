
# Plots for the SI etc.





#################################################################################
# setup
repo <- <<<< FILEPATH REDACTED >>>>>
setwd(repo)
root <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")

source('./all_functions.R')

datpath = sprintf('<<<< FILEPATH REDACTED >>>>>/%s',run_date)
figpath = sprintf('<<<< FILEPATH REDACTED >>>>>/%s',run_date)
dir.create(figpath)
#################################################################################






#################################################################################
#################################################################################

################
# make raw data Plots
load('<<<< FILEPATH REDACTED >>>>>/simple_africa_polygon.RData')
shp =shapefile("<<<< FILEPATH REDACTED >>>>>/africa_ad0.shp")

# bring in the dataset for all africa
run_date='2017_05_20_09_44_46'
df <- fread(paste0('<<<< FILEPATH REDACTED >>>>>/output/',run_date,'/full_training_data.csv'))

require(fields)
df[,mr:=died/N]
  df[,s:=(1-mr)^m]
  md5 <- df[,.(mr=1-prod(s),N=sum(N),age=5,sbh_wgt=prod(sbh_wgt),m=60,s=prod(s)),
              by=.(year,cluster_id,longitude,latitude,shapefile,location_code,nid,source,country,data_type)]
  df <- data.table(smartbind(df,md5))
shp=subset(shp,country_id!='YEM')
for(a in 1:5){
pdf(paste0(sharedir,'/output/',run_date, "/rawdataplot_age",a,".pdf"), height = 5, width = 5)
  par(mfrow=c(2,2))
  for(y in c(2000,2005,2010,2015)){
    message(paste(a,y))
    tmp = subset(df,age==a)
    mx =  quantile(tmp$mr,probs=.90,na.rm=T)
    tmp = subset(tmp,year==y)
    quilt.plot(tmp$longitude,tmp$latitude,tmp$mr,main=y,zlim=c(0,mx),nrow=48,ncol=48,nlevels=10)
    lines(shp,lwd=.1)
  }
dev.off()
}



#################################
# NULL vs FULL deviances

null_rd = '2017_05_20_19_15_09'
full_rd = '2017_05_20_09_44_46' # can be same date or not

nullfs  <- list.files(path=sprintf('%s/output/%s',sharedir,null_rd),pattern="died_model")
fullfs  <- list.files(path=sprintf('%s/output/%s',sharedir,full_rd),pattern="died_model")

nullfs <- nullfs[grep('0nullmodel',nullfs)]
fullfs <- fullfs[grep('0NA',fullfs)]

res = data.table(expand.grid(reg=Regions,age=1:4),
                 full_deviance = NA,
                 null_deviance = NA,
                 pct_explained = NA)

for(n in nullfs){
  a <- strsplit(gsub('bin','',n),'_')[[1]][4]
  r <- strsplit(gsub('bin','',n),'_')[[1]][5]
  f   <- sprintf('died_model_eb_bin%s_%s_0NA.RData',a,r)

  message(sprintf('loading objects for bin %s and region %s',a,r))
  nm <- load(sprintf('%s/%s',sprintf('%s/output/%s',sharedir,null_rd),n));   nm <- get(nm)
  fm <- load(sprintf('%s/%s',sprintf('%s/output/%s',sharedir,full_rd),f));   fm <- get(fm)

  # get deviance
  res$full_deviance[res$age==a & res$reg==r] = round(fm$dic$deviance.mean,2)
  res$null_deviance[res$age==a & res$reg==r] = round(nm$dic$deviance.mean,2)
}

res[,pct_explained := paste0(round(abs((full_deviance-null_deviance)/full_deviance)*100,2),'%')]
res<-res[order(age,-reg)]
write.csv(res,file=sprintf('%s/output/%s/model_deviances.csv',sharedir,null_rd))
write.csv(res,file=sprintf('%s/output/%s/model_deviances.csv',sharedir,full_rd))





#############################################################
## covariate importances, used in RTR
# if stacking and inla were in two different rds
laststack_rd <- '2017_05_20_01_10_49'
inla_rd      <- '2017_05_20_09_44_46'

# get raw covs
rc <- c('unrakedmss', 'unrakedmsw', 'unrakedmatedu_yrs', 'lights_new', 'access',
          'evi', 'LST_day', 'irrigation', 'total_pop', 'malaria_pfpr',
          'fertility_infill', 'urban_rural')

age = 1
yr  = 2000

res <- data.table()

for(age in 1:4){
  message(age)

  # get inla fit
  load(paste0('<<<< FILEPATH REDACTED >>>>>/',inla_rd,'/died_model_eb_bin',age,'_africa_0NA.RData'))

  # get fit data
  fitdat <- fread(paste0('<<<< FILEPATH REDACTED >>>>>/',laststack_rd,'/died_df_going_into_stacking_bin',age,'_africa_0NA.csv'))

  for(yr in c(2000,2005,2010,2015)){
  message(yr)
    # get stacker OBJECTS
    load(paste0('<<<< FILEPATH REDACTED >>>>>/',laststack_rd,'/stacking_objects_africa_',age,'_0NA_',yr,'.RData'))

    x= get.cov.wts(stacked_model_objects,res_fit,rc,fitdat)
    x$age = age; x$year = yr
    res<-rbind(res,x[row.names(x)=='INLA COMBINED',])
  }
}

write.csv(res,file=paste0(sharedir,'/output/',inla_rd,'/','covariate_importances.csv'))



#############
# plot SBH CBH raw data relationship
  lvl='svy'
  message(lvl)
  aggvarname <-switch(lvl,
                      svy = 'nid',
                      ad1     = "admin_1",
                      ad2     = "admin_2",
                      cluster = "cluster.no")

  # load DHS test dataset
  cbh <- fread('<<<< FILEPATH REDACTED >>>>>/CBH_with_SBH_1998_2016.csv',     stringsAsFactors = FALSE) # new data

# and remove NAs
nrow(cbh)
cbh[,isnabti:=is.na(cbh$birthtointerview_cmc),] #table(cbh$nid,cbh$isnabti)
cbh[,isnacad:=is.na(cbh$child_age_at_death_months),]
cbh <- cbh[!is.na(cbh$child_age_at_death_months), ]
nrow(cbh)

# remove records with no birth to interview time
nrow(cbh)
cbh <- cbh[!is.na(cbh$birthtointerview_cmc), ]
nrow(cbh)

# make unique survey ID, used later
cbh$cluster_id <- cbh$nid

cbh <- cbh[,isAODna:=sum(child_age_at_death_months),by=.(mid,cluster_id)] # dropping whole mother observation when this is dropped because SBH info wont change..
cbh <- cbh[!is.na(isAODna),]

cbh <- cbh[order(cluster_id),]
if(mean(sort(unique(cbh$cluster_id))==unique(cbh$cluster_id))!=1) stop('SORT THE CLUSTER IDS!')

# quickfix - pt wont find decimal months
cbh$child_age_at_death_months=floor(cbh$child_age_at_death_months)

#   tabulate exposures & deaths (CBH)
# tabulate exposures/deaths (takes birth-level CBH data and tabulates to the cluster-age-period or survey-age-period)
# res is returned as a tabulation of exposure months and death events by cluster/survey-age-period
stopifnot(packageVersion('seegMBG')=='0.1.2')
tab <- periodTabulate(age_death = cbh$child_age_at_death_months,
                      birth_int = cbh$birthtointerview_cmc,
                      cluster_id = cbh$cluster_id, # later change this to be by mothers age and parity...
                      windows_lower = c(0, 1,  12, 36, 0),
                      windows_upper = c(0, 11, 35, 59, 59),
                      # 4 x 5 year periods
                      period = 60,
                      nperiod = 4, # the last bin could be so long ago its not useful
                      # IHME's monthly estimation method
                      method = 'monthly',
                      cohorts = 'one',
                      inclusion = 'enter',
                      # monthly mortality rates within each bin
                      mortality = 'monthly',
                      n_cores = 1,
                      verbose = TRUE)
tabsave=tab

sbh_tab <- cbh[,.(ced=sum(ced),ceb=sum(ceb)),by=.(cluster_id)]
tab = merge(sbh_tab,tab,by='cluster_id')
tab$logit_q_cbh= qlogis(tab$died/tab$exposed)
tab$logit_q_sbh= qlogis(tab$ced/tab$ceb)

ages = c('0-1 mo','1-11 mo','12-35 mo', '36-59 mo', '0-59 mo')
pers = c('15-19 years','10-14 years','5-9 years','0-4 years')

tab = data.table(tab)

# by age bin and period, check bivariate relationship
for(a in 1:5) { # 5 age groups
  age=ages[a]
  for(p in 1:4) { # 4 five year periods preceding the survey
    per = pers[p]

    tmp = subset(tab, age_bin==a & period==p)
    tmp <- subset(tmp,logit_q_cbh!=-Inf & logit_q_sbh!=-Inf)
    mod = lm(logit_q_cbh~logit_q_sbh,data=tmp)

    rsq = round(summary(mod)$r.squared,3)
    int = round(summary(mod)$coefficients[1,1],3)
    beta= round(summary(mod)$coefficients[2,1],3)

    ann = data.frame(
       xpos = c(-Inf),
       ypos =  c(Inf),
       annotateText = paste0("beta=",beta,'\n','R-sq=',rsq),
       hjustvar = c(-.1) ,
       vjustvar = c(1.25))


    assign(paste0('g',a,p),
      ggplot(tmp, aes(x=logit_q_sbh,y=logit_q_cbh))+
          geom_point()+
          geom_abline(slope=beta,intercept=int)+
          xlab(ifelse(a==5,'logit(CD/CEB)',""))+
          ylab(ifelse(p==4,sprintf('logit(%s probability)',age),""))+
          ggtitle(ifelse(a==1,sprintf('%s before the survey',per),""))+
          geom_text(data=ann,aes(x=xpos,y=ypos,hjust=hjustvar,vjust=vjustvar,
                  label=annotateText),size=6) +
          theme_bw()
    )

  }
}


pdf('<<<< FILEPATH REDACTED >>>>>/test2.pdf',width=16,height=20)
grid.arrange( g14,g13,g12,g11,
              g24,g23,g22,g21,
              g34,g33,g32,g31,
              g44,g43,g42,g41,
              g54,g53,g52,g51, nrow=5,ncol=4)
dev.off()











#################################################################################
# Pull estimates from other sources and compare
# GAVI FCE countries
# GBD at national level
# GBD Subnational estimates (Kenya, South Africa)

figdir=paste0(sharedir,'/output/',run_date)
dir.create(figdir); dir.create(sprintf('%s/GAVI',figdir))


# load my own predictions - get
# need to save raked draws.. for now just use raked mean rasters
r=brick(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/died_mean_child_raked_stack.tif',run_date))
lowr=brick(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/died_lower_child_raked_stack.tif',run_date))
upr=brick(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/died_upper_child_raked_stack.tif',run_date))

nr=brick(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/died_mean_neonatal_raked_stack.tif',run_date))
load(sprintf('%s/input/africa_simple_raster_list.RData',sharedir))
p=crop(raster_list[['pop_raster']],r)

ur <- brick(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/died_mean_child_unraked_stack.tif',run_date))
unr <- brick(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/died_mean_neonatal_unraked_stack.tif',run_date))


####################### Make a quick map
message('MAKING QUICK PLOTS')
pdf(sprintf('%s/child_raked_mean.pdf',figdir),width=8,height=8)
breaks <- c(0, 25,
              26:50,
              51:200,
              1000)
  col.f1 <- colorRampPalette(c("#e58bba", "#f2e8b5"))
  col.f2 <- colorRampPalette(c("#f2e8b5", "#ed152e"))
  col   <- c("#74039E",
             col.f1(25),
             col.f2(150),
             "#ED152E")
rr=r
values(rr)=as.vector(rr)*1000

raster::plot(rr,breaks=breaks,col=col,maxpixels=length(rr))
dev.off()

pdf(sprintf('%s/neonatal_raked_mean.pdf',figdir),width=8,height=8)
breaks <- c(0, 12,
              13:25,
              26:100,
              1000)
  col.f1 <- colorRampPalette(c("#e58bba", "#f2e8b5"))
  col.f2 <- colorRampPalette(c("#f2e8b5", "#ed152e"))
  col   <- c("#74039E",
             col.f1(13),
             col.f2(75),
             "#ED152E")
rr=nr
values(rr)=as.vector(rr)*1000

plot(rr,breaks=breaks,col=col,maxpixels=length(rr))
dev.off()
pdf(sprintf('%s/child_unraked_mean.pdf',figdir),width=8,height=8)
breaks <- c(0, 25,
              26:50,
              51:200,
              1000)
  col.f1 <- colorRampPalette(c("#e58bba", "#f2e8b5"))
  col.f2 <- colorRampPalette(c("#f2e8b5", "#ed152e"))
  col   <- c("#74039E",
             col.f1(25),
             col.f2(150),
             "#ED152E")
rr=ur
values(rr)=as.vector(rr)*1000

raster::plot(rr,breaks=breaks,col=col,maxpixels=length(rr))
dev.off()

pdf(sprintf('%s/neonatal_unraked_mean.pdf',figdir),width=8,height=8)
breaks <- c(0, 12,
              13:25,
              26:100,
              1000)
  col.f1 <- colorRampPalette(c("#e58bba", "#f2e8b5"))
  col.f2 <- colorRampPalette(c("#f2e8b5", "#ed152e"))
  col   <- c("#74039E",
             col.f1(13),
             col.f2(75),
             "#ED152E")
rr=unr
values(rr)=as.vector(rr)*1000

plot(rr,breaks=breaks,col=col,maxpixels=length(rr))
dev.off()


#### DO RAKED UPPER AND LOWER FOR EACH YEAR AND group
## IE EACH ONE WILL BE 4x3.
pdf(sprintf('%s/child_ci_mean.pdf',figdir),width=8,height=8)
breaks <- c(0, 25,
              26:50,
              51:200,
              1000)
  col.f1 <- colorRampPalette(c("#e58bba", "#f2e8b5"))
  col.f2 <- colorRampPalette(c("#f2e8b5", "#ed152e"))
  col   <- c("#74039E",
             col.f1(25),
             col.f2(150),
             "#ED152E")
rr=r;lowrr=lowr;uprr=upr
values(rr)=as.vector(rr)*1000
values(lowrr)=as.vector(lowrr)*1000
values(uprr)=as.vector(uprr)*1000

par(mfrow=c(4,3),mar = c(0.1, 0.1, 0.1, 0.1))
for(i in 1:4){
plot(rr[[i]],breaks=breaks,col=col,maxpixels=length(rr)/4,bty ="n",axes=F,legend=F)
plot(lowrr[[i]],breaks=breaks,col=col,maxpixels=length(rr)/4,bty ="n",axes=F,legend=F)
plot(uprr[[i]],breaks=breaks,col=col,maxpixels=length(rr)/4,bty ="n",axes=F,legend=F)
}
dev.off()





#################################################################################
#################################################################################
### GBD Estimates Calibration Plots
#################################################################################
datpath = sprintf('<<<< FILEPATH REDACTED >>>>>/%s',run_date)

# get GBD Data
  gbd  <- load_u5m_gbd(gaul_list = get_gaul_codes('africa'), getci = TRUE, age_group='child')
  gbd2 <- load_u5m_gbd(gaul_list = get_gaul_codes('africa'), getci = TRUE, age_group='neonatal')
  gbd$group = "child"; gbd2$group = 'neonatal'
  gbd = rbind(gbd,gbd2)
  gbd <- rename(gbd, c('mean' ='gbd_mean',
                       'lower'='gbd_lower',
                       'upper'='gbd_upper'))
  gbd$name = as.numeric(as.character(gbd$name))
  gbd$year = as.numeric(as.character(gbd$year))

  # get Adm0 Estimates (raw) from MBG
  geo  <- fread(sprintf('%s/died_child_adm0_geo.csv',datpath))
  geo2 <- fread(sprintf('%s/died_neonatal_adm0_geo.csv',datpath))
  geo$group = "child"; geo2$group = 'neonatal'
  geo = rbind(geo,geo2)
  geo <- rename(geo, c('mean' ='geo_mean',
                       'lower'='geo_lower',
                       'upper'='geo_upper'))
  geo$name = as.numeric(as.character(geo$name))
  geo$year = as.numeric(as.character(geo$year))

  # merge them
  m <- merge(gbd,geo,by=c('name','year','group'))

  # get country names
  gaul_to_loc_id <- fread("<<<< FILEPATH REDACTED >>>>>/gaul_to_loc_id.csv")
  m <- merge(m,gaul_to_loc_id,by.x='name',by.y='GAUL_CODE')
  m <- m[!m$loc_nm_sh %in% c('Libya','Algeria','Tunisia'),]

  # per 1000 livebirths
  sd.cols <- c("gbd_mean","gbd_lower", "gbd_upper", "geo_mean", "geo_lower", "geo_upper")
  ids     <- c('year','ihme_lc_id','group','loc_name')
  m       <- m[,lapply(.SD,function(x){x*1000}),.SDcols = sd.cols,by=ids]

  # remove countries we do not report
  m <- subset(m,!ihme_lc_id %in% c('STP','COM','CPV'))

  # plot with error bars
  require(ggrepel)
  pdf(sprintf('%s/gbd_geo_adm0_compare.pdf',figdir),width=8,height=8)
  for(g in c('child','neonatal')){
    tmp = subset(m,group==g)
    gg=ggplot(tmp,aes(y=gbd_mean,x=geo_mean))+
      geom_abline(intercept=0,slope=1,colour='red')+
      geom_point(size=1,col='red')+
      geom_errorbarh(aes(xmax = geo_lower, xmin = geo_upper),alpha=0.5,height=0)+
      geom_errorbar( aes(ymax = gbd_lower, ymin = gbd_upper),alpha=0.5,width=0)+
      theme_bw()+
      ylab('GBD 2016 Estimate (per 1,000 livebirths)') +
      xlab('Geospatial Estimate (per 1,000 livebirths)')+
      facet_wrap(~year,ncol=2)+
      scale_x_continuous(limits = c(0, max(tmp$geo_upper)))+
      scale_y_continuous(limits = c(0, max(tmp$gbd_upper)))+
      theme(strip.background = element_rect(fill="white"))+
      ggtitle(g)
    plot(gg)
    gg=ggplot(tmp,aes(y=gbd_mean,x=geo_mean,label=loc_name))+
      geom_abline(intercept=0,slope=1,colour='red')+
      geom_point(size=1.5,col='black')+
      geom_text_repel(size=1.5,force=0.05, col = 'gray', segment.color = 'gray', segment.size = 0.1)+
      theme_bw()+
      ylab('GBD 2016 Estimate (per 1,000 livebirths)') +
      xlab('Geospatial Estimate (per 1,000 livebirths)')+
      facet_wrap(~year,ncol=2)+
      scale_x_continuous(limits = c(0, max(tmp$geo_upper)))+
      scale_y_continuous(limits = c(0, max(tmp$gbd_upper)))+
      theme(strip.background = element_rect(fill="white"))+
      ggtitle(g)
     plot(gg)
  }
  dev.off()



## GBD ZAF AND KEN SUBNATIONALS COMPARISON
message('comparing with gbd subnational estimates for ken and zaf')


# load GBD DATA and match geographies
library(data.table)
  gbd <- fread(paste0(ifelse(Sys.info()['sysname']=="Windows",'J:','/home/j'),"<<<< FILEPATH REDACTED >>>>>/as_withshock.csv"))

locs = fread("<<<< FILEPATH REDACTED >>>>>/gaul_to_loc_id.csv")
gbd = gbd[c(grep("KEN_",gbd$ihme_loc_id),grep("ZAF_",gbd$ihme_loc_id) ) ,]
gbd = gbd[year_id %in% c(2000,2005,2010,2015) & sex_id == 3 ,]
gbd = merge(gbd,locs,by.x='ihme_loc_id',by.y='ihme_lc_id',all.x=TRUE)

# pull in geo data
ad1 <- shapefile('<<<< FILEPATH REDACTED >>>>>/g2015_2014_1.shp') # ZAF
ad2 <- shapefile('<<<< FILEPATH REDACTED >>>>>/admin2013_2.shp') # KEN

# subset shapes
zafshp <- ad1[ad1$ADM0_NAME=="South Africa",]
kenshp <- ad2[ad2$COUNTRY_ID =="KEN",]

# make it so we can merge GBD names with shapefiles
# use variable easyname for merging
gbd[,easyname   := tolower(gsub(' ', '', loc_name))]
zafshp$easyname  = tolower(gsub(' ', '', zafshp$ADM1_NAME))
kenshp$easyname  = tolower(gsub(' ', '', kenshp$NAME))
gbd$easyname[gbd$easyname=='elgeyo-marakwet']='elgeyomarakwet'
gbd$easyname[gbd$easyname=="murang’a"]='muranga'
gbd$easyname[gbd$easyname=="tharakanithi"]='tharaka'
gbd$easyname[gbd$easyname=="kilifi"]='malindi'
gbd$easyname[gbd$easyname=="north-west"]='northwest'

#### make geo estimates for these levels
# load data
r=brick(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/died_mean_child_raked_stack.tif',run_date))
load(sprintf('%s/input/africa_simple_raster_list.RData',sharedir))
p=crop(raster_list[['pop_raster']],r)

zafr  =rasterize(zafshp,p)
kenr  =rasterize(kenshp,p)
zafr_attributes <- data.table(levels(zafr)[[1]])[,c('easyname','ID'),with=F]
kenr_attributes <- data.table(levels(kenr)[[1]])[,c('easyname','ID'),with=F]

years=c(2000,2005,2010,2015)
getgeomean <- function(ad2){
  p[is.na(p)]=0
  ad2=raster::mask(ad2,p)
  ad2_cell <- as.vector(extract(ad2, cellIdx(p)))
  ad2_cell = paste(ad2_cell,rep(years,each=length(ad2_cell)/4),sep='_')
  pop_cell = as.vector(extract(p,cellIdx(p)))
  pop_cell = round(pop_cell,1)
  pop_totals_ad2 <- tapply(pop_cell, ad2_cell, sum)
  pop_totals_ad2_cell <- pop_totals_ad2[match(ad2_cell,names(pop_totals_ad2))]
  pop_wt <- pop_cell / pop_totals_ad2_cell

  r2 <- as.vector(extract(r, cellIdx(p)))
  r2 <- r2*pop_wt
  geo_mean <- tapply(r2, ad2_cell, sum, na.rm=TRUE)

  return(geo_mean)
}

zafmean = getgeomean(zafr)
kenmean = getgeomean(kenr)

# merge geo and gbd
gbd = merge(gbd,kenr_attributes,by='easyname',all.x=T)
gbd = merge(gbd,zafr_attributes,by='easyname',all.x=T)
gbd$ID = gbd$ID.x; gbd$ID[is.na(gbd$ID)]=gbd$ID.y[is.na(gbd$ID)]
gbd[,id:=paste0(ID,'_',year_id)]
zgbd = subset(gbd,parent_id==196)
kgbd = subset(gbd,parent_id==180)

zgbd$geo_mean<-zafmean[match(zgbd$id,names(zafmean))]
kgbd$geo_mean<-kenmean[match(kgbd$id,names(kenmean))]

res <- rbind(zgbd,kgbd)

res$country = 'Kenya'; res$country[res$parent_id==196] = 'South Africa'

res = subset(res,age_group_id==1)
write.csv(res,sprintf('<<<< FILEPATH REDACTED >>>>>/%s/zafkencompare.csv',run_date))

require(ggplot2); library(ggrepel)
pdf(sprintf('%s/output/%s/GBD_ken_zaf_scatter_compare.pdf',sharedir,run_date))
for(c in c('Kenya','South Africa')){
  tmp = subset(res,country==c)
  gg=ggplot(tmp,aes(y=qx_mean*1000,x=geo_mean*1000,label=loc_name))+
        geom_abline(intercept=0,slope=1,colour='red')+
        geom_point(size=1.5,col='black')+
        geom_text_repel(size=1.5,force=0.05, col = 'gray', segment.color = 'gray', segment.size = 0.1) +
        theme_bw()+
        ylab('GBD Subnational Estimate (per 1,000 livebirths)') +
        xlab('Geospatial Estimate (per 1,000 livebirths)')+
        facet_wrap(~year_id,ncol=2)+
        scale_x_continuous(limits = c(0, max(tmp$geo_mean*1000)))+
        scale_y_continuous(limits = c(0, max(tmp$qx_upper*1000)))+
        theme(strip.background = element_rect(fill="white"))+
        ggtitle(c)
  plot(gg)
}
dev.off()




#########################################################
#########################################################
# GAVI comparison
# set stuff
countries=c('moz','zmb','uga','tcd','cmr')
gauls = c(zmb = 270, uga = 253, moz = 170, tcd = 50, cmr = 45)
years= c(2000,2005,2010,2015)



# load laura predictions and district shapefiles
predlist=list();shplist=list();rlist=list()
for(c in countries){
  message(c)
  preds=fread(sprintf('<<<< FILEPATH REDACTED >>>>>/%s/03_multilevel_models/q5_preds.csv',c)) # load data
  preds=preds[c(grep('dist',preds$level),grep('dept',preds$level)),]    # keep district level
  preds=subset(preds,t%in%years)            # keep only years I estimate for
  preds$iso3     = c                        # make and iso variable
  preds$ad0_gaul = gauls[names(gauls)==c]   # make a gaul variable
  predlist[[length(predlist)+1]]=preds

  assign(sprintf('%s_preds',c),preds)
  # grab shapefile associated with these sae estimates
  inputs=read.csv((sprintf('<<<< FILEPATH REDACTED >>>>>/%s/03_multilevel_models/_final/2017_paper/model_inputs.csv',c)),
                  col.names=c("thing",'location'))
  tmp=load(paste0('/home/j/',eval(parse(text=as.character(inputs$location[inputs$thing=='shape_files'])))[1]))
  tmp = get(tmp)
  lvl = as.character(unique(preds$level))
  tmp@data$idvar=as.numeric(paste0(tmp@data[,get(lvl)],gauls[names(gauls)==c]))
  shplist[[c]]=tmp #shp, plus matching varname
  rlist[[c]]  =rasterize(tmp,p,field='idvar')

  rm(list=c('preds','inputs','distmap'))
}
sae=do.call(rbind,predlist)

# get weighted mean across all areas defined in sae dataset (do in matrices not rasters)
ad2=do.call(raster::merge,unname(rlist))
p[is.na(p)]=0
ad2=raster::mask(ad2,p)
ad2_cell <- as.vector(extract(ad2, cellIdx(p)))
ad2_cell = paste(ad2_cell,rep(years,each=length(ad2_cell)/4),sep='_')
p[is.na(ad2_cell)]=0
pop_cell = as.vector(extract(p,cellIdx(p)))
pop_cell = round(pop_cell,1)
pop_totals_ad2 <- tapply(pop_cell, ad2_cell, sum)
pop_totals_ad2_cell <- pop_totals_ad2[match(ad2_cell,names(pop_totals_ad2))]
pop_wt <- pop_cell / pop_totals_ad2_cell

r2 <- as.vector(extract(r, cellIdx(p)))
r2 <- r2*pop_wt
geo_mean <- tapply(r2, ad2_cell, sum, na.rm=TRUE)

# map means back to sae
sae[,id:=paste0(area,ad0_gaul,'_',t),]
sae$geo_mean<-geo_mean[match(sae$id,names(geo_mean))]
sae$sae_mean<-sae$mean

# several basic plots to make:
# 1. scatter
require(ggplot2); require(ggrepel)
pdf(sprintf('%s/GAVI_scatter_compare.pdf',figdir))
for(c in countries){
  tmp = subset(sae,iso3==c)
  gg=ggplot(tmp,aes(y=mean*1000,x=geo_mean*1000,label=area_name))+
        geom_abline(intercept=0,slope=1,colour='red')+
        geom_point(size=1.5,col='black')+
        theme_bw()+
        ylab('GAVI FCE Estimate (per 1,000 livebirths)') +
        xlab('Geospatial Estimate (per 1,000 livebirths)')+
        facet_wrap(~t,ncol=2)+
        scale_x_continuous(limits = c(0, max(tmp$geo_mean*1000)))+
        scale_y_continuous(limits = c(0, max(tmp$ub*1000)))+
        theme(strip.background = element_rect(fill="white"))+
        ggtitle(c)
  plot(gg)
}
dev.off()
















#################################################
#################################################
### CBH SBH validation
# Bring in data to fit at survey level, see if holds at admin 1 and 2
svy<-fread('<<<< FILEPATH REDACTED >>>>>/prepped_cbh_sbh_svy.csv')
adm1<-data.table(read.csv('<<<< FILEPATH REDACTED >>>>>/prepped_cbh_sbh_ad1.csv'))

svy$level='svy'
adm1$level='adm1'

# add regions
regs <- fread("<<<< FILEPATH REDACTED >>>>>/gaul_to_loc_id.csv")
regs$level=NULL
svy <- svy[, ihme_lc_id := as.character(country)]
svy <- merge(svy,regs,by='ihme_lc_id',all.x=T)

### clean up a few things
# drop unneeded age bin 5
adm1$died <- as.numeric(as.character(adm1$died))

adm1[,exposed:=as.numeric(as.character(exposed))]
adm1[,yrlag:=as.numeric(as.character(yrlag))]

svy  <- subset(svy ,age_bin!=5)
adm1 <- subset(adm1,age_bin!=5)

svy  <- subset(svy ,exposed!=0)
adm1 <- subset(adm1,exposed!=0)

svy [,id      := paste0(id,level)]
adm1[,id      := paste0(id,level)]

svy[,age_bin  := factor(age_bin)]
svy[,lag      := factor(lag)]

adm1[,age_bin := factor(age_bin)]
adm1[,lag     := factor(lag)]
adm1[,lceb    := log(ceb)]
adm1[,lced    := log(ced)]
adm1[,lexposed:= log(exposed)]

adm1$yrsinper <- adm1$yrlag+2
adm1$yrsinper[adm1$yrsinper>5] <- 5

svy$yrsinper <- svy$yrlag+2
svy$yrsinper[svy$yrsinper>5] <- 5

# 5 folds by survey
set.seed(1)
svys  <- unique(svy$nid)
f     <- sample(rep_len(1:5, length.out=length(svys)))
folds <- data.table(nid=svys,fold=f)
svy   <- merge(svy, folds,by='nid',all.x=T)
adm1  <- merge(adm1,folds,by='nid',all.x=T)

# make blank cv pred slot
svy[,pred := NA]
adm1[,pred := NA]

svy [,died_orig := died]
adm1[,lexposed_orig    := lexposed]

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Train p_hat models on survey level data
if(grepl("geos", Sys.info()[4])) INLA:::inla.dynload.workaround()

## First model, for p_hat
f_resp <- died ~ 1
f_sbh_fixed  <- ~ logit_sbh +
                  factor(lag) + factor(age_bin) +
                  logit_sbh:factor(lag) + logit_sbh:factor(age_bin) + logit_sbh:factor(lag):factor(age_bin) + factor(age_bin):factor(lag) +
                  mean_matage + period + propmothers15_19 + propmothers20_24 +
                  propmothers25_39  + pr1 + pr2 + yrsinper +
                  factor(lag):mean_matage
# include cohort specific logit_sbh?
f_random_intercepts <- ~  f(factor(country),model='iid') + f(factor(cluster_id),model='iid') #cid is same as nid

f_formula <- f_resp + f_sbh_fixed + f_random_intercepts

# fit the model 5 times and save prediction
for(f in 1:5){
  message(paste0('Fold ',f))
  svy$died <- svy$died_orig
  svy$died[svy$fold==f] <- NA
  bm <- inla(f_formula,
              data=svy,
              Ntrials=svy$exposed,
              family = 'binomial',
              num.threads = 1,
              control.inla = list(int.strategy = 'ccd', h = 1e-3, tolerance = 1e-6),
              verbose=FALSE,
              control.predictor=list(link=1),control.compute=list(config = TRUE))
  svy$died <- svy$died_orig
  svy$pred[svy$fold==f] <- bm$summary.linear.predictor$mean[svy$fold==f]
}

## FULL TO SAVE results
bm <- inla(f_formula,
            data=svy,
            Ntrials=svy$exposed,
            family = 'binomial',
            num.threads = 1,
            control.inla = list(int.strategy = 'ccd', h = 1e-3, tolerance = 1e-6),
            verbose=FALSE,
            control.predictor=list(link=1),control.compute=list(config = TRUE))
res <-rbind(summary(bm)$fixed[,1:6],summary(bm)$hyperpar)
res$name = row.names(res)
fwrite(res,file='<<<< FILEPATH REDACTED >>>>>/p_model_fit.csv')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Train N_hat model

f_resp   <- exposed ~ 1
f_fixed  <- ~  lceb + factor(lag) + factor(age_bin) +
                   lceb:yrsinper + lceb:factor(age_bin) + lceb:yrsinper:factor(age_bin) +
                  mean_matage + period + propmothers15_19 + propmothers20_24 +
                  propmothers25_39  +  pr1 + pr2 + yrsinper + lced +
                  yrsinper:factor(age_bin)+
                  factor(lag):mean_matage

f_random_intercepts <- ~  f(factor(country) ,model='iid') + f(factor(id),model='iid') + f(factor(nid),model='iid')

f_formula <- f_resp + f_fixed  + f_random_intercepts

# fit the model 5 times and save prediction
for(f in 1:5){
  message(paste0('Fold ',f))
  adm1$lexposed <- adm1$lexposed_orig
  adm1$lexposed[adm1$fold==f] <- NA
  nm <- inla(f_formula,
             data=adm1,
             family = 'poisson',
             num.threads = 1,
             control.inla = list(int.strategy = 'ccd', h = 1e-3, tolerance = 1e-6),
             verbose=F,
             control.predictor=list(link=1),control.compute=list(config=TRUE))
  adm1$lexposed <- adm1$lexposed_orig
  adm1$pred[adm1$fold==f] <- nm$summary.linear.predictor$mean[adm1$fold==f]
}

## FULL TO SAVE results
nm <- inla(f_formula,
           data=adm1,
           family = 'poisson',
           num.threads = 1,
           control.inla = list(int.strategy = 'ccd', h = 1e-3, tolerance = 1e-6),
           verbose=F,
           control.predictor=list(link=1),control.compute=list(config=TRUE))
res <-rbind(summary(nm)$fixed[,1:6],summary(nm)$hyperpar)
res$name = row.names(res)
fwrite(res,file='<<<< FILEPATH REDACTED >>>>>/N_model_fit.csv')
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# RMSE and ME
adm1[,exp_pred := exp(pred)]
svy [,exp_pred := plogis(pred)]

# N HAT
sqrt(mean((adm1$exp_pred-adm1$exposed)^2))
mean(adm1$exp_pred-adm1$exposed)
mean(adm1$exposed)

sqrt(mean((adm1$pred-adm1$lexposed)^2))
mean(adm1$pred-adm1$lexposed)
mean(adm1$lexposed)


adm1resexp=adm1[, .( rmse=  sqrt(mean((exp_pred-exposed)^2)),
          me = mean(exp_pred-exposed,na.rm=T),
          meanpred=  mean(exp_pred,na.rm=T),
          meanobs=  mean(exposed,na.rm=T) ),
        by = .(period,age_bin)]
adm1resexp

adm1reslog <-adm1[, .( rmse=  sqrt(mean((pred-lexposed)^2)),
          me = mean(pred-lexposed,na.rm=T),
          meanpred=  mean(pred,na.rm=T),
          meanobs=  mean(lexposed,na.rm=T)  ),
        by = .(age_bin,period)]
adm1reslog


# P HAT
sqrt(mean((svy$exp_pred-svy$died/svy$exposed)^2))
mean(svy$exp_pred-svy$died/svy$exposed)
mean(svy$died/svy$exposed)

sqrt(mean((svy$pred[svy$direct!=-Inf]-svy$direct[svy$direct!=-Inf])^2))
mean(svy$pred[svy$direct!=-Inf]-svy$direct[svy$direct!=-Inf],na.rm=T)
mean(svy$direct[svy$direct!=-Inf],na.rm=T)


svyresplogis=svy[, .( rmse=  sqrt(mean((exp_pred-died/exposed)^2)),
          me = mean(exp_pred-died/exposed,na.rm=T),
          meanpred=  mean(exp_pred,na.rm=T),
          meanobs=  mean(died/exposed,na.rm=T) ),
        by = .(period,age_bin)]
svyresplogis


fwrite(adm1resexp,file='<<<< FILEPATH REDACTED >>>>>/N_res_exp.csv')
fwrite(svyresplogis,file='<<<< FILEPATH REDACTED >>>>>/P_res_plogis.csv')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Plot OOS predictions

pdf('<<<< FILEPATH REDACTED >>>>>/oossamplefit_phat2.pdf')
ggplot(svy,aes(x=pred,y=direct,colour=factor(age_bin)))+
    geom_point()+
    theme_bw()+ylab('Direct CBH')+xlab('Indirect SBH estimate')+
    guides(colour=guide_legend(title="Age Bin"))+
    geom_abline(slope=1,intercept=0,colour='red')+ylim(c(-9.8,-1))
dev.off()

# test plot and effss
pdf('<<<< FILEPATH REDACTED >>>>>/oossamplefit_Nhat2.pdf')
ggplot(adm1,aes(x=pred,y=lexposed,colour=factor(age_bin)))+
    geom_point()+
    theme_bw()+ylab('Direct CBH')+xlab('SBH estimate')+
    guides(colour=guide_legend(title="Age Bin"))+
    geom_abline(slope=1,intercept=0,colour='red')
dev.off()











###########################################################
###########################################################
### PULL PARAM ESTIMATES FROM MODEL FIT OBJECTS


 message("::::loading in pre-INLA objects to get spde")

  holdout = 0
  rr = 'africa'
  ind_gp = 'u5m'
  ind = 'died'
  rd = run_date


for(age in 1:4){
  pathaddin  <-  paste0('_bin',age,'_',rr,'_',holdout)

  load(paste0('<<<< FILEPATH REDACTED >>>>>.RData'))

    modnames = child_model_names = c('gam','gbm','ridge','lasso')

    all_fixed_effects = "gam + gbm + ridge + lasso"


    ## for stacking, overwrite the columns matching the model_names so
    ## that we can trick inla into being our stacker
    df = df[,paste0(child_model_names) := lapply(child_model_names,
                                                 function(x) get(paste0(x,'_cv_pred')))]

    ## Create SPDE INLA stack
    if(grepl("geos", Sys.info()[4])) INLA:::inla.dynload.workaround()
    input_data <- build_mbg_data_stack(df = df,
                                       fixed_effects = all_fixed_effects,
                                       mesh_s = mesh_s,
                                       mesh_t = mesh_t,
                                       use_ctry_res = FALSE,
                                       use_nugget = FALSE,
                                       exclude_cs    = c(modnames),
                                       usematernnew=F)

    spde <- input_data[[2]]
    ## this is what we neede!

    message('::::loading in INLA fit\n')
    load(paste0('<<<< FILEPATH REDACTED >>>>>.RData'))
    res_fit = x



    ## now we extract what we need from the fit to get transformed spatial params
    res.field <- inla.spde2.result(res_fit, 'space', spde, do.transf=TRUE)

    ## nominal range at 0.025, 0.5, 0.975 quantiles
    range   <- inla.qmarginal(c(0.025, 0.5, 0.975), res.field$marginals.range.nominal[[1]])
    nom.var <- inla.qmarginal(c(0.025, 0.5, 0.975), res.field$marginals.variance.nominal[[1]])
    spat.hyps <- rbind(range, nom.var)
    rownames(spat.hyps) <- c('Nominal Range', 'Nominal Variance')

    ## other hyperparmas
    hyps <- summary(res_fit)$hyperpar[-(1:2), ] ## first two rows are
                                                ## theta1, theta2 which
                                                ## we have in range and
                                                ## nom.var

    colnames(spat.hyps) <- colnames(hyps)[3:5]
    ## fixed effects
    fixed <- summary(res_fit)$fixed[,1:6]

    ## combine them all and just keep three quantiles

    all.res <- rbind(fixed[, 3:5],
                     spat.hyps,
                     hyps[, 3:5])

write.csv(all.res,paste0('<<<< FILEPATH REDACTED >>>>>.csv'))
}
