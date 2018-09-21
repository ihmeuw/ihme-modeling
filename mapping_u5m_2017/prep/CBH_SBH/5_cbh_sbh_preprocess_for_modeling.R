# Make crosswalking data from the CBH - SBH data


for(lvl in c('svy','ad1')){
  message(lvl)
  aggvarname <-switch(lvl,
                      svy = 'nid',
                      ad1     = "admin_1",
                      ad2     = "admin_2",
                      cluster = "cluster.no")

  # load DHS test dataset
  data <- fread('<<<< FILEPATH REDACTED >>>>>/CBH_with_SBH_1998_2016.csv',     stringsAsFactors = FALSE) # new data

  # drop two outlier datasets
  data=data[!nid%in%c(21173,672),]

  data$year = as.numeric(data$year)
  data=data[data$year>=1998,]

  # Name for aggregating
  data[,cluster_id:=paste(get(aggvarname),nid,country),]

  # define periods
  period_end <- as.Date('2017-12-01')
  periods <- data.frame(period = 1:4,
                        year = c(2015, 2010, 2005, 2000))

  # define numbers of periods and age bins
  n_period <- nrow(periods)
  width <- 60
  n_age <- 4


  ######################################################################
  ## Get survey-wise parity ratios

  first <- function (x) x[1]
  pr    <- data[,.(ceb=first(ceb),
                mage=first(mothers_age_group)),
                by=.(mid,nid)]
  pr   <- pr[,.(parity=mean(ceb)),by=.(nid,mage)]
  pr1  <- pr[mage=='15-19',]
  pr2  <- pr[mage=='20-24',]
  pr3  <- pr[mage=='25-29',]
  prA  <- merge(pr1,pr2,by='nid')
  prA  <- merge(prA,pr3,by='nid')
  prA  <- prA[,.(pr1=parity.x/parity.y, #p(15-19)/p(20-24)
                 pr2=parity.y/parity),  #p(20-24)/p(25-29)
                by=nid]
  data <- merge(data,prA,by='nid',all.x=TRUE)

  ######################################################################

  ######################################################################
  ##  get proportions of mothers by ages for each cluster
  pr    <- data[,.(ceb=first(ceb),
                      mage=first(mothers_age_group)),
                     by=.(mid,cluster_id)]
  pr <- pr[ceb>0,] # only interested in mothers
  pr <- pr[,.(mothers = sum(.N)),by=.(cluster_id,mage)]
  pr <- pr[,totmothers := sum(mothers,na.rm=T),by=cluster_id]
  pr1  <- pr[mage=='15-19',]
  pr2  <- pr[mage=='20-24',]
  pr3  <- pr[mage%in%c('25-29','30-34','35-39'),]
  pr3  <- pr3[,.(mothers=sum(mothers,na.rm=T)),by=.(cluster_id,totmothers)]
  pr4  <- pr[mage%in%c('40-44','45-49'),]
  pr4  <- pr4[,.(mothers=sum(mothers,na.rm=T)),by=.(cluster_id,totmothers)]

  prt  <- pr[,.(totmothers=first(totmothers)),by=cluster_id]
  pr1  <- pr1[,.(propmothers15_19=mothers/totmothers),by=.(cluster_id)]
  pr2  <- pr2[,.(propmothers20_24=mothers/totmothers),by=.(cluster_id)]
  pr3  <- pr3[,.(propmothers25_39=mothers/totmothers),by=.(cluster_id)]
  pr4  <- pr4[,.(propmothers40_49=mothers/totmothers),by=.(cluster_id)]
  prA  <- merge(pr1,pr2,by='cluster_id',all=TRUE)
  prA  <- merge(prA,pr3,by='cluster_id',all=TRUE)
  prA  <- merge(prA,pr4,by='cluster_id',all=TRUE)
  prA  <- merge(prA,prt,by='cluster_id',all=TRUE)

  data <- merge(data,prA,by='cluster_id',all.x=TRUE)
  data$propmothers15_19[is.na(data$propmothers15_19)]=0
  data$propmothers20_24[is.na(data$propmothers20_24)]=0
  data$propmothers25_39[is.na(data$propmothers25_39)]=0
  data$propmothers40_49[is.na(data$propmothers40_49)]=0

  ######################################################################

  ######################################################################
  ##  get proportions of WOMEN by ages for each cluster

  pr   <- copy(data)
  age  <- pr[,.(mean_womage = mean(mothers_age,na.rm=T)),by=.(cluster_id)]
  pr   <- pr[,.(mothers_age_group=first(mothers_age_group)),by=.(mid,cluster_id)]
  pr   <- pr[,.(women = sum(.N)),by=.(cluster_id,mothers_age_group)]
  pr   <- pr[,totwomen := sum(women),by=cluster_id]
  pr1  <- pr[mothers_age_group=='15-19',]
  pr2  <- pr[mothers_age_group=='20-24',]
  pr3  <- pr[mothers_age_group%in%c('25-29','30-34','35-39'),]
  pr4  <- pr[mothers_age_group%in%c('40-44','45-49'),]
  pr3  <- pr3[,.(women=sum(women)),by=.(cluster_id,totwomen)]
  pr4  <- pr4[,.(women=sum(women)),by=.(cluster_id,totwomen)]

  prt  <- pr[,.(totwomen=first(totwomen)),by=cluster_id]
  pr1  <- pr1[,.(propwomen15_19=women/totwomen),by=.(cluster_id)]
  pr2  <- pr2[,.(propwomen20_24=women/totwomen),by=.(cluster_id)]
  pr3  <- pr3[,.(propwomen25_39=women/totwomen),by=.(cluster_id)]
  pr4  <- pr4[,.(propwomen40_49=women/totwomen),by=.(cluster_id)]
  prA  <- merge(pr1,pr2,by='cluster_id',all=TRUE)
  prA  <- merge(prA,pr3,by='cluster_id',all=TRUE)
  prA  <- merge(prA,pr4,by='cluster_id',all=TRUE)
  prA  <- merge(prA,prt,by='cluster_id',all=TRUE)

  data <- merge(data,prA,by='cluster_id',all.x=TRUE)
  data <- merge(data,age,by='cluster_id',all.x=TRUE)
  data$propwomen15_19[is.na(data$propwomen15_19)]=0
  data$propwomen20_24[is.na(data$propwomen20_24)]=0
  data$propwomen25_39[is.na(data$propwomen25_39)]=0
  data$propwomen40_49[is.na(data$propwomen40_49)]=0

  ###############################################
  # set up CBH for tabulation

  # women with no children are being dropped here
  nrow(data)
  data <- data[!is.na(interview_date_cmc),]
  data <- data[,isAODna:=sum(child_age_at_death_months),by=.(mid,cluster_id)] # dropping whole mother observation if any missingness
  data <- data[!is.na(isAODna),]
  data <- data[!is.na(birthtointerview_cmc),] # all rows that have women with 0 CEB (nonmothers) are now dropped.
  nrow(data)

  # Most surveys ask for AOD in years after 24mo.
  # Add 6 months, If death fell in year of survey then use halfway mark between age and DOS
  data$aod_orig <- data$child_age_at_death_months
  data[,marked := child_age_at_death_months %in% c(24,36,48)]
  data$child_age_at_death_months[data$marked] <- data$child_age_at_death_months[data$marked] + 6
  data[,marked2 := child_age_at_death_months > birthtointerview_cmc & marked]
  data$child_age_at_death_months[data$marked2] <- (data$aod_orig[data$marked2]) +
        round((data$birthtointerview_cmc[data$marked2] - data$aod_orig[data$marked2])/2,0)

  idm  <- cbind(cluster_id=sort(unique(data$cluster_id)),id=1:length(unique(data$cluster_id)))
  data <- merge(data,idm,by='cluster_id')
  data <- data[order(as.numeric(id)),]

  level <- lvl

  message('tabulation')
  message(level)
  set.seed(1)

  # get dataset
  data_tmp <- data.frame(data)

  # get the index for aggregating individuals (vector of cluster or survey id)
  agg_index <- data_tmp$cluster_id

  # confirm a few checks before continuing
  stopifnot(mean(unique(data_tmp$cluster_id)==sort(unique(data_tmp$cluster_id)))==1)
  stopifnot(packageVersion('seegMBG')=='0.1.2')

  # Monthly resolution
  data_tmp$child_age_at_death_months=floor(data_tmp$child_age_at_death_months)


  #   i. tabulate exposures & deaths for survey/cluster and test/train
  # tabulate exposures/deaths (takes birth-level CBH data and tabulates to the cluster-age-period or survey-age-period)
  # res is returned as a tabulation of exposure months and death events by cluster/survey-age-period
  # periods in this case are not relative to survey, but match our analytical periods
  res <- periodTabulate(age_death       = data_tmp$child_age_at_death_months,
                        birth_int       = data_tmp$birthtointerview_cmc,
                        cluster_id      = agg_index,
                            # define windows for tabulation
                        windows_lower   = c(0, 1, 12,  36,0),
                        windows_upper   = c(0, 11, 35, 59,59),
                            # 4 x 5 year periods
                        period          = width,
                        nperiod         = n_period,
                        period_end      = period_end,
                        interview_dates = cmc2Date(data_tmp$interview_date_cmc),
                            # IHME's monthly estimation method
                        method          = 'monthly',
                        cohorts         = 'one',
                        inclusion       = 'enter',
                            # monthly mortality rates within each bin
                        mortality       = 'monthly',
                        n_cores = 1,
                        verbose = TRUE)
                            # note that period 1 is the most recent in this output

  #   ii. tabulate CEB & CED for survey/cluster and test/train
  # aggregate relevant columns by mothers (ie just take first row from each mother)
  first <- function (x) x[1]
  dd = data.table(data_tmp)
  ced_ceb_mother = dd[, .(ceb =    first(ceb),
                          ced =    first(ced),
                          matage = first(mothers_age),
                          agg    = first(cluster_id)),
                         by = mid]
  ced_ceb_mother <- subset(ced_ceb_mother,ceb>0)

  # aggregate mother level data by cluster/survey (agg level)
  ced <- tapply(ced_ceb_mother$ced, ced_ceb_mother$agg, sum)
  ceb <- tapply(ced_ceb_mother$ceb, ced_ceb_mother$agg, sum)

  # match them up
  # idx_* returns as the indexes where cluster_id in res matched that in the mother specfic vectors (length of res, but values in ceb and ced)
  idx_ced <- match(res$cluster_id, names(ced))
  idx_ceb <- match(res$cluster_id, names(ceb))

  # add them to the results
  res$ceb <- ceb[idx_ceb]
  res$ced <- ced[idx_ced]

  # get all other info too
  otherinfo <- dd[, .(pr1    = first(pr1),
                      pr2    = first(pr2),
                      propmothers15_19=first(propmothers15_19),
                      propmothers20_24=first(propmothers20_24),
                      propmothers25_39=first(propmothers25_39),
                      propmothers40_49=first(propmothers40_49),
                      totmothers=first(totmothers),
                      mean_womage=first(mean_womage),
                      propwomen15_19=first(propwomen15_19),
                      propwomen20_24=first(propwomen20_24),
                      propwomen25_39=first(propwomen25_39),
                      propwomen40_49=first(propwomen40_49),
                      totwomen=first(totwomen)),
                        by = .(cluster_id)  ]
  res <- merge(res,otherinfo,by='cluster_id',all=TRUE)

  nrow(res)
  res <- subset(res, !is.na(res$ced))
  nrow(res)

  mean_mat_age <- aggregate(matage~agg,data=ced_ceb_mother,FUN=mean)
  mean_ceb     <- aggregate(ceb   ~agg,data=ced_ceb_mother,FUN=mean)

  res <- merge(res,mean_mat_age,by.x='cluster_id',by.y='agg',all.x=T)
  res <- merge(res,mean_ceb    ,by.x='cluster_id',by.y='agg',all.x=T)
  res$mean_ceb=res$ceb.y
  res$mean_matage=res$matage
  res$ceb=res$ceb.x
  res$ceb.y=res$ceb.x=res$matage=NULL


  years     <- tapply(data_tmp$year,    agg_index, raster::modal)
  countries <- tapply(data_tmp$country, agg_index, raster::modal)

  idx_year <- match(res$cluster_id, names(years))
  idx_ctry <- match(res$cluster_id, names(countries))

  res$survey_year <- as.numeric(years[idx_year])
  res$country     <- countries[idx_ctry]
  res$per_year <- periods$year[match(res$period,periods$period)]

  # add lag, this is the survey year - central year of time period
  res$yrlag <- res$survey_year - res$per_year

  # changing to be in period space
  periods2     <- data.frame(y=2017:1998,p=rep(1:4,each=5))
  res$surv_per <- periods2$p[match(res$survey_year,periods2$y)]
  res$lag      <- res$period - res$surv_per
  res <- subset(res, lag >= 0)

  # save a mapping of exposures to be used later
  ressave2 <- res
  surveys <- unique(data[,c('nid','cluster_id'),with=F])
  res <- merge(res,surveys,by='cluster_id',all.x=TRUE)
  res <- merge(res,idm,by='cluster_id'    ,all.x=TRUE)

  # Fit shrink model to replace zero numerators
  res$ced_orig  = res$ced
  res$died_orig = res$died
  message('Imputing Zeroes')
  res2=data.table()
  for(s in unique(res$nid)){
    message(s)
    tmp = res[res$nid==s,]
      message('sbh')
      sbhmod = inla(ced~1+f(id,model='iid'),
                        family='binomial',
                        data=tmp,
                        Ntrials=ceb,
                        num.threads = getOption('cores'),
                        control.inla = list(int.strategy = 'eb', h = 1e-3, tolerance = 1e-6),
                        control.predictor=list(link=1))
       tmp$ced=sbhmod$summary.fitted.values$mean*tmp$ceb
    res2=rbind(res2,tmp)
  }
  res=data.frame(res2)

  # get the logit transfrom of q from cbh and CED/CEB from sbh
  res$logit_sbh <- qlogis(res$ced/res$ceb)
  res$direct    <- qlogis(res$died/res$exposed)

  # any final hang alongs
  res <- na.omit(res)

  # save
  write.csv(res,file=sprintf('<<<< FILEPATH REDACTED >>>>>/prepped_cbh_sbh_%s_2.csv',lvl))

}
