####
intstrat = 'ccd' # https://www.math.ntnu.no/inla/r-inla.org/papers/CSDAinla_revision.pdf

first <- function (x) x[1] # helpful

########################
# pull data at the individual level
sbh    <- fread(paste0(datadrive,'Africa_SBH_1998_2016_WN','.csv'),stringsAsFactors = FALSE)
sbhagg <- fread(paste0(datadrive,'Africa_SBH_1998_2016_Aggregated','.csv'),stringsAsFactors = FALSE) # confirm we agg the same

# There are two surveys we dont have at the individual level
# The agg only surveys will need to be treated slightly different. we can only get prop_women not prop_mothers
aggonlyinds <- c(14161, 130073)

# Not include at this round
sbh <- subset(sbh, !(nid %in% c(256244, 257045, 257826, 285891, 285893, 285993, 286019, 286020, 286022, 256243, 256201, 256365)))

###################################
# Make an aggregator id
# either nid lat long, or nid shapefile location are ids

# group by rounded (to nearest 10th lat long) for one survey
sbh$latnum [sbh$nid==57990]<-round(sbh$latnum [sbh$nid==57990],1)
sbh$longnum[sbh$nid==57990]<-round(sbh$longnum[sbh$nid==57990],1)

# round lat and long
sbh[,longnum := round(longnum,3)]
sbh[,latnum  := round(latnum,3)]

# make ids
sbh[, unique_combo := paste0(nid,source,country,year,latnum,longnum,shapefile,location_name,location_code)]
ids <- data.table(unique_combo=sort(unique(sbh$unique_combo)),cluster_id=1:length(sort(unique(sbh$unique_combo))))
sbh <- merge(sbh,ids,by='unique_combo',all=TRUE)
sbh$unique_combo <- NULL
sbh[,mid := 1:sum(.N)]

# drop observations where either ceb or ced are missing
sbh <- subset(sbh, !is.na(ceb+ced))

# fix for some surveys, only if full survey is missing weights, dont weight. if only some obs are missing, drop them.
ttmiss   <- table(sbh[is.na(weight)]$nid)
tt       <- table(sbh$nid)[names(table(sbh$nid)) %in% names(ttmiss)]
ttmiss   <- ttmiss[order(names(ttmiss))]
tt       <- tt[order(names(tt))]
nidfull  <- as.numeric(names(tt)[tt==ttmiss]) # full survey missing weight
nidpart  <- as.numeric(names(tt)[tt!=ttmiss]) # some obs missing weight
sbh$weight[is.na(sbh$weight) & sbh$nid %in% nidfull]=1

# There is only one sole observation missing a weight, drop it
sbh <- subset(sbh, !is.na(weight))

######################################################################
## Get survey-wise parity ratios

pr   <- sbh[,.(parity=weighted.mean(ceb,w=weight,na.rm=TRUE)),
             by=.(nid,age_group_of_woman)]
pr1  <- pr[age_group_of_woman=='15-19',]
pr2  <- pr[age_group_of_woman=='20-24',]
pr3  <- pr[age_group_of_woman=='25-29',]
prA  <- merge(pr1,pr2,by='nid')
prA  <- merge(prA,pr3,by='nid')
prA  <- prA[,.(pr1=parity.x/parity.y, #p(15-19)/p(20-24)
               pr2=parity.y/parity),  #p(20-24)/p(25-29)
              by=nid]
sbh <- merge(sbh,prA,by='nid',all.x=TRUE)


############################
# no weighting point data
sbh[,p:=!is.na(latnum)&!is.na(longnum)]
sbh$weight[sbh$p==TRUE]=1

######################################################################
##  get proportions of MOTHERS by ages for each cluster

pr   <- sbh[ceb>0,] # mothers only
pr   <- pr[,.(mothers = sum(weight)),by=.(cluster_id,age_group_of_woman)]
pr   <- pr[,totmothers := sum(mothers),by=cluster_id]
pr1  <- pr[age_group_of_woman=='15-19',]
pr2  <- pr[age_group_of_woman=='20-24',]
pr3  <- pr[age_group_of_woman%in%c('25-29','30-34','35-39'),]
pr4  <- pr[age_group_of_woman%in%c('40-44','45-49'),]
pr3  <- pr3[,.(mothers=sum(mothers)),by=.(cluster_id,totmothers)]
pr4  <- pr4[,.(mothers=sum(mothers)),by=.(cluster_id,totmothers)]

prt  <- pr[,.(totmothers=first(totmothers)),by=cluster_id]
pr1  <- pr1[,.(propmothers15_19=mothers/totmothers),by=.(cluster_id)]
pr2  <- pr2[,.(propmothers20_24=mothers/totmothers),by=.(cluster_id)]
pr3  <- pr3[,.(propmothers25_39=mothers/totmothers),by=.(cluster_id)]
pr4  <- pr4[,.(propmothers40_49=mothers/totmothers),by=.(cluster_id)]
prA  <- merge(pr1,pr2,by='cluster_id',all=TRUE)
prA  <- merge(prA,pr3,by='cluster_id',all=TRUE)
prA  <- merge(prA,pr4,by='cluster_id',all=TRUE)
prA  <- merge(prA,prt,by='cluster_id',all=TRUE)

sbh <- merge(sbh,prA,by='cluster_id',all.x=TRUE)
sbh$propmothers15_19[is.na(sbh$propmothers15_19)]=0
sbh$propmothers20_24[is.na(sbh$propmothers20_24)]=0
sbh$propmothers25_39[is.na(sbh$propmothers25_39)]=0
sbh$propmothers40_49[is.na(sbh$propmothers40_49)]=0



######################################################################
##  get proportions of WOMEN by ages for each cluster

pr   <- copy(sbh)
age  <- pr[,.(mean_womage = weighted.mean(age_year,w=weight,na.rm=T)),by=.(cluster_id)]
pr   <- pr[,.(women = sum(weight)),by=.(cluster_id,age_group_of_woman)]
pr   <- pr[,totwomen := sum(women),by=cluster_id]

pr1  <- pr[age_group_of_woman=='15-19',]
pr2  <- pr[age_group_of_woman=='20-24',]
pr3  <- pr[age_group_of_woman%in%c('25-29','30-34','35-39'),]
pr4  <- pr[age_group_of_woman%in%c('40-44','45-49'),]
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

sbh <- merge(sbh,prA,by='cluster_id',all.x=TRUE)
sbh <- merge(sbh,age,by='cluster_id',all.x=TRUE)
sbh$propwomen15_19[is.na(sbh$propwomen15_19)]=0
sbh$propwomen20_24[is.na(sbh$propwomen20_24)]=0
sbh$propwomen25_39[is.na(sbh$propwomen25_39)]=0
sbh$propwomen40_49[is.na(sbh$propwomen40_49)]=0

######################################################################
##  get CEB, CED, and Mean_Matage
if(grepl("geos", Sys.info()[4])) INLA:::inla.dynload.workaround()

sbh     <- subset(sbh, ceb != 0)
sbhsave <- copy(sbh)

# scale weights
sbhsave$i=1
tmp <- sbhsave[,.(tot = sum(i),totw=sum(weight)),
                by=.(cluster_id,nid,source,country,year,latnum,longnum,shapefile,
                     location_name,location_code)]
tmp[,scale:=tot/totw]
sbh <- merge(sbh,tmp,by=c('cluster_id','nid','source','country','year','latnum','longnum','shapefile',
'location_name','location_code'),all.x=T)
sbh[,nweight := weight*scale]

sbh <- sbh[,.(ceb=sum(ceb*nweight,na.rm=TRUE),
               ced=sum(ced*nweight,na.rm=TRUE),
               cebN=sum(ceb,na.rm=TRUE),
               cedN=sum(ced,na.rm=TRUE),
               meanceb=weighted.mean(ceb,w=weight,na.rm=TRUE),
               meanced=weighted.mean(ced,w=weight,na.rm=TRUE),
               mean_matage=weighted.mean(age_year,w=weight,na.rm=TRUE)),
             by=.(cluster_id,nid,source,country,year,latnum,longnum,shapefile,
                  location_name,location_code,pr1,pr2,
                  propmothers15_19,propmothers20_24,propmothers25_39,
                  propmothers40_49,totmothers,propwomen15_19,propwomen20_24,
                  propwomen25_39,propwomen40_49,totwomen,mean_womage) ]
sbh <- subset(sbh,!is.na(nid))


# Add a cluster_id to 8 lonely observations
sbh$cluster_id[is.na(sbh$cluster_id)] <- (max(sbh$cluster_id,na.rm=TRUE)+1):((max(sbh$cluster_id,na.rm=TRUE))+nrow(sbh[is.na(cluster_id),]))

# Built in check: cluster_id unique
stopifnot(nrow(sbh[duplicated(cluster_id),])==0)


####################################################################
## SBH data will need some reshaping so we can rbind with xw and run the model.

# we will need 16 potential observations for each piece of SBH data to map to CBH crosswalk
sbh <- sbh[rep(seq_len(nrow(sbh)), each=16),]
sbh[,age_bin := rep(1:4,nrow(sbh)/4)]
sbh[,period  := rep(rep(1:4,each=4),nrow(sbh)/16)]
sbh[,age_bin :=  factor(age_bin)]

# map periods as they will be in the xwalk data
# note that 1 is most recent here..
mapper <- data.table(year=1998:2017,
                     surv_per=rep(4:1,each=5))
sbh <- merge(sbh,mapper,by='year',all.x=TRUE)

# make lag
sbh[,lag := period-surv_per]
sbh <- subset(sbh,lag >= 0) # in this case neg lag means future.
sbh[,lag     :=  factor(lag)]

# make lag year
periods         <- data.frame(period = 1:4, year = c(2015, 2010, 2005, 2000))
sbh$per_year    <- periods$year[match(sbh$period,periods$period)]
sbh$yrlag       <- sbh$year - sbh$per_year
sbh$survey_year <- sbh$year; sbh$year=NULL

# make NA CBH Data
sbh$exposed <- sbh$died <- NA

# shrink model to get away from zero
sbh <- subset(sbh, !ced>ceb) # drops 4 oddball observations

sbh$ced_orig  <- sbh$ced
sbh$died_orig <- sbh$died

sbh2 <- data.table()
for(s in unique(sbh$nid)){
  tmp <- sbh[sbh$nid==s,]
    message(s)
    sbhmod = inla(ced~-1+f(cluster_id,model='iid'),
                  family='binomial',
                  data=tmp,
                  Ntrials=ceb,
                  num.threads = 1,
                  control.inla = list(int.strategy = 'eb', h = 1e-3, tolerance = 1e-6),
                  control.predictor=list(link=1))
    tmp$ced=sbhmod$summary.fitted.values$mean*tmp$ceb
  sbh2=rbind(sbh2,tmp)
}

sbh <- data.table(sbh2)

# interactive checks
summary(sbh2[ced!=ced_orig&ced_orig==0,]$ced) # make sure they are pretty small
cor(sbh$ced,sbh$ced_orig) # very very close to 1

# logit tranform
sbh[,logit_sbh := qlogis(ced/ceb)]


######################################################################
##  Two agg surveys that we only have propwomen for, so we build a crosswalk just for those. Right now using propwomen instead of propmothers in a slightly tweaked model
# make agg data table as similar as only as possible

# get those surveys out of sbhagg
agg <- sbhagg[nid %in% aggonlyinds,]

## Get survey-wise parity ratios, include them in the model
pr   <- agg[,.(p1=sum(ceb*ceb_15_19,na.rm=TRUE),
               p2=sum(ceb*ceb_20_24,na.rm=TRUE),
               p3=sum(ceb*ceb_25_29,na.rm=TRUE)),
               by=.(nid)]
pr[,pr1:=p1/p2]
pr[,pr2:=p2/p3]
pr$p1 <- pr$p2 <- pr$p3 <- NULL
agg <- merge(agg,pr,by='nid')
agg[,pr1 := ceb_15_19/ceb_20_24]
agg[,pr2 := ceb_20_24/ceb_25_29]

agg[,mean_womage := women_15_19*17+
                    women_20_24*22+
                    women_25_29*27+
                    women_30_34*32+
                    women_35_39*37+
                    women_40_44*42+
                    women_45_49*47]

agg[,propwomen15_19 := women_15_19]
agg[,propwomen20_24 := women_20_24]
agg[,propwomen25_39 := women_25_29+women_30_34+women_35_39]
agg[,propwomen40_49 := women_40_44+women_45_49]
agg[,totwomen := sum_women]
agg[,cluster_id := (1:nrow(agg))+max(sbh$cluster_id)]
agg[,logit_sbh :=qlogis(ced/ceb)]

agg <- agg[rep(seq_len(nrow(agg)), each=16),]
agg[,age_bin := rep(1:4,nrow(agg)/4)]
agg[,period  := rep(rep(1:4,each=4),nrow(agg)/16)]
agg[,age_bin :=  factor(age_bin)]

agg <- merge(agg,mapper,by='year',all.x=TRUE)

# make lag
agg[,lag := period-surv_per]
agg <- subset(agg,lag >= 0) # in this case neg lag means future.
agg[,lag     :=  factor(lag)]

# make lag year
agg$per_year    <- periods$year[match(agg$period,periods$period)]
agg$yrlag       <- agg$year - agg$per_year
agg$survey_year <- agg$year; agg$year=NULL

# make NA CBH Data
agg$exposed   <- agg$died <- NA
agg$ced_orig  <- agg$ced
agg$died_orig <- agg$died

# pare agg down to just the needed variables
agg<-agg[,c("cluster_id", "nid", "source", "country", "latnum", "longnum",
          "shapefile", "location_name", "location_code", "pr1", "pr2",
          "totwomen", "propwomen15_19", "propwomen20_24", "propwomen25_39",
          "propwomen40_49", "ceb", "ced", "age_bin", "period", "surv_per",
          "lag", "per_year", "yrlag", "survey_year", "died", "exposed", "ced_orig",
          "died_orig", "logit_sbh",'mean_womage'),with=FALSE]


####################################################################
##  crosswalk

# bring in Prepped Crosswalk data and wrangle a little bit
svy  <- fread('<<<< FILEPATH REDACTED >>>>>/prepped_cbh_sbh_svy_2.csv')
adm1 <- data.table(read.csv('<<<< FILEPATH REDACTED >>>>>/prepped_cbh_sbh_ad1_2.csv'))
svy$level   <- 'svy'
adm1$level  <- 'adm1'

svy$V1 <- NULL
adm1$X <- NULL

svy[,ced := as.numeric(ced)]
svy[,ceb := as.numeric(ceb)]
adm1[,ced := as.numeric(ced)]
adm1[,ceb := as.numeric(ceb)]

svy  <- subset(svy,age_bin!=5)
adm1 <- subset(adm1,age_bin!=5)

xw          <- data.table(rbind(svy,adm1))
xw$died     <- as.numeric(as.character(xw$died))
xw$exposed  <- as.numeric(as.character(xw$exposed))
xw$per_year <- as.numeric(as.character(xw$per_year))

xw   <- subset(xw,age_bin!=5)   # drop any total 5q0 age bin, not used here
xw   <- subset(xw,exposed>5)    # min ss for modelling

xw$X=xw$V1=NULL
xw   <- na.omit(xw)
xw[,id      :=  paste0(id,level)]
xw[,age_bin :=  factor(age_bin)]
xw[,lag     :=  factor(lag)]
xw[,cluster_id := id]

# split out xw for svy and adm1 as they serve different needs
xws <- subset(xw,level=='svy')
xwa <- subset(xw,level=='adm1')

# Make a combined dataset of SBH and XW data
xws$test <- 1
xwa$test <- 1
sbh$test <- 0
mds <- data.table(smartbind(xws,sbh))
mda <- data.table(smartbind(xwa,sbh))

mds[,yrlag := as.numeric(yrlag)]
mds$yrsinper <- mds$yrlag+2
mds$yrsinper[mds$yrsinper>5] <- 5

# a few extra steps for variables nneded for the N model
mda$lexposed <- log(mda$exposed)
mda$lceb     <- log(mda$ceb)
mda$lced     <- log(mda$ced)
mda$yrlag <- as.numeric(as.character(mda$yrlag))
mda$yrsinper <- mda$yrlag+2
mda$yrsinper[mda$yrsinper>5] <- 5

# for agg model
agg$test=0
mdsagg <- data.table(smartbind(xws,agg))
mdaagg <- data.table(smartbind(xwa,agg))
mdaagg$lexposed <- log(mdaagg$exposed)
mdaagg$lceb     <- log(mdaagg$ceb)
mdaagg$lced     <- log(mdaagg$ced)
mdaagg$yrlag <- as.numeric(as.character(mdaagg$yrlag))
mdaagg$yrsinper <- mdaagg$yrlag+2
mdaagg$yrsinper[mdaagg$yrsinper>5] <- 5

mdsagg[,yrlag := as.numeric(yrlag)]
mdsagg$yrsinper <- mdsagg$yrlag+2
mdsagg$yrsinper[mdsagg$yrsinper>5] <- 5


  ####################################################################
  ####################################################################
  ## First model, for p_hat
  message('Modeling P')
  f_resp <- died ~ 1
  f_sbh_fixed  <- ~ logit_sbh +
                    factor(lag) + factor(age_bin) +
                    logit_sbh:factor(lag) + logit_sbh:factor(age_bin) + logit_sbh:factor(lag):factor(age_bin) +
                    factor(lag):factor(age_bin) + mean_matage + period + propmothers15_19 + propmothers20_24 +
                    propmothers25_39 + pr1 + pr2 + yrsinper + factor(lag):mean_matage
  f_random_intercepts <- ~  f(factor(country) ,model='iid') + f(factor(cluster_id),model='iid') # cluster id is NID here

  f_formula <- f_resp + f_sbh_fixed + f_random_intercepts

  # fit the model
  bm <- inla(f_formula,
             data             = mds,
             Ntrials          = mds$exposed,
             family           = 'binomial',
             num.threads      = 1,
             control.inla     = list(int.strategy = intstrat, h = 1e-3, tolerance = 1e-6),
             verbose          = TRUE,
             control.predictor= list(link=1),control.compute=list(config = TRUE))

  # SIMILAR MODEL BUT FOR AGG ONLY
  message('Modeling P for agg data')
  f_sbh_fixed2  <- ~ logit_sbh +
                    factor(lag) + factor(age_bin) +
                    logit_sbh:factor(lag) + logit_sbh:factor(age_bin) + logit_sbh:factor(lag):factor(age_bin) +
                    factor(lag):factor(age_bin) + period + propwomen15_19 + propwomen20_24 +
                    propwomen25_39  + pr1 + pr2 + yrsinper + factor(lag):mean_womage
  f_formula2 <- f_resp + f_sbh_fixed2 + f_random_intercepts

  # fit the model
  bmagg <- inla(f_formula2,
             data             = mdsagg,
             Ntrials          = mdsagg$exposed,
             family           = 'binomial',
             num.threads      = 1,
             control.inla     = list(int.strategy = intstrat, h = 1e-3, tolerance = 1e-6),
             verbose          = TRUE,
             control.predictor= list(link=1),control.compute=list(config = TRUE))


  ####################################################################
  ####################################################################
  ## Second model, for N_hat, on adm1 level data

  message('Modeling N')
  f_resp3   <- exposed ~ 1
  f_fixed3  <- ~ lceb + factor(lag) + factor(age_bin) +
                 lceb:yrsinper + lceb:factor(age_bin) + lceb:yrsinper:factor(age_bin) +
                 mean_matage + period + lced + propmothers15_19 + propmothers20_24 +
                 propmothers25_39 + pr1 + pr2 + yrsinper + yrsinper:factor(age_bin) +
                 factor(lag):mean_matage

  f_random_intercepts3 <- ~  f(factor(country) ,model='iid') + f(factor(nid),model='iid') + f(factor(cluster_id),model='iid')

  f_formula3 <- f_resp3 + f_fixed3  + f_random_intercepts3

  nm <- inla(f_formula3,
            data=mda,
            family = 'poisson',
            num.threads = 5,
            control.inla = list(int.strategy = intstrat, h = 1e-3, tolerance = 1e-6),
            verbose=TRUE,
            control.predictor=list(link=1),control.compute=list(config=TRUE))

  # SIMILAR MODEL BUT FOR AGG ONLY
  message('Modeling N for agg data')
  f_resp3   <- exposed ~ 1
  f_fixed4  <- ~ lceb + factor(lag) + factor(age_bin) +
                 lceb:yrsinper + lceb:factor(age_bin) + lceb:yrsinper:factor(age_bin) +
                period + lced + propwomen15_19 + propwomen20_24 +
                propwomen25_39 + pr1 + pr2 + yrsinper + yrsinper:factor(age_bin) +
                factor(lag):mean_womage

  f_formula4 <- f_resp3 + f_fixed4  + f_random_intercepts3

  # fit the model
  nmagg <- inla(f_formula4,
            data=mdaagg,
            family = 'poisson',
            num.threads = 1,
            control.inla = list(int.strategy = intstrat, h = 1e-3, tolerance = 1e-6),
            verbose=TRUE,
            control.predictor=list(link=1),control.compute=list(config=TRUE))


if(grepl("geos", Sys.info()[4])) INLA:::inla.dynload.workaround()


####################################################################
####################################################################
## Simulate uncertainty
message('Sim Uncertainty')
draws <- 10000

# FOR Regular SBH
# sample Ns
message('N uncertainty in full model ')
psn <- inla.posterior.sample(draws,nm)
psN <- matrix(NA, ncol = draws,
              nrow = sum(mda$test==0))
pb <- txtProgressBar(min = 0, max = draws, initial = 0)
for(i in 1:draws){
  setTxtProgressBar(pb,i)
  psN[,i] <- psn[[i]][2]$latent[grep('Predictor',rownames(psn[[1]][2]$latent))][mda$test==0]
}
close(pb)

# getting predictions of N
Nhat_draws <- matrix(NA,nrow=nrow(psN),ncol=ncol(psN))
message('N prediction uncertainty in full model')
pb = txtProgressBar(min = 0, max = draws, initial = 0)
for(i in 1:draws){
  setTxtProgressBar(pb,i)
  Nhat_draws[,i] <- rpois( nrow(psN),
                                  exp(psN[,i]))
}
close(pb)
Nhat_draws <- Nhat_draws


# sample Ps
psp <- inla.posterior.sample(draws,bm)
psP <- matrix(NA, ncol = draws,
              nrow = sum(mds$test==0))
message('P uncertainty in full model ')
pb = txtProgressBar(min = 0, max = draws, initial = 0)
for(i in 1:draws){
  setTxtProgressBar(pb,i)
  psP[,i] <- psp[[i]][2]$latent[grep('Predictor',rownames(psp[[1]][2]$latent))][mds$test==0]
}
close(pb)

phat_draws <- plogis(psP)


# draw Nplus hat from another binomial with sample size N
Nplushat_draws <- matrix(rbinom(length(Nhat_draws),
                                as.vector(round(Nhat_draws,0)),
                                as.vector(phat_draws)),
                            ncol=draws)

# add everything onto the SBH data table
sbh[,exposed      := apply(Nhat_draws,    1,mean,na.rm=T)]
sbh[,died         := apply(Nplushat_draws,1,mean,na.rm=T)]
sbh[,p_hat        := apply(phat_draws,1,mean,na.rm=T)]
sbh[,Nplushat_var := apply(Nplushat_draws,1,var,na.rm=T)]
sbh[,analytical_var := (p_hat*exposed*(1-p_hat)) ]

# make the weight based on model variance. this weight is the inverse of variance from modelling, above expected from binomial.
sbh[,sbh_wgt := (     ( (Nplushat_var)/(p_hat*exposed*(1-p_hat)) )  )^(-1) ]


########################
# UNCERTAINTY FOR AGG DATA AS WELL

# sample Ns
message('N uncertainty in agg model ')
psn <- inla.posterior.sample(draws,nmagg)
psN <- matrix(NA, ncol = draws,
              nrow = sum(mdaagg$test==0))
pb <- txtProgressBar(min = 0, max = draws, initial = 0)
for(i in 1:draws){
  setTxtProgressBar(pb,i)
  psN[,i] <- psn[[i]][2]$latent[grep('Predictor',rownames(psn[[1]][2]$latent))][mdaagg$test==0]
}
close(pb)

# getting predictions of N
Nhat_draws <- matrix(NA,nrow=nrow(psN),ncol=ncol(psN))
message('N prediction uncertainty in agg model ')
pb = txtProgressBar(min = 0, max = draws, initial = 0)
for(i in 1:draws){
  setTxtProgressBar(pb,i)
    Nhat_draws[,i] <- rpois( nrow(psN),          )
                                    exp(psN[,i]))
}
close(pb)

Nhat_draws <- Nhat_draws

# sample Ps
psp <- inla.posterior.sample(draws,bmagg)
psP <- matrix(NA, ncol = draws,
              nrow = sum(mdsagg$test==0))
message('N uncertainty in agg model ')
pb <- txtProgressBar(min = 0, max = draws, initial = 0)
for(i in 1:draws){
  setTxtProgressBar(pb,i)
  psP[,i] <- psp[[i]][2]$latent[grep('Predictor',rownames(psp[[1]][2]$latent))][mdsagg$test==0]
}
close(pb)

phat_draws <- plogis(psP)


# draw Nplus hat from another binomial with sample size N
Nplushat_draws <- matrix(rbinom(length(Nhat_draws),
                                as.vector(round(Nhat_draws,0)),
                                as.vector(phat_draws)),
                            ncol=draws)

# add everything onto the SBH data table
agg[,exposed      := apply(Nhat_draws,    1,mean,na.rm=T)]
agg[,died         := apply(Nplushat_draws,1,mean,na.rm=T)]
agg[,p_hat        := apply(phat_draws,1,mean,na.rm=T)]
agg[,Nplushat_var := apply(Nplushat_draws,1,var,na.rm=T)]
agg[,analytical_var := (p_hat*exposed*(1-p_hat)) ]

# make the weight based on model variance.this weight is the inverse of variance from modelling, above expected from binomial.
agg[,sbh_wgt := (     ( (Nplushat_var)/(p_hat*exposed*(1-p_hat)) )  )^(-1) ]

###############################
# combine agg and sbh into one datasets
sbh$agg <- 0
agg$agg <- 1

dat <- data.table(smartbind(sbh,agg))

####################################################################
####################################################################
## Clean up so it maps with CBH processed data

# first save a raw full sbh file for posterity
dir.create(paste0(sharedir,'/input/dated/',run_date,'/'))

# pare it down to it merges with CBH data
dat[,data_type := 'sbh']
sbh_sry <- dat[,c('cluster_id', 'exposed', 'died', 'per_year', 'age_bin',
                  'longnum', 'latnum', 'shapefile', 'location_code',
                  'nid', 'source', 'country', 'data_type','sbh_wgt'), with=FALSE]
setnames(sbh_sry,new=c('latitude','longitude','year'),old=c('latnum','longnum','per_year'))

write.csv(sbh_sry,
            file = paste0('/<<<< FILEPATH REDACTED >>>>>/mortality_sbh_w3.csv'),
            row.names = FALSE)


####################################################################
####################################################################
