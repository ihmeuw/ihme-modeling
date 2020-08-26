## Explore changing the definition of ETEC from any to ST-ETEC ##
library(plyr)
library(ggplot2)
library(gridExtra)
library(lme4)
library(metafor)

######################################################################
## Prep data ##
######################################################################
  ### Set standard names ###
  base_names <- c("etec","st_etec","lt_etec")
  pcr_names <- paste0("tac_",base_names)
  lab_names <- paste0("lab_",base_names)

  ## Prep MAL-ED data ##
  tac_maled <- c("ETEC","LT_ETEC","ST_ETEC")
  lab_maled <- c("etec","lt_etec","st_etec")
    tac <- read.csv("filepath")
    micro <- read.csv("filepath")

    # Set EPEC
    micro$tEPEC <- micro$epec
    micro$aEPEC <- micro$atypicalepec
    micro$EPEC <- micro$epec + micro$atypicalepec

    ## Some data prep for TAC results ##
    tac$ETEC <- ifelse(tac$ST_ETEC < tac$LT_ETEC, tac$ST_ETEC, tac$LT_ETEC)
    tac.names <- c("ETEC","ST_ETEC","LT_ETEC")

    keep.tac <- tac[,c("pid","month_ss","agedays","stooltype","stooltype7","stooltype14","stooltype28","srfspectype","pilot","list1","list2","flag24","matchid", tac.names)]
    keep.tac$case <- ifelse(keep.tac$stooltype7=="D1",1, ifelse(keep.tac$stooltype7=="M1",0,NA))

    keep.tac$keep_month <- ifelse(keep.tac$month_ss %in% c(3,6,9,12,15,18,21,24), 1, 0)
    keep.tac$keep_overall <- ifelse(keep.tac$case==1, 1, ifelse(keep.tac$keep_month==1,1,0))
    setnames(keep.tac, c("ETEC","ST_ETEC","LT_ETEC"), c("tac_etec","tac_st_etec","tac_lt_etec"))

    ## Get MAL-ED Data ##
    keep.micro <- micro[,c("pid","agedays","flag_diarrhea",lab_maled,"EPEC","tEPEC","aEPEC")]
    keep.micro[is.na(keep.micro)] <- 0

    colnames(keep.micro)[4:6] <- c("lab_etec","lab_lt_etec","lab_st_etec")

    maled <- join(keep.micro, keep.tac, by=c("pid", "agedays"))

    m1 <- subset(maled, list1==1 & keep_overall==1)
    ##  Use either list1 or list2 ##
    maled$list3 <- ifelse(maled$list1==1,1,ifelse(maled$list2==1,1,0))
    maled$keep_stool <- ifelse(maled$stooltype7=="",0,1)
    m2 <- subset(maled, list3==1 & keep_stool==1)
    maled <- m2

  ## Get GEMS data ##
    gems_data <- read.csv("filepath")
    tac_gems <- c("tac_ETEC","tac_ST_ETEC","tac_LT_ETEC")
    lab_gems <- c("ETEC_ALL","ETEC_anyST","ETEC_LTonly")

    gems <- subset(gems_data, !is.na(case.control))
    gems$age_years <- ifelse(gems$CASE_AGE_CAT==1,0, ifelse(gems$CASE_AGE_CAT==2,1,2))
    gems$case <- gems$case.control

    gems$EPEC <- ifelse(gems$aEPEC==1,1,ifelse(gems$tEPEC==1,1,0))

    setnames(gems, c("tac_ETEC","tac_ST_ETEC","tac_LT_ETEC"), c("tac_etec","tac_st_etec","tac_lt_etec"))
    setnames(gems, c("ETEC_ALL","ETEC_anyST","ETEC_LTonly"), c("lab_etec","lab_st_etec","lab_lt_etec"))

    gems$pid <- paste0("g",gems$CHILDID)
    gems$caseid <- gems$CASEID
    gems$site <- gems$site.names
    gems$source <- "GEMS"
    gems <- gems[,c("age_years","case","source","site","caseid","pid", pcr_names, lab_names, "tEPEC","aEPEC","EPEC")]

    maled$age_years <- ifelse(maled$agedays<365,0,1)
    maled$caseid <- maled$pid
    maled$site <- substr(maled$caseid,1,2)
    maled$source <- "MALED"
    maled <- maled[,c("pid","caseid","case","source","site","age_years", pcr_names, lab_names,"tEPEC","aEPEC","EPEC")]

    df <- rbind(maled, gems)

    locs <- read.csv("filepath")
    loc_map <- data.frame(site=c("TZ","SA","PK","PE","NP","Mozambique","Mali","Kenya","Karachi","India","IN","Gambia","BR","BG","Bangladesh"),
                          location_name=c("Tanzania","South Africa","Pakistan","Peru","Nepal","Mozambique","Mali","Kenya","Pakistan","India","India","The Gambia","Brazil","Bangladesh","Bangladesh"))
    loc_map <- join(loc_map, locs[,c("location_name","super_region_name","region_name","location_id")], by="location_name")

    df <- join(df, loc_map, by="site")
##############################################################################
    ## Data Prepped ##
##############################################################################

##############################################################################
    ## Sum tEPEC and ST-ETEC and find proportion by site/study ##
##############################################################################
    ecoli <- aggregate(cbind(tEPEC, aEPEC, lab_st_etec, lab_lt_etec, EPEC, lab_etec) ~ source + location_name + super_region_name + region_name, data=df, function(x) sum(x))
    ecoli$prop_tepec <- ecoli$tEPEC / ecoli$EPEC
    ecoli$prop_stetec <- ecoli$lab_st_etec / ecoli$lab_etec

    ecoli$se_tepec <- sqrt(ecoli$prop_tepec * (1 - ecoli$prop_tepec) / ecoli$EPEC)
    ecoli$se_stetec <- sqrt(ecoli$prop_stetec * (1- ecoli$prop_stetec) / ecoli$lab_etec)

    ecoli <- ecoli[order(ecoli$super_region_name,ecoli$location_name),]
    ecoli$label <- paste0(ecoli$source," ", ecoli$location_name)

##############################################################################
## Run meta-analysis/regression for typical EPEC ##
  ## Varies by Study but not by Super region
    # mepec <- rma(yi=prop_tepec, sei=se_tepec, mods=~source, data=ecoli, slab=paste0(source," ", location_name))

    ecoli$label <- factor(ecoli$label, levels=ecoli$label)
    ggplot(data=ecoli, aes(x=prop_tepec, y=(label), col=super_region_name)) + geom_point(aes(shape=source), size=5) +
      geom_errorbarh(aes(xmin=prop_tepec-se_tepec*1.96, xmax=prop_tepec+se_tepec*1.96), height=0, lwd=1.25) + theme_bw()

    # re_mepec <- lmer(prop_tepec ~ source + (1|super_region_name), weights=1/se_tepec^2, data=ecoli) #
    # summary(re_mepec)
    # ranef(re_mepec)

  ## We think this varies by super region from our literature review!
    locs <- read.csv("filepath")
    epec_lit <- read.csv("filepath")
    epec_lit <- subset(epec_lit, citation!="GEMS")
    epec_lit <- join(epec_lit, locs[,c('location_id',"location_name","super_region_name")], by="location_name")
    epec_lit$super_region_name <- ifelse(is.na(epec_lit$super_region_name), "Latin America and Caribbean", as.character(epec_lit$super_region_name))
    setnames(epec_lit, c("ratio_tepec","standard_error"), c("prop_tepec","se_tepec"))

    compare_df <- rbind(ecoli[,c("super_region_name","label","prop_tepec","se_tepec")], epec_lit[,c("super_region_name","label","prop_tepec","se_tepec")])
    compare_df <- join(compare_df, locs[locs$level==1,c("location_id","super_region_name")], by="super_region_name")

  ## Test again using super region as predictor!
    compare_df <- compare_df[order(compare_df$label),]
    mepec <- rma(yi=prop_tepec, sei=se_tepec, mods=~factor(super_region_name), data=compare_df, slab=label)
    summary(mepec)
    forest(mepec)

###########################################
    ## This is the used analysis!
###########################################
  ## using that dataset, convert to logit space as many of those data are widely variable
  library(boot)
  compare_df$lg_mean <- logit(compare_df$prop_tepec)

  compare_df$delta_lg_se <- sapply(1:nrow(compare_df), function(i) {
      ratio_i <- compare_df[i, "prop_tepec"]
      ratio_se_i <- compare_df[i, "se_tepec"]
      deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
    })

  # Order the super regions
  compare_df$super_order <- reorder(compare_df$super_region_name, compare_df$location_id)
  lg_mepec <- rma(yi=lg_mean, sei=delta_lg_se, mods=~super_order, data=subset(compare_df, prop_tepec>0), slab=label)
  summary(lg_mepec)
  forest(lg_mepec, transf= function(x) inv.logit(x))

  ggplot(data=compare_df, aes(x=prop_tepec, y=(label), col=super_region_name)) + geom_point(size=5) +
    geom_errorbarh(aes(xmin=prop_tepec-se_tepec*1.96, xmax=prop_tepec+se_tepec*1.96), height=0, lwd=1.25) + theme_bw()
  ggplot(compare_df, aes(x=super_region_name, y=prop_tepec, col=super_region_name)) + geom_boxplot() + guides(col=F) + theme_bw() +
    theme(axis.text.x=element_text(angle=60, hjust=1))
## Make a prediction matrix
  supers <- locs[locs$level==1, c("location_name","location_id")]
  supers <- supers[order(supers$location_id),]
  supers <- supers$location_name
  # predict(mepec, newmods = matrix(supers))
  betas <- as.numeric(lg_mepec$b)
  intercept <- betas[1]
  pred_ratio <- betas + intercept
  pred_ratio[1] <- intercept
  pred_se <- as.numeric(lg_mepec$se)
  prediction <- data.frame(super_region_name=supers, pred_ratio, pred_se)

## Use draws to get mean and se in linear space
  for(i in 1:1000){
    draw <- rnorm(length(prediction$super_region_name), mean=prediction$pred_ratio, sd=prediction$pred_se)
    prediction[,paste0("draw_",i)] <- inv.logit(draw)
  }
  prediction$pred_ratio <- rowMeans(prediction[,4:1003])
  prediction$pred_se <- apply(prediction[,4:1003], 1, function(x) sd(x))

  prediction <- prediction[,c("super_region_name","pred_ratio","pred_se")]

  write.csv(prediction, "filepath", row.names=F)

#######################################################################################################
## Run meta-analysis/regression for ST ETEC ##
  ## Seems to vary by both Study and by Super region
  ggplot(data=ecoli, aes(x=prop_stetec, y=(label), col=super_region_name)) + geom_point(aes(shape=source), size=5) +
    geom_errorbarh(aes(xmin=prop_stetec-se_stetec*1.96, xmax=prop_stetec+se_stetec*1.96), height=0, lwd=1.25) + theme_bw()

  re_metec <- lmer(prop_stetec ~ source + (1|super_region_name), weights=1/se_stetec^2, data=ecoli)
  summary(re_metec)
  ranef(re_metec)

  metec <- rma(yi=prop_stetec, sei=se_stetec, mods=~source + super_region_name, data=ecoli, slab=paste0(source," ", location_name))
  summary(metec)
  forest(metec)

###########################################
## This is the used analysis!
###########################################
  ecoli <- join(ecoli, locs[locs$level==1,c("location_id","super_region_name")], by="super_region_name")
  ## using that dataset, convert to logit space as many of those data are widely variable
  ecoli$lg_st <- logit(ecoli$prop_stetec)

  ecoli$delta_lg_st_se <- sapply(1:nrow(ecoli), function(i) {
    ratio_i <- ecoli[i, "prop_stetec"]
    ratio_se_i <- ecoli[i, "se_stetec"]
    deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
  })

  # Order the super regions
    ecoli$super_order <- reorder(ecoli$super_region_name, ecoli$location_id)
  # Model
    lg_metec <- rma(yi=lg_st, sei=delta_lg_st_se, mods=~super_order + source, data=subset(ecoli, prop_stetec>0), slab=label)
    summary(lg_metec)
    forest(lg_metec, transf= function(x) inv.logit(x))
  # Model without regions
    lg_etec_i <- rma(yi=lg_st, sei=delta_lg_st_se, mods=~source, data=subset(ecoli, prop_stetec>0), slab=label)
    summary(lg_etec_i)

  ## Make a prediction matrix
  supers <- unique(ecoli$super_order)
  # supers <- locs[locs$level==1, c("location_name","location_id")]
  # supers <- supers[order(supers$location_id),]
  # supers <- supers$location_name
  # predict(mepec, newmods = matrix(supers))
  betas <- as.numeric(lg_metec$b)
  intercept <- betas[1]
  pred_ratio <- betas + intercept
  pred_ratio[1] <- intercept
  pred_se <- as.numeric(lg_metec$se)
  prediction <- data.frame(super_region_name=supers, pred_ratio=pred_ratio[1:3], pred_se=pred_se[1:3])

  # Find the missing super regions
  msupers <- data.frame(location_name=locs[locs$level==1, c("location_name")])
  msupers <- subset(msupers, !(location_name %in% prediction$super_region_name))

  # Set the prediction from those to be the model without regions intercept
  pred_i <- data.frame(msupers, pred_ratio=lg_etec_i$b[1], pred_se = lg_etec_i$se[1])
  setnames(pred_i, "location_name","super_region_name")

  prediction <- rbind(prediction, pred_i)

  # Since the source type is a predictor of this ratio, have separate ratios for cv_inpatient, non
  prediction$cv_inpatient <- 1
  pred2 <- prediction
  pred2$cv_inpatient <- 0
  pred2$pred_ratio <- pred2$pred_ratio + lg_etec_i$b[2]
  pred2$pred_se <- pred2$pred_se + lg_etec_i$se[2]
  prediction <- rbind(prediction, pred2)

  ## Use draws to get mean and se in linear space
  for(i in 1:1000){
    draw <- rnorm(length(prediction$super_region_name), mean=prediction$pred_ratio, sd=prediction$pred_se)
    prediction[,paste0("draw_",i)] <- inv.logit(draw)
  }
  prediction$pred_ratio <- rowMeans(prediction[,5:1004])
  prediction$pred_se <- apply(prediction[,5:1004], 1, function(x) sd(x))

  prediction <- prediction[,c("super_region_name","pred_ratio","pred_se","cv_inpatient")]

  write.csv(prediction, "filepath", row.names=F)

########################################################################
  ## Finished!! ##
