## Combine and then compare GEMS and MALED tac data ##
## The only part of the code that directly goes into
## GBD analyses is up to line 72.
######################################################

library(plyr)
library(ggplot2)
library(scales)

### Set standard names ###
# EPEC from MALED is tEPEC
base_names <- c("adenovirus","aeromonas","campylobacter","cryptosporidium","ehist","epec","etec","norovirus","rotavirus","salmonella","shigella","cholera","st_etec")
pcr_names <- paste0("tac_",base_names)
lab_names <- paste0("lab_",base_names)

## Get MAL-ED Data ##
tac_maled <- c("adenovirus", "aeromonas", "campylobacter", "cryptosporidium", "ehist", "tEPEC", "ETEC", "norovirus","rotavirus","salmonella","shigella_eiec","v_cholerae","ST_ETEC")
micro <- read.csv("filepath")
lab_maled <- c("adeno","aeromonas","campy","crypto","ehist","epec","etec","nrvgeno2","rota","salmonella","shigella","vibrio","st_etec")
keep.micro <- micro[,c("pid","cafsex","agedays","flag_diarrhea",lab_maled)]
keep.micro[is.na(keep.micro)] <- 0
colnames(keep.micro)[5:17] <- lab_names

setnames(keep.micro, "cafsex", "sex")
keep.micro$sex <- keep.micro$sex - 1

keep.tac <- read.csv("filepath")

maled <- join(keep.micro, keep.tac, by=c("pid", "agedays"))

## Get hospitalizations
surv <- read.csv("filepath")
  surv$agedays <- surv$age
maled <- join(maled, surv[,c("pid","agedays","hosp","gemsdef")], by=c("pid","agedays"))

## Note the keep_overall
m1 <- subset(maled, list1==1 & keep_overall==1)
## Use either list1 or list2 ##
maled$list3 <- ifelse(maled$list1==1,1,ifelse(maled$list2==1,1,0))
maled$keep_stool <- ifelse(maled$stooltype7=="",0,1)
m2 <- subset(maled, list3==1 & keep_stool==1)

maled <- m2

## Get GEMS data ##
tac_gems <- c("tac_adenovirus","tac_aeromonas","tac_campylobacter","tac_cryptosporidium","tac_entamoeba",
                "tac_TEPEC", "tac_norovirus", "tac_rotavirus","tac_salmonella", "tac_shigella_eiec","tac_ETEC","tac_v_cholerae","tac_ST_ETEC")
lab_gems <- c("F18_RES_ADENO4041", "F16_AEROMONAS", "F16_CAMPY_JEJUNI_COLI", "F18_RES_CRYPTOSPOR",
                "F18_RES_ENTAMOEBA", "F17_tEPEC","F17_ETEC", "F19_NOROVIRUS", "F18_RES_ROTAVIRUS", "F16_SALM_NONTYPHI", "F16_SHIG_SPP","F16_VIB_CHOLERAE","ETEC_anyST")
gems_data <- read.csv("filepath")

gems <- subset(gems_data, !is.na(case.control))
gems$age_years <- ifelse(gems$CASE_AGE_CAT==1,0, ifelse(gems$CASE_AGE_CAT==2,1,2))
gems$sex <- ifelse(is.na(gems$F2_GENDER), gems$F6_GENDER, gems$F2_GENDER)
gems$hospitalized <- gems$F4B_ADMIT
gems <- gems[,c("CASEID","case.control","age_years","site.names","sex","hospitalized",tac_gems, lab_gems)]
gems$case <- gems$case.control
gems$msd <- 1

## rBind those data ##
head(gems)
colgems <- colnames(gems)[7:19]
colgems <- ifelse(colgems=="tac_TEPEC", "tac_epec", colgems)
colgems <- ifelse(colgems=="tac_ETEC", "tac_etec", colgems)
colgems <- ifelse(colgems=="tac_ST_ETEC", "tac_st_etec", colgems)
colgems <- ifelse(colgems=="tac_shigella_eiec", "tac_shigella", colgems)
colgems <- ifelse(colgems=="tac_entamoeba","tac_ehist",colgems)
colgems <- ifelse(colgems=="tac_v_cholerae","tac_cholera",colgems)

colnames(gems)[7:19] <- colgems
colnames(gems)[20:32] <- lab_names
gems$pid <- paste0("g",gems$CASEID)
gems$site <- gems$site.names

gems <- gems[,c("pid","sex","case","site","age_years","hospitalized","msd", colgems, lab_names)]
gems$source <- "GEMS"

maled$age_years <- ifelse(maled$agedays<365,0,1)
maled$hospitalized <- maled$hosp
maled$msd <- maled$gemsdef
maled <- maled[,c("pid","sex","case","site","age_years","hospitalized","msd", tac_maled, lab_names)]
colnames(maled)[8:20] <- pcr_names
maled$source <- "MALED"

df <- rbind(maled, gems)
write.csv(df, "filepath", row.names=F)

##################################################################################
## Plot accuracy, odds ratios ##
accuracy <- read.csv("filepath")
maled_table <- read.csv("filepath")

pdf("filepath")
  source('/filepath/max_accuracy_functions.R')
  interval <- 0.1
  combined <- c()
  for(p in pcr_names){
    pathogen <- p
    output <- data.frame()
    out.maled <- data.frame()
    out.gems <- data.frame()
    ming <- floor(min(df[df$case==1 & df$source=="GEMS",pathogen], na.rm=T)) + 1
    minm <- floor(min(df[df$case==1 & df$source=="MALED",pathogen], na.rm=T)) + 1
    min <- max(ming, minm)
    is_case <- subset(df, !is.na(case))$case
    for(i in seq(min,35,interval)){
      a <- cross_table(value=i, data=subset(df, !is.na(case)), pathogen)
      a.maled <- cross_table(value=i, data=subset(df, !is.na(case) & source=="MALED"), pathogen)
      a.gems <- cross_table(value=i, data=subset(df, !is.na(case) & source=="GEMS"), pathogen)
      output <- rbind(output, data.frame(a, ct=i, type="Both"))
      out.maled <- rbind(out.maled, data.frame(a.maled, ct=i, type="MALED"))
      out.gems <- rbind(out.gems, data.frame(a.gems, ct=i, type="GEMS"))
    }
    output_total <- rbind(output, out.maled, out.gems)
    l <- loess(accuracy ~ ct,  data=output)
    output$p <- predict(l, data.frame(ct=seq(min,35,interval)))
    infl <- c(FALSE, diff(diff(output$p)>0)!=0)

    if(length(infl[infl==T])==0){
      combined_max <- min
    } else {
      combined_max <- output$ct[output$p==max(output$p[infl==T])]
    }

    yloc <- max(output$accuracy)

    vgems <- accuracy$ct.inflection[accuracy$loop_tac==pathogen]
    vmaled <- maled_table$ct_inflection[maled_table$loop_tac==pathogen]
    p <- ggplot(output_total, aes(x=ct, y=accuracy, col=type)) + geom_point() + stat_smooth(method="loess", se=F) + 
      theme_bw(base_size = 12) + ggtitle(paste0("Accuracy of diagnostic determining case status\n",substr(pathogen,5,20))) +
      geom_vline(xintercept=vgems, lty=2) + geom_vline(xintercept=combined_max, lty=3) + scale_color_discrete("Source") +
      annotate("text", label="GEMS only", x=vgems, y=yloc) + annotate("text", label="Combined", x=combined_max, y=yloc-0.01)
    print(p)
    o <- ggplot(output_total, aes(x=ct, y=odds, col=type)) + geom_point() + stat_smooth(method="loess", se=F) + 
      theme_bw(base_size = 12) + ggtitle(paste0("Odds Ratio\n",substr(pathogen,5,20))) + geom_hline(yintercept=1, lty=2) +
      # geom_vline(xintercept=vgems, lty=2) +  geom_vline(xintercept=combined_max, lty=3) +
      # annotate("text", label="GEMS only", x=vgems, y=0.5) + annotate("text", label="Combined", x=combined_max, y=1.5) +
      scale_color_discrete("Source") + ylab("Odds ratio") + xlab("Cycle Threshold")
    print(o)
    m <- melt(output_total[,c("prevalence_case","prevalence_control","ct","type")], id.vars=c("ct","type"))
    q <- ggplot(m, aes(x=ct, y=value, col=type, lty=variable)) + stat_smooth(method="loess", se=F) + 
      theme_bw(base_size = 12) + ggtitle(paste0("Prevalence in samples\n",substr(pathogen,5,20))) +
      scale_color_discrete("Source") + scale_linetype("", labels=c("Cases","Controls")) +
      # geom_vline(xintercept=vgems, lty=2) + geom_vline(xintercept=combined_max, lty=3) +
      # annotate("text", label="GEMS only", x=vgems, y=max(m$value)-0.01) + annotate("text", label="Combined", x=combined_max, y=max(m$value)-0.02) +
      scale_x_continuous("Cycle Threshold") + scale_y_continuous("Prevalence", labels=percent)
    print(q)
    q <- ggplot(output_total, aes(x=ct, y=paf, col=type)) + geom_point() + stat_smooth(method="loess", se=F) + 
      theme_bw(base_size = 12) + ggtitle(paste0("Attributable fraction\n",substr(pathogen,5,20))) +
      # geom_vline(xintercept=vgems, lty=2) + geom_vline(xintercept=combined_max, lty=3) +
      # annotate("text", label="GEMS only", x=vgems, y=max(output_total$paf)-0.01) + annotate("text", label="Combined", x=combined_max, y=max(output_total$paf)-0.02) +
      scale_color_discrete("Source") + scale_x_continuous("Cycle Threshold") + scale_y_continuous("Attributable Fraction", labels=comma)
    print(q)

   combined <- rbind(combined, data.frame(pathogen=substr(pathogen,5,20), combined_max, vgems))
  }
dev.off()

#############################################################
## Create binary indicators ##
combined$loop_tac <- paste0("tac_",combined$pathogen)
for(t in pcr_names){
  ct <- combined$combined_max[combined$loop_tac==t]
  positive <- ifelse(df[,t] < ct, 1, 0)
  df[,paste0("binary_",substr(t,5,20))] <- ifelse(df[,t] < ct,1,0)
}

df_case <- subset(df, case==1)

binary <- paste0("binary_",substr(pcr_names,5,20))
acc_table <- data.frame()
acc_plot <- data.frame()
for(i in 1:12){
  b <- binary[i]
  l <- lab_names[i]
  vtac <- df_case[,b]
  vlab <- df_case[,l]
  r <- table(vtac, vlab)
  sensitivity <- r[2,2]/(r[2,2]+r[2,1])
  specificity <- r[1,1]/(r[1,1]+r[1,2])

  vtac <- df_case[df_case$source=="GEMS",b]
  vlab <- df_case[df_case$source=="GEMS",l]
  r <- table(vtac, vlab)
  sensitivity_gems <- r[2,2]/(r[2,2]+r[2,1])
  specificity_gems <- r[1,1]/(r[1,1]+r[1,2])

  vtac <- df_case[df_case$source=="MALED",b]
  vlab <- df_case[df_case$source=="MALED",l]
  r <- table(vtac, vlab)
  sensitivity_maled <- r[2,2]/(r[2,2]+r[2,1])
  specificity_maled <- r[1,1]/(r[1,1]+r[1,2])

  out.df <- data.frame(pathogen=substr(b,8,20), sensitivity, specificity, sensitivity_gems, specificity_gems, sensitivity_maled, specificity_maled)
  acc_table <- rbind(acc_table, out.df)
  acc_plot <- rbind(acc_plot, data.frame(pathogen=substr(b,8,20), sensitivity, specificity, type="Total"),
                    data.frame(pathogen=substr(b,8,20), sensitivity=sensitivity_gems, specificity=specificity_gems, type="GEMS"),
                    data.frame(pathogen=substr(b,8,20), sensitivity=sensitivity_maled, specificity=specificity_maled, type="MALED"))
}
acc_table

pdf("filepath")
  ggplot(data=acc_plot, aes(x=pathogen, y=sensitivity, fill=type)) + geom_bar(stat="identity", position=position_dodge(width=0.9), col="black") + theme_bw() +
    theme(axis.text.x = element_text(angle=90, hjust=1)) + ggtitle("Sensitivity")
  ggplot(data=acc_plot, aes(x=pathogen, y=specificity, fill=type)) + geom_bar(stat="identity", position=position_dodge(width=0.9), col="black") + theme_bw() +
    theme(axis.text.x = element_text(angle=90, hjust=1)) + ggtitle("Specificity")
dev.off()

###################################################################################
## Calculate simple OR at GEMS cutoff ##
###################################################################################
scatter_odds <- data.frame()
bar_odds <- data.frame()
output <- data.frame()
out.maled <- data.frame()
out.gems <- data.frame()
for(p in pcr_names){
  pathogen <- p
  vgems <- accuracy$ct.inflection[accuracy$loop_tac==pathogen]
  positive <- ifelse(df[,p]<vgems,1,0)
  case <- df$case
  r <- table(positive, case)
  odds <- r[2,2]*r[1,1]/r[1,2]/r[2,1]

  r <- table(positive[df$source=="GEMS"], case[df$source=="GEMS"])
  godds <- r[2,2]*r[1,1]/r[1,2]/r[2,1]

  r <- table(positive[df$source=="MALED"], case[df$source=="MALED"])
  modds <- r[2,2]*r[1,1]/r[1,2]/r[2,1]

  a <- cross_table(value=vgems, data=subset(df, !is.na(case)), pathogen)
  a.maled <- cross_table(value=vgems, data=subset(df, !is.na(case) & source=="MALED"), pathogen)
  a.gems <- cross_table(value=vgems, data=subset(df, !is.na(case) & source=="GEMS"), pathogen)

  output <- rbind(output, data.frame(a, pathogen=substr(pathogen,5,20), type="Total"))
  out.maled <- rbind(out.maled, data.frame(a.maled, pathogen=substr(pathogen,5,20), type="MALED"))
  out.gems <- rbind(out.gems, data.frame(a.gems, pathogen=substr(pathogen,5,20), type="GEMS"))

  bar_odds <- rbind(bar_odds, data.frame(pathogen=substr(pathogen,5,20),odds=odds, type="Total"),
                    data.frame(pathogen=substr(pathogen,5,20),odds=godds, type="GEMS"),
                    data.frame(pathogen=substr(pathogen,5,20),odds=modds, type="MALED"))
  scatter_odds <- rbind(scatter_odds, data.frame(pathogen=substr(pathogen,5,20), odds, godds, modds))
}
output_total <- rbind(output, out.maled, out.gems)

pdf("filepath")
  ggplot(data=bar_odds, aes(x=pathogen, y=odds, fill=type)) + geom_bar(stat="identity", position=position_dodge(width=0.9), col="black") + 
    theme_bw() + geom_hline(yintercept=1, lty=2) + ggtitle("Odds ratio at Ct cutoff") +
    theme(axis.text.x = element_text(angle=90, hjust=1)) + xlab("") + ylab("Odds")
  ggplot(data=scatter_odds, aes(x=godds, y=modds, col=pathogen)) + geom_point(size=3, alpha=0.75) + theme_bw() + geom_abline(intercept=0, slope=1) + 
    geom_hline(yintercept=1, lty=2) + geom_vline(xintercept=1, lty=2) +
    xlab("Odds in GEMS") + ylab("Odds in MALED")
  ggplot(data=output_total, aes(x=pathogen, y=prevalence_case, fill=type)) + geom_bar(stat="identity", position=position_dodge(width=0.9), col="black") + 
    theme_bw() + ggtitle("Prevalence in cases at Ct cutoff") +
    theme(axis.text.x = element_text(angle=90, hjust=1)) + xlab("") + ylab("Prevalence")
  ggplot(data=output_total, aes(x=pathogen, y=paf, fill=type)) + geom_bar(stat="identity", position=position_dodge(width=0.9), col="black") + 
    theme_bw() + ggtitle("Attributable fraction at Ct cutoff") +
    theme(axis.text.x = element_text(angle=90, hjust=1)) + xlab("") + ylab("Attributable Fraction")
dev.off()

###########################################################################
## Ct Density plots ##
pdf("filepath")
df$case_name <- ifelse(df$case==0,"Control","Case")
for(n in pcr_names){
  df$val <- df[,n]
  p <- ggplot(subset(df, val<35 & !is.na(case)), aes(x=val, fill=source)) + geom_density(alpha=0.5) + facet_wrap(~case_name) + theme_bw() + 
    ggtitle(p) + xlab("Ct") + ggtitle(substr(n,5,20))
  print(p)
  p <- ggplot(subset(df, val<35 & !is.na(case)), aes(x=val, fill=as.factor(case))) + geom_density(alpha=0.5) + facet_wrap(~source) + theme_bw() + 
    ggtitle(p) + xlab("Ct") + ggtitle(substr(n,5,20)) +
    scale_fill_discrete(labels=c("Control","Case"), "")
  print(p)
}
dev.off()

############################################################################
## Mixed effects odds ratio compare w/o other pathogens ##
binary_names <- paste0("binary_",base_names)
out.prob <- data.frame()
for(b in binary_names){
  # pathogen
  form <- as.formula(paste("case ~ ", paste0(b,":factor(age_years)"), "+ (1|pid) + (1|site)"))
  g <- glmer(form, data=subset(df, source=="GEMS"), family="binomial")
  m <- glmer(form, data=subset(df, source=="MALED"), family="binomial")

  odds <- data.frame(pathogen=substr(b,8,20),age=c(1,2), gems=exp(fixef(g)[2:3]), maled=exp(fixef(m)[2:3]))
  out.prob <- rbind.data.frame(out.prob, odds)
}
ggplot(data=out.prob, aes(x=gems, y=maled, col=pathogen)) + geom_point(size=2) + facet_wrap(~age) + scale_x_continuous(limits=c(0,10))+
  scale_y_continuous(limits=c(0,10)) + geom_abline(intercept=0, slope=1) + theme_bw() +
  geom_vline(xintercept=1, lty=2) + geom_hline(yintercept=1, lty=2)
