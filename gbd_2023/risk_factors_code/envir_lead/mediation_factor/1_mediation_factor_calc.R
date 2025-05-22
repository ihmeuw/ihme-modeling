#calculate a mediation factor for bone lead and SBP


rm(list=ls())

#libraries ################################
library(readr)

# load in draws ##############################
#Load in the RR draws- remove the first empty column
draws<-fread("FILEPATH/nhanesInternalAnalysis_RR_draws.csv")[,-c("V1")]

#only keep the type=distribution since this is the exp levels that we have in our distribution weights file
draws<-draws[type=="distribution"]

#load in the exposure distribution
dist<-fread("FILEPATH/lead_distribution.csv")[,-c("rei_id","unit")]

# Merge dist and draws ###############################
draws<-merge(draws,dist,by.x="b0", by.y="exposure",all.x=T)

# split #############################
#grab the sbp adjusted draws and the non-sbp adjusted
#remove extra columns. We don't need location id since the rrs are not location-specific
sbp<-draws[sbp_adjusted==T & model=="adjFullPspline_cvd",-c("b1","type","model","sbp_adjusted","outcome","location_id")]
non_sbp<-draws[sbp_adjusted==F & model=="adjEduPspline_cvd",-c("b1","type","model","sbp_adjusted","outcome","location_id")]

# take exp of rr ##############################
sbp[,rr:=exp(rr)]
non_sbp[,rr:=exp(rr)]

# weighted mean ########################################
sbp[,mean_sbp:=weighted.mean(rr, w = density),by=c("draw")]
non_sbp[,mean_non_sbp:=weighted.mean(rr, w = density),by=c("draw")]

# edits ################################
#remove columns that we no longer need
sbp<-sbp[,-c("b0","rr","density")]
non_sbp<-non_sbp[,-c("b0","rr","density")]

# take the unique of the data to get 1 row for each draw
sbp<-unique(sbp)
non_sbp<-unique(non_sbp)

# merge the 2 datasets #######################
data<-merge(sbp,non_sbp,by="draw")

# Calculate the mediation factor UIs ###########################
#Mediation Factor = RRcrude - RRadjusted / (RRcrude - 1)
#RRcrude = non_sbp adjusted analysis
#RRadjusted = sbp adjusted analysis

data[,mf:=(mean_non_sbp-mean_sbp)/(mean_non_sbp-1)]

# find the mean, upper and lower UIs

data[,':='(mf_mean=mean(mf),
           mf_lower=quantile(mf,c(0.025)),
           mf_upper=quantile(mf,c(0.975)))]

# Export MF draws #################################
write_excel_csv(data[,c("draw","mf")],"FILEPATH/mediation_factor_draws.csv")

# calculate the "True mean" ###############################
#this mean is calculated from a point estimate 

# recreate the 2 datasets
#grab the sbp adjusted draws and the non-sbp adjusted
#remove extra columns. We don't need location id since the rrs are not location-specific
sbp<-draws[sbp_adjusted==T & model=="adjFullPspline_cvd",-c("b1","type","model","sbp_adjusted","outcome","location_id")]
non_sbp<-draws[sbp_adjusted==F & model=="adjEduPspline_cvd",-c("b1","type","model","sbp_adjusted","outcome","location_id")]


#take the mean, collapsing the draws (so each exposure has a mean)
sbp[,mean_sbp:=mean(rr),by=c("b0")]
non_sbp[,mean_non_sbp:=mean(rr),by=c("b0")]

# now remove columns we don't need
sbp<-sbp[,-c("draw","rr")]
non_sbp<-non_sbp[,-c("draw","rr")]

sbp<-unique(sbp)
non_sbp<-unique(non_sbp)

# take the exp(x) of the means
sbp[,mean_sbp:=exp(mean_sbp)]
non_sbp[,mean_non_sbp:=exp(mean_non_sbp)]

# take the weighted mean
sbp[,mean_sbp_final:=weighted.mean(mean_sbp, w = density)]
non_sbp[,mean_non_sbp_final:=weighted.mean(mean_non_sbp, w = density)]

# calculate the mediation factor
sbp_mean<-unique(sbp$mean_sbp_final)
non_sbp_mean<-unique(non_sbp$mean_non_sbp_final)

mf<-(non_sbp_mean-sbp_mean)/(non_sbp_mean-1)

#make a table that is the true mean and the UIs from the draws
final<-data.table(mean_mf = mf,
                     lower_mf = unique(data$mf_lower),
                     upper_mf = unique(data$mf_upper))

#export
write_excel_csv(final,"FILEPATH/mediation_factor.csv")

