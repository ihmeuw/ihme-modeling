#################################################
#    Proportion of cases underreported          #
# created: 18th August 2017 by USERNAME         #
# last updated: 18th August 2017 by USERNAME    #
#################################################

#set filepath
filepath<-c("FILEPATH")

#load in data
underreport<-read.csv(paste0(filepath,'underreporting_means_only.csv'),
                 stringsAsFactors = FALSE)

#create metafor object
#xi = numerator / number of positives
#ni = denominator / total number sampled
# PR as is a proportion

underreport_metafor<-escalc(measure = 'PR',
                        xi=underreport$reported_cases,
                        ni=underreport$total_n)

#model using Der-Simionian Random Effects
modeldl_underreport<-rma(yi=underreport_metafor$yi,
                    vi=underreport_metafor$vi,
                    method='DL')

#produce forest plot
pdf(file=paste0(filepath,Sys.Date(),'_proportion_reported.pdf'))
forest(modeldl_underreport,
       slab=underreport$study,
       order='prec',
       refline=c(modeldl_underreport$b,modeldl_underreport$ci.lb,modeldl_underreport$ci.ub)
)

dev.off()

###############################
#takes 1,000 draws uniformly from within prediction interval
rand_prop<-runif(1000,modeldl_underreport$ci.lb, modeldl_underreport$ci.ub)

#convert into scalar max bound
scalar_underreport_max<-1/rand_prop

#convert to 1,000 underreporting scalars
scalar_underreport<-NA
for (i in 1:1000){
  scalar_underreport[i]<-runif(1, 1, sample(scalar_underreport_max,1))
}

pdf(file=paste0(filepath,
                Sys.Date(),
                '_underreporting_bauplan.pdf'))
par(mfrow=c(1,2))

hist(scalar_underreport_max,
     main='Upper Bound of Underreporting',
     xlab=paste0('Max limit: ',
                 round(mean(scalar_underreport_max),2),
                 ' [CI:',
                 round(quantile(scalar_underreport_max,0.05),2),
                 '-',
                 round(quantile(scalar_underreport_max,0.95),2),
                 ']')
     )
hist(scalar_underreport,
     main='Underreporting factor',
     xlab=paste0('Correction scalar: ',
                 round(mean(scalar_underreport),2),
                 ' [CI:',
                 round(quantile(scalar_underreport,0.05),2),
                 '-',
                 round(quantile(scalar_underreport,0.95),2),
                 ']')
)
dev.off()
