#Generate  proportions for severity splits for HF due to Chagas
rm(list = setdiff(ls(), c(lsf.str(), "jpath", "hpath", "os")))
date <- gsub("-", "_", Sys.Date())

library(doBy)

df <- data.frame(study = rep(c(1, 2, 3, 5), each=8), sex=rep(rep(c("female", "male"), each=4),4), nyha=rep(rep(c(1, 2, 3, 4), 2), 4),
				 subjects=c(37, 6, 1, 0, 70, 4, 0, 0, 37, 10, 2, 0, 63, 8, 0, 1, 43, 62, 23, 13, 72, 78, 28, 21, 322, 28, 19, 20, 253, 32, 23, 28))
df$nyha.collapse <- with(df, ifelse(nyha==1, 2, nyha))				 

#Separates asymptomatic, mild, moderate, severe into 4 categories
df.all <- summaryBy(subjects~nyha, data=df, FUN=sum)
df.all$prop <- with(df.all, subjects.sum/sum(subjects.sum))
df.all$ess <- with(df.all, subjects.sum)
df.all$quantile <- 0.975
df.all$z <- qnorm(df.all$quantile)
df.all$se <- with(df.all, sqrt(prop * (1-prop)/ess + z^2/(4*ess^2)))

df.all$lower <- with(df.all, 1/(1 + z^2/ess) * (prop + z^2/(2*ess) - z * se))  
df.all$upper <- with(df.all, 1/(1 + z^2/ess) * (prop + z^2/(2*ess) + z * se))

##Used in GBD 2015 and GBD 2016; changing to a four-part split for GBD 2017
# #Groups asymptomatic and mild together, moderate, severe into 4 categories
# df.collapse <- summaryBy(subjects~nyha.collapse, data=df, FUN=sum)
# df.collapse$prop <- with(df.collapse, subjects.sum/sum(subjects.sum))