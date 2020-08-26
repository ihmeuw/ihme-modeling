
######################
#Convert Prev DM to mean FPG
#######################
#use distribution-based method from Stan
rm(list = ls())

dm<-read.csv(file_path)
#read in files
match<-read.csv (file_path)
match$sex_id<-match$sex
match$prev_mean_1<-round(match$prev_mean,3)

match.1<-aggregate(mean~sex_id+prev_mean_1, FUN=mean, match)

#round 1: match at full digits
dm$dm<-'dm'
dm$prev_mean_1<-dm$value
dm$prev_mean_1<-round(dm$prev_mean,3)

match.1$match<-'match'

r.1<-merge(dm, match.1, by=c('sex_id','prev_mean_1'), all.x=T)
r.1$mean<-ifelse(is.na(r.1$match),12.5, r.1$mean)


table(r.1$dm, r.1$match,exclude=NULL)

library(ggplot2)
pdf(file_path)
p<-ggplot(r.1, aes(x=prev_mean_1, y=mean))+facet_wrap(sex_id~parameter)+geom_point()
print(p)
dev.off()

write.csv(r.1,file_path)
