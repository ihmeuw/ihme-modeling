########
#Process NHANES codes for BMD
#Uses
#1. SD
#2. Exposure
#3. TMREL
#4. Distribution
########

library(readstata13)
mround <- function(x,base){
  base*round(x/base)
}

##################
#Read in NHANES files
##################
#NHANES 2007
nhanes<-read.dta13('FILEPATH')
demo<-read.dta13('FILEPATH')
demo$sex<-demo$riagendr
demo$age<-demo$ridageyr

nhanes$nid<-25914
nhanes$year_start<-2007
nhanes$year_end<-2008
nhanes$field_citation_value<-'National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC). United States National Health and Nutrition Examination Survey 2007-2008. Hyattsville, United States: National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC), 2009.'
nhanes$mean<-nhanes$dxxnkbmd
nhanes$unit<-'gm/cm2'

nhanes.1<-merge(nhanes, demo, by='seqn', all.x=T)
nhanes_25914<-nhanes.1

#NHANES 2009
nhanes<-read.dta13('FILEPATH')
demo<-read.dta13('FILEPATH')
demo$sex<-demo$riagendr
demo$age<-demo$ridageyr
nhanes$nid<-48332
nhanes$year_start<-2009
nhanes$year_end<-2010
nhanes$field_citation_value<-'National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC). United States National Health and Nutrition Examination Survey 2009-2010. Hyattsville, United States: National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC), 2011.'
nhanes$mean<-nhanes$dxxnkbmd
nhanes$unit<-'gm/cm2'

nhanes.1<-merge(nhanes, demo, by='seqn', all.x=T)
nhanes_48332<-nhanes.1

#NHANES 2013
nhanes<-read.dta13('FILEPATH')
demo<-read.dta13('FILEPATH')
demo$sex<-demo$riagendr
demo$age<-demo$ridageyr
nhanes$nid<-165892
nhanes$year_start<-2013
nhanes$year_end<-2014
nhanes$field_citation_value<-'National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC). United States National Health and Nutrition Examination Survey 2013-2014. Hyattsville, United States: National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC).'
nhanes$mean<-nhanes$dxxnkbmd
nhanes$unit<-'gm/cm2'

nhanes.1<-merge(nhanes, demo, by='seqn', all.x=T)
nhanes_165892<-nhanes.1

#NHANES 2005
nhanes<-read.dta13('FILEPATH')
demo<-read.dta13('FILEPATH')
demo$sex<-demo$riagendr
demo$age<-demo$ridageyr
nhanes$nid<-47478
nhanes$year_start<-2005
nhanes$year_end<-2006
nhanes$field_citation_value<-'National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC). United States National Health and Nutrition Examination Survey 2005-2006. Hyattsville, United States: National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC), 2007.'
nhanes$mean<-nhanes$dxxnkbmd
nhanes$unit<-'gm/cm2'

nhanes.1<-merge(nhanes, demo, by='seqn', all.x=T)
nhanes_47478<-nhanes.1

#NHANES 1998

nhanes<-read.dta13('FILEPATH')
nhanes$sex<-nhanes$hssex
nhanes$age<-nhanes$hsageir
nhanes$nid<-48604
nhanes$year_start<-1988
nhanes$year_end<-1994
nhanes$field_citation_value<-'National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC). United States National Health and Nutrition Examination Survey 1988-1994. Hyattsville, United States: National Center for Health Statistics (NCHS), Centers for Disease Control and Prevention (CDC).'
nhanes$mean<-nhanes$bdpfnbmd
nhanes$unit<-'gm/cm2'

nhanes.1<-nhanes
#nhanes.1<-merge(nhanes, demo, by='seqn', all.x=T)
nhanes.1<-nhanes.1[nhanes.1$mean<8000,] #outlier extreme values

nhanes_48604<-nhanes.1

###
#bind all the nhanes files

keep<-c('nid','seqn','age','sex','mean','unit','field_citation_value','year_start','year_end')
nhanes_165892<-nhanes_165892[keep]
nhanes_25914<-nhanes_25914[keep]
nhanes_47478<-nhanes_47478[keep]
nhanes_48332<-nhanes_48332[keep]
nhanes_48604<-nhanes_48604[keep]

bmd_nhanes<-rbind(nhanes_165892, nhanes_25914, nhanes_47478,nhanes_48332,nhanes_48604)
bmd_nhanes<-bmd_nhanes[!is.na(bmd_nhanes$mean),]

bmd_nhanes$age_start<-mround(bmd_nhanes$age,5)

#format
bmd_nhanes<-bmd_nhanes[bmd_nhanes$age>=20,] #subset to 20+ years
tapply(bmd_nhanes$age, bmd_nhanes$year_id, summary)
bmd_nhanes$ct<-1
aggregate(ct~year_id, bmd_nhanes, FUN=sum)

#####################
###CREATE files for different parts of the RF process
########################
#Ensemble distribution

#write out microdata file for distribution
bmd_nhanes$location_id<-102
bmd_nhanes$year_id<-bmd_nhanes$year_start
bmd_nhanes$age_year<-bmd_nhanes$age
bmd_nhanes$sex_id<-bmd_nhanes$sex
bmd_nhanes$data<-bmd_nhanes$mean
micro<-bmd_nhanes[,-which(names(bmd_nhanes) %in% c('seqn','age','sex','mean','unit','field_citation_value', 'year_start','year_end','age_start','age_end'))]

#1 file to run distribution in >70
dist_age.1<-micro[micro$age_year<70,]
write.csv(dist_age.1, 'FILEPATH/bmd_micro_nhanes_20_69yrs.csv')

#2 file to run distribution in <70
dist_age.2<-micro[micro$age_year>=70,]
write.csv(dist_age.2, 'FILEPATH/bmd_micro_nhanes_70yrs_plus.csv')

#3 fil to run for all ages
dist_age.3<-micro
write.csv(dist_age.3, 'FILEPATH/bmd_micro_nhanes_all_age.csv')

#TMREL
#99th percentile of NHANES 2005-2014 by age and sex

library(plyr)
library(dplyr)
prob=c(.99)

tmrel<-as.data.frame(summarise(group_by(bmd_nhanes,age_start,sex),
                               draw = quantile(mean, probs = prob[1])))

pdf('FILEPATH')
r<-ggplot(tmrel, aes(x=age_start, y=draw))+facet_grid(~sex)+geom_point()+  theme(panel.background = element_rect(fill='white', colour='black'))+ggtitle('BMD TMREL: 99th percentile of BMD (g/cm2) from \n femoral neck extracted from NHANES by age and sex')+xlab('Age')+ylab('BMD (g/cm2)')+coord_cartesian(ylim = c(0,1.7))
print(r)
dev.off()

write.csv(tmrel, 'FILEPATH')

#create draw file
age<-read.csv('FILEPATH')
tmrel<-merge(age, tmrel, by='age_start', all.y=T)

draw<-tmrel
draw<-draw[draw$age_start>=20,]

old<-draw[draw$age_group_id==32,]
old$age_group_id<-235

draw.1<-rbind(draw, old)
draw.1$location_id<-1
draw.2<-draw.1[,which(names(draw.1) %in% c('age_group_id','sex','draw','location_id'))]

x <- c( "1990","1995","2000","2005","2010","2017")
y<-seq(0,999,by=1)
nedf <- data.frame( "year_id" = x)
test<-data.frame('draw_'=y)
test.1<-merge(nedf,test, all=T)

draw.3<-merge(draw.2,test.1, all=T)
draw.3$year_id<-as.character(draw.3$year_id)

library(reshape2)
tmrel_bmd <- reshape(draw.3, idvar = c("age_group_id","sex","year_id","location_id"), timevar = "draw_", direction = "wide")
names(tmrel_bmd) <- gsub(x = names(tmrel_bmd), pattern = "\\.", replacement = "_")

write.csv(tmrel_bmd,'FILEPATH')

####

#SD

#subset data to >20 years

bmd_nhanes$sex_id<-ifelse(bmd_nhanes$sex==1,'Male','Female')
bmd_nhanes<-bmd_nhanes[bmd_nhanes$age>=20,]

pdf('FILEPATH')
p<-ggplot(bmd_nhanes, aes(x=age, y=mean, color=as.factor(year_start)))+facet_grid(~sex_id)+geom_point(alpha=0.5)
p<-p+theme(panel.background = element_rect(fill='white', colour='black'))+ggtitle('Recorded BMD (g/cm2) from femoral neck in NHANES')+xlab('Age')+ylab('BMD (g/cm2)')+coord_cartesian(ylim = c(0,1.7))

#Collapsed bmd
bmd_nhanes$age_start<-mround(bmd_nhanes$age,5)
bmd_nhanes$age_end<-bmd_nhanes$age_start+4
collapse<-aggregate(mean~age_start+age_end+sex_id+year_start+year_end+nid, bmd_nhanes, FUN=mean)

q<-ggplot(collapse, aes(x=age_start, y=mean, color=as.factor(year_start)))+facet_grid(~sex_id)+geom_line()+geom_point(alpha=0.5)+  theme(panel.background = element_rect(fill='white', colour='black'))+ggtitle('Mean BMD (g/cm2) from femoral neck in NHANES')+xlab('Age')+ylab('BMD (g/cm2)')+coord_cartesian(ylim = c(0,1.7))

#collapsed infromation for exposure model
bmd_nhanes$sample_size<-1
collapse_ct<-aggregate(sample_size~age_start+age_end+sex_id+year_start+year_end+nid, bmd_nhanes, FUN=sum)
collapse.1<-merge(collapse,collapse_ct,by=c('age_start','age_end','sex_id','year_start','year_end','nid'), all=T)
collapse.1$seq<-''
collapse.1$input_type<-'extracted'
collapse.1$source_type<-'Survey - other/unknown'
collapse.1$location_id<-102
collapse.1$sex<-collapse.1$sex_id
collapse.1$location_name<-'United States'
collapse.1$measure<-'continuous'
collapse.1$unit_type<-'Person'
collapse.1$unit_value_as_published <-1
collapse.1$representative_name <-'Unknown'
collapse.1$urbanicity_type<-'Unknown'
collapse.1$case_definition<-'BMD from femoral neck g/cm2'
collapse.1$extractor<-'ongl'
write.xlsx(collapse.1,'FILEPATH',sheetName='extraction')