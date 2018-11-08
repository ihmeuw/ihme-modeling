###
#Code to calculate Dm outcomes by type
###

source(filepath)
source(filepath)
library(dplyr)

demographics <- get_demographics(gbd_team="epi")
ages <- unlist(demographics$age_group_id, use.names=F)
#Arguments
argue <- commandArgs(trailingOnly = T)
location <- as.numeric(argue[1])

#Directories
outdir <- filepath

#input ME's
me_id_t1<-model id for type 1
me_id_parent<-model id for overall Dm

  #vision loss
me_id_vl_mod<-model id for moderate vision loss
me_id_vl_sev<-model id for severe vision loss
me_id_vl_blind<-model id for blindness

  #Other sequelae
me_id_foot<-model id for foot proportion #keep proportion the same for now
me_id_neuro<-model id for neuropathy proportion #keep proportion the same for now
me_id_amp<-model id for amputation #keep proportion the same for now

age_group_id<-c('2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','30','31','32','235')

########
#Read in files
########

#Get draws for models

##Diabetes
prev_t1 <- data.frame(get_draws("modelable_entity_id", me_id_t1, "epi", measure_id=5, status="best",location_id=location, age_group_id=age_group_id))
prev_t1$type<-'1'

prev_parent <- data.frame(get_draws("modelable_entity_id", me_id_parent, source="epi", measure_id=5, status="best",location_id=location, age_group_id=age_group_id))
prev_parent$type<-'parent'

##Vision loss
vl_mod<- data.frame(get_draws("modelable_entity_id", me_id_vl_mod, source="epi", measure_id=5, status="best",location_id=location)) #moderate
vl_sev<- data.frame(get_draws("modelable_entity_id", me_id_vl_sev, source="epi", measure_id=5, status="best",location_id=location)) #severe
vl_blind<- data.frame(get_draws("modelable_entity_id", me_id_vl_blind, source="epi", measure_id=5, status="best",location_id=location)) #blind

##Amputation
prop_amp_t1<- data.frame(get_draws("modelable_entity_id",me_id_amp, source="epi", measure_id=5,gbd_round_id=5,status="best",location_id=location, age_group_id=age_group_id)) #amputation T1

##Ulcer
prop_ulcer_t1<- data.frame(get_draws("modelable_entity_id",me_id_foot, source="epi", measure_id=18,gbd_round_id=5,status="best",location_id=location, age_group_id=age_group_id)) #ulcer T1

##Neuropathy
prop_neuro_t1<- data.frame(get_draws("modelable_entity_id",me_id_neuro, source="epi", measure_id=18,gbd_round_id=5, status="best",location_id=location, age_group_id=age_group_id)) #neuro T1

#########
#Format
#########

#reshape
prev_t1.1<-melt(prev_t1, id=c("location_id", 'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))
prev_parent.1<-melt(prev_parent, id=c("location_id",'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))

#Merge the parent and type 1 together
df<-merge(prev_t1.1,prev_parent.1, by=c("location_id", "age_group_id", "sex_id", "year_id","measure_id",'variable'), all=T)

df$prev_t2<-df$value.y-df$value.x #parent - t1.
df$prev_t1<-df$value.x #type 1
df$prev_t2[df$prev_t2<0]<-0 #assumption

#########
#Squeeze T1 and T2 into Parent
#########
df$prev_total<-df$prev_t2+df$prev_t1
df$prev_t1_ratio<-df$prev_t1/df$prev_total
df$prev_t2_ratio<-df$prev_t2/df$prev_total

df<-merge(df,prev_parent.1, by=c('location_id','age_group_id','sex_id','year_id','measure_id','variable'), all=T)
df$prev_t1_squeeze<-df$value*df$prev_t1_ratio
df$prev_t2_squeeze<-df$value*df$prev_t2_ratio

#Assumption
df$prev_t2_squeeze<-ifelse(df$age_group_id<7,0,df$prev_t2_squeeze)#impose assumption that the prevalence of T2 under 10 years is 0.
df$prev_t1_squeeze<-ifelse(df$age_group_id<7,df$value, df$prev_t1_squeeze) #impose assumption that the prevalence of T1 under 10 years is the same as the parent.
df$prev_t2_squeeze[df$prev_t2_squeeze<0]<-0

#########
#Calculate ratio to split T1/T2
#########

df$prev_t1<-df$prev_t1_squeeze
df$prev_t2<-df$prev_t2_squeeze

df<- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'prev_t1','prev_t2','prev_t1_ratio','prev_t2_ratio')]

######
#Vision loss
######
#Reshape files to long
vl_mod.1<-melt(vl_mod, id=c("location_id", 'model_version_id', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))
vl_sev.1<-melt(vl_sev, id=c("location_id",'model_version_id', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))
vl_blind.1<-melt(vl_blind, id=c("location_id",'model_version_id', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))

#merge parent onto child
df_vl_mod<-merge(vl_mod.1,df , by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)
df_vl_sev<-merge(vl_sev.1,df ,  by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)
df_vl_blind<-merge(vl_blind.1,df , by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)

#multiply proportions by the parent
df_vl_mod$t1<-df_vl_mod$prev_t1_ratio*df_vl_mod$value
df_vl_mod$t2<-df_vl_mod$prev_t2_ratio*df_vl_mod$value
df_vl_sev$t1<-df_vl_sev$prev_t1_ratio*df_vl_sev$value
df_vl_sev$t2<-df_vl_sev$prev_t2_ratio*df_vl_sev$value
df_vl_blind$t1<-df_vl_blind$prev_t1_ratio*df_vl_blind$value
df_vl_blind$t2<-df_vl_blind$prev_t2_ratio*df_vl_blind$value

#Add vision estimates together per type
keep<-c('location_id','age_group_id','sex_id','year_id','t1','t2','variable')
df_vl_mod<-df_vl_mod[keep]
df_vl_sev<-df_vl_sev[keep]
df_vl_blind<-df_vl_blind[keep]

vision.1<-merge(df_vl_mod, df_vl_sev,by=c('location_id','age_group_id','sex_id','year_id','variable'), type='full')
vision.2<-merge(vision.1,df_vl_blind,by=c('location_id','age_group_id','sex_id','year_id','variable'), type='full')

vision.2$mod.t1<-vision.2$t1.x
vision.2$sev.t1<-vision.2$t1.y
vision.2$blind.t1<-vision.2$t1
vision.2$mod.t2<-vision.2$t2.x
vision.2$sev.t2<-vision.2$t2.y
vision.2$blind.t2<-vision.2$t2

vision.2$vision.1<-vision.2$mod.t1+vision.2$sev.t1+vision.2$blind.t1
vision.2$vision.2<-vision.2$mod.t2+vision.2$sev.t2+vision.2$blind.t2

#Cast the dataframe and split for each type of VL
df_vl_mod_1.cast1 <- vision.2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'mod.t1')]
df_vl_mod_1.cast<-reshape(df_vl_mod_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_mod_1.cast) <- gsub("mod.t1.draw", "draw", names(df_vl_mod_1.cast))
write.csv(df_vl_mod_1.cast, paste0(outdir,filepath), row.names=F)

df_vl_mod_2.cast1 <- vision.2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'mod.t2')]
df_vl_mod_2.cast<-reshape(df_vl_mod_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_mod_2.cast) <- gsub("mod.t2.draw", "draw", names(df_vl_mod_2.cast))
write.csv(df_vl_mod_2.cast,paste0(outdir,filepath), row.names=F)

df_vl_sev_1.cast1 <- vision.2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'sev.t1')]
df_vl_sev_1.cast<-reshape(df_vl_sev_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_sev_1.cast) <- gsub("sev.t1.draw", "draw", names(df_vl_sev_1.cast))
write.csv(df_vl_sev_1.cast, paste0(outdir,filepath), row.names=F)

df_vl_sev_2.cast1 <- vision.2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'sev.t2')]
df_vl_sev_2.cast<-reshape(df_vl_sev_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_sev_2.cast) <- gsub("sev.t2.draw", "draw", names(df_vl_sev_2.cast))
write.csv(df_vl_sev_2.cast, paste0(outdir,filepath), row.names=F)

df_vl_blind_1.cast1 <- vision.2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'blind.t1')]
df_vl_blind_1.cast<-reshape(df_vl_blind_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_blind_1.cast) <- gsub("blind.t1.draw", "draw", names(df_vl_blind_1.cast))
write.csv(df_vl_blind_1.cast, paste0(outdir,filepath), row.names=F)

df_vl_blind_2.cast1 <- vision.2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'blind.t2')]
df_vl_blind_2.cast<-reshape(df_vl_blind_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_blind_2.cast) <- gsub("blind.t2.draw", "draw", names(df_vl_blind_2.cast))
write.csv(df_vl_blind_2.cast, paste0(outdir,filepath), row.names=F)

###################
#Outcomes split
##################
#Reshape files to long

#amputation
amp.1<-melt(prop_amp_t1, id=c("location_id",  "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','model_version_id','metric_id'))
amp.1$prop<-amp.1$value

#ulcer
ulcer.1<-melt(prop_ulcer_t1, id=c("location_id","model_version_id", "age_group_id", "sex_id", "year_id",'measure_id','model_version_id','modelable_entity_id','metric_id'))
ulcer.1$prop<-ulcer.1$value

#neuro
neuro.1<-melt(prop_neuro_t1, id=c("location_id","model_version_id",  "age_group_id", "sex_id", "year_id",'measure_id','model_version_id','modelable_entity_id','metric_id'))
neuro.1$prop<-neuro.1$value

#pull out columns of interest
amp.1<- amp.1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'prop')]
ulcer.1<- ulcer.1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'prop')]
neuro.1<- neuro.1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'prop')]

############
#merge parent onto child
############
amp.1$year_id[amp.1$year_id==2016]<-2017
ulcer.1$year_id[ulcer.1$year_id==2016]<-2017
neuro.1$year_id[neuro.1$year_id==2016]<-2017

amp.1$variable<-as.character(amp.1$variable)
ulcer.1$variable<-as.character(ulcer.1$variable)
neuro.1$variable<-as.character(neuro.1$variable)
df$variable<-as.factor(df$variable)

df_amp.1<-merge(amp.1,df ,  by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)
df_ulcer.1<-merge(ulcer.1,df , by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)
df_neuro.1<-merge(neuro.1,df ,  by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)

############
#multiply proportions by the parent
############
#Same proportion for non-vision loss outcomes
df_amp.1$t1<-df_amp.1$prop*df_amp.1$prev_t1
df_amp.1$t2<-df_amp.1$prop*df_amp.1$prev_t2
df_ulcer.1$t1<-df_ulcer.1$prop*df_ulcer.1$prev_t1
df_ulcer.1$t2<-df_ulcer.1$prop*df_ulcer.1$prev_t2
df_neuro.1$t1<-df_neuro.1$prop*df_neuro.1$prev_t1
df_neuro.1$t2<-df_neuro.1$prop*df_neuro.1$prev_t2

df_amp.1$type<-'amp'
df_ulcer.1$type<-'ulcer'
df_neuro.1$type<-'neuro'

#Add all the files together

vision.2$type<-'vision'
vision.2$t1<-vision.2$vision.1
vision.2$t2<-vision.2$vision.2
vision.2<- vision.2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'t1','t2','type')]

df_amp.1<- df_amp.1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'t1','t2','type')]
df_neuro.1<- df_neuro.1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'t1','t2','type')]
df_ulcer.1<- df_ulcer.1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'t1','t2','type')]

#############
out.neuro_vision<-merge(df_neuro.1, vision.2,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)
out.neuro_vision<-merge(out.neuro_vision, df,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)


#Rescale results to ensure that (neuropathy + vision loss) < 0.9 of parent
out.neuro_vision$rescale_neuro_1<-ifelse(((out.neuro_vision$t1.x+out.neuro_vision$t1.y)>(0.90*out.neuro_vision$prev_t1)),((out.neuro_vision$t1.x/(out.neuro_vision$t1.x+out.neuro_vision$t1.y))*(0.9*out.neuro_vision$prev_t1)),out.neuro_vision$t1.x ) #T1
out.neuro_vision$rescale_neuro_2<-ifelse(((out.neuro_vision$t2.x+out.neuro_vision$t2.y)>(0.90*out.neuro_vision$prev_t2)),((out.neuro_vision$t2.x/(out.neuro_vision$t2.x+out.neuro_vision$t2.y))*(0.9*out.neuro_vision$prev_t2)),out.neuro_vision$t2.x ) #T2


######
#Calculate uncomplicated DM
prev_uncomp<-out.neuro_vision
prev_uncomp$uncomp_t1<-ifelse(prev_uncomp$age_group_id<6,prev_uncomp$prev_t1, abs(prev_uncomp$prev_t1-prev_uncomp$rescale_neuro_1-prev_uncomp$t1.y)) #set a rule that there is a delay in potential sequelae occurring. no sequelae can occur in a type 1 dm <5 years
prev_uncomp$uncomp_t2<-ifelse(prev_uncomp$age_group_id<9,prev_uncomp$prev_t2,abs(prev_uncomp$prev_t2-prev_uncomp$rescale_neuro_2-prev_uncomp$t2.y)) #set a rule that there is a delay in potential sequelae occurring. no sequelae can occur in a type 2 dm <20 years

#Cast the dataframe and save uncomplicated DM
#T1
df_uncomp_1.cast1 <- prev_uncomp[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'uncomp_t1')]
df_uncomp_1.cast<-reshape(df_uncomp_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_1.cast) <- gsub("uncomp_t1.draw", "draw", names(df_uncomp_1.cast))
write.csv(df_uncomp_1.cast, paste0(outdir, filepath), row.names=F)

#t2
df_uncomp_2.cast1 <- prev_uncomp[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'uncomp_t2')]
df_uncomp_2.cast<-reshape(df_uncomp_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_2.cast) <- gsub("uncomp_t2.draw", "draw", names(df_uncomp_2.cast))
write.csv(df_uncomp_2.cast, paste0(outdir,filepath), row.names=F)


#Rescale results to ensure that (amputation + foot ulcer) < 0.9*neuropathy
out.neuro_vision.1<- out.neuro_vision[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_neuro_1','rescale_neuro_2')]

out.amp_ulcer<-merge(df_ulcer.1,df_amp.1,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)
out.amp_ulcer<-merge(out.neuro_vision.1,out.amp_ulcer,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)

out.amp_ulcer$rescale_amp_1<-ifelse(((out.amp_ulcer$t1.x+out.amp_ulcer$t1.y)>(0.90*out.amp_ulcer$rescale_neuro_1)),((out.amp_ulcer$t1.y/(out.amp_ulcer$t1.x+out.amp_ulcer$t1.y))*(0.9*out.amp_ulcer$rescale_neuro_1)),out.amp_ulcer$t1.y ) #amp_T1
out.amp_ulcer$rescale_amp_2<-ifelse(((out.amp_ulcer$t2.x+out.amp_ulcer$t2.y)>(0.90*out.amp_ulcer$rescale_neuro_2)),((out.amp_ulcer$t2.y/(out.amp_ulcer$t2.x+out.amp_ulcer$t2.y))*(0.9*out.amp_ulcer$rescale_neuro_2)),out.amp_ulcer$t2.y ) #amp_T2

out.amp_ulcer$rescale_ulcer_1<-ifelse(((out.amp_ulcer$t1.x+out.amp_ulcer$t1.y)>(0.90*out.amp_ulcer$rescale_neuro_1)),((out.amp_ulcer$t1.x/(out.amp_ulcer$t1.x+out.amp_ulcer$t1.y))*(0.9*out.amp_ulcer$rescale_neuro_1)),out.amp_ulcer$t1.x ) #ulcer_T1
out.amp_ulcer$rescale_ulcer_2<-ifelse(((out.amp_ulcer$t2.x+out.amp_ulcer$t2.y)>(0.90*out.amp_ulcer$rescale_neuro_2)),((out.amp_ulcer$t2.x/(out.amp_ulcer$t2.x+out.amp_ulcer$t2.y))*(0.9*out.amp_ulcer$rescale_neuro_2)),out.amp_ulcer$t2.x ) #ulcer_T2

#############
#Incidence
#############

#Get draws for incidence of T1 and Parent
inc_t1 <- data.frame(get_draws("modelable_entity_id", me_id_t1, "epi", measure_id=6, status="best",location_id=location, age_group_id=age_group_id))
inc_t1$type<-'1'
inc_parent <- data.frame(get_draws("modelable_entity_id", me_id_parent, source="epi", measure_id=6, status="best",location_id=location, age_group_id=age_group_id))
inc_parent$type<-'parent'

#reshape
inc_t1.1<-melt(inc_t1, id=c("location_id", 'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))
inc_parent.1<-melt(inc_parent, id=c("location_id",'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))

#Merge the parent and type 1 together
df<-merge(inc_t1.1,inc_parent.1, by=c("location_id", "age_group_id", "sex_id", "year_id","measure_id",'variable'), all=T)
df$inc_parent<-df$value.y
df$inc_t1<-df$value.x

df$inc_t2<-abs(df$inc_parent-df$inc_t1) #impose a bound so no incidence can be <0
df$inc_t2<-ifelse(df$age_group_id<7,0,df$inc_t2) #impose no t2 incidence in people <10 years.

df_inc1<- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t1')]
df_inc2<- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t2')]


#T1
df_uncomp_1.cast1_inc <- df_inc1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t1')]
df_uncomp_1.cast<-reshape(df_uncomp_1.cast1_inc , idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_1.cast) <- gsub("inc_t1.draw", "draw", names(df_uncomp_1.cast))
write.csv(df_uncomp_1.cast, paste0(outdir, filepath), row.names=F)

#t2
df_uncomp_2.cast1_inc <- df_inc2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t2')]
df_uncomp_2.cast<-reshape(df_uncomp_2.cast1_inc, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_2.cast) <- gsub("inc_t2.draw", "draw", names(df_uncomp_2.cast))
write.csv(df_uncomp_2.cast, paste0(outdir, filepath), row.names=F)

########
#######
#Save Results
########
#######

#Cast the dataframe and save outcomes
#T1
df_neuro_1 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_neuro_1')]
df_neuro_1.cast<-reshape(df_neuro_1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_neuro_1.cast) <- gsub("rescale_neuro_1.draw", "draw", names(df_neuro_1.cast))
write.csv(df_neuro_1.cast, paste0(outdir, filepath), row.names=F)

df_amp_1 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_amp_1')]
df_amp_1.cast<-reshape(df_amp_1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_amp_1.cast) <- gsub("rescale_amp_1.draw", "draw", names(df_amp_1.cast))
write.csv(df_amp_1.cast, paste0(outdir, filepath), row.names=F)

df_ulcer_1 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_ulcer_1')]
df_ulcer_1.cast<-reshape(df_ulcer_1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_ulcer_1.cast) <- gsub("rescale_ulcer_1.draw", "draw", names(df_ulcer_1.cast))
write.csv(df_ulcer_1.cast, paste0(outdir, filepath), row.names=F)


#t2
df_neuro_2 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_neuro_2')]
df_neuro_2.cast<-reshape(df_neuro_2 , idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_neuro_2.cast) <- gsub("rescale_neuro_2.draw", "draw", names(df_neuro_2.cast))
write.csv(df_neuro_2.cast, paste0(outdir, filepath), row.names=F)

df_amp_2 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_amp_2')]
df_amp_2.cast<-reshape(df_amp_2, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_amp_2.cast) <- gsub("rescale_amp_2.draw", "draw", names(df_amp_2.cast))
write.csv(df_amp_2.cast, paste0(outdir, filepath), row.names=F)

df_ulcer_2 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_ulcer_2')]
df_ulcer_2.cast<-reshape(df_ulcer_2, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_ulcer_2.cast) <- gsub("rescale_ulcer_2.draw", "draw", names(df_ulcer_2.cast))
write.csv(df_ulcer_2.cast, paste0(outdir, filepath), row.names=F)



