###
#Code to calculate proportion split of outcomes using the ratio of T1 and T2
###

source("FILEPATH")
source("FILEPATH")

demographics <- get_demographics(gbd_team="epi", gbd_round_id=7)
ages <- unlist(demographics$age_group_id, use.names=F)

#Arguments
argue <- commandArgs(trailingOnly = T)
locations <- as.numeric(argue[1])

#Directories
outdir <- "FILEPATH"
location<-locations

#input MEs
me_id_t1<-24633
me_id_parent<-24632
me_id_t2<-24634

#vision loss
me_id_vl_mod<-2014
me_id_vl_sev<-2015
me_id_vl_blind<-2016

#other sequelae
me_id_foot<-2007
me_id_neuro<-2008
me_id_amp<-2010

age_group_id <- ages
step<-'iterative'
gbd_round<-7

########
#Read in files
########

#Get draws for prevalence of T1 and Parent
prev_t1 <- data.table(get_draws("modelable_entity_id", me_id_t1, "epi", measure_id=5, gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations))
prev_t1$type<-'1'
prev_parent <- data.table(get_draws("modelable_entity_id", me_id_parent, source="epi", measure_id=5,gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations))
prev_parent$type<-'parent'


##V.L
vl_mod<-data.table(get_draws("modelable_entity_id", me_id_vl_mod,  "epi", measure_id=5, gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations)) #moderate
vl_sev<- data.table(get_draws("modelable_entity_id", me_id_vl_sev,  "epi", measure_id=5, gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations)) #severe
vl_blind<- data.table(get_draws("modelable_entity_id", me_id_vl_blind, "epi",  measure_id=5, gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations))#blind

##Amputation
prop_amp_t1<- data.table(get_draws("modelable_entity_id",me_id_amp, source="epi", measure_id=5, gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations)) #amputation T1

##Ulcer
prop_ulcer_t1<-data.table(get_draws("modelable_entity_id",me_id_foot, source="epi", measure_id=18, gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations)) #ulcer T1

##Neuropathy
prop_neuro_t1<- data.table(get_draws("modelable_entity_id",me_id_neuro, source="epi", measure_id=18, gbd_round_id=gbd_round, age_group_id=age_group_id, decomp_step=step, location_id=locations)) #neuro T1

#########
#Format
#########
#reshape
prev_t1.1<-melt(prev_t1, id=c("location_id", 'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))
prev_parent.1<-melt(prev_parent, id=c("location_id",'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))

####Type 2
#SUBTRACTION: calculate type 2 by subtraction

#Merge the parent and type 1 together to create a single dataframe with t1, t2 and overall dm.
df<-merge(prev_t1.1,prev_parent.1, by=c("location_id", "age_group_id", "sex_id", "year_id","measure_id",'variable'), all=T)
df$prev_t1<-df$value.x #type 1
df$prev_parent<-df$value.y #parent

# If t1 is > parent, replace parent with t1 estimates. T2 will then be 0 for those values
df$prev_parent<-ifelse(df$prev_t1 > df$prev_parent,df$prev_t1, df$prev_parent)

# For <15 years, replace parent with t1 estimates
df$prev_parent<-ifelse(df$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7),df$prev_t1, df$prev_parent)

#create type 2 from subtraction method
df$prev_t2<-df$prev_parent-df$prev_t1 #parent - t1

#########
#Squeeze T1 and T2 into Parent
#########
df$prev_total<-df$prev_t2+df$prev_t1
df$prev_t1_ratio<-df$prev_t1/df$prev_total
df$prev_t2_ratio<-df$prev_t2/df$prev_total
df$prev_t2_ratio<-ifelse(df$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7),0, df$prev_t2_ratio) #force all diabetes <15 years to be Type 1 only
df$prev_t1_ratio<-ifelse(df$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7),1,df$prev_t1_ratio) #force all diabetes <15 years to be Type 1 only


df$prev_t1_squeeze<-df$prev_total*df$prev_t1_ratio
df$prev_t2_squeeze<-df$prev_total*df$prev_t2_ratio

#########
#relabel prevalences to the squeezed versions and clean dataframe
#########

df$prev_t1<-df$prev_t1_squeeze
df$prev_t2<-df$prev_t2_squeeze

df<- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'prev_t1','prev_t2','prev_t1_ratio','prev_t2_ratio')]

#######
#Create overall type 2 estimates
#######
df_2.cast1 <- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'prev_t2')]
df_2.cast<-reshape(df_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_2.cast) <- gsub("prev_t2.draw", "draw", names(df_2.cast))
write.csv(df_2.cast, paste0(outdir,'me_24634/', locations, ".csv"), row.names=F)


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

#multiply proportions by the parent to split out types of vision loss into type-specific
df_vl_mod$t1<-df_vl_mod$prev_t1_ratio*df_vl_mod$value
df_vl_mod$t2<-df_vl_mod$prev_t2_ratio*df_vl_mod$value
df_vl_sev$t1<-df_vl_sev$prev_t1_ratio*df_vl_sev$value
df_vl_sev$t2<-df_vl_sev$prev_t2_ratio*df_vl_sev$value
df_vl_blind$t1<-df_vl_blind$prev_t1_ratio*df_vl_blind$value
df_vl_blind$t2<-df_vl_blind$prev_t2_ratio*df_vl_blind$value


###############################
###############################

#########################
#####Figure out how much dm in other types of sequelae
#########################

###########
#Aggregate vision loss to calculate total VL
###########
#type of vl
df_vl_mod$source<-'moderate vl'
df_vl_sev$source<-'severe vl'
df_vl_blind$source<-'blindness'

#bind all the vision loss together
vl<-rbind(df_vl_blind,df_vl_sev, df_vl_mod)

#aggregate for total vl
vl.1<-aggregate(t1~location_id+age_group_id+sex_id+year_id+variable, vl, FUN=sum)
vl.2<-aggregate(t2~location_id+age_group_id+sex_id+year_id+variable, vl, FUN=sum)

vl.1$variable<-as.character(vl.1$variable)
vl.2$variable<-as.character(vl.2$variable)

vision<-merge(vl.1, vl.2, by=c('location_id','age_group_id','sex_id','year_id','variable'), all=T)
vision$type<-'vision loss'

#Calculate proportion of different types of vision loss against vision loss envelope
vision_prop<-merge(vl, vision, by=c('location_id','age_group_id','sex_id','year_id','variable'), all=T)
vision_prop$prop_t1<-vision_prop$t1.x/vision_prop$t1.y
vision_prop$prop_t2<-vision_prop$t2.x/vision_prop$t2.y

#rules
vision_prop$prop_t1[vision_prop$age_group_id %in% c(2, 3, 388, 389, 238, 34)]<-0 #set a rule that there is a delay in potential sequelae occurring. no sequelae can occur in a type 1 dm <5 years
vision_prop$prop_t2[vision_prop$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7, 8)]<-0 #set a rule that there is a delay in potential sequelae occurring. no sequelae can occur in a type 2 dm <20 years
vision_prop$prop_t1[is.na(vision_prop$prop_t1)]<-0
vision_prop$prop_t2[is.na(vision_prop$prop_t2)]<-0

vision_prop<- vision_prop[,c("age_group_id", "sex_id", "location_id", "year_id", "prop_t1","prop_t2","source","variable")]


###########
#Reshape other sequelae files to long
###########

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

amp.1$variable<-as.character(amp.1$variable)
ulcer.1$variable<-as.character(ulcer.1$variable)
neuro.1$variable<-as.character(neuro.1$variable)
df$variable<-as.character(df$variable)

df_amp<-merge(amp.1,df ,  by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)
df_ulcer<-merge(ulcer.1,df , by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)
df_neuro<-merge(neuro.1,df ,  by=c("location_id","age_group_id", "sex_id", "year_id",'variable'), all.x=T)

############
#multiply proportions by the parent
############
#Same proportion for non-vision loss outcomes
df_amp$t1<-df_amp$prop*df_amp$prev_t1
df_amp$t2<-df_amp$prop*df_amp$prev_t2
df_ulcer$t1<-df_ulcer$prop*df_ulcer$prev_t1
df_ulcer$t2<-df_ulcer$prop*df_ulcer$prev_t2
df_neuro$t1<-df_neuro$prop*df_neuro$prev_t1
df_neuro$t2<-df_neuro$prop*df_neuro$prev_t2

df_amp$type<-'amp'
df_ulcer$type<-'ulcer'
df_neuro$type<-'neuro'

#Add all the files together
df_amp<- df_amp[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'t1','t2','type')]
df_neuro<- df_neuro[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'t1','t2','type')]
df_ulcer<- df_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'t1','t2','type')]

#############
#Rescale neuropathy and vision if necessary
out.neuro_vision<-merge(df_neuro, vision,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)
out.neuro_vision<-merge(out.neuro_vision, df,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)

#Rescale results to ensure that (neuropathy + vision loss) < 0.9 of parent
out.neuro_vision$rescale_neuro_1<-ifelse(((out.neuro_vision$t1.x+out.neuro_vision$t1.y)>(0.90*out.neuro_vision$prev_t1)),((out.neuro_vision$t1.x/(out.neuro_vision$t1.x+out.neuro_vision$t1.y))*(0.9*out.neuro_vision$prev_t1)),out.neuro_vision$t1.x ) #T1- rescale
out.neuro_vision$rescale_neuro_2<-ifelse(((out.neuro_vision$t2.x+out.neuro_vision$t2.y)>(0.90*out.neuro_vision$prev_t2)),((out.neuro_vision$t2.x/(out.neuro_vision$t2.x+out.neuro_vision$t2.y))*(0.9*out.neuro_vision$prev_t2)),out.neuro_vision$t2.x ) #T2- rescale
out.neuro_vision$rescale_vision_1<-ifelse(((out.neuro_vision$t1.x+out.neuro_vision$t1.y)>(0.90*out.neuro_vision$prev_t1)),((out.neuro_vision$t1.y/(out.neuro_vision$t1.x+out.neuro_vision$t1.y))*(0.9*out.neuro_vision$prev_t1)),out.neuro_vision$t1.y ) #T1- rescale
out.neuro_vision$rescale_vision_2<-ifelse(((out.neuro_vision$t2.x+out.neuro_vision$t2.y)>(0.90*out.neuro_vision$prev_t2)),((out.neuro_vision$t2.y/(out.neuro_vision$t2.x+out.neuro_vision$t2.y))*(0.9*out.neuro_vision$prev_t2)),out.neuro_vision$t2.y ) #T2- rescale

out.neuro_vision<- out.neuro_vision[,c("age_group_id", "sex_id", "location_id", "year_id", "variable","rescale_vision_2","rescale_vision_1","rescale_neuro_2","rescale_neuro_1","prev_t1","prev_t2")]

##########
#Split out rescaled vision loss into moderate, severe and blindness
vl_neuro<-merge(out.neuro_vision,vision_prop , by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"),all=T)
vl_neuro$vl_1<-vl_neuro$rescale_vision_1*vl_neuro$prop_t1
vl_neuro$vl_2<-vl_neuro$rescale_vision_2*vl_neuro$prop_t2

##################
#Cast the dataframe and split out each type of vision loss by type of DM.
#Save files
mod<-vl_neuro[vl_neuro$source=='moderate vl',]
df_vl_mod_1.cast1 <- mod[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'vl_1')]
df_vl_mod_1.cast<-reshape(df_vl_mod_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_mod_1.cast) <- gsub("vl_1.draw", "draw", names(df_vl_mod_1.cast))
write.csv(df_vl_mod_1.cast, paste0(outdir,'me_19716/', locations, ".csv"), row.names=F)

df_vl_mod_2.cast1 <- mod[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'vl_2')]
df_vl_mod_2.cast<-reshape(df_vl_mod_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_mod_2.cast) <- gsub("vl_2.draw", "draw", names(df_vl_mod_2.cast))
write.csv(df_vl_mod_2.cast, paste0(outdir,'me_19730/', locations, ".csv"), row.names=F)

sev<-vl_neuro[vl_neuro$source=='severe vl',]
df_vl_sev_1.cast1 <- sev[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'vl_1')]
df_vl_sev_1.cast<-reshape(df_vl_sev_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_sev_1.cast) <- gsub("vl_1.draw", "draw", names(df_vl_sev_1.cast))
write.csv(df_vl_sev_1.cast, paste0(outdir,'me_19717/', locations, ".csv"), row.names=F)

df_vl_sev_2.cast1 <- sev[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'vl_2')]
df_vl_sev_2.cast<-reshape(df_vl_sev_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_sev_2.cast) <- gsub("vl_2.draw", "draw", names(df_vl_sev_2.cast))
write.csv(df_vl_sev_2.cast, paste0(outdir,'me_19731/', locations, ".csv"), row.names=F)

blind<-vl_neuro[vl_neuro$source=='blindness',]
df_vl_blind_1.cast1 <- blind[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'vl_1')]
df_vl_blind_1.cast<-reshape(df_vl_blind_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_blind_1.cast) <- gsub("vl_1.draw", "draw", names(df_vl_blind_1.cast))
write.csv(df_vl_blind_1.cast, paste0(outdir,'me_19718/', locations, ".csv"), row.names=F)

df_vl_blind_2.cast1 <- blind[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'vl_2')]
df_vl_blind_2.cast<-reshape(df_vl_blind_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_vl_blind_2.cast) <- gsub("vl_2.draw", "draw", names(df_vl_blind_2.cast))
write.csv(df_vl_blind_2.cast, paste0(outdir,'me_19732/', locations, ".csv"), row.names=F)

##############################

#############
#calculate uncomplicated DM= DM with no vision loss and no nueropathy.
prev_uncomp<-out.neuro_vision
#set a rule that there is a delay in potential sequelae occurring. no sequelae can occur in a type 1 dm <5 years
prev_uncomp$rescale_vision_1[prev_uncomp$age_group_id %in% c(2, 3, 388, 389, 238, 34)]<-0
prev_uncomp$rescale_neuro_1[prev_uncomp$age_group_id %in% c(2, 3, 388, 389, 238, 34)]<-0

#set a rule that there is a delay in potential sequelae occurring. no sequelae can occur in a type 2 dm <20 years
prev_uncomp$rescale_vision_2[prev_uncomp$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7, 8)]<-0
prev_uncomp$rescale_neuro_2[prev_uncomp$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7, 8)]<-0

prev_uncomp$uncomp_t1<-prev_uncomp$prev_t1-prev_uncomp$rescale_neuro_1 -prev_uncomp$rescale_vision_1
prev_uncomp$uncomp_t2<-prev_uncomp$prev_t2-prev_uncomp$rescale_neuro_2 -prev_uncomp$rescale_vision_2


##############################
#Cast the dataframe and save uncomplicated DM
#T1
df_uncomp_1.cast1 <- prev_uncomp[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'uncomp_t1')]
df_uncomp_1.cast<-reshape(df_uncomp_1.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_1.cast) <- gsub("uncomp_t1.draw", "draw", names(df_uncomp_1.cast))
write.csv(df_uncomp_1.cast, paste0(outdir,'me_19706/5_', locations, ".csv"), row.names=F)

#t2
df_uncomp_2.cast1 <- prev_uncomp[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'uncomp_t2')]
df_uncomp_2.cast<-reshape(df_uncomp_2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_2.cast) <- gsub("uncomp_t2.draw", "draw", names(df_uncomp_2.cast))
write.csv(df_uncomp_2.cast, paste0(outdir,'me_19720/5_', locations, ".csv"), row.names=F)
##############################

########################

#Rescale results to ensure that (amputation + foot ulcer) < 0.9*neuropathy
out.neuro_vision.1<- out.neuro_vision[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_neuro_1','rescale_neuro_2','rescale_vision_1','rescale_vision_2')]

out.amp_ulcer<-merge(df_ulcer,df_amp,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)
out.amp_ulcer<-merge(out.neuro_vision,out.amp_ulcer,by=c("age_group_id", "sex_id", "location_id", "year_id", "variable"), all=T)

out.amp_ulcer$rescale_amp_1<-ifelse(((out.amp_ulcer$t1.x+out.amp_ulcer$t1.y)>(0.90*out.amp_ulcer$rescale_neuro_1)),((out.amp_ulcer$t1.y/(out.amp_ulcer$t1.x+out.amp_ulcer$t1.y))*(0.9*out.amp_ulcer$rescale_neuro_1)),out.amp_ulcer$t1.y ) #amp_T1
out.amp_ulcer$rescale_amp_2<-ifelse(((out.amp_ulcer$t2.x+out.amp_ulcer$t2.y)>(0.90*out.amp_ulcer$rescale_neuro_2)),((out.amp_ulcer$t2.y/(out.amp_ulcer$t2.x+out.amp_ulcer$t2.y))*(0.9*out.amp_ulcer$rescale_neuro_2)),out.amp_ulcer$t2.y ) #amp_T2

out.amp_ulcer$rescale_ulcer_1<-ifelse(((out.amp_ulcer$t1.x+out.amp_ulcer$t1.y)>(0.90*out.amp_ulcer$rescale_neuro_1)),((out.amp_ulcer$t1.x/(out.amp_ulcer$t1.x+out.amp_ulcer$t1.y))*(0.9*out.amp_ulcer$rescale_neuro_1)),out.amp_ulcer$t1.x ) #ulcer_T1
out.amp_ulcer$rescale_ulcer_2<-ifelse(((out.amp_ulcer$t2.x+out.amp_ulcer$t2.y)>(0.90*out.amp_ulcer$rescale_neuro_2)),((out.amp_ulcer$t2.x/(out.amp_ulcer$t2.x+out.amp_ulcer$t2.y))*(0.9*out.amp_ulcer$rescale_neuro_2)),out.amp_ulcer$t2.x ) #ulcer_T2


##############################
#Cast the dataframe and save neuro, amp, and ulcer
#T1
df_neuro_1 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_neuro_1')]
df_neuro_1.cast<-reshape(df_neuro_1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_neuro_1.cast) <- gsub("rescale_neuro_1.draw", "draw", names(df_neuro_1.cast))
write.csv(df_neuro_1.cast, paste0(outdir,'/me_19709/', locations, ".csv"), row.names=F)

df_amp_1 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_amp_1')]
df_amp_1.cast<-reshape(df_amp_1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_amp_1.cast) <- gsub("rescale_amp_1.draw", "draw", names(df_amp_1.cast))
write.csv(df_amp_1.cast, paste0(outdir,'/me_19713/', locations, ".csv"), row.names=F)

df_ulcer_1 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_ulcer_1')]
df_ulcer_1.cast<-reshape(df_ulcer_1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_ulcer_1.cast) <- gsub("rescale_ulcer_1.draw", "draw", names(df_ulcer_1.cast))
write.csv(df_ulcer_1.cast, paste0(outdir,'/me_19707/', locations, ".csv"), row.names=F)


#t2
df_neuro_2 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_neuro_2')]
df_neuro_2.cast<-reshape(df_neuro_2 , idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_neuro_2.cast) <- gsub("rescale_neuro_2.draw", "draw", names(df_neuro_2.cast))
write.csv(df_neuro_2.cast, paste0(outdir,'me_19723/', locations, ".csv"), row.names=F)

df_amp_2 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_amp_2')]
df_amp_2.cast<-reshape(df_amp_2, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_amp_2.cast) <- gsub("rescale_amp_2.draw", "draw", names(df_amp_2.cast))
write.csv(df_amp_2.cast, paste0(outdir,'me_19727/', locations, ".csv"), row.names=F)

df_ulcer_2 <- out.amp_ulcer[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'rescale_ulcer_2')]
df_ulcer_2.cast<-reshape(df_ulcer_2, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_ulcer_2.cast) <- gsub("rescale_ulcer_2.draw", "draw", names(df_ulcer_2.cast))
write.csv(df_ulcer_2.cast, paste0(outdir,'me_19721/', locations, ".csv"), row.names=F)

##############################



#############
#Incidence
#############

#Get draws for incidence of T1 and Parent
inc_t1 <- data.frame(get_draws("modelable_entity_id", me_id_t1, "epi", measure_id=6,gbd_round=gbd_round, age_group_id=age_group_id, decomp_step = step, location_id=locations))
inc_t1$type<-'1'
inc_parent <- data.frame(get_draws("modelable_entity_id", me_id_parent,"epi", measure_id=6, gbd_round=gbd_round, age_group_id=age_group_id, decomp_step = step, location_id=locations))
inc_parent$type<-'parent'

#reshape
inc_t1.1<-melt(inc_t1, id=c("location_id", 'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))
inc_parent.1<-melt(inc_parent, id=c("location_id",'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))

#Merge the parent and type 1 together
df<-merge(inc_t1.1,inc_parent.1, by=c("location_id", "age_group_id", "sex_id", "year_id","measure_id",'variable'), all=T)
df$inc_parent<-df$value.y
df$inc_t1<-df$value.x

df$inc_parent<-ifelse(df$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7),df$inc_t1,df$inc_parent) #replace <15 parent with type 1 incidence 
df$inc_t2<-abs(df$inc_parent-df$inc_t1) #impose a bound so no incidence can be <0
df$inc_t2<-ifelse(df$age_group_id %in% c(2, 3, 388, 389, 238, 34, 6, 7),0,df$inc_t2) #impose no t2 incidence in people <15 years.

df_inc1<- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t1')]
df_inc2<- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t2')]


#T1
df_uncomp_1.cast1_inc <- df_inc1[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t1')]
df_uncomp_1.cast<-reshape(df_uncomp_1.cast1_inc , idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_1.cast) <- gsub("inc_t1.draw", "draw", names(df_uncomp_1.cast))
write.csv(df_uncomp_1.cast, paste0(outdir,'me_19706/6_', locations, ".csv"), row.names=F)

#t2
df_uncomp_2.cast1_inc <- df_inc2[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'inc_t2')]
df_uncomp_2.cast<-reshape(df_uncomp_2.cast1_inc, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_uncomp_2.cast) <- gsub("inc_t2.draw", "draw", names(df_uncomp_2.cast))
write.csv(df_uncomp_2.cast, paste0(outdir,'me_19720/6_', locations, ".csv"), row.names=F)

