###
#Code to calculate create t2 diabetes from subtraction steps.
#Only use if type 2 model isn't run.
#Creates draws that are stored in the database
###

source("FILEPATH")
source("FILEPATH")

library(dplyr)


demographics <- get_demographics(gbd_team="epi",gbd_round_id=6)
ages <- unlist(demographics$age_group_id, use.names=F)
#Arguments
argue <- commandArgs(trailingOnly = T)
locations <- as.numeric(argue[1])

#Directories
outdir <- "FILEPATH"

#input ME's
me_id_t1<-24633
me_id_parent<-24632
me_id_t2<-24634

age_group_id<-c('2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','30','31','32','235')

step<-'step4'
########
#Read in files
########

#Get draws for prevalence of T1 and Parent
prev_t1 <-     data.frame(get_draws("modelable_entity_id", me_id_t1, "epi",     measure_id=5, gbd_round_id=6,location_id=locations, age_group_id=age_group_id, decomp_step=step))
prev_t1$type<-'1'
prev_parent <- data.frame(get_draws("modelable_entity_id", me_id_parent, "epi", measure_id=5, gbd_round_id=6,location_id=locations, age_group_id=age_group_id, decomp_step=step))
prev_parent$type<-'parent'


#########
#Format
#########
#reshape
prev_t1.1<-    melt(prev_t1,     id=c("location_id", 'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))
prev_parent.1<-melt(prev_parent, id=c("location_id",'model_version_id','type', "age_group_id", "sex_id", "year_id",'measure_id','modelable_entity_id','metric_id'))


#################
# type 2 models are run as a subtraction

#Merge the parent and type 1 together to create a single dataframe with t1, t2 and overall dm.
df<-merge(prev_t1.1,prev_parent.1, by=c("location_id", "age_group_id", "sex_id", "year_id","measure_id",'variable'), all=T) # (if no prev type 2 is pulled)
df$prev_t1<-df$value.x #type 1
df$prev_parent<-df$value.y #parent

# assumption: If t1 is > parent, replace parent with t1 estimates. T2 will then be 0 for those values
df$prev_parent<-ifelse(df$prev_t1 > df$prev_parent,df$prev_t1, df$prev_parent)

#create type 2 from subtraction method
df$prev_t2<-df$prev_parent-df$prev_t1 #parent - t1



#Save files of draws

df_type2.cast1 <- df[,c("age_group_id", "sex_id", "location_id", "year_id", "variable",'prev_t2')]
df_type2.cast<-reshape(df_type2.cast1, idvar=c("age_group_id", "sex_id", "location_id", "year_id"), timevar="variable", direction="wide")
names(df_type2.cast) <- gsub("prev_t2.draw", "draw", names(df_type2.cast))
write.csv(df_type2.cast, paste0(outdir,'me_18656/', locations, ".csv"), row.names=F)
