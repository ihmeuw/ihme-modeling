  j_root <- 'FILEPATH'

require(data.table)

version_no<-2

# set output paths
output_path_summary_stats<-paste0('FILEPATH',version_no,"/")
output_path_bootstrap<-paste0('FILEPATH',version_no,"/")

## bring in the dataset
interpolation <- fread(paste0(j_root,"FILEPATH.csv"))
interpolation[,key:=as.numeric(key)]
Appended_MEPS_DW_ordered <- fread(paste0(j_root,"FILEPATH.csv"))
Appended_MEPS_DW_ordered[,key:=as.numeric(key)]
data <- merge(interpolation, Appended_MEPS_DW_ordered, by = c("key",'dw','predict','sf'), all=T,with=F)
data[,id:=as.numeric(id)]

# replace spaces in variable names with _, replaces commas with nothing, and - with _
setnames(data,names(data),gsub(" ","_",names(data)))
setnames(data,names(data),gsub(",","",names(data)))
setnames(data,names(data),gsub("-","_",names(data)))
setnames(data,names(data),gsub("\\(","",names(data)))
setnames(data,names(data),gsub("\\)","",names(data)))
setnames(data,names(data),gsub("/","_",names(data)))
setnames(data,names(data),tolower(names(data)))

# get list of conditions within MEPS
comos_data<-grep('^t',names(data),value=T)

# get list of conditions for which we need DWs
comos_to_predict<-fread(paste0(j_root,"FILEPATH.csv"))
comos_to_predict<-comos_to_predict[!yld_cause==""]

# create map from source to MEPS dummy var
source<-unique(comos_to_predict[,source])
meps_dummy_map<-data.table(source)
meps_dummy_map[grep('MEPS',source),"MEPS":=1]
meps_dummy_map[is.na(MEPS)==T,MEPS:=0]

# merge dummy map onto comos_to_predict, only keep those that use MEPS as sources
comos_to_predict<-merge(comos_to_predict,meps_dummy_map,by="source")
comos_to_predict<-comos_to_predict[MEPS==1]
comos_to_predict[,comos:=paste0("t",yld_cause)]
comos_to_predict<-tolower(unique(comos_to_predict[,comos]))

# subset list of all coniditions to just those which we want to predict
comos<-comos_to_predict[which(comos_to_predict%in%comos_data)]

#set up parallelization
slots<- 4
mem <- slots*2
shell <- 'FILEPATH.sh'
script <- paste0(j_root, 'FILEPATH/3a_analysis_meps.R')
project <- '-P PROJECT '
sge_output_dir <- '-o /FILEPATH -e /FILEPATH '

for (como in comos){
  job_name<- paste0('-N lasso_linear_',como)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, script, como, output_path_summary_stats, output_path_bootstrap))
  print(paste(sys_sub, shell, script, como, output_path_summary_stats, output_path_bootstrap))
}
