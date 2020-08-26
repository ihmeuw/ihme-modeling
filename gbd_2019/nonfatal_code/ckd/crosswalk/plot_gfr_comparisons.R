#--------------------------------------------------------------
# Project: Non-Fatal CKD Estimation
# Purpose: Plot distributions of ACR and GBR in microdata sources
# used for CKD 
#--------------------------------------------------------------

# setup -------------------------------------------------------

# clear workspace
rm(list=ls())

require(data.table)
require(ggplot2)

# set the path to where ubcov extraction are stored (they should all be in .csv format)
extract_path<-"FILEPATH"

# set the path to the directory where you want to store diagnostic plots
plot_dir<-"FILEPATH"
#   -----------------------------------------------------------------------


#  prep -------------------------------------------------------------------

# create a list of all files in the ubcov extraction directory 
file_list<-list.files(extract_path,full.names = T)

# read in all files and place in a list
dt_list<-lapply(file_list, fread)

# assign each extraction a sequence number (seqn)
dt_list<-lapply(dt_list, function(x) x[,seqn:=.N])

# separate each study data.table into data.tables for creatinine, gfr, and acr 
dt_list_cr<-lapply(dt_list, function(x) x[,.(survey_name,year,seqn,serum_creatinine)])
dt_list_cg_gfr<-lapply(dt_list[which(lapply(dt_list,function(x) "gfr_cg" %in% names(x))==T)], function(x) x[,.(survey_name,year,seqn,gfr_cg)])
dt_list_gfr<-lapply(dt_list, function(x) x[,.(survey_name,year,seqn,gfr,gfr_mdrd)])
dt_list_acr<-dt_list[lapply(lapply(dt_list,names), function(x) any(x == "acr"))==T]
dt_list_acr<-lapply(dt_list_acr, function(x) x[,.(survey_name,year,seqn,acr)])

# bind list into one data set for each measure and make long on reference/alternative
dt_gfr<-rbindlist(dt_list_gfr,use.names=T)
dt_gfr<-melt(dt_gfr,id.vars = c("seqn","survey_name","year"))
dt_cg_gfr<-rbindlist(dt_list_cg_gfr,use.names = T)
dt_cg_gfr<-melt(dt_cg_gfr,id.vars = c("seqn","survey_name","year"))
dt_gfr<-rbindlist(list(dt_gfr,dt_cg_gfr),use.names = T)

dt_cr<-rbindlist(dt_list_cr,use.names = T)
dt_cr<-melt(dt_cr,id.vars = c("seqn","survey_name","year"))

dt_acr<-rbindlist(dt_list_acr,use.names = T)
dt_acr<-melt(dt_acr,id.vars = c("seqn","survey_name","year"))
# cap ACR values for plotting purposes 
dt_acr[value>500,value:=500]

#   -----------------------------------------------------------------------


# plot --------------------------------------------------------------------

pdf(paste0(plot_dir,"gfr_distributions.pdf"),width=15)
for (survey in unique(dt_gfr[,survey_name])){
  plot_dt<-dt_gfr[survey_name==survey]
  for (yr in unique(plot_dt[,year])){
    gg<-ggplot(data = plot_dt[year==yr],aes(x=value,fill=variable))+
      geom_density(alpha=0.3)+
      geom_vline(xintercept = c(15,30,60))+
      ggtitle(paste(survey, yr))+
      theme_bw()
    print(gg)    
  }
}
dev.off()

pdf(paste0(plot_dir,"creat_distributions.pdf"),width=15)
for (survey in unique(dt_cr[,survey_name])){
  plot_dt<-dt_cr[survey_name==survey]
  for (yr in unique(plot_dt[,year])){
    gg<-ggplot(data = plot_dt[year==yr],aes(x=value,fill=variable))+
      geom_density(alpha=0.3)+
      ggtitle(paste(survey, yr))+
      theme_bw()
    print(gg)    
  }
}
dev.off()

pdf(paste0(plot_dir,"acr_distributions.pdf"),width=15)
for (survey in unique(dt_acr[,survey_name])){
  plot_dt<-dt_acr[survey_name==survey]
  for (yr in unique(plot_dt[,year])){
    gg<-ggplot(data = plot_dt[year==yr],aes(x=value,fill=variable))+
      geom_density(alpha=0.3)+
      geom_vline(xintercept = 30)+
      ggtitle(paste(survey, yr))+
      theme_bw()
    print(gg)    
  }
}
dev.off()

#   -----------------------------------------------------------------------


