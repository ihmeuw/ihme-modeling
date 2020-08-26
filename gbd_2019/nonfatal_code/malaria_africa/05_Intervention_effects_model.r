
########################################################################################################################  
##### load #######  ##########################################################################################
data<-read.csv(FILEPATH)

if(CDI){
	data<-read.csv(FILEPATH)
}

if(Ethiopia){
	data[data$cn_name2=='Ethiopia' & data$year==2011,'Nexamined']=rep(200,length(data[data$cn_name2=='Ethiopia' & data$year==2011,'Nexamined']))
	tmp<-data[data$cn_name2=='Ethiopia' & data$year==2011,]
	tmp$year_start<-tmp$year_start+2
	tmp$year_end<-tmp$year_end+2
	tmp$yearqtr<-tmp$yearqtr+2
	tmp$year<-tmp$year+2
	tmp$year_covariate<-tmp$year_covariate+2
	tmp$date<-tmp$date+2
	data<-rbind(data,tmp)
}

# switch congo to have same effect as DRC 59 -> 68
cn<-raster(paste(FILEPATH,sep="")) #load raster
cn[cn==59]=68
NAvalue(cn)=-9999
convert.nodes<-function(r){
	v<-getValues(r)
	NAs<-is.na(v)
	v<-v[!NAs]
	un<-unique(v)
	l=length(un)
	tmp=v
	for(i in 1:l){
		tmp[v==un[i]]=i	
	}
	v=tmp
	r[!NAs]=v
	return(r)
}
cnn=convert.nodes(cn)
cn<-raster(paste(FILEPATH,sep="")) # reload load raster to remove drc congo fix

############################ get country itn use  and irs values ########################################
POPULATIONS<-read.csv(FILEPATH) # load table to match gaul codes to country names
indicators<-read.csv(FILEPATH) # comes from old stock and flow and serves now as a template

# we need the new method results from population weighted rasters
times=2000:2015
itn.stack<-raster::stack(paste('FILEPATH',times,'.ITN.use.yearavg.adj.tif',sep=""))
pop<-raster::stack(paste0('FILEPATH',times,'.total.population.tif'))
NAvalue(pop)<--9999
KEY<-read.csv('FILEPATH') # 
cn<-raster(paste('FILEPATH',sep=""))
NAvalue(cn)<--9999
stv<-getValues(itn.stack)
popv<-getValues(pop) # has sligntly more NAs due to islands and coasts
cnv<-getValues(cn)
NAvals<-is.na(cnv) | is.na(rowSums(popv)) | is.na(rowSums(stv)) # find NA values
library(doParallel)
registerDoParallel(cores=60)
indicators.raster<-foreach(i=1:nrow(indicators)) %dopar% {
	gaul<-KEY[as.character(indicators[i,1])==KEY[,3],1] # get gaul code
	tmp1<-cnv[!NAvals];tmp2<-popv[!NAvals,];tmp3<-stv[!NAvals,] # subset rasters by NA
	cells<-tmp1==gaul # get cells with gaul
	tmp1<-tmp1[cells];tmp2<-tmp2[cells,];tmp3<-tmp3[cells,] # subset rasters by gaul
	tmp2<- sweep(tmp2,2,colSums(tmp2),"/") # scale populations to sum to 1. They are now weights
	
	newind<-rep(NA,length(times))
	for( j in 1:length(times)){
		newind[j] <- (tmp3[,j])%*%(tmp2[,j])
	}
	out<-list()
	out[[1]]<-as.character(indicators[i,1])
	out[[2]]<-newind
	return(out)
}
indicatorsr<-matrix(nrow=nrow(indicators),ncol=length(times)+1)
indicatorsr[,1]<-as.character(indicators[,1])
for(i in 1:length(indicators.raster)){
	if(indicatorsr[i,1]!=indicators.raster[[i]][[1]]) warning(paste('Name mismatch'))
	indicatorsr[i,2:ncol(indicatorsr)]<-indicators.raster[[i]][[2]]
	
}
indicatorsr<-as.data.frame(indicatorsr,stringsAsFactors=FALSE) 
indicatorsr[,2:ncol(indicatorsr)]<-apply(indicatorsr[,2:ncol(indicatorsr)],2,as.numeric) # convert numbers to numeric

colnames(indicatorsr)<-c(0,2000:2015)
rownames(indicatorsr)<-rownames(indicators)

############################################# ITN split baseline
indicators<-indicatorsr # replace old indicators THIS IS KEY *****
names<-as.character(indicators[,1])
for(i in 1:nrow(indicators)){
	names[i]=as.character(POPULATIONS[as.character(POPULATIONS$NAME)==names[i],'COUNTRY_ID']) # get 3 letter country codes
}
indicators[,1]<-as.character(names)
threshold=0.09
indmat<-matrix(data=0,nrow=nrow(indicators),ncol=ncol(indicators))
for(i in 1:nrow(indicators)){
	for(j in 2:ncol(indicators)){
	 	if(indicators[i,j]<threshold){
	  		indmat[i,j]=1
	  	}
		if(indicators[i,j]>threshold){
			break
		}
	}
}
indmat[,1]=1
data$cn_itn<-rep(0,nrow(data))
data$cn_itn[data$year<2000]=1
cnames<-colnames(indicators)
rnames<-as.character(indicators[,1])
for(i in 1:nrow(indicators)){
	for(j in 2:ncol(indicators)){
		wh<-data$cn_name==rnames[i] & data$year==cnames[j]
		data$cn_itn[wh]=indmat[i,j]
	}
}

############################################# IRS split baseline
 indicators2<-read.csv('FILEPATH',strip.white = TRUE)

names<-as.character(indicators2[,1])
for(i in 1:nrow(indicators2)){
	names[i]=as.character(POPULATIONS[as.character(POPULATIONS$NAME)==names[i],'COUNTRY_ID']) # get 3 letter country codes
}

indicators2[,1]<-factor(names)
colnames(indicators2)<-c(0,2000:2015)
threshold=0.3
indmat2<-matrix(data=0,nrow=nrow(indicators2),ncol=ncol(indicators2))
for(i in 1:nrow(indicators2)){
	switch=TRUE
	for(j in 2:ncol(indicators2)){
	 	if(indicators2[i,j]<threshold & switch){
	  		indmat2[i,j]=1
	  	}
		if(indicators2[i,j]>threshold){
			break
		}
	}
}
indmat2[,1]=1

data$cn_irs<-rep(NA,nrow(data))

data$cn_irs[data$year<2000]=1

cnames<-colnames(indicators2)
rnames<-as.character(indicators2[,1])
for(i in 1:nrow(indicators2)){
	for(j in 2:ncol(indicators2)){
		wh<-data$cn_name==rnames[i] & data$year==cnames[j]
		data$cn_irs[wh]=indmat2[i,j]
	}
}


data<-data[!is.na(data$cn_itn) & !is.na(data$cn_irs),]

########################################################################################################################  
data<-data[data$year>=1995,] # only use data after 1995

########################################################################################################################  
#get intervention data
times=2000:2015
itn.stack<-raster::stack(paste('FILEPATH',times,'.ITN.use.yearavg.adj.tif',sep=""))
NAvalue(itn.stack)<--9999
ex<-raster(paste('/FILEPATH.tif',sep="")) #load raster
NAvalue(ex)=-9999
v<-getValues(ex)
un<-unique(!is.na(v))


itnuse<-rep(NA,nrow(data))
itnuse1<-rep(NA,nrow(data))
itnuse2<-rep(NA,nrow(data))
itnuse3<-rep(NA,nrow(data))
itnuse4<-rep(NA,nrow(data))
itnuse5<-rep(NA,nrow(data))

data$year_lag1<-data$year-1
data$year_lag2<-data$year-2
data$year_lag3<-data$year-3
data$year_lag4<-data$year-4
data$year_lag5<-data$year-5

itnuse[data$year<2000]=0
itnuse1[data$year_lag1<2000]=0
itnuse2[data$year_lag2<2000]=0
itnuse3[data$year_lag3<2000]=0
itnuse4[data$year_lag4<2000]=0
itnuse5[data$year_lag5<2000]=0
# extract use data
k=1
for(i in 2000:2015){
	itnuse[data$year==i]=itn.stack[[k]][data$cellnumber[data$year==i]]
	itnuse1[data$year_lag1==i]=itn.stack[[k]][data$cellnumber[data$year_lag1==i]]
	itnuse2[data$year_lag2==i]=itn.stack[[k]][data$cellnumber[data$year_lag2==i]]
	itnuse3[data$year_lag3==i]=itn.stack[[k]][data$cellnumber[data$year_lag3==i]]
	itnuse4[data$year_lag4==i]=itn.stack[[k]][data$cellnumber[data$year_lag4==i]]
	itnuse5[data$year_lag5==i]=itn.stack[[k]][data$cellnumber[data$year_lag5==i]]
	
	k=k+1
}
data$itnuse<-itnuse
data$itnuse1<-itnuse1
data$itnuse2<-itnuse2
data$itnuse3<-itnuse3
data$itnuse4<-itnuse4
data$itnuse5<-itnuse5

data$itnavg4<-rowMeans(cbind(itnuse,itnuse1,itnuse2,itnuse3))

########################################################################################################################  
#load ACT

data$act<-rep(NA,nrow(data))
un_year<-unique(data$year)
for(i in 1:length(un_year)){
	if(un_year[i]<2004){
		data[data$year==un_year[i],'act']<-0
	}else{
		act=raster(paste0('/FILEPATH_',un_year[i],'.tif'))
		actlag=raster(paste0('/FILEPATH_',un_year[i]-1,'.tif'))
		NAvalue(act)=-9999	
		NAvalue(actlag)=-9999		
		act<-calc(stack(act,actlag),mean)			
		data[data$year==un_year[i],'act']<-act[data$cellnumber[data$year==un_year[i]]]
	}
}

########################################################################################################################  
#load IRS
data$irs<-rep(NA,nrow(data))
un_year<-unique(data$year)
for(i in 1:length(un_year)){
	if(un_year[i]<2000){
		data[data$year==un_year[i],'irs']<-0
	}else{
		irs=raster(paste0('/FILEPATH',un_year[i],'.IRS.tif'))
		irslag=raster(paste0('/FILEPATH',un_year[i]-1,'.IRS.tif'))
		NAvalue(irs)=-9999		
		NAvalue(irslag)=-9999				
		irs<-calc(stack(irs,irslag),mean)
		data[data$year==un_year[i],'irs']<-irs[data$cellnumber[data$year==un_year[i]]]
	}
}


########################################################################################################################  
data1<-data[data$cn_itn==1 & data$cn_irs==1,] # zero is normal, one is baseline
###########################################
# IF YOU WANT TO PREDICT BASELINE STOP HERE!
# AND GO TO SCRIPT Baseline Predict PRcube.r
############################################
########################################################################################################################  
#adjust for IHME as this should be inverted
#data$act=1-data$act
############################################

Bpr<-raster('FILEPATH')
NAvalue(Bpr)<-(-9999)
data$bpr<-Bpr[data$cellnumber]
data$bprsq<-data$bpr*data$bpr
### you need to figure out why there are NAS IMPROTANT
#data[is.na(data$itnavg4),]
#data[is.na(data$bpr),]


data<-data[complete.cases(data),]

emplogit<-function(Y,N){
	top=Y*N+0.5
	bottom=N*(1-Y)+0.5
	return(log(top/bottom))
}
data$PfPr_logit<-emplogit(data$PfPr,data$Nexamined)

interact<-function(x,y)  (y^0.4-y^20)*(x^(1.5*y)) 
data$interact<--interact(data$itnavg4,data$bpr)
data$irsinteract<--data$irs
#data$irsinteract<--interact(data$irs,data$bpr)
data$actinteract<--data$act
data$bpr_emp=emplogit(data$bpr,1000)

master<-data
form=as.formula(paste0('~ ',paste('V',1:20,sep='',collapse='+'),'+ bpr_emp+ interact + irsinteract + actinteract'))

design_matrix<-model.matrix(form,data=as.data.frame(data))

form=as.formula(paste0('~',paste('V',1:20,sep='',collapse='+')))
design_matrix2<-model.matrix(form,data=as.data.frame(data1))
########################################################################################################################  



colnames(design_matrix)<-paste0('V',1:ncol(design_matrix))
colnames(design_matrix2)<-paste0('V',1:ncol(design_matrix2))

xyz<-ll.to.xyz(data1[,c('longitude','latitude')])
data1<-cbind(data1,xyz)
xyz<-ll.to.xyz(data[,c('longitude','latitude')])
data<-cbind(data,xyz)


comb<-rbind(data[,c('longitude','latitude')],data1[,c('longitude','latitude')])
xyz<-as.data.frame(ll.to.xyz(comb))
un<-paste(xyz$x,xyz$y,xyz$z,sep=':')
dup<-!duplicated(un)		

mesh = inla.mesh.2d(loc=cbind(xyz[dup,'x'],xyz[dup,'y'],xyz[dup,'z']),
  cutoff=0.006, #should be 0.003
  min.angle=c(25,25),
  max.edge=c(0.015,1) )


 
spde = inla.spde2.matern(mesh,alpha=2)


data$gyear<-data$year
data$gyear[data$gyear<2000]=2000 # collapse 1995-1999 to 2000
data$gyear[data$year>=2014]=2013 #adjust year so temporal function


mesh1d=inla.mesh.1d(seq(2000,2014,by=4),interval=c(2000,2014),degree=2, boundary=c('free'))

est.cov<-as.list(as.data.frame(design_matrix))
est.cov$year<-data$gyear-1999
est.cov$year[est.cov$year<=0]=1



data$PfPr_logit<-emplogit(data$PfPr,data$Nexamined)
iid<-list(iid=raster::extract(cnn,cbind(data$longitude,data$latitude)))

A.est =
  inla.spde.make.A(mesh, loc=cbind(data[,'x'],data[,'y'],data[,'z']),group=data[,'gyear'],group.mesh=mesh1d)
  
	#-- Create index matrix --#
field.indices =
  inla.spde.make.index("field", n.spde=mesh$n,n.group=mesh1d$m)

  stack.est =
inla.stack(data=list(response=data$PfPr_logit),
		   A=list(A.est,1),
		   effects=
			 list(c(field.indices),
					c(est.cov,iid)
				  ),
		   tag="est", remove.unused=TRUE,compress=TRUE)

formula<- as.formula(paste(
	paste("response ~ -1 + "),
	paste("f(field, model=spde,group=field.group, control.group=list(model='ar1'))+",sep=""),
	paste("f(iid, model='iid') +",sep=""),
	paste("f(V25, model='clinear',range=c(0,Inf),initial=1) +",sep=""),
	paste(paste('V',1:(ncol(design_matrix)-1),sep=''),collapse='+'),
	sep="")
)

stack.est<-stack.est

#-- Call INLA and get results --#
mod.pred =   inla(formula,
			 data=inla.stack.data(stack.est),
			 family="gaussian",
			 ##################################################
			# control.mode=list(theta=thetac, restart=TRUE,fixed=FALSE), ###
			 ##################################################
			 control.predictor=list(A=inla.stack.A(stack.est), compute=TRUE,quantiles=NULL),
			 control.compute=list(cpo=TRUE, dic=TRUE,config=TRUE),
			 keep=FALSE, verbose=TRUE,#num.threads=5,
			 control.inla= list(strategy = 'gaussian',
			 int.strategy='ccd',
			 verbose=TRUE,fast=TRUE,dz=1,
			 step.factor=1,
			 stupid.search=FALSE)
	 )      

index= inla.stack.index(stack.est,"est")$data
lp=mod.pred$summary.linear.predictor$mean[index]
#a=mod.pred$summary.random$iid


spde.res2 <- inla.spde2.result(mod.pred, "field", spde)
range=spde.res2$marginals.range.nominal$range.nominal.1
plot(gc.dist(range[,1]),range[,2])
variance=spde.res2$marginals.variance.nominal$variance.nominal.1
print(paste('The mean range is',gc.dist(exp(spde.res2$summary.log.range.nominal$mean))))

TAG<-'MBGW3_counterfactual'

if(!file.exists(paste0('FILEPATH',TAG))) dir.create(paste0('FILEPATH',TAG))

sink(paste0('/FILEPATH',TAG,'/',TAG,'.txt'))
cat(paste(Sys.Date(),'\n'))
cat('Using full temporal length, set at 2013, mesh=4\n')
cat('interation is   (y^0.4-y^20)*(x^(1.5*y)) best AIC  unconstrained \n')
cat('this includes 1 baseline term, empirical baseline\n')
cat('only act is clinear \n')
cat('Include country specific random \n')
cat('dense mesh + ccd + gaussian  \n')
cat('adjusted Cote Divoire\n')
cat('collapse 1996 to 1999 into 2000\n')
cat('changed Nexamined in ethiopia\n')
cat('run using new net data\n')
cat('ACT 2 year average\n')
cat('IHME act\n')
cat('TOGO AND SENGAL ADDED\n')
cat('NEW NET NUMBERS FROM RBM-HWG\n')
cat('Donals IHME ACT 5km WITH drug resitance stuff in there\n')
cat('this is the counterfactual research\n')
sink()

save.image(paste0('FILEPATH',TAG,'/',TAG,'.rdata'))

###################################################################################################################################################
###################################################################################################################################################
###################################################################################################################################################
counter = 1 # 1 = real, 2= itn at 2000, 3= IRS at 2000, 4= act at 2000, 5= all at 2000


TAG<-'MBGW3_counterfactual'
TAG<-'MBGW3'

load(paste0('FILEPATH',TAG,'/',TAG,'.rdata'))
require('raster')
require('rgdal')
library(INLA)
library(RColorBrewer)
library(zoo) 
get.pred.locs<-function(){
	cn<-raster(paste('FILEPATH',sep="")) #load raster
	NAvalue(cn)=-9999
	pred_val<-getValues(cn)#get values again
	w<-is.na(pred_val) #find NAs again
	index<-1:length(w) 
	index<-index[!w]
	pred_locs<-xyFromCell(cn,1:ncell(cn))  #get prediction locations
	pred_locs<-pred_locs[!w,] #remove NA cells
	############### Plotting anomoly
	colnames(pred_locs)<-c('longitude','latitude')
	pred_locs<-ll.to.xyz(pred_locs) #get locations
	return(pred_locs)
}
TAG<-'MBGW3_counterfactual'

get.pred.covariates<-function(year,month){
		cn<-raster(paste('FILEPATH',sep="")) #load raster
		NAvalue(cn)=-9999
		pred_val<-getValues(cn)#get values again
		w<-is.na(pred_val) #find NAs again
		index<-1:length(w) 
		index<-index[!w]
		pred_locs<-xyFromCell(cn,1:ncell(cn))  #get prediction locations
		pred_locs<-pred_locs[!w,] #remove NA cells
		############### Plotting anomoly
		colnames(pred_locs)<-c('longitude','latitude')
		pred_locs<-ll.to.xyz(pred_locs) #get locations
		folder_list<-paste('V',1:20,sep='')
		pred_mat<-matrix(nrow=nrow(pred_locs),ncol=20)

		pred_year=year
		pred_month=month
		library(doParallel)
		registerDoParallel(cores=60)
		pred_mat<-foreach(i=1:length(folder_list), .combine=cbind) %dopar% {
				print(i)
				file<-folder_list[i]
				layer_list<-list.files(paste('FILEPATH',file,'/',sep=''),pattern='*.tif')
				test<-layer_list[1]
				test<-unlist(strsplit(test,"\\."))
				v=rep(NA,nrow(pred_locs))
				############################################
				if(test[3]==0 & test[4]==0){ 		# completely static variable
					layer<-layer_list[1]
					r<-raster(paste('FILEPATH',file,'/',layer,sep=''))
					NAvalue(r)=-9999
					v<-r[index]
				############################################
				} else if(test[3]!=0 & test[4]==0) {	# year only dynamic variable
						layer<-paste('Pf.',file,'.',pred_year,'.0.tif',sep='') # layer location		
						r<-raster(paste('FILEPATH',file,'/',layer,sep='')) # load raster
						NAvalue(r)=-9999
						
						v<-r[index]
				############################################		
				} else if(test[3]==0 & test[4]!=0) { # month only dynamic variable
						layer<-paste('Pf.',file,'.0.',pred_month,'.tif',sep='')
						r<-raster(paste('FILEPATH',file,'/',layer,sep=''))
						NAvalue(r)=-9999
						
						v<-r[index]			
				############################################			
				} else if(test[3]!=0 & test[4]!=0) { #fully dynamic variable
						layer<-paste('Pf.',file,'.',pred_year,'.',pred_month,'.tif',sep='') # layer location		
						r<-raster(paste('FILEPATH',file,'/',layer,sep='')) # load raster
						NAvalue(r)=-9999
						
						v<-r[index]			
					}
				return(v)
			}
		pred_mat<-cbind(rep(1,nrow(pred_locs)),pred_mat)
		return(pred_mat)

	}


time_points<-2000:2015


iidcn<-cnn
iid.inla<-mod.pred$summary.random$iid
iidcn[!is.na(cnn)]=0
for(i in 1:nrow(iid.inla)){
	id<-iid.inla[i,'ID']
	iidcn[cnn==id]=iid.inla[i,'mean']
}

xxx=length(time_points)
for(xxx in 1:length(time_points)){
		dics<-cors<-c()
		pred_year=time_points[xxx]
		pred_month1="01"
		pred_month2="04"
		pred_month3="07"
		pred_month4="10"
		pred_year_cov=pred_year
		if(time_points[xxx]<=2000) pred_year_cov=2001 # covariates are not full in 2000

#		pred_mat1_s<-get.pred.covariates(pred_year_cov,pred_month1)
#		pred_mat2_s<-get.pred.covariates(pred_year_cov,pred_month2)
#		pred_mat3_s<-get.pred.covariates(pred_year_cov,pred_month3)
#		pred_mat4_s<-get.pred.covariates(pred_year_cov,pred_month4)

		pred_mat1_s<-get.pred.covariates(pred_year_cov,pred_month1)
		pred_mat2_s<-pred_mat1_s
		pred_mat3_s<-pred_mat1_s
		pred_mat4_s<-pred_mat1_s

	for(counter in 6){

		Bpr<-raster('/FILEPATH')

		pred_year_ITN=pred_year
		if(time_points[xxx]<2000) pred_year_ITN=1999 # covariates are not full in 2000
		if(counter==2 | counter==5) pred_year_ITN=2000

		itn.names<-paste('FILEPATH',pred_year_ITN,'.ITN.use.yearavg.adj.tif',sep='')
		itn.names1<-paste('/FILEPATH',pred_year_ITN-1,'.ITN.use.yearavg.adj.tif',sep='')
		itn.names2<-paste('/FILEPATH',pred_year_ITN-2,'.ITN.use.yearavg.adj.tif',sep='')
		itn.names3<-paste('/FILEPATH',pred_year_ITN-3,'.ITN.use.yearavg.adj.tif',sep='')

		itnr0<-raster(itn.names)
		itnr1<-raster(itn.names1)
		itnr2<-raster(itn.names2)
		itnr3<-raster(itn.names3)


		st2<-stack(itnr0,itnr1,itnr2,itnr3)
		NAvalue(st2)<--9999
		itnr<-calc(st2,mean)

		if(counter==6){
			#itn to 100%
			itnr[!is.na(itnr)]=1
		}

		cn<-raster('FILEPATH.tif') # master country layer
		NAvalue(cn)=-9999
		pred_val<-getValues(cn)#get values again
		w<-is.na(pred_val) #find NAs again
		index<-1:length(w) 
		index<-index[!w]

		br<-Bpr[index]
		itn<-itnr[index]
		inter<-interact(itn,br)
		br=emplogit(br,1000)

		brsq<-br*br

		# zero  scenario
		zeros<-rep(0,length(br))

		pred_year=time_points[xxx]	# get prediction year

		# add country specific effets
		iid.value<-iidcn[index]

		pred_year_IRS=pred_year
		if(time_points[xxx]<2000) pred_year_IRS=1999 # covariates are not full in 2000
		if(counter==3 | counter==5) pred_year_IRS=2000


		irs_r<-raster(paste0('/FILEPATH',pred_year_IRS,'.IRS.tif')) # master country layer
		irs_r2<-raster(paste0('/FILEPATH',pred_year_IRS-1,'.IRS.tif')) # master country layer
		NAvalue(irs_r)=-9999
		NAvalue(irs_r2)=-9999
		irs_r=calc(stack(irs_r,irs_r2),mean)
		irs<-irs_r[index]

		pred_year_ACT=time_points[xxx]
		if(time_points[xxx]<=1970) pred_year_ACT=1971 # covariates are not full in 2000
		if(time_points[xxx]==2014) pred_year_ACT=2015 # IMPORTANT

		if(counter==4 | counter==5) {
			#counter factual
			act_r<-raster(paste0('/FILEPATH',pred_year_ACT,'.tif'))# master country layer
			act_r2<-raster(paste0('FILEPATH',pred_year_ACT-1,'.tif')) # master country layer
			# factual
			act_r_test=raster(paste0('FILEPATH/',pred_year_ACT,'.ACT.tif'))
 			act_r2_test=raster(paste0('FILEPATH',pred_year_ACT-1,'.ACT.tif'))
			NAvalue(act_r)=NAvalue(act_r2)=NAvalue(act_r_test)=NAvalue(act_r2_test)=-9999	
			
			act_r[act_r>act_r_test]=act_r_test[act_r>act_r_test] # if counter is greater than actual, give values of actual. This makes no negatives
			act_r2[act_r2>act_r2_test]=act_r2_test[act_r2>act_r2_test]
			act_r[act_r<0]=0
			act_r2[act_r2<0]=0
		
		} else {
		#	act_r<-raster(paste0('/FILEPATH,'.ACT.tif')) # master country layer
		#	act_r2<-raster(paste0('FILEPATH'.ACT.tif'))# master country layer
 			act_r=raster(paste0('FILEPATH',pred_year_ACT,'.ACT.tif'))
 			act_r2=raster(paste0('FILEPATH',pred_year_ACT-1,'.ACT.tif'))
		
		}

		
		NAvalue(act_r)=-9999
		NAvalue(act_r2)=-9999
		act_r=calc(stack(act_r,act_r2),mean)
		act<-act_r[index]
		###############
		# IHME adjust
		#act<-1-act
		###############
		extracovs<-cbind(br,inter,irs,act) #old model


		colnames(pred_mat1_s)<-paste0('V',0:20)
		colnames(pred_mat2_s)<-paste0('V',0:20)
		colnames(pred_mat3_s)<-paste0('V',0:20)
		colnames(pred_mat4_s)<-paste0('V',0:20)


		pred_mat1<-cbind(pred_mat1_s,extracovs)
		pred_mat2<-cbind(pred_mat2_s,extracovs)
		pred_mat3<-cbind(pred_mat3_s,extracovs)
		pred_mat4<-cbind(pred_mat4_s,extracovs)


		pred_locs<-get.pred.locs()

		#########################################
		pred_year=time_points[xxx]	     ########
		if(pred_year>=2014) pred_year=2013 # adjust prediction year for field
		if(pred_year<=2000) pred_year=2000 # adjust prediction year for field

		#########################################

		pred_year1=pred_year
		pred_year2=pred_year
		pred_year3=pred_year
		pred_year4=pred_year
		pryears<-c(pred_year1,pred_year2,pred_year3,pred_year4)

		library(doParallel)
		registerDoParallel(cores=60)
		Apreds<-foreach(i=1:4,.packages='INLA') %dopar% {
			zzz=pryears[i]
			return(inla.spde.make.A(mesh, loc=cbind(pred_locs[,'x'],pred_locs[,'y'],pred_locs[,'z']),group=rep(zzz,nrow(pred_locs)),group.mesh=mesh1d))
		}
		A.pred_dynamic1=Apreds[[1]]
		A.pred_dynamic2=Apreds[[2]]
		A.pred_dynamic3=Apreds[[3]]
		A.pred_dynamic4=Apreds[[4]]

		pred1=pred_year1
		pred2=pred_year2
		pred3=pred_year3
		pred4=pred_year4


		cn<-raster('FILEPATH') # master country layer
		NAvalue(cn)=-9999


		limits<-raster('FILEPATH')
		NAvalue(limits)<--9999
		limits[limits==9999 | limits==0 | limits==1]=0
		limits[limits==2]=1

		pred_val<-getValues(cn)#get values again
		w<-is.na(pred_val) #find NAs again

		a=mod.pred$summary.random$V25
		a=a[2:nrow(a),2]/a[2:nrow(a),1]
		actparam<-mean(a[!is.infinite(a)])

		Beta<-c(mod.pred$summary.fixed[,1],-actparam)
		Beta[23]=-Beta[23]
		Beta[24]=-Beta[24]

		fixed_effects1<-as.numeric(Beta%*%t(pred_mat1))	
		fixed_effects2<-as.numeric(Beta%*%t(pred_mat2))	
		fixed_effects3<-as.numeric(Beta%*%t(pred_mat3))	
		fixed_effects4<-as.numeric(Beta%*%t(pred_mat4))	

		lp1=fixed_effects1 + drop(A.pred_dynamic1%*%mod.pred$summary.random$field$mean) +iid.value
		lp2=fixed_effects2 + drop(A.pred_dynamic2%*%mod.pred$summary.random$field$mean) +iid.value
		lp3=fixed_effects3 + drop(A.pred_dynamic3%*%mod.pred$summary.random$field$mean) +iid.value
		lp4=fixed_effects4 + drop(A.pred_dynamic4%*%mod.pred$summary.random$field$mean) +iid.value
		lp = cbind(lp1, lp2,lp3,lp4)



		lp<-fun3(lp)
		lp<-rowMeans(lp)

		colfunc <- colorRampPalette(c("blue","cyan","yellow","orange","red"))

		P<-cn
		P[!w]<-(lp)
		P[limits==0]=0
		P[is.na(limits)]=0
		P[is.na(cn)]=NA

		if(counter==1){
			writeRaster(P,paste0('FILEPATH',TAG,'/',TAG,'.',time_points[xxx],'.PR.tif'),NAflag=-9999,overwrite=TRUE)
		} else if(counter==2){
			writeRaster(P,paste0('FILEPATH',TAG,'/',TAG,'.',time_points[xxx],'.PR.ITN.tif'),NAflag=-9999,overwrite=TRUE)
		} else if(counter==3){
			writeRaster(P,paste0('FILEPATH',TAG,'/',TAG,'.',time_points[xxx],'.PR.IRS.tif'),NAflag=-9999,overwrite=TRUE)
		} else if(counter==4){
			writeRaster(P,paste0('FILEPATH',TAG,'/',TAG,'.',time_points[xxx],'.PR.ACT.tif'),NAflag=-9999,overwrite=TRUE)
		} else if(counter==5){
			writeRaster(P,paste0('/FILEPATH',TAG,'/',TAG,'.',time_points[xxx],'.PR.ALL.tif'),NAflag=-9999,overwrite=TRUE)
		}
		 else if(counter==6){
			writeRaster(P,paste0('FILEPATH',TAG,'/',TAG,'.',time_points[xxx],'.PR.ITN100.tif'),NAflag=-9999,overwrite=TRUE)
		}
	}
}


TAG<-'MBGW3_counterfactual'

### actual maps
library(raster)
ncells<-2926621
i=2015

		library(doParallel)
		registerDoParallel(cores=60)
		out<-foreach(i=2000:2015,.packages='INLA') %dopar% {

#for(i in 2000:2015){

	sets<-(i-4):i # get sets for average
	sets<-sets[sets>=2000] #remove invalid years

	ptm <- proc.time()
	cn<-raster(paste('FILEPATH',sep="")) #load raster to store new values
	NAvalue(cn)=-9999 ##
	mat<-matrix(nrow=ncells,ncol=length(sets)) #precompute matrix to get values
	for(k in 1:length(sets)){
			f1<-paste0('FILEPATH',TAG,'/',TAG,'.',sets[k],'.PR.tif')
			mat[,k]<-getValues(raster(f1))	# get values	
	}
	wh<-complete.cases(mat) # find NAs
	mat<-as.matrix(mat[wh,]) # strip NAs
	rates<-rep(NA,nrow(mat)) # precompute rate vector
	rates_min<-apply(mat,1,min)
	rates_max<-apply(mat,1,max)
	index<-which(rates_min==rates_max)
	index2<-which(rates_min!=rates_max)
	mean_pr<-rowMeans(mat)
	rates[index]=0 # set these to zero
	X<-as.matrix(drop(cbind(rep(1,length(sets)),sets)))
	for(k in index2){ # linear model loop
		rates[k]<-lm.fit(X,mat[k,])$coefficients[2] # after benchmarking there is nothing faster
	}	
	proc.time() - ptm
	rates[rates>0]=0 # set positive rates to 0
	rates=abs(rates) # take abs on negative rates
	rates<-rates/mean_pr			
	newvals<-rep(NA,ncells) # pre compute new vector
	newvals[wh]<-rates # add non NA values
	cn<-setValues(cn,newvals) # set values over cn raster
	writeRaster(cn,paste0('FILEPATH',TAG,'/',i,'.5yr.decline.PR.tif'),NAflag=-9999,overwrite=T)	# write	

	### actual maps
	library(raster)
	ncells<-2926621
	sets<-(i-4):i # get sets for average
	sets<-sets[sets>=2000] #remove invalid years

	ptm <- proc.time()
	cn<-raster(paste('FILEPATH',sep="")) #load raster to store new values
	NAvalue(cn)=-9999 ##
	mat<-matrix(nrow=ncells,ncol=length(sets)) #precompute matrix to get values
	for(k in 1:length(sets)){
			f1<-paste0('FILEPATH',TAG,'/',TAG,'.',sets[k],'.PR.ITN.tif')
			mat[,k]<-getValues(raster(f1))	# get values	
	}
	wh<-complete.cases(mat) # find NAs
	mat<-as.matrix(mat[wh,]) # strip NAs
	rates<-rep(NA,nrow(mat)) # precompute rate vector
	rates_min<-apply(mat,1,min)
	rates_max<-apply(mat,1,max)
	index<-which(rates_min==rates_max)
	index2<-which(rates_min!=rates_max)
	mean_pr<-rowMeans(mat)
	rates[index]=0 # set these to zero
	X<-as.matrix(drop(cbind(rep(1,length(sets)),sets)))
	for(k in index2){ # linear model loop
		rates[k]<-lm.fit(X,mat[k,])$coefficients[2] # after benchmarking there is nothing faster
	}	
	proc.time() - ptm
	rates[rates>0]=0 # set positive rates to 0
	rates=abs(rates) # take abs on negative rates
	rates<-rates/mean_pr			
	newvals<-rep(NA,ncells) # pre compute new vector
	newvals[wh]<-rates # add non NA values
	cn<-setValues(cn,newvals) # set values over cn raster
	writeRaster(cn,paste0('FILEPATH',TAG,'/',i,'.5yr.decline.PR.ITN.tif'),NAflag=-9999,overwrite=T)	# write	

	### actual maps
	library(raster)
	ncells<-2926621
	sets<-(i-4):i # get sets for average
	sets<-sets[sets>=2000] #remove invalid years

	ptm <- proc.time()
	cn<-raster(paste('/FILEPATH',sep="")) #load raster to store new values
	NAvalue(cn)=-9999 ##
	mat<-matrix(nrow=ncells,ncol=length(sets)) #precompute matrix to get values
	for(k in 1:length(sets)){
			f1<-paste0('FILEPATH/',TAG,'/',TAG,'.',sets[k],'.PR.IRS.tif')
			mat[,k]<-getValues(raster(f1))	# get values	
	}
	wh<-complete.cases(mat) # find NAs
	mat<-as.matrix(mat[wh,]) # strip NAs
	rates<-rep(NA,nrow(mat)) # precompute rate vector
	rates_min<-apply(mat,1,min)
	rates_max<-apply(mat,1,max)
	index<-which(rates_min==rates_max)
	index2<-which(rates_min!=rates_max)
	mean_pr<-rowMeans(mat)
	rates[index]=0 # set these to zero
	X<-as.matrix(drop(cbind(rep(1,length(sets)),sets)))
	for(k in index2){ # linear model loop
		rates[k]<-lm.fit(X,mat[k,])$coefficients[2] # after benchmarking there is nothing faster
	}	
	proc.time() - ptm
	rates[rates>0]=0 # set positive rates to 0
	rates=abs(rates) # take abs on negative rates
	rates<-rates/mean_pr			
	newvals<-rep(NA,ncells) # pre compute new vector
	newvals[wh]<-rates # add non NA values
	cn<-setValues(cn,newvals) # set values over cn raster
	writeRaster(cn,paste0('FILEPATH',TAG,'/',i,'.5yr.decline.PR.IRS.tif'),NAflag=-9999,overwrite=T)	# write	

	### actual maps
	library(raster)
	ncells<-2926621
	sets<-(i-4):i # get sets for average
	sets<-sets[sets>=2000] #remove invalid years

	ptm <- proc.time()
	cn<-raster(paste('FILEPATH',sep="")) #load raster to store new values
	NAvalue(cn)=-9999 ##
	mat<-matrix(nrow=ncells,ncol=length(sets)) #precompute matrix to get values
	for(k in 1:length(sets)){
			f1<-paste0('FILEPATH',TAG,'/',TAG,'.',sets[k],'.PR.ACT.tif')
			mat[,k]<-getValues(raster(f1))	# get values	
	}
	wh<-complete.cases(mat) # find NAs
	mat<-as.matrix(mat[wh,]) # strip NAs
	rates<-rep(NA,nrow(mat)) # precompute rate vector
	rates_min<-apply(mat,1,min)
	rates_max<-apply(mat,1,max)
	index<-which(rates_min==rates_max)
	index2<-which(rates_min!=rates_max)
	mean_pr<-rowMeans(mat)
	rates[index]=0 # set these to zero
	X<-as.matrix(drop(cbind(rep(1,length(sets)),sets)))
	for(k in index2){ # linear model loop
		rates[k]<-lm.fit(X,mat[k,])$coefficients[2] # after benchmarking there is nothing faster
	}	
	proc.time() - ptm
	rates[rates>0]=0 # set positive rates to 0
	rates=abs(rates) # take abs on negative rates
	rates<-rates/mean_pr			
	newvals<-rep(NA,ncells) # pre compute new vector
	newvals[wh]<-rates # add non NA values
	cn<-setValues(cn,newvals) # set values over cn raster
	writeRaster(cn,paste0('FILEPATH',TAG,'/',i,'.5yr.decline.PR.ACT.tif'),NAflag=-9999,overwrite=T)	# write	

	### actual maps
	library(raster)
	ncells<-2926621
	sets<-(i-4):i # get sets for average
	sets<-sets[sets>=2000] #remove invalid years

	ptm <- proc.time()
	cn<-raster(paste('FILEPATH',sep="")) #load raster to store new values
	NAvalue(cn)=-9999 ##
	mat<-matrix(nrow=ncells,ncol=length(sets)) #precompute matrix to get values
	for(k in 1:length(sets)){
			f1<-paste0('FILEPATH',TAG,'/',TAG,'.',sets[k],'.PR.ALL.tif')
			mat[,k]<-getValues(raster(f1))	# get values	
	}
	wh<-complete.cases(mat) # find NAs
	mat<-as.matrix(mat[wh,]) # strip NAs
	rates<-rep(NA,nrow(mat)) # precompute rate vector
	rates_min<-apply(mat,1,min)
	rates_max<-apply(mat,1,max)
	index<-which(rates_min==rates_max)
	index2<-which(rates_min!=rates_max)
	mean_pr<-rowMeans(mat)
	rates[index]=0 # set these to zero
	X<-as.matrix(drop(cbind(rep(1,length(sets)),sets)))
	for(k in index2){ # linear model loop
		rates[k]<-lm.fit(X,mat[k,])$coefficients[2] # after benchmarking there is nothing faster
	}	
	proc.time() - ptm
	rates[rates>0]=0 # set positive rates to 0
	rates=abs(rates) # take abs on negative rates
	rates<-rates/mean_pr			
	newvals<-rep(NA,ncells) # pre compute new vector
	newvals[wh]<-rates # add non NA values
	cn<-setValues(cn,newvals) # set values over cn raster
	writeRaster(cn,paste0('FILEPATH',TAG,'/',i,'.5yr.decline.PR.ALL.tif'),NAflag=-9999,overwrite=T)	# write	

}

		### actual maps
		library(raster)
		ncells<-2926621
		for(i in 1971:2015){ #loop for years
		  sets<-(i-4):i # get sets for average
		  sets<-sets[sets>=1970] #remove invalid years
		  library(doParallel)
		  registerDoParallel(cores=60)
		  out<-foreach(j=1:100) %dopar% { # 1st inner loop which is parallel for realisations
		    ptm <- proc.time()
		    cn<-raster(paste('FILEPATH',sep="")) #load raster to store new values
		    NAvalue(cn)=-9999 ##
		    mat<-matrix(nrow=ncells,ncol=length(sets)) #precompute matrix to get values
		    for(k in 1:length(sets)){
		      f1<-paste0('FILEPATH',sets[k],'.',j,'.PR.tif')
		      mat[,k]<-getValues(raster(f1))	# get values	
		    }
		    wh<-complete.cases(mat) # find NAs
		    mat<-mat[wh,] # strip NAs
		    rates<-rep(NA,nrow(mat)) # precompute rate vector
		    rates_min<-apply(mat,1,min)
		    rates_max<-apply(mat,1,max)
		    index<-which(rates_min==rates_max)
		    index2<-which(rates_min!=rates_max)
		    mean_pr<-rowMeans(mat)
		    rates[index]=0 # set these to zero
		    X<-as.matrix(drop(cbind(rep(1,length(sets)),sets)))
		    for(k in index2){ # linear model loop
		      rates[k]<-lm.fit(X,mat[k,])$coefficients[2] # after benchmarking there is nothing faster
		    }	
		    proc.time() - ptm
		    rates[rates>0]=0 # set positive rates to 0
		    rates=abs(rates) # take abs on negative rates
		    rates<-rates/mean_pr			
		    newvals<-rep(NA,ncells) # pre compute new vector
		    newvals[wh]<-rates # add non NA values
		    cn<-setValues(cn,newvals) # set values over cn raster
		    writeRaster(cn,paste0('FILEPATH.',i,'.',j,'.5yr.decline.PR.tif'),NAflag=-9999,overwrite=T)	# write	
		  }
		}
