model_string = '
				data{
						Q<-(n*4+1)	
						MV<-MV_avg
				}
				model {
		################################
					################################
					# ***** stock and flow model #
					################################
					# PRIORs
					# prior loss functions for LLINS
					for(i in 1:n){
						std_N[i]<- ifelse(i<=4,2,0.2)  # standard deviation manufacturer TWEAK
					}

					 ############# LLIN MODEL
					  for (i in 1:z) {
					  
						for (j in 1:z) {
							SiUSER[i,j] <-  exp(-((x1[i] - x1[j])/rho_sq1)^2) + ifelse(i==j,tau1,0) 
						}
					  }
					  rho_sq1 ~ dunif(0,1)
	    			  tau1 ~ dunif(0,0.1)

					  for (i in 1:z) {
						 mu1[i]=0
					  }
					  y1~ dmnorm(mu1,SiUSER) 
	  
					  for (i in 1:n) {
						for (j in 1:z) {
							Sigma_pred1[i,j] <-  exp(-((i - x1[j])/rho_sq1)^2)
						}
					  }			  
						p1<-Sigma_pred1%*%inverse(SiUSER)%*%y1

				 ############# ITN MODEL

					  for (i in 1:z2) {
						for (j in 1:z2) {
							Sigma2[i,j] <-  exp(-((x2[i] - x2[j])/rho_sq2)^2)  +ifelse(i==j,tau2,0) 
						}
					  }
					  rho_sq2 ~ dunif(0,1)
  					  tau2 ~ dunif(0,0.1)
	  
					  for (i in 1:z2) {
						 mu2[i]=0
					  }
					  y2~ dmnorm(mu2,Sigma2) 
	  
					  for (i in 1:n) {
						for (j in 1:z2) {
							Sigma_pred2[i,j] <- exp(-((i - x2[j])/rho_sq2)^2)
						}
					  }			  
					p2<-Sigma_pred2%*%inverse(Sigma2)%*%y2					
												
					#initialise manufacturer and NMCP
					for(j in 1:n){
						# manufacturer takes actual value
						s_m[j] ~ dunif(0, 0.075) 	 # error in llin manufacturer	
						mu[j]~dnorm(MANUFACTURER[j],((MANUFACTURER[j]+1e-12)*s_m[j])^-2) T(0,)
						s_d[j] ~ dunif(0, 0.01) 	 # error in llin NMCP				
						s_d2[j] ~ dunif(0, 0.01) 	 # error in ITN NMCP		

				
						delta_raw[j]<-ifelse(p1[j]>0,p1[j]*year_population[j],0)
						delta2_raw[j]<-ifelse(p2[j]>0,p2[j]*year_population[j],0)					
											
					}
					
								
					#initialise with zero stock
					delta[1] <- ifelse(delta_raw[1]>mu[1],mu[1],delta_raw[1])
					able[1]<-mu[1]
					par2[1]~dunif(1,24)
					extra[1]~dbeta(2,par2[1]) #beta distribution extra
					delta_l[1]<-delta[1]+((able[1]-delta[1])*extra[1])
					Psi[1] <- able[1]-delta_l[1]		
				
					#loop to get stocks and capped deltas
					for(j in 2:n){
						delta[j] <- ifelse(delta_raw[j]>(mu[j]+Psi[j-1]),mu[j]+Psi[j-1],delta_raw[j])					
						able[j] <- Psi[j-1] + mu[j]	
						par2[j]~dunif(1,24)
						extra[j]~dbeta(2,par2[j])
						delta_l[j]<-delta[j]+((able[j]-delta[j])*extra[j])
						Psi[j] <- able[j]-delta_l[j]	
					}
			####################################	LLINS
					for(i in 1:4){ # change according to size of MV
						k[1,i]~dunif(16,18) 
						L[1,i]~dunif(1,20.7)		

					}
										
					for(i in 5:nrow_mv){ # change according to size of MV
						k[1,i]~dunif(16,18) 
						L[1,i]~dunif(4,20.7)
					}
					mv_k<-k%*%MV		
					mv_L<-L%*%MV


					#llins
					for(j in 1:n){ #for 1:18 time points
	
						xx1[1,j]<-(-0.25)
						xx2[1,j]<-(-0.25)
						xx3[1,j]<-(-0.25)
						xx4[1,j]<-(-0.25)
					
						g.m[j,1]~dunif(0,1)
						g.m[j,2]~dunif(0,1)
						g.m[j,3]~dunif(0,1)
						g.m[j,4]~dunif(0,1)
			
						g.m[j,5]<-sum(g.m[j,1:4])
						g.m[j,6]<-g.m[j,1]/g.m[j,5]
						g.m[j,7]<-g.m[j,2]/g.m[j,5]
						g.m[j,8]<-g.m[j,3]/g.m[j,5]
						g.m[j,9]<-g.m[j,4]/g.m[j,5]
			
						for(i in 1:Q){ # quarterly
							ind1[i,j]<-ifelse(((i-1)/4)<(j-1+0.25),0,1) # counter to set zero if not the right time
							ind2[i,j]<-ifelse(((i-1)/4)<(j-1+0.5),0,1) # counter to set zero if not the right time
							ind3[i,j]<-ifelse(((i-1)/4)<(j-1+0.75),0,1) # counter to set zero if not the right time
							ind4[i,j]<-ifelse(((i-1)/4)<(j-1+1),0,1) # counter to set zero if not the right time

							ind_delta1[i,j]<-ifelse(((i-1)/4)==(j-1+0.25),1,0) # counter to set zero if not the right time
							ind_delta2[i,j]<-ifelse(((i-1)/4)==(j-1+0.5),1,0) # counter to set zero if not the right time
							ind_delta3[i,j]<-ifelse(((i-1)/4)==(j-1+0.75),1,0) # counter to set zero if not the right time
							ind_delta4[i,j]<-ifelse(((i-1)/4)==(j-1+1),1,0) # counter to set zero if not the right time

							delta_store[i,j]<-ind_delta1[i,j]*(delta_l[j]*g.m[j,6]) + ind_delta2[i,j]*(delta_l[j]*g.m[j,7]) + ind_delta3[i,j]*(delta_l[j]*g.m[j,8]) + ind_delta4[i,j]*(delta_l[j]*g.m[j,9])
				
							xx1[i+1,j]<-ifelse(ind1[i,j]==1,xx1[i,j]+0.25,xx1[i,j]+0) # counts the loss function
							xx2[i+1,j]<-ifelse(ind2[i,j]==1,xx2[i,j]+0.25,xx2[i,j]+0) # counts the loss function
							xx3[i+1,j]<-ifelse(ind3[i,j]==1,xx3[i,j]+0.25,xx3[i,j]+0) # counts the loss function
							xx4[i+1,j]<-ifelse(ind4[i,j]==1,xx4[i,j]+0.25,xx4[i,j]+0) # counts the loss function
				
							nets1[i,j]<-ifelse(xx1[i+1,j]>=mv_L[j],0,ind1[i,j]*(delta_l[j]*g.m[j,6])*exp(mv_k[j]-mv_k[j]/(1-(xx1[i+1,j]/mv_L[j])^2))) #multiplies the loss function
							nets2[i,j]<-ifelse(xx2[i+1,j]>=mv_L[j],0,ind2[i,j]*(delta_l[j]*g.m[j,7])*exp(mv_k[j]-mv_k[j]/(1-(xx2[i+1,j]/mv_L[j])^2))) #multiplies the loss function
							nets3[i,j]<-ifelse(xx3[i+1,j]>=mv_L[j],0,ind3[i,j]*(delta_l[j]*g.m[j,8])*exp(mv_k[j]-mv_k[j]/(1-(xx3[i+1,j]/mv_L[j])^2))) #multiplies the loss function
							nets4[i,j]<-ifelse(xx4[i+1,j]>=mv_L[j],0,ind4[i,j]*(delta_l[j]*g.m[j,9])*exp(mv_k[j]-mv_k[j]/(1-(xx4[i+1,j]/mv_L[j])^2))) #multiplies the loss function
				
				
							
							ThetaM[i,j]<-nets1[i,j]+nets2[i,j]+nets3[i,j]+nets4[i,j] # starts discounting
				
						}
					}
		
			####################################	ITNS
					for(i in 1:nrow_mv){
						k2[1,i]~dunif(16,18) 
						L2[1,i]~dunif(1.5,20.7)	
					}
					mv_k2<-k2%*%MV
					mv_L2<-L2%*%MV

		
					for(j in 1:n){

						xx1_itn[1,j]<-(-0.25)
						xx2_itn[1,j]<-(-0.25)
						xx3_itn[1,j]<-(-0.25)
						xx4_itn[1,j]<-(-0.25)
			
						g2.m[j,1]~dunif(0,1)
						g2.m[j,2]~dunif(0,1)
						g2.m[j,3]~dunif(0,1)
						g2.m[j,4]~dunif(0,1)
			
						g2.m[j,5]<-sum(g2.m[j,1:4])
						g2.m[j,6]<-g2.m[j,1]/g2.m[j,5]
						g2.m[j,7]<-g2.m[j,2]/g2.m[j,5]
						g2.m[j,8]<-g2.m[j,3]/g2.m[j,5]
						g2.m[j,9]<-g2.m[j,4]/g2.m[j,5]			

						for(i in 1:Q){
							ind1_itn[i,j]<-ifelse(((i-1)/4)<(j-1+0.25),0,1) # counter to set zero if not the right time
							ind2_itn[i,j]<-ifelse(((i-1)/4)<(j-1+0.5),0,1) # counter to set zero if not the right time
							ind3_itn[i,j]<-ifelse(((i-1)/4)<(j-1+0.75),0,1) # counter to set zero if not the right time
							ind4_itn[i,j]<-ifelse(((i-1)/4)<(j-1+1),0,1) # counter to set zero if not the right time


							ind_itn_delta1[i,j]<-ifelse(((i-1)/4)==(j-1+0.25),1,0) # counter to set zero if not the right time
							ind_itn_delta2[i,j]<-ifelse(((i-1)/4)==(j-1+0.5),1,0) # counter to set zero if not the right time
							ind_itn_delta3[i,j]<-ifelse(((i-1)/4)==(j-1+0.75),1,0) # counter to set zero if not the right time
							ind_itn_delta4[i,j]<-ifelse(((i-1)/4)==(j-1+1),1,0) # counter to set zero if not the right time

							delta_store2[i,j]<-ind_itn_delta1[i,j]*(delta2_raw[j]*g2.m[j,6])+ind_itn_delta2[i,j]*(delta2_raw[j]*g2.m[j,7])+ind_itn_delta3[i,j]*(delta2_raw[j]*g2.m[j,8])+ind_itn_delta4[i,j]*(delta2_raw[j]*g2.m[j,9])

				
							xx1_itn[i+1,j]<-ifelse(ind1_itn[i,j]==1,xx1_itn[i,j]+0.25,xx1_itn[i,j]+0) # counts the loss function
							xx2_itn[i+1,j]<-ifelse(ind2_itn[i,j]==1,xx2_itn[i,j]+0.25,xx2_itn[i,j]+0) # counts the loss function
							xx3_itn[i+1,j]<-ifelse(ind3_itn[i,j]==1,xx3_itn[i,j]+0.25,xx3_itn[i,j]+0) # counts the loss function
							xx4_itn[i+1,j]<-ifelse(ind4_itn[i,j]==1,xx4_itn[i,j]+0.25,xx4_itn[i,j]+0) # counts the loss function
				
							nets1_itn[i,j]<-ifelse(xx1_itn[i+1,j]>=mv_L2[j],0,ind1_itn[i,j]*(delta2_raw[j]*g2.m[j,6])*exp(mv_k2[j]-mv_k2[j]/(1-(xx1_itn[i+1,j]/mv_L2[j])^2)))
							nets2_itn[i,j]<-ifelse(xx2_itn[i+1,j]>=mv_L2[j],0,ind2_itn[i,j]*(delta2_raw[j]*g2.m[j,7])*exp(mv_k2[j]-mv_k2[j]/(1-(xx2_itn[i+1,j]/mv_L2[j])^2)))
							nets3_itn[i,j]<-ifelse(xx3_itn[i+1,j]>=mv_L2[j],0,ind3_itn[i,j]*(delta2_raw[j]*g2.m[j,8])*exp(mv_k2[j]-mv_k2[j]/(1-(xx3_itn[i+1,j]/mv_L2[j])^2)))
							nets4_itn[i,j]<-ifelse(xx4_itn[i+1,j]>=mv_L2[j],0,ind4_itn[i,j]*(delta2_raw[j]*g2.m[j,9])*exp(mv_k2[j]-mv_k2[j]/(1-(xx4_itn[i+1,j]/mv_L2[j])^2)))

							ThetaM2[i,j]<-nets1_itn[i,j]+nets2_itn[i,j]+nets3_itn[i,j]+nets4_itn[i,j] # starts discounting
						}
					}		
					
				
			for(i in 1:Q){
				ThetaT[i]<-sum(ThetaM[i,1:n])
				ThetaT2[i]<-sum(ThetaM2[i,1:n])
				llinD[i]<-sum(delta_store[i,1:n])
				itnD[i]<-sum(delta_store2[i,1:n])
			}

			for(i in 1:n2){
				j[i]<-index2a[i]	 
				j2[i]<-index2b[i]	 	
				
				pred1[i]<-sa[i]*ThetaT[j[i]]+sb[i]*ThetaT[j2[i]]	
				pred2[i]<-sa[i]*ThetaT2[j[i]]+sb[i]*ThetaT2[j2[i]]	
				pred3[i]<-pred1[i]+pred2[i]
					
				mTot_llin[i] ~ dnorm(pred1[i],sTot_llin[i]^-2)	T(llinlimL[i],llinlimH[i])
				mTot_itn[i] ~ dnorm(pred2[i],sTot_itn[i]^-2) T(itnlimL[i],itnlimH[i])
			}
		
			trace~dunif(1,5000)
			sample<-round(trace)

			trace2~dunif(1,5000)
			sample2<-round(trace2)
			
			p1_b1<-prop1_b1[sample]
			p1_b2<-prop1_b2[sample]
			p1_b3<-prop1_b3[sample]
			p1_b4<-prop1_b4[sample]
			p1_b5<-prop1_b5[sample]
			p1_b6<-prop1_b6[sample]
			p1_b7<-prop1_b7[sample]
			p1_b8<-prop1_b8[sample]
			p1_b9<-prop1_b9[sample]
			p1_b10<-prop1_b10[sample]
			p1_i1<-prop1_i1[sample]
			p1_i2<-prop1_i2[sample]
			p1_i3<-prop1_i3[sample]
			p1_i4<-prop1_i4[sample]
			p1_i5<-prop1_i5[sample]
			p1_i6<-prop1_i6[sample]
			p1_i7<-prop1_i7[sample]
			p1_i8<-prop1_i8[sample]
			p1_i9<-prop1_i9[sample]
			p1_i10<-prop1_i10[sample]

			p0_b1<-prop0_b1[sample2]
			p0_b2<-prop0_b2[sample2]
			p0_b3<-prop0_b3[sample2]
			p0_p1<-prop0_p1[sample2]
			p0_p2<-prop0_p2[sample2]
			p0_i1<-prop0_i1[sample2]
			for(i in 1:Q){	
				ThetaT3[i]<-ifelse(((ThetaT[i]+ThetaT2[i])/(PAR*IRS*population[i]))<0,0,((ThetaT[i]+ThetaT2[i])/(PAR*IRS*population[i])))
				for(j in 1:10){
						prop0[i,j]<-p0_i1 + p0_p1*j + p0_p2*pow(j,2) + p0_b1*ThetaT3[i] + p0_b2*pow(ThetaT3[i],2) + p0_b3*pow(ThetaT3[i],3)	
				}
				prop1[i,1]<-p1_i1 + p1_b1*ThetaT3[i]
				prop1[i,2]<-p1_i2 + p1_b2*ThetaT3[i]
				prop1[i,3]<-p1_i3 + p1_b3*ThetaT3[i]
				prop1[i,4]<-p1_i4 + p1_b4*ThetaT3[i]
				prop1[i,5]<-p1_i5 + p1_b5*ThetaT3[i]
				prop1[i,6]<-p1_i6 + p1_b6*ThetaT3[i]
				prop1[i,7]<-p1_i7 + p1_b7*ThetaT3[i]
				prop1[i,8]<-p1_i8 + p1_b8*ThetaT3[i]
				prop1[i,9]<-p1_i9 + p1_b9*ThetaT3[i]
				prop1[i,10]<-p1_i10 + p1_b10*ThetaT3[i]		
			}
	}'

# full sample run
n.adapt=10000
update=1000000
n.iter=50000
thin=100

#n.adapt=1000
#update=1000
#n.iter=1000
#thin=10


jags<-c()
jags <- jags.model(file=textConnection(model_string),
                   data = SVY,
                   n.chains = 1,
                   n.adapt=n.adapt)
update(jags,n.iter=update)

jdat <- coda.samples(jags,variable.names=c('extra','delta_l','able','nets1','nets2','nets3','nets4','nets1_itn','nets2_itn','nets3_itn','nets4_itn','xx1','xx2','xx3','xx4','xx1_itn','xx2_itn','xx3_itn','xx4_itn','g.m','g2.m','delta_store','llinD','itnD','ThetaT3','prop1','prop0','mv_k2','mv_L2','ThetaT2','ThetaM2','delta','delta2_raw','delta_raw','mu','Psi','s_m','s_d','ThetaT','ThetaM','mv_k','mv_L'),n.iter=n.iter,thin=thin) 

