'''
Description: Runs GPR on all data using selected parameters 
'''

import pylab as pl
import numpy as np
import math
import os
import sys
import getpass

user = getpass.getuser()
# user = sys.argv[1]
os.chdir('filepath' % (user))
import gpr; reload(gpr)


'''
Get GPR settings
'''

cc = sys.argv[1]
sexes = ['male','female']
huncert = int(sys.argv[2])
hsim = int(sys.argv[3])

if (huncert == int(1)):
	dr = int(1000)
	iters = range((hsim-1)*25+1,(hsim-1)*25+1+25)
else:
	dr = int(1000)
	iters = range(1,2)

# transformation for the GPR step, choose from: ('log10','ln','logit','logit10')
transform = 'logit' 


'''
Import data
'''
hitercount = 0
for iter in iters:
	for ss in sexes:
		if (huncert == int(1)):
			os.chdir('filepath')
			data = pl.csv2rec('filepath' % (iter), missing='NA')

		else:
			os.chdir('filepath' % ('filepath' if os.name=='posix' else 'filepath'))
			data = pl.csv2rec('filepath', missing='NA')

		# data
		index = (data['ihme_loc_id'] == cc) & (data['sex'] == ss) & (data['data'] == 1)
		data_year = pl.array(data['year'][index])
		data_mort = pl.array(data['log_mort'][index])
		data_stderr = pl.array(data['log_stderr'][index])
		data_category = pl.array(data['category'][index])


		# prior
		index = (data['ihme_loc_id'] == cc) & (data['sex'] == ss)
		prior_year = pl.array(data['year'][index])
		if (transform == 'log10'):
			prior_mort = pl.log(pl.array(data['pred2final'][index]))/pl.log(10) # this is to convert the prior to log base-10 space
		elif (transform == 'ln'):
			prior_mort = pl.log(pl.array(data['pred2final'][index])) # this is to convert the prior to natural log space
		elif (transform == 'logit'):
			prior_mort = pl.log(pl.array(data['pred2final'][index])/(1 - pl.array(data['pred2final'][index]))) # this is to convert the prior to logit space
		elif (transform == 'logit10'):
			prior_mort = pl.log(pl.array(data['pred2final'][index])/(1 - pl.array(data['pred2final'][index])))/pl.log(10) # this is to convert the prior to logit10 space

		# prediction years & country data type 
		predictionyears = pl.array(range(int(math.floor(data['year'].min())),int(math.floor(data['year'].max()+1)))) + 0.5
		type = data['type'][index][0]
		data_mse = data['mse'][index][0]


		'''
		Import best parameters 
		'''
		
		os.chdir('filepath' % ('filepath' if os.name=='posix' else 'filepath'))
		data = pl.csv2rec('filepath', missing='NA')

		best_scale = float(data['scale'][(data['best']==1) & (data['type']==type)][0])
		best_amp2 = float(data['amp2x'][(data['best']==1) & (data['type']==type)][0]) * data_mse

		#	if (type=="no data"):
		#		best_amp2 = float(data['amp2'][(data['best']==1) & (data['type']==type)][0])

		#if cc=="MNE":
		#	best_scale = 20
		#	best_amp2 = 2
		print("Best amp2 is:", best_amp2)

		'''
		Test all parameter combinations
		'''

		# fit model, get predictions 

		if (len(data_year) == 0): # no data model 
			[M,C] = gpr.gpmodel_all(prior_mort, prior_year, best_scale, best_amp2)
			d = gpr.gpmodel_all_pred(M, C, dr, predictionyears)
		else: # data model 
			if (dr == int(10)):
				[gpmort, allobs, allyear, allvar, bias_vr, bias_sibs] = gpr.gpmodel(prior_mort, prior_year, data_mort, data_year, data_stderr, data_category, best_scale, best_amp2, cc)
				d = gpr.gpmodel_pred(gpmort, allyear, allvar, allobs, bias_vr, bias_sibs, 5000, 3000, 2, predictionyears)
			else:
				[gpmort, allobs, allyear, allvar, bias_vr, bias_sibs] = gpr.gpmodel(prior_mort, prior_year, data_mort, data_year, data_stderr, data_category, best_scale, best_amp2, cc)
				d = gpr.gpmodel_pred(gpmort, allyear, allvar, allobs, bias_vr, bias_sibs, 5000, 3000, 2, predictionyears)

		if (transform == 'log10'):
			unlog_est = gpr.results(10**d) # log base 10 space
		elif (transform == 'ln'):
			unlog_est = gpr.results(math.e**d) # natural log space
		elif (transform == 'logit'):
			unlog_est = gpr.results((math.e**d)/(1+(math.e**d))) # logit space
		elif (transform == 'logit10'):
			unlog_est = gpr.results((10**d)/(1+(10**d))) # logit10 space

		# save the predictions
		
		all_est = []

		for i in range(len(predictionyears)):
			all_est.append((cc, ss, predictionyears[i], unlog_est['med'][i], unlog_est['lower'][i], unlog_est['upper'][i]))

		all_est = pl.array(all_est, [('ihme_loc_id', '|S32'), ('sex', '|S32'), ('year', '<f8'), ('mort_med', '<f8'), ('mort_lower', '<f8'), ('mort_upper', '<f8')])

		## no need to save the summary if we're doing the HIV draws version
		if (huncert == int(1)):
			os.chdir('filepath')
		else:
			os.chdir('filepath')
			pl.rec2csv(all_est, 'filepath' %(cc, ss))

		# save the sims 
		all_sim = []

		for i in range(len(predictionyears)):
			for s in range(dr):
				if (transform == 'log10'):
					all_sim.append((cc, ss, predictionyears[i], s, 10**d[s][i])) # log base 10 space
				elif (transform == 'ln'):
					all_sim.append((cc, ss, predictionyears[i], s, math.e**d[s][i])) # natural log space
				elif (transform == 'logit'):
					all_sim.append((cc, ss, predictionyears[i], s, ((math.e**d[s][i])/(1+(math.e**d[s][i]))))) # logit space
				elif (transform == 'logit10'):
					all_sim.append((cc, ss, predictionyears[i], s, ((10**d[s][i])/(1+(10**d[s][i]))))) # logit10 space
					
		all_sim = pl.array(all_sim, [('ihme_loc_id', '|S32'), ('sex', '|S32'), ('year', '<f8'), ('sim', '<f8'), ('mort', '<f8')])
		all_sim['sim'] = all_sim['sim']+((iter-1)*1000)
		
		if (huncert == int(1)):
			# 
			hitercount = hitercount + 1
			if (hitercount == 50):
				os.chdir('filepath')
				drawlist = pl.csv2rec('filepath' % (cc), missing='NA')
				all_sim_comp = np.append(all_sim_comp, all_sim, axis=1)
				# subset to just the draws that are chosen from the file
				c = set(drawlist['sim']) & set(all_sim_comp['sim']) 
				c #contains elements of a that are in b (and vice versa) 
				indices = np.where([x in c for x in all_sim_comp['sim']])[0] 
				print(indices) #indices of b where the elements of a in b occur 
				all_sim_comp = all_sim_comp[indices]
				os.chdir('filepath')
				pl.rec2csv(all_sim_comp, 'filepath' %(cc, hsim))
			elif (hitercount == 1):
				all_sim_comp = all_sim
			else:
				print(all_sim)
				all_sim_comp = np.append(all_sim_comp, all_sim, axis=1)
		else:
			os.chdir('filepath')
			pl.rec2csv(all_sim, 'filepath' %(cc, ss))
	#end
	
