###############################################
## GET SYMPTOMATIC PROP FROM MRBRT CROSSWALK ##
###############################################

#UTERINE FIBROIDS, SEE WHAT PROPORTION OF CASES ARE SYMPTOMATIC 
j_temp <- "FILEPATH"
coef_fpath <- paste0(j_temp, "FILEPATH" )
print(coef_fpath)

network_coefs <- data.table(read.csv(paste0(coef_fpath, "FILEPATH")))
network_coefs

#logit space
symp_coef <- network_coefs[x_cov == "cv_symptomatic", beta_soln]
symp_coef

#normal space
norm_sympcoef <- invlogit(symp_coef)
norm_sympcoef

#factors
asymptomatic_factor = 1 / (1 + norm_sympcoef)
symptomatic_factor = norm_sympcoef

asymptomatic_factor + symptomatic_factor == 1

print(symptomatic_factor)
print(asymptomatic_factor)


