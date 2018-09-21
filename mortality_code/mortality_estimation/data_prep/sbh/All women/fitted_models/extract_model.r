## extract the fixed and random effects from the models by method (MAC, MAP, TFBC, and TFBP) to be used when calculating 5q0 from summary birth histories in individual surveys.
# Note - this must be run on an earlier version of R than the current one. (ex. R 3.0.1)

require(lme4.0)
require(plyr)

load("strDirectory/All women/models.rdata")

###################################
## output.mac
###################################

df.varnames <- data.frame()
df.error <- data.frame()
df.iso_re <- data.frame()
df.reg_re <- data.frame()
df.fe <- data.frame()

for (a in names(output.mac)) {
    

    model <- output.mac[[a]]

    
    fit = model$original.fit
    error = data.frame(model$error)
    error$group <- a
    error$type <- rownames(error)
    
    dep.var.notrans = all.vars(formula(terms(fit)))[1]
    dep.var = as.character(formula(terms(fit))[2])
    vns <- data.frame(type=c("depvar", "tranform"), varname=c(dep.var.notrans, dep.var))
    
    indep.var = attr(terms(fit), "term.labels")
    vns <- rbind(vns, data.frame(type="indepvar", varname=indep.var))
    vns$group <- a
    
    # Extract fixed effects
    fe <- data.frame(fixef(fit))
    fe$group <- a
    fe$param <- rownames(fe)
    
    # Extract random effects
    iso_re <- model$re.dist$iso3[,c("iso3", "RE.mean")]
    names(iso_re) <- c("iso3", "iso3.re")
    iso_re$group <- a
    
    reg_re <- model$re.dist$gbdregion[,c("gbdregion", "RE.mean")]
    names(reg_re) <- c("gbdregion", "region.re")
    reg_re$group <- a
 
    df.varnames <- rbind(df.varnames, vns)
    df.error <- rbind(df.error, error)
    df.iso_re <- rbind(df.iso_re, iso_re)
    df.reg_re <- rbind(df.reg_re, reg_re)
    df.fe <- rbind(df.fe, fe)
}

write.csv(df.fe, "strDirectory/All women/fitted_models/output.mac/output.mac.fe.csv", row.names=F)
write.csv(df.varnames, "strDirectory/All women/fitted_models/output.mac/output.mac.varnames.csv", row.names=F)
write.csv(df.iso_re, "strDirectory/All women/fitted_models/output.mac/output.mac.iso_re.csv", row.names=F)
write.csv(df.reg_re, "strDirectory/All women/fitted_models/output.mac/output.mac.reg_re.csv", row.names=F)
write.csv(df.error, "strDirectory/All women/fitted_models/output.mac/output.mac.errors.csv", row.names=F)


#############################################
####  output.mac.reftime
#############################################


df.varnames <- data.frame()
df.error <- data.frame()
df.iso_re <- data.frame()
df.reg_re <- data.frame()
df.fe <- data.frame()

for (a in names(output.mac.reftime)) {
  # a = "15-19"
  model <- output.mac.reftime[[a]]
  
  
  fit = model$original.fit
 
  dep.var.notrans = all.vars(formula(terms(fit)))[1]
  dep.var = as.character(formula(terms(fit))[2])
  vns <- data.frame(type=c("depvar", "tranform"), varname=c(dep.var.notrans, dep.var))
  
  indep.var = attr(terms(fit), "term.labels")
  vns <- rbind(vns, data.frame(type="indepvar", varname=indep.var))
  vns$group <- a
  
  # Extract fixed effects
  fe <- data.frame(summary(fit)$coefficients)
  fe <- rename(fe, c("Estimate" = "fixef.fit"))
  fe$group <- a
  fe$param <- rownames(fe)
  fe <- fe[,c("fixef.fit", "group", "param")]

   
  df.varnames <- rbind(df.varnames, vns)
  df.fe <- rbind(df.fe, fe)
}


write.csv(df.fe, "strDirectory/All women/fitted_models/output.mac.reftime/output.mac.reftime.fe.csv", row.names=F)
write.csv(df.varnames, "strDirectory/All women/fitted_models/output.mac.reftime/output.mac.reftime.varnames.csv", row.names=F)


#####################################################
######### output.map ##############################
#####################################################




df.varnames <- data.frame()
df.error <- data.frame()
df.iso_re <- data.frame()
df.reg_re <- data.frame()
df.fe <- data.frame()

for (a in names(output.map)) {
  
  model <- output.map[[a]]
  
  
  fit = model$original.fit
  error = data.frame(model$error)
  error$group <- a
  error$type <- rownames(error)
  
  dep.var.notrans = all.vars(formula(terms(fit)))[1]
  dep.var = as.character(formula(terms(fit))[2])
  vns <- data.frame(type=c("depvar", "tranform"), varname=c(dep.var.notrans, dep.var))
  
  indep.var = attr(terms(fit), "term.labels")
  vns <- rbind(vns, data.frame(type="indepvar", varname=indep.var))
  vns$group <- a
  
  # Extract fixed effects
  fe <- data.frame(fixef(fit))
  fe$group <- a
  fe$param <- rownames(fe)
  
  # Extract random effects
  iso_re <- model$re.dist$iso3[,c("iso3", "RE.mean")]
  names(iso_re) <- c("iso3", "iso3.re")
  iso_re$group <- a
  
  reg_re <- model$re.dist$gbdregion[,c("gbdregion", "RE.mean")]
  names(reg_re) <- c("gbdregion", "region.re")
  reg_re$group <- a
  
  df.varnames <- rbind(df.varnames, vns)
  df.error <- rbind(df.error, error)
  df.iso_re <- rbind(df.iso_re, iso_re)
  df.reg_re <- rbind(df.reg_re, reg_re)
  df.fe <- rbind(df.fe, fe)
}

write.csv(df.fe, "strDirectory/All women/fitted_models/output.map/output.map.fe.csv", row.names=F)
write.csv(df.varnames, "strDirectory/All women/fitted_models/output.map/output.map.varnames.csv", row.names=F)
write.csv(df.iso_re, "strDirectory/All women/fitted_models/output.map/output.map.iso_re.csv", row.names=F)
write.csv(df.reg_re, "strDirectory/All women/fitted_models/output.map/output.map.reg_re.csv", row.names=F)
write.csv(df.error, "strDirectory/All women/fitted_models/output.map/output.map.errors.csv", row.names=F)

#############################################################
########## output.tfbc
#############################################################


df.varnames <- data.frame()
df.error <- data.frame()
df.iso_re <- data.frame()
df.reg_re <- data.frame()
df.fe <- data.frame()

for (a in names(output.tfbc)) {
  
  model <- output.tfbc[[a]]
  
  
  fit = model$original.fit
  error = data.frame(model$error)
  error$group <- a
  error$type <- rownames(error)
  
  dep.var.notrans = all.vars(formula(terms(fit)))[1]
  dep.var = as.character(formula(terms(fit))[2])
  vns <- data.frame(type=c("depvar", "tranform"), varname=c(dep.var.notrans, dep.var))
  
  indep.var = attr(terms(fit), "term.labels")
  vns <- rbind(vns, data.frame(type="indepvar", varname=indep.var))
  vns$group <- a
  
  # Extract fixed effects
  fe <- data.frame(fixef(fit))
  fe$group <- a
  fe$param <- rownames(fe)
  
  # Extract random effects
  iso_re <- model$re.dist$iso3[,c("iso3", "RE.mean")]
  names(iso_re) <- c("iso3", "iso3.re")
  iso_re$group <- a
  
  reg_re <- model$re.dist$gbdregion[,c("gbdregion", "RE.mean")]
  names(reg_re) <- c("gbdregion", "region.re")
  reg_re$group <- a
  
  df.varnames <- rbind(df.varnames, vns)
  df.error <- rbind(df.error, error)
  df.iso_re <- rbind(df.iso_re, iso_re)
  df.reg_re <- rbind(df.reg_re, reg_re)
  df.fe <- rbind(df.fe, fe)
}

write.csv(df.fe, "strDirectory/All women/fitted_models/output.tfbc/output.tfbc.fe.csv", row.names=F)
write.csv(df.varnames, "strDirectory/All women/fitted_models/output.tfbc/output.tfbc.varnames.csv", row.names=F)
write.csv(df.iso_re, "strDirectory/All women/fitted_models/output.tfbc/output.tfbc.iso_re.csv", row.names=F)
write.csv(df.reg_re, "strDirectory/All women/fitted_models/output.tfbc/output.tfbc.reg_re.csv", row.names=F)
write.csv(df.error, "strDirectory/All women/fitted_models/output.tfbc/output.tfbc.errors.csv", row.names=F)


#########################################################
###### output.tfbc.reftime
########################################################



df.varnames <- data.frame()
df.error <- data.frame()
df.iso_re <- data.frame()
df.reg_re <- data.frame()
df.fe <- data.frame()

for (a in names(output.tfbc.reftime)) {

  model <- output.tfbc.reftime[[a]]
  
  
  fit = model$original.fit
  
  dep.var.notrans = all.vars(formula(terms(fit)))[1]
  dep.var = as.character(formula(terms(fit))[2])
  vns <- data.frame(type=c("depvar", "tranform"), varname=c(dep.var.notrans, dep.var))
  
  indep.var = attr(terms(fit), "term.labels")
  vns <- rbind(vns, data.frame(type="indepvar", varname=indep.var))
  vns$group <- a
  
  # Extract fixed effects
  fe <- data.frame(summary(fit)$coefficients)
  fe <- rename(fe, c("Estimate" = "fixef.fit"))
  fe$group <- a
  fe$param <- rownames(fe)
  fe <- fe[,c("fixef.fit", "group", "param")]
  

  #   
  df.varnames <- rbind(df.varnames, vns)
  
  df.fe <- rbind(df.fe, fe)
}


write.csv(df.fe, "strDirectory/All women/fitted_models/output.tfbc.reftime/output.tfbc.reftime.fe.csv", row.names=F)
write.csv(df.varnames, "strDirectory/All women/fitted_models/output.tfbc.reftime/output.tfbc.reftime.varnames.csv", row.names=F)


##############################################################
#####output.tfbp
##############################################################



df.varnames <- data.frame()
df.error <- data.frame()
df.iso_re <- data.frame()
df.reg_re <- data.frame()
df.fe <- data.frame()

for (a in names(output.tfbp)) {
  
  model <- output.tfbp[[a]]
  
  
  fit = model$original.fit
  error = data.frame(model$error)
  error$group <- a
  error$type <- rownames(error)
  
  dep.var.notrans = all.vars(formula(terms(fit)))[1]
  dep.var = as.character(formula(terms(fit))[2])
  vns <- data.frame(type=c("depvar", "tranform"), varname=c(dep.var.notrans, dep.var))
  
  indep.var = attr(terms(fit), "term.labels")
  vns <- rbind(vns, data.frame(type="indepvar", varname=indep.var))
  vns$group <- a
  
  # Extract fixed effects
  fe <- data.frame(fixef(fit))
  fe$group <- a
  fe$param <- rownames(fe)
  
  # Extract random effects
  iso_re <- model$re.dist$iso3[,c("iso3", "RE.mean")]
  names(iso_re) <- c("iso3", "iso3.re")
  iso_re$group <- a
  
  reg_re <- model$re.dist$gbdregion[,c("gbdregion", "RE.mean")]
  names(reg_re) <- c("gbdregion", "region.re")
  reg_re$group <- a
  
  df.varnames <- rbind(df.varnames, vns)
  df.error <- rbind(df.error, error)
  df.iso_re <- rbind(df.iso_re, iso_re)
  df.reg_re <- rbind(df.reg_re, reg_re)
  df.fe <- rbind(df.fe, fe)
}

write.csv(df.fe, "strDirectory/All women/fitted_models/output.tfbp/output.tfbp.fe.csv", row.names=F)
write.csv(df.varnames, "strDirectory/All women/fitted_models/output.tfbp/output.tfbp.varnames.csv", row.names=F)
write.csv(df.iso_re, "strDirectory/All women/fitted_models/output.tfbp/output.tfbp.iso_re.csv", row.names=F)
write.csv(df.reg_re, "strDirectory/All women/fitted_models/output.tfbp/output.tfbp.reg_re.csv", row.names=F)
write.csv(df.error, "strDirectory/All women/fitted_models/output.tfbp/output.tfbp.errors.csv", row.names=F)






