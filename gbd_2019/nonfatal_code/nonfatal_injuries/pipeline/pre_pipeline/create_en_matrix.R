rm(list = ls())
if(!require(pacman)) install.packages("pacman")
pacman::p_load(foreign, data.table, magrittr, DirichletReg)

data_dir <- paste0("FILEPATH")
out_dir <- paste0("FILEPATH")

directories = c(paste0(out_dir, "/01_draws/inp"), 
                paste0(out_dir, "/01_draws/otp"),
                paste0(out_dir, "/02_summary/inp"),
                paste0(out_dir, "/02_summary/otp"))
lapply(directories, dir.create, recursive = TRUE)

EN = paste0("FILEPATH") %>% fread
ndraw <- 1000
e_codes <- EN[e_code != "", as.character(unique(e_code))]
platform <- c(0, 1)

source(paste0(workdir,"dirichlet_predict_fun.R"))

name = paste0("draw_", 0:(ndraw - 1))

covarsdata = expand.grid(list("high_income" = c(0, 1),
                              "sex" = c(1, 2),
                              "age" = c(0, 1, seq(5, 95, by = 5)))) %>% data.table

for(ECODE in e_codes){
  for(PLATFORM in platform){
    cat(paste0("Beginning loop for e-code ", ECODE, " and platform ", PLATFORM, "\n"))
    ecode_sub = EN[e_code == ECODE]
    
    ncodes = grep("^N", names(ecode_sub), value = TRUE)
    covars = c("high_income", "inpatient", "age", "sex")
    
    platform_sub = ecode_sub[inpatient == PLATFORM,]
    counter = 0
    
    ncodes_drop = vector(length = length(ncodes))
    
    for(NCODES in 1:length(ncodes)){
      if(sum(platform_sub[,ncodes[NCODES],with = F]) == 0){ncodes_drop[NCODES] = T; counter = counter + 1}
    }
    
    DRdata = DR_data(platform_sub[, ncodes[!ncodes_drop], with = F])
    
    regression = DirichReg(DRdata ~ high_income + sex + age, platform_sub, verbosity = 1)
    converged = regression$optimization[[1]]
    
    results = confint.DirichletRegModel(regression)
    coeffs = results$coef
    stderror = results$se
    
    coeffs_ul = unlist(coeffs)
    
    params = data.table(coeffs_ul, stderror)
    draws = matrix(nrow = length(coeffs_ul), ncol = ndraw,NA) # simulate the draws
    
    for(PARAMETERS in 1:nrow(params)){
      draws[PARAMETERS,] = rnorm(ndraw, params[[PARAMETERS,1]],params[[PARAMETERS,2]])
    }
    
    predict_draws = matrix(nrow = nrow(covarsdata)*length(ncodes[!ncodes_drop]), ncol = ndraw, NA)
    
    for(DRAW in 1:ndraw){
      ncoef = draws[,DRAW]
      names(ncoef) = names(coeffs_ul)
      ncoef = relist(ncoef, coeffs)
      
      prediction = predict.DirichletRegModel.newcoef(regression, covarsdata, newcoef = ncoef)
      prediction = prediction %>% data.table
      setnames(prediction, 1:length(ncodes[!ncodes_drop]), ncodes[!ncodes_drop])
      
      prediction = prediction %>% melt
      predict_draws[,DRAW] = prediction[,value]
      if(DRAW %% 25 == 0) cat("Draw ",DRAW," Complete","\n")
    } # finished with draws loop
    
    predict_draws = predict_draws %>% data.table
    setnames(predict_draws,1:ndraw,name)
    predict_draws = data.table(ecode = rep(ECODE, nrow(predict_draws)),
                               n_code = rep(ncodes[!ncodes_drop], each = nrow(covarsdata)),
                               inpatient = rep(PLATFORM, nrow(predict_draws)),
                               covarsdata,
                               predict_draws)
    
    if(counter == 0){
      complete = predict_draws
    } else {
      zero_models = data.table(matrix(nrow = length(ncodes[ncodes_drop])*nrow(covarsdata), ncol = 1000, 0))
      setnames(zero_models, 1:1000,name)
      zero_models = data.table(ecode = rep(ECODE, nrow(zero_models)),
                               n_code = rep(ncodes[ncodes_drop], each = nrow(covarsdata)),
                               inpatient = rep(PLATFORM, nrow(zero_models)),
                               covarsdata,
                               zero_models)
      
      complete = rbind(predict_draws, zero_models)
    }
    
    if(PLATFORM == 1){
      plat <- "inp"
    } else {
      plat <- "otp"
    }
    
    filepath <- paste0(out_dir, "/01_draws/", plat, "/", ECODE, ".csv")
    
    cat("Writing draws file to ", filepath, "\n")
    fwrite(complete, filepath, row.names = F, showProgress = TRUE)
    cat(paste0("Finished with e-code ", ECODE, " and platform ", plat, " draws file."))
    
    complete[, mean := apply(complete[ , name, with = F], 1, mean)]
    complete[, ll := apply(complete[ , name, with = F], 1, quantile, probs = c(0.025))]
    complete[, ul := apply(complete[ , name, with = F], 1, quantile, probs = c(0.975))]
    complete[, c("ll", "ul") := t(apply(complete[, name, with = F], 1, quantile, probs = c(0.025, 0.975)))]
    
    complete = complete[ ,c("n_code",covars,"mean","ll","ul"), with = F]
    
    filepath <- paste0(out_dir, "/02_summary/", plat, "/", ECODE, ".csv")
    
    cat("Writing summary file to ", filepath, "\n")
    fwrite(complete, filepath, row.names = F, showProgress = TRUE)
    cat(paste0("Finished with e-code ", ECODE, " and platform ", PLATFORM, " summary file. \n"))
  }
}

ECODES <- "inj_medical"

for(i in 1:length(platform)){
  inj_medical = expand.grid(list("ecode" = "inj_medical",
                                 "n_code" = ncodes,
                                 "inpatient" = platform[i],
                                 "high_income" = c(0, 1),
                                 "sex" = c(1, 2),
                                 "age" = c(0, 1, seq(5, 95, by = 5)))) %>% data.table
  
  inj_medical[, (name) := 0]
  inj_medical[n_code == "N46", (name) := 1]
  
  if(platform[i] == 1){
    plat <- "inp"
  } else {
    plat <- "otp"
  }
  
  filepath <- paste0(out_dir, "/01_draws/", plat, "/", "inj_medical.csv")
  fwrite(inj_medical, filepath, row.names = F, showProgress = T)
  
  inj_medical[, mean := apply(inj_medical[ , name, with = F], 1, mean)]
  inj_medical[, ll := apply(inj_medical[ , name, with = F], 1, quantile, probs = c(0.025))]
  inj_medical[, ul := apply(inj_medical[ , name, with = F], 1, quantile, probs = c(0.975))]
  inj_medical[, c("ll", "ul") := t(apply(inj_medical[, name, with = F], 1, quantile, probs = c(0.025, 0.975)))]
  
  inj_medical = inj_medical[ ,c("n_code",covars,"mean","ll","ul"), with = F]
  
  filepath <- paste0(out_dir, "/02_summary/", plat, "/", "inj_medical.csv")
  fwrite(inj_medical, filepath, row.names = F, showProgress = T)
}

dir <- paste0(out_dir, "/02_summary/inp")
files <- grep(".csv", list.files(dir), value = T)

ecodes <- gsub(".csv", "", files)
ecodes <- ecodes[!grepl("en_matrix", ecodes)]
ecodes <- ecodes[ecodes != "inj_medical" & !ecodes %in% grep("summ", ecodes, value = TRUE)]

read.e <- function(ecode){

  path.inp <- paste0(out_dir, "/02_summary/inp/", ecode, ".csv")
  data.inp <- path.inp %>% fread

  path.otp <- paste0(out_dir, "/02_summary/otp/", ecode, ".csv")
  data.otp <- path.otp %>% fread
  
  data <- rbind(data.inp, data.otp, use.names = TRUE)
  
  data[, e_code := ecode]
  
  return(data)
}

fitted <- lapply(ecodes, read.e) %>% rbindlist(use.names = TRUE)
fitted <- fitted[, list(n_code, high_income, inpatient, age, sex, e_code, mean)]
setnames(fitted, "mean", "fitted")

raw <- paste0("FILEPATH") %>% fread
ncodes <- grep("N", names(raw), value = TRUE)

raw <- raw[, (ncodes) := lapply(.SD, function(x) x / raw[['totals']]), .SDcols = ncodes]

raw <- melt(raw, id.vars = c("age", "sex", "high_income", "inpatient", "e_code"), measure.vars = patterns("^N"))
setnames(raw, c("variable", "value"), c("n_code", "raw"))

diag_dir <- "FILEPATH"
master <- merge(raw, fitted, by = c("age", "sex", "high_income", "inpatient", "e_code", "n_code"))

fwrite(master, paste0(diag_dir, "/model_fit.csv"))

