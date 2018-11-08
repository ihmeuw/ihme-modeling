#----DEPENDENCIES-----------------------------------------------------------------------------------------------------------------
# load packages, install if missing
pacman::p_load(data.table, parallel, plyr, reshape2)


#********************************************************************************************************************************
#this function preps the various RR curves by reading their output files and then appending into a single list
prepRR <- function(age.cause.number,
                   rr.dir) {

  cause.code <- age.cause[age.cause.number, 1]
  age.code <- age.cause[age.cause.number, 2]

  # parameters that define these curves are used to generate age/cause specific RRs for a given exposure level
 
  if(grepl("phase",rr.dir)==T){
    fitted.parameters <- fread(paste0(rr.dir, "/params_", cause.code, "_", age.code,"_phaseII.csv"))
   } else{
      fitted.parameters <- fread(paste0(rr.dir, "/params_", cause.code, "_", age.code, ".csv"))
    }

  setnames(fitted.parameters, "V1", "draws")

  return(fitted.parameters)

}

#********************************************************************************************************************************

#********************************************************************************************************************************
# calculate RRs due to air PM exposure using various methods and
calculatePAFs <- function(age.cause.number,
                          exposure.object,
                          rr.curves,
                          metric.type,
                          fx.draws=draws.required,
                          fx.cores=1){

  #define the columns we will be creating
  amb.exp.cols <- paste0("draw_",1:fx.draws)
  hap.exp.cols <- paste0("hap_exp_",1:fx.draws)
  tot.exp.cols <- paste0("tot_exp_",1:fx.draws)
  hap.prop.cols <- paste0("hap_prop_",1:fx.draws)
  amb.rr.cols <- paste0("amb_rr_", 1:fx.draws)
  tot.rr.cols <- paste0("tot_rr_", 1:fx.draws)
  hap.rr.cols <- paste0("hap_rr_", 1:fx.draws)
  rr.cols <- paste0("rr_", 1:fx.draws)
  paf.cols <- paste0("paf_", 1:fx.draws)
  out.cols <- paste0("draw_", 0:(draws.required-1))
  amb.paf.cols <- paste0("amb_paf_", 1:fx.draws)
  hap.paf.cols <- paste0("hap_paf_", 1:fx.draws)
  draw.cols <- paste0("draw_", 1:fx.draws)
  all.cols <- lapply(ls()[grep('cols', ls())], function(x, ...) get(x)) #can be passed to drawTracker

  # pull cause/age of interest from list defined by loop#
  cause.code <- age.cause[age.cause.number, 1]
  age.code <- age.cause[age.cause.number, 2]

  # display loop status
  message(paste0(metric.type, " - Cause:", cause.code, " - Age:", age.code))

  dt <- copy(exposure.object)

  # create ratios by which to adjust RRs for morbidity for applicable causes (these are derived from literature values)
  if (cause.code == "cvd_ihd" & metric.type == "yld") {

    cvd.ratio <- 0.141

  } else if (cause.code == "cvd_stroke" & metric.type == "yld") {

    cvd.ratio <- 0.553

  } else {

    cvd.ratio <- 1

  }

  calcRR <- function(col.n, ratio, exp, exp.cols, curve, curve.n) {

    crude <- fobject$eval(exp[, get(exp.cols[col.n])], curve[[curve.n]][col.n, ])
    out <- ratio * crude - ratio + 1

    return(out)

  }
  

  # Generate the RRs using the evaluation function and then scale them using the predefined ratios
  dt[, c(amb.rr.cols) := lapply(1:fx.draws,
                                 calcRR, # Use function object, the exposure, and the RR parameters to calculate PAF
                                 ratio=cvd.ratio,
                                 exp=exposure.object,
                                 exp.cols=amb.exp.cols,
                                 curve=rr.curves,
                                 curve.n=age.cause.number)]

  #check vals
  subtract <- sapply(all.cols,
                     drawTracker,
                     random.draws=sample(fx.draws, 3)) %>% unlist
  check <- head(dt[, -(subtract), with=F])
  
  
 

  #merge on the hap exposure, which will split the ambient dt into 3 (hap groupings = male/female/child)
  if(cause.code!='lri') hap.exp <- hap.exp[grouping!='child'] #child exposure only relevant to LRI
  dt[, merge := 1]
  dt <- merge(dt, hap.exp, by='merge')
  
  #sum ambient and hap to get total exposure for the pop exposed to hap
  dt[,(tot.exp.cols):= lapply(1:fx.draws,
                              function(draw) {
                                get(hap.exp.cols[draw]) + get(amb.exp.cols[draw])
                              })]

  # Generate the HAP RRs using the evaluation function and then scale them using the predefined ratios
  dt[, c(tot.rr.cols) := lapply(1:fx.draws,
                                 calcRR, # Use function object, the exposure, and the RR parameters to calculate PAF
                                 ratio=cvd.ratio,
                                 exp=dt[,tot.exp.cols,with=F],
                                 exp.cols=tot.exp.cols,
                                 curve=rr.curves,
                                 curve.n=age.cause.number)]

  dt[, (rr.cols) := lapply(1:fx.draws,
                            function(draw) {
                              (1 - get(hap.prop.cols[draw])) * get(amb.rr.cols[draw]) + get(hap.prop.cols[draw]) * get(tot.rr.cols[draw])
                              })]

  dt[, (paf.cols) := lapply(.SD, function(x) (x-1)/x), .SDcols=rr.cols]

  dt[, (amb.paf.cols) := lapply(1:fx.draws,
                                  function(draw) {
                                    proportion <- get(amb.exp.cols[draw]) / (get(amb.exp.cols[draw]) + get(hap.prop.cols[draw]) * get(hap.exp.cols[draw]))
                                    get(paf.cols[draw]) * proportion %>% return
                                  })]

  dt[, (hap.paf.cols) := lapply(1:fx.draws,
                                       function(draw) {
                                         proportion <- (get(hap.prop.cols[draw]) * get(hap.exp.cols[draw]))  / (get(amb.exp.cols[draw]) + get(hap.prop.cols[draw]) * get(hap.exp.cols[draw]))
                                         get(paf.cols[draw]) * proportion %>% return
                                       })]
  
  #Uses equation given by NAME. Excess risk for the combined exposure minus the excess risk due to ambient plus 1
  #This will be used in RR_max calculation for SEVs
  dt[, (hap.rr.cols) := lapply(1:fx.draws,
                               function(draw){
                                 (get(tot.rr.cols[draw]) - 1) - (get(amb.rr.cols[draw]) -1) +1
                               })]

  #check vals
  subtract <- lapply(all.cols,
                     drawTracker,
                     random.draws=sample(fx.draws, 3)) %>% unlist

  check <- head(dt[, -(subtract), with=F])
  
  
  
  
  #browser()

  # reshape long to store each of the PAFs (all air, ambient, and hap)
  #then, rename to standardize according to GBD upload standards
  reshapeAir <- function(type,
                         cols.list,
                         long.dt,
                         ...) {

    cols <- cols.list[[type]]
    out <- dt[, c(cols, 'grouping'), with=F]
    out[, risk := type]
    setnames(out, cols, out.cols)
    return(out)

  }

  output <- lapply(c('air', 'air_pm', 'air_hap'),
                   reshapeAir,
                   cols.list=list('air'=paf.cols, 'air_pm'=amb.paf.cols, 'air_hap'=hap.paf.cols),
                   long.dt=dt) %>% rbindlist
  
  output[,measure:="paf"]
  
  rr.max <- lapply(c("air","air_pm","air_hap"),
                   reshapeAir,
                   cols.list=list("air"=rr.cols, "air_pm"=amb.rr.cols, "air_hap"=hap.rr.cols),
                   long.dt=dt) %>% rbindlist
  
  rr.max[,measure:="rr"]
  
  output <- rbind(output,rr.max)

  # Set up variables
  output[, 'cause' := cause.code]
  output[, 'age' := age.code]

  # create variable to store type (yll/yld)
  output[, type := metric.type]

  # generate mean and CI for summary figures
  output[, index := seq_len(.N)] # add index
  output[, draw_lower := quantile(.SD, c(.025)), .SDcols=out.cols, by="index"]
  output[, draw_mean := rowMeans(.SD), .SDcols=out.cols, by="index"]
  output[, draw_upper := quantile(.SD, c(.975)), .SDcols=out.cols, by="index"]
  
  return(output)

}
#********************************************************************************************************************************