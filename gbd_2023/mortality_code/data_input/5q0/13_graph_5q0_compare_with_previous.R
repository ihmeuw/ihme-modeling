###############################################################################
## Description: Graph all stages of child mortality prediction, data, and other
##              pertinant information
###############################################################################

rm(list=ls())

Sys.unsetenv("PYTHONPATH")

library(foreign)
library(RColorBrewer)
library(data.table)
library(argparse)
library(devtools)
library(methods)
library(mortdb, lib.loc = "FILEPATH")

# Get arguments
if(!interactive()) {

  parser <- ArgumentParser()
  parser$add_argument('--version_id', type="integer", required=TRUE,
                      help='The version_id for this run of 5q0')
  parser$add_argument('--gbd_year', type="integer", required=TRUE, help="GBD Year")
  parser$add_argument('--end_year', type="integer", required=TRUE, help="End Year")
  parser$add_argument('--code_dir', type ="character", required=TRUE, help="Code being run")
  args <- parser$parse_args()
  list2env(args, .GlobalEnv)


} else {

  version_id <-
  gbd_year <-
  end_year <-

}

yml_dir <- gsub("child-mortality", "", code_dir)
yml <- readLines(paste0("FILEPATH"))

pre_gbd_year <- yml[grepl("gbd_year_previous", yml)]
pre_gbd_year <- as.numeric(gsub("\\D", "", pre_gbd_year))

gbd_round_id <- get_gbd_round(gbd_year)

loc_map <- get_locations(gbd_year = gbd_year, gbd_type = "ap_old")[, .(ihme_loc_id, location_name,
                                                                       region_name, super_region_name)]
loc_map <- loc_map[order(super_region_name, region_name, location_name)]

## the best version from last run
best_old_version_id <- mortdb::get_proc_version('5q0', 'estimate', run_id="best", gbd_year=pre_gbd_year)
comp_version_id <- 100

send_slack <- T

gen_graphing_source <- function(data) {
  data$graphing.source <- NA
  data$graphing.source[grepl("vr|vital registration", tolower(data$source))] <- "VR"
  data$graphing.source[grepl("srs", tolower(data$source))] <- "SRS"
  data$graphing.source[grepl("dsp", tolower(data$source))] <- "DSP"
  data$graphing.source[grepl("census", tolower(data$source)) & !grepl("intra-census survey", data$source)] <- "Census"
  data$graphing.source[grepl("_ipums_", data$source) & !grepl("Survey",data$source)] <- "Census"
  data$graphing.source[data$source == "DHS" | data$source == "dhs" | data$source == "dhs in" | grepl("_dhs", data$source)] <- "Standard_DHS"
  data$graphing.source[grepl("^dhs .*direct", data$source1) & !grepl("sp", data$source1)] <- "Standard_DHS"
  data$graphing.source[tolower(data$source) %in% c("dhs itr", "dhs sp", "dhs statcompiler") | grepl("dhs sp", data$source)] <- "Other_DHS"
  data$graphing.source[grepl("mics|multiple indicator cluster", tolower(data$source))] <- "MICS"
  data$graphing.source[tolower(data$source) %in% c("cdc", "cdc-rhs", "cdc rhs", "rhs-cdc", "reproductive health survey") | grepl("cdc-rhs|cdc rhs", data$source)] <- "RHS"
  data$graphing.source[grepl("world fertility survey|wfs|world fertitlity survey", tolower(data$source))] <- "WFS"
  data$graphing.source[tolower(data$source) == "papfam" | grepl("papfam", data$source)] <- "PAPFAM"
  data$graphing.source[tolower(data$source) == "papchild" | grepl("papchild", data$source)] <- "PAPCHILD"
  data$graphing.source[tolower(data$source) == "lsms" | grepl("lsms", data$source)] <- "LSMS"
  data$graphing.source[tolower(data$source) == "mis" | tolower(data$source) == "mis final report" | grepl("mis", data$source)] <- "MIS"
  data$graphing.source[tolower(data$source) == "ais" | grepl("ais", data$source)] <- "AIS"
  data$graphing.source[is.na(data$graphing.source) & data$data == 1] <- "Other"

  return(data)
}

output_dir <- paste0("FILEPATH")

# Get current file paths
location_file <- paste0("FILEPATH")
upload_estimate_file <- paste0("FILEPATH")
spacetime_output_file <- paste0("FILEPATH")
spacetime_hyperparameters_file <- paste0("FILEPATH")


# Comparison file paths
comparison_dir <- paste0("FILEPATH")
comparison_stage_1_2_file <- paste0("FILEPATH")
comparison_gpr_file <- paste0("FILEPATH")
old_spacetime_hyperparameters_file <- paste0("FILEPATH")
unraked_prevround <- paste0("FILEPATH")

# Comparison file paths
comparison_dir2 <- paste0("FILEPATH")
comparison_stage_1_2_file2 <- paste0("FILEPATH")
comparison_gpr_file2 <- paste0("FILEPATH")
old_spacetime_hyperparameters_file2 <- paste0("FILEPATH")
unraked_prev <- paste0("FILEPATH")

location_data <- fread(location_file, header=T, stringsAsFactors=F)
parent_locs <- location_data[,.(location_id, ihme_loc_id,location_name)]
setnames(parent_locs, c("location_id","location_name","ihme_loc_id"),c("parent_id","parent_name","parent_ihme_loc_id"))
location_data <- merge(location_data,parent_locs , by = "parent_id",all.x = T)
location_data[parent_id == 6, `:=`(parent_name = "China", parent_ihme_loc_id = "CHN")]

# Get comparison of sources
source_comparison <- fread(paste0("FILEPATH"))
source_comparison <- source_comparison[, c('ihme_loc_id', 'source1',
                                           paste0('reference_',gbd_year),
                                           paste0('source_adjustment_',gbd_year),
                                           paste0('reference_',pre_gbd_year),
                                           paste0('source_adjustment_',pre_gbd_year)),with=F]
source_comparison <- unique(source_comparison)
source_comparison <- source_comparison[!is.na(get(paste0("source_adjustment_",gbd_year)))]


setwd(paste0("FILEPATH"))

####################
## Load Everything
####################
shocks <-
start_year <-
renums <-
comp.old <-
chn.prov.only <-
natonly <-
chnonly <-
mexonly <-
plot_outliers <-
comp.other.version <-
plot.unrake <-
location.plot <-

# Get GPR data
d1 <- fread(upload_estimate_file, header=T, stringsAsFactors=F)
d1 <- merge(d1, location_data[, c("location_id", "ihme_loc_id")], by = c("location_id"))
d1 <- d1[d1$estimate_stage_id == 3, c("ihme_loc_id", "viz_year", "mean", "lower", "upper")]


# Get 1st & 2nd stage predictions
d2 <- fread(spacetime_output_file, header=T, stringsAsFactors=F)

# Get previous round 1st & 2nd stage predictions
d2b <- fread(comparison_stage_1_2_file, header=T, stringsAsFactors=F)

# Merge current and previous round 1st & 2nd stage predictions
d2c <- d2b
d2c <- d2c[, c("ihme_loc_id", "year", "pred.1b", "pred.2.final", "mse")]
d2c <- unique(d2c)
setnames(d2c, old=c("pred.1b", "pred.2.final", "mse"), new=c("pred.1b_old", "pred.2.final_old", "mse_old"))
d2 <- merge(d2, d2c, all.x=T, by=c("ihme_loc_id", "year"))

# Get current data points
d3 <- fread(spacetime_output_file, header=T, stringsAsFactors=F)
d3 <- d3[d3$data == 1, ]
d3 <- gen_graphing_source(d3)
setnames(d3, old = c("mort2", "mort"), new = c("q5", "adj.q5"))
d3$outlier <- 0

d3 <- d3[, c("ihme_loc_id", "year", "q5", "graphing.source", "method", "outlier", "adj.q5", "reference")]
names(d3)[2:5] <- c("year", "mort", "source", "type")

# Get unraked data and summarize
unraked_current <- fread(paste0("FILEPATH"))
unraked_current <- unraked_current[, .(mort_unraked_current = mean(mort)), by=c('ihme_loc_id', 'year', 'location_id')]

# Read in comparison GPR
compare.gpr <- fread(comparison_gpr_file, header=T)
compare.gpr <- merge(compare.gpr, location_data[, c("location_id", "ihme_loc_id")], by = c("location_id"))
compare.gpr <- compare.gpr[compare.gpr$estimate_stage_id == 3, c("ihme_loc_id", "viz_year", "mean", "lower", "upper")]
setnames(compare.gpr, "viz_year","year")

if(comp.other.version){
  # Read in comparison GPR2
  compare.gpr2 <- fread(comparison_gpr_file2, header=T)
  compare.gpr2 <- merge(compare.gpr2, location_data[, c("location_id", "ihme_loc_id")], by = c("location_id"))
  compare.gpr2 <- compare.gpr2[compare.gpr2$estimate_stage_id == 3, c("ihme_loc_id", "viz_year", "mean", "lower", "upper")]
  setnames(compare.gpr2, "viz_year","year")
}

# Get UNICEF data
unicef <- get_mort_outputs("5q0 comparison source", "data", gbd_year = gbd_year)
unicef <- unicef[source_name == "UNICEF"]
setnames(unicef, c("viz_year", "mean"), c("year", "q5.med.unicef"))
unicef[, source := "UNICEF"]
unicef <- unicef[, .(ihme_loc_id, year, q5.med.unicef, source)]

# Get WPP 2022 data
wpp <- fread("FILEPATH")
wpp <- wpp[sex_id == 3]
wpp <- wpp[, .(ihme_loc_id = location, year = collection_date_start + 0.5, u5mr = value, source = "WPP 2022")]

# Get comparison unraked results
unraked_prev <- fread(paste0("FILEPATH"))
unraked_prev <- unraked_prev[, .(mort_unraked_prev = mean(mort)), by=c('ihme_loc_id', 'year', 'location_id')]

# Merge unraked results together
unraked <- merge(unraked_current, unraked_prev, by=c('ihme_loc_id', 'year', 'location_id'), all=T)

# Change names of d1 file
setnames(d1, old = c("viz_year", "mean", "lower", "upper"),
         new = c("year", "q5.med","q5.lower","q5.upper"))
d <- merge(d1, d2,by = c("ihme_loc_id","year") ,all = T)
d$reference[is.na(d$reference)] <- 0

# Update GBD region and super region column names
names(d)[names(d) == "region_name"] <- "gbd_region"
names(d)[names(d) == "super_region_name"] <- "gbd_super_region"

# Order data
d <- d[order(d$ihme_loc_id, d$year, d$data),]

# Get spacetime hyperparameters
old_params <- fread(old_spacetime_hyperparameters_file)
old_params$lambda_old <- old_params$lambda
old_params$zeta_old <- old_params$zeta
old_params$amp2x_old <- old_params$amp2x
old_params$scale_old <- old_params$scale
old_params <- old_params[, c('location_id', 'lambda_old', 'zeta_old',
                             'amp2x_old', 'scale_old')]
params <- fread(spacetime_hyperparameters_file)
params <- params[, c('location_id', 'lambda', 'zeta', 'amp2x', 'scale')]


# Merge hyperparameters with data
d <- merge(d, old_params, by="location_id", all.x=TRUE)
d <- merge(d, params, by="location_id")
d <- d[order(d$year),]

# Get outliered data
if(plot_outliers){
  data_version <- get_proc_lineage("5q0","estimate", run_id = version_id,gbd_year=gbd_year)
  data_version <- as.integer(data_version[parent_process_name == "5q0 data",parent_run_id])
  outliers <- get_mort_outputs("5q0","data", run_id = data_version, outlier_run_id = version_id,gbd_year=gbd_year)
  outliers <- outliers[outlier==1]
  outliers[, year := year_id + 0.5]
  outliers$source <- tolower(outliers$source)
  outliers <- gen_graphing_source(outliers)
  outliers[is.na(graphing.source) & grepl("dhs", source), graphing.source :=  "Standard_DHS"]
  outliers[is.na(graphing.source), graphing.source :=  "Other"]
  outliers[, type := method_name]
  outliers[method_name=="SBH-MAC", type:="SBH"]
  outliers[method_name=="Dir-Unadj", type:="HH"]
  outliers <- outliers[, .SD,.SDcols = c("ihme_loc_id", "year", "mean", "graphing.source", "type")]
}

####################
## Classify sources & categories
####################
## categories (Symbols)
symbols <- c(21,24,25,23)
names(symbols) <- c("VR/SRS/DSP", "CBH", "SBH", "HH")
for (ii in names(symbols)) {
  d3$pch[d3$type==ii] <- symbols[ii]
  if(plot_outliers) outliers$pch[outliers$type==ii] <- symbols[ii]
}

## sources (colors)
colors <- c(VR="purple", SRS="green1", DSP="green2", Standard_DHS="orange",
            Other_DHS="red", RHS="deeppink", PAPFAM="hotpink", PAPCHILD="hotpink1",
            Census="blue", Other="chocolate4", MICS="darkgreen", WFS="mediumspringgreen", LSMS="violetred4",
            MIS="khaki4", AIS="steelblue2",MCHS = "hotpink2", "MoH Routine Reporting" = "cyan",
            "Census (IPUMS)" = "hotpink", "Inter-censal Survey (IPUMS)" = "mediumspringgreen",
            ENADID = "orange", VR_old = "red")
for (ii in names(colors)) {
  d3$color[d3$source == ii] <- colors[ii]
  if(plot_outliers) outliers$color[outliers$graphing.source == ii] <- colors[ii]
}


d3$fill <- d3$color
d3$fill[d3$outlier == 1] <- NA
dorig <- d

# order locations for plotting
d <- merge(d, loc_map, by = "ihme_loc_id")
d[, partial_loc_id := substr(ihme_loc_id, 1, 4)]

#####################
## Define graph functions
#####################

###############################################################################
## Plot points, adjusted points, and final GPR estimates
plot_data <- function(d,d3, prerake, compare.gpr = NULL,compare.gpr2 = NULL, level = 3,compare.gpr.unicef=NULL, compare.gpr.wpp=NULL, dorig = NULL,renums=NULL,compare.old = NULL,compare.old.stages = NULL, source_comparison = NULL, outliers = NULL, urban = NULL) {
  # d1:

  ## determine range and set up plot
  if(!is.null(compare.old)){
    ylim <- range(c(d$mort2,d$mort,d3$mort, d3$adj.q5, compare.old$q5.med, unlist(d[,c("q5.lower", "q5.upper", "q5.med","pred.1b","pred.2.final")])), na.rm=T)
  }else{
    ylim <- range(c(d$mort2,d$mort,d3$mort, d3$adj.q5, unlist(d[,c("q5.lower", "q5.upper", "q5.med","pred.1b","pred.2.final")])), na.rm=T)
  }
  plot(0,0,xlim=c(ss+0.5,end_year+0.5),ylim=ylim, xlab=" ", ylab=" ", tck=1, col.ticks="gray95")

  ## plot predictions
  polygon(c(d$year, rev(d$year)), c(d$q5.lower, rev(d$q5.upper)), col="gray65", border="gray65")
  lines(d$year, d$q5.med, col="black", lty=1, lwd=3)

  ## plot stage 1 and 2
  lines(d$year, d$pred.1b, col="green", lty=1, lwd=2)
  lines(d$year, d$pred.1b_old, col="green", lty=2, lwd=2)
  lines(d$year, d$pred.2.final, col="blue", lty=1, lwd=2)
  lines(d$year, d$pred.2.final_old, col="blue", lty=2, lwd=2)

  ## plot unraked estimates
  if(plot.unrake==T & level>3){
    lines(prerake$year, prerake$mort_unraked_current, col = "cyan", lty = 1, lwd = 2)
    lines(prerake$year, prerake$mort_unraked_prev, col="cyan", lty=2, lwd= 1)
  }

  ## plot previous version of predictions
  if(!is.null(compare.old)){
    lines(compare.old$year, compare.old$q5.med, col="purple", lty = 1, lwd = 2)
    lines(compare.old$year, compare.old$q5.lower, col="purple", lty=2, lwd=1)
    lines(compare.old$year, compare.old$q5.upper, col="purple", lty=2, lwd=1)

  }

  ## plot Previous Best estimates
  lines(compare.gpr$year, compare.gpr$mean, col = "red", lty = 1, lwd = 2)
  lines(compare.gpr$year, compare.gpr$lower, col="red", lty=2, lwd=1)
  lines(compare.gpr$year, compare.gpr$upper, col="red", lty=2, lwd=1)

  if(comp.other.version){
    ## plot comparsion estimates i.e. gbd 2017
    lines(compare.gpr2$year, compare.gpr2$mean, col = "coral", lty = 1, lwd = 2)
    lines(compare.gpr2$year, compare.gpr2$lower, col="coral", lty=2, lwd=1)
    lines(compare.gpr2$year, compare.gpr2$upper, col="coral", lty=2, lwd=1)
  }

  # plot unicef estimates
  lines(compare.gpr.unicef$year, compare.gpr.unicef$q5.med.unicef, col = "purple", lty = 1, lwd = 2)

  # plot wpp estimates
  lines(compare.gpr.wpp$year, compare.gpr.wpp$u5mr, col = "gold", lty = 1, lwd = 2)

  # For India Urban/Rural, plot the opposite region GPR
  if (!is.null(urban)) {
    lines(urban$year, urban$q5.med, col="yellow", lty=1, lwd=2)
  }

  points(d3$year,d3$mort,pch=d3$pch,col=d3$color2,bg=d3$fill2)
  points(d3$year,d3$adj.q5,pch=d3$pch,col=d3$color,bg=d3$fill)

  # plot outliered data
  points(outliers$year,outliers$mean,pch=outliers$pch,col=outliers$color2)

  ## plot black circles around reference data
  points(d3$year[d3$reference == 1], d3$mort[d3$reference == 1], col = "black", pch = 1, cex = 2, lwd = 1.5)

  leg.loc <- "topright"

  if(!is.null(compare.old)){
    legend(leg.loc,
           legend=c("New estimates", "GBD 2015", "Prerake", "Stage 1: Regression", "Stage 2: Space-time",
                    "VR/SRS/DSP", "CBH", "SBH", "HH", "Transparent points are","original data points","Hollow = outlier or shock"),
           col=c("black","red","purple","green","blue",
                 "black", "black", "black", "black", NA, NA, NA),
           lty=c(2,1,1,1,1,rep(NA,7)), lwd=c(2,2,2,2,2,rep(NA,7)),
           pch=c(NA,NA,NA,NA,NA,symbols,NA,NA,NA),pt.bg=c(NA,NA,NA,NA,NA,rep("black",4),NA,NA,NA),
           bg="white", ncol=2, cex = 0.5)
  }else{
    legendtext= c(paste0("GPR GBD ",gbd_year), paste0("GPR GBD ",pre_gbd_year))
    legendcol = c("black", "red")
    legendlty = c(1,1)

    if(comp.other.version){
      legendtext = c(legendtext, "")
      legendcol = c(legendcol, "coral")
      legendlty = c(legendlty,1)
    }

    if(plot.unrake){
      legendtext = c(legendtext, paste0("Pre-rake ", gbd_year),paste0("Pre-rake ", pre_gbd_year))
      legendcol = c(legendcol, "cyan","cyan")
      legendlty = c(legendlty, 1,4)
    }

    legendtext = c(
      legendtext,"UNICEF", "WPP",
      "Stage 1 Current", paste0("Stage 1 GBD ",pre_gbd_year),
      "Stage 2 Current: Space Time", paste0("Stage 2 GBD ",pre_gbd_year),
      "VR/SRS/DSP", "CBH", "SBH", "HH", "Transparent points are", "original data points",
      ""
    )
    legendcol = c(legendcol,"purple", "gold", "green","green", "blue", "blue",
                  "black", "black", "black", "black", NA, NA, NA)
    legendlty = c(legendlty, 1,1,1,2,1, 2, rep(NA,6))
    if (!is.null(urban)) {
      legendcol <- c(legendcol, "yellow")
      legendtext <- c(legendtext, "Opposite Urban/Rural")
      legendlty <- c(legendlty, 2)
    }
    legend(leg.loc,
           legend=legendtext,
           col=legendcol,
           lty=legendlty,
           lwd=c(rep(1,length(legendtext)-7),rep(NA,7)),
           pch=c(rep(NA,length(legendtext)-7), symbols,NA,NA,NA),
           pt.bg=c(rep(NA,length(legendtext)-7),rep("black",4),NA,NA,NA),
           bg="white", ncol=2, cex = 0.5, inset=c(0,.1))

  }

  if (sum(d$data)>0) legend("top",
                            legend=gsub("_", " ", names(colors)[names(colors) %in% d3$source]),
                            fill=colors[names(colors) %in% d3$source],
                            border=colors[names(colors) %in% d3$source],
                            horiz=T, bg="white",cex=0.6)


  #show survey RE
  if(renums){
    source_comparison <- source_comparison[order(source_comparison$source1),]
    rownames(source_comparison) <- NULL
    inds <- (!duplicated(source_comparison$source1) & !is.na(source_comparison$source1) & source_comparison$source1 != "")
    inds <- inds[1:length(inds)]
    str1 <- "Mixed Effects Adjustment Factor: \n"
    for(i in which(inds)){
      sourcen <- source_comparison$source1[i]
      st <- 30
      ind <- regexec(" ",substring(sourcen,31))[[1]][1]
      while(ind != -1){
        bk <- st+ind
        sourcen <- paste(substring(sourcen,1,bk-1),"\n\t\t\t",substring(sourcen,bk),sep = "")
        st <- bk+30
        ind <- regexec(" ",substring(sourcen, st))[[1]][1]
      }
      str1 <- paste(str1, toString(sourcen),":",toString(round(1000*source_comparison$source_adjustment_2020[i])/1000)," (", toString(round(1000*source_comparison$source_adjustment_2019[i])/1000),")\n")
    }
    str1 <- paste(str1, "\n", "Country Random Effects: ", toString(round(1000*dorig$ctr_re[1])/1000), sep = "")
    mtext(text = str1, side = 4, cex = 0.5,  las = 1)
  }
}



for (ss in start_year) {
  cat(paste("Starting year",ss,"\n")); flush.console()

  # Read in comparison Unicef data
  compare.gpr.unicef <- unicef[year >= ss,]
  compare.gpr.unicef <- compare.gpr.unicef[order(ihme_loc_id, year),]

  # Read in comparison WPP data
  compare.gpr.wpp <- wpp[year >= ss]
  compare.gpr.wpp <- compare.gpr.wpp[order(ihme_loc_id, year)]

  if(comp.old){
    compare.old <- fread("FILEPATH", header=T)
    names(compare.old)[3:5] <- c("q5.med","q5.lower","q5.upper")
    compare.old <- compare.old[compare.old$year>=ss,]
  }


  d3 <- d3[d3$year >= ss,]
  d <- d[d$year >= ss,]
  compare.gpr <- compare.gpr[compare.gpr$year >= ss,]
  if(comp.other.version) compare.gpr2 <- compare.gpr2[compare.gpr2$year >= ss,]
  prerake <- unraked[year >= ss]

  # For plotting points adjusted by RE
  makeTransparent<-function(someColor, alpha=100){
    newColor<-col2rgb(someColor)
    apply(newColor, 2, function(curcoldata){rgb(red=curcoldata[1], green=curcoldata[2],
                                                blue=curcoldata[3],alpha=alpha, maxColorValue=255)})
  }

  d3$color2 <- makeTransparent(d3$color)
  d3$fill2 <- makeTransparent(d3$color)
  if(plot_outliers) outliers$color2 <- makeTransparent(outliers$color)

  ####################
  ## Construct plots
  ####################

  if(chn.prov.only){
    d <- d[grepl("X(.{2})",d$ihme_loc_id),]
  }

  output_graph_dir <- paste0("FILEPATH")

  # Make location-specific graphs
  dir.create(file.path(paste0("FILEPATH")),showWarnings = FALSE)

  # Order locations for plotting
  d <- d[order(super_region_name, region_name, partial_loc_id, location_name)]

  for (rr in unique(d$gbd_region)) {
    cat(paste(rr, "\n")); flush.console()
    for (cc in unique(d$ihme_loc_id[d$gbd_region==rr])) {
      # Generate PDFs

      output_graph_file <- paste0("FILEPATH")
      pdf(output_graph_file, width=15, height=10)

      cat(paste(cc, "\n")); flush.console()
      if(natonly == 1 & (grepl("X..$",cc) | (cc %in% c("HKG","MAC","BMU","PRI")))) next
      if(chnonly & !((grepl("X..$",cc) | cc == "CHN_44533") & rr == "East Asia")) next
      if(mexonly & !((grepl("X..$",cc) | cc == "MEX") & rr == "Central Latin America")) next
      ii <- (d$ihme_loc_id==cc)
      jj <- (d3$ihme_loc_id==cc)
      kk <- (compare.gpr.unicef$ihme_loc_id == cc)
      ll <- (compare.gpr$ihme_loc_id == cc)
      mm <- (dorig$ihme_loc_id == cc)
      nn <- (source_comparison$ihme_loc_id == cc)
      level <- location_data[ihme_loc_id == cc, level]
      oo <- prerake[ihme_loc_id==cc]
      pp <- (compare.gpr.wpp$ihme_loc_id == cc)

      # Check if location is Rural/Urban India
      urban <- NULL
      if (grepl("Rural|Urban", location_data[ihme_loc_id==cc, location_name])) {
        state_id <- location_data[ihme_loc_id==cc, parent_id]
        opposite_location <- location_data[parent_id == state_id & ihme_loc_id != cc, ihme_loc_id]
        urban <- d[d$ihme_loc_id==opposite_location,]
      }

      if(comp.old) nn <- (compare.old$ihme_loc_id == cc)
      if(renums) par(xaxs="i", oma=c(0,0,2,7), mar=c(3,3,2,2)) else par(xaxs="i", oma=c(0,0,2,0), mar=c(3,3,2,2))
      if(comp.old){
        plot_data(d = d[ii,],d3 = d3[jj,], compare.gpr = compare.gpr[ll,],level = level ,prerake = oo, compare.gpr.unicef = compare.gpr.unicef[kk,], dorig = dorig[mm,],renums = renums,compare.old = compare.old[nn,], source_comparison = source_comparison[nn,], compare.gpr.wpp = compare.gpr.wpp[pp,], outliers = outliers[ihme_loc_id==cc,]) #, compare.old.stages = compare.old.stages[oo,])
      }else{
        if(comp.other.version == T){
          plot_data(d[ii,],d3[jj,],compare.gpr[ll,], compare.gpr2=compare.gpr2[compare.gpr2$ihme_loc_id==cc,],level = level,prerake = oo,
                    compare.gpr.unicef = compare.gpr.unicef[kk,], dorig = dorig[mm,],renums = renums,
                    source_comparison = source_comparison[nn,], compare.gpr.wpp = compare.gpr.wpp[pp,],
                    outliers = outliers[ihme_loc_id==cc,], urban = urban)
        }else{
          plot_data(d[ii,],d3[jj,],compare.gpr[ll,], compare.gpr2=compare.gpr2[compare.gpr2$ihme_loc_id==cc,],level = level,prerake = oo,
                    compare.gpr.unicef = compare.gpr.unicef[kk,], dorig = dorig[mm,],renums = renums,
                    source_comparison = source_comparison[nn,], compare.gpr.wpp = compare.gpr.wpp[pp,],
                    outliers = outliers[ihme_loc_id==cc,], urban = urban)
        }
      }

      mtext(paste(gsub("_", " ", rr), "\n", location_data[ihme_loc_id==cc, location_name], " (", cc, ") 5q0 Estimates \n",
                  "parent_id: ",location_data[ihme_loc_id==cc, parent_ihme_loc_id]," ", location_data[ihme_loc_id==cc, parent_name],"\n",
                  "density: ", toString(round(1000*d$data_density[ii][1])/1000),
                  ", lambda: ", d$lambda[ii][1], " (", d$lambda_old[ii][1], ")",
                  ", zeta: ", d$zeta[ii][1], " (", d$zeta_old[ii][1], ")",
                  ", amp: ", round(d$mse[ii][1], digits = 4), " (", round(d$mse_old[ii][1], digits = 4), ")",
                  ", amp scalar: ", d$amp2x[ii][1], " (", d$amp2x_old[ii][1], ")",
                  ", scale: ", d$scale[ii][1], " (", d$scale_old[ii][1], ")",
                  sep = ""))
      dev.off()
    }
  }
}




#send slack messsage
if(send_slack){
  message <- paste0("")
  send_slack_message(message,channel = "", icon = "", botname = "")
}

