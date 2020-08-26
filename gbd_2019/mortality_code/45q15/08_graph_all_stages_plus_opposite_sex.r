###############################################################################
## Description: Graph all stages of adult mortality prediction, data, and other
##              pertinant information
###############################################################################

rm(list=ls())
library(foreign); library(lme4); library(RColorBrewer); library(lattice); library(argparse); library(data.table)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

root <- "FILEPATH"
shocks <- F
comparison <- T
start_year  <- 1950
param_selection <- F

# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id 45q15 model run')
parser$add_argument('--data_45q15_version', type="character", required=TRUE,
                    help='Version id of 45q15 data')
parser$add_argument('--ddm_estimate_version', type="character", required=TRUE,
                    help="DDM estimate version id")

args <- parser$parse_args()
version_id <- args$version_id
ddm_version_id <- args$ddm_estimate_version
data_run_id <- args$data_45q15_version

working_dir = "FILEPATH"
dir.create(paste0(working_dir, "/graphs"), showWarnings = FALSE)
dir.create(paste0(working_dir, "/graphs/location_specific"), showWarnings = FALSE)

current_gbd_year <- 2019
prev_gbd_year <- 2017
gbd_type <- "gbd2017"
comp_gbd_type <- "current" # Options: gbd2013, current, both
plot.unrake <- T
send_slack <- T

# get gbd 2019 data
raw <- get_mort_outputs(model_name = "45q15",
                        model_type = "data",
                        run_id = data_run_id,
                        outlier_run_id = 'active',
                        location_set_id = 82)
raw[, upload_45q15_data_id := NULL]
unadjusted_raw <- raw[adjustment == 0]
unadjusted_raw[, adjustment := NULL]
raw <- raw[adjustment == 1]
raw[, adjustment := NULL]

# merge on obs45q15
setnames(unadjusted_raw, "mean", "obs45q15")
by_vars = names(raw)[names(raw) != "mean"]
raw <- merge(raw, unadjusted_raw, all = T, by = by_vars)

raw <- raw[!is.na(raw$ihme_loc_id),]

if(nrow(raw[year_id < 1950]) > 0) {
  raw = raw[year_id >= 1950,]
}

# Determine VR complete
vr_complete_source_type_ids <- c(1, 2, 3, 20, 21, 22, 23, 18, 19, 24,
                                 25, 26, 27, 28, 29, 30, 31, 32, 33)
raw[source_type_id %in% vr_complete_source_type_ids & method_id == 1, vr_complete := T]
raw[is.na(vr_complete), vr_complete := F]


raw[sex_id == 2, sex := 'female']
raw[sex_id == 1, sex := 'male']
raw[sex_id == 3, sex := 'both']

# rename and remove variables unnecessary for modeling
setnames(raw, c("mean", "outlier", "source_name"), c("adj45q15", "exclude", "source_type"))

columns_to_remove <- c("run_id", "method_name", "viz_year", "source_type_id",
                       "sex_id", "underlying_nid", "nid", "age_group_id")
raw[, (columns_to_remove) := NULL]

current_file <- list(est=paste0(working_dir, "/draws/estimated_45q15_noshocks_wcovariate.csv"),
                     pred=paste0(working_dir, "/data/input_data.csv"),
                     data=raw)

# UN WPP 2019
unpop <- fread("FILEPATH")

# Get data for previous GBD
prev_gbd_45q15_data_version <- get_proc_version("45q15", "data", "best", gbd_year = prev_gbd_year)
prev_gbd_45q15_est_version <- get_proc_version("45q15", "estimate", "best", gbd_year = prev_gbd_year)
prev_gbd_ddm_est_version <- get_proc_version("ddm", "estimate", "best", gbd_year = prev_gbd_year)

raw_prev <- get_mort_outputs(model_name = "45q15",
                             model_type = "data",
                             run_id = prev_gbd_45q15_data_version,
                             outlier_run_id = prev_gbd_45q15_est_version,
                             location_set_id = 82)
raw_prev[, upload_45q15_data_id := NULL]
unadjusted_raw_prev <- raw_prev[adjustment == 0]
unadjusted_raw_prev[, adjustment := NULL]
raw_prev <- raw_prev[adjustment == 1]
raw_prev[, adjustment := NULL]

# merge on obs45q15
setnames(unadjusted_raw_prev, "mean", "obs45q15")
by_vars = names(raw_prev)[names(raw_prev) != "mean"]
raw_prev <- merge(raw_prev, unadjusted_raw_prev, all = T, by = by_vars)

raw_prev <- raw_prev[!is.na(raw_prev$ihme_loc_id),]

if(nrow(raw_prev[year_id < 1950]) > 0) {
  raw_prev <- raw_prev[year_id >= 1950,]
}

# Determine VR complete
vr_complete_source_type_ids <- c(1, 2, 3, 20, 21, 22, 23, 18, 19, 24,
                                 25, 26, 27, 28, 29, 30, 31, 32, 33)
raw_prev[source_type_id %in% vr_complete_source_type_ids & method_id == 1, vr_complete := T]
raw_prev[is.na(vr_complete), vr_complete := F]

raw_prev[sex_id == 2, sex := 'female']
raw_prev[sex_id == 1, sex := 'male']
raw_prev[sex_id == 3, sex := 'both']

# rename and remove variables unnecessary for modeling
setnames(raw_prev, c("mean", "outlier", "source_name"), c("adj45q15", "exclude", "source_type"))

columns_to_remove <- c("run_id", "method_name", "viz_year", "source_type_id",
                       "sex_id", "underlying_nid", "nid", "age_group_id")
raw_prev[, (columns_to_remove) := NULL]

prev_gbd_path <- paste0("FILEPATH/45q15/", prev_gbd_45q15_est_version)
comparison_file <- list(est=paste0(prev_gbd_path, "/draws/estimated_45q15_noshocks_wcovariate.txt"),
                        pred=paste0(prev_gbd_path, "/data/input_data.txt"),
                        data=raw_prev)

compare_version <- "new"

codes <- get_locations()
iso3_map <- codes[,c("local_id_2013","ihme_loc_id","region_name","super_region_name")]
locnames <- codes[,c("ihme_loc_id","location_name","region_name","parent_id","location_id")]

graph_order<- rbind(locnames, locnames)
setorder(graph_order, "ihme_loc_id")
graph_order$sex <- c("male","female")
graph_order[, graphing_order := .I]
ind <- graph_order[substr(ihme_loc_id,1,4)=="IND_"]
ind[parent_id==163, parent_id:=location_id]
setorder(ind, "parent_id", -"sex","location_name")
ind[,ind_graphing_order:=.I]
ind[grepl("Urban",location_name),ind_graphing_order:=ind_graphing_order-1]
ind[grepl("Rural",location_name),ind_graphing_order:=ind_graphing_order+1]
ind[, graphing_order := as.integer(graph_order[ihme_loc_id=="IND"&sex=="female",graphing_order]) + ind_graphing_order]
graph_order <- rbind(graph_order[substr(ihme_loc_id,1,4)!="IND_"], ind[,!"ind_graphing_order"])
setorder(graph_order, "graphing_order")

####################
## Load Everything
####################

## define function for getting all data and all predictions and covariates
prep_data <- function(file1, file2, file3, start_year) {
  ##  file1 is results
  ##  file2 is predictions (for old), or input_data (for new)
  ##  file3 is data
  ##  start_year is the year to start comparisons (usually 1950)
  ##  gbd_type = gbd_2010 or gbd_2013
  ##  version is related to compare_version (new vs. old results format)

  d1 <- fread(file1, stringsAsFactors = F)
  d1 <- as.data.frame(d1)
  names(d1)[names(d1) == "med_stage1"] <- "pred.1.noRE"
  names(d1)[names(d1) == "med_stage2"] <- "pred.2.final"
  d2 <- fread(file2, stringsAsFactors = F)
  d2 <- as.data.frame(d2)
  d2 <- unique(d2[,c("ihme_loc_id","sex","year","LDI_id","mean_yrs_educ","hiv","type")]) # Just want the covs and types, not the unique datapts
  d <- merge(d1,d2)
  d$stderr <- NA

  if (is.character(file3)) {
    d3 <- fread(file3, header=T, stringsAsFactors=F)
    d3 <- as.data.frame(d3)

    ## categorize data (needed for excluded data)
    d3$category[d3$adjust == "complete"] <- "complete"
    d3$category[d3$adjust == "ddm_adjusted"] <- "ddm_adjust"
    d3$category[d3$adjust == "gb_adjusted"] <- "gb_adjust"
    d3$category[d3$adjust == "unadjusted"] <- "no_adjust"
    d3$category[d3$source_type == "SIBLING_HISTORIES"] <- "sibs"
  } else {
    d3 <- copy(file3)
    ## assign data categories
    ## category I: Complete
    d3[vr_complete == T, category := "complete"]
    ## category II: DDM adjusted (include all subnational)
    d3[method_id == 2 & vr_complete == F, category := "ddm_adjust"]
    ## category IV: Unadjusted
    d3[method_id == 1 & vr_complete == F, category := "no_adjust"]
    ## category V: Sibs
    d3[method_id == 5 & vr_complete == F, category := "sibs"]
    ## category VI: DSS
    d3[method_id == 11, category := "dss"]
    d3[, c("method_id") := NULL]
    d3[, deaths_source := source_type]
  }

  ## format the data
  d3$exclude <- as.numeric((d3$exclude)>0)
  d3$year <- floor(d3$year) + 0.5

  d3 <- d3[d3$sex!="both",c("ihme_loc_id","year","sex","adj45q15","deaths_source","source_type","exclude","category")]
  setnames(d3, "adj45q15", "mort")
  d3$data <- 1

  ## merge in non-excluded data
  x <- nrow(d)
  temp1 <- merge(d, d3[d3$exclude==0,], by=c("ihme_loc_id","year","sex"), all=T)

  ## merge in excluded data
  x <- nrow(d) + sum(d3$exclude==1)

  temp2 <- merge(unique(d[,c("ihme_loc_id","year","sex","mort_med","mort_lower","mort_upper","unscaled_mort",
                             "LDI_id","mean_yrs_educ","hiv","type",
                             "pred.1.noRE","pred.2.final")]),
                 d3[d3$exclude==1,], by=c("ihme_loc_id","year","sex"), all.y=T)

  temp2$data <- 1
  d <- merge(temp1, temp2, all=T)

  ## get country names
  d$region_name <- d$location_name <- NULL
  d <- merge(d, locnames, by="ihme_loc_id", all.x=T)
  d <- d[order(d$ihme_loc_id, d$sex, d$year, d$data),]
  d <- d[d$year>start_year,]
}

## get current and (if applicable) comparitor data
global_amplitude <- fread(paste0(working_dir, "/stage_2/prediction_model_results_all_stages.csv"))
global_amplitude <- global_amplitude$mse[1]
d <- prep_data(file1=current_file[[1]], file2=current_file[[2]], file3=current_file[[3]],start_year)
if (comparison) {
  c <- prep_data(file1=comparison_file[[1]], file2=comparison_file[[2]], comparison_file[[3]],start_year)
  c$ihme_loc_id[c$ihme_loc_id == "CHN"] <- "CHN_44533"
}

# merge prev GBD parameters onto current ones for comparison
old_params <- fread(paste0(prev_gbd_path, "/data/calculated_data_density.csv"))
old_params <- old_params[, list(ihme_loc_id, sex, scale, amp2x, zeta, lambda)]
setnames(old_params, c("scale", "amp2x", "zeta", "lambda"), c("scale_old", "amp2x_old", "zeta_old", "lambda_old"))

## current selected parameters
params <- as.data.frame(fread(paste0(working_dir, "/data/calculated_data_density.csv")))
params <- merge(params, old_params, by = c("ihme_loc_id", "sex"), all.x = T)

p <- merge(params[params$best==1,], unique(d[,c("ihme_loc_id", "sex")]))
p <- p[,c("ihme_loc_id", "sex", "data_density", "scale", "amp2x", "zeta", "lambda", "scale_old", "amp2x_old", "zeta_old", "lambda_old")]

d <- merge(d[,names(d)!="type"], p)
d <- d[order(d$ihme_loc_id, d$sex, d$year, d$data),]

####################
## Classify sources & categories
####################
## set categories (Symbols)
symbols <- c(23, 24, 24, 25, 21, 22)
names(symbols) <- c("complete", "ddm_adjust", "gb_adjust", "no_adjust", "sibs", "dss")
for (ii in names(symbols)) {
  d$pch[d$category==ii] <- symbols[ii]
  if (comparison) c$pch[c$category==ii] <- symbols[ii]
}

## set sources (colors)
colors <- c(VR="purple", SRS="green1", DSP="green2", DSS="orange",
            DHS="red", RHS="orange1", PAPFAM="hotpink", PAPCHILD="hotpink1",
            Census="blue", MOH="cadetblue1", MICS="aquamarine",
            HOUSEHOLD="coral", SSPC="coral3", FFPS="darkgoldenrod1",
            AIS="darkred", LSMS="firebrick1", SUSENAS="darkseagreen2",
            SUPAS="navy", DC="magenta2",
            Other="chocolate4")

d$color[d$source_type =="VR"] <- colors["VR"]
d$color[grepl("SRS", d$source_type, ignore.case = T)] <- colors["SRS"]
d$color[grepl("DSP", d$source_type, ignore.case = T)] <- colors["DSP"]
d$color[grepl("DSS", d$source_type, ignore.case = T)] <- colors["DSS"]
d$color[grepl("DHS", d$deaths_source, ignore.case = T) | grepl("dhs",d$deaths_source, ignore.case = T)] <- colors["DHS"]
d$color[grepl("CDC-RHS", d$deaths_source, ignore.case = T) | (tolower(d$deaths_source) == "rhs")] <- colors["RHS"]
d$color[grepl("PAPFAM", d$deaths_source, ignore.case = T)] <- colors["PAPFAM"]
d$color[grepl("PAPCHILD", d$deaths_source, ignore.case = T)] <- colors["PAPCHILD"]
d$color[grepl("CENSUS", d$source_type, ignore.case = T)  | grepl("census", d$deaths_source, ignore.case = T)] <- colors["Census"]

d$color[grepl("MOH Survey", d$source_type, ignore.case = T)] <- colors["MOH"]
d$color[grepl("Household", d$source_type, ignore.case = T)] <- colors["HOUSEHOLD"]
d$color[grepl("MICS", d$source_type, ignore.case = T)] <- colors["MICS"]
d$color[grepl("SSPC", d$source_type, ignore.case = T)] <- colors["SSPC"]
d$color[grepl("FFPS", d$source_type, ignore.case = T)] <- colors["FFPS"]
d$color[grepl("AIS", d$source_type, ignore.case = T)] <- colors["AIS"]
d$color[grepl("LSMS", d$source_type, ignore.case = T)] <- colors["LSMS"]
d$color[grepl("SUSENAS", d$source_type, ignore.case = T)] <- colors["SUSENAS"]
d$color[grepl("SUPAS", d$source_type, ignore.case = T)] <- colors["SUPAS"]

d$color[(tolower(d$source_type) == "dc")] <- colors["DC"]

d$color[is.na(d$color) & d$data==1] <- colors["Other"]

fwrite(d, paste0(working_dir, "/graphs/graph_data_test.csv"), row.names = F)

d$fill <- d$color
d$fill[d$exclude == 1] <- NA

if (comparison) {
  c$fill <- c$color <- "gray"
  c$fill[c$exclude == 1] <- NA
}



####################
## Define graph functions
####################


plot_preds_data_compare <- function(d, c, c2 = NULL, start_year,comp_gbd_type, unpop, opp_sex) {

  ## determine range and set up plot
  ylim <- range(c(unlist(d[,c("mort_lower", "mort_upper", "mort", "pred.1.noRE", "pred.2.final")]),
                  unlist(c[,c("mort_lower", "mort_upper", "mort", "pred.1.noRE", "pred.2.final")])), na.rm=T)
  plot(0,0,xlim=c(start_year,current_gbd_year),ylim=ylim,
       xlab=" ", ylab=" ", tck=1, col.ticks="gray95")

  ## plot current predictions
  polygon(c(d$year, rev(d$year)), c(d$mort_lower, rev(d$mort_upper)), col="gray65", border="gray65")
  lines(d$year, d$pred.1.noRE, col="red", lty=1, lwd=2)
  lines(d$year, d$pred.2.final, col="blue", lty=1, lwd=2)
  if(plot.unrake){
    lines(d$year, d$unscaled_mort, col="yellow", lty=1, lwd=3)
  }
  lines(d$year, d$mort_med, col="black", lty=1, lwd=3)

  # plot opposite sex prediction
  lines(opp_sex$year, opp_sex$mort_med, col = "green", lty = 1, lwd = 2)

  ## plot comparison predictions
  lines(c$year, c$pred.1.noRE, col="red", lty=2, lwd=2)
  lines(c$year, c$pred.2.final, col="blue", lty=2, lwd=2)
  lines(c$year, c$mort_lower, col="grey", lty=2, lwd=1)
  lines(c$year, c$mort_upper, col="grey", lty=2, lwd=1)
  lines(c$year, c$mort_med, col="black", lty=2, lwd=2)

  if (unique(d$ihme_loc_id) %in% unpop$ihme_loc_id){
    temp_unpop <- unpop[ihme_loc_id == unique(d$ihme_loc_id) & sex == unique(d$sex),]
    points(temp_unpop$year_id, temp_unpop$value, pch = 19, col = "hotpink", cex = 1.5)
  }

  ## plot comparison data
  points(c$year, c$mort, pch=c$pch, col=c$color, bg=c$fill, cex=1.5)

  ## plot current data
  points(d$year, d$mort, pch=d$pch, col=d$color, bg=d$fill, cex=1.5)
  ## legend
  if(plot.unrake){
    legend("bottomleft",
           legend=c("1st stage","GBD2017 1st stage", "2nd stage","GBD2017 2nd stage", "GPR Unscaled" ,"GPR", "GBD2017", "Opposite sex GPR",
                    "DDM Complete", "DDM Adjusted", "Unadjusted Rates", "Sibs", "DSS"),
           cex=0.5,
           col=c("red","red", "blue", "blue","yellow", "black", "black", "green", rep("black", 5)),
           lty=c(1,2, 1,2, 1, 2, 2,1, rep(NA, 5)), lwd=c(rep(2,8), rep(NA, 5)),
           pch=c(rep(NA, 8), 23, 24, 25, 21, 22), pt.bg=c(rep(NA,8), rep(1, 5)),
           bg="white", ncol = 2)
  }else{
    legend("bottomleft",
           legend=c("1st stage","GBD2017 1st stage", "2nd stage","GBD2017 2nd stage", "GPR", "GBD2017", "Opposite sex GPR",
                    "DDM Complete", "DDM Adjusted", "Unadjusted Rates", "Sibs", "DSS"),
           cex=0.5,
           col=c("red","red", "blue", "blue", "black", "black", "green", rep("black", 5)),
           lty=c(1,2, 1,2,  1, 2, 1, rep(NA, 5)), lwd=c(rep(2,7), rep(NA, 5)),
           pch=c(rep(NA, 7), 23, 24, 25, 21, 22), pt.bg=c(rep(NA,7), rep(1, 5)),
           bg="white", ncol = 2)
  }

  if(unique(d$ihme_loc_id) %in% unpop$ihme_loc_id){
    legend("top", legend=c("UN WPP 2019",names(colors)[colors %in% d$color]), fill=c("hotpink",colors[colors %in% d$color]), border=c("hotpink",colors[colors %in% d$color]), horiz=T, bg="white")
  }
  else if (sum(d$data[!is.na(d$data)])>0){
    legend("top", legend=names(colors)[colors %in% d$color], fill=colors[colors %in% d$color], border=colors[colors %in% d$color], horiz=T, bg="white")
  }

}


####################
## Make parameter plots
if (comparison == T) {
  pdf(paste0(working_dir,"/graphs/comparison_45q15_all_stages_noshocks_opposite_sex_",start_year,"_", version_id, "TEST_WPP2019.pdf"), width=15, height=10)

  for (rr in sort(unique(graph_order$region_name))) {
    cat(paste(rr, "\n")); flush.console()
    for(row in which(graph_order[,region_name] == rr)){
      cc <- graph_order[row,ihme_loc_id]
      ss <- graph_order[row,sex]
      print(cc)
      ii <- (d$ihme_loc_id==cc & d$sex==ss)
      if (nrow(d[ii,]) != 0) {
        jj <- (c$ihme_loc_id==cc & c$sex==ss)
        opp_sex <- d[d$ihme_loc_id==cc & d$sex!=ss,]
        data_density <- unique(d[ii,]$data_density)
        amp2x <- unique(d[ii,]$amp2x)
        scale <- unique(d[ii,]$scale)
        lambda <- unique(d[ii,]$lambda)
        zeta <- unique(d[ii,]$zeta)

        amp2x_old <- unique(d[ii,]$amp2x_old)
        scale_old <- unique(d[ii,]$scale_old)
        lambda_old <- unique(d[ii,]$lambda_old)
        zeta_old <- unique(d[ii,]$zeta_old)

        par(xaxs="i", oma=c(0,0,2,0), mar=c(3,3,2,2))

        plot_preds_data_compare(d[ii,], c[jj,], c2= NULL, start_year, comp_gbd_type, unpop, opp_sex)
        mtext(paste(gsub("_", " ", rr), "\n",
                    d$location_name[ii][1], " (", cc, ")  ", ss, " 45q15 Estimates \n",
                    "Years covered: ", length(unique(d$year[d$ihme_loc_id == cc & d$sex == ss & !is.na(d$category)& d$exclude != 1])),"\n",
                    "density: ", data_density, ", ",
                    "scale: ", scale, " (", scale_old, "), ",
                    "amplitude: ", global_amplitude, ", ",
                    "zeta: ", zeta, " (", zeta_old, "), ",
                    "lambda: ", lambda, " (", lambda_old, "), ", sep=""))
      }
    }

  } #end of loop
  dev.off()

  # Make location-specific graphs
  for (rr in sort(unique(d$region_name))) {
    cat(paste(rr, "\n")); flush.console()
    for (cc in sort(unique(d$ihme_loc_id[d$region_name==rr]))) {
      pdf(paste0(working_dir,"/graphs/location_specific/45q15_diagnostics_", cc, "_", start_year, "_", version_id, ".pdf"), width=15, height=10)
      for (ss in c("male", "female")) {
        ii <- (d$ihme_loc_id==cc & d$sex==ss)
        if (nrow(d[ii,]) != 0) {
          jj <- (c$ihme_loc_id==cc & c$sex==ss)
          opp_sex <- d[d$ihme_loc_id==cc & d$sex!=ss,]
          data_density <- unique(d[ii,]$data_density)
          amp2x <- unique(d[ii,]$amp2x)
          scale <- unique(d[ii,]$scale)
          lambda <- unique(d[ii,]$lambda)
          zeta <- unique(d[ii,]$zeta)

          amp2x_old <- unique(d[ii,]$amp2x_old)
          scale_old <- unique(d[ii,]$scale_old)
          lambda_old <- unique(d[ii,]$lambda_old)
          zeta_old <- unique(d[ii,]$zeta_old)

          par(xaxs="i", oma=c(0,0,2,0), mar=c(3,3,2,2))

          plot_preds_data_compare(d[ii,], c[jj,], c2 = NULL,start_year, comp_gbd_type, unpop, opp_sex)
          mtext(paste(gsub("_", " ", rr), "\n",
                      d$location_name[ii][1], " (", cc, ")  ", ss, " 45q15 Estimates \n",
                      "Years covered: ", length(unique(d$year[d$ihme_loc_id == cc & d$sex == ss & !is.na(d$category)& d$exclude != 1])),"\n",
                      "density: ", data_density, ", ",
                      "scale: ", scale, " (", scale_old, "), ",
                      "amplitude: ", global_amplitude, ", ",
                      "zeta: ", zeta, " (", zeta_old, "), ",
                      "lambda: ", lambda, " (", lambda_old, "), ", sep=""))
        }
      }
      dev.off()
    }
  }

}
