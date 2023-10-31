################################################################################
## Description: Graph two different sets of DDM results for comparison purposes
################################################################################

library(foreign); library(plyr); library(reshape); library(haven)
library(data.table)

end_year <- x ## last year in the analysis
include_comp_point_est <- F ## whether you want to include point estimates for comparator
subversion <- NA # subversion optional
comp_version_id <- NA # set to compare version, optional
comp_subversion <- NA # set compare subversion, optional
gbd2019_version <- get_proc_version("ddm", "estimate", run_id = "best", gbd_year = 2019)
main_dir <- paste0("FILEPATH")
comp_dir <- paste0("FILEPATH")
comp_dir_2019 <- paste0("FILEPATH")

# make output directories
output_graph_dir <- paste0("FILEPATH")
dir.create(file.path(output_graph_dir), showWarnings = FALSE)
dir.create(file.path("FILEPATH"), showWarnings = FALSE)

# utility functions ===============================================================

# make transparent color function
makeTransparent<-function(someColor, alpha=100) {
  newColor<-col2rgb(someColor)
  apply(newColor, 2,
        function(curcoldata) rgb(red=curcoldata[1],
                                 green=curcoldata[2],
                                 blue=curcoldata[3],
                                 alpha=alpha,maxColorValue=255)
  )
}

# Remove graphs for USA, NOR, JPN, GBR subnationals
remove_some <- function(dt){
  toMatch <- c("USA_", "NOR_", "JPN_", "GBR_")
  pattern <- paste(toMatch,collapse="|")
  return(dt[!grepl(pattern, ihme_loc_id)])
}

# format columns
format_cols <- function(dt){
  setnames(dt, "source", "source_type")
  dt[, iso3_sex_source := paste0(ihme_loc_id, "&&", sex, "&&", source_type)]
  dt <- dt[,list(iso3_sex_source, year, final_comp)]
  return(dt)
}

# read in data =====================================================================

## completeness by method and final completeness
all <- data.table(haven::read_dta(paste0("FILEPATH"), encoding='latin1'))
final_comp <- data.table(haven::read_dta(paste0("FILEPATH")))

## hyperparameters
hyper_param <- fread(paste0("FILEPATH"))

## comparator completeness by method and final completeness
if(!is.na(comp_version_id)){
  comp <- data.table(haven::read_dta("FILEPATH", encoding = "latin1"))
  final_comp_prev <- data.table(haven::read_dta("FILEPATH"))
}

## GBD 2019 final completeness
final_comp_2019 <- data.table("FILEPATH")

## GBD2019 Data
data_2019 <- setDT(haven::read_dta("FILEPATH"), encoding = "latin1")

## find years that we have census populations for DDM
pop <- data.table(haven::read_dta(paste0("FILEPATH")))

# deaths
deaths <- data.table(haven::read_dta(paste0("FILEPATH")))

# prep ==============================================================================

# convert ccmp methods to lowercase for plotting
all[, comp_type:=tolower(comp_type)]
if(!is.na(comp_version_id)) comp[, comp_type:=tolower(comp_type)]

# Remove graphs for USA, NOR, JPN, GBR subnationals
all <- remove_some(all)
final_comp <- remove_some(final_comp)
if(!is.na(comp_version_id)){
  comp <- remove_some(comp)
  final_comp_prev <- remove_some(final_comp_prev)
}
final_comp_2019 <- remove_some(final_comp_2019)
pop <- remove_some(pop)
deaths <- remove_some(deaths)

# some formatting
final_comp <- format_cols(final_comp)
if(!is.na(comp_version_id)) final_comp_prev <- format_cols(final_comp_prev)
final_comp_2019 <- format_cols(final_comp_2019)

# combine method specific and final completeness by version
all <- merge(all, final_comp, by = c('iso3_sex_source', 'year'), all.x = T)
all[is.na(final_comp), final_comp := 0]
if(!is.na(comp_version_id)){
  comp <- merge(comp, final_comp_prev, by = c('iso3_sex_source', 'year'), all.x = T)
  comp[is.na(final_comp), final_comp := 0]
}

# format & merge on hyperparameters
specific_lambda_locs <- unique(hyper_param[iso3_sex_source != "default"]$graph_id)
hyper_param <- hyper_param[, list(graph_id, lambda)]
all[iso3_sex_source %in% specific_lambda_locs, graph_id := iso3_sex_source]
all[!iso3_sex_source %in% specific_lambda_locs, graph_id := "default"]
all <- merge(all, hyper_param, by = 'graph_id', all.x = T)
all$graph_id <- NULL

## Attach alphas for graphing
all$alpha <- 5
all$alpha[grepl("PAN", all$iso3_sex_source)] <- 1
all$alpha[grepl("MNG", all$iso3_sex_source)] <- 1
all$alpha[grepl("PHL", all$iso3_sex_source)] <- 0.8
all$alpha[grepl("TUR", all$iso3_sex_source)] <- 0.7
all$alpha[grepl("UZB", all$iso3_sex_source)] <- 0.7

all$zeta <- 0.95

# format pop
pop$source_type[grepl("SRS", pop$source_type)] <- "SRS"
pop$source_type[grepl("DSP", pop$source_type)] <- "DSP"
pop$source_type[grepl("SSPC|DC", pop$source_type)] <- "SSPC-DC"
pop$iso3_sex_source <- paste(pop$ihme_loc_id, pop$sex, pop$source_type, sep="&&")
ss <- grepl("SAU",pop$ihme_loc_id) & grepl("VR", pop$iso3_sex_source)
mar <- grepl("MAR",pop$ihme_loc_id) & grepl("VR", pop$iso3_sex_source)
pop <- pop[pop$sex == "both" | (pop$sex != "both" & (ss | mar)),]
years <- do.call("rbind", strsplit(pop$pop_years, " "))
for (ii in 1:2) years[,ii] <- substr(years[,ii],0,4)
pop <- data.frame(unique(rbind(cbind(pop[,"iso3_sex_source"], year = years[,1]),cbind(pop[,"iso3_sex_source"], year = years[,2]))))
names(pop) <- c("iso3_sex_source", "year")
pop$year <- as.numeric(as.character(pop$year))

# format deaths
deaths$source_type[grepl("SRS", deaths$source_type)] <- "SRS"
deaths$source_type[grepl("DSP", deaths$source_type)] <- "DSP"
deaths$source_type[grepl("SSPC|DC", deaths$source_type)] <- "SSPC-DC"
deaths$iso3_sex_source <- paste(deaths$ihme_loc_id, deaths$sex, deaths$source_type, sep="&&")

# merge on colors and symbols ==========================================================

colors_symbols <- data.frame(comp_type=c("ggb", "seg", "ggbseg", "ccmp_aplus_no_migration","ccmp_aplus_migration"),
                             pch=c(24, 25, 23, 22, 21),
                             col=c("green", "blue", "purple", "turquoise3", "lightcoral"), stringsAsFactors=F)

all <- merge(all, colors_symbols, by="comp_type", all.x=T)
all <- all[order(all$region_name, all$iso3_sex_source, all$year, all$comp_type),]

data_2019 <- merge(data_2019, colors_symbols, by='comp_type', all.x=T)
data_2019 <- data_2019[order(region_name, iso3_sex_source, year, comp_type)]
data_2019[, col := makeTransparent(col, alpha=40)]

if(!is.na(comp_version_id)){
  comp <- merge(comp, colors_symbols, by="comp_type", all.x=T)
  comp$col <- makeTransparent(comp$col, alpha=65)
  comp <- comp[order(region_name, iso3_sex_source, year, comp_type)]
}

final_comp_2019 <- final_comp_2019[order(iso3_sex_source, year)]

# make plots ===========================================================================

#### Plotting function ####
plot_ddm <- function(all, comp, all_2019, final_comp_2019, cc) {
  temp <- all[all$iso3_sex_source == cc,]
  if(!is.na(comp_version_id)) temp2 <- comp[iso3_sex_source == cc,]
  temp3 <- final_comp_2019[final_comp_2019$iso3_sex_source == cc,]
  temp4 <- data_2019[iso3_sex_source== cc,]

  if (fixed) ylim <- c(0,2) else ylim <- range(na.omit(unlist(temp[,c("comp", "pred1", "pred2", "u5_comp", "u5_comp_pred", "pred", "lower", "upper", "trunc_lower", "trunc_upper")])))
  plot(0, 0, xlim=c(1950,end_year), ylim=ylim, type="n", xlab="Year", ylab="comp")

  # Uncertainty intervals
  if (grepl("VR|SRS|DSP", cc)){
    polygon(c(temp$year, rev(temp$year)), c(temp$trunc_lower, rev(temp$trunc_upper)), col= makeTransparent("gray20", alpha = 50), border=makeTransparent("gray20", alpha = 50))
    polygon(c(rev(temp$year), temp$year), c(rev(temp$trunc_lower), temp$trunc_upper), col= makeTransparent("gray20", alpha = 50), border=makeTransparent("gray20", alpha = 50))
    legend("topleft", c("child", "ggb", "seg", "ggbseg", "ccmp_aplus_no_migration","ccmp_aplus_migration",
                        "child", "child_prev", "1st", "1st_prev", "2nd", "Truncated Completeness", "CI truncated (VR/SRS/DSP/MCCD/CR)", "Final Completeness","GBD 2019", "GBD2019 UI"),
           col=c("red","green","blue","purple","turquoise3","magenta","red","red", "orange", "orange","black","black","grey35","yellow","green4", "black"),
           pch=c(19,24,25,23,22,21,NA,NA,NA,NA,NA,NA,NA,NA,NA),
           pt.bg=c("red","green","blue","purple","turquoise3","magenta",NA,NA,NA,NA,NA,NA,NA,NA,NA),
           fill=c(NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,"gray35",NA,NA),
           border=c(NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,"grey35",NA,NA),
           lwd=c(NA,NA,NA,NA,NA,NA,1,1,1,1,3,3,NA,3,3,1),
           lty=c(NA,NA,NA,NA,NA,NA,1,2,1,2,1,2,NA,1,1,2),
           ncol=2, bg="white")
  } else{
    polygon(c(temp$year, rev(temp$year)), c(temp$lower, rev(temp$upper)), col=makeTransparent("gray20", alpha = 20), border="gray20")
    polygon(c(rev(temp$year), temp$year), c(rev(temp$upper), temp$lower), col=makeTransparent("gray20", alpha = 20), border="gray20")

    legend("topleft", c("child", "ggb", "seg", "ggbseg", "ccmp_aplus_no_migration", "ccmp_aplus_migration",
                        "child", "child_prev", "1st", "1st_prev", "2nd", "Truncated Completeness", "CI untruncated (VR/SRS/DSP/MCCD/CR)", "Final Completeness","GBD 2019", "GBD2019 UI"),
           col=c("red","green","blue","purple","turquoise3","magenta","red", "red", "orange", "orange", "black","black", makeTransparent("gray20", alpha = 20), "yellow", "green4", "black"),
           pch=c(19,24,25,23,22,21,NA,NA,NA,NA,NA,NA,NA,NA,NA),
           pt.bg=c("red","green","blue","purple","turquoise3","magenta",NA,NA,NA,NA,NA,NA,NA,NA,NA),
           fill=c(NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,makeTransparent("gray20", alpha = 20),NA,NA),
           border=c(NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA, "grey20",NA,NA),
           lwd=c(NA,NA,NA,NA,NA,NA,1,1,1,1,3,3,NA,3,3,1),
           lty=c(NA,NA,NA,NA,NA,NA,1,2,1,2,1,2,NA,1,1,2),
           ncol=2, bg="white")
  }

  # child completeness
  lines(temp$year, temp$u5_comp_pred, col="red", lwd=2, lty=1)
  points(temp$year, temp$u5_comp, pch=19, col="red", cex=1.5)

  # comparison child completeness
  if(!is.na(comp_version_id)){
    lines(temp2$year, temp2$u5_comp_pred, col=makeTransparent("red",alpha=65), lwd=2, lty=1)
    points(temp2$year, temp2$u5_comp, pch=19, col=makeTransparent("red",alpha=65), cex=1.25)
  }

  # lines for each stage
  lines(temp$year, temp$pred1, col="orange", lwd=2, lty=1)
  lines(temp$year, temp$pred, col="black", lwd=4, lty=1)
  lines(temp$year, temp$trunc_pred, col="black", lwd=4, lty=2)
  lines(temp$year, temp$pred, col="black", lwd=4, lty=2)
  lines(temp$year, temp$final_comp, col="yellow", lwd=4, lty=2)

  # comparison lines for each stage
  if(!is.na(comp_version_id)){
    lines(temp2$year, temp2$pred1, col=makeTransparent("orange",alpha=65), lwd=2, lty=1)
    lines(temp2$year, temp2$pred, col=makeTransparent("black",alpha=95), lwd=3, lty=1)
    lines(temp2$year, temp2$trunc_pred, col=makeTransparent("black",alpha=95), lwd=3, lty=2)
    lines(temp2$year, temp2$final_comp, col=makeTransparent("yellow",alpha=95), lwd=4, lty=2)
  }

  # GBD 2019 lines
  lines(temp3$year, temp3$final_comp, col="green4", lwd=3, lty=1) # final
  lines(temp4$year, temp4$pred1, col='orange', lwd=1, lty=2) # 1st stage
  lines(temp4$year, temp4$u5_comp_pred, col='red', lwd=1, lty=2) # U5

  # GBD2019 UI
  if (grepl("VR|SRS|DSP", cc)) {
    lines(temp4$year, temp4$trunc_lower, col = 'black', lwd=1, lty=2)
    lines(temp4$year, temp4$trunc_upper, col = 'black', lwd=1, lty=2)
  } else {
    lines(temp4$year, temp4$lower, col = 'black', lwd=1, lty=2)
    lines(temp4$year, temp4$upper, col = 'black', lwd=1, lty=2)
  }

  # line at 0.95
  abline(a=0.95, b=0, col="blueviolet", lwd=2)
  # line at 1
  abline(a=1, b=0, col='blueviolet', lwd=1)

  # DDM points

  # comparison DDM points
  if(include_comp_point_est==T){
    points(temp2$year, temp2$comp, pch=temp2$pch, col=temp2$col, bg=ifelse(temp2$exclude==1, NA, temp2$col), cex=1.75)
  }

  # old DDM points
  points(temp4$year, temp4$comp, pch=temp4$pch, col=temp4$col, bg = ifelse(temp4$exclude==1, NA, temp4$col), cex=1.75)
  # current DDM points
  points(temp$year, temp$comp, pch=temp$pch, col=temp$col, bg=ifelse(temp$exclude==1, NA, temp$col), cex=2)

  # Census/deaths tick marks
  deaths_years <- deaths$deaths_years[deaths$iso3_sex_source == cc]
  tmp_deaths_years <- strsplit(deaths_years, " ")
  deaths_years <- as.numeric(unlist(tmp_deaths_years, recursive = TRUE, use.names = TRUE))
  pop_years <- pop$year[pop$iso3_sex_source==cc]
  if (grepl("CHN", cc)) {
    pop_years <- pop_years %in% c(1964, 1982, 1990, 2000, 2010)
  }
  for (ii in deaths_years){
    if (ii %in% pop_years){
      x_coord <- ii + .25
    } else{
      x_coord <- ii
    }
    segments(x0=x_coord, y0=(ylim[1]-0.04*(ylim[2]-ylim[1])), x1=x_coord, y1=ylim[1], lwd=1.5, col = rgb(1,0,0, 0.2))
  }
  for (ii in pop$year[pop$iso3_sex_source==cc]){
    segments(x0=ii, y0=(ylim[1]-0.04*(ylim[2]-ylim[1])), x1=ii, y1=ylim[1], lwd=4)
  }
  split <- strsplit(cc, "&&")
  iso3.sex.source <- do.call(rbind, split)
  tmp_iso3 <- iso3.sex.source[,1]
  tmp_sex <- iso3.sex.source[,2]
  tmp_source <- iso3.sex.source[,3]

  title(main=paste("Completeness Estimates\n", temp$ihme_loc_id[1], "  -  ", temp$region_name[1], "  -  ", temp$location_name[1], "\n", tmp_iso3, "&&", tmp_sex, "&&", tmp_source,
                   "\n", "lambda: ", temp$lambda[1],
                   ", zeta: ", temp$zeta[1],
                   ", alpha: ", temp$alpha[1], sep=""))
}

#### Looping  ####
for (fixed in c(T,F)) {
  # Combined plot
  pdf(paste0("FILEPATH"), width=15, height=10)
  par(xaxs="i")
  for (sr in sort(unique(all$super_region_name))) {
    tempall <- all[all$super_region_name==sr,]
    for (rr in sort(unique(tempall$region_name))) {
      tempall2 <- tempall[tempall$region_name==rr,]
      for (cc in sort(c(unique(tempall2$iso3_sex_source[grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)]),unique(tempall2$iso3_sex_source[!grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)])))) {
        plot_ddm(all, comp, data_2019, final_comp_2019, cc)
      }
    }
  }

  dev.off()

  # Location specific plots
  for (sr in sort(unique(all$super_region_name))) {
    tempall <- all[all$super_region_name==sr,]
    for (rr in sort(unique(tempall$region_name))) {
      tempall2 <- tempall[tempall$region_name==rr,]
      for (cc in sort(c(unique(tempall2$iso3_sex_source[grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)]),unique(tempall2$iso3_sex_source[!grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)])))) {

        pdf(paste0("FILEPATH"), width=15, height=10)
        par(xaxs="i")
        plot_ddm(all, comp, data_2019, final_comp_2019, cc)
        dev.off()
      }
    }
  }
}



