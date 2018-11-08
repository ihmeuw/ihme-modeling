######################################################################
## Author: 
## Date Created: 18 August 2013
## Description: Graph two different sets of DDM results for comparison purposes
######################################################################

# rm(list=ls())
library(foreign); library(plyr); library(reshape); library(haven)
library(data.table)
if (Sys.info()[1] == "Linux") root <- "/home/j" else root <- "J:"

source(paste0(root,"FILEPATH"))

## last year in the analysis
end_year <- 2017

# whether you want all plots to have the same axis scale {T} or not {F} or to do both {c(T,F)}
fixed <- c(T,F)

# make transparent color function
makeTransparent<-function(someColor, alpha=100) {
  newColor<-col2rgb(someColor)
  apply(newColor, 2,
        function(curcoldata) rgb(red=curcoldata[1],green=curcoldata[2],blue=curcoldata[3],alpha=alpha,maxColorValue=255)
  )
}

## Read in data
all <- data.table(read_dta(paste0(main_dir, "d08_smoothed_completeness.dta")))


all <- all[!grepl("USA_", ihme_loc_id) & !grepl("NOR_", ihme_loc_id) & !grepl("JPN_", ihme_loc_id) & !grepl("GBR_", ihme_loc_id)]


final_comp <- data.table(read_dta(paste0(main_dir, "d08_final_comp.dta")))


final_comp <- final_comp[!grepl("USA_", ihme_loc_id) & !grepl("NOR_", ihme_loc_id) & !grepl("JPN_", ihme_loc_id) & !grepl("GBR_", ihme_loc_id)]


setnames(final_comp, "source", "source_type")
final_comp[, iso3_sex_source := paste0(ihme_loc_id, "&&", sex, "&&", source_type)]
final_comp <- final_comp[, list(iso3_sex_source, year, final_comp)]

all <- merge(all, final_comp, by = c('iso3_sex_source', 'year'), all.x = T)
all[is.na(final_comp), final_comp := 0]


hyper_param <- fread(paste0("FILEPATH"))
specific_lambda_locs <- unique(hyper_param[iso3_sex_source != "default"]$graph_id)
hyper_param <- hyper_param[, list(graph_id, lambda)]
all[iso3_sex_source %in% specific_lambda_locs, graph_id := iso3_sex_source]
all[!iso3_sex_source %in% specific_lambda_locs, graph_id := "default"]

all <- merge(all, hyper_param, by = 'graph_id', all.x = T)
all$graph_id <- NULL



all$alpha <- 5
all$alpha[grepl("PAN", all$iso3_sex_source)] <- 1
all$alpha[grepl("MNG", all$iso3_sex_source)] <- 1
all$alpha[grepl("PHL", all$iso3_sex_source)] <- 0.8
all$alpha[grepl("TUR", all$iso3_sex_source)] <- 0.7
all$alpha[grepl("UZB", all$iso3_sex_source)] <- 0.7

all$zeta <- 0.95


comp <- read.dta(paste0(root, "FILEPATH"))


comp <- comp[!grepl("USA_", comp$ihme_loc_id) & !grepl("NOR_", comp$ihme_loc_id) & !grepl("JPN_", comp$ihme_loc_id) & !grepl("GBR_", comp$ihme_loc_id),]


### Plot results
## get colors and symbols
all <- merge(all, data.frame(comp_type=c("ggb", "seg", "ggbseg"), pch=c(24,25,23),
                             col=c("green", "blue", "purple"), stringsAsFactors=F), all.x=T)
all <- all[order(all$region_name, all$iso3_sex_source, all$year, all$comp_type),]

comp <- merge(comp, data.frame(comp_type=c("ggb", "seg", "ggbseg"), pch=c(24,25,23),
                               col=makeTransparent(c("green", "blue", "purple"),alpha=65), stringsAsFactors=F), all.x=T)
comp <- comp[order(comp$region_name, comp$iso3_sex_source, comp$year, comp$comp_type),]

pop <- data.table(read_dta(paste0(main_dir, "d05_formatted_ddm.dta")))

pop <- pop[!grepl("USA_", ihme_loc_id) & !grepl("NOR_", ihme_loc_id) & !grepl("JPN_", ihme_loc_id) & !grepl("GBR_", ihme_loc_id)]

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



deaths <- data.table(read_dta(paste0(main_dir, "d05_formatted_ddm.dta")))


deaths <- deaths[!grepl("USA_", ihme_loc_id) & !grepl("NOR_", ihme_loc_id) & !grepl("JPN_", ihme_loc_id) & !grepl("GBR_", ihme_loc_id)]

deaths$source_type[grepl("SRS", deaths$source_type)] <- "SRS"
deaths$source_type[grepl("DSP", deaths$source_type)] <- "DSP"
deaths$source_type[grepl("SSPC|DC", deaths$source_type)] <- "SSPC-DC"
deaths$iso3_sex_source <- paste(deaths$ihme_loc_id, deaths$sex, deaths$source_type, sep="&&")


output_graph_dir <- paste0("FILEPATH", version_id)
dir.create(file.path(output_graph_dir), showWarnings = FALSE)
dir.create(file.path(paste0(output_graph_dir, "/location_specific")),
           showWarnings = FALSE)
for (fixed in fixed) {
  pdf(paste0(output_graph_dir, "FILEPATH", if(fixed) paste0("_fixed_range.pdf") else paste0(".pdf")), width=15, height=10)
  par(xaxs="i")
  for (sr in sort(unique(all$super_region_name))) {
    tempall <- all[all$super_region_name==sr,]
    for (rr in sort(unique(tempall$region_name))) {
      tempall2 <- tempall[tempall$region_name==rr,]
      for (cc in sort(c(unique(tempall2$iso3_sex_source[grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)]),unique(tempall2$iso3_sex_source[!grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)])))) {
        

        temp <- all[all$iso3_sex_source == cc,]
        temp2 <- comp[comp$iso3_sex_source == cc,]
        if (fixed) ylim <- c(0,2) else ylim <- range(na.omit(unlist(temp[,c("comp", "pred1", "pred2", "u5_comp", "u5_comp_pred", "pred", "lower", "upper", "trunc_lower", "trunc_upper")])))
        plot(0, 0, xlim=c(1950,end_year), ylim=ylim, type="n", xlab="Year", ylab="comp")
        
        # Uncertainty intervals
        if (grepl("VR|SRS|DSP", cc)){
          polygon(c(temp$year, rev(temp$year)), c(temp$trunc_lower, rev(temp$trunc_upper)), col= makeTransparent("gray20", alpha = 50), border=makeTransparent("gray20", alpha = 50))
          polygon(c(rev(temp$year), temp$year), c(rev(temp$trunc_lower), temp$trunc_upper), col= makeTransparent("gray20", alpha = 50), border=makeTransparent("gray20", alpha = 50))
          legend("topleft", c("child", "ggb", "seg", "ggbseg", "child", "1st", "2nd", "Truncated Completeness", "CI truncated (VR/SRS/DSP/MCCD/CR)", "Final Completeness"),
                 col=c("red","green","blue","purple","red","orange","black","black","grey35","yellow"),
                 pch=c(19,24,25,23,NA,NA,NA,NA,NA,NA,NA),
                 pt.bg=c("red","green","blue","purple",NA,NA,NA,NA,NA,NA),
                 fill=c(NA,NA,NA,NA,NA,NA,NA,NA,"gray35",NA),
                 border=c(NA,NA,NA,NA,NA,NA,NA,NA,"grey35",NA),
                 lwd=c(NA,NA,NA,NA,1,1,3,3,NA,3),
                 lty=c(NA,NA,NA,NA,1,1,1,2,NA,1),
                 ncol=2, bg="white")
        } else{
          polygon(c(temp$year, rev(temp$year)), c(temp$lower, rev(temp$upper)), col=makeTransparent("gray20", alpha = 20), border="gray20")
          polygon(c(rev(temp$year), temp$year), c(rev(temp$upper), temp$lower), col=makeTransparent("gray20", alpha = 20), border="gray20")
          
          legend("topleft", c("child", "ggb", "seg", "ggbseg", "child", "1st", "2nd", "Truncated Completeness", "CI untruncated (VR/SRS/DSP/MCCD/CR)", "Final Completeness"),
                 col=c("red","green","blue","purple","red","orange","black","black", makeTransparent("gray20", alpha = 20), "yellow"),
                 pch=c(19,24,25,23,NA,NA,NA,NA,NA,NA),
                 pt.bg=c("red","green","blue","purple",NA,NA,NA,NA,NA,NA,NA),
                 fill=c(NA,NA,NA,NA,NA,NA,NA,NA,makeTransparent("gray20", alpha = 20),NA),
                 border=c(NA,NA,NA,NA,NA,NA,NA,NA, "grey20",NA),
                 lwd=c(NA,NA,NA,NA,1,1,3,3,NA,3),
                 lty=c(NA,NA,NA,NA,1,1,1,2,NA,1),
                 ncol=2, bg="white")
        }
        

        lines(temp$year, temp$u5_comp_pred, col="red", lwd=2, lty=1)
        points(temp$year, temp$u5_comp, pch=19, col="red", cex=1.5)
        

        lines(temp2$year, temp2$u5_comp_pred, col=makeTransparent("red",alpha=65), lwd=2, lty=1)
        points(temp2$year, temp2$u5_comp, pch=19, col=makeTransparent("red",alpha=65), cex=1.25)
        

        lines(temp$year, temp$pred1, col="orange", lwd=2, lty=1)
        lines(temp$year, temp$pred, col="black", lwd=4, lty=1)
        lines(temp$year, temp$trunc_pred, col="black", lwd=4, lty=2)
        lines(temp$year, temp$pred, col="black", lwd=4, lty=2)
        lines(temp$year, temp$final_comp, col="yellow", lwd=4, lty=2)
        

        lines(temp2$year, temp2$pred1, col=makeTransparent("orange",alpha=65), lwd=2, lty=1)
        lines(temp2$year, temp2$pred, col=makeTransparent("black",alpha=95), lwd=3, lty=1)
        lines(temp2$year, temp2$trunc_pred, col=makeTransparent("black",alpha=95), lwd=3, lty=2)
        

        abline(a=0.95, b=0, col="blueviolet", lwd=2)
        

        points(temp$year, temp$comp, pch=temp$pch, col=temp$col, bg=ifelse(temp$exclude==1, NA, temp$col), cex=2)
        

        points(temp2$year, temp2$comp, pch=temp2$pch, col=temp2$col, bg=ifelse(temp2$exclude==1, NA, temp2$col), cex=1.75)
        

        deaths_years <- deaths$deaths_years[deaths$iso3_sex_source == cc]
        tmp_deaths_years <- strsplit(deaths_years, " ")
        deaths_years <- as.numeric(unlist(tmp_deaths_years, recursive = TRUE, use.names = TRUE))
        pop_years <- pop$year[pop$iso3_sex_source==cc]
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
        
        title(main=paste(temp$ihme_loc_id[1], "  -  ", temp$region_name[1], "  -  ", temp$location_name[1], "\n", tmp_iso3, "&&", tmp_sex, "&&", tmp_source,
                         "\n", "lambda: ", temp$lambda[1],
                         ", zeta: ", temp$zeta[1],
                         ", alpha: ", temp$alpha[1], sep=""))
      }
    }
  }
  dev.off()
  

  for (sr in sort(unique(all$super_region_name))) {
    tempall <- all[all$super_region_name==sr,]
    for (rr in sort(unique(tempall$region_name))) {
      tempall2 <- tempall[tempall$region_name==rr,]
      for (cc in sort(c(unique(tempall2$iso3_sex_source[grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)]),unique(tempall2$iso3_sex_source[!grepl("VR|SRS|DSP|MCCD|CR", tempall2$iso3_sex_source)])))) {
        pdf(paste0(output_graph_dir, "FILEPATH", if(fixed) paste0("_fixed_range.pdf") else paste0(".pdf")), width=15, height=10)
        par(xaxs="i")

        temp <- all[all$iso3_sex_source == cc,]
        temp2 <- comp[comp$iso3_sex_source == cc,]
        if (fixed) ylim <- c(0,2) else ylim <- range(na.omit(unlist(temp[,c("comp", "pred1", "pred2", "u5_comp", "u5_comp_pred", "pred", "lower", "upper", "trunc_lower", "trunc_upper")])))
        plot(0, 0, xlim=c(1950,end_year), ylim=ylim, type="n", xlab="Year", ylab="comp")
        

        if (grepl("VR|SRS|DSP", cc)){
          polygon(c(temp$year, rev(temp$year)), c(temp$trunc_lower, rev(temp$trunc_upper)), col= makeTransparent("gray20", alpha = 50), border=makeTransparent("gray20", alpha = 50))
          polygon(c(rev(temp$year), temp$year), c(rev(temp$trunc_lower), temp$trunc_upper), col= makeTransparent("gray20", alpha = 50), border=makeTransparent("gray20", alpha = 50))
          legend("topleft", c("child", "ggb", "seg", "ggbseg", "child", "1st", "2nd", "Truncated Completeness", "CI truncated (VR/SRS/DSP/MCCD/CR)", "Final Completeness"),
                 col=c("red","green","blue","purple","red","orange","black","black","grey35","yellow"),
                 pch=c(19,24,25,23,NA,NA,NA,NA,NA,NA,NA),
                 pt.bg=c("red","green","blue","purple",NA,NA,NA,NA,NA,NA),
                 fill=c(NA,NA,NA,NA,NA,NA,NA,NA,"gray35",NA),
                 border=c(NA,NA,NA,NA,NA,NA,NA,NA,"grey35",NA),
                 lwd=c(NA,NA,NA,NA,1,1,3,3,NA,3),
                 lty=c(NA,NA,NA,NA,1,1,1,2,NA,1),
                 ncol=2, bg="white")
        } else{
          polygon(c(temp$year, rev(temp$year)), c(temp$lower, rev(temp$upper)), col=makeTransparent("gray20", alpha = 20), border="gray20")
          polygon(c(rev(temp$year), temp$year), c(rev(temp$upper), temp$lower), col=makeTransparent("gray20", alpha = 20), border="gray20")
          
          legend("topleft", c("child", "ggb", "seg", "ggbseg", "child", "1st", "2nd", "Truncated Completeness", "CI untruncated (VR/SRS/DSP/CR)", "Final Completeness"),
                 col=c("red","green","blue","purple","red","orange","black","black", makeTransparent("gray20", alpha = 20), "yellow"),
                 pch=c(19,24,25,23,NA,NA,NA,NA,NA,NA),
                 pt.bg=c("red","green","blue","purple",NA,NA,NA,NA,NA,NA,NA),
                 fill=c(NA,NA,NA,NA,NA,NA,NA,NA,makeTransparent("gray20", alpha = 20),NA),
                 border=c(NA,NA,NA,NA,NA,NA,NA,NA, "grey20",NA),
                 lwd=c(NA,NA,NA,NA,1,1,3,3,NA,3),
                 lty=c(NA,NA,NA,NA,1,1,1,2,NA,1),
                 ncol=2, bg="white")
        }
        

        lines(temp$year, temp$u5_comp_pred, col="red", lwd=2, lty=1)
        points(temp$year, temp$u5_comp, pch=19, col="red", cex=1.5)
        

        lines(temp2$year, temp2$u5_comp_pred, col=makeTransparent("red",alpha=65), lwd=2, lty=1)
        points(temp2$year, temp2$u5_comp, pch=19, col=makeTransparent("red",alpha=65), cex=1.25)
        

        lines(temp$year, temp$pred1, col="orange", lwd=2, lty=1)
        lines(temp$year, temp$pred, col="black", lwd=4, lty=1)
        lines(temp$year, temp$trunc_pred, col="black", lwd=4, lty=2)
        lines(temp$year, temp$pred, col="black", lwd=4, lty=2)
        lines(temp$year, temp$final_comp, col="yellow", lwd=4, lty=2)
        

        lines(temp2$year, temp2$pred1, col=makeTransparent("orange",alpha=65), lwd=2, lty=1)
        lines(temp2$year, temp2$pred, col=makeTransparent("black",alpha=95), lwd=3, lty=1)
        lines(temp2$year, temp2$trunc_pred, col=makeTransparent("black",alpha=95), lwd=3, lty=2)
        

        abline(a=0.95, b=0, col="blueviolet", lwd=2)
        

        points(temp$year, temp$comp, pch=temp$pch, col=temp$col, bg=ifelse(temp$exclude==1, NA, temp$col), cex=2)
        

        points(temp2$year, temp2$comp, pch=temp2$pch, col=temp2$col, bg=ifelse(temp2$exclude==1, NA, temp2$col), cex=1.75)
        

        deaths_years <- deaths$deaths_years[deaths$iso3_sex_source == cc]
        tmp_deaths_years <- strsplit(deaths_years, " ")
        deaths_years <- as.numeric(unlist(tmp_deaths_years, recursive = TRUE, use.names = TRUE))
        pop_years <- pop$year[pop$iso3_sex_source==cc]
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
        
        title(main=paste(temp$ihme_loc_id[1], "  -  ", temp$region_name[1], "  -  ", temp$location_name[1], "\n", tmp_iso3, "&&", tmp_sex, "&&", tmp_source,
                         "\n", "lambda: ", temp$lambda[1],
                         ", zeta: ", temp$zeta[1],
                         ", alpha: ", temp$alpha[1], sep=""))
        dev.off()
      }
    }
  }
}
