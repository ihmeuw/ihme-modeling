####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Create forest plot for IPV results
#
####################################################################################################################################################################

rm(list=ls())

# set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

library(data.table)
library(dplyr)
library(openxlsx)
library(ggplot2)
library(stringr)
library(ggsci)
library(forestplot)
library(tidyr)
library(cowplot)
library(grid)
library(gridExtra)

invisible(sapply(list.files("FILEPATH", full.names = T), source))
cause_ids <- get_ids("cause")
rei_ids <- get_ids("rei")
aesth <- theme_bw() + theme(axis.title.x = element_text(size=14,face="plain", family='Sans'),
                            axis.title.y = element_text(size=14,face="plain", family='Sans'),
                            axis.text.x = element_text(size=14,face="plain", family='Sans'),
                            axis.text.y = element_text(size=14,face="plain", family='Sans'),
                            strip.text = element_text(size=14,face="bold"),
                            plot.background = element_blank(),
                            # plot.title =element_blank(),
                            legend.text=element_text(size=14, family='Sans'),
                            legend.title=element_text(size=14, face='bold', family='Sans'),
                            plot.title=element_text(size=14, face='bold', family='Sans'),
                            plot.caption=element_text(size=14, family='Sans'))

paper_dir <- 'FILEPATH'
in_dir <- 'FILEPATH'

ipv_pairs <- c('abuse_ipv_exp-hi', 'abuse_ipv_exp-mental_anxiety', 'abuse_ipv_exp-mental_unipolar_mdd', 'abuse_ipv_exp-inj_suicide', 'abuse_ipv_exp-maternal_abort_mi')
shape_levs <- c("physical; sexual","physical","physical; psychological","sexual","physical; sexual; psychological")
risk <- 'Intimate partner violence'

# Plotting function
plot_results <- function(data_tmp = data, draws_tmp = draws, inner_lower_tmp = inner_lower, inner_upper_tmp = inner_upper){
  
  data_tmp[Study_ID=='romito 2009' & subgroup_analysis_free_text=='', subgroup_analysis_free_text:='women aged <30 years']
  # health outcome
  cause <- stringr::str_split(ro, "-")[[1]][2]
  if (cause=='maternal_abort_mi'){cause<-'maternal_abort_mis'}
  if (cause=='asthma'){cause<-'resp_asthma'}
  if (cause=='hi'){cause<-'hiv'}
  health_outcome <- ifelse(cause %in% cause_ids$acause, cause_ids$cause_name[cause_ids$acause == cause], cause)
  
  plot_ro_pair <- paste0(health_outcome)
  print(ro)
  print(paste0(inner_lower, '-', inner_upper))
  
  gg <- ggplot()+
    geom_vline(xintercept=1, linetype='dashed')+
    geom_pointrange(data=data_tmp, aes(x=exp(ln_rr), xmin=exp(ln_rr-1.96*ln_rr_se), xmax=exp(ln_rr+1.96*ln_rr_se), 
                                       y=reorder(str_to_title(Study_ID), Study_ID),
                                       color=as.factor(is_outlier),
                                       shape=str_to_title(violence_type)),
                    position=position_dodge2(0.3))+
    geom_vline(xintercept=round(exp(draws_tmp$V3[2]),2), linetype='solid', color="#6DBCC3")+
    geom_rect(aes(xmin = round(exp(draws_tmp$V1[2]),2), xmax = round(exp(draws_tmp$V5[2]),2),
                  ymin=0, ymax=length(unique(data_tmp$Study_ID))+1), fill="#6DBCC3",
              alpha = 0.35, inherit.aes = FALSE)+
    geom_rect(aes_string(xmin = inner_lower_tmp, xmax = inner_upper_tmp,
                         ymin=0, ymax=length(unique(data_tmp$Study_ID))+1), fill="#6DBCC3",
              alpha = 0.35, inherit.aes = FALSE)+
    geom_vline(xintercept=round(exp(draws_tmp$V2[2]),2), linetype='solid', color="#FF0000")+
    scale_color_manual(values=c('1'='red', '0'='black'))+
    scale_shape_manual(values=c('Physical; Sexual'=16, 'Physical'=0, 'Sexual'=2, 'Physical; Sexual; Psychological'=5, 'Physical; Psychological'=8), drop=FALSE)+
    labs(title=paste0(plot_ro_pair), 
         color='Is Outlier',
         y='Study ID', 
         x='Effect Size (95%UI)',
         shape='Violence types included in\nexposure definition')+
    guides(color = guide_legend(order = 1), shape = guide_legend(order = 2))+
    scale_x_log10()+
    aesth

  return(gg)
}

# Calling in data
for(ro in ipv_pairs){
  
  data <- fread(paste0(in_dir, ro, '/', ro, '.csv'))
  og_data <- fread(paste0(in_dir, ro, '/raw-', ro, '.csv'))
  in_dir2 <- paste0(in_dir, ro)
  
  data <- merge(data, og_data[, .(Study_ID, violence_type, violence_type_combination, age_mean, age_sd, age_lower, age_upper, sex, exposed_level, exposure_definition, outcome_name)])
  
  data[, violence_type:=as.factor(violence_type)]
  
  model_out <- yaml::read_yaml(paste0(in_dir2, '/summary.yaml'))
  draws <- fread(paste0(in_dir2, '/outer_quantiles.csv'))
  inner_draws <- fread(paste0(in_dir2, '/inner_quantiles.csv'))
  covs <- yaml::read_yaml(paste0(in_dir2, '/cov_finder_result.yaml'))$selected_covs
  
  covs2 <- c()
  for (n in 1:length(covs)){
    covs2 <- paste0(covs2, ', ', covs[n])
  }
  covs2 <- str_sub(covs2, 2, -1)
  if (covs2==" NULL, ") {covs2 <- 'none'}
  
  inner_lower <- round(exp(inner_draws$V1[2]),2)
  inner_upper <- round(exp(inner_draws$V5[2]),2)
  
  assign(paste0(ro, "_data"), data)
  assign(paste0(ro, "_model_results"), model_out)
  assign(paste0(ro, "_cov"), covs2)
  assign(paste0(ro, "_draws"), draws)
  assign(paste0(ro, "_inner_upper"), inner_upper)
  assign(paste0(ro, "_inner_lower"), inner_lower)
}

gg_List <- list()  
for(ro in ipv_pairs){
    
  print(ro)
    data <- get(paste0(ro, "_data"))
    draws <- get(paste0(ro, "_draws"))
    inner_lower <- get(paste0(ro, "_inner_lower"))
    inner_upper <- get(paste0(ro, "_inner_upper"))

    print(inner_lower)
    print(inner_upper)

    gg_final <- plot_results()
    if (ro=='abuse_ipv_exp-mental_unipolar_mdd'){
      lg <- get_legend(gg_final)
    }
    gg_final <- gg_final + theme(legend.position = 'none')
    gg_List[[ro]] <- gg_final
    
}

p1<-plot_grid(gg_List$`abuse_ipv_exp-mental_unipolar_mdd`, 
              gg_List$`abuse_ipv_exp-maternal_abort_mi`, 
              gg_List$`abuse_ipv_exp-hi`,
              gg_List$`abuse_ipv_exp-mental_anxiety`,
              gg_List$`abuse_ipv_exp-inj_suicide`,
              lg,
              ncol=2,
              align='vh',
              axis='lrtb')

fig_title <- ggdraw() + draw_label('Figure 1. Forest plots for intimate partner violence against women and five health outcomes identified through the systematic review of the literature.', fontface='bold', size=16)

p4 <- plot_grid(fig_title, p1, ncol=1, rel_heights=c(0.05, 1), vjust='0')

ggsave(paste0(paper_dir, 'IPV_figure1_', gsub('-', '_', Sys.Date()), '.pdf'), p4, device = cairo_pdf,
       width = 25, height = 20, units = "in")




