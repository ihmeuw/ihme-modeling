####################################################################################################################################################################
# 
# Author: USERNAME
# Purpose: Create forest plots for CSA results
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

library(crosswalk002, lib.loc = "FILEPATH")

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

csa_pairs <- list.files(in_dir)
csa_pairs <- csa_pairs[!csa_pairs %like% 'raw']
sex_levs <- c('Combined Men and Women', 'Women', 'Men')
risk <- 'Childhood sexual abuse'

# Plotting function
plot_results <- function(data_tmp = data, draws_tmp = draws, inner_lower_tmp = inner_lower, inner_upper_tmp = inner_upper){
  
  # health outcome
  cause <- stringr::str_split(ro, "-")[[1]][2]
  if (cause=='maternal_abort_mi'){cause<-'maternal_abort_mis'}
  if (cause=='asthma'){cause<-'resp_asthma'}
  if (cause=='hi'){cause<-'hiv'}
  if (cause=='diabetes'){cause <- 'diabetes_typ2'}
  health_outcome <- ifelse(cause %in% cause_ids$acause, cause_ids$cause_name[cause_ids$acause == cause], cause)
  
  plot_ro_pair <- paste0(health_outcome)
  print(ro)
  print(paste0(inner_lower, '-', inner_upper))
  
  data_tmp[sex=='Female', sex:='Women']
  data_tmp[sex=='Male', sex:='Men']

  gg <- ggplot()+
    geom_vline(xintercept=1, linetype='dashed')+
    geom_pointrange(data=data_tmp, aes(x=exp(ln_rr), xmin=exp(ln_rr-1.96*ln_rr_se), xmax=exp(ln_rr+1.96*ln_rr_se), 
                                       y=reorder(str_to_title(Study_ID), Study_ID),
                                       color=as.factor(is_outlier),
                                       shape=str_to_title(sex)),
                    position=position_dodge2(0.4))+
    geom_vline(xintercept=round(exp(draws_tmp$V3[2]),2), linetype='solid', color="#6DBCC3")+
    geom_rect(aes_string(xmin = round(exp(draws_tmp$V1[2]),2), xmax = round(exp(draws_tmp$V5[2]),2),
                         ymin=0, ymax=length(unique(data$study_id))+1), fill="#6DBCC3",
              alpha = 0.35, inherit.aes = FALSE)+
    geom_rect(aes_string(xmin = inner_lower_tmp, xmax = inner_upper_tmp,
                         ymin=0, ymax=length(unique(data$study_id))+1), fill="#6DBCC3",
              alpha = 0.35, inherit.aes = FALSE)+
       geom_vline(xintercept=round(exp(draws_tmp$V2[2]),2), linetype='solid', color="#FF0000")+
    scale_color_manual(values=c('1'='red', '0'='black'), drop=FALSE)+
    scale_shape_manual(values=c('Combined'=16, 'Women'=0, 'Men'=2), labels = function(x) str_wrap(x, width = 15), position='left', drop=FALSE)+
    labs(title=plot_ro_pair, 
         color='Is Outlier',
         y='Study ID', 
         x='Effect Size (95%UI)',
         shape='Gender')+
    theme_bw()+
    scale_x_log10()+
    guides(color = guide_legend(order = 1), shape = guide_legend(order = 2))+
    aesth
  return(gg)
}

# Calling in data
for(ro in csa_pairs){
  
  data <- fread(paste0(in_dir, ro, '/', ro, '.csv'))
  og_data <- fread(paste0(in_dir, ro, '/raw-', ro, '.csv'))
  in_dir2 <- paste0(in_dir, ro)
  
  data <- merge(data, og_data[, .(Study_ID, violence_type, age_mean, sex, exposed_level, exposure_definition, outcome_name)])
  data[sex=='Combined Male and Female', sex:='Combined']
  
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
  
  beta <- model_out$beta[1]
  beta_sd <- model_out$beta[2]
  gamma <- model_out$gamma[1]
  
  sd_new <- sqrt(beta_sd^2 + gamma)

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
for(ro in csa_pairs){
  
  print(ro)
  data <- get(paste0(ro, "_data"))
  draws <- get(paste0(ro, "_draws"))
  inner_lower <- get(paste0(ro, "_inner_lower"))
  inner_upper <- get(paste0(ro, "_inner_upper"))
  
  print(inner_lower)
  print(inner_upper)
  
  gg_final <- plot_results()
  
  if (ro=='abuse_csa-mental_alcohol'){
    lg <- get_legend(gg_final)
  }
  gg_final <- gg_final + theme(legend.position = 'none')

  gg_List[[ro]] <- gg_final
  
}

#CREATE FIG 2 - panels for five outcomes with 2-star assoc. ---------------------------------------------------------------------------------------------------------------------

p1<-plot_grid(gg_List$`abuse_csa-mental_alcohol`,
              gg_List$`abuse_csa-inj_suicide`, 
              gg_List$`abuse_csa-mental_unipolar_mdd`,
              gg_List$`abuse_csa-mental_anxiety`,
              gg_List$`abuse_csa-asthma`, 
              lg,
              ncol=2,
              align='vh',
              axis='lrtb')


fig_title <- ggdraw() + draw_label('Figure 2. Forest plots for childhood sexual abuse and five outcomes with an estimated three- or two-star association identified through the systematic review of the literature.', fontface='bold', size=16)

p4 <- plot_grid(fig_title, p1, ncol=1, rel_heights=c(0.05, 1), vjust='0')

ggsave(paste0(paper_dir, 'CSA_figure2_', gsub('-', '_', Sys.Date()), '.pdf'), p4, device = cairo_pdf,
       width = 25, height = 20, units = "in")

#CREATE FIG 3 - panels for five MH outcomes with one-star assoc ---------------------------------------------------------------------------------------------------------------------
p1<-plot_grid(gg_List$`abuse_csa-diabetes`, 
              gg_List$`abuse_csa-hi`,
              gg_List$`abuse_csa-maternal_abort_mi`,
              gg_List$`abuse_csa-std`,
              gg_List$`abuse_csa-mental_drug`,
              lg,
              ncol=2,
              align='vh',
              axis='lrtb')

fig_title <- ggdraw() + draw_label('Figure 3. Forest plots for childhood sexual abuse and five health outcomes with an estimated one-star association identified through the systematic review of the literature.', fontface='bold', size=16)

p4 <- plot_grid(fig_title, p1, ncol=1, rel_heights=c(0.05, 1), vjust='0')

ggsave(paste0(paper_dir, 'CSA_figure3_', gsub('-', '_', Sys.Date()), '.pdf'), p4, device = cairo_pdf,
       width = 25, height = 20, units = "in")

#CREATE FIG 4 - panels for five non-MH outcomes with one-star assocation  ---------------------------------------------------------------------------------------------------------------------
p1<-plot_grid(gg_List$`abuse_csa-mental_conduct`, 
              gg_List$`abuse_csa-mental_eating_bulimia`,
              gg_List$`abuse_csa-mental_schizo`,
              gg_List$`abuse_csa-cvd_ihd`, 
              gg_List$`abuse_csa-mental_eating_anorexia`,
              lg,
              ncol=2,
              align='vh',
              axis='lrtb')

fig_title <- ggdraw() + draw_label('Figure 4. Forest plots for childhood sexual abuse and five health outcomes with an estimated one- or zero-star association identified through the systematic review of the literature.', fontface='bold', size=16)

p4 <- plot_grid(fig_title, p1, ncol=1, rel_heights=c(0.05, 1), vjust='0')

ggsave(paste0(paper_dir, 'CSA_figure4_', gsub('-', '_', Sys.Date()), '.pdf'), p4, device = cairo_pdf,
       width = 25, height = 20, units = "in")

# SAVE EACH INDIVIDUALLY ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

for(ro in csa_pairs){
  
  print(ro)
  data <- get(paste0(ro, "_data"))
  draws <- get(paste0(ro, "_draws"))
  model_out <- get(paste0(ro, "_model_results"))
  
  beta <- model_out$beta[1]
  beta_sd <- model_out$beta[2]
  gamma <- model_out$gamma[1]
  
  sd_new <- sqrt(beta_sd^2 + gamma)
  
  inner_lower <- exp(beta - (qnorm(0.975)*sd_new))
  inner_upper <- exp(beta + (qnorm(0.975)*sd_new))
  
  gg_final <- plot_results()

  ggsave(paste0(paper_dir, ro, '_', gsub('-', '_', Sys.Date()), '_UIwithGammaOnly.pdf'), gg_final, device = cairo_pdf,
         width = 14, height = 8, units = "in")
}

for (ro in csa_pairs){
  gg <- gg_List[[paste0(ro)]]
  ggsave(paste0(paper_dir, ro, '_', gsub('-', '_', Sys.Date()), '.pdf'), gg, device = cairo_pdf,
         width = 14, height = 8, units = "in")
}
