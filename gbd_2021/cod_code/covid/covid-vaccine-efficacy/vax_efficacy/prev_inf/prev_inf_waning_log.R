#Previous infection waning immunity (time since infection)

library(tidyverse)
library(reshape2)
library(data.table)

severity_type <- 'infection'
initial_variant <- 'Ancestral'
second_variant <- 'Non_Omicron'

output_dir <- 'FILEPATH'


pinf <- fread("FILEPATH/previous_infection_immunity.csv")

#keep only "0=inlcude" of "exclude this source from analysis" variable
ve1 <- subset(pinf, exclude == 0)

ve2 <- subset(ve1, select = c(exclude, study_id, author, location_id, location_name, piv, prim_inf_var, variant, symptom_severity, severity, 
                              sample_size, efficacy_mean, efficacy_lower, efficacy_upper, pinf, average_time_since_infection,
                              start_interval, end_interval))

ve2 <- mutate(ve2, sev_severity = if_else(symptom_severity == "Severe", "severe",
                                          if_else(symptom_severity == "severe", "severe",
                                                  if_else(symptom_severity == "Infection", "infection",
                                                          if_else(symptom_severity == "infection", "infection",
                                                                  if_else(symptom_severity == "asymptomatic, Mild, Moderate, Severe", "symptomatic", 
                                                                          if_else(symptom_severity == "Asymptompatic, Mild, Moderate + Severe", "symptomatic",
                                                                                  if_else(symptom_severity == "Mild, Moderate", "symptomatic",
                                                                                          if_else(symptom_severity == "Mild, Moderate + Severe", "symptomatic",
                                                                                                  if_else(symptom_severity == "Moderate, Severe", "symptomatic", "NA"))))))))))

#rename prim_variant_name primary variant name
ve2 <- mutate(ve2, prim_variant_name = if_else(prim_inf_var == "Ancestral", "Ancestral",
                                       if_else(prim_inf_var == "all variant", "Ancestral",         #original: "all variant", "all variant",
                                       if_else(prim_inf_var == "Ancestral, B.1.1.7", "Ancestral",  #original: "Ancestral, B.1.1.7", "mixed",
                                       if_else(prim_inf_var == "mixed variant", "Ancestral",       #original: "mixed variant", "mixed",
                                       if_else(prim_inf_var == "mixed", "Ancestral",               #original: "mixed", "mixed",
                                       if_else(prim_inf_var == "B.1.1.7", "Ancestral",             #changed to "Ancestral" for this category be considered in the data availability plot
                                       if_else(prim_inf_var == "B.1.351", "Beta",
                                       if_else(prim_inf_var == "B.1.617.2", "Ancestral",           #changed to "Ancestral" for this category be considered in the data availability plot
                                       if_else(prim_inf_var == "B.1.1.529", "Omicron",
                                       if_else(prim_inf_var == "B.1.1.529.1", "Omicron",
                                       if_else(prim_inf_var == "B.1.1.529.2", "Omicron", "NA"))))))))))))

#rename variant (subsequent variant) variable
ve2 <- mutate(ve2, variant_name = if_else(variant == "Ancestral", "Ancestral",
                                  if_else(variant == "all variant", "Ancestral",                 #original: "all variant", "all variant",
                                  if_else(variant == "mixed variant", "Ancestral",               #original: "mixed variant", "mixed",
                                  if_else(variant == "mixed", "Ancestral",                       #original: "mixed", "mixed",
                                  if_else(variant == "mixed ", "Ancestral",                      #original: "mixed ", "mixed",
                                  if_else(variant == "Ancestral, B.1.1.7", "Ancestral",          #original: "Ancestral, B.1.1.7", "mixed", 
                                  if_else(variant == "B.1.1.7", "Alpha",
                                  if_else(variant == "B.1.351", "Beta",
                                  if_else(variant == "B.1.617.2", "Delta",
                                  if_else(variant == "B.1.1.529", "Omicron",
                                  if_else(variant == "B.1.1.529.1", "Omicron",
                                  if_else(variant == "B.1.1.529.2", "Omicron", "NA")))))))))))))

# adding column for non omicron and omicron for combine analysis
# renaming variants 
ve2 <- mutate(ve2, group_analysis = if_else(variant_name == "Ancestral", "Non_Omicron",
                                            if_else(variant_name == "Alpha", "Non_Omicron",
                                                if_else(variant_name == "Beta", "Non_Omicron",
                                                    if_else(variant_name == "Delta", "Non_Omicron",
                                                            if_else(variant_name == "Omicron", "Omicron",
                                                                    if_else(variant_name == "Omicron", "Omicron",
                                                                            if_else(variant_name == "Omicron", "Omicron", "NA"))))))))


#new study_id_2 variable (to differentiate studies with the same study_id but different variants)
ve2$study_id2 = paste(ve2$study_id,ve2$prim_variant_name, ve2$variant_name,  sep = "&")

#keep week after infection studies (1: only time since infection and 2:average time since infection)
ve2 <- subset(ve2, pinf == 1 | pinf == 2)

#number of studies and countries
length(unique(ve2$study_id))
length(unique(ve2$location_name))
table(ve2$location_name)
table(ve2$study_id)

#weeks after previous infection
ve2$end_interval <- as.numeric(as.character(ve2$end_interval))
ve2$start_interval <- as.numeric(as.character(ve2$start_interval))
ve2 <- mutate(ve2, mid_point1 = ((end_interval - start_interval)/2) + start_interval)

#combine in "mid_point" tow columns
ve2 <- mutate(ve2, mid_point= if_else(is.na(ve2$mid_point1), as.numeric(ve2$average_time_since_infection), as.numeric(ve2$mid_point1)))
                                   
#calculate the OR from efficacy_mean
ve2$efficacy_mean <- as.numeric(as.character(ve2$efficacy_mean))
ve2 <- mutate(ve2, or = (1 - efficacy_mean))

#excluding values below 0 because <0 did not work in logit 
spaceve2 <- ve2[!(ve2$efficacy_mean < 0), ]

# calculating e_mean from efficacy_mean
ve2 <- mutate(spaceve2, e_mean = efficacy_mean - 0.1)

# subsetting sev_severity dt
ve2 <- ve2[sev_severity == severity_type]

# storing negative e_mean values
negative_e_mean <- ve2[e_mean <= 0]

#run the model using logit space
ve2$e_mean <- as.numeric(as.character(ve2$e_mean))
ve2$efficacy_upper <- as.numeric(as.character(ve2$efficacy_upper))
ve2$efficacy_lower <- as.numeric(as.character(ve2$efficacy_lower))
ve2 <- mutate(ve2, se = (efficacy_upper - efficacy_lower)/3.92)
ve2 <- subset(ve2, sev_severity == severity_type)

#transform to logit
library(crosswalk002, lib.loc = "FILEPATH")
logit <- delta_transform(mean = ve2$e_mean, sd = ve2$se, transformation = "linear_to_logit")

names(logit) <- c("mean_logit", "sd_logit")
vacc_logit <- cbind(ve2, logit)
ve2 <- vacc_logit

# subsetting sev_severity dt
ve2 <- ve2[sev_severity == severity_type]

#to convert the effectiveness CI in OR standard error 
ve2$efficacy_upper <- as.numeric(as.character(ve2$efficacy_upper))
ve2 <- mutate(ve2, or_lower = (1 - efficacy_upper))
ve2$efficacy_lower <- as.numeric(as.character(ve2$efficacy_lower))
ve2 <- mutate(ve2, or_upper = (1 - efficacy_lower))
ve2 <- mutate(ve2, se = (or_upper - or_lower)/3.92)

#transforming in log space
ve2 <- mutate(ve2, or_log = log(or))
ve2 <- mutate(ve2, se_log = (log(or_upper) - log(or_lower))/3.92)

#meta-regression spline models with separate models by vaccine and outcome

library(dplyr)
library(mrbrt002, lib.loc = "FILEPATH")

#Previous infection
infec <- subset(ve2, prim_variant_name == initial_variant)

#Only including second variant data for ancestral, Alpha, Delta
#inf <- infec[!(infec$variant_name == "Omicron"), ]
#inf <- inf[!(inf$variant_name == "Beta"), ]

#exclude outliers for ancestral, Alpha, Delta
#inf <- inf[!(inf$efficacy_mean == 0.280), ]
#inf <- inf[!(inf$efficacy_mean == 0.420), ]
#inf <- inf[!(inf$efficacy_mean == 0.480), ]

#all second variant = Omicron
inf <- subset(infec, group_analysis == second_variant)

#3.5.1 - Setting priors and shape constraints on splines
ve6 <- MRData()
ve6$load_df(
  data = inf,  
  col_obs = "mean_logit",
  col_obs_se = "sd_logit",
  col_covs = list("mid_point"),
  col_study_id = "study_id2" )

mod1 <- MRBRT(
  data = ve6,
  cov_models = list(
    LinearCovModel("intercept", use_re = TRUE),  #TRUE #FALSE: Reduce the variation of the random effect #use_re  = use random effect
    LinearCovModel(
      alt_cov = "mid_point",
      use_spline = TRUE,
      #spline_knots = array(c(0, 0.25, 0.5, 0.75, 1)),
      #spline_knots = array(c(0, 0.45, 0.90, 1)),             # 1.0 12 24  
      #spline_knots = array(c(0, 0.67, 1)),                    # 1.0 18
      spline_knots = array(seq(0, 1, length.out = 8)),       # 1.0  9.5 18.0 26.5    #(seq(0, 1, by = 0.2)
      spline_degree = 2L,
      spline_knots_type = 'domain', #use domain or frequency (according to where are more data points density)
      spline_r_linear = TRUE,
      spline_l_linear = FALSE,
      prior_spline_monotonicity = 'decreasing'
      # prior_spline_convexity = "convex"
      # prior_spline_maxder_gaussian = array(c(0, 0.01))
      # prior_spline_maxder_gaussian = rbind(c(0,0,0,0,-1), c(Inf,Inf,Inf,Inf,0.0001))
    )
  )
)

mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

df_pred3 <- data.frame(mid_point = seq(0, 104, by = 0.1))

dat_pred3 <- MRData()
dat_pred3$load_df(
  data = df_pred3, 
  col_covs=list('mid_point')
)

df_pred3$pred5 <- mod1$predict(data = dat_pred3)

#3.2.3 â Uncertainty from fixed effects and between-study heterogeneity

n_samples3 <- 1000L
set.seed(1)
samples3 <- mod1$sample_soln(sample_size = n_samples3)

draws3 <- mod1$create_draws(
  data = dat_pred3,
  beta_samples = samples3[[1]],
  gamma_samples = samples3[[2]], 
  random_study = TRUE )

df_pred3$pred5 <- mod1$predict(dat_pred3)
df_pred3$pred_lo <- apply(draws3, 1, function(x) quantile(x, 0.025)) #, na.rm = T
df_pred3$pred_hi <- apply(draws3, 1, function(x) quantile(x, 0.975)) #, na.rm = T

#Transforming back to VE
#calculate the VE from OR log scale
df_pred3 <- mutate(df_pred3, pinf = (1 - exp(pred5)))
#convert the OR CI to effectiveness CI 
df_pred3 <- mutate(df_pred3, pinf_lower = (1 - exp(pred_hi)))
df_pred3 <- mutate(df_pred3, pinf_upper = (1 - exp(pred_lo)))

#convert VE logit to VE ve = (plogis(pred5) # + 0.1
df_pred3 <- mutate(df_pred3, ve = (plogis(pred5)))
df_pred3 <- mutate(df_pred3, pinf = (plogis(pred5) + 0.1))
df_pred3 <- mutate(df_pred3, ve_lower = (plogis(pred_lo))) 
df_pred3 <- mutate(df_pred3, ve_upper = (plogis(pred_hi))) 


#plot
inf <- mutate(inf, insesqua = (1/sqrt(se))/10)

with(inf, plot(mid_point, e_mean, xlim = c(0, 110), ylim = c(0, 1), cex = insesqua)) 
with(df_pred3, lines(mid_point, pinf))

#visualize knot locations
for (k in mod1$cov_models[[2]]$spline_knots) abline(v = k, col = "gray")

groups1 <- unique(inf$study_id2)

for (grp in groups1) {
  df_tmp <- filter(inf, study_id2 == grp)
  with(arrange(df_tmp, mid_point), lines(mid_point, e_mean, lty = 2, col = "gray"))
}

# where the knot locations are on the spline  
get_knots <- function(model, cov_model_name) {
  model$cov_models[[which(model$cov_model_names == cov_model_name)]]$spline$knots
}

get_knots(model = mod1, cov_model_name = "mid_point")

#saving curves estimates for comparison plot
#ancestral, Alpha, Delta
past_inf_detla <- df_pred3
fwrite(past_inf, file.path(output_dir, "past_inf_ancest_non_omi.csv"))

#saving points
fwrite(inf, file.path(output_dir, "inf_ancest_non_omi.csv"))
#Omicron
# past_inf_o <- df_pred3
# fwrite(past_inf_o, "/mnt/share/code/covid-19/user/jdalos/vax_efficacy/past_inf_o.csv")

######
#plot waning prev_inf

inf <- mutate(inf, insesqua = (1/sqrt(se))/10)
#symp <- mutate(sev, insesqua = (1/sqrt(se))/10)    #only one study and 2 points
#sev <- mutate(sev, insesqua = (1/sqrt(se))/10)     ##only one study and 4 points

library(ggplot2)
library(formattable)
f1 <- "Times"

previnfplot <- ggplot() + 
  geom_line(data = df_pred3, mapping = aes(x = mid_point, y = pinf), size = 1) +
  geom_point(data = inf, mapping = aes(x = mid_point, y = e_mean, color = study_id2, size = insesqua), 
             show.legend = FALSE) +
  geom_ribbon(data=df_pred3, aes(x=mid_point, ymin=pinf_lower, ymax=pinf_upper), alpha=0.4, fill="pink") +
  ylim(c(-0.2, 1))+
  xlim(c(0, 110)) +
  theme_classic() +
  theme(text=element_text(size=20, family = f1))+
        #legend.position = "bottom") +
  #panel.grid = element_blank()) 
  #panel.border = element_blank())
  guides(linetype = FALSE) +
  #theme(axis.title = element_text(size = 40)) +
  #theme(axis.text = element_text(size = 10)) +
  labs(y="Previous infection protection", x = "Week after infection", colour = "Primary and second variant") +
  #theme(legend.text = element_text(size = 12)) +
  #theme_light() +
  #theme_classic() +
  geom_hline(aes(yintercept = c(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1), linetype = "yellow"))+
  guides(size = FALSE)
  

print(previnfplot)

#saving figures in pdf for submission Fig3PanelA: ancestral, Alpha and Delta / Fig3PanelB:Omicron
outfile <- "FILEPATH"
ggsave(file = paste0(outfile, "Fig3PanelA", ".pdf"), previnfplot, device = cairo_pdf, #Fig3PanelA #Fig3PanelB
       width = 105.5, height = 91.5, units = "mm", limitsize = T,
       dpi = 320)

