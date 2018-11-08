#' Meta Analysis for Relative Risk Curves w/ Dismod ODE 
library(data.table)
library(ggplot2)
library(magrittr)
library(openxlsx)
library(readr)

#' Load necessary functions and define filepaths
source(FILEPATH/relative_risk_estimation_helper_functions.R)
cause_map   <- fread(FILEPATH/cause_map.csv)
dismod.root <- FILEPATH/dismod
save.root   <- paste0(dismod.root, "draws_for_PAF/")
former_max  <- 10

######################### VISUALIZING DATA #########################################
#' Set Global options - the cause, the exposure metric, and load the data
cause       <- 
metric      <-  #' [pack, smoked, cig, quit] PY, YS, C/D, Quit
cause.path  <- paste0(dismod.root,cause,"/")
data.path   <- paste0(cause.path,cause,"_data.xlsx")

#' Read in data and exclude all prespecified data points
raw         <- read_data(data.path, metric = metric)
raw         <- raw[exclude == 0,]

#' If cause is a CVD, extrapolate the data into age groups from 40 to 100 using log-linear assumption, using mean age of the data point as starting point
if(cause %in% c("ihd", "stroke", "afib_and_flutter", "peripheral_artery_disease", "aortic_aneurism") & metric == "cig"){
  raw <- cvd_age(raw)
  raw <- raw[mean >=1]
}

#Impute Standard error of studies missing them with proposed method. Can 
#either exclude, replace with 95th percentile SD, or use the linear regression method relating log mean to log SD. 
raw <- impute_std(raw, method = "high")

#' If quitting data, transform the RR data into proportion of relative risk reduced
if(metric == "quit"){
  raw <- transform_former(raw, new_max = former_max, new_min = 1, bound = "keep")
  raw[,min_exp := min(exp_lower), by = .(author_year, study.name)]
  raw <- raw[exp_lower != min_exp]
}

#' Plot the data for visual inspection
data.plot <- plot_data(raw,xmax=60,ymin=0,ymax=40,color.var="sex") +
  guides(color = F) 
data.plot

save_name <- "quit_data_facet"
print_pdf(data.plot, save.path = paste0(cause.path, paste0("/",save_name, ".pdf")))

######################## RUN DISMOD CURVE ##########################
#' Creates output folders and necessary variables
sex <- ifelse(length(raw[,unique(sex)])==1, raw[,unique(sex)], "Both")
date <- gsub(" |:","-",Sys.time())
identify <- "all" #' Optional identifying name for a directory
outputs <- paste0(dismod.root, cause, "/",metric,"_",sex,"_",identify,"_",date)
notes <- "knot equal"

#' Specify model parameters - spline knots, covariates, smoothing parameters, monotonicity, intercept. 
dose            <- choose_knots(raw, 7, "equal") %>% round(1) %>% sort 
adjust          <- F
covariates      <- c("ones", "sex")  #Need at minimum sex and intercept
smoothing       <- .07 %>% rep(3) 
adjust          <- F
monotonic       <- "none"
ones.cov        <- 1
#' Due to protective effects of Parkinson's, its current smoker curve is actually more similar to a typical former smoking curve
start.value     <- ifelse(metric == "quit" & cause != "parkinson", former_max, 1)

#' Prepare the Dismod directory and files
prep_ode(raw = raw,
         covariates = covariates,
         smoothing = smoothing,
         dose = dose,
         midpoint=F,
         adjust = adjust,
         start.prior = start.value,
         monotonic = monotonic,
         outputs = outputs,
         ones.cov = ones.cov,
         bound.lower=0,
         sex_effect = F)

write_csv(data.table(notes), paste0(outputs,"/run_notes.csv"))

#' Submit job to cluster - This is where the actual meta-regression model is run
job_name <- paste0("ODE_",cause,"_",metric,"_",sex)
qsub_ode(job_name,
         script = paste0(dismod.root,"ODE_run.sh"),
         slots = 10,
         memory = 20,
         arguments = outputs,
         cluster_project = PROJECT,
         logs.o = OUTPUT_LOG,
         logs.e = ERROR_LOG)

#' Putting in block. Look at results
if(T){
  job_hold(job_name = job_name, file_list = paste0(outputs,"/sample_out.csv"))
  
  #' After ODE finishes, summarize draw files and save in output folder
  summarize_outputs(outputs, force=F)
  
  #################### PLOT RISK CURVES #######################################
  df_draws <- fread(paste0(outputs, "/rr_draws.csv"))
  df_compressed <- fread(paste0(outputs, "/rr_summary.csv"))
  df_data <-fread(paste0(outputs, "/data_in.csv")) %>% 
    format_data()
  
  #df_data <- df_data %>% format_data
  
  figure.study <- plot_risk(df_data[],
                            df_compressed[],
                            xmin=0,
                            ymin=0,
                            xmax=50,
                            ymax=3,
                            color.line = "sex",
                            color.var="study",
                            show.legend = T)
  
  figure.study <- figure.study +
    ggtitle(paste(cause)) +
    guides(color=F)+
    xlab(metric) +
    ylab("RR") +
    ggtitle(cause) 
  
  print_pdf(figure.study, save.path = paste0(outputs, "/rr_summary.pdf"))
  figure.study.facet <- figure.study + facet_wrap(~study)
  print_pdf(figure.study.facet, save.path = paste0(outputs, "/rr_summary_facet.pdf"))
  figure.study
}

#' Expand draws by sex and age group
df_draws <- expand_draws(cause, metric, df_draws)