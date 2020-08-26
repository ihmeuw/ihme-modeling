# launcher for running survival draws in parallel on the cluster
# this will launch one job for each cancer
user <- Sys.info()[["user"]]

source(paste0("FILEPATH/cluster_tools.r"))

# change cancerList for whatever cancer runs being done

cancerList <- c("neo_leukemia_ml_chronic", "neo_leukemia_other", "neo_liver", "neo_lung", "neo_lymphoma", "neo_melanoma")


#c("neo_brain", "neo_colorectal", "neo_esophageal", "neo_gallbladder",  
#                "neo_hodgkins", "neo_kidney", "neo_larynx", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic",
#                "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other",  "neo_meso", "neo_mouth", "neo_myeloma", "neo_nasopharynx",
#                "neo_other_cancer", "neo_otherpharynx", "neo_pancreas", "neo_stomach", "neo_thyroid", "neo_prostate", 
#                "neo_testicular", "neo_cervical", "neo_ovarian", "neo_uterine")

#c('neo_bone','neo_eye','neo_eye_other','neo_eye_rb','neo_liver_hbl', 'neo_lymphoma_burkitt','neo_lymphoma_other','neo_neuro','neo_liver_hbl','neo_tissue_sarcoma') 

#c("neo_hodgkins", "neo_kidney", "neo_larynx", "neo_leukemia_ll_acute", "neo_leukemia_ll_chronic",
##                "neo_leukemia_ml_acute", "neo_leukemia_ml_chronic", "neo_leukemia_other", "neo_liver", "neo_lung",
 #               "neo_lymphoma", "neo_melanoma", "neo_meso", "neo_mouth", "neo_myeloma", "neo_nasopharynx",
 #               "neo_other_cancer", "neo_otherpharynx", "neo_pancreas", "neo_stomach", "neo_thyroid", "neo_prostate", 
 #               "neo_testicular", "neo_cervical", "neo_ovarian", "neo_uterine")
    
for(cancer in cancerList){
    launch_jobs(paste0("FILEPATH/generateSurvivalDraws_worker.R"),
                script_arguments = cancer, memory_request = 60, 
                time = "1:00:00", num_threads = 45,
                output_path = "FILEPATH/rel_surv_jobs/",
                error_path = "FILEPATH/rel_surv_jobs/",
                job_header = paste0("surv_", cancer)) 
}