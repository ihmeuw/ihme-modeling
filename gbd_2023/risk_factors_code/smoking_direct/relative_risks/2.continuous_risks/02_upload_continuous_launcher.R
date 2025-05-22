# upload the RR mr-brt files
rm(list = ls())
source(FILEPATH)

results_folder <- FILEPATH
ARCHIVE <- FILEPATH
FILEPATH <- FILEPATH

pairs_3knots <- c("colon_and_rectum_cancer",
                  "copd",
                  "diabetes",
                  "nasopharyngeal_cancer",
                  "prostate_cancer",
                  "stroke",
                  "tb")

pair_info <- list(
  smoking_lung_cancer = list(
    rei_id = "99",
    cause_id = "426",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "lung_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "lung_cancer", "_knots_3", ".pkl")
  ),
  smoking_copd = list(
    rei_id = "99",
    cause_id = "509",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "copd", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "copd", "_knots_3", ".pkl")
  ),
  smoking_ihd = list(
    rei_id = "99",
    cause_id = "493",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "ihd", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "ihd", "_knots_3", ".pkl")
  ),
  smoking_stroke = list(
    rei_id = "99",
    cause_id = "494",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "stroke", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "stroke", "_knots_3", ".pkl")
  ),
  smoking_lri = list(
    rei_id = "99",
    cause_id = "322",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "lri", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "lri", "_knots_3", ".pkl")
  ),
  smoking_tb = list(
    rei_id = "99",
    cause_id = "297",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "tb", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "tb", "_knots_3", ".pkl")
  ),
  smoking_asthma = list(
    rei_id = "99",
    cause_id = "515",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "asthma", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "asthma", "_knots_3", ".pkl")
  ),
  smoking_diabetes = list(
    rei_id = "99",
    cause_id = "587",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "diabetes", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "diabetes", "_knots_3", ".pkl")
  ),
  smoking_afib_and_flutter = list(
    rei_id = "99",
    cause_id = "500",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "afib_and_flutter", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "afib_and_flutter", "_knots_3", ".pkl")
  ),
  smoking_aortic_aneurism = list(
    rei_id = "99",
    cause_id = "501",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "aortic_aneurism", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "aortic_aneurism", "_knots_3", ".pkl")
  ),
  smoking_peripheral_artery_disease = list(
    rei_id = "99",
    cause_id = "502",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "peripheral_artery_disease", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "peripheral_artery_disease", "_knots_3", ".pkl")
  ),
  smoking_alzheimer_other_dementia = list(
    rei_id = "99",
    cause_id = "543",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "alzheimer_other_dementia", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "alzheimer_other_dementia", "_knots_3", ".pkl")
  ),
  smoking_parkinson = list(
    rei_id = "99",
    cause_id = "544",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "parkinson", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "parkinson", "_knots_3", ".pkl")
  ),
  smoking_multiple_sclerosis = list(
    rei_id = "99",
    cause_id = "546",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "multiple_sclerosis", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "multiple_sclerosis", "_knots_3" ,".pkl")
  ),
  smoking_esophageal_cancer = list(
    rei_id = "99",
    cause_id = "411",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "esophageal_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "esophageal_cancer", "_knots_3", ".pkl")
  ),
  smoking_stomach_cancer = list(
    rei_id = "99",
    cause_id = "414",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "stomach_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "stomach_cancer", "_knots_3", ".pkl")
  ),
  smoking_liver_cancer = list(
    rei_id = "99",
    cause_id = "417",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "liver_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "liver_cancer", "_knots_3", ".pkl")
  ),
  smoking_laryngeal_cancer = list(
    rei_id = "99",
    cause_id = "423",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "laryngeal_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "laryngeal_cancer", "_knots_3", ".pkl")
  ),
  smoking_breast_cancer = list(
    rei_id = "99",
    cause_id = "429",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "breast_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "breast_cancer", "_knots_3", ".pkl")
  ),
  smoking_cervical_cancer = list(
    rei_id = "99",
    cause_id = "432",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "cervical_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "cervical_cancer", "_knots_3", ".pkl")
  ),
  smoking_colon_and_rectum_cancer = list(
    rei_id = "99",
    cause_id = "441",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "colon_and_rectum_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "colon_and_rectum_cancer", "_knots_3", ".pkl")
  ),
  smoking_lip_oral_cavity_cancer = list(
    rei_id = "99",
    cause_id = "444",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "lip_oral_cavity_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "lip_oral_cavity_cancer", "_knots_3", ".pkl")
  ),
  smoking_nasopharyngeal_cancer = list(
    rei_id = "99",
    cause_id = "447",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "nasopharyngeal_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "nasopharyngeal_cancer", "_knots_3", ".pkl")
  ),
  smoking_other_pharynx_cancer = list(
    rei_id = "99",
    cause_id = "450",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "other_pharynx_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "other_pharynx_cancer", "_knots_3", ".pkl")
  ),
  smoking_pancreatic_cancer = list(
    rei_id = "99",
    cause_id = "456",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "pancreatic_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "pancreatic_cancer", "_knots_3", ".pkl")
  ),
  smoking_bladder_cancer = list(
    rei_id = "99",
    cause_id = "474",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "bladder_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "bladder_cancer", "_knots_3", ".pkl")
  ),
  smoking_kidney_cancer = list(
    rei_id = "99",
    cause_id = "471",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "kidney_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "kidney_cancer", "_knots_3", ".pkl")
  ),
  smoking_leukemia = list(
    rei_id = "99",
    cause_id = "487",
    risk_unit = "pack-year",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "leukemia", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "leukemia", "_knots_3", ".pkl")
  ),
  smoking_cataracts = list(
    rei_id = "99",
    cause_id = "671",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "cataracts", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "cataracts", "_knots_3", ".pkl")
  ),
  smoking_macular_degeneration = list(
    rei_id = "99",
    cause_id = "672",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "macular_degeneration", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "macular_degeneration", "_knots_3", ".pkl")
  ),
  smoking_gallbladder_diseases = list(
    rei_id = "99",
    cause_id = "534",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "gallbladder_diseases", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "gallbladder_diseases", "_knots_3", ".pkl")
  ),
  smoking_lbp = list(
    rei_id = "99",
    cause_id = "630",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "lbp", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "lbp", "_knots_3", ".pkl")
  ),
  smoking_peptic_ulcer = list(
    rei_id = "99",
    cause_id = "527",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "peptic_ulcer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "peptic_ulcer", "_knots_3", ".pkl")
  ),
  smoking_rheumatoid_arthritis = list(
    rei_id = "99",
    cause_id = "627",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "rheumatoid_arthritis", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "rheumatoid_arthritis", "_knots_3", ".pkl")
  ),
  smoking_prostate_cancer = list(
    rei_id = "99",
    cause_id = "438",
    risk_unit = "cigarettes per day",
    signal_model_path = paste0(FILEPATH, "01_template_pkl_files/", "prostate_cancer", "_knots_3", ".pkl"),
    linear_model_path = paste0(FILEPATH, "04_mixed_effects_pkl_files/", "prostate_cancer", "_knots_3", ".pkl")
  )
)

for (pair in names(pair_info)) {
  print(paste0("upload pair=", pair))
  results_folder <- file.path(ARCHIVE, pair)
  if (!dir.exists(results_folder)) {
    dir.create(results_folder)
  }
  do.call(upload_results, c(pair_info[[pair]], list(results_folder = results_folder)))
}
