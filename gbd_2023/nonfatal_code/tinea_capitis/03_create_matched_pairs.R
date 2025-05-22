## Boilerplate
library(metafor)
library(msm)
library(plyr)
library(dplyr)
library(boot)
library(ggplot2)
library(openxlsx)
library(readxl)
library(reticulate)


library(dplyr)
library(reticulate)
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")
"%ni%"<- Negate("%in%")
source('FILEPATH')
source("FILEPATH")

locs <- as.data.table(get_location_metadata(location_set_id = 35, release_id=16))

# Read in data
df_orig <- read.csv(paste0(modeling_dir, "FILEPATH",cause_name,".csv"))

# Claims, MarketScan and Poland data:

# Poland Locations:
poland_locs <- locs$location_id[locs$parent_id==51]

# Marketscan locations:
marketscan_locs <- locs$location_id[locs$parent_id==102]

#drop all claims and marketscan:
df_orig<- df_orig[df_orig$location_id %ni% c(marketscan_locs,poland_locs), ]

names(df_orig)
df_orig <- df_orig %>% select(-c(starts_with("cv")))

# select dr diagnosis:
unique(df_orig$case_diagnostics)

dr_diagnosis<- c("positive by KOH and/or culture",
                 "fully clinically examined for skin diseases by two dermatologists (A.A.H. A.P.M.L.). Specific attention was focused on clinical signs of fungal infection on the scalp (scaling hair loss black dots chicken skin effect pustules and scarring alopecia). Samples from scales and hairs were taken by gentle brushing with glass slides and tweezers (Fig. 1) and transported at room temperature to the Mycology Laboratory of the Department of Dermatology of the Leiden University Medical Centre in Leiden the Netherlands. The samples can be kept at room temperature for several months (and probably longer) without affecting the viability of the fungi.6 The specimens were examined by direct microscopic examination using 20% potassium hydroxide (KOH) solution. For the cultures modified Sabouraud s dextrose agar with chloramphenicol was used and the specimens were incubated for 28 days at a temperature of 28 C. Species identification was based on growth rate macroscopic aspect and microscopic examination.",
                 "At each village a suitable outdoor central location was selected. Screens were erected so that a complete skin examination could be conducted in privacy. All individuals were examined simultaneously by two dermatologists one of whom was Nepali. To ensure that skin examinations were as complete as possible individuals were examined by dermatologists of the same gender. Individuals were free to refuse examination at all stages of the proceedings and genitals were examined only if an individual was willing and the examining dermatologists regarded it as necessary. All diagnoses were made clinically. Where there was disagreement between a pair of examiners a third dermatologist examined the individual concerned and the majority opinion prevailed. Three of the four dermatologists (S.L.W. M.S. H.M.P.) involved in the study have wide experience of managing dermatology and leprosy patients in Nepal.",
                 "Dermatological diagnosis was made mainly clinically. Laboratory investigations were used to confirm difficult diagnoses. Skin scrapings nail clippings and hairs were obtained as appropriate and treated for 15-30 minutes with 1-2 drops of 20% KOH before being examined microscopically for fungal hyphae. Fungal cultures were done on SabouraudÃ¢â‚¬â„¢s Dextrose Agar and considered positive if there was growth on day 14 or 28 and negative if there was no growth on day 28. Pus swabs were collected for gram stain and cultures were done on Blood and MacConkey Agar. The bacterial cultures were considered negative if there was no growth after 48 hours of incubation. Specimens were obtained from burrows of suspected scabies lesions for direct microcopy for mites. Skin biopsies were taken as appropriate for routine histological examination. There were no special tests done to confirm viral lesions.",
                 "the diagnosis of the dermatologic condition was made based on a detailed review of history clinical features complete physical examination including skin and necessary tests such as gram's stain potassium hydroxide examination tzanck test hematology and biochemistry analysis urine analysis wood's lamp examination diascopy purified protein derivative (tuberculin) chest x-ray skin biopsy and other investigations as needed.",
                 "physical examination by at least one dermatologist",
                 "all children were submitted to a careful examination of the scalp conducted by a team of physicians. children with lesions that were clinically indicative of possible tinea capitis were enrolled for further participation. samples for mycological examination were taken from these children. each sample was subjected to direct microscopic examination using 30% potassium hydroxide (koh) solution and cultivation on sabouraud's dextrose agar supplemented with 0.5 g/l chloramphenicol and 0.4 g/l cycloheximide.",
                 "infected hair and scalp scrapings were collected in sampling packets and transferred to the centre de biologie clinique (institut pasteur de madagascar antananarivo). hairs were examined under wood's ultraviolet light. direct microscopy slides were prepared by using potassium hydroxide (koh) for scalp scrapings and hair samples were then seeded on two separate plates of sabouraud's dextrose agar media containing chloramphenicol one with and the other without cycloheximide. the agar plates were incubated at a temperature of 27Â°c for four weeks. isolated dermatophytes were identified based on growth rate microscopic morphology of slide cultures and production and potential diffusion of pigment on potato dextrose agar as well as on baxter media (difco nancy france) and borelli (lactrimel) media (angers france).media containing chloramphenicol one with and the other without cycloheximide. the agar plates were incubated at a temperature of 27Â°c for four weeks. isolated dermatophytes were identified based on growth rate microscopic morphology of slide cultures and production and potential diffusion of pigment on potato dextrose agar as well as on baxter media (difco nancy france) and borelli (lactrimel) media (angers france).",
                 "Interviewer administered questionnaire\nwas used to obtain socio-demographic and\nclinical data from all children who had written\nparental consent. Exclusion criteria were nonconsenting\nparents and non-assenting pupils.\nThe socioeconomic status of the children was\ndetermined by parentsâ€™ occupation, level of\neducation and living conditions. \nPhysical examination of each student was\ncarried out in a well-lit room with adequate\nventilation. The scalp and skin were examined\nfor dermatophyte infections, complications of\nthe infection and any other dermatoses. The\nclinical diagnosis of tinea capitis was made\nusing the following criteria: scaly patches on\nthe scalp, with or without hair loss; partial hair\nloss with broken-off hairs, brittle and lustreless\nhair strands, annular lesions with fairly\nsharp margins, massive scaling, folliculitis,\nkerion and favus.\nThose with a clinical diagnosis of tinea capitis\nhad scalp and hair scrapings for microscopy\nand culture to confirm the diagnosis. The\nlesions were cleaned with 70% ethyl alcohol;\nthis is aimed at clearing out the bacterial flora\nand etiological bacteria of possible super infection.\nSkin scrapings were collected into white\nenvelopes using the blunt edge of a sterile surgical\nscalpel blade (one for each child) from\nthe erythematous, peripheral actively growing\nmargins of the lesion. Hair samples were the\ndull, lustreless hair strands and the stubs were chosen and plucked using sterile forceps. The\nenvelopes were sealed with office clips and\ntransported to the mycology laboratory for\ndirect microscopy and culture. All patients with\ntinea capitis were treated with oral griseofulvin\ntablets and topical antifungal agent"
                 )

df_orig$cv_dr_diagnosis<- ifelse(df_orig$case_diagnostics %in% dr_diagnosis, 1, 0)
sum(df_orig$cv_dr_diagnosis)
unique(df_orig$age_start[df_orig$cv_dr_diagnosis==1])

nrow(df_orig[df_orig$is_outlier==0,])
nrow(df_orig[df_orig$cv_dr_diagnosis==0,])
nrow(df_orig[df_orig$cv_dr_diagnosis==1,])

names(df_orig)
unique(df_orig$source_type)
unique(df_orig$sampling_type)
unique(df_orig$case_name)
unique(df_orig$case_definition)
unique(df_orig$input_type)
unique(df_orig$case_diagnostics)

nrow(df_orig[df_orig$case_diagnostics=="physical examination by at least one dermatologist",])
nrow(df_orig[df_orig$case_diagnostics=="dermatologist diagnosis" ,])
nrow(df_orig[df_orig$case_diagnostics=="One dermatologist and three registrars in the dermatology unit carried out the physical examinations." ,])

# Matched pairs
df_orig$cv_not_dr_diagnosis <- ifelse(df_orig$cv_dr_diagnosis!=1,1,0)
df_orig$is_reference <- ifelse(df_orig$cv_dr_diagnosis==1,1,0)
df_orig$cv_diag_pcr <- 0 # add dummy pcr col to work with the crosswalk matched pairs code
df_orig$rownum <- 1:nrow(df_orig)

age_bins <- c(1,2,5,10,20,40,60,80,100)
df_matched <- bundle_crosswalk_collapse(df_orig,
                                        reference_name="is_reference",
                                        covariate_name="cv_not_dr_diagnosis",
                                        age_cut=age_bins,
                                        year_cut=c(seq(1972,2015,5),2020),
                                        merge_type="between",
                                        include_logit = T,
                                        location_match = "global",
                                        release_id=16)

write.csv(df_matched, paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"), row.names=F)
write.csv(df_orig, paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"), row.names=F)
