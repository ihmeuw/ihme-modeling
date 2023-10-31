import numpy as np

EXPOSURE_TO_ADMISSION = list(range(10, 14))
EXPOSURE_TO_SEROCONVERSION = list(range(14, 18))
ADMISSION_TO_DEATH = list(range(12, 16))


def make_duration_dict(exposure_to_admission: int,
                       exposure_to_seroconversion: int,
                       admission_to_death: int,):
    durations = {
        'exposure_to_case': exposure_to_admission,
        'exposure_to_admission': exposure_to_admission,
        'exposure_to_seroconversion': exposure_to_seroconversion,
        'exposure_to_death': exposure_to_admission + admission_to_death,
        
        'pcr_to_sero': exposure_to_seroconversion - exposure_to_admission,
        'admission_to_sero': exposure_to_seroconversion - exposure_to_admission,
        
        'sero_to_death': (exposure_to_admission + admission_to_death) - exposure_to_seroconversion,
    }
    
    # CASE_TO_DEATH = EXPOSURE_TO_DEATH - EXPOSURE_TO_CASE
    # PCR_TO_SERO = EXPOSURE_TO_SEROPOSITIVE - EXPOSURE_TO_CASE

    # ADMISSION_TO_SERO = EXPOSURE_TO_SEROPOSITIVE - EXPOSURE_TO_ADMISSION
    # ADMISSION_TO_DEATH = EXPOSURE_TO_DEATH - EXPOSURE_TO_ADMISSION

    # SERO_TO_DEATH = EXPOSURE_TO_DEATH - EXPOSURE_TO_SEROPOSITIVE
    
    return durations


def get_duration_dist(n_samples: int):
    durations = [make_duration_dict(eta, ets, atd)
     for n, (eta, ets, atd) in enumerate(zip(
         np.random.choice(EXPOSURE_TO_ADMISSION, size=n_samples).tolist(),
         np.random.choice(EXPOSURE_TO_SEROCONVERSION, size=n_samples).tolist(),
         np.random.choice(ADMISSION_TO_DEATH, size=n_samples).tolist(),
     ))]
    
    return durations
