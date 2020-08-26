import getpass

UHC_DIR = FILEPATH
ID_COLS = ['location_id', 'year_id']
DRAW_COLS = ['draw_' + str(d) for d in range(1000)]
YEAR_ID = [1990, 2010, 2019]

POPULATION_AGE = {
    'Reproductive and new born':[2, 3] + list(range(7, 16)),
    '<5':[2, 3, 4, 5],
    '5-19':[6, 7, 8],
    '20-64':[9, 10, 11, 12, 13, 14, 15, 16, 17],
    '65+':[18, 19, 20, 30, 31, 32, 235],
    'All ages': list(range(2, 21)) + [30, 31, 32, 235],
    '1-4': [5]
}

################################################################################

RESCALE = ['cfr', 'log_cfr', 'outputs', 'mmr', 'birth_mort', 'risk_standardized_mortality']

LOG_SCALE = ['log_cfr', 'outputs', 'mmr', 'birth_mort', 'risk_standardized_mortality']

NOT_USED = [
    # 'EC of HPV immunization',
    # 'EC of rehabilitation after complex injuries',
    # 'EC treatment for substance abuse',
    # 'EC treatment mental health disorders',
    # 'EC cataract surgery',
    'EC of palliation',
    # 'EC of WHO recommended child immunization (HepB)',
    # 'EC of WHO recommended child immunization (Rota)',
    # 'EC of WHO recommended child immunization (Hib3)',
    # 'EC of tuberculosis treatment',
    # 'EC of ARV treatment',
    'EC of elevated blood pressure management',
    'EC of diabetes managment',
    'EC of preventive chemotherapy for schistosomiasis',
    'EC of preventive chemotherapy for lymphatic filariasis',
    'EC breastfeeding promotion',
    'EC of refractive error correction',
    'EC of treatment for congenital heart disease',
    'EC treatment of testicular cancer',
    'EC treatment of hypertensive heart disease',
    'EC of WHO recommended child immunization (PCV3)',
    'EC of WHO recommended child immunization (HepB)',
    'EC of WHO recommended child immunization (Rota)',
    'EC of WHO recommended child immunization (Hib3)',
    'EC for treatment of child wasting',
    'EC of vector control for malaria',
    # 'EC treatment of epilepsy',
    'EC for hodgkin lymphoma',
    'EC for childhood leukemias'
    # 'EC treatment of IHD'
    # 'EC of treatment for severe hip osteoarthritis',
    # 'EC of preventive chemotherapy for NTDs',
    # 'EC antenatal care',
    # 'EC management of labor and delivery',
    # 'EC treatment of edentulism'
]

PROXIES = {
    # 'covariates':{
    # #     'EC antenatal care':{
    # #         'covariate_id':8, 'gbd_id':[366], 'gbd_id_type':['cause_id']
    # #     },
    # #     # 'EC management of labor and delivery':{
    # #     #     'covariate_id':51, 'gbd_id':[366], 'gbd_id_type':['cause_id']
    # #     # },
    #     # 'EC skilled birth attendance':{
    #     #     'covariate_id':143, 'gbd_id':[366], 'gbd_id_type':['cause_id']
    #     # }
    # },
    'mmr':{
        ##################################################################
        ###################### COVERAGE LOG RESCALE ######################
        ##################################################################
        # We apply 50% of maternal DALYs to anc for the mother and the other half to contraception
        'EC antenatal, postpartum and postnatal care for the mother':{
            'gbd_id':[366], 'gbd_id_type':['cause_id'], 'measure_id':1, 'split_maternal': True
        }
    },
    'art':{
        'EC of ARV treatment':{
            'gbd_id':[298], 'gbd_id_type':['cause_id'], 'measure_id':5
        }
    },
    'vaccines':{
        'EC of WHO recommended child immunization (DTP3)':{
            'path': FILEPATH,
            'gbd_id':([338, 339, 340],), 'gbd_id_type':(['cause_id', 'cause_id', 'cause_id'],)
        },
        'EC of WHO recommended child immunization (MCV1)':{
            'path': FILEPATH, 
            'gbd_id':([341],), 'gbd_id_type':(['cause_id'],)
        }
        # 'EC of WHO recommended child immunization (PCV3)':{
        #     'path': FILEPATH,
        #     'gbd_id':([188, 294],), 'gbd_id_type':(['rei_id', 'cause_id'],)
        # },
        # 'EC of WHO recommended child immunization (HepB)':{
        #     'path': FILEPATH, 
        #     'gbd_id':([402, 418, 522],), 
        #     'gbd_id_type':(['cause_id', 'cause_id', 'cause_id'],)
        # },
        # 'EC of WHO recommended child immunization (Rota)':{
        #     'path': FILEPATH, 
        #     'gbd_id':([181, 294],), 
        #     'gbd_id_type':(['rei_id', 'cause_id'],)
        # },
        # 'EC of WHO recommended child immunization (Hib3)':{
        #     'path': FILEPATH,
        #     'gbd_id':([189, 294],),
        #     'gbd_id_type':(['rei_id', 'cause_id'],)
        # }
    },
    # 'outputs':{
    #     ##################################################################
    #     ###################### COVERAGE LOG RESCALE ######################
    #     ##################################################################
    #     # 'EC dental care':{
    #     #     'gbd_id':[682], 'gbd_id_type':['cause_id'], 'measure_id':5
    #     # },
    #     # 'EC treatment of edentulism':{
    #     #     'gbd_id':[684], 'gbd_id_type':['cause_id'], 'measure_id':5
    #     # },
    #     # 'EC of refractive error correction':{
    #     #     'gbd_id':[999], 'gbd_id_type':['cause_id'], 'measure_id':5
    #     # }
    # },
    'cfr':{
        ##################################################################
        #################### COVERAGE NORMAL RESCALE #####################
        ##################################################################
        ## M:I
        'EC of treatment for diarrohea':{
            'gbd_id':[302], 'gbd_id_type':['cause_id'], 'measure_id':6
        },
        'EC of treatment for lower respiratory infections':{
            'gbd_id':[322], 'gbd_id_type':['cause_id'], 'measure_id':6
        },
        'EC surgical care for appendicitis':{
            'gbd_id':[529],
            'gbd_id_type':['cause_id'],
            'measure_id':6
        },
        'EC surgical care for paralytic ileus and intestinal obstruction':{
            'gbd_id':[530],
            'gbd_id_type':['cause_id'],
            'measure_id':6
        },      
        'EC treatment of breast cancer':{
            'gbd_id':[429],
            'gbd_id_type':['cause_id'],
            'measure_id':6
        },  
        'EC treatment of cervical cancer':{
            'gbd_id':[432],
            'gbd_id_type':['cause_id'],
            'measure_id':6
        },      
        'EC treatment of uterine cancer':{
            'gbd_id':[435],
            'gbd_id_type':['cause_id'],
            'measure_id':6
        },
        'EC treatment of colon/rectum cancer':{
            'gbd_id':[441],
            'gbd_id_type':['cause_id'],
            'measure_id':6
        },
        # 'EC treatment of testicular cancer':{
        #     'gbd_id':[468],
        #     'gbd_id_type':['cause_id'],
        #     'measure_id':6
        # },                
        # 'EC for childhood leukemias':{
        #     'gbd_id':[487], 'gbd_id_type':['cause_id'],'measure_id':6
        # },
        'EC for acute lymphoid leukemia':{
            'gbd_id':[845], 'gbd_id_type':['cause_id'],'measure_id':6
        },
        # 'EC for hodgkin lymphoma':{
        #     'gbd_id':[484], 'gbd_id_type':['cause_id'],'measure_id':6
        # },  
        'EC of tuberculosis treatment':{
            'gbd_id':[297, 948, 949, 950],
            'gbd_id_type':['cause_id', 'cause_id', 'cause_id', 'cause_id'],
            'measure_id':6
        }
    },
    'log_cfr':{
        ##################################################################
        ###################### COVERAGE LOG RESCALE ######################
        ##################################################################
        ## M:P
        # 'EC of treatment for congenital heart disease':{
        #     'gbd_id':[643], 'gbd_id_type':['cause_id'], 'measure_id':5
        # },
        'EC treatment of stroke':{
            'gbd_id':[494], 'gbd_id_type':['cause_id'], 'measure_id':5
        },
        'EC treatment of chronic kidney disease':{
            'gbd_id':[589], 'gbd_id_type':['cause_id'], 'measure_id':5
        },
        # 'EC treatment of hypertensive heart disease':{
        #     'gbd_id':[498], 'gbd_id_type':['cause_id'], 'measure_id':5
        # },
        'EC treatment of epilepsy':{
            'gbd_id':[545], 'gbd_id_type':['cause_id'], 'measure_id':5
        },
        'EC treatment of asthma':{
            'gbd_id':[515], 'gbd_id_type':['cause_id'], 'measure_id':5
        },
        'EC treatment of chronic obstructive pulmonary disease':{
            'gbd_id':[509], 'gbd_id_type':['cause_id'], 'measure_id':5
        },
        'EC treatment of diabetes':{
            'gbd_id':[587], 'gbd_id_type':['cause_id'], 'measure_id':5
        }
    },
    'birth_mort':{
        ##################################################################
        ###################### COVERAGE LOG RESCALE ######################
        ##################################################################
        # 'EC prevention of stillbirths':{
        #     'file':FILEPATH
        # },
        'EC antenatal, peripartum and postnatal care for the newborn':{
            'file':FILEPATH
        }
    },
    # 'wasting':{
    #     'EC for treatment of child wasting':{
    #         'path': FILEPATH
    #         'gbd_id':[240, 294],
    #         'gbd_id_type':['rei_id', 'cause_id']
    #     },
    # },
    'disk':{
        # We apply 50% of maternal DALYs to anc for the mother and the other half to contraception
        'EC modern contraception':{
            'file':FILEPATH, 
            'gbd_id':[366], 'gbd_id_type':['cause_id'], 'split_maternal': True
        }
        # 'EC breastfeeding promotion':{
        #     'path':'FILEPATH',
        #     'gbd_id':[93, 294], 'gbd_id_type':['rei_id', 'cause_id'], 'age_group_id':[3, 4]
        # },
        # 'EC of vector control for malaria':{
        #     'file':'FILEPATH',
        #     'gbd_id':[345], 'gbd_id_type':['cause_id']
        # },
        # 'EC of elevated blood pressure management':{
        #     'path':'FILEPATH',
        #     'gbd_id':[107, 294],
        #     'gbd_id_type':['rei_id', 'cause_id']
        # },
        # 'EC of diabetes managment':{
        #     'path':'FILEPATH',
        #     'gbd_id':[105, 294],
        #     'gbd_id_type':['rei_id', 'cause_id']
        # },
        # 'EC of preventive chemotherapy for schistosomiasis':{
        #     'file':'FILEPATH',
        #     'gbd_id':[351], 'gbd_id_type':['cause_id']
        # },
        # 'EC of preventive chemotherapy for lymphatic filariasis':{
        #     'file':'FILEPATH',
        #     'gbd_id':[354], 'gbd_id_type':['cause_id']
        # }
    },
    'risk_standardized_mortality':{
        'EC treatment of IHD':{
            'gbd_id':[493], 'gbd_id_type':['cause_id'], 'measure_id':6
        }
    }
}

SPLIT_OVERLAP = {
    # 'EC skilled birth attendance':0.5,
    # 'EC antenatal, postpartum and postnatal care for the mother':0.5
}

PAF_DELETION = {
    # 'EC treatment of stroke':[105, 107],
    # 'EC treatment of IHD':[105, 107],
    # 'EC treatment of end-stage renal disease':[105, 107],
    # 'EC of treatment for lower respiratory infections':[188],
    # 'EC of treatment for diarrohea':[181]
}