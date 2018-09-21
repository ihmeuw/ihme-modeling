'''
This part of the code sets up directories to store the draws retrieved from the epi database
and executes the squeeze and save_custom_results code
'''

import os
import shutil
import json
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from job_utils import draws, parsers
from job_utils import getset
import subprocess
from db_queries import get_best_model_versions
from db_queries import get_location_metadata
import pandas as pd
import datetime

cong_heart = {
	"name": {
		"type": "cong_heart"
	},
	"heart failure": {
		"type": "split",
		"srcs": {
			"mild": "2179",
			"moderate": "2180",
			"severe": "2181"
		}
	},
	"env": {
		"type": "envelope",
		"srcs": {
			"tot":"10437",
			"bundle": "628"
		}
	},
	"Critical malformations of great vessels, congenital valvular heart disease and patent ductus arteriosis": {
		"type": "sub_group",
		"srcs": {
			"tot": "10441",
			"bundle": "632"
		},
		"trgs": {
			"sqzd": "10936",
			"none": "15768",
			"mild": "15769",
			"moderate": "15770",
			"severe": "15771"
		}
	},
	"Ventricular septal defect and atrial septal defect": {
		"type": "sub_group",
		"srcs": {
			"tot": "10443",
			"bundle": "634"
		},
		"trgs": {
			"sqzd": "10937",
			"none": "15773",
			"mild": "15774",
			"moderate": "15775",
			"severe": "15776",
			"asymptomatic": "11392"
		}
	},
	"Single ventricle and single ventricle pathway heart defects": {
		"type": "sub_group",
		"srcs": {
			"tot": "10439",
			"bundle": "630"
		},
		"trgs": {
			"sqzd": "10935",
			"none": "15760",
			"mild": "15761",
			"moderate": "15762",
			"severe": "15763"
		}
	},
	"Severe congenital heart defects excluding single ventricle and single ventricle pathway": {
		"type": "sub_group",
		"srcs": {
			"tot": "10445"
		},
		"trgs": {
			"sqzd": "10938",
			"none": "15764",
			"mild": "15765",
			"moderate": "15766",
			"severe": "15767"
		}
	},
	"Other": {
		"type": "other",
		"srcs": {
			"tot": "10934",
			"bundle": "808"
		},
		"trgs": {
			"sqzd": "10939",
			"none": "15756"
		}
	}	
}

cong_neural = {
	"name": {
		"type": "cong_neural"
	},
	"env": {
		"type": "envelope",
		"srcs": {
			"tot": "10415",
			"bundle": "608"
		}
	},
	"Spina Bifida": {
		"type": "sub_group",
		"srcs": {
			"tot": "10421",
			"bundle": "614"
		},
		"trgs": {
          "sqzd": "10940"
        }
	},
	"Anencephaly": {
		"type": "sub_group",
		"srcs": {
			"tot": "10417",
			"bundle": "610"
		},
		"trgs": {
          "sqzd": "10942"
        }
	},
	"Encephalocele": {
		"type": "sub_group",
		"srcs": {
			"tot": "10419",
			"bundle": "612"
		},
		"trgs": {
          "sqzd": "10941"
        }
	}
}
cong_msk = {
	"name": {
		"type": "cong_msk"
	},
	"env": {
		"type": "envelope",
		"srcs": {
			"tot": "10407",
			"bundle": "602"
		}
	},
	"Limb reduction deficits": {
		"type": "sub_group",
		"srcs": {
			"tot": "10409",
			"bundle": "604"
		},
		"trgs": {
          "sqzd": "10926"
        }
	},
	"Polydactyly and syndactyly": {
		"type": "sub_group",
		"srcs": {
			"tot": "10411",
			"bundle": "606"
		},
		"trgs": {
          "sqzd": "10927"
        }
	},
	"Other": {
		"type": "other",
		"srcs": {
			"tot": "10925",
			"bundle": "799"
		},
		"trgs": {
          "sqzd": "10928"
        }
	}
}

cong_digestive = {
	"name": {
		"type": "cong_digestive"
	},
	"env": {
		"type": "envelope",
		"srcs": {
			"tot": "10427",
			"bundle": "620"
		}
	},
	"Congenital diaphragmatic hernia": {
		"type": "sub_group",
		"srcs": {
			"tot": "10429",
			"bundle": "622"
		},
		"trgs": {
          "sqzd": "10930"
        }
	},
	"Congenital atresia and/or stenosis of the digestive tract": {
		"type": "sub_group",
		"srcs": {
			"tot": "10433",
			"bundle": "626"
		},
		"trgs": {
          "sqzd": "10932"
        }
	},
	"Congenital malformations of the abdominal wall": {
		"type": "sub_group",
		"srcs": {
			"tot": "10431",
			"bundle": "624"
		},
		"trgs": {
          "sqzd": "10931"
        }
	},
	"Other": {
		"type": "other",
		"srcs": {
			"tot": "10929",
			"bundle": "803"
		},
		"trgs": {
          "sqzd": "10933"
        }
	}
}


root = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")


for cause in [cong_heart, cong_msk, cong_digestive, cong_neural]:

	me_map = cause
	cause_name = me_map['name']['type']
	
	# make server directory
	# note that this directory gets written over every time the code is run
	out_dir = "{root}/{proc}".format(root=root, proc=cause_name)
	if not os.path.exists(out_dir):
		os.makedirs(out_dir)
		os.makedirs(os.path.join(out_dir, 'graphs'))
	else:
		shutil.rmtree(out_dir)
		os.makedirs(out_dir)
		os.makedirs(os.path.join(out_dir, 'graphs'))
	
	# make output directories
	save_ids = []
	for _, v in me_map.items():
		outputs = v.get("trgs",{})
		for trg_key, me_id in outputs.items():
			os.makedirs(os.path.join(out_dir, str(me_id)))
			save_ids.append(me_id)

	# submit squeeze job for each cause and form a string of job names
	job_string = ''
	for i in [1990, 1995, 2000, 2005, 2010, 2016]: 
		year_id = i
		job_name = "squeeze_{0}_{1}".format(year_id, cause_name)
		job_string = job_string + ',' + job_name
		call = ('qsub -l mem_free=20.0G -pe multi_slot 10'
					' -cwd -P proj_custom_models'
					' -o {FILEPATH}'
					' -e {FILEPATH} -N {4}'
					' cluster_shell.sh squeeze.py \'{0}\' {1} {2} {3}'.format(json.dumps(me_map), out_dir, year_id, cause_name,job_name))
		#print call
		subprocess.call(call, shell=True)

	# get location_metadata for graphing step
	loc_df = get_location_metadata(location_set_id=35, gbd_round_id=4)
	loc_df.to_csv(os.path.join(out_dir, 'graphs', 'location_metadata.csv'), encoding='utf-8')

	# graph
	# need-hold_jid + job_string flag to hold jobs until squeezes are done
	# only graphing three estimation years now
	for i in [1990, 2005, 2016]: 
		year_id = i
		#job_string= "no_holds"
		call = ('qsub  -hold_jid {2} -cwd -P proj_custom_models '
					' -o {FILEPATH}'
					' -e {FILEPATH} -N {0}_graph_{1}'
					' r_shell.sh congenital_stacked_bar.R {0} {1}'.format(cause_name, year_id, job_string))
		
		subprocess.call(call, shell=True)
	
	# get version ids of models used in squeeze calculations
	# upload this information in the save description
	model_version_list = []
	for mapper_key, v in me_map.items():
		outputs = v.get("srcs",{})
		for src_key, me_id in outputs.items():
			if src_key == "tot" and mapper_key != "Other":
				model_version_list.append(me_id)
	#try to only query the database once
	df = get_best_model_versions(entity='modelable_entity',ids=model_version_list, gbd_round_id=4)
	model_ids_str_for_save = ", ".join(str(e) for e in df['model_version_id'].tolist())
	if cause_name == "cong_heart":
		model_ids_str_for_save += ", Heart failure me_ids best as of {0}".format(str(datetime.date.today()))
	
	# save custom results with hold_jid
	for save_id in save_ids:
		#need-hold_jid + job_string flag to hold save jobs until squeezes are done
		#check to see if the share paths exists before executing
		#job_string= "no_holds"
		call = ('qsub  -hold_jid {3} -l mem_free=80.0G -pe multi_slot 40' 
					' -cwd -P proj_custom_models -o'
		            ' -o {FILEPATH}'
		            ' -e {FILEPATH} -N {1}_save_{0}'
		            ' cluster_shell.sh save_squeeze.py {0} {1} \'{2}\''.format(save_id, cause_name, model_ids_str_for_save, job_string))

		subprocess.call(call, shell=True)
