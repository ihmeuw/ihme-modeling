import os
import shutil
import pandas as pd
import subprocess
import json
from job_utils import getset
from db_queries import get_best_model_versions
from gbd_artifacts.severity_prop import SeverityPropMetadata
import time

# TODO: Change the username and/or root path so that the person running the 
# code has write and read privileges to the output directories
username = "USERNAME"
root = "FILEPATH"
error_path = "FILEPATH"
output_path = "FILEPATH"

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)

# descriptions
sequence = "SEQUENCE NUMBER"
sev_split_description = "DESCRIPTION".format(sequence)
env_description = "DESCRIPTION".format(sequence)
pid_split_description = "DESCRIPTION".format(sequence)
female_attr_description = "DESCRIPTION".format(sequence)
male_attr_description = "DESCRIPTION".format(sequence)
excess_description = "DESCRIPTION".format(sequence)


class SevSplit(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):
        # compile submission arguments
        global sev_split_description
        
        # To start the pipeline off, we need to apply severity splits to
        # 3248 Klinefelter syndrome
        # 3247 Turner syndrome
        # 10425 Congenital genital anomalies
        # 2073 Endometriosis
        # 2068 PCOS
        # The split database assigns split ids to aid with version control

        split_map = {
            'klinefelter': {
                'split_id': 140,
                'parent_id': 18754,
                'child_ids': [3070, 2213, 2659, 2214, 2218, 3069],
                'sex_restrict': [1],
                'measure_id': [5]
                },
            'turner': {
                'split_id': 236,
                'parent_id': 18753,
                'child_ids': [3021, 2207, 2653, 2208],
                'sex_restrict':[2],
                'measure_id': [5]
                },
            'urogen': {
                'split_id': 458,
                'parent_id': 10425,
                'child_ids': [11031, 11032, 11033, 11034, 11035, 11036, 11037, 
                                11038, 11039, 11040, 11041, 11042, 11043, 
                                11044, 11045, 11046],
                'sex_restrict': [1,2],
                'measure_id': [5]
            },
            'endo': {
                'split_id': 404,
                'parent_id': 2073,
                'child_ids': [2074, 2075, 3089, 2889, 9672, 9673, 9674, 9671],
                'sex_restrict': [2],
                'measure_id': [5,6]
                },
            'pcos': {
                'split_id': 401,
                'parent_id': 2068,
                'child_ids': [2936, 2937, 9675, 9676],
                'sex_restrict': [2],
                'measure_id': [5,6]
                }
            }

        # Get best models versions of parent_ids for save step
        parent_meids = []
        for identity, nested_dict in split_map.items():
            parent_meids.append(nested_dict['parent_id'])
        mvid_df = get_best_model_versions(entity='modelable_entity',
            ids=parent_meids, gbd_round_id=5)

        for identity, nested_dict in split_map.items():
            split_id = nested_dict['split_id']
            split_meta = SeverityPropMetadata(split_id=split_id)
            split_version_id = split_meta.best_version
            meta_version = split_meta.get_metadata_version(split_version_id)
            parent_id = int(meta_version.parent_meid())
            assert parent_id == nested_dict['parent_id']
            
            # make server directory
            directory = os.path.join(root, 'sev_split', str(parent_id))

            if os.path.exists(directory):
                shutil.rmtree(directory)
                os.makedirs(directory)
            else:
                os.makedirs(directory)

            split_params = [
                "--split_version_id", str(split_version_id),
                "--output_dir", directory]

            sev_split = 'split_{}'.format(parent_id)
            call = ('qsub -hold_jid {hj} -pe multi_slot 15'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' cluster_shell.sh'
                ' sev_splits.py'
                ' {arg1}'.format(hj=self.hold,
                    o=output_path,
                    e=error_path,
                    jn=sev_split,
                    arg1=' '.join(split_params)))
            subprocess.call(call, shell=True)

            # save the results
            hold = sev_split
            pmvid = mvid_df.loc[mvid_df.modelable_entity_id==parent_id, 'model_version_id'].item()
            description = sev_split_description + ". Used model_version_id {} for parent modelable_entity_id {}.".format(pmvid, parent_id)

            for save_id in nested_dict['child_ids']:
                save_params = [
                    str(save_id),
                    "--description", "\'{}\'".format(description),
                    "--input_dir", os.path.join(directory, str(save_id)),
                    "--best",
                    "--sexes",
                    " ".join([str(x) for x in nested_dict['sex_restrict']]),
                    "--meas_ids", 
                    " ".join([str(x) for x in nested_dict["measure_id"]]),
                    "--file_pattern", "{location_id}.h5"]

                job_name = 'split_save_{}'.format(save_id)
                self.save_hold = self.save_hold + ',' + job_name
                call = ('qsub -hold_jid {hj}'
                        ' -pe multi_slot 15'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' save.py'
                        ' {arg1}'.format(hj=hold,
                            o=output_path,
                            e=error_path,
                            jn=job_name,
                            arg1=' '.join(save_params)))
                subprocess.call(call, shell=True)
        return self.save_hold



class PID(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):

        # compile submission arguments
        pid_env_id = 20419
        chlam_prop_id = 2520
        gono_prop_id = 2521
        other_prop_id = 2522
        chlam_id =  2931
        gono_id = 2932
        other_id = 2933

        # make server directory
        directory = "{root}/{proc}".format(root=root, proc="pid_split")

        if os.path.exists(directory):
            shutil.rmtree(directory)
            os.makedirs(directory)
        else:
            os.makedirs(directory)
        
        split_params = [
            str(pid_env_id),
            "--target_meids", str(chlam_id), str(gono_id), str(other_id),
            "--prop_meids", str(chlam_prop_id), str(gono_prop_id), str(other_prop_id),
            "--split_meas_ids", "5", "6",
            "--prop_meas_id", "18",
            "--output_dir", directory]

        pid_split = "pid_split"

        call = ('qsub -hold_jid {hj} -pe multi_slot 15'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' cluster_shell.sh'
                ' epi_splits_pid.py'
                ' {arg1}'.format(hj=self.hold,
                    o=output_path,
                    e=error_path,
                    jn=pid_split, 
                    arg1=' '.join(split_params)))
        subprocess.call(call, shell=True)

        # save the results
        hold = 'no_holds'
        for save_id in [chlam_id, gono_id, other_id]:

            save_params = [
                str(save_id), 
                "--description", "\'{}\'".format(pid_split_description),
                "--input_dir", os.path.join(directory, str(save_id)), 
                "--best",
                "--sexes", "2",
                "--meas_ids", "5", "6",
                "--file_pattern", "{location_id}.h5"]

            job_name = 'pid_save_{}'.format(save_id)
            self.save_hold = self.save_hold + ',' + job_name
            call = ('qsub -hold_jid {hj} -pe multi_slot 15'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' cluster_shell.sh'
                ' save.py'
                ' {arg1}'.format(hj=hold,
                    o=output_path,
                    e=error_path,
                    jn=job_name,
                    arg1=' '.join(save_params)))
            subprocess.call(call, shell=True)
        return self.save_hold

class Westrom(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):

        me_map = {
          "summarize_gonn_inc": {
            "kwargs": {
              "source_me_id": "2932",
              "target_me_id": "3023"
            },
          },
          "summarize_chlam_inc": {
            "kwargs": {
              "source_me_id": "2931",
              "target_me_id": "3022"
            },
          },
          "summarize_other_inc": {
            "kwargs": {
              "source_me_id": "2933",
              "target_me_id": "3024"
            }
          }
        }
        
        for identity, nested_dict in me_map.items():
           
            # compile submission arguments
            source_me_id = nested_dict['kwargs']['source_me_id']
            target_me_id = nested_dict['kwargs']['target_me_id']

            west_params = [
                "--source_me_id", source_me_id,
                "--target_me_id", target_me_id
            ]

            westrom_job = "westrom_{}".format(identity)
            
            call = ('qsub -hold_jid {hj} -pe multi_slot 8'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' westrom.py'
                    ' {arg1}'.format(hj=self.hold,
                        o=output_path,
                        e=error_path,
                        jn=westrom_job, 
                        arg1=' '.join(west_params)))
            subprocess.call(call, shell=True)


class Envelope(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):

        me_map = {
        "primary_infert_env": {
            "kwargs": {
                "male_prop_id": "2823",
                "female_prop_id": "2824",
                "exp_id": "2794",
                "env_id": "2421",
                "male_env_id": "3339",
                "female_env_id": "3340"
                }
        },
        "secondary_infert_env": {
            "kwargs": {
                "male_prop_id": "2825",
                "female_prop_id": "2826",
                "exp_id": "2795",
                "env_id": "2422",
                "male_env_id": "3341",
                "female_env_id": "3342"
            }
        }}

        for identity, nested_dict in me_map.items():

            # compile submission arguments
            male_prop_id = nested_dict['kwargs']['male_prop_id']
            female_prop_id = nested_dict['kwargs']['female_prop_id']
            exp_id = nested_dict['kwargs']['exp_id']
            env_id = nested_dict['kwargs']['env_id']
            male_env_id = nested_dict['kwargs']['male_env_id']
            female_env_id = nested_dict['kwargs']['female_env_id']

            # make server directory
            directory = "{root}/{proc}".format(root=root, proc=identity)
            
            if os.path.exists(directory):
                shutil.rmtree(directory)
                os.makedirs(directory)
            else:
                os.makedirs(directory)
            
            # make output directories
            for _id in [male_env_id, female_env_id]:
                sub_dir = os.path.join(directory, str(_id))
                if not os.path.exists(sub_dir):
                    os.makedirs(sub_dir)

            env_params = ["--male_prop_id", str(male_prop_id), 
                            "--female_prop_id", str(female_prop_id), 
                            "--exp_id", str(exp_id), 
                            "--env_id", str(env_id), 
                            "--male_env_id", str(male_env_id), 
                            "--female_env_id", str(female_env_id), 
                            "--out_dir", str(directory), "--year_id"]

            
            # parallelize by location
            env_string = ''
            for i in [1990, 1995, 2000, 2005, 2010, 2017]:
                env_job = "{}_{}".format(identity, i)
                env_string = env_string + ',' + env_job
                call = ('qsub -hold_jid {hj} -pe multi_slot 8'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' calc_env.py'
                        ' {arg1}'.format(hj=self.hold,
                            o=output_path,
                            e=error_path,
                            jn=env_job,
                            arg1=' '.join(env_params + [str(i)])))
                subprocess.call(call, shell=True)
            
            hold = env_string
            # save the results
            for save_id in [male_env_id, female_env_id]:

                save_params = [
                    str(save_id), 
                    "--description", "\'{}\'".format(env_description),
                    "--input_dir", os.path.join(directory, str(save_id)), 
                    "--best",
                    "--meas_ids", "5",
                    "--file_pattern", "{year_id}.h5"]
                
                job_name = 'env_save_{}'.format(save_id)
                self.save_hold = self.save_hold + ',' + job_name
                call = ('qsub -hold_jid {hj}'
                        ' -pe multi_slot 15'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' save.py'
                        ' {arg1}'.format(hj=hold,
                            o=output_path,
                            e=error_path,
                            jn=job_name,
                            arg1=' '.join(save_params)))
                subprocess.call(call, shell=True)
        return self.save_hold

class FemaleInfert(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):

        # compile submission arguments
        me_map = { 
        "env": {
            "type": "envelope",
            "srcs": {
                "prim": "3340",
                "sec": "3342"
            }
        },
        "resid": {
            "type": "residual",
            "trgs": {
                "prim": "2071",
                "sec": "2072"
            }
        },
        "excess": {
            "type": "excess",
            "trgs": {
                "endo": "9748",
                "pcos": "9743"
            }
        },
        "cong_uro": {
            "type": "locked",
            "srcs": {
                "inf_only": "11033",
                "ag": "11037",
                "uti": "11039",
                "imp": "11041",
                "ag_uti": "11042",
                "ag_imp": "11044",
                "imp_uti": "11045",
                "ag_imp_uti": "11046"
            }
        },
        "turner": {
            "type": "locked",
            "srcs": {
                "nohf": "2208",
                "hf": "2653"
            }
        },
        "sepsis": {
            "type": "sub_group",
            "srcs": {
                "tot": "2624"
            },
            "trgs": {
                "sec": "9678"
            }
        },
        "pcos_asymp": {
            "type": "sub_group",
            "excess": "pcos",
            "srcs": {
                "tot": "9675"
            },
            "trgs": {
                "prim": "2069",
                "sec": "3088"
            }
        },
        "pcos_disfig": {
            "type": "sub_group",
            "excess": "pcos",
            "srcs": {
                "tot": "9676"
            },
            "trgs": {
                "prim": "2938",
                "sec": "3087"
            }
        },
        "endo_asymp": {
            "type": "sub_group",
            "excess": "endo",
            "srcs": {
                "tot": "9671"
            },
            "trgs": {
                "prim": "2076",
                "sec": "2077"
            }
        },
        "endo_mild": {
            "type": "sub_group",
            "excess": "endo",
            "srcs": {
                "tot": "9672"
            },
            "trgs": {
                "prim": "2959",
                "sec": "2962"
            }
        },
        "endo_mod": {
            "type": "sub_group",
            "excess": "endo",
            "srcs": {
                "tot": "9673"
            },
            "trgs": {
                "prim": "2960",
                "sec": "2963"
            }
        },
        "endo_sev": {
            "type": "sub_group",
            "excess": "endo",
            "srcs": {
                "tot": "9674"
            },
            "trgs": {
                "prim": "2961",
                "sec": "2964"
            }
        },
        "gono": {
            "type": "sub_group",
            "srcs": {
                "tot": "3023"
            },
            "trgs": {
                "prim": "1639",
                "sec": "1640"
            }
        },
        "chlam": {
            "type": "sub_group",
            "srcs": {
                "tot": "3022"
            },
            "trgs": {
                "prim": "1633",
                "sec": "1634"
            }
        },
        "otherstd": {
            "type": "sub_group",
            "srcs": {
                "tot": "3024"
            },
            "trgs": {
                "prim": "1645",
                "sec": "1646"
            }
        }}

        # make server directory
        directory = "{root}/{proc}".format(root=root, 
            proc='female_cause_attribution')
        
        if os.path.exists(directory):
            shutil.rmtree(directory)
            os.makedirs(directory)
        else:
            os.makedirs(directory)


        # make output directories
        save_ids = []
        for mapper in me_map.values():
            outputs = mapper.get("trgs", {})
            for me_id in outputs.values():
                if not os.path.exists(os.path.join(directory, str(me_id))):
                    os.makedirs(os.path.join(directory, str(me_id)))
                save_ids.append(me_id)
        
        attr_params = ["--me_map", "\'{}\'".format(json.dumps(me_map)),
                       "--out_dir", directory,
                       "--location_id"]
        
        # parallelize by location
        fem_attr_string = ''
        for i in getset.get_most_detailed_location_ids():
            fem_attr_job = "fem_attr_{}".format(i)
            fem_attr_string = fem_attr_string + ',' + fem_attr_job
            call = ('qsub -hold_jid {hj}'
                    ' -pe multi_slot 4'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' female_attr.py'
                    ' {arg1}'.format(hj=self.hold,
                        o=output_path,
                        e=error_path,
                        jn=fem_attr_job, 
                        arg1=' '.join(attr_params + [str(i)])))
            subprocess.call(call, shell=True)
        
        # save the results
        hold = fem_attr_string
        for save_id in save_ids:

            save_params = [
                str(save_id), 
                "--description", "\'{}\'".format(female_attr_description),
                "--input_dir", os.path.join(directory, str(save_id)), 
                "--best",
                "--sexes", "2",
                "--meas_ids", "5",
                "--file_pattern", "{location_id}.h5"]

            job_name = 'fem_attr_save_{}'.format(save_id)
            self.save_hold = self.save_hold + ',' + job_name
            call = ('qsub -hold_jid {hj} -pe multi_slot 15'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' cluster_shell.sh'
                ' save.py'
                ' {arg1}'.format(hj=hold,
                    o=output_path,
                    e=error_path,
                    jn=job_name,
                    arg1=' '.join(save_params)))
            subprocess.call(call, shell=True)
        return self.save_hold

class MaleInfert(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):

        # compile submission arguments
        me_map = {
        "env": {
            "srcs": {
                "prim": "3339",
                "sec": "3341"
            }
        },
        "idio": {
            "trgs": {
                "prim": "2061",
                "sec": "2062"
            }
        },
        "kline": {
            "srcs": {
                "bord": "2218",
                "mild": "2659",
                "asym": "3069"
            }
        },
        "cong_uro": {
            "srcs": {
                "inf_only": "11033",
                "ag": "11037",
                "uti": "11039",
                "imp": "11041",
                "ag_uti": "11042",
                "ag_imp": "11044",
                "imp_uti": "11045",
                "ag_imp_uti": "11046"
            }
        }}

        # make server directory
        directory = "{root}/{proc}".format(root=root, 
            proc='male_cause_attribution')

        if os.path.exists(directory):
            shutil.rmtree(directory)
            os.makedirs(directory)
        else:
            os.makedirs(directory)

        # make output directories
        save_ids = []
        for mapper in me_map.values():
            outputs = mapper.get("trgs", {})
            for me_id in outputs.values():
                if not os.path.exists(os.path.join(directory, str(me_id))):
                    os.makedirs(os.path.join(directory, str(me_id)))
                save_ids.append(me_id)

        attr_params = ["--me_map", "\'{}\'".format(json.dumps(me_map)),
                       "--out_dir", directory,
                       "--location_id"]

        # attribution jobs by year_id
        male_attr_string = ''
        for i in getset.get_most_detailed_location_ids():
            male_attr_job = "male_attr_{}".format(i)
            male_attr_string = male_attr_string + ',' + male_attr_job
            call = ('qsub -hold_jid {hj}'
                    ' -pe multi_slot 4'
                    ' -cwd -P proj_custom_models'
                    ' -o {o}'
                    ' -e {e}'
                    ' -N {jn}'
                    ' cluster_shell.sh'
                    ' male_attr.py'
                    ' {arg1}'.format(hj=self.hold,
                        o=output_path,
                        e=error_path,
                        jn=male_attr_job, 
                        arg1=' '.join(attr_params + [str(i)])))
            subprocess.call(call, shell=True)

        # save the results
        hold = male_attr_string
        for save_id in save_ids:
            save_params = [
                str(save_id), 
                "--description", "\'{}\'".format(male_attr_description),
                "--input_dir", os.path.join(directory, str(save_id)), 
                "--best",
                "--sexes", "1",
                "--meas_ids", "5",
                "--file_pattern", "{location_id}.h5"]

            call = ('qsub -hold_jid {hj} -pe multi_slot 15'
                ' -cwd -P proj_custom_models'
                ' -o {o}'
                ' -e {e}'
                ' -N {jn}'
                ' cluster_shell.sh'
                ' save.py'
                ' {arg1}'.format(hj=hold,
                    o=output_path,
                    e=error_path,
                    jn='male_attr_save_{}'.format(save_id),
                    arg1=' '.join(save_params)))
            subprocess.call(call, shell=True)


class Excess(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):

        me_map = {
        "pcos_excess": {
            "kwargs":{
                "excess": "9743",
                "redist": {
                    "2936": "9741",
                    "2937": "9742"
                },
                "copy": {
                    "2068":"9741"
                }
            }
        },
        "endo_excess": {
            "kwargs": {
                "excess": "9748",
                "redist": {
                    "2889": "9744",
                    "2074": "9745",
                    "2075": "9746",
                    "3089": "9747"
                },
                "copy": {
                    "2073":"9744"
                }
            }
        }}

        for identity, nested_dict in me_map.items():
            excess_id = nested_dict['kwargs']['excess']
            redist_map = nested_dict['kwargs']['redist']
            copy_map = nested_dict['kwargs']['copy']

            # make server directory
            directory = "{root}/{proc}".format(root=root, proc=identity)
            
            if os.path.exists(directory):
                shutil.rmtree(directory)
                os.makedirs(directory)
            else:
                os.makedirs(directory)

            # make output directories
            for me_id in redist_map.values():
                sub_dir = os.path.join(directory, str(me_id))
                if not os.path.exists(sub_dir):
                    os.makedirs(sub_dir)

            exs_params = ["--excess_id", str(excess_id),
                          "--redist_map", 
                          "\'{}\'".format(json.dumps(redist_map)),
                          "--copy_map",
                          "\'{}\'".format(json.dumps(copy_map)),
                          "--out_dir", directory,
                          "--year_id"]

            # excess jobs by year_id
            excess_string = ''
            for i in [1990, 1995, 2000, 2005, 2010, 2017]:
                excess_job = "{}_{}".format(identity, i)
                excess_string = excess_string + ',' + excess_job
                call = ('qsub -hold_jid {hj}'
                        ' -pe multi_slot 5'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' excess.py'
                        ' {arg1}'.format(hj=self.hold,
                            o=output_path,
                            e=error_path,
                            jn=excess_job, 
                            arg1=' '.join(exs_params + [str(i)])))
                subprocess.call(call, shell=True)
            
            hold = excess_string
            # save the results
            for save_id in redist_map.values():
                if save_id in copy_map.values():
                    meas_ids = [5,6]
                else:
                    meas_ids = [5]

                save_params = [
                    str(save_id), 
                    "--description", "\'{}\'".format(excess_description),
                    "--input_dir", os.path.join(directory, str(save_id)), 
                    "--best",
                    "--sexes", "2",
                    "--meas_ids",
                    " ".join([str(x) for x in meas_ids]),
                    "--file_pattern", "{year_id}.h5"]

                call = ('qsub -hold_jid {hj}'
                        ' -pe multi_slot 15'
                        ' -cwd -P proj_custom_models'
                        ' -o {o}'
                        ' -e {e}'
                        ' -N {jn}'
                        ' cluster_shell.sh'
                        ' save.py'
                        ' {arg1}'.format(hj=hold,
                            o=output_path,
                            e=error_path,
                            jn='{}_save_{}'.format(identity, save_id),
                            arg1=' '.join(save_params)))
                subprocess.call(call, shell=True)


if __name__ == "__main__":
    sev = SevSplit()
    hold1a = sev.execute()
    pid = PID()
    hold1b = pid.execute() 
    westrom = Westrom(hold1b) 
    westrom.execute() 
    env = Envelope() 
    hold1c = env.execute()
    time.sleep(10)
    maleattr = MaleInfert(hold1a + "," + hold1c) 
    maleattr.execute()
    #femattr = FemaleInfert() 
    #hold2 = femattr.execute() 
    #time.sleep(10)
    #excess = Excess(hold2)
    #excess.execute()


