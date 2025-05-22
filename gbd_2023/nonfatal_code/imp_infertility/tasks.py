######################################################################################
## PURPOSE:
## tasks.py is a submitter script that breaks the infertility pipeline into object-oriented classes. 
## Each class has the following three things in common: (1) Parameter lists that are passed to 
## subscripts and that explain how source and target modelable_entities relate to one another, 
## (2) execution of a data manipulation step that performs some type of custom calculation on the input 
## data (parallelized by location_id or year_id where applicable), and (3) execution of a save step 
## that saves the results from (2) to a database, either via save_results_epi() or upload_epi_data().
######################################################################################

import os
import shutil
import pandas as pd
import subprocess
import json
from db_queries import get_best_model_versions, get_location_metadata
from gbd_artifacts.severity_prop import SeverityPropMetadata
import time
import getpass
import glob
from pathlib import Path

username = getpass.getuser()
root = os.path.join('FILEPATH', USERNAME)
#################### UPDATE WITH FILEPATHS ###########################################
error_path = "FILEPATH"
output_path = "FILEPATH"
infert_path = 'FILEPATH'
code_dir = 'FILEPATH'
######################################################################################

if not os.path.exists(error_path):
    os.makedirs(error_path)
if not os.path.exists(output_path):
    os.makedirs(output_path)
if not os.path.exists(infert_path):
    os.makedirs(infert_path)

RELEASE_ID = OBJECT

# import JSON file with me_id source and target information 
#code_dir = os.path.dirname(os.path.abspath(__file__))
filepath = os.path.join(code_dir,'FILEPATH')
with open(filepath, 'r') as f:
    imap = json.load(f)

# descriptions
sev_split_description = f"Infertility severity split, see {infert_path}SevSplit.csv for model version ids"
pid_split_description = f"Infertility PID split with incidence, see {infert_path}PID.csv for model version ids"
env_description = f"Infertility env run, see {infert_path}Envelope.csv for model version ids"
female_attr_description = f"Infertility female attribution, see {infert_path}FemaleInfert.csv for model version ids"
male_attr_description = f"Infertility male attribution, see {infert_path}MaleInfert.csv for model version ids"
excess_description = f"Infertility excess redistribution, see  {infert_path}Excess.csv for model version ids"

# locations to parallelize over
loc_meta = get_location_metadata()
most_detailed_location_ids = loc_meta.loc[
    loc_meta.most_detailed==1,'location_id'].tolist()

class SevSplit(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):
        # compile submission arguments
        global sev_split_description
        
        # To start the pipeline off, we need to apply severity splits to
        # Endometriosis
        # PCOS
        # The split database assigns split ids to aid with version control
        map_name = "SevSplit"
        split_map = imap[map_name]

        # Get best models versions of parent_ids for save step
        parent_meids = []
        for identity, nested_dict in split_map.items():
            parent_meids.append(nested_dict['parent_id'])
        
        mvid_df = get_best_model_versions(entity='modelable_entity', 
            ids=parent_meids)

        for identity, nested_dict in split_map.items():
            split_id = nested_dict['split_id']
            split_meta = SeverityPropMetadata(split_id=split_id, 
                release_id=RELEASE_ID)
            split_version_id = split_meta.best_version
            meta_version = split_meta.get_metadata_version(split_version_id)
            parent_id = int(meta_version.parent_meid())
            assert parent_id == nested_dict['parent_id']
            
            # make output directory
            directory = os.path.join(infert_path, str(parent_id))

            if os.path.exists(directory):
                shutil.rmtree(directory)
                os.makedirs(directory)
            else:
                os.makedirs(directory)

            split_params = [str(GBD_ROUND_ID), str(RELEASE_ID),
                "--split_version_id", str(split_version_id),
                "--output_dir", directory]

            for count,i in enumerate(most_detailed_location_ids):

                sev_split = 'split_{}_loc_{}'.format(parent_id, i)

                call = (
                    ' -o {o}'
                    ' -e {e}'
                    ' -J {jn}'
                    ' {shell}'
                    ' -s {script}'
                    ' {arg1}'.format(o=output_path,
                        e=error_path,
                        jn=sev_split,
                        shell='{}cluster_shell.sh'.format(code_dir),
                        script='{}sev_splits.py'.format(code_dir),
                        arg1=' '.join(split_params + ['--location_id', str(i)])))
                subprocess.call(call, shell=True)

        mvid_df.to_csv(os.path.join(infert_path, f'{map_name}.csv'),
            index=False, encoding='utf8')

        return self.save_hold


class PID(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):

        pid_map = imap["PID"]
        # compile submission arguments
        pid_env_id = pid_map["pid_env_id"] 
        chlam_prop_id = pid_map["chlam_prop_id"]
        gono_prop_id = pid_map["gono_prop_id"]
        other_prop_id = pid_map["other_prop_id"]
        chlam_id =  pid_map["chlam_id"]
        gono_id = pid_map["gono_id"]
        other_id = pid_map["other_id"]

        # make server directory
        directory = "{root}/{proc}".format(root=infert_path, proc="pid_split")

        if os.path.exists(directory):
            shutil.rmtree(directory)
            os.makedirs(directory)
        else:
            os.makedirs(directory)

        mvid_df = get_best_model_versions(entity='modelable_entity', 
            ids=[pid_env_id, chlam_prop_id, gono_prop_id, other_prop_id],
            release_id = RELEASE_ID)

        split_params = [
            str(pid_env_id),
            "--target_meids", str(chlam_id), str(gono_id), str(other_id),
            "--prop_meids", str(chlam_prop_id), str(gono_prop_id), str(other_prop_id),
            "--split_measure_ids", "5", "6",
            "--prop_meas_id", "18",
            "--output_dir", directory]

        pid_split = "pid_split"

        call = (
                ' -o {o}'
                ' -e {e}'
                ' -J {jn}'
                ' {shell}'
                ' -s {script}'
                ' {arg1}'.format(hj=self.hold,
                    o=output_path,
                    e=error_path,
                    jn=pid_split, 
                    shell='{}cluster_shell.sh'.format(code_dir),
                    script='{}epi_splits_pid.py'.format(code_dir),
                    arg1=' '.join(split_params)))
        subprocess.call(call, shell=True)

        mvid_df.to_csv(os.path.join(infert_path, 'PID.csv'), index=False,
            encoding='utf8')

        return self.save_hold

class Westrom(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):

        me_map = imap["Westrom"]

        src_list = []

        for identity, nested_dict in me_map.items():
           
            # compile submission arguments
            source_me_id = nested_dict['kwargs']['source_me_id']
            target_me_id = nested_dict['kwargs']['target_me_id']

            src_list.append(source_me_id)

            west_params = [
                "--source_me_id", str(source_me_id),
                "--target_me_id", str(target_me_id)
            ]

            westrom_job = "westrom_{}".format(identity)
            
            call = (
                    ' -o {o}'
                    ' -e {e}'
                    ' -J {jn}'
                    ' {shell}'
                    ' -s {script}'
                    ' {arg1}'.format(hj=self.hold,
                        o=output_path,
                        e=error_path,
                        jn=westrom_job,
                        shell = "{}cluster_shell.sh".format(code_dir),
                        script = "{}westrom.py".format(code_dir),
                        arg1=' '.join(west_params)))
            subprocess.call(call, shell=True)

        mvid_df = get_best_model_versions(entity='modelable_entity',
            ids=src_list)
        mvid_df.to_csv(os.path.join(infert_path, 'Westrom.csv'), index=False,
            encoding='utf8')


class Envelope(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):

        map_name = "Envelope"
        me_map = imap[map_name]

        src_list = []

        for identity, nested_dict in me_map.items():

            # compile submission arguments
            male_prop_id = nested_dict['kwargs']['male_prop_id']
            female_prop_id = nested_dict['kwargs']['female_prop_id']
            exp_id = nested_dict['kwargs']['exp_id']
            env_id = nested_dict['kwargs']['env_id']
            male_env_id = nested_dict['kwargs']['male_env_id']
            female_env_id = nested_dict['kwargs']['female_env_id']

            src_list.extend([male_prop_id, female_prop_id, exp_id, env_id])

            # make server directory
            directory = "{root}/{proc}".format(root=infert_path, proc=identity)
            
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
                          "--out_dir", str(directory),
                          "--year_id"]

            # parallelize by location
            env_string = ''
            for i in [1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023, 2024]:
                
                env_job = "{}_{}".format(identity, i)
                env_string = env_string + ',' + env_job
                call = (
                        ' -o {o}'
                        ' -e {e}'
                        ' -J {jn}'
                        ' {shell}'
                        ' -s {script}'
                        ' {arg1}'.format(hj=self.hold,
                            o=output_path,
                            e=error_path,
                            jn=env_job,
                            shell="{}cluster_shell.sh".format(code_dir),
                            script="{}calc_env.py".format(code_dir),
                            arg1=' '.join(env_params + [str(i)])))
                subprocess.call(call, shell=True)

        mvid_df = get_best_model_versions(entity='modelable_entity',
            ids=src_list)
        mvid_df.to_csv(os.path.join(infert_path, f'{map_name}.csv'),
            index=False, encoding='utf8')

        return self.save_hold

class FemaleInfert(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold
        self.save_hold = ''

    def execute(self):

        # compile submission arguments
        map_name = "FemaleInfert"
        me_map = imap[map_name]

        # make server directory
        directory = "{root}/{proc}".format(root=infert_path,
            proc='female_cause_attribution')

        if os.path.exists(directory):
            print("Deleting old results")
            shutil.rmtree(directory)
            print("Finished deleting old results")
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

        # get src ids for model_version_id export
        src_list = []
        for mapper in me_map.values():
            inputs = mapper.get("srcs", {})
            for me_id in inputs.values():
                src_list.append(me_id)

        attr_params = ["--map_name", map_name,
                       "--out_dir", directory,
                       "--location_id"]
        
        # parallelize by location
        fem_attr_list = []
        for i in most_detailed_location_ids:
            fem_attr_job = "fem_attr_{}".format(i)
            fem_attr_list.append(fem_attr_job)
            call = (
                    ' -o {o}'
                    ' -e {e}'
                    ' -J {jn}'
                    ' {shell}'
                    ' -s {script}'
                    ' {arg1}'.format(hj=self.hold,
                        o=output_path,
                        e=error_path,
                        jn=fem_attr_job, 
                        shell="{}cluster_shell.sh".format(code_dir),
                        script="{}female_attr.py".format(code_dir),
                        arg1=' '.join(attr_params + [str(i)])))
            subprocess.call(call, shell=True)

        mvid_df = get_best_model_versions(entity='modelable_entity',
            ids=src_list)
        mvid_df.to_csv(os.path.join(infert_path, f'{map_name}.csv'),
            index=False, encoding='utf8')

        return self.save_hold

class MaleInfert(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):

        # compile submission arguments
        map_name = "MaleInfert"
        me_map = imap[map_name]

        # make server directory
        directory = "{root}/{proc}".format(root=infert_path,
            proc='male_cause_attribution')

        if os.path.exists(directory):
            print("Deleting old results")
            shutil.rmtree(directory)
            print("Finished deleting old results")
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

        # get src ids for model_version_id export
        src_list = []
        for mapper in me_map.values():
            inputs = mapper.get("srcs", {})
            for me_id in inputs.values():
                src_list.append(me_id)

        attr_params = ["--map_name", map_name,
                       "--out_dir", directory,
                       "--location_id"]

        # attribution jobs by year_id
        male_attr_list = []
        #for i in most_detailed_location_ids:
        for i in [44916]:
            male_attr_job = "male_attr_{}".format(i)
            male_attr_list.append(male_attr_job)
            call = (
                    ' -o {o}'
                    ' -e {e}'
                    ' -J {jn}'
                    ' {shell}'
                    ' -s {script}'
                    ' {arg1}'.format(hj=self.hold,
                        o=output_path,
                        e=error_path,
                        jn=male_attr_job,
                        shell="{}cluster_shell.sh".format(code_dir),
                        script="{}male_attr.py".format(code_dir),
                        arg1=' '.join(attr_params + [str(i)])))
            subprocess.call(call, shell=True)

        mvid_df = get_best_model_versions(entity='modelable_entity',
            ids=src_list)
        mvid_df.to_csv(os.path.join(infert_path, f'{map_name}.csv'),
            index=False, encoding='utf8')


class Excess(object):
    def __init__(self, hold='no_holds'):
        self.hold = hold

    def execute(self):

        map_name = "Excess"
        me_map = imap["Excess"]

        src_list = []

        for identity, nested_dict in me_map.items():
            excess_id = nested_dict['kwargs']['excess']
            redist_map = nested_dict['kwargs']['redist']
            copy_map = nested_dict['kwargs']['copy']

            # make server directory
            directory = "{root}/{proc}".format(root=infert_path, proc=identity)
            
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

            src_list.extend([excess_id] + 
                [int(x) for x in list(redist_map.keys())] +
                [int(x) for x in list(copy_map.keys())])

            exs_params = ["--map_name", map_name,
                          "--excess_map", identity,
                          "--out_dir", directory,
                          "--year_id"]

            # excess jobs by year_id
            excess_string = ''
            for i in [1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022, 2023, 2024]:
                excess_job = "{}_{}".format(identity, i)
                excess_string = excess_string + ',' + excess_job
                call = (
                        ' -o {o}'
                        ' -e {e}'
                        ' -J {jn}'
                        ' {shell}'
                        ' -s {script}'
                        ' {arg1}'.format(hj=self.hold,
                            o=output_path,
                            e=error_path,
                            jn=excess_job,
                            shell="{}cluster_shell.sh".format(code_dir),
                            script="{}excess.py".format(code_dir),
                            arg1=' '.join(exs_params + [str(i)])))
                subprocess.call(call, shell=True)

        mvid_df = get_best_model_versions(entity='modelable_entity',
            ids=src_list)
        mvid_df.to_csv(os.path.join(infert_path, f'{map_name}.csv'),
            index=False, encoding='utf8')


if __name__ == "__main__":
#    sev = SevSplit()
#    hold1a = sev.execute()
#    pid = PID()
#    hold1b = pid.execute()
#    westrom = Westrom()
#    westrom.execute()
#    env = Envelope()
#    hold1c = env.execute()
#    time.sleep(10)
#    femattr = FemaleInfert()
#    hold2 = femattr.execute()
#    time.sleep(10)
#    maleattr = MaleInfert()
#    maleattr.execute()
#    time.sleep(10)
#    excess = Excess()
#    excess.execute()
