import subprocess
import os
import sys
import sqlalchemy as sql
import datetime
from hybridizer.core import run_query, insert_row, read_creds, send_email
from hybridizer.database import get_cause_hierarchy_version, get_location_hierarchy_version

# Get variables from arguments
user, global_model_version_id, developed_model_version_id, server = \
    [sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]]
print user, global_model_version_id, developed_model_version_id, server

# Read credentials to access database
creds = read_creds()
server_name = "SERVER".format(server)

# Get version id's for cause and location sets and metadata
cause_set_version_id, cause_metadata_version_id = \
    get_cause_hierarchy_version(server=server_name)
location_set_version_id, location_metadata_version_id = \
    get_location_hierarchy_version(server=server_name)

# Read input model data from SQL
sql_query = """SELECT
                   mv.model_version_id,
                   mv.cause_id,
                   c.acause,
                   mv.sex_id,
                   mv.age_start,
                   mv.age_end
               FROM
                   cod.model_version mv
               JOIN
                   shared.cause c USING (cause_id)
               WHERE
                   model_version_id IN ({global_model_version_id},
                                        {developed_model_version_id})
               """.format(global_model_version_id=global_model_version_id,
                          developed_model_version_id=developed_model_version_id)
feeder_model_data = run_query(sql_query, server=server_name)

# Isolate the variables that have been read in as a dataframe
sex_id = feeder_model_data['sex_id'].drop_duplicates()[0]
cause_id = feeder_model_data['cause_id'].drop_duplicates()[0]
acause = feeder_model_data['acause'].drop_duplicates()[0]
age_start = feeder_model_data['age_start']
age_end = feeder_model_data['age_end']

# Check to make sure that the age restrictions match up.
if len(age_start.unique()) > 1 or len(age_end.unique()) > 1:
  # Send an email to the modeler telling them that their ages didn't match up.
  message_for_body = '''
        <p>Hello {user},</p>
        <p>The hybrid of {global_model} and {developed_model} for {acause} and sex {sex} \
            has failed to start because they had different age ranges. Please resubmit this
            hybrid by selecting two models with the same age restrictions.</p>
        <p>Regards,</p>
        <p>Your Friendly Neighborhood Hybridizer</p>
        '''.format(user=user,
                   global_model=global_model_version_id,
                   developed_model=developed_model_version_id,
                   acause=acause,
                   sex=sex_id)
  send_email(['{user}@EMAIL'.format(user=user)],
             "Hybrid of {global_model} and {developed_model} ({acause}) failed due to mismatched age restrictions".
             format(global_model=global_model_version_id,
                    developed_model=developed_model_version_id,
                    acause=acause),
             message_for_body)
else:
  age_start = age_start.drop_duplicates()[0]
  age_end = age_end.drop_duplicates()[0]

# Create hybrid model entry
model_version_metadata = sql.MetaData()

engine = sql.create_engine('DATABASE'.
                           format(creds=creds, server=server_name, database="cod"))
model_version_table = sql.Table('model_version', model_version_metadata,
                                autoload=True, autoload_with=engine)

description = "Hybrid of models {global_model_version_id} and \
                        {developed_model_version_id}".\
    format(global_model_version_id=global_model_version_id,
           developed_model_version_id=developed_model_version_id)

ins = model_version_table.insert().values(cause_id=cause_id,
                                          description=description,
                                          location_set_version_id=location_set_version_id,
                                          cause_set_version_id=cause_set_version_id,
                                          previous_model_version_id=global_model_version_id,
                                          status=0,
                                          is_best=0,
                                          environ=20,
                                          sex_id=sex_id,
                                          age_start=age_start,
                                          age_end=age_end,
                                          locations_exclude='',
                                          date_inserted=datetime.datetime.now(),
                                          inserted_by=user,
                                          last_updated=datetime.datetime.now(),
                                          last_updated_by=user,
                                          last_updated_action='INSERT',
                                          model_version_type_id=3
                                          )
model_version_id = insert_row(ins, engine)


# Set up folder structure for hybrid results
base_dir = "FILEPATH".format(acause, model_version_id)
print acause
print model_version_id
if not os.path.exists(base_dir):
  os.makedirs(base_dir)
subprocess.call(
    "sudo chmod 777 {base_dir} -R".format(base_dir=base_dir), shell=True)

# Append rows to model relation table
model_version_relation_meta = sql.MetaData()
model_relation_table = sql.Table('model_version_relation',
                                 model_version_relation_meta, autoload=True,
                                 autoload_with=engine)
ins = model_relation_table.insert().values(parent_id=model_version_id,
                                           child_id=global_model_version_id,
                                           model_version_relation_note="2017 Hybrid Model",
                                           date_inserted=datetime.datetime.now(),
                                           inserted_by=user,
                                           last_updated=datetime.datetime.now(),
                                           last_updated_by=user,
                                           last_updated_action='INSERT'
                                           )
insert_row(ins, engine)
ins = model_relation_table.insert().values(parent_id=model_version_id,
                                           child_id=developed_model_version_id,
                                           model_version_relation_note="2017 Hybrid Model",
                                           date_inserted=datetime.datetime.now(),
                                           inserted_by=user,
                                           last_updated=datetime.datetime.now(),
                                           last_updated_by=user,
                                           last_updated_action='INSERT'
                                           )
insert_row(ins, engine)


# Prepare variables to submit job to cluster
sh_script = "FILEPATH"
py_script = "FILEPATH"

sudo = 'sudo -u {user} sh -c'.format(user=user)
qsub = '". FILEPATH '
name = '-N cod_hybrid_{model_version_id} -P proj_codem'.\
    format(model_version_id=model_version_id)
outputs = '-e {base_dir}/ -o {base_dir}/'.format(base_dir=base_dir)
slots = '-pe multi_slot 12'
scripts = '{sh_script} {py_script} {model_version_id} {global_model_version_id}\
                            {developed_model_version_id} {server}"'\
                            .format(sh_script=sh_script, py_script=py_script,
                                    model_version_id=model_version_id,
                                    global_model_version_id=global_model_version_id,
                                    developed_model_version_id=developed_model_version_id,
                                    server=server_name)
codem_call = ' '.join([sudo, qsub, name, outputs, slots, scripts])


# Submit job to cluster
process = subprocess.Popen(codem_call, shell=True, stdout=subprocess.PIPE)
out, err = process.communicate()

print "Done"
