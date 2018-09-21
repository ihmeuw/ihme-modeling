import sqlalchemy as sql
import pandas as pd
from hybridizer.core import run_query, execute_statement
from hybridizer.database import get_location_hierarchy_version
from hybridizer.model_data import ModelData
import hybridizer.log_utilities as l
import smtplib
import logging
import sys
import os


model_version_id = int(sys.argv[1])
global_model = int(sys.argv[2])
developed_model = int(sys.argv[3])
server_name = sys.argv[4]

def get_cause_dict():
    """
    Returns information stored in the shared.cause table with a key of cause_id.
    This does not include hierarchy-specific information like age start or age
    end.

    :return: dict
        cause information
    """
    cause_data = run_query('SELECT * FROM shared.cause;', \
                    server=server_name).set_index('cause_id')
    output = {}
    for i in cause_data.index:
        output[i] = {}
        for c in cause_data.columns:
            output[i][c] = cause_data.ix[i, c]
    return output


def read_model_draws(draws_filepath, filter_statement=None):
    """
    Read in CODEm/custom model draws from a given filepath, optionally
    filtered by location_id.

    :param draws_filepath: str
        file path to the hdf file containing the model draws to read in
    :param filter_statement: list of strings
        list of strings containing SQL boolean statements to filter by
    :return: dataframe
        pandas dataframe containing the (filtered) model draws
    """
    if filter_statement:
        data = pd.read_hdf(draws_filepath, key="data", where=filter_statement)
    else:
        data = pd.read_hdf(draws_filepath, key="data")
    return data


def get_locations(location_set_version_id):
    """
    Get a DataFrame of locations and their hierarchy data from the
    specified location hierarchy version

    :param location_set_version_id: int
        the version id for the set of locations to retrieve
    :return: dataframe
        pandas dataframe containing the location hierarchy information
    """
    call = "Call shared.view_location_hierarchy_history({loc_set_vid})".\
        format(loc_set_vid=location_set_version_id)
    return run_query(call, server=server_name)


def get_model_properties(model_version_id):
    """
    Gets a dictionary of the model information for a specified model_version_id

    :param model_version_id: int
        specification for which model version to retrieve
    :return: dict
        dictionary where each entry is a column of the model_version table
    """
    call = "SELECT * FROM cod.model_version WHERE model_version_id = {};".\
        format(model_version_id)
    model_data = run_query(call, server=server_name)
    output = {}
    for c in model_data.columns:
        output[c] = model_data.ix[0, c]
    return output


def get_excluded_locations(developed_model):
    """
    Get a list of the excluded locations from the developed model

    We will use these IDs to figure out which locations to pull from the global
    model and which ones to pull from the developed model.

    :param developed_model: int
        model_version_id for the developed model of interest
    :return: list of ints
        list of the location ids that are excluded in the given developed model
    """
    model_data = get_model_properties(developed_model)
    return model_data['locations_exclude'].split(' ')


def tag_location_from_path(path, location_id):
    """
    Tag whether or not a location ID is in a path

    :param path: list of ints
        list of location id's to search
    :param location_id: int
        location_id to search for in path
    :return: boolean
        True if location_id is in path, False otherwise
    """
    if location_id in path:
        return True
    else:
        return False


def get_locations_for_models(developed_model_id, location_set_version_id):
    """
    Marks the locations in a given location set verion as either developed
    (data rich) or global (not data rich) and returns a list of the locations
    in each of those two categories

    :param developed_model_id: int
        model from which certain locations were excluded
    :param location_set_version_id: int
        which location set version the location_ids come from
    :return: tuple(list of ints, list of ints)
        list of data-rich location ids and list of global location ids
        corresponding to the input developed model
    """
    excluded_locations = get_excluded_locations(developed_model_id)
    loc_hierarchy_data = get_locations(location_set_version_id)
    # Set the 'global_model' variable to False initially, then update to True
    # for non-data rich locations
    loc_hierarchy_data['global_model'] = False
    for location_id in excluded_locations:
        # if an excluded location is in the path to the top parent, mark the
        # model as global
        loc_hierarchy_data.ix[loc_hierarchy_data['global_model']==False, \
            'global_model'] = loc_hierarchy_data['path_to_top_parent'].\
            map(lambda x: tag_location_from_path(x.split(','), location_id))
        loc_hierarchy_data = \
            loc_hierarchy_data.ix[(loc_hierarchy_data['is_estimate']==1)]

    developed_location_list =\
        loc_hierarchy_data.ix[loc_hierarchy_data['global_model']==False, \
        'location_id'].tolist()
    global_location_list = \
        loc_hierarchy_data.ix[loc_hierarchy_data['global_model']==True, \
        'location_id'].tolist()

    return global_location_list, developed_location_list


def chunks(l, n):
    """
    Yield successive n-sized chunks from l

    :param l: list
        input list to chunk
    :param n: int
        size of each chunk
    :return: list
        yields lists of size n from the initial list l
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


def read_draws_by_location(draws_filepath, location_ids):
    """
    Reads draws from an hdf file in chunks of 20, then concatenates them
    into the dataframe to be returned

    :param draws_filepath: str
        path to an hdf file containing the draws for a given model
    :param location_ids: list of ints
        list of location ids to filter the draws by
    :return: dataframe
        pandas dataframe of the draws for a given list of locations
    """
    data_all = []
    temp_all_loc_ids = chunks(location_ids, 20)
    for temp_loc_ids in temp_all_loc_ids:
        data = read_model_draws(draws_filepath, \
            "location_id in ["+','.join([str(x) for x in temp_loc_ids])+"]")
        data_all.append(data)
    return pd.concat(data_all).reset_index(drop=True)


def get_draws_filepath(model_properties, cause_dict):
    """
    Gets the filepath to a model's draws file based on input model properties
    and cause information

    :param model_properties: dict
        dictionary containing the cause_id, sex_id, and model_version_id
        for the model in question
    :param cause_dict: dict
        dictionary containing information about a given cause_id
    :return: str
        path to the h5 draws file for a given model
    """
    sex_dict = {1: 'male', 2: 'female'}
    acause = cause_dict[model_properties['cause_id']]['acause']
    sex_name = sex_dict[model_properties['sex_id']]
    model_version_id = model_properties['model_version_id']
    return "FILEPATH"


def send_email(recipients, subject, msg_body):
    """
    Sends an email to user-specified recipients with a given subject and
    message body

    :param recipients: list of strings
        users to send the results to
    :param subject: str
        the subject of the email
    :param msg_body: str
        the content of the email
    """
    SMTP_SERVER = 'SERVER'
    SMTP_PORT = 'PORT'
    sender_name = "CODEm Hybridizer"
    sender = 'USERNAME'
    password = 'PASSWORD'

    headers = ["From: " + sender_name + "<" + sender + ">",
               "Subject: " + subject,
               "To: " + ', '.join(recipients),
               "MIME-Version: 1.0",
               "Content-Type: text/html"]
    headers = "\r\n".join(headers)

    msg_body = headers + "\r\n\r\n" + msg_body

    session = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)

    session.ehlo()
    session.starttls()
    session.ehlo
    session.login(sender, password)

    session.sendmail(sender, recipients, msg_body)
    session.quit()

# Get cause and model data for all 3 models
cause_data = get_cause_dict()
developed_model_properties = get_model_properties(developed_model)
global_model_properties = get_model_properties(global_model)
hybrid_model_properties = get_model_properties(model_version_id)

# set up the logging file
log_dir = 'FILEPATH'
l.setup_logging(log_dir, 'hybridizer')

try:

    # Get list of locations for global and developing model
    location_set_version_id, location_metadata_version_id = \
        get_location_hierarchy_version(server=server_name)
    global_location_list, developed_location_list = \
        get_locations_for_models(developed_model, location_set_version_id)

    print "Global locations:"
    print sorted(global_location_list)
    print "Developed locations:"
    print sorted(developed_location_list)

    # Go through data-rich and global models to concatenate all data
    data_all = []
    # global
    global_draws_filepath = get_draws_filepath(global_model_properties,
                                                                cause_data)
    data_all.append(read_draws_by_location(global_draws_filepath,
                                                global_location_list))

    # data rich
    developed_draws_filepath = get_draws_filepath(developed_model_properties,
                                                                    cause_data)
    data_all.append(read_draws_by_location(developed_draws_filepath,
                                            developed_location_list))

    data_all = pd.concat(data_all).reset_index(drop=True)
    for col in ['location_id', 'year_id', 'age_group_id']:
        data_all[col] = data_all[col].map(lambda x: int(x))

    # Create ModelData instance (uses save_results.py)
    m = ModelData(model_version_id=model_version_id,
                  data_draws=data_all,
                  index_columns=['location_id', 'year_id', 'sex_id', 'age_group_id', 'cause_id'],
                  envelope_column='envelope',
                  pop_column='pop',
                  data_columns=['draw_{}'.format(x) for x in xrange(1000)],
                  # 35,
                  server=server_name)

    # Run prep steps for upload
    print "Aggregating locations"
    logging.info("Aggregating locations")
    m.aggregate_locations()

    print "Save draws"
    logging.info("Save draws")
    m.save_draws()

    print "Generate all ages"
    logging.info("Generate all ages")
    m.generate_all_ages()

    print "Generate summaries"
    logging.info("Generate summaries")
    m.generate_summaries()

    print "Save summaries"
    logging.info("Save summaries")
    m.save_summaries()

    print "Upload summaries"
    logging.info("Upload summaries")
    m.upload_summaries()

    print "Update status"
    logging.info("Update status")
    m.update_status()

    # Send email when completed
    logging.info("Sending email")
    user = get_model_properties(model_version_id)['inserted_by']
    print user
    message_for_body = '''
    <p>Hello {user},</p>
    <p>The hybrid of {global_model} and {developed_model} for {acause} \
            has completed and saved as model {model_version_id}.</p>
    <p>Please check your model and then, if everything looks good, \
        mark it as best.</p>
    <p></p>
    '''.format(user=user, global_model=global_model, \
        developed_model=developed_model, \
        acause=cause_data[hybrid_model_properties['cause_id']]['acause'], \
        model_version_id=model_version_id)
    send_email(['USERNAME'],
            "Hybrid of {global_model} and {developed_model}\
            ({acause}) has completed".\
            format(global_model=global_model,
            developed_model=developed_model,
            acause=cause_data[hybrid_model_properties['cause_id']]['acause']),
        message_for_body)

except Exception as e:

    logging.exception('Failed to hybridize results: {}'.format(e))
    # update status in database to 7 (failure)
    sql_statement = """
        UPDATE cod.model_version
        SET status = 7
        WHERE model_version_id = {model_version_id}
        """.format(model_version_id=model_version_id)
    execute_statement(sql_statement, server=server_name)

    # Update status on model_version table
    sql_statement = """
        UPDATE cod.model_version
        SET status = 7
        WHERE model_version_id = {model_version_id}
        """.format(model_version_id=model_version_id)
    execute_statement(sql_statement, server=server_name)


    # Send email when completed
    user = get_model_properties(model_version_id)['inserted_by']
    print user
    message_for_body = '''
    <p>Hello {user},</p>
    <p>The hybrid of {global_model} and {developed_model} for {acause} \
        has failed.</p>
    <p>Please check the following log file:</p>
    <p>{log_filepath}</p>
    <p></p>
    '''.format(user=user, log_filepath=log_dir+'/hybridizer.txt', \
        global_model=global_model, developed_model=developed_model, \
        acause=cause_data[hybrid_model_properties['cause_id']]['acause'], \
        model_version_id=model_version_id)
    send_email(['USERNAME'],
        "Hybrid of {global_model} and {developed_model} ({acause}) failed".\
            format(global_model=global_model,
            developed_model=developed_model,
            acause=cause_data[hybrid_model_properties['cause_id']]['acause']),
            message_for_body)
