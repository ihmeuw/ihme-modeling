import argparse
import ast
from db_tools.ezfuncs import get_session
from db_tools.query_tools import exec_query
from cause_mvid_info import pull_mvid

def run_parent_adjust(cause_id, children, mvt, vers, detail, sex):
    ##########################################
    #Add oldCorrect model and its children
    #into the cod.model_version_relation table
    #for codCorrect
    ##########################################
    session = get_session('cod')
    parent = pull_mvid(cause_id, mvt, sex=sex)
    assert parent, "No parent saved"
    parent = parent[0]
    if mvt == 9:
        desc = 'oldC v%s hybrid of %s (%s)' % (vers, children, detail)
    elif mvt == 10:
        desc = 'oldC v%s target (%s)' % (vers, detail)
    try:
        for child in children:
            query = """INSERT INTO cod.model_version_relation
                    (parent_id, child_id, model_version_relation_note)
                    VALUES ({parent}, {child}, "{desc}")
                    """.format(parent=parent, child=child, desc=desc)
            exec_query(query, session=session)
        session.commit()
    except:
        session.rollback()
        session.close()
        raise

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--cause_id",
            help="cause_id",
            type=int)
    parser.add_argument(
            "--children",
            help="children models",
            type=str)
    parser.add_argument(
            "--mvt",
            help="model version type",
            dUSERt=10,
            type=int)
    parser.add_argument(
            "--vers",
            help="version",
            dUSERt=1,
            type=int)
    parser.add_argument(
            "--detail",
            help="details",
            dUSERt="",
            type=str)
    parser.add_argument(
            "--sex",
            help="sex",
            dUSERt=1,
            type=int)
    args = parser.parse_args()
    cause_id = args.cause_id
    children = args.children
    children = ast.literal_eval(children)
    mvt = args.mvt
    vers = args.vers
    detail = args.detail
    sex = args.sex

    run_parent_adjust(cause_id, children, mvt, vers, detail, sex)
