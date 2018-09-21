import subprocess
import sys

from db_tools.ezfuncs import query, get_session
from db_tools.query_tools import exec_query
from db_tools.loaders import Infiles


class Uploader(object):
    def __init__(self, gbd_round_id, conn_def):
        self.gbd_round_id = gbd_round_id
        self.conn_def = conn_def
        self.codcorrect_vers = self.get_best_codcorrect_vers()

    def get_best_codcorrect_vers(self):
        q = ('SELECT output_version_id FROM cod.output_version '
             'WHERE status = 1 and is_best = 1 and code_version = {}'
             .format(self.gbd_round_id))
        result = query(q, conn_def='cod')
        return result.ix[0, 'output_version_id']

    def get_kit_version_id(self):
        q = ('SELECT MAX(kit_version_id) AS kit_vers '
             'FROM gbd.kit_version '
             'WHERE kit_id = 2')
        vers_df = query(q, conn_def=self.conn_def)
        return int(vers_df.loc[0, 'kit_vers'])

    def add_kit_vers(self):
        sesh = get_session(self.conn_def)
        query = ('INSERT INTO gbd.kit_version '
                 '(kit_id, kit_version_note, '
                 'gbd_round_id, kit_version_status_id) '
                 'VALUES '
                 '(2, "MMR run for codcorrect version {}", {}, -1)'
                 .format(self.codcorrect_vers, self.gbd_round_id))
        exec_query(query, session=sesh, close=True)

    def get_git_hash(self):
        return str(subprocess.check_output(["git", "rev-parse", "HEAD"]))

    def create_tables(self):
        self.add_kit_vers()
        kit_version_id = self.get_kit_version_id()
        code_version = self.get_git_hash().rstrip('\n')
        description = ("MMR Upload from CoDCorrect vers {}"
                       .format(self.codcorrect_vers))
        q = ('CALL gbd.new_gbd_process_version({}, 12, "{}", "{}", '
             'NULL, {})'.format(self.gbd_round_id, description,
                                code_version, kit_version_id))
        process_vers = query(q, conn_def=self.conn_def)
        return int(process_vers.ix[0, 'v_return_string'].split()[2]
                   .replace('"', "").replace(",", ""))

    def add_metadata_to_process_version(self, process_version_id):
        sesh = get_session(self.conn_def)
        query = ('INSERT INTO gbd.gbd_process_version_metadata ('
                 'gbd_process_version_id, '
                 'metadata_type_id, '
                 'val) '
                 'VALUES '
                 '({pv_id}, '
                 '1, '
                 '"{ov_id}")').format(pv_id=process_version_id,
                                      ov_id=self.codcorrect_vers)
        exec_query(query, session=sesh, close=True)

    def prep_upload(self, conn_def):
        process_vers = self.create_tables()
        self.add_metadata_to_process_version(process_vers)
        return process_vers

    def upload(self, sm, process_vers, in_dir, with_replace=True):
        '''Args: sm: 'single' or 'multi', referring to which table to use
                 process_vers: comes from the output of above
                 in_dir: filepath of csvs to be uploaded
                 conn_def: 'gbd' or 'gbd_test'
        '''
        table = 'output_mmr_%s_year_v%s' % (sm, process_vers)
        sesh = get_session(self.conn_def)
        inf = Infiles(table=table, schema='gbd', session=sesh)
        inf.indir(path=in_dir, with_replace=with_replace,
                  partial_commit=True, commit=True)
        print "Uploaded!"


if __name__ == '__main__':
    upload_type, process_vers, gbd_round_id, conn_def, in_dir = sys.argv[1:6]
    Uploader(gbd_round_id, conn_def).upload(upload_type, process_vers, in_dir)
