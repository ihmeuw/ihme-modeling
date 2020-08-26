import argparse
import subprocess
from typing import Dict

from db_tools.ezfuncs import query, get_session
from db_tools.loaders import Infiles

from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd_outputs_versions.compare_version import CompareVersion
from gbd_outputs_versions.convenience import bleeding_edge
from gbd_outputs_versions.gbd_process import GBDProcessVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv

import mmr_constants


class Uploader(object):

    def __init__(self, conn_def, codcorrect_vers=None, decomp_step=None,
        gbd_round_id=None):
        self.conn_def = conn_def
        self.codcorrect_vers = codcorrect_vers
        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id
        if decomp_step:
            self.decomp_step_id = decomp_step_id_from_decomp_step(
                self.decomp_step, self.gbd_round_id)

    def get_git_hash(self):
        try:
            hash = str(subprocess.check_output(["git", "rev-parse", "HEAD"]))
        except:
            hash = "test git hash"
        return hash

    def _get_metadata_dict(self):
        metadata_dict: Dict[int, int] = {}
        q = """
            SELECT gbd_process_version_id FROM gbd_process_version_metadata
            WHERE metadata_type_id = :metadata_type_id
            ORDER BY last_updated DESC
            LIMIT 1
        """
        for gbd_meta_type_id in mmr_constants.REQUIRED_GBD_METADATA_TYPE_IDS:
            if (
                gbd_meta_type_id ==
                mmr_constants.CODCORRECT_GBD_METADATA_TYPE_ID
            ):
                metadata_dict[gbd_meta_type_id] = self.codcorrect_vers
            else:
                raise NotImplementedError('MMR only needs codcorrect!')
                # metadata_dict[gbd_meta_type_id] = query(
                #     q,
                #     parameters={'metadata_type_id': gbd_meta_type_id},
                #     conn_def='gbd'
                # )['gbd_process_version_id'].iat[0]

        return metadata_dict
    
    def create_tables(self):
        code_version = self.get_git_hash().rstrip('\n')
        description = (
            f"Annual MMR Upload from CoDCorrect vers {self.codcorrect_vers}. "
            f"Regional scalar correction.")
        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=mmr_constants.MMR_GBD_PROCESS_ID,
            gbd_process_version_note=description,
            code_version=code_version,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            metadata=self._get_metadata_dict(),
            env=DBEnv.PROD)
        return pv.gbd_process_version_id

    def prep_upload(self):
        process_vers = self.create_tables()
        return process_vers

    def activate_process_version(self, process_version_id, session):
        pv = GBDProcessVersion(process_version_id, env=DBEnv.PROD)
        pv.mark_best()

    def create_compare_version(self, process_version_id):

        cv = CompareVersion.get_best(
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step)
        
        if not cv:
            cv = CompareVersion.add_new_version(
                self.gbd_round_id,
                self.decomp_step,
                'MMR: ' + process_version_id
            )

        cv.add_process_version(process_version_id)

        bleeding_edge_df = bleeding_edge(cv.compare_version_id)

        # Add bleeding edge processes if they aren't MMR
        for idx in bleeding_edge_df.index.tolist():
            if (
                bleeding_edge_df.loc[
                    idx, 'gbd_process_id'
                ] != mmr_constants.MMR_GBD_PROCESS_ID
            ):
                cv.add_process_version(
                    bleeding_edge_df.loc[idx, 'gbd_process_version_id'])

    def _get_latest_compare_version(self):
        q = """
        SELECT compare_version_id
        FROM gbd.compare_version
        WHERE decomp_step_id = :decomp_step_id
        ORDER BY last_updated DESC
        LIMIT 1
        """
        res = query(
            q,
            parameters={'decomp_step_id': self.decomp_step_id},
            conn_def='gbd')

        if res.empty:
            raise RuntimeError(
                f'could not get most recent compare_version_id with '
                f'decomp_step_id {self.decomp_step_id}')

        return res['compare_version_id'].iat[0]
        

    def upload(self, sm, process_vers, in_dir, with_replace=True):
        '''Args: sm: 'single' or 'multi', referring to which table to use
                 process_vers: comes from the output of above
                 in_dir: filepath of csvs to be uploaded
                 conn_def: 'gbd' or 'gbd_test'
        '''
        table = f'output_mmr_{sm}_year_v{process_vers}'
        sesh = get_session(self.conn_def)
        inf = Infiles(table=table, schema='gbd', session=sesh)
        inf.indir(path=in_dir, with_replace=with_replace,
                  partial_commit=True, commit=True)
        self.activate_process_version(
            process_version_id=process_vers, session=sesh)
        self.create_compare_version(process_version_id=process_vers)
        print("Uploaded!")


if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("upload_type", type=str)
    parser.add_argument("process_vers", type=int)
    parser.add_argument("conn_def", type=str)
    parser.add_argument("in_dir", type=str)
    args = parser.parse_args()

    Uploader(args.conn_def).upload(args.upload_type, args.process_vers,
        args.in_dir)
