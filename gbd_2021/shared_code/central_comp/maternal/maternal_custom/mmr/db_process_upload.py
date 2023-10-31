import argparse
import subprocess
import logging
from typing import Dict

from db_tools.ezfuncs import query, get_session
from db_tools.loaders import Infiles

from gbd.decomp_step import decomp_step_id_from_decomp_step
from gbd_outputs_versions.compare_version import CompareVersion
from gbd_outputs_versions.convenience import bleeding_edge
from gbd_outputs_versions.gbd_process import GBDProcessVersion
from gbd_outputs_versions import db as gbd_output_db

import mmr_constants


class Uploader(object):

    def __init__(
        self,
        codcorrect_vers: int,
        decomp_step: str,
        gbd_round_id: int,
        db_env: str
    ):

        self.codcorrect_vers = codcorrect_vers
        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id
        self.db_env = (
            gbd_output_db.DBEnvironment.PROD if db_env == 'prod'
            else gbd_output_db.DBEnvironment.DEV
        )

        self.decomp_step_id = decomp_step_id_from_decomp_step(
            self.decomp_step, self.gbd_round_id
        )

    def get_git_hash(self):
        try:
            git_hash = str(subprocess.check_output(["git", "rev-parse", "HEAD"]))
        except Exception:
            git_hash = "test git hash"
        return git_hash

    def _get_metadata_dict(self):
        """MMR requires metadata to create a process version."""
        metadata_dict: Dict[int, int] = {}
        for gbd_metadata_type in mmr_constants.MMR_METADATA_TYPE_IDS:
            if (gbd_metadata_type == 'codcorrect'):
                metadata_dict[
                    mmr_constants.MMR_METADATA_TYPE_IDS[gbd_metadata_type]
                ] = self.codcorrect_vers
            else:
                raise NotImplementedError(
                    'MMR only supports the following metadata types: {}'.format(
                        mmr_constants.MMR_METADATA_TYPE_IDS.keys()
                    )
                )

        return metadata_dict

    def create_tables(self):
        code_version = self.get_git_hash().rstrip('\n')
        description = (
            f"Annual MMR Upload from CoDCorrect vers {self.codcorrect_vers}. "
            f"Regional scalar correction.")

        # dev versions check dev cod for metadata, so we just omit it.
        if self.db_env == gbd_output_db.DBEnvironment.DEV:
            metadata = {}
        else:
            metadata = self._get_metadata_dict()

        pv = GBDProcessVersion.add_new_version(
            gbd_process_id=mmr_constants.MMR_GBD_PROCESS_ID,
            gbd_process_version_note=description,
            code_version=code_version,
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step,
            metadata=metadata,
            env=self.db_env)
        return pv.gbd_process_version_id

    def prep_upload(self):
        process_vers = self.create_tables()
        return process_vers

    def activate_process_version(self, process_version_id, session):
        pv = GBDProcessVersion(process_version_id, env=self.db_env)
        pv.mark_best()

    def create_compare_version(self, process_version_id):

        cv = CompareVersion.get_best(
            gbd_round_id=self.gbd_round_id,
            decomp_step=self.decomp_step)

        if not cv:
            cv = CompareVersion.add_new_version(
                self.gbd_round_id,
                self.decomp_step,
                'MMR: ' + str(process_version_id)
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

    def upload(self, upload_type: str, process_vers: int, in_dir, with_replace=True):
        '''
        Args: 
            upload_type: 'single' or 'multi', referring to which table to use
            process_vers: associated gbd_process_version
            in_dir: filepath of csvs to be uploaded
            with_replace: boolean, passed into Infile.indir
        '''
        table = f'output_mmr_{upload_type}_year_v{process_vers}'
        sesh = get_session(gbd_output_db.env_to_default_conn_def(self.db_env))
        infile_obj = Infiles(table=table, schema='gbd', session=sesh)
        infile_obj.indir(
            path=in_dir, with_replace=with_replace,
            partial_commit=True, commit=True)
        self.activate_process_version(
            process_version_id=process_vers, session=sesh)
        self.create_compare_version(process_version_id=process_vers)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--upload-type", type=str)
    parser.add_argument("--process-vers", type=int)
    parser.add_argument("--in-dir", type=str)
    parser.add_argument("--codcorrect-version", type=str)
    parser.add_argument("--gbd-round-id", type=int)
    parser.add_argument("--decomp-step", type=str)
    parser.add_argument("--db-env", type=str)
    args = parser.parse_args()

    uploader_obj = Uploader(
        args.codcorrect_version,
        args.decomp_step,
        args.gbd_round_id,
        args.db_env
    )
    uploader_obj.upload(args.upload_type, args.process_vers, args.in_dir)
