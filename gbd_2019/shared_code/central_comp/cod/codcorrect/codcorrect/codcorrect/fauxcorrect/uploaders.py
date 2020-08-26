from glob import glob
import logging
import os
from typing import List, Optional

from db_tools import ezfuncs, loaders
import sqlalchemy
from sqlalchemy.exc import IntegrityError

from fauxcorrect.parameters.master import (
    MachineParameters, CoDCorrectParameters)
from fauxcorrect.utils import constants


class Uploader:

    DATABASE: Optional[str] = None
    CONN_DEF: Optional[str] = None
    SCHEMA: Optional[str] = None

    def __init__(self, version: MachineParameters, measure_id: int,
                 upload_type='single'):
        self.version: MachineParameters = version
        self.measure_id: int = measure_id
        self.upload_type: str = upload_type
        self._table_name = None
        self.session: Optional[sqlalchemy.orm.session.Session] = None
        self.infiler: Optional[loaders.Infiles] = None

    @property
    def session(self) -> sqlalchemy.orm.session.Session:
        if not self._session:
            self.session = ezfuncs.get_session(self.CONN_DEF)
        return self._session

    @session.setter
    def session(self, value: sqlalchemy.orm.session.Session) -> None:
        self._session = value

    @property
    def infiler(self) -> loaders.Infiles:
        if not self._infiler:
            self.infiler = loaders.Infiles(
                table=self.table_name,
                schema=self.SCHEMA,
                session=self.session
            )
        return self._infiler

    @infiler.setter
    def infiler(self, value: loaders.Infiles) -> None:
        self._infiler = value

    @property
    def summary_directories(self) -> List[str]:
        raise NotImplementedError

    @property
    def table_name(self) -> str:
        raise NotImplementedError

    def upload(self):
        for directory in self.summary_directories:
            logging.info(f"Uploading directory: {directory}")
            self.infiler.indir(
                path=directory,
                with_replace=False,
                partial_commit=True,
                commit=True,
                no_raise=(IntegrityError)
            )

    @classmethod
    def create_constructor(cls, database: str):
        for child in cls.__subclasses__():
            if getattr(child, 'DATABASE') == database:
                return child


class GbdUploader(Uploader):

    DATABASE: str = constants.DataBases.GBD
    CONN_DEF: str = constants.ConnectionDefinitions.GBD
    SCHEMA: str = constants.GBD.DataBase.SCHEMA

    @property
    def summary_directories(self) -> List[str]:
        return sorted(glob(
            os.path.join(
                self.version.parent_dir,
                constants.FilePaths.SUMMARY_DIR,
                constants.FilePaths.GBD_UPLOAD,
                self.upload_type,
                str(self.measure_id),
                '*'
            )
        ))

    @property
    def table_name(self) -> str:
        if not self._table_name:
            self._table_name = self.version.GBD_UPLOAD_TABLE.format(
                upload_type=self.upload_type,
                process_version_id=self.version.gbd_process_version_id)
        return self._table_name


class CodUploader(Uploader):

    DATABASE: str = constants.DataBases.COD
    CONN_DEF: str = constants.ConnectionDefinitions.COD
    SCHEMA: str = constants.COD.DataBase.SCHEMA

    @property
    def summary_directories(self) -> List[str]:
        return sorted(glob(
            os.path.join(
                self.version.parent_dir,
                constants.FilePaths.SUMMARY_DIR,
                constants.FilePaths.COD_UPLOAD,
                str(self.measure_id),
                '*'
            )
        ))

    @property
    def table_name(self) -> str:
        if not self._table_name:
            self._table_name = self.version.COD_UPLOAD_TABLE
        return self._table_name


class CodCorrectDiagnosticUploader(Uploader):

    DATABASE: str = constants.DataBases.CODCORRECT
    CONN_DEF: str = constants.ConnectionDefinitions.CODCORRECT
    SCHEMA: str = constants.Diagnostics.DataBase.SCHEMA

    # Diagnostic upload does not apply to fauxcorrect, multi uploads, ylls
    def __init__(self, version: CoDCorrectParameters, measure_id: int,
                 upload_type='single'):
        if not isinstance(version, CoDCorrectParameters):
            raise ValueError(
                "Diagnostic uploads not applicable to fauxcorrect. "
                f"Got version {version}")
        if measure_id != constants.Measures.Ids.DEATHS:
            raise ValueError(
                "Diagnostic uploads not applicable to YLLs. Got measure_id "
                "{measure_id}")
        if upload_type != constants.FilePaths.SINGLE_DIR:
            raise ValueError(
                "Diagnostic uploads not applicable multi year. "
                f"Got type {upload_type}")

        super(CodCorrectDiagnosticUploader, self).__init__(
            version=version,
            measure_id=measure_id,
            upload_type=upload_type
        )

    @property
    def summary_directories(self) -> List[str]:
        return [
            os.path.join(
                self.version.parent_dir,
                constants.FilePaths.DIAGNOSTICS_DIR,
                constants.FilePaths.DIAGNOSTICS_UPLOAD_FILE
            )
        ]

    def upload(self):
        for f in self.summary_directories:
            logging.info(f"Uploading file: {f}")
            self.infiler.infile(
                path=f,
                with_replace=False,
                commit=True,
                partial_commit=True,
                no_raise=(IntegrityError)
            )

    @property
    def table_name(self) -> str:
        if not self._table_name:
            self._table_name = self.version.DIAGNOSTIC_UPLOAD_TABLE
        return self._table_name
