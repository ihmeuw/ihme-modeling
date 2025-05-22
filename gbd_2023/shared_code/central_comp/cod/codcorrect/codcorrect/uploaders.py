import logging
from typing import List, Optional

import sqlalchemy
from sqlalchemy.exc import IntegrityError

import db_tools_core
from db_tools import loaders
from gbd.constants import measures

from codcorrect.legacy.parameters.machinery import MachineParameters
from codcorrect.legacy.utils import constants
from codcorrect.lib import db

logger = logging.getLogger(__name__)


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
            self.session = db_tools_core.get_session(self.CONN_DEF)
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
        return self.version.file_system.get_summary_directories(
            self.DATABASE, self.measure_id, summary_type=self.upload_type,
        )

    @property
    def table_name(self) -> str:
        raise NotImplementedError

    def upload(self):
        for directory in self.summary_directories:
            logger.info(f"Uploading directory: {directory}")
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

    DATABASE: Optional[str] = None
    CONN_DEF: Optional[str] = None
    SCHEMA: Optional[str] = None

    @property
    def table_name(self) -> str:
        if not self._table_name:
            self._table_name = self.version.GBD_UPLOAD_TABLE.format(
                upload_type=self.upload_type,
                process_version_id=self.version.gbd_process_version_id)
        return self._table_name


class CodUploader(Uploader):
    """Uploader class for the db.

    The cod upload differs from the uploads to the other databases. Historically for the
    cod upload, CodCorrect would create a partition on TABLE to upload to that table.
    However, as all CodCorrect estimates are uploaded to the same table, unlike the
    gbd upload where each new CodCorrect creates its own table.
    """

    DATABASE: Optional[str] = None
    CONN_DEF: Optional[str] = None
    SCHEMA: Optional[str] = None

    # Create temporary cod output upload table upon initialization.
    # Preferred immediately before upload to avoid creating an upload table
    def __init__(
        self, version: MachineParameters, measure_id: int, upload_type: str = "single"
    ):
        super(CodUploader, self).__init__(
            version=version,
            measure_id=measure_id,
            upload_type=upload_type,
        )
        self.cod_output_version_id = version.cod_output_version_id
        self._table_name = db.create_cod_output_table(
            version.cod_output_version_id, self.session
        )

    def upload(self) -> None:
        """Call parent upload method and then run clean-up sproc."""
        super(CodUploader, self).upload()
        db.finalize_cod_output_table(self.cod_output_version_id, self.session)

    @property
    def table_name(self) -> str:
        return self._table_name


class CodCorrectDiagnosticUploader(Uploader):

    DATABASE: Optional[str] = None
    CONN_DEF: Optional[str] = None
    SCHEMA: Optional[str] = None

    # Diagnostic upload does not apply to multi uploads, ylls
    def __init__(self, version: MachineParameters, measure_id: int,
                 upload_type='single'):
        if measure_id != measures.DEATH:
            raise ValueError(
                "Diagnostic uploads not applicable to YLLs. Got measure_id "
                f"{measure_id}")
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
    def table_name(self) -> str:
        if not self._table_name:
            self._table_name = self.version.DIAGNOSTIC_UPLOAD_TABLE
        return self._table_name
