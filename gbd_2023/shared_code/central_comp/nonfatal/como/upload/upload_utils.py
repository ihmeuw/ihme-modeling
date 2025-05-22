"""Dual upload utils."""

from typing import Optional

from pydantic.dataclasses import dataclass as pydantic_dataclass

from gbd import constants as gbd_constants

from como.lib import constants as como_constants


@pydantic_dataclass
class UploadTask:
    """Dataclass for storing arguments used in public and internal uploads."""

    component: str
    process_version_id: int
    measure_id: int
    como_dir: str
    location_id: int
    year_type: como_constants.YearType
    public_upload_test: Optional[bool] = None
    process_id: int = gbd_constants.gbd_process.EPI
