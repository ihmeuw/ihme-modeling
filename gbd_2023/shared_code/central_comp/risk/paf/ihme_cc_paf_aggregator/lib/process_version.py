import gbd_outputs_versions
from ihme_cc_cache import FileBackedCacheReader

from ihme_cc_paf_aggregator import __version__
from ihme_cc_paf_aggregator.lib import constants, types


def metadata_from_settings(settings: constants.PafAggregatorSettings) -> dict:
    """Metadata suitable for GBD outputs versions, from PAF settings."""
    return {
        constants.PAF_VERSION_METATADA: settings.version_id,
        constants.YEAR_IDS_METATADA: settings.year_id,
        constants.N_DRAWS_METATADA: settings.n_draws,
    }


def create_process_version(
    manifest_path: types.PathOrStr, mark_special: bool, env: str = constants.PROD
) -> gbd_outputs_versions.GBDProcessVersion:
    """Create a process version."""
    prod_enum = gbd_outputs_versions.DBEnvironment.PROD
    env_enum = prod_enum if env == constants.PROD else gbd_outputs_versions.DBEnvironment.DEV

    cache_reader = FileBackedCacheReader(manifest_path)
    age_meta_from_draw = cache_reader.get(constants.PostRunCacheContents.AGE_META_FROM_DRAW)
    settings = constants.PafAggregatorSettings(
        **cache_reader.get(constants.CacheContents.SETTINGS)
    )
    metadata = (
        age_meta_from_draw.groupby(constants.MEASURE_ID)
        .apply(lambda subdf: subdf[constants.AGE_GROUP_ID].tolist())
        .to_dict()
    )
    metadata_arg = {
        constants.MEASURE_IDS_METADATA: list(metadata.keys()),
        constants.AGE_GROUP_IDS_METADATA: metadata,
    }
    metadata_arg.update(metadata_from_settings(settings))

    if mark_special:
        metadata_arg.update({constants.THREE_FOUR_FIVE_METADATA: 1})
        status = constants.SPECIAL_STATUS
    else:
        status = constants.ACTIVE_STATUS

    pv = gbd_outputs_versions.GBDProcessVersion.add_new_version(
        gbd_process_id=constants.GBD_PROCESS_ID,
        gbd_process_version_note=description_from_version(settings.version_id),
        code_version=__version__,
        metadata=metadata_arg,
        release_id=settings.release_id,
        env=env_enum,
        validate_env=prod_enum,
    )

    pv.update_status(status)
    return pv


def description_from_version(output_version_id: int) -> str:
    """Create description for gbd process version."""
    return f"ihme_cc_paf_aggregator v{output_version_id}"
