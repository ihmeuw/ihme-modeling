import re
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders import get_current_location_hierarchy

CONF = Configurator()


def is_country_vr(model_group, return_bool=True):
    """
    Determine whether a given model is VR by country.
    """
    country_vr = re.match("VR-([A-Z]{3})", model_group)
    if return_bool:
        return bool(country_vr)
    else:
        return country_vr


def is_region_vr(model_group, return_bool=True):
    """
    Determine whether a given model group is VR by region.
    """
    region_vr = re.match("VR-(R\d{1,2})", model_group)
    if return_bool:
        return bool(region_vr)
    else:
        return region_vr


def is_super_region_va(model_group):
    """
    Determine whether a given model group is VA by super region.
    """
    return model_group in ['VA-158', 'VA-166', 'VA-137', 'VA-103', 'VA-4', 'VA-64', 'VA-31']


def is_country_vr_non_subnat(model_group):
    """
    Determine whether a given NR model is VR by country and non-subnational.
    """
    country_vr = is_country_vr(model_group, return_bool=False)
    country_vr_non_subnat = (
        country_vr and country_vr.group(1) not in CONF.get_id("subnational_modeled_iso3s"))
    return bool(country_vr_non_subnat)


def get_region_model_group(model_group, lh=None, **lh_kwargs):
    """
    For a VR country noise reduction model group,
    determine the corresponding region model group.
    """
    country_vr = is_country_vr(model_group, return_bool=False)
    assert country_vr,\
        "Only makes sense to get the region model group if this is country VR"
    iso3 = country_vr.group(1)
    if lh is None:
        lh = get_current_location_hierarchy(**lh_kwargs)
    assert iso3 in lh.iso3.unique(), f"{iso3} is not a valid country"

    region_mg = "VR-" + lh.loc[
        lh.location_id == lh.loc[
            lh.ihme_loc_id == iso3, 'region_id'].unique()[0],
        'ihme_loc_id'
    ].unique()[0]
    return region_mg


def get_fallback_model_group(model_group, **lh_kwargs):
    """
    Get fallback model group for super region VA and country VR model groups,
    used to noise reduce sparse causes.
    """
    if is_super_region_va(model_group):
        # All super region VA uses the global VA model
        # as its fallback
        return "VA-G"
    elif is_country_vr_non_subnat(model_group):
        # Country VR uses region
        return get_region_model_group(model_group, **lh_kwargs)
    else:
        return "NO_NR"
