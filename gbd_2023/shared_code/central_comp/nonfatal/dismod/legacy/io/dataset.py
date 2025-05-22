from dataclasses import dataclass


@dataclass()
class Dataset:
    """Class representing a logical dataset stored on disk"""

    name: str
    file_name: str
    extension: str = "pq"


CASCADE_ROOT_DATASETS = [
    Dataset("simple", "simple", extension="csv"),
    Dataset("value", "value", extension="csv"),
]

_input_datasets = [
    Dataset("data", "data"),
    Dataset("data_noarea", "data_noarea"),
    Dataset("data_ally", "data_ally"),
    Dataset("data_pred", "datapred_noarea"),
    Dataset("effect", "effect"),
    Dataset("predin", "pred_mesh"),
    Dataset("rate", "rate"),
    Dataset("prior_upload", "model_prior"),
] + CASCADE_ROOT_DATASETS


_output_datasets = [
    Dataset("posterior", "post_ode"),
    Dataset("posterior_summ", "post_ode_summary"),
    Dataset("posterior_upload", "model_estimate_fit"),
    Dataset("adj_data_upload", "model_data_adj"),
    Dataset("info", "post_info"),
    Dataset("allyadjout", "post_ally_adj"),
    Dataset("dataadjout", "post_data_adj"),
    Dataset("datapredout", "post_data_pred"),
    Dataset("drawout", "post_pred_draws"),
    Dataset("child_prior", "child_priors"),
    Dataset("drawout_summ", "post_pred_draws_summary"),
    Dataset("effect_upload", "model_effect"),
]

_input_datasets_name_map = {ds.name: ds for ds in _input_datasets}
_output_datasets_name_map = {ds.name: ds for ds in _output_datasets}
ALL_DATASETS = {**_input_datasets_name_map, **_output_datasets_name_map}
