import pandas as pd
from spxmod.model import XModel


def get_coef(model: XModel) -> pd.DataFrame:
    regmod_model = model.core
    names = [v.name for v in regmod_model.params[0].variables]
    coefs = regmod_model.opt_coefs
    df_coef = pd.DataFrame(dict(name=names, coef=coefs))

    spans = pd.concat(
        [
            var_builder.space.span.assign(cov=var_builder.name)
            for var_builder in model.var_builders
        ],
        axis=0,
        ignore_index=True,
    )
    for col in spans.columns:
        if col.endswith("_id"):
            spans[col] = spans[col].fillna(-1)
            spans[col] = spans[col].astype(int)

    df_coef = pd.concat([df_coef, spans], axis=1)
    return df_coef
