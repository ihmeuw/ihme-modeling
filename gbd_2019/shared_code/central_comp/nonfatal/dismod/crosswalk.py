import logging
from cascade_ode import db


def row_table_name_from_model_version_id(model_version_id):

    return db.execute_select(
        f"""
        select row_table_name from crosswalk_version.crosswalk_version
        JOIN
        epi.model_version using (crosswalk_version_id)
        where
            model_version_id = {model_version_id}
        """).row_table_name.iat[0]


def verify_no_crosswalking(df, raise_exc=True):
    """
    Since we're doing out of dismod crosswalking now, we want to make sure
    we don't accidently introduce a mixture of single sex and both sex
    data. This function checks for that and raises a RuntimeError if it occurs.

    In theory, validations earlier in the workflow should prevent this from
    happening in bundle data.
    """
    assert 'x_sex' in df and 'integrand' in df, (
        "Malformed df -- should contain x_sex and integrand fields")

    test_df = df[df.integrand != 'mtall'][['x_sex', 'integrand']]

    sexes_in_df = set(test_df.x_sex)
    males_and_both = {0.5, 0}
    females_and_both = {-0.5, 0}

    illegal = (
        males_and_both.issubset(sexes_in_df)
        or
        females_and_both.issubset(sexes_in_df)
    )

    if illegal:
        counts = df.groupby(['integrand', 'x_sex']).size()
        msg = (
            f"Found mixture of sexes in df that could lead to crosswalking: "
            f"{sexes_in_df}. The row count breakdown by measure follows:\n "
            f"{counts}")
        if raise_exc:
            raise RuntimeError(msg)
        else:
            log = logging.getLogger(__name__)
            log.warning(msg)


def standardize_sex(df):
    """
    If we only have both-sex data, each male/female specific model will
    (depending on fix-sex setting) probably shift x_sex to be non zero. And
    then priors are added that are 0. So that introduces crosswalking. For
    the both-sex case, we want to set all rows so x_sex=0.
    """
    assert 'x_sex' in df and 'orig_sex' in df and 'integrand' in df, (
        "Malformed df -- should contain x_sex and orig_sex fields")

    only_both_sex = set(df.orig_sex) == {0}

    if not only_both_sex:
        return df

    df['x_sex'] = 0
    return df
