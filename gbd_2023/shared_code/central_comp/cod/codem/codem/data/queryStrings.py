"""Query strings that are used for the CODEm module query submodule.

They may need to be updated as the database changes but changes should only
really necessitate changes in this script of variables.
"""

from typing import Final

locQueryStr = """
SELECT
    lhh.path_to_top_parent,
    lhh.location_id,
    COALESCE(slhh.is_estimate, 0) AS standard_location
FROM
    shared.location_hierarchy_history lhh
LEFT JOIN
    shared.location_hierarchy_history slhh
    ON lhh.location_id = slhh.location_id
    AND slhh.location_set_version_id = :standard_location_set_version_id
    AND slhh.location_set_id = 101
WHERE
    lhh.location_set_version_id = :location_set_version_id
    AND lhh.most_detailed = 1
ORDER BY lhh.sort_order
"""

getDatabaseParamsStr = """
SELECT * FROM cod.model_version
WHERE model_version_id in :model_version_id
"""

metaQueryStr = """
SELECT DISTINCT
    model.covariate_model_version_id as covariate_model_id,
    model.lag,
    trans.transform_type_short,
    model.level,
    model.offset,
    model.direction,
    model.reference,
    model.site_specific,
    model.p_value
FROM
    cod.model_covariate model
LEFT JOIN shared.transform_type trans USING (transform_type_id)
WHERE model.model_version_id = :model_version_id
"""

covNameQueryStr = """
SELECT
    cmv.model_version_id AS covariate_model_id,
    cov.covariate_name_short AS stub,
    cov.covariate_id AS covariate_id
FROM
    covariate.model_version cmv
INNER JOIN
    shared.covariate cov
    ON cov.covariate_id = cmv.covariate_id
WHERE
    cmv.model_version_id IN :model_version_id
"""

submodel_query_str = """
    INSERT INTO cod.submodel_version (
    model_version_id ,
    submodel_type_id,
    submodel_dep_id,
    weight,
    rank
    )
    VALUES
    (:model_version_id, :submodel_type_id, :submodel_dep_id, :weight, :rank);
"""

submodel_cov_write_str = """
    INSERT INTO cod.submodel_version_covariate (
    submodel_version_id,
    covariate_model_version_id
    )
    VALUES
    (:submodel_version_id, :covariate_model_version_id);
"""

status_write = """
    INSERT INTO cod.model_version_log (
    model_version_id,
    model_version_log_entry
    )
    VALUES
    (:model_version_id, :model_version_log_entry);
"""


pv_write = """
    UPDATE cod.model_version SET {tag} = :value WHERE model_version_id = :model_version_id
"""

submodel_summary_query = """
    SELECT rank, weight,
        submodel_version_id,
        submodel_type_name as Type,
        submodel_dep_name as Dependent_Variable
    FROM  (SELECT * FROM
            cod.submodel_version WHERE model_version_id = :model_version_id
            ORDER BY rank) as model
    LEFT JOIN cod.submodel_type USING (submodel_type_id)
    LEFT JOIN cod.submodel_dep USING (submodel_dep_id);
"""

count_rows_query = """
    SELECT COUNT(1) AS count
    FROM {table}
    WHERE model_version_id = :model_version_id
"""

validation_table = """
<table class="reference" style="width:100%" border="1">
<tr>
    <th>Model</th>
    <th>RMSE In</th>
    <th>Rmse Out</th>
    <th>Trend In</th>
    <th>Trend Out</th>
    <th>Coverage In</th>
    <th>Coverage Out</th>
    <th>Mean Error In</th>
    <th>Mean Error Out</th>
</tr>
<tr>
    <td>Ensemble</td>
    <td>{RMSE_in_ensemble: .4f}</td>
    <td>{RMSE_out_ensemble: .4f}</td>
    <td>{trend_in_ensemble: .4f}</td>
    <td>{trend_out_ensemble: .4f}</td>
    <td>{coverage_in_ensemble: .4f}</td>
    <td>{coverage_out_ensemble: .4f}</td>
    <th>{mean_error_in_ensemble: .4f}</th>
    <th>{mean_error_out_ensemble: .4f}</th>
</tr>
<tr>
    <td>Top SubModel</td>
    <td>--</td>
    <td>{RMSE_out_sub: .4f}</td>
    <td>--</td>
    <td>{trend_out_sub: .4f}</td>
    <td>--</td>
    <td>{coverage_out_sub: .4f}</td>
    <td>--</td>
    <td>{mean_error_out_sub: .4f}</td>
</tr>
</table>
"""

frame_work = """
<style>
table, th, td {{
    border: 1px solid black;
    border-collapse: collapse;
    padding: 3px;
    text-align: right;
}}
</style>
<p>Hi {user},</p>
<p>Your {cause_name} model, "{description}", sex {sex}, age {start_age}-{end_age} has completed! Full results can be viewed using the CoDViz tool found here: <a href="{codx_link}">{codx_link}</a>. Graphs of the Global estimates are attached to this email.</p>
<p>The ensemble model chose a PSI of {best_psi}. Predictive validity for the model is summarized below. <br> {validation_table} </p>
<p>The model included {number_of_submodels} submodels which are summarized in the model type table below and in detail in the attached CSV. <br> {model_type_table} </p>
<p>There were {number_of_covariates} covariates selected, which are summarized below. The number of submodels and the draws (total number of draws from submodels with that covariate) associated with each covariate are plotted in the attachments. <br> {covariate_table} </p>
<p>Additional outputs are located at {path_to_outputs}.</p>
<p>CODEm</p>
"""  # noqa: E501
