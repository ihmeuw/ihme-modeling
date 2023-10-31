'''
These are the query strings that are used for the CODEm module query submodule.
They may need to be updated as the database changes but changes should only
really necessitate changes in this script of variables.
'''

codQueryStr = '''
SELECT
    d.cf_raw,
    d.cf_final AS cf,
    d.sample_size,
    d.variance_rd_logit_cf as gc_var_lt_cf,
    d.variance_rd_log_dr as gc_var_ln_rate,
    d.age_group_id AS age,
    d.sex_id AS sex,
    d.year_id AS year,
    d.location_id,
    d.representative_id AS national,
    dt.data_type_short AS source_type,
    rv.refresh_id AS refresh_id
FROM
    cod.cv_data d
INNER JOIN
    cod.cv_data_version dv
    ON d.data_version_id = dv.data_version_id
INNER JOIN
    cod.refresh_versions rv
    ON rv.data_version_id = d.data_version_id
INNER JOIN
    cod.data_type dt
    ON dv.data_type_id = dt.data_type_id
INNER JOIN
    shared.location_hierarchy_history lhh
    ON d.location_id = lhh.location_id
LEFT JOIN
    cod.outlier_history AS o
    ON o.source_id         = dv.source_id
    AND o.model_version_id = {model_version_id}
    AND o.nid              = dv.nid
    AND o.data_type_id     = dv.data_type_id
    AND o.location_id      = d.location_id
    AND o.year_id          = d.year_id
    AND o.age_group_id     = d.age_group_id
    AND o.sex_id           = d.sex_id
    AND o.cause_id         = d.cause_id
    AND o.site_id          = d.site_id
WHERE
    d.cause_id = {c}
    AND d.sex_id = {s}
    AND d.year_id >= {sy}
    AND d.age_group_id IN ({age_groups})
    AND lhh.location_set_version_id = {loc_set_id}
    AND (o.is_outlier IS NULL OR o.is_outlier = 0)
    AND lhh.most_detailed = 1
    AND rv.refresh_id = {rv}
'''

locQueryStr = '''
SELECT
    lhh.path_to_top_parent,
    lhh.location_id,
    COALESCE(slhh.is_estimate, 0) AS standard_location
FROM
    shared.location_hierarchy_history lhh
LEFT JOIN
    shared.location_hierarchy_history slhh
    ON lhh.location_id = slhh.location_id
    AND slhh.location_set_version_id = {s_loc_set_ver_id}
    AND slhh.location_set_id = 101
WHERE
    lhh.location_set_version_id = {loc_set_ver_id}
    AND lhh.most_detailed = 1
ORDER BY lhh.sort_order
'''

metaQueryStr = '''
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
WHERE model.model_version_id = {mvid}
'''

covNameQueryStr = '''
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
    cmv.model_version_id IN ({})
'''

submodel_query_str = '''
    INSERT INTO cod.submodel_version (
    model_version_id ,
    submodel_type_id,
    submodel_dep_id,
    weight,
    rank
    )
    VALUES

    ({0}, {1}, {2}, {3}, {4})
    ;
    '''

submodel_get_id = '''
    SELECT submodel_version_id FROM cod.submodel_version
    WHERE model_version_id = {0}
    AND rank = {1}
    ORDER BY date_inserted DESC;
'''

submodel_cov_write_str = '''
    INSERT INTO cod.submodel_version_covariate (
    submodel_version_id,
    covariate_model_version_id
    )
    VALUES

    ({0}, {1})
    ;
'''

status_write = '''
    INSERT INTO cod.model_version_log (
    model_version_id,
    model_version_log_entry
    )
    VALUES

    ({0}, '{1}')
    ;
'''


pv_write = '''
    UPDATE cod.model_version SET {0} = {1} WHERE model_version_id = {2}
'''

submodel_summary_query = '''
    SELECT rank, weight,
        submodel_version_id,
        submodel_type_name as Type,
        submodel_dep_name as Dependent_Variable
    FROM  (SELECT * FROM
            cod.submodel_version WHERE model_version_id = {0}
            ORDER BY rank) as model
    LEFT JOIN cod.submodel_type USING (submodel_type_id)
    LEFT JOIN cod.submodel_dep USING (submodel_dep_id);
'''

get_best_feeder_cause_ids = '''
    SELECT DISTINCT cause_id
    FROM cod.model_version
    WHERE gbd_round_id = :gbd_round_id AND
          decomp_step_id = :decomp_step_id AND
          model_version_type_id = 3 AND
          is_best = 1
'''

mark_model_best = '''
    UPDATE cod.model_version
    SET is_best = :is_best,
        best_start = :best_start,
        best_end = NULL,
        best_user = :best_user,
        best_description = :best_description
    WHERE model_version_id = :model_version_id
'''

unmark_model_best = '''
    UPDATE cod.model_version
    SET is_best = :is_best,
        best_end = :best_end
    WHERE model_version_id = :model_version_id
'''

validation_table = '''
<table class="reference" style="width:100%" border="1">
<tr>
    <th>Model</th>
    <th>RMSE In</th>
    <th>Rmse Out</th>
    <th>Trend In</th>
    <th>Trend Out</th>
    <th>Coverage In</th>
    <th>Coverage Out</th>
</tr>
<tr>
    <td>Ensemble</td>
    <td>{RMSE_in_ensemble: .4f}</td>
    <td>{RMSE_out_ensemble: .4f}</td>
    <td>{trend_in_ensemble: .4f}</td>
    <td>{trend_out_ensemble: .4f}</td>
    <td>{coverage_in_ensemble: .4f}</td>
    <td>{coverage_out_ensemble: .4f}</td>
</tr>
<tr>
    <td>Top SubModel</td>
    <td>--</td>
    <td>{RMSE_out_sub: .4f}</td>
    <td>--</td>
    <td>{trend_out_sub: .4f}</td>
    <td>--</td>
    <td>--</td>
</tr>
</table>
'''

frame_work = '''
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
'''

cause_name = '''
SELECT cause_name
FROM shared.cause
WHERE cause_id = {cause_id}
'''

model_date = '''
SELECT date(date_inserted) as model_date
FROM cod.model_version
WHERE model_version_id = {mvid}
'''

model_version_type_id = '''
SELECT model_version_type_id
FROM cod.model_version
WHERE model_version_id = {mvid}
'''
