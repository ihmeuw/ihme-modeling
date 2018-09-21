codQueryStr = '''SELECT
    d.cf_final AS cf,
    d.sample_size,
    d.age_group_id as age,
    d.sex_id as sex,
    d.year_id as year,
    d.location_id,
    d.representative_id as national,
    dt.data_type_short as source_type
FROM
    cod.cm_data d
    INNER JOIN
    cod.cm_data_version dv
    ON d.data_version_id = dv.data_version_id
    INNER JOIN
    cod.data_type dt
    ON dv.data_type_id = dt.data_type_id
    INNER JOIN
    shared.location_hierarchy_history lhh
    ON d.location_id = lhh.location_id
WHERE
    d.cause_id = {c}
    AND d.sex_id = {s}
    AND d.year_id >= {sy}
    AND dv.status = 1
    AND d.age_group_id >= {sa}
    AND d.age_group_id <= {ea}
    AND d.age_group_id IN (
        SELECT age_group_id FROM shared.age_group_set_list WHERE age_group_set_id = 12 AND is_estimate = 1)
    ## Note that age_group_set_id 12 is specific to gbd 2016
    AND lhh.location_set_version_id = {loc_set_id}
    AND d.is_outlier = 0
    AND lhh.is_estimate = 1
     '''

mortQueryStr = '''SELECT
    o.mean_env_hivdeleted AS envelope,
    o.mean_pop AS pop,
    o.location_id,
    o.year_id as year,
    o.age_group_id as age,
    o.sex_id as sex
FROM
    mortality.output o
    INNER JOIN
    mortality.output_version ov
    ON o.output_version_id = ov.output_version_id
    INNER JOIN
    shared.location_hierarchy_history lhh
    ON o.location_id = lhh.location_id
WHERE
    ov.is_best = 1
    AND o.age_group_id >= {sa}
    AND o.age_group_id <= {ea}
    AND o.age_group_id IN (
        SELECT age_group_id FROM shared.age_group_set_list WHERE age_group_set_id = 12 AND is_estimate = 1)
    ## Note that age_group_set_id 12 is specific to gbd 2016
    AND o.year_id >= {sy}
    AND o.sex_id = {s}
    AND lhh.location_set_version_id = {loc_set_id}
    # AND location_type_id NOT IN(1, 6, 7)
    AND lhh.is_estimate = 1
     '''

locQueryStr = '''SELECT
path_to_top_parent,
location_id
FROM
    shared.location_hierarchy_history lhh
WHERE
    lhh.location_set_version_id = {loc_set_ver_id}
    AND is_estimate = 1
ORDER BY sort_order
     '''

cvQueryStr = '''
SELECT
    m.mean_value,
    m.age_group_id AS age,
    m.sex_id AS sex,
    m.year_id AS year,
    m.location_id,
    cv.covariate_name_short AS name
FROM
    covariate.model m
    INNER JOIN
    covariate.model_version mv
    ON m.model_version_id = mv.model_version_id
    INNER JOIN
    covariate.data_version dv
    on mv.data_version_id = dv.data_version_id
    INNER JOIN
    shared.covariate cv
    ON dv.covariate_id = cv.covariate_id
    INNER JOIN
    shared.location_hierarchy_history lhh
    ON m.location_id = lhh.location_id
WHERE
    m.model_version_id = {mvid}
    AND
    (m.age_group_id IN (SELECT age_group_id
                        FROM shared.age_group_set_list
                        WHERE age_group_set_id = 12 AND is_estimate = 1)
    ## Note that age_group_set_id 12 is specific to gbd 2016
    OR m.age_group_id = 22)
    AND lhh.location_set_version_id = {loc_set_id}
    AND lhh.is_estimate = 1
'''

metaQueryStr = '''SELECT model_version_id as covariate_model_id,
                cov.covariate_name_short AS stub, lag,
                transform_type_short, level,
                direction, offset, reference, site_specific, p_value
                FROM (SELECT DISTINCT
                      model.covariate_model_version_id as model_version_id,
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
                      WHERE model.model_version_id = {0}
                      ) AS temp
                LEFT JOIN covariate.model_version USING (model_version_id)
                LEFT JOIN covariate.data_version USING (data_version_id)
                LEFT JOIN shared.covariate cov USING (covariate_id)
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
    SELECT rank,
        submodel_version_id,
        submodel_type_name as Type,
        submodel_dep_name as Dependent_Variable
    FROM  (SELECT * FROM
            cod.submodel_version WHERE model_version_id = {0}
            ORDER BY rank) as model
    LEFT JOIN cod.submodel_type USING (submodel_type_id)
    LEFT JOIN cod.submodel_dep USING (submodel_dep_id);
'''

codem_run_time = '''
SELECT TIMESTAMPDIFF(MINUTE,
       (SELECT date_inserted FROM cod.model_version_log
            WHERE model_version_id={model_version_id}
            AND model_version_log_entry ="Querying For model Parameters started."
            ORDER BY date_inserted DESC LIMIT 1),
       (SELECT date_inserted FROM cod.model_version_log
            WHERE model_version_id={model_version_id}
            AND model_version_log_entry ="Getting all the logistical stuff started."
            ORDER BY date_inserted DESC LIMIT 1)) as time;
'''

validation_table = '''
<table class="reference" style="width:100%" border="2">
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
    <td>USER</td>
    <td>{RMSE_out_sub: .4f}</td>
    <td>USER</td>
    <td>{trend_out_sub: .4f}</td>
    <td>USER</td>
    <td>USER</td>
</tr>
</table>
'''

frame_work = '''
<p>Hey {user},</p>
<p>Your model {description} in {sex} from age group {start_age} to {end_age} has completed!</p>
<p>Wall run time to compute the results was approximately {run_time} minutes.</p>
<p>Full results can be viewed using the Cod Viz tool found here:</p>
<p>{codx_link}</p>
<p>The ensemble model chose a psi of {best_psi}. Predictive validity for the model is:</p>
{validation_table}
<p>Graphs of the global estimates are attached to this email.</p>
<p>The model included {number_of_submodels} submodels which are summarized below.</p>
{submodel_table}
<p>Additional outputs are located at {path_to_outputs}</p>
<p>CODEm2.0</p>
'''
