CREATE OR REPLACE FUNCTION rds.calc_cases_gbd2016(
  p_conf_cases        DOUBLE PRECISION,
  p_corr_unconf_cases DOUBLE PRECISION,
  p_reporting_compl   DOUBLE PRECISION,
  p_cases_calc_method TEXT DEFAULT 'GBD2016_LOWER_ESTIMATE',
  p_public_ts         DOUBLE PRECISION DEFAULT 0,
  p_any_ts            DOUBLE PRECISION DEFAULT 0
)
  RETURNS DOUBLE PRECISION AS
$BODY$
DECLARE
  v_cases DOUBLE PRECISION;
BEGIN
  -- Override the treatment seeking figures where the formula used does not require the modifier

  -- 'GBD2016_LOWER_ESTIMATE' cases estimate is the only one which requires the 'any treatment
  -- seeking' modifier
  IF p_cases_calc_method != 'GBD2016_LOWER_ESTIMATE'
  THEN
    p_any_ts = 1.0;
  END IF;

  -- Validate that the required parameters are present
  IF p_conf_cases IS NULL OR
     p_corr_unconf_cases IS NULL OR
     p_reporting_compl IS NULL
  THEN
    RAISE EXCEPTION
    'Can''t compute ''cases'': required parameters are not all supplied: received confirmed cases %, corrected unconfirmed cases %, reporting completeness %',
    p_conf_cases, p_corr_unconf_cases, p_reporting_compl;
  ELSIF p_public_ts <= 0 OR
        p_any_ts <= 0 OR
        p_public_ts IS NULL OR
        p_any_ts IS NULL
    THEN
      RAISE EXCEPTION
      'The ''public'' and ''any'' treatment seeking values must be >0 when using the ''GBD2016_LOWER_ESTIMATE'' cases formula: received public % and any %',
        p_public_ts, p_any_ts;
  END IF;

  IF p_cases_calc_method = 'GBD2016_LOWER_ESTIMATE'
  THEN
    v_cases = (p_conf_cases + p_corr_unconf_cases) * p_any_ts / (p_reporting_compl * p_public_ts);
  ELSIF p_cases_calc_method = 'GBD2016_UPPER_ESTIMATE'
    THEN
      v_cases = (p_conf_cases + p_corr_unconf_cases) / (p_reporting_compl * p_public_ts);
  ELSE -- Stipulated calculation method not (yet) supported
    RAISE EXCEPTION
    'Unknown cases calculation method %', p_cases_calc_method;
  END IF;

  RETURN v_cases;
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE;
