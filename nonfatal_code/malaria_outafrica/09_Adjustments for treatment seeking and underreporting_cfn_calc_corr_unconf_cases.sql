CREATE OR REPLACE FUNCTION rds.calc_corr_unconf_cases(
  p_unconf_cases double precision,
  p_spr          double precision
) RETURNS double precision AS
$BODY$
BEGIN
  IF p_unconf_cases IS NULL OR
     p_spr IS NULL
  THEN
    RAISE EXCEPTION
      'Can''t compute ''corrected unconfirmed cases'': ''unconfirmed cases'' and ''spr'' parameters are required';
  ELSIF p_spr > 1.0 OR p_spr < 0 THEN
    RAISE EXCEPTION
      'Invalid ''spr'' value %: must be in the range 0-1', p_spr;
  END IF;

  RETURN p_unconf_cases * p_spr;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE;
