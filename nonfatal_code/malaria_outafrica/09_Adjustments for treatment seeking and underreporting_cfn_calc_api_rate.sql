CREATE OR REPLACE FUNCTION rds.calc_api_rate(
  p_cases      double precision,
  p_population double precision,
  p_per        int DEFAULT 1000
) RETURNS double precision AS
$BODY$
BEGIN
  IF p_cases IS NULL AND
     p_population IS NULL
  THEN
    RAISE EXCEPTION
      'Can''t compute ''API'': ''cases'' and ''population'' are required parameters';
  ELSIF p_population IS NULL THEN
    RAISE EXCEPTION
      'Can''t compute ''API'': ''population'' is a required parameter';
  ELSIF p_cases IS NULL THEN
    RAISE EXCEPTION
      'Can''t compute ''API'': ''cases'' is a required parameter';
  ELSIF p_per < 0 THEN
    RAISE EXCEPTION
      'Cannot calculate cases per %: must be a positive integer', p_per;
  END IF;

  RETURN p_cases / p_population * p_per;
END;
$BODY$
  LANGUAGE plpgsql IMMUTABLE;
