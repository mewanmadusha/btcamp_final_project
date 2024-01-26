
-- This identifies consecutive rows (A+) in the ECDC_GLOBAL table where the number of cases (A.CASES) is greater than 100. The query selects the first and last rows of each identified sequence, providing the country region, continent, ISO code, and the starting and ending number of cases.
SELECT *
FROM ECDC_GLOBAL
MATCH_RECOGNIZE (
  PARTITION BY COUNTRY_REGION
  ORDER BY DATE
  MEASURES
    FIRST(A.COUNTRY_REGION) AS COUNTRY_REGION_START,
    FIRST(A.CONTINENTEXP) AS CONTINENTEXP,
    FIRST(A.ISO3166_1) AS ISO3166_1,
    FIRST(A.CASES) AS START_CASES,
    LAST(A.CASES) AS END_CASES
  ONE ROW PER MATCH
  PATTERN (A+)
  DEFINE
    A AS A.CASES > 100
);
