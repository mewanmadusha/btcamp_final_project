-- main query
-- SELECT PEOPLE_TOTAL_2ND_DOSE/POPULATION RATIO_VACCINATED, *
-- FROM JHU_VACCINES A
-- JOIN (
--     SELECT STATE, SUM(TOTAL_POPULATION) POPULATION
--     FROM DEMOGRAPHICS
--     GROUP BY 1
-- ) B
-- ON A.STABBR=B.STATE
-- QUALIFY ROW_NUMBER() OVER(PARTITION BY PROVINCE_STATE ORDER BY DATE DESC)=1
-- ORDER BY DATE DESC, RATIO_VACCINATED DESC;


-- Creating a materialized view for the demographics subquery
-- CREATE MATERIALIZED VIEW mv_demographics AS
-- SELECT state, SUM(total_population) AS population
-- FROM demographics
-- GROUP BY state;

EXPLAIN SELECT people_total_2nd_dose / population AS ratio_vaccinated, a.*, b.population
FROM jhu_vaccines a
JOIN (
   SELECT state, SUM(total_population) AS population
   FROM demographics
   GROUP BY state
) b ON a.stabbr = b.state
QUALIFY row_number() OVER (PARTITION BY province_state ORDER BY date DESC) = 1
ORDER BY date DESC, ratio_vaccinated DESC;