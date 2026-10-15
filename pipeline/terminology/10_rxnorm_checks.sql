-- =============================================================================
-- 10_rxnorm_checks.sql — the medication mapping step 1: register the pinned RxNorm release and check it is usable for
-- the mapping. TERMINOLOGY ONLY: no MIMIC data is read. Run with run_sql.py:
--   --db dqm_ref --param version=20261005 --param loc=s3://.../dqm/ref/rxnorm/2026-10-05
--   --param vsloc=s3://.../dqm/ref/valuesets/<run>
-- (value-set codes are uploaded first by fetch_valuesets_to_s3.py: oid, system, code)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dqm_ref;

CREATE EXTERNAL TABLE IF NOT EXISTS dqm_ref.rxnconso_{version} (
  rxcui string, lat string, ts string, lui string, stt string, sui string, ispref string,
  rxaui string, saui string, scui string, sdui string, sab string, tty string, code string,
  str string, srl string, suppress string, cvf string, trailing_empty string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE LOCATION '{loc}/rxnconso/';

CREATE EXTERNAL TABLE IF NOT EXISTS dqm_ref.rxnsat_{version} (
  rxcui string, lui string, sui string, rxaui string, stype string, code string, atui string,
  satui string, atn string, sab string, atv string, suppress string, cvf string, trailing_empty string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE LOCATION '{loc}/rxnsat/';

CREATE EXTERNAL TABLE IF NOT EXISTS dqm_ref.rxnrel_{version} (
  rxcui1 string, rxaui1 string, stype1 string, rel string, rxcui2 string, rxaui2 string,
  stype2 string, rela string, rui string, srui string, sab string, sl string, c13 string, c14 string,
  suppress string, cvf string, trailing_empty string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE LOCATION '{loc}/rxnrel/';

CREATE EXTERNAL TABLE IF NOT EXISTS dqm_ref.value_set_codes_{version} (oid string, system string, code string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION '{vsloc}/';

-- 1. The files loaded: rows per table (RxNorm-sourced rows separately)
SELECT 'rxnconso' AS t, count(*) AS rows_all, count_if(sab = 'RXNORM') AS rows_rxnorm FROM dqm_ref.rxnconso_{version}
UNION ALL SELECT 'rxnsat', count(*), count_if(sab = 'RXNORM') FROM dqm_ref.rxnsat_{version}
UNION ALL SELECT 'rxnrel', count(*), count_if(sab = 'RXNORM') FROM dqm_ref.rxnrel_{version};

-- 2. NDCs that RxNorm itself asserts (propagated to its normal forms, not suppressed), by term type
SELECT c.tty, count(DISTINCT s.atv) AS ndcs, count(DISTINCT s.rxcui) AS concepts
FROM dqm_ref.rxnsat_{version} s
JOIN dqm_ref.rxnconso_{version} c ON c.rxcui = s.rxcui AND c.sab = 'RXNORM'
WHERE s.atn = 'NDC' AND s.sab = 'RXNORM' AND s.suppress = 'N'
GROUP BY 1 ORDER BY 2 DESC;

-- 3. Term types of the codes in the measures' medication value sets (which level the mapping must reach)
SELECT v.oid, c.tty, count(DISTINCT v.code) AS codes
FROM dqm_ref.value_set_codes_{version} v
LEFT JOIN dqm_ref.rxnconso_{version} c ON c.rxcui = v.code AND c.sab = 'RXNORM' AND c.suppress <> 'O'
WHERE v.system = 'http://www.nlm.nih.gov/research/umls/rxnorm'
GROUP BY 1, 2 ORDER BY 1, 3 DESC;

-- 4. Brand -> generic through RxNorm's tradename relationship, on the two insulin brands seen in the
--    open demo (FINDINGS 2026-10-07): Humalog 865098 and Humulin R 311036. Both directions listed,
--    with the term types, so the direction convention is checked rather than assumed.
SELECT r.rxcui1, c1.tty AS tty1, r.rela, r.rxcui2, c2.tty AS tty2, c2.str AS str2
FROM dqm_ref.rxnrel_{version} r
JOIN dqm_ref.rxnconso_{version} c1 ON c1.rxcui = r.rxcui1 AND c1.sab = 'RXNORM' AND c1.tty IN ('SCD', 'SBD')
JOIN dqm_ref.rxnconso_{version} c2 ON c2.rxcui = r.rxcui2 AND c2.sab = 'RXNORM' AND c2.tty IN ('SCD', 'SBD')
WHERE r.sab = 'RXNORM' AND r.rela IN ('tradename_of', 'has_tradename')
  AND (r.rxcui1 IN ('865098', '311036') OR r.rxcui2 IN ('865098', '311036'))
ORDER BY 1, 4;
