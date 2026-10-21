/**
 * The pipeline: six steps, each a view over the one before.
 *
 * `sql(url, previous)` returns the statement that is shown on the page and run
 * in DuckDB-Wasm, so what a reader sees is what ran. Step 1 reads the raw
 * Parquet (every column text, empty cells still empty strings); every later
 * step selects from the view the step before it made.
 */

const BLANK = (c) => `NULLIF(trim(${c}), '')`;

export const STEPS = [
  {
    id: 1,
    title: 'Load raw',
    summary: 'Every column is text, exactly as the city exported it.',
    sql: (url) => `CREATE OR REPLACE VIEW step_1 AS
SELECT *
FROM read_parquet('${url}');`,
  },
  {
    id: 2,
    title: 'Cast types',
    summary: 'Blank cells become NULL; ids and coordinates become numbers; ZIP codes stay text but must be five digits.',
    sql: () => `CREATE OR REPLACE VIEW step_2 AS
SELECT
  TRY_CAST(${BLANK('unique_key')} AS BIGINT)  AS unique_key,
  ${BLANK('created_date')}                    AS created_date,
  ${BLANK('closed_date')}                     AS closed_date,
  ${BLANK('agency')}                          AS agency,
  ${BLANK('agency_name')}                     AS agency_name,
  ${BLANK('complaint_type')}                  AS complaint_type,
  ${BLANK('descriptor')}                      AS descriptor,
  ${BLANK('location_type')}                   AS location_type,
  CASE WHEN regexp_matches(trim(incident_zip), '^[0-9]{5}')
       THEN left(trim(incident_zip), 5) END   AS incident_zip,
  ${BLANK('incident_address')}                AS incident_address,
  ${BLANK('city')}                            AS city,
  ${BLANK('status')}                          AS status,
  ${BLANK('community_board')}                 AS community_board,
  ${BLANK('borough')}                         AS borough,
  ${BLANK('open_data_channel_type')}          AS open_data_channel_type,
  TRY_CAST(${BLANK('latitude')} AS DOUBLE)    AS latitude,
  TRY_CAST(${BLANK('longitude')} AS DOUBLE)   AS longitude
FROM step_1;`,
  },
  {
    id: 3,
    title: 'Parse timestamps',
    summary: 'The two date columns become real timestamps, and each request gets its calendar day.',
    sql: () => `CREATE OR REPLACE VIEW step_3 AS
SELECT
  * REPLACE (
    TRY_CAST(created_date AS TIMESTAMP) AS created_date,
    TRY_CAST(closed_date  AS TIMESTAMP) AS closed_date
  ),
  CAST(TRY_CAST(created_date AS TIMESTAMP) AS DATE) AS created_day
FROM step_2;`,
  },
  {
    id: 4,
    title: 'Normalise borough and agency',
    summary: 'Consistent case; the placeholder values "Unspecified" and "UNKNOWN" become NULL.',
    sql: () => `CREATE OR REPLACE MACRO title_case(s) AS
  array_to_string(list_transform(string_split(lower(s), ' '), w -> upper(w[1:1]) || w[2:]), ' ');

CREATE OR REPLACE VIEW step_4 AS
SELECT
  * REPLACE (
    upper(trim(agency)) AS agency,
    CASE WHEN upper(borough) = 'UNSPECIFIED' THEN NULL
         ELSE title_case(borough) END AS borough,
    title_case(city) AS city,
    CASE WHEN upper(status) = 'UNSPECIFIED' THEN NULL ELSE status END AS status,
    CASE WHEN upper(open_data_channel_type) = 'UNKNOWN' THEN NULL
         ELSE title_case(open_data_channel_type) END AS open_data_channel_type
  )
FROM step_3;`,
  },
  {
    id: 5,
    title: 'Drop duplicates and impossible rows',
    summary: 'One row per request id, and no request that was closed before it was created.',
    sql: () => `CREATE OR REPLACE VIEW step_5 AS
SELECT *
FROM step_4
WHERE unique_key IS NOT NULL
  AND (closed_date IS NULL OR closed_date >= created_date)
QUALIFY row_number() OVER (PARTITION BY unique_key ORDER BY created_date) = 1;`,
  },
  {
    id: 6,
    title: 'Derive resolution hours',
    summary: 'Hours from created to closed, to two decimals; NULL while a request is still open.',
    sql: () => `CREATE OR REPLACE VIEW step_6 AS
SELECT
  *,
  round(date_diff('second', created_date, closed_date) / 3600.0, 2) AS resolution_hours
FROM step_5;`,
  },
];

export const FINAL_STEP = STEPS.length;
export const viewName = (id) => `step_${id}`;
