WITH encounters_data AS (
  SELECT 
    enctrnum,
    personnum
  FROM 
    cleansed.adminsystemsclaims.encounters
),
claims_data AS (
  SELECT
    clmnum,
    enctrnum,
    postingdate,
    totalcharges
  FROM
    cleansed.adminsystemsclaims.claims
),
combined_claims AS (
  SELECT
    encs.personnum,
    cls.clmnum,
    cls.enctrnum,
    cls.postingdate,
    cls.totalcharges
  FROM
    claims_data cls inner join encounters_data encs on cls.enctrnum = encs.enctrnum
),
person_data AS (
  SELECT
    addrcountyofresidence,
    demographicsbirthdate,
    demographicssex,
    disableddisabled,
    fulltimestudent,
    hearingimpaired,
    maritalstatus,
    personnum,
    race
  FROM
    cleansed.adminsystemsmember.person
)
SELECT
  coalesce(cast(pd.demographicsbirthdate as DATE), to_date('01-01-0001', 'dd-MM-yyyy')) as birthdate_dt,
  cast(coalesce(cc.clmnum, -1) as INT) as claim_no,
  cast(coalesce(nullif(pd.addrcountyofresidence, ''), 'MISSING') as STRING) as county_txt,
  cast(coalesce(nullif(pd.disableddisabled, ''), 'MISSING') as STRING) as disabled_txt,
  cast(coalesce(cc.enctrnum, -1) as BIGINT) as encounter_no,
  cast(coalesce(nullif(pd.demographicssex, ''), 'MISSING') as STRING) as gender_txt,
  cast(coalesce(nullif(pd.hearingimpaired, ''), 'MISSING') as STRING) as hearingimpaired_txt,
  cast(coalesce(nullif(pd.maritalstatus, ''), 'MISSING') as STRING) as maritalstatus_txt,
  cast(coalesce(pd.personnum, -1) as INT) as patient_id,
  cast(coalesce(pd.race, -1) as INT) as race_no,
  cast(coalesce(nullif(pd.fulltimestudent, ''), 'MISSING') as STRING) as student_txt,
  cast(coalesce(cc.totalcharges, 0) as FLOAT) as totalcharges_amt
FROM
  combined_claims cc inner join person_data pd on cc.personnum = pd.personnum