-- --------------------------------------------------------------------------------------------------------------------
-- The "Maternal_Health_ETL".patient_history_staging table contains past pregnancy details, substance use, and chronic 
-- conditions of mothers.
-- 
-- Substance use: type of drug used, years of use, usage during pregnancy
-- Pregnancy history: number of past pregnancies, newborn weights, and gestational age
-- Chronic conditions: chronic diabetes and other diseases
-- 
-- These metrics help assess the patient’s medical background and prenatal risks.
-- --------------------------------------------------------------------------------------------------------------------

-- Check Current Database and Set Search Path
select current_database();
SELECT schema_name FROM information_schema.schemata;
SET search_path TO maternal_health_schema;

-- View and Stage Patient History Table
select * from patient_history order by patient_id asc;
CREATE TABLE patient_history_staging(LIKE patient_history INCLUDING ALL);
insert into patient_history_staging select * from patient_history ;
select * from patient_history_staging ;

-- --------------------------------------------------------------------------------------------------------------------
-- 1. Duplicate Check
-- --------------------------------------------------------------------------------------------------------------------
with duplicate_cte as (
    select *,
    row_number() over(partition by patient_id) as rn
    from patient_history_staging
)
select * from duplicate_cte where rn > 1;
-- Result: No duplicate values found

-- --------------------------------------------------------------------------------------------------------------------
-- 2. Data Standardization
-- --------------------------------------------------------------------------------------------------------------------
-- Replacing coded values with readable labels for clear interpretation

-- Standardizing 'drugs_preference'
update patient_history_staging set drugs_preference = 'No' where drugs_preference = '0';
update patient_history_staging set drugs_preference = 'Marijuana' where drugs_preference = '1';
update patient_history_staging set drugs_preference = 'Crack' where drugs_preference = '2';
update patient_history_staging set drugs_preference = 'Cocaine' where drugs_preference = '3';
update patient_history_staging set drugs_preference = 'Marijuana plus cocaine' where drugs_preference = '4';

-- Standardizing 'drugs_during_pregnancy'
update patient_history_staging set drugs_during_pregnancy = 'No' where drugs_during_pregnancy = '0';
update patient_history_staging set drugs_during_pregnancy = 'Yes' where drugs_during_pregnancy = '1';

-- Standardizing 'chronic_diabetes'
update patient_history_staging set chronic_diabetes = 'No' where chronic_diabetes = '0';
update patient_history_staging set chronic_diabetes = 'Yes' where chronic_diabetes = '1';

-- Standardizing 'chronic_diseases'
update patient_history_staging set chronic_diseases = 'No' where chronic_diseases = '0';
update patient_history_staging set chronic_diseases = 'Yes' where chronic_diseases = '1';

-- Standardizing gestational age categories
update patient_history_staging set gestational_age_past_newborn_1 = 'FullTerm' where gestational_age_past_newborn_1 = 'Full_Term';
update patient_history_staging set gestational_age_past_newborn_1 = 'PreTerm' where gestational_age_past_newborn_1 = '0';
update patient_history_staging set gestational_age_past_newborn_2 = 'FullTerm' where gestational_age_past_newborn_2 = '1';
update patient_history_staging set gestational_age_past_newborn_2 = 'PreTerm' where gestational_age_past_newborn_2 = '0';
update patient_history_staging set gestational_age_past_newborn_3 = 'FullTerm' where gestational_age_past_newborn_3 = '1';
update patient_history_staging set gestational_age_past_newborn_3 = 'PreTerm' where gestational_age_past_newborn_3 = '0';
update patient_history_staging set gestational_age_past_4_newborn = 'FullTerm' where gestational_age_past_4_newborn = '1';
update patient_history_staging set gestational_age_past_4_newborn = 'PreTerm' where gestational_age_past_4_newborn = '0';

-- --------------------------------------------------------------------------------------------------------------------
-- 3. Blank Value Check
-- --------------------------------------------------------------------------------------------------------------------
select * from patient_history_staging
where drugs_preference = '' or
      drugs_years_use = '' or
      drugs_during_pregnancy = '' or
      past_newborn_1_weight = '' or
      gestational_age_past_newborn_1 = '' or
      past_newborn_2_weight = '' or
      gestational_age_past_newborn_2 = '' or
      past_newborn_3_weight = '' or
      gestational_age_past_newborn_3 = '' or
      past_newborn_4_weight = '' or
      gestational_age_past_4_newborn = '' or
      chronic_diabetes = '' or
      chronic_diseases = ''
order by patient_id;
-- Result: No blank values found

-- --------------------------------------------------------------------------------------------------------------------
-- 4. Outlier Detection in 'chronic_diabetes'
-- --------------------------------------------------------------------------------------------------------------------
WITH word_counts AS (
    SELECT 
        unnest(string_to_array(lower(chronic_diabetes), ' ')) AS word,
        COUNT(*) AS word_count
    FROM patient_history_staging
    GROUP BY word
)
SELECT * FROM word_counts WHERE word_count = 1;

select * from patient_history_staging where chronic_diabetes like '8%';
-- Result: 2 rows found with outlier values in chronic_diabetes

-- --------------------------------------------------------------------------------------------------------------------
-- Final Check
-- --------------------------------------------------------------------------------------------------------------------
select count(*) from patient_history_staging where drugs_preference = 'No';
select * from patient_history_staging;
-- Data is now standardized, cleaned, and ready for insertion 
-- --------------------------------------------------------------------------------------------------------------------
