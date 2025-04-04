----------------------------------------------------------------------------------------------------------------------------------------------
-- The "Maternal_Health_ETL".gestational_outcomes table contains pregnancy outcomes and newborn health indicators, including:

-- Gestational metrics: ultrasound-based gestational age, inclusion age, and age at birth

-- Fetal health: fetal weight at ultrasound, weight percentile, and expected birth weight

-- Prenatal and delivery data: number of prenatal appointments, delivery mode, and cesarean section reason

-- Newborn measurements: weight, height, head circumference, and thoracic perimeter at birth

-- Birth condition indicators: Apgar scores at 1 and 5 minutes, meconium presence, intubation, airway aspiration,
-- and use of pediatric resuscitation maneuvers

-- Complications and hospital stay: maternal hospital duration, hypertension, preeclampsia, gestational diabetes, and treatments

-- These fields support analysis of birth outcomes and cross-validation of newborn and maternal conditions.
----------------------------------------------------------------------------------------------------------------------------------------------





create table "Maternal_Health_ETL".gestational_outcomes (
    case_id int primary key,  -- unique identifier for each patient

    -- Gestational milestones and scans
    ultrasound_gestational_age FLOAT,            --no changes applied.
    gestational_age_at_inclusion FLOAT,          --no changes applied.
    gestational_age_at_birth FLOAT,              --no changes applied.

    -- Fetal assessment
    fetal_weight_at_ultrasound FLOAT,            --1. changed text to float 2. changed "not_applicable" to null
    weight_fetal_percentile FLOAT,               --1. changed text to float 2. changed "not_applicable" to null
												  --3. has Outliers (25)

    -- Prenatal care
    number_prenatal_appointments INT,           --no changes applied.

    -- Delivery information
    delivery_mode TEXT,                          --1. changed int to text; recoded (1: Vaginal, 2: Cesarean, etc.)
    cesarean_section_reason TEXT,                --1. changed "not_applicable" to null 2. changed codes 8 and 12 to its resp. reasons

    -- Newborn expected outcomes
    expected_weight_for_the_newborn FLOAT,       --1. changed 'not_applicable' to null. 2. changed text to float
												  --3. has Outliers (875)

    -- Newborn measurements
    newborn_weight FLOAT,                        --1. changed text to float
    newborn_height FLOAT,                        --1. changed text to float
    newborn_head_circumference FLOAT,            --1. changed 'not_applicable' to null. 2. changed text to float
    thoracic_perimeter_newborn FLOAT,            --1. changed 'not_applicable' to null. 2. changed text to float

    -- Birth and postnatal indicators
    meconium_labor INT,                          --no changes applied.
    apgar_1st_min INT,							  --no changes applied.
    apgar_5th_min INT,                           --no changes applied.
    pediatric_resuscitation_maneuvers INT,       --no changes applied.
    newborn_intubation INT,                      --no changes applied.
    newborn_airway_aspiration INT,               --no changes applied.

    -- Hospitalization details
    mothers_hospital_stay INT,                    --no changes applied.
	
	-- Pregnancy Complications
	hospital_hypertension INT,
    preeclampsia_record_pregnancy int,
    gestational_diabetes_mellitus int,
	disease_diagnose_during_pregnancy TEXT,
    treatment_disease_pregnancy TEXT
	
);



INSERT INTO "Maternal_Health_ETL".gestational_outcomes (
    case_id,

    -- Gestational milestones and scans
    ultrasound_gestational_age,
    gestational_age_at_inclusion,
    gestational_age_at_birth,

    -- Fetal assessment
    fetal_weight_at_ultrasound,
    weight_fetal_percentile,

    -- Prenatal care
    number_prenatal_appointments,

    -- Delivery information
    delivery_mode,
    cesarean_section_reason,

    -- Newborn expected outcomes
    expected_weight_for_the_newborn,

    -- Newborn measurements
    newborn_weight,
    newborn_height,
    newborn_head_circumference,
    thoracic_perimeter_newborn,

    -- Birth and postnatal indicators
    meconium_labor,
    apgar_1st_min,
    apgar_5th_min,
    pediatric_resuscitation_maneuvers,
    newborn_intubation,
    newborn_airway_aspiration,

    -- Hospitalization details
    mothers_hospital_stay,

	-- Pregnancy Complications
	hospital_hypertension,
    preeclampsia_record_pregnancy,
    gestational_diabetes_mellitus,
	disease_diagnose_during_pregnancy,
    treatment_disease_pregnancy

	
)
SELECT
    MH.case_id,

    -- Gestational milestones and scans
    MH.ultrasound_gestational_age,
    MH.gestational_age_at_inclusion,
    MH.gestational_age_at_birth,

    -- Fetal assessment
    MH.fetal_weight_at_ultrasound,
    MH.weight_fetal_percentile,

    -- Prenatal care
    MH.number_prenatal_appointments,

    -- Delivery information
    MH.delivery_mode,
    MH.cesarean_section_reason,

    -- Newborn expected outcomes
    MH.expected_weight_for_the_newborn,

    -- Newborn measurements
    MH.newborn_weight,
    MH.newborn_height,
    MH.newborn_head_circumference,
    MH.thoracic_perimeter_newborn,

    -- Birth and postnatal indicators
    MH.meconium_labor,
    MH.apgar_1st_min,
    MH.apgar_5th_min,
    MH.pediatric_resuscitation_maneuvers,
    MH.newborn_intubation,
    MH.newborn_airway_aspiration,

    -- Hospitalization details
    MH.mothers_hospital_stay,

	-- Pregnancy Complications
	MH.hospital_hypertension,
    MH.preeclampsia_record_pregnancy,
    MH.gestational_diabetes_mellitus,
	MH.disease_diagnose_during_pregnancy,
    MH.treatment_disease_pregnancy

FROM "Maternal_Health_ETL".data_import_to_MH_ETL MH;


select * from "Maternal_Health_ETL".data_import_to_MH_ETL

-- ultrasound_gestational_age
-- ---------------------------
select distinct ultrasound_gestational_age from "Maternal_Health_ETL".data_import_to_MH_ETL order by ultrasound_gestational_age desc;


-- gestational_age_at_inclusion
-- ----------------------------
select distinct gestational_age_at_inclusion from "Maternal_Health_ETL".data_import_to_MH_ETL order by gestational_age_at_inclusion desc;


-- gestational_age_at_birth
-- ------------------------
select distinct gestational_age_at_birth from "Maternal_Health_ETL".data_import_to_MH_ETL order by gestational_age_at_birth desc;


-- fetal_weight_at_ultrasound
-- --------------------------
select distinct fetal_weight_at_ultrasound from "Maternal_Health_ETL".data_import_to_MH_ETL order by fetal_weight_at_ultrasound desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL set fetal_weight_at_ultrasound = null where fetal_weight_at_ultrasound = 'not_applicable';

alter table "Maternal_Health_ETL".data_import_to_MH_ETL alter column fetal_weight_at_ultrasound 
type float using replace(fetal_weight_at_ultrasound, ',', '')::float;


-- weight_fetal_percentile
-- -----------------------
select distinct weight_fetal_percentile from "Maternal_Health_ETL".data_import_to_MH_ETL order by weight_fetal_percentile desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL set weight_fetal_percentile = null where weight_fetal_percentile = 'not_applicable';

alter table "Maternal_Health_ETL".data_import_to_MH_ETL alter column weight_fetal_percentile 
type float using replace(weight_fetal_percentile, ',', '')::float;


-- number_prenatal_appointments
-- ----------------------------
select distinct number_prenatal_appointments from "Maternal_Health_ETL".data_import_to_MH_ETL order by number_prenatal_appointments desc;


-- delivery_mode
-- -------------
select distinct delivery_mode from "Maternal_Health_ETL".data_import_to_MH_ETL order by delivery_mode desc;

alter table "Maternal_Health_ETL".data_import_to_MH_ETL alter column delivery_mode type text using delivery_mode::text;

update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Vaginal' where delivery_mode = '1';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Cesarean' where delivery_mode = '2';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Instrumental Vaginal' where delivery_mode = '3';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Water Birth' where delivery_mode = '4';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'VBAC' where delivery_mode = '5';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Emergency Cesarean' where delivery_mode = '6';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Elective Cesarean' where delivery_mode = '7';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Breech Vaginal' where delivery_mode = '8';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Forceps' where delivery_mode = '9';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Vacuum Extraction' where delivery_mode = '10';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = 'Unknown' where delivery_mode = '11';
update "Maternal_Health_ETL".data_import_to_MH_ETL set delivery_mode = null where delivery_mode = '12';


-- cesarean_section_reason
-- ------------------------
select distinct cesarean_section_reason from "Maternal_Health_ETL".data_import_to_MH_ETL order by cesarean_section_reason desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL set cesarean_section_reason = null where cesarean_section_reason = 'not_applicable';

update "Maternal_Health_ETL".data_import_to_MH_ETL set cesarean_section_reason = null where cesarean_section_reason = 'no_answer';

update "Maternal_Health_ETL".data_import_to_MH_ETL set cesarean_section_reason = null where cesarean_section_reason = ' ';

update "Maternal_Health_ETL".data_import_to_MH_ETL set cesarean_section_reason = 'Maternal Request' where cesarean_section_reason = '8';

update "Maternal_Health_ETL".data_import_to_MH_ETL set cesarean_section_reason = null where cesarean_section_reason = '12';



--expected_weight_for_the_newborn
---------------------------------

select distinct expected_weight_for_the_newborn from "Maternal_Health_ETL".data_import_to_MH_ETL order by expected_weight_for_the_newborn desc;

alter table "Maternal_Health_ETL".data_import_to_MH_ETL alter column expected_weight_for_the_newborn
type float using replace(expected_weight_for_the_newborn, ',', '')::float;



--newborn_weight
----------------
select distinct newborn_weight from "Maternal_Health_ETL".data_import_to_MH_ETL order by newborn_weight desc;

alter table "Maternal_Health_ETL".data_import_to_MH_ETL alter column newborn_weight
type float using replace(newborn_weight, ',', '')::float;



-- newborn_height
-- --------------
select distinct newborn_height from "Maternal_Health_ETL".data_import_to_MH_ETL order by newborn_height desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL set newborn_height = null where newborn_height = 'not_applicable';

alter table "Maternal_Health_ETL".data_import_to_MH_ETL alter column newborn_height type float using replace(newborn_height, ',', '')::float;



--newborn_head_circumference
----------------------------

select distinct newborn_head_circumference from "Maternal_Health_ETL".data_import_to_MH_ETL order by newborn_head_circumference desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL set newborn_head_circumference = null where newborn_head_circumference = 'not_applicable';

alter table "Maternal_Health_ETL".data_import_to_MH_ETL alter column newborn_head_circumference type float using replace(newborn_head_circumference, ',', '')::float;



--thoracic_perimeter_newborn
----------------------------

select distinct thoracic_perimeter_newborn from "Maternal_Health_ETL".data_import_to_MH_ETL order by thoracic_perimeter_newborn desc;



-- meconium_labor
-- --------------
select distinct meconium_labor from "Maternal_Health_ETL".data_import_to_MH_ETL order by meconium_labor desc;



-- apgar_1st_min
-- -------------
select distinct apgar_1st_min from "Maternal_Health_ETL".data_import_to_MH_ETL order by apgar_1st_min desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL
set apgar_1st_min = 9
where apgar_1st_min = 99

-- apgar_5th_min
-- -------------
select distinct apgar_5th_min from "Maternal_Health_ETL".data_import_to_MH_ETL order by apgar_5th_min desc;



-- pediatric_resuscitation_maneuvers
-- ----------------------------------
select distinct pediatric_resuscitation_maneuvers from "Maternal_Health_ETL".data_import_to_MH_ETL order by pediatric_resuscitation_maneuvers desc;


-- newborn_intubation
-- ------------------
select distinct newborn_intubation from "Maternal_Health_ETL".data_import_to_MH_ETL order by newborn_intubation desc;



-- newborn_airway_aspiration
-- -------------------------
select distinct newborn_airway_aspiration from "Maternal_Health_ETL".data_import_to_MH_ETL order by newborn_airway_aspiration desc;



-- mothers_hospital_stay
-- ---------------------
select distinct mothers_hospital_stay from "Maternal_Health_ETL".data_import_to_MH_ETL order by mothers_hospital_stay desc;



--hospital_hypertension
-----------------------
select distinct hospital_hypertension from "Maternal_Health_ETL".data_import_to_MH_ETL;



--preeclampsia_record_pregnancy
--------------------------------
select distinct preeclampsia_record_pregnancy from "Maternal_Health_ETL".data_import_to_MH_ETL;


--gestational_diabetes_mellitus
-------------------------------
select distinct gestational_diabetes_mellitus from "Maternal_Health_ETL".data_import_to_MH_ETL;

select *
from "Maternal_Health_ETL".gestational_outcomes
where gestational_diabetes_mellitus = 1
  and treatment_disease_pregnancy not in (
    'insulina', 'metformina', 'diet', 'medication', 'Sem tto', 'Sem TTo', 'aspirina'
  );





--disease_diagnose_during_pregnancy
-----------------------------------
select distinct disease_diagnose_during_pregnancy from "Maternal_Health_ETL".data_import_to_MH_ETL;

update "Maternal_Health_ETL".data_import_to_MH_ETL
set disease_diagnose_during_pregnancy = null
where disease_diagnose_during_pregnancy = '0';


--treatment_disease_pregnancy
-----------------------------
select distinct treatment_disease_pregnancy from "Maternal_Health_ETL".data_import_to_MH_ETL;


select *
from "Maternal_Health_ETL".data_import_to_MH_ETL
where treatment_disease_pregnancy in ('45', '0');


update "Maternal_Health_ETL".data_import_to_MH_ETL
set treatment_disease_pregnancy = null
where treatment_disease_pregnancy in ('45', '0');

--------------------------------------------------------------------------------------------------------------------------------------------------------
--1. Newborn Intubation or Aspiration but High Apgar Score
--------------------------------------------------------------------------------------------------------------------------------------------------------
--Problem: apgar_1st_min >= 8, but newborn_intubation = 1 or airway_aspiration = 1
--High Apgar score at 1 minute (≥8) typically indicates good newborn condition, making intubation or airway aspiration medically unlikely.
--------------------------------------------------------------------------------------------------------------------------------------------------------


select *
from "Maternal_Health_ETL".gestational_outcomes
where apgar_1st_min >= 8
  and (newborn_intubation = 1 and newborn_airway_aspiration = 1);
--2 rows

update "Maternal_Health_ETL".gestational_outcomes
set newborn_intubation = 0,
    newborn_airway_aspiration = 0
where apgar_1st_min >= 8 
  and (newborn_intubation = 1 and newborn_airway_aspiration = 1);



select case_id, newborn_intubation, newborn_airway_aspiration, apgar_5th_min
from "Maternal_Health_ETL".gestational_outcomes
where newborn_intubation = 1
  and newborn_airway_aspiration = 1
  and apgar_5th_min >= 8;


update "Maternal_Health_ETL".gestational_outcomes
set newborn_intubation = 0,
    newborn_airway_aspiration = 0
where apgar_5th_min >= 8 
  and (newborn_intubation = 1 and newborn_airway_aspiration = 1);

--------------------------------------------------------------------------------------------------------------------------------------------------------




--------------------------------------------------------------------------------------------------------------------------------------------------------
--2. Preeclampsia Reported but No Hospital Hypertension
--------------------------------------------------------------------------------------------------------------------------------------------------------
--Problem: preeclampsia_record_pregnancy = 1, but hospital_hypertension = 0 or NULL
--Preeclampsia almost always involves elevated BP, It’s likely hypertension was present but not recorded
--------------------------------------------------------------------------------------------------------------------------------------------------------

select *
from "Maternal_Health_ETL".gestational_outcomes
where preeclampsia_record_pregnancy = 1
  and (hospital_hypertension is null or hospital_hypertension = 0);
--7 rows

update "Maternal_Health_ETL".gestational_outcomes
set hospital_hypertension=1
where preeclampsia_record_pregnancy = 1
  and (hospital_hypertension is null or hospital_hypertension = 0);
  
-------------------------------------------------------------------------------------------------------------------------------------------------------




--------------------------------------------------------------------------------------------------------------------------------------------------------
--3. fetal_weight_at_ultrasound Vs newborn_weight
--------------------------------------------------------------------------------------------------------------------------------------------------------
--Problem: fetal_weight_at_ultrasound > newborn_weight
--Fetal weight at ultrasound = 2732 g → impossible, as it's more than double the final newborn weight.
--------------------------------------------------------------------------------------------------------------------------------------------------------


select * from "Maternal_Health_ETL".gestational_outcomes where fetal_weight_at_ultrasound > newborn_weight;

update "Maternal_Health_ETL".gestational_outcomes
set fetal_weight_at_ultrasound = null
where fetal_weight_at_ultrasound =2732;
--------------------------------------------------------------------------------------------------------------------------------------------------------




/*
--------------------------------------------------------------------------------------------------------------------------------------------------------
--4. hospital hypertension Vs hospital_systolic_blood_pressure and hospital_diastolic_blood_pressure
--------------------------------------------------------------------------------------------------------------------------------------------------------
--Problem : High BP values but no hospital hypertension
--Hospital hypertension is marked as 0 despite high systolic (≥140) and diastolic (≥90) pressures, indicating a likely data entry error.
--------------------------------------------------------------------------------------------------------------------------------------------------------

select * from "Maternal_Health_ETL".data_import_to_MH_ETL
where hospital_hypertension = 0
  and hospital_systolic_blood_pressure >= 140
  and hospital_diastolic_blood_pressure >= 90;

update "Maternal_Health_ETL".gestational_outcomes
set hospital_hypertension = 1
where hospital_hypertension = 0
  and hospital_systolic_blood_pressure >= 140
  and hospital_diastolic_blood_pressure >= 90;
-------------------------------------------------------------------------------------------------------------------------------------------------------
*/




-------------------------------------------------------------------------------------------------------------------------------------------------------
--5.Problem: pediatric_resuscitation_maneuvers is marked as 1, indicating emergency newborn care, but there are no signs of distress like low Apgar, 
--intubation, aspiration, or meconium. (2 cases). 
-------------------------------------------------------------------------------------------------------------------------------------------------------


select case_id, pediatric_resuscitation_maneuvers, newborn_intubation, newborn_airway_aspiration, apgar_1st_min, meconium_labor
from "Maternal_Health_ETL".gestational_outcomes
where pediatric_resuscitation_maneuvers = 1
  and (
    (newborn_intubation is null or newborn_intubation = 0) and
    (newborn_airway_aspiration is null or newborn_airway_aspiration = 0) and
    (apgar_1st_min is null or apgar_1st_min >= 6) and
    (meconium_labor is null or meconium_labor = 0)
  );


update "Maternal_Health_ETL".gestational_outcomes
set pediatric_resuscitation_maneuvers = 0
where pediatric_resuscitation_maneuvers = 1
  and (newborn_intubation is null or newborn_intubation = 0)
  and (newborn_airway_aspiration is null or newborn_airway_aspiration = 0)
  and (apgar_1st_min is null or apgar_1st_min >= 6)
  and (meconium_labor is null or meconium_labor = 0);

--------------------------------------------------------------------------------------------------------------------------------------------------------





--------------------------------------------------------------------------------------------------------------------------------------------------------
-- 6. Meconium Absent but Intubation and Aspiration Performed
--------------------------------------------------------------------------------------------------------------------------------------------------------
select case_id, meconium_labor, apgar_1st_min, pediatric_resuscitation_maneuvers, newborn_intubation, newborn_airway_aspiration
from "Maternal_Health_ETL".gestational_outcomes
where meconium_labor = 0
  and newborn_intubation = 1
  and newborn_airway_aspiration = 1;

update "Maternal_Health_ETL".gestational_outcomes 
set meconium_labor = 1 
where meconium_labor = 0
  and newborn_intubation = 1
  and newborn_airway_aspiration = 1;
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 7. Low Apgar (≤6) but No Emergency Intervention Documented
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, apgar_1st_min, newborn_intubation, newborn_airway_aspiration, meconium_labor
from "Maternal_Health_ETL".gestational_outcomes
where apgar_1st_min <= 6
  and newborn_intubation = 0
  and newborn_airway_aspiration = 0
  and (meconium_labor is null or meconium_labor = 0);

-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 8. Aspiration Without Intubation in Low Apgar Cases 
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, apgar_1st_min, newborn_intubation, newborn_airway_aspiration, meconium_labor
from "Maternal_Health_ETL".gestational_outcomes
where apgar_1st_min <= 6
  and newborn_airway_aspiration = 1
  and newborn_intubation = 0;

update "Maternal_Health_ETL".gestational_outcomes
set newborn_intubation = 1
where apgar_1st_min <= 6
  and newborn_airway_aspiration = 1
  and newborn_intubation = 0;
-------------------------------------------------------------------------------------------------------------------------------------------------------



-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 9. Pediatric Maneuvers Performed but No Intubation for Low Apgar
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, apgar_1st_min, newborn_intubation, newborn_airway_aspiration, meconium_labor, pediatric_resuscitation_maneuvers
from "Maternal_Health_ETL".gestational_outcomes
where apgar_1st_min <= 6
  and newborn_intubation = 0;
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 10. Cesarean Reason Present but Delivery Mode is Not Cesarean
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, delivery_mode, cesarean_section_reason
from "Maternal_Health_ETL".gestational_outcomes
where cesarean_section_reason is not null
  and cesarean_section_reason != ''
  and lower(cast(delivery_mode as text)) not like '%cesar%';


update "Maternal_Health_ETL".gestational_outcomes
set cesarean_section_reason = null
where cesarean_section_reason is not null
  and cesarean_section_reason != ''
  and lower(cast(delivery_mode as text)) not like '%cesar%';
-------------------------------------------------------------------------------------------------------------------------------------------------------



-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 11. Ultrasound Gestational Age Greater Than Age at Birth
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, ultrasound_gestational_age, gestational_age_at_birth
from "Maternal_Health_ETL".gestational_outcomes
where ultrasound_gestational_age > gestational_age_at_birth;
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
--12. Cesarean Performed But No Hospital Stay Logged
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, mothers_hospital_stay, delivery_mode
from "Maternal_Health_ETL".gestational_outcomes
where mothers_hospital_stay = 0
  and lower(cast(delivery_mode as text)) like '%cesar%';

-------------------------------------------------------------------------------------------------------------------------------------------------------





-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 13. Inclusion Date Greater Than Birth Date
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, gestational_age_at_inclusion, gestational_age_at_birth
from "Maternal_Health_ETL".gestational_outcomes
where gestational_age_at_inclusion > gestational_age_at_birth;

update "Maternal_Health_ETL".gestational_outcomes
set gestational_age_at_inclusion = null
where case_id = 47;

-------------------------------------------------------------------------------------------------------------------------------------------------------
