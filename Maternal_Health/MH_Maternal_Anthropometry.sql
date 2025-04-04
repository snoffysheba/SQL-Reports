----------------------------------------------------------------------------------------------------------------------------------------------
--The "Maternal_Health_ETL".maternal_anthropometry table contains maternal body measurements across pregnancy, including:

--Prepregnancy data: weight, BMI, BMI category

--Trimester weights: 1st, 2nd, 3rd trimester weights

--Inclusion & prepartum measurements: maternal height and weight at study inclusion and near labor

--Current BMI and fat distribution: subcutaneous, visceral, total fat (periumbilical and preperitoneal)

--Circumference metrics: neck, hip, waist, calf, and brachial circumference

--Skinfold thickness: triceps, subscapular, supra-iliac

--These metrics help assess maternal fat distribution, and physical changes during pregnancy.
----------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE "Maternal_Health_ETL".maternal_anthropometry (
    case_id int PRIMARY KEY,

    -- Prepregnancy info
    prepregnant_weight FLOAT,  				--1. updated "no_answer to null" 2. changed text to float 
    prepregnant_bmi FLOAT,	   					--1. updated "no_answer to null" 2. changed text to float 3. has Outliers
    bmi_according_who TEXT,						--1. updated "not_applicable" and 0 to null 
												--2. changed codes 1: Underweight 2: Healthy Weight 3: Overweight
												--3. validated across current_bmi_according_who

	-- First to third trimester weights
    current_maternal_weight_1st_tri FLOAT,		--1. updated "not_applicable to null" 2. changed text to float
    current_maternal_weight_2nd_tri FLOAT,		--1. updated "not_applicable to null" 2. changed text to float
    current_maternal_weight_3rd_tri FLOAT,		--1. updated "not_applicable to null" 2. changed text to float 3. has Outliers
    maternal_weight_at_inclusion FLOAT,		--no changes applied. 
    height_at_inclusion FLOAT,					--1. no changes applied. 2. validated across prepartum_maternal_height

    -- Prepartum measurements (near labor)
    prepartum_maternal_weight FLOAT,   		--1. updated "not_applicable to null" 2. changed text to float
    prepartum_maternal_height FLOAT,			--1. updated "not_applicable to null" 2. changed text to float
	
	-- Current BMI measures
    current_bmi FLOAT,							--no changes applied. 
    current_bmi_according_who TEXT, 			--1. changed int to float. 2. updated "not_applicable" and 0 to null 
												--2. changed codes 1: Underweight 2: Healthy Weight 3: Overweight

    -- Fat distribution 
    periumbilical_subcutanous_fat FLOAT, 		--1. updated "not_applicable to null" 2. changed text to float
    periumbilical_visceral_fat FLOAT,	  		--1. updated "not_applicable to null" 2. changed text to float
    periumbilical_total_fat FLOAT,				--1. Populated the null values (periumbilical_subcutanous_fat + periumbilical_visceral_fat)
    preperitoneal_subcutaneous_fat FLOAT,		--no changes applied. 
    preperitoneal_visceral_fat FLOAT,			--no changes applied. 
	preperitoneal_total_fat FLOAT,				--added new column (preperitoneal_subcutaneous_fat + preperitoneal_visceral_fat)
	
    -- Circumference-based measures
    maternal_brachial_circumference FLOAT,		--no changes applied. 
    circumference_maternal_calf FLOAT,			--no changes applied. 
    maternal_neck_circumference FLOAT,			--no changes applied. 
    maternal_hip_circumference FLOAT,			--no changes applied. 
    maternal_waist_circumference FLOAT,		--no changes applied. 

    -- Skinfold measurements
    mean_tricciptal_skinfold FLOAT,			--no changes applied. 
    mean_subscapular_skinfold FLOAT,			--no changes applied. 
    mean_supra_iliac_skin_fold FLOAT 			--no changes applied. 
);


INSERT INTO "Maternal_Health_ETL".maternal_anthropometry (
    case_id,

    -- Prepregnancy info
    prepregnant_weight,
    prepregnant_bmi,
    bmi_according_who,

    -- First to third trimester weights
    current_maternal_weight_1st_tri,
    current_maternal_weight_2nd_tri,
    current_maternal_weight_3rd_tri,
    maternal_weight_at_inclusion,
    height_at_inclusion,

    -- Prepartum measurements
    prepartum_maternal_weight,
    prepartum_maternal_height,

    -- Current BMI measures
    current_bmi,
    current_bmi_according_who,

    -- Fat distribution
    periumbilical_subcutanous_fat,
    periumbilical_visceral_fat,
    periumbilical_total_fat,
    preperitoneal_subcutaneous_fat,
    preperitoneal_visceral_fat,
	preperitoneal_total_fat,

    -- Circumference-based measures
    maternal_brachial_circumference,
    circumference_maternal_calf,
    maternal_neck_circumference,
    maternal_hip_circumference,
    maternal_waist_circumference,

    -- Skinfold measurements
    mean_tricciptal_skinfold,
    mean_subscapular_skinfold,
    mean_supra_iliac_skin_fold
)
SELECT 
    MH.case_id,

    -- Prepregnancy info
    MH.prepregnant_weight,
    MH.prepregnant_bmi,
    MH.bmi_according_who,

    -- First to third trimester weights
    MH.current_maternal_weight_1st_tri,
    MH.current_maternal_weight_2nd_tri,
    MH.current_maternal_weight_3rd_tri,
    MH.maternal_weight_at_inclusion,
    MH.hight_at_inclusion,

    -- Prepartum measurements
    MH.prepartum_maternal_weight,
    MH.prepartum_maternal_heigh,

    -- Current BMI measures
    MH.current_bmi,
    MH.current_bmi_according_who,

    -- Fat distribution
    MH.periumbilical_subcutanous_fat,
    MH.periumbilical_visceral_fat,
    MH.periumbilical_total_fat,
    MH.preperitoneal_subcutaneous_fat,
    MH.preperitoneal_visceral_fat,
	(coalesce(MH.preperitoneal_subcutaneous_fat, 0) + coalesce(MH.preperitoneal_visceral_fat, 0)),
        
    -- Circumference-based measures
    MH.maternal_brachial_circumference,
    MH.circumference_maternal_calf,
    MH.maternal_neck_circumference,
    MH.maternal_hip_circumference,
    MH.maternal_waist_circumference,

    -- Skinfold measurements
    MH.mean_tricciptal_skinfold,
    MH.mean_subscapular_skinfold,
    MH.mean_supra_iliac_skin_fold
FROM "Maternal_Health_ETL".data_import_to_MH_ETL MH;

select * from "Maternal_Health_ETL".maternal_anthropometry


----------------------------------------------------------------------------------------------------------------------------------------------
--Data Cleaning on the Staging table - "Maternal_Health_ETL".data_import_to_MH_ETL
----------------------------------------------------------------------------------------------------------------------------------------------
--1. "not_applicable" or "no_answer" changed to null
--2. Data Type changed from "text" to "int"/Codes 
--3. Changed codes to their respective values and Data Type changed (if any)
--4. Outliers Identified
----------------------------------------------------------------------------------------------------------------------------------------------

--prepregnant_weight
select distinct prepregnant_weight from "Maternal_Health_ETL".data_import_to_MH_ETL order by prepregnant_weight desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set prepregnant_weight=null
where prepregnant_weight='no_answer'


ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN prepregnant_weight TYPE FLOAT USING prepregnant_weight::FLOAT;


--prepregnant_bmi
select distinct prepregnant_bmi from "Maternal_Health_ETL".data_import_to_MH_ETL order by prepregnant_bmi desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set prepregnant_bmi=null
where prepregnant_bmi='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN prepregnant_bmi TYPE FLOAT USING prepregnant_bmi::FLOAT;

--bmi_according_who
select distinct bmi_according_who from "Maternal_Health_ETL".data_import_to_MH_ETL order by bmi_according_who desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set bmi_according_who=null
where bmi_according_who='not_applicable'

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set bmi_according_who=null
where bmi_according_who='0'

--https://obesitymedicine.org/blog/new-icd-10-codes-for-obesity-treatment-advancements-in-accurate-diagnosis-and-care/
--https://www.cdc.gov/bmi/adult-calculator/bmi-categories.html

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set bmi_according_who='Underweight'
where bmi_according_who='1'

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set bmi_according_who='Healthy weight'
where bmi_according_who='2'

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set bmi_according_who='Overweight'
where bmi_according_who='3'


--current_maternal_weight_1st_tri
select distinct current_maternal_weight_1st_tri from "Maternal_Health_ETL".data_import_to_MH_ETL order by current_maternal_weight_1st_tri desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set current_maternal_weight_1st_tri=null
where current_maternal_weight_1st_tri='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN current_maternal_weight_1st_tri TYPE FLOAT USING current_maternal_weight_1st_tri::FLOAT;


--current_maternal_weight_2nd_tri
select distinct current_maternal_weight_2nd_tri from "Maternal_Health_ETL".data_import_to_MH_ETL order by current_maternal_weight_2nd_tri desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set current_maternal_weight_2nd_tri=null
where current_maternal_weight_2nd_tri='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN current_maternal_weight_2nd_tri TYPE FLOAT USING current_maternal_weight_2nd_tri::FLOAT;


--current_maternal_weight_3rd_tri
select distinct current_maternal_weight_3rd_tri from "Maternal_Health_ETL".data_import_to_MH_ETL order by current_maternal_weight_3rd_tri desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set current_maternal_weight_3rd_tri=null
where current_maternal_weight_3rd_tri='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN current_maternal_weight_3rd_tri TYPE FLOAT USING current_maternal_weight_3rd_tri::FLOAT;


--maternal_weight_at_inclusion
select distinct maternal_weight_at_inclusion from "Maternal_Health_ETL".data_import_to_MH_ETL order by maternal_weight_at_inclusion desc;


--height_at_inclusion
select distinct hight_at_inclusion from "Maternal_Health_ETL".data_import_to_MH_ETL order by hight_at_inclusion desc;



--current_bmi
select distinct current_bmi from "Maternal_Health_ETL".data_import_to_MH_ETL order by current_bmi desc;


--current_bmi_according_who
select distinct current_bmi_according_who from "Maternal_Health_ETL".data_import_to_MH_ETL order by current_bmi_according_who desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set current_bmi_according_who=null
where current_bmi_according_who=0

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN current_bmi_according_who TYPE TEXT USING current_bmi_according_who::TEXT;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set current_bmi_according_who='Underweight'
where current_bmi_according_who='1'

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set current_bmi_according_who='Healthy weight'
where current_bmi_according_who='2'

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set current_bmi_according_who='Overweight'
where current_bmi_according_who='3'


--prepartum_maternal_weight
select distinct prepartum_maternal_weight from "Maternal_Health_ETL".data_import_to_MH_ETL order by prepartum_maternal_weight desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set prepartum_maternal_weight=null
where prepartum_maternal_weight='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN prepartum_maternal_weight TYPE FLOAT USING prepartum_maternal_weight::FLOAT;



--prepartum_maternal_height
select distinct prepartum_maternal_heigh from "Maternal_Health_ETL".data_import_to_MH_ETL order by prepartum_maternal_heigh desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set prepartum_maternal_heigh=null
where prepartum_maternal_heigh='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN prepartum_maternal_heigh TYPE FLOAT USING prepartum_maternal_heigh::FLOAT;


--periumbilical_subcutanous_fat
select distinct periumbilical_subcutanous_fat from "Maternal_Health_ETL".data_import_to_MH_ETL order by periumbilical_subcutanous_fat desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set periumbilical_subcutanous_fat=null
where periumbilical_subcutanous_fat='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN periumbilical_subcutanous_fat TYPE FLOAT USING periumbilical_subcutanous_fat::FLOAT;


--periumbilical_visceral_fat
select distinct periumbilical_visceral_fat from "Maternal_Health_ETL".data_import_to_MH_ETL order by periumbilical_visceral_fat desc;

update "Maternal_Health_ETL".data_import_to_MH_ETL 
set periumbilical_visceral_fat=null
where periumbilical_visceral_fat='not_applicable'

ALTER TABLE "Maternal_Health_ETL".data_import_to_MH_ETL
ALTER COLUMN periumbilical_visceral_fat TYPE FLOAT USING periumbilical_visceral_fat::FLOAT;


--periumbilical_total_fat
select distinct periumbilical_total_fat from "Maternal_Health_ETL".data_import_to_MH_ETL order by periumbilical_total_fat desc;


--preperitoneal_subcutaneous_fat
select distinct preperitoneal_subcutaneous_fat from "Maternal_Health_ETL".data_import_to_MH_ETL order by preperitoneal_subcutaneous_fat desc;


--preperitoneal_visceral_fat
select distinct preperitoneal_visceral_fat from "Maternal_Health_ETL".data_import_to_MH_ETL order by preperitoneal_visceral_fat desc;


----------------------------------------------------------------------------------------------------------------------------------------------
--5. height_at_inclusion Vs prepartum_maternal_height
--This validation helps determine if two columns hold mostly similar values across rows, 
--so one can be safely retained to simplify the dataset.
----------------------------------------------------------------------------------------------------------------------------------------------

with mismatches as (
    select 
        case_id,
        row_number() over (order by case_id) as mismatch_count
    from "Maternal_Health_ETL".maternal_anthropometry
    where height_at_inclusion is distinct from prepartum_maternal_height
)
select 
    a.case_id,
    a.height_at_inclusion,
    a.prepartum_maternal_height,
    m.mismatch_count
from "Maternal_Health_ETL".maternal_anthropometry a
left join mismatches m on a.case_id = m.case_id
order by a.case_id;

-- Upon analysis, the two columns have many differing values, so both should be retained as they provide distinct information.
----------------------------------------------------------------------------------------------------------------------------------------------




----------------------------------------------------------------------------------------------------------------------------------------------
--6. bmi_according_who Vs current_bmi_according_who
--This validation helps determine if two columns hold mostly similar values across rows, 
--so one can be safely retained to simplify the dataset.
----------------------------------------------------------------------------------------------------------------------------------------------

select case_id, bmi_according_who, current_bmi_according_who
from "Maternal_Health_ETL".maternal_anthropometry
where bmi_according_who is distinct from current_bmi_according_who;

-- Upon analysis, the two columns have many differing values, so both should be retained as they provide distinct information.
----------------------------------------------------------------------------------------------------------------------------------------------





----------------------------------------------------------------------------------------------------------------------------------------------
--7. periumbilical_subcutanous_fat (column a), periumbilical_visceral_fat (column b) and periumbilical_total_fat (column c)

--periumbilical_total_fat = periumbilical_subcutanous_fat + periumbilical_visceral_fat
--1.Verifying that column C doesn't already have values where both column A and column B are null
--2.Updating column C with sum of column A and column B, where column C is null
--3.Checking whether any row exists where the sum of column A and column B is not equal to column C, 
--indicating a potential inconsistency in calculated values.
--4.If 3 exists, updating column C as sum of column A and column B
----------------------------------------------------------------------------------------------------------------------------------------------

select 
    case_id,
    periumbilical_subcutanous_fat,
    periumbilical_visceral_fat,
    periumbilical_total_fat
from "Maternal_Health_ETL".maternal_anthropometry 
where periumbilical_subcutanous_fat is null
  and periumbilical_visceral_fat is null
  and periumbilical_total_fat is not null;
--0 rows

select case_id, 
       periumbilical_subcutanous_fat, 
       periumbilical_visceral_fat, 
       periumbilical_total_fat    
from "Maternal_Health_ETL".maternal_anthropometry
where periumbilical_total_fat is null
  and (periumbilical_subcutanous_fat is not null or periumbilical_visceral_fat is not null);
--12 rows

update "Maternal_Health_ETL".maternal_anthropometry
set periumbilical_total_fat = 
    coalesce(periumbilical_subcutanous_fat, 0) + coalesce(periumbilical_visceral_fat, 0)
where periumbilical_total_fat is null
  and (periumbilical_subcutanous_fat is not null or periumbilical_visceral_fat is not null);
--12 rows

select count(*) as mismatch_count
from "Maternal_Health_ETL".maternal_anthropometry
where coalesce(periumbilical_subcutanous_fat, 0) + coalesce(periumbilical_visceral_fat, 0)
      != coalesce(periumbilical_total_fat, 0);
--27 rows

update "Maternal_Health_ETL".maternal_anthropometry
set periumbilical_total_fat = 
    coalesce(periumbilical_subcutanous_fat, 0) + coalesce(periumbilical_visceral_fat, 0)
where (coalesce(periumbilical_subcutanous_fat, 0) + coalesce(periumbilical_visceral_fat, 0))
      != coalesce(periumbilical_total_fat, 0);
--27 rows

select * from "Maternal_Health_ETL".maternal_anthropometry;

/*
Few related commands are commented out and saved to be used for future use.

alter table "Maternal_Health_ETL".maternal_anthropometry
drop column periumbilical_total_fat;


alter table "Maternal_Health_ETL".maternal_anthropometry
alter column periumbilical_total_fat type numeric(10, 1)


update "Maternal_Health_ETL".maternal_anthropometry ma
set 
    periumbilical_subcutanous_fat = di.periumbilical_subcutanous_fat,
    periumbilical_visceral_fat = di.periumbilical_visceral_fat,
    periumbilical_total_fat = di.periumbilical_total_fat
from "Maternal_Health_ETL".data_import_to_MH_ETL di
where ma.case_id = di.case_id;
*/
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
--8. preperitoneal_total_fat = preperitoneal_subcutaneous_fat + preperitoneal_visceral_fat
-------------------------------------------------------------------------------------------------------------------------------------------------------

alter table "Maternal_Health_ETL".maternal_anthropometry
alter column preperitoneal_total_fat type numeric(10, 1)

select * from "Maternal_Health_ETL".maternal_anthropometry;

-------------------------------------------------------------------------------------------------------------------------------------------------------





-------------------------------------------------------------------------------------------------------------------------------------------------------
--9. Outliers
-------------------------------------------------------------------------------------------------------------------------------------------------------


-- 1. prepregnant_bmi (case id = 6 , value = 55.36 )

select * from "Maternal_Health_ETL".maternal_anthropometry where prepregnant_bmi = 55.36


--Upon analysis, 
--If BMI = 55.36 and weight = 133 kg, then:
--Height = √(133 / 55.36) ≈ 1.55 meters (≈ 5 ft 1 inch)
--High-BMI, healthy-outcome pregnancy (14 other high-BMI cases, BMI >= 40)
-------------------------------------------------------------------------------------------------------------------------------------------------------


--2. current_maternal_weight_3rd_tri (case id = 237 , value = 999)

select * from "Maternal_Health_ETL".maternal_anthropometry where current_maternal_weight_3rd_tri = 999

-- Upon analysis,
--All weight entries for the mother across trimesters show a gradual, realistic increase.
--Sudden jump to 999 kg in 3rd trimester is clearly a mistake.
--Replacing 999 with NULL in the current_maternal_weight_3rd_tri column for this row.

update "Maternal_Health_ETL".maternal_anthropometry set current_maternal_weight_3rd_tri = null where current_maternal_weight_3rd_tri = 999

select * from "Maternal_Health_ETL".maternal_anthropometry where case_id = 237

-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 10. Hip Circumference Not Greater Than Waist Circumference
-------------------------------------------------------------------------------------------------------------------------------------------------------

select case_id, maternal_hip_circumference, maternal_waist_circumference, maternal_neck_circumference
from "Maternal_Health_ETL".maternal_anthropometry
where not (
    maternal_hip_circumference > maternal_waist_circumference
);
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 11. Maternal Weight Increase Across Trimesters
-------------------------------------------------------------------------------------------------------------------------------------------------------


select case_id, current_maternal_weight_1st_tri, current_maternal_weight_2nd_tri, current_maternal_weight_3rd_tri
from "Maternal_Health_ETL".maternal_anthropometry
where current_maternal_weight_1st_tri is not null
  and current_maternal_weight_2nd_tri is not null
  and current_maternal_weight_3rd_tri is not null
  and not (
    current_maternal_weight_1st_tri < current_maternal_weight_2nd_tri
    and current_maternal_weight_2nd_tri < current_maternal_weight_3rd_tri
  );

select * 
from "Maternal_Health_ETL".maternal_anthropometry 
where case_id = 129;
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 12. Mismatch in BMI Category Labels
-------------------------------------------------------------------------------------------------------------------------------------------------------


select *
from "Maternal_Health_ETL".maternal_anthropometry
where current_bmi_according_who is not null
  and bmi_according_who is not null
  and current_bmi_according_who != bmi_according_who;

-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 13. Weight Loss >10kg From Prepregnancy to 1st Trimester
-------------------------------------------------------------------------------------------------------------------------------------------------------


select case_id, prepregnant_weight, current_maternal_weight_1st_tri,
       prepregnant_weight - current_maternal_weight_1st_tri as weight_drop
from "Maternal_Health_ETL".maternal_anthropometry
where prepregnant_weight is not null
  and current_maternal_weight_1st_tri is not null
  and (prepregnant_weight - current_maternal_weight_1st_tri) > 10;

select * 
from "Maternal_Health_ETL".maternal_anthropometry 
where case_id in (132, 137);
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 14. Prepregnancy Weight Not Less Than Prepartum Weight
-------------------------------------------------------------------------------------------------------------------------------------------------------


select case_id, 
       cast(prepregnant_weight as numeric) as prepregnant_weight,
       cast(prepartum_maternal_weight as numeric) as prepartum_maternal_weight
from "Maternal_Health_ETL".maternal_anthropometry
where prepregnant_weight is not null
  and prepartum_maternal_weight is not null
  and not (cast(prepregnant_weight as numeric) < cast(prepartum_maternal_weight as numeric));

select * 
from "Maternal_Health_ETL".maternal_anthropometry 
where case_id in (214, 194);
-------------------------------------------------------------------------------------------------------------------------------------------------------





-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 15. Extreme Outliers in Skinfold Measurement Patterns
-------------------------------------------------------------------------------------------------------------------------------------------------------


select case_id, 
       mean_tricciptal_skinfold, 
       mean_subscapular_skinfold, 
       mean_supra_iliac_skin_fold
from "Maternal_Health_ETL".maternal_anthropometry
where mean_tricciptal_skinfold is not null
  and mean_subscapular_skinfold is not null
  and mean_supra_iliac_skin_fold is not null
  and not (
    mean_tricciptal_skinfold < mean_subscapular_skinfold
    and mean_subscapular_skinfold < mean_supra_iliac_skin_fold
  )
  and (
    abs(mean_tricciptal_skinfold - mean_subscapular_skinfold) > 10
    or abs(mean_subscapular_skinfold - mean_supra_iliac_skin_fold) > 10
  );

select * 
from "Maternal_Health_ETL".maternal_anthropometry 
where case_id in (69, 196, 241, 242, 261);
-------------------------------------------------------------------------------------------------------------------------------------------------------




-------------------------------------------------------------------------------------------------------------------------------------------------------
-- 16. Extreme Difference in Maternal Height Measurements (>5 cm)
-------------------------------------------------------------------------------------------------------------------------------------------------------


select case_id, 
       height_at_inclusion, 
       prepartum_maternal_height,
       abs(height_at_inclusion - prepartum_maternal_height) as height_difference
from "Maternal_Health_ETL".maternal_anthropometry
where height_at_inclusion is not null
  and prepartum_maternal_height is not null
  and abs(height_at_inclusion - prepartum_maternal_height) > 5;
-------------------------------------------------------------------------------------------------------------------------------------------------------

