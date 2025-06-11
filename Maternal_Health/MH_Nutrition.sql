-- --------------------------------------------------------------------------------------------------------------------
-- The "MaternalHealth".Nutrition_staging table captures daily dietary intake of pregnant women.
-- 
-- This includes meal timing (breakfast, snacks, lunch, dinner), food categories (fruits, vegetables, pasta), and patterns
-- (whether consumed or not). Binary indicators (0/1) are converted to clear labels ('No'/'Yes') for analysis.
-- 
-- These variables help assess maternal nutritional behavior during pregnancy.
-- --------------------------------------------------------------------------------------------------------------------

-- Set schema context
SELECT schema_name FROM information_schema.schemata;
SET search_path TO MaternalHealth;

-- --------------------------------------------------------------------------------------------------------------------
-- 1. Staging Table Creation
-- --------------------------------------------------------------------------------------------------------------------
CREATE TABLE Nutrition_staging (LIKE Maternalhealth.Nutrition INCLUDING ALL);
INSERT INTO Nutrition_staging SELECT * FROM Nutrition;
SELECT * FROM Nutrition_staging ORDER BY patient_id ASC;

-- --------------------------------------------------------------------------------------------------------------------
-- 2. Data Type Standardization
-- --------------------------------------------------------------------------------------------------------------------
-- Converting coded binary columns (0/1) to TEXT for value transformation
ALTER TABLE Nutrition_staging ALTER COLUMN breakfast_meal TYPE INT;
ALTER TABLE Nutrition_staging ALTER COLUMN morning_snack TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN lunch_meal TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN afternoon_snack TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN meal_dinner TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN supper_meal TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN bean TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN fruits TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN vegetables TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN embedded_food TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN pasta TYPE TEXT;
ALTER TABLE Nutrition_staging ALTER COLUMN cookies TYPE TEXT;

-- --------------------------------------------------------------------------------------------------------------------
-- 3. Label Transformation
-- --------------------------------------------------------------------------------------------------------------------
-- Transforming binary codes into human-readable values

-- Meal consumption indicators
UPDATE Nutrition_staging SET breakfast_meal = 'No' WHERE breakfast_meal = '0';
UPDATE Nutrition_staging SET breakfast_meal = 'Yes' WHERE breakfast_meal = '1';
UPDATE Nutrition_staging SET morning_snack = 'No' WHERE morning_snack = '0';
UPDATE Nutrition_staging SET morning_snack = 'Yes' WHERE morning_snack = '1';
UPDATE Nutrition_staging SET lunch_meal = 'No' WHERE lunch_meal = '0';
UPDATE Nutrition_staging SET lunch_meal = 'Yes' WHERE lunch_meal = '1';
UPDATE Nutrition_staging SET afternoon_snack = 'No' WHERE afternoon_snack = '0';
UPDATE Nutrition_staging SET afternoon_snack = 'Yes' WHERE afternoon_snack = '1';
UPDATE Nutrition_staging SET meal_dinner = 'No' WHERE meal_dinner = '0';
UPDATE Nutrition_staging SET meal_dinner = 'Yes' WHERE meal_dinner = '1';
UPDATE Nutrition_staging SET supper_meal = 'No' WHERE supper_meal = '0';
UPDATE Nutrition_staging SET supper_meal = 'Yes' WHERE supper_meal = '1';

-- Food category indicators
UPDATE Nutrition_staging SET bean = 'No' WHERE bean = '0';
UPDATE Nutrition_staging SET bean = 'Yes' WHERE bean = '1';
UPDATE Nutrition_staging SET fruits = 'No' WHERE fruits = '0';
UPDATE Nutrition_staging SET fruits = 'Yes' WHERE fruits = '1';
UPDATE Nutrition_staging SET vegetables = 'No' WHERE vegetables = '0';
UPDATE Nutrition_staging SET vegetables = 'Yes' WHERE vegetables = '1';
UPDATE Nutrition_staging SET embedded_food = 'No' WHERE embedded_food = '0';
UPDATE Nutrition_staging SET embedded_food = 'Yes' WHERE embedded_food = '1';
UPDATE Nutrition_staging SET pasta = 'No' WHERE pasta = '0';
UPDATE Nutrition_staging SET pasta = 'Yes' WHERE pasta = '1';
UPDATE Nutrition_staging SET cookies = 'No' WHERE cookies = '0';
UPDATE Nutrition_staging SET cookies = 'Yes' WHERE cookies = '1';

-- --------------------------------------------------------------------------------------------------------------------
-- 4. Final Check
-- --------------------------------------------------------------------------------------------------------------------
SELECT * FROM Nutrition_staging ORDER BY patient_id ASC;
-- Data is now labeled, structured, and clean for analysis or merging with other health metrics.
-- --------------------------------------------------------------------------------------------------------------------