-- PROJECT - DATA CLEANING --

### Plan

-- 1. Remove Duplicates --
-- 2. Standarise the data -- 
-- 3. Null Values/Blank Values
-- 4. Remove Any Columns

#Create similar table, for data safety
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

-- CLEANING --

#Starting from adding additional ID's, and deleting duplicates after

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are our real duplicates 
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;

-- these are the ones we want to delete where the row number is > 1 or 2or greater essentially
-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. 
-- Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;


DELETE
FROM layoffs_staging2
WHERE row_num >1;


SELECT *
FROM layoffs_staging2;
#yaaay 

-- 2. Standardize Data --
#trim

SELECT company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company); #trim blank spaces

#
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

#if names are similar, with extra marks:
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

#continue:

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';
#change date from txt to "timeseries"

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') AS date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); 
#now only in dateformat but still txt

SELECT date
FROM layoffs_staging2;

#change for DATE
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

#NULL and BLANK VALUES

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

#we need Join for it to figure out that from same companies we have same industry if the same location
#first search

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
     AND t1.location = t2_location
WHERE (t1,industry IS NULL OR t1.inndustry = '')
AND t2.industry IS NOT NULL;

#OR
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
     AND t1.location = t2_location
WHERE (t1,industry IS NULL OR t1.inndustry = '')
AND t2.industry IS NOT NULL;


#we have to do it differently - when its blan '' we insseert null val and update THEN, delete other null after from totel and %laid off
#NULL and BLANK VALUES

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

--
#searching

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

#update

UPDATE layoffs_staging t1 #updatujemy t1
JOIN layoffs_staging2 t2 #join on t2
	ON t1.company = t2.company 
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL; #t1 is blank one a t2 nie jest blank

#delete with total and %
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

