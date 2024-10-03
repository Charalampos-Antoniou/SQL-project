-- 1. DATA CLEANING --

USE Layoffs

SELECT *
FROM layoffs;

SELECT * 
INTO layoffs_staging
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- 2. IDENTIFYING DUPLICATES --

SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company, industry, total_laid_off, percentage_laid_off, date ORDER BY date) as row_num	
FROM layoffs_staging;

-- Creating a cte to find duplicate rows -- 

WITH duplicate_cte AS 
(
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions ORDER BY date) as row_num	
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

-- Deleting duplicates from cte and from the layoffs_staging table --

WITH duplicate_cte AS 
(
SELECT *,
	ROW_NUMBER() OVER(
	PARTITION BY company,location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions ORDER BY date) as row_num	
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging;

-- 3. STANDARDIZING DATA --

SELECT company, TRIM(company)
FROM layoffs_staging;

UPDATE layoffs_staging
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY 1;

SELECT *
FROM layoffs_staging
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT location
FROM layoffs_staging
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging
WHERE country LIKE 'United States%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging
WHERE country LIKE 'United States%'
ORDER BY 1;

UPDATE layoffs_staging
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT date
FROM layoffs_staging;

SELECT date, CONVERT(DATE, date, 120)
FROM layoffs_staging;

UPDATE layoffs_staging
SET date = CONVERT(DATE, date, 120);

ALTER TABLE layoffs_staging
ALTER COLUMN date DATE;

SELECT *
FROM layoffs_staging;

-- 4. HANDLING NULL VALUES --

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL ;

-- Industry Column has nulls --

SELECT DISTINCT(industry)
FROM layoffs_staging;

SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging
WHERE company='Airbnb';

-- Matching Industries that have the same company name and location in order to populate the NULLS--

SELECT t1.industry, t2.industry
FROM layoffs_staging t1
JOIN layoffs_staging t2
	ON t1.company=t2.company
	AND t1.location=t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

-- Populating NULLS with industry that--

UPDATE t1
SET t1.industry = t2.industry
FROM layoffs_staging t1
JOIN layoffs_staging t2
    ON t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging
WHERE company LIKE 'Bally%';

-- Deleting rows if columns: total_laid_off, percentage_laid_off, are NULLS --

SELECT *
FROM layoffs_staging;

SELECT *
FROM layoffs_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging;

-- 5. EXPLONATORY DATA ANALYSIS --

USE Layoffs

SELECT *
FROM layoffs_staging;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging;

-- Companies that lost of all their employees --

SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1;

-- Companies that lost of all their employees with largest layoffs --

SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

-- Companies that lost of all their employees and had a lot of funding order by desc--

SELECT *
FROM layoffs_staging
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Companies that had the most layoffs order by desc --

SELECT company, sum(total_laid_off)
FROM layoffs_staging
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(date), MAX(date)
FROM layoffs_staging;

-- Industries that had the most layoffs order by desc --

SELECT industry, sum(total_laid_off)
FROM layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

-- Countries that had the most layoffs --

SELECT country, sum(total_laid_off)
FROM layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

-- Years that had the most layoffs --

SELECT YEAR(date), sum(total_laid_off)
FROM layoffs_staging
GROUP BY YEAR(date)
ORDER BY 2 DESC;

-- Most layoffs by stage that the companies were in --

SELECT stage, sum(total_laid_off)
FROM layoffs_staging
GROUP BY stage
ORDER BY 2 DESC;

-- Rolling total of layoffs --

SELECT FORMAT(date, 'yyyy-MM') AS year_month, sum(total_laid_off)
FROM layoffs_staging
WHERE FORMAT(date, 'yyyy-MM') IS NOT NULL
GROUP BY FORMAT(date, 'yyyy-MM')
ORDER BY 1 ASC;

WITH rolling_total AS 
(
    SELECT YEAR(date) AS year, MONTH(date) AS month, SUM(total_laid_off) AS total_off
    FROM layoffs_staging
    WHERE date IS NOT NULL
    GROUP BY YEAR(date), MONTH(date)
)
SELECT CAST(year AS VARCHAR) + '-' + RIGHT('0' + CAST(month AS VARCHAR), 2) AS year_month, 
	total_off,
    SUM(total_off) OVER (ORDER BY year, month ASC) AS rolling_total
FROM rolling_total;

SELECT company, YEAR(date), sum(total_laid_off)
FROM layoffs_staging
GROUP BY company, YEAR(date)
ORDER BY 3 DESC;

-- Year by Year most layoffs by company --

WITH Company_year (company, years, total_laid_off) AS 
(
SELECT company, YEAR(date), sum(total_laid_off)
FROM layoffs_staging
GROUP BY company, YEAR(date)
), Company_Year_Rank AS 
(
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

