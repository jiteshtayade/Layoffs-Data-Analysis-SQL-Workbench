use world_layofffs;

SELECT count(*) FROM layoffs
;

-- 1. Remove Duplicates
-- 2. Standarize Data
-- 3. REmove Null values or blank values
-- 4. Remove Columns

CREATE TABLE layoffs_stage1
 Select * from layoffs;
 

 
  Select *,
  ROW_NUMBER() OVER(PARTITION BY company,location,industry) AS row_num
  from layoffs_stage1;
  
  
  
  With dup_cte as 
  (
  Select *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location,industry, total_laid_off,percentage_laid_off,stage, country, `date`,funds_raised_millions) AS row_num
  from layoffs_stage1

  )
  SELECT * FROM dup_cte
  WHERE row_num > 1;
  
  CREATE TABLE `layoffs_stage2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

  SELECT * FROM layoffs_stage2;
  
  Insert into layoffs_stage2
   Select *,
  ROW_NUMBER() OVER(
  PARTITION BY company, location,industry, total_laid_off,percentage_laid_off,stage, country, `date`,funds_raised_millions) AS row_num
  from layoffs_stage1;
  
Delete from layoffs_stage2 
where row_num > 1;

SELECT * FROM layoffs_stage2
where row_num > 1;
;

-- STANDARIZE DATA

SELECT company, (TRIM(company)) FROM layoffs_stage2;

update layoffs_stage2 
set company = TRIM(company);

SELECT company FROM layoffs_stage2;


SELECT DISTINCT industry
FROM layoffs_stage2
ORDER BY industry;

select * from layoffs_stage2
where industry like 'crypto%';

UPDATE layoffs_stage2
SET industry = 'Crypto'
WHERE industry like 'crypto%';

SELECT DISTINCT location
FROM layoffs_stage2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_stage2
ORDER BY country;

UPDATE layoffs_stage2
SET country = TRIM(TRAILING '.' FROM country );

SELECT `date` ,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_stage2;

UPDATE layoffs_stage2
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_stage2 
MODIFY COLUMN `date` date;

-- null
SELECT * FROM layoffs_stage2
WHERE total_laid_off IS NULL
and percentage_laid_off IS NULL;

SELECT *
FROM layoffs_stage2
WHERE industry = ''
OR industry IS NULL;

SELECT *
FROM layoffs_stage2
WHERE company = 'Airbnb';

UPDATE layoffs_stage2
SET industry = NULL
WHERE industry = '';

SELECT t1.industry, t2.industry 
FROM layoffs_stage2 AS t1 
JOIN layoffs_stage2 AS t2
		ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_stage2 AS t1 
JOIN layoffs_stage2 AS t2
		ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT * from layoffs_stage2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE from layoffs_stage2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_stage2
DROP COLUMN row_num ;

-- Exploratory Data Analysis
select * from layoffs_stage2;


SELECT max(total_laid_off) FROM layoffs_stage2;

select * 
	from layoffs_stage2
    where percentage_laid_off = 1 
    order by funds_raised_millions desc;
    
select company, sum(total_laid_off)
	from layoffs_stage2
    group by company
    order by 2 desc;

select min(`date`), max(`date`) from layoffs_stage2; 

select industry, sum(total_laid_off)
	from layoffs_stage2
    group by industry
    order by 2 desc;

select country, sum(total_laid_off)
	from layoffs_stage2
    group by country
    order by 2 desc;

select year(`date`), sum(total_laid_off)
	from layoffs_stage2 where year(`date`) is not null
    group by year(`date`)
    order by 1 desc;

##rolling sum of layoffs

SELECT 
    
    DATE_FORMAT(`date`, '%m-%Y') AS month_year,
    sum(total_laid_off)
FROM layoffs_stage2
WHERE `date` IS NOT NULL
group by month_year
ORDER BY month_year;

with rolling_cycle as (
select 
	substring(`date`, 1, 7) as `Month`,
    sum(total_laid_off) as total_layoff,
    year(`date`) as `year`
    from layoffs_stage2
    where substring(`date`, 1, 7) is not null 
    group by substring(`date`, 1, 7), year(`date`)
    order by 1
    )
    
    select `Month`, total_layoff,
    sum(total_layoff) over(partition by `year` order by `Month`) as rooling_total_layoff_month
    from rolling_cycle;

# ranking top 5 companies wrt thier lay off
with company_year(company, years, total_laid_off) as (
select company, year(`date`),sum(total_laid_off)
	from layoffs_stage2
    group by company,year(`date`)
    ),
    company_year_rank as(
		select * ,
		dense_rank() over(partition by years order by total_laid_off desc) as ranks
		from company_year
		where years is not null
    )
    select * from company_year_rank
    where ranks <= 5;
    