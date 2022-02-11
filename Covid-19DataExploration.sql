
/*
Covid-19 data as of 08/02/2022
Source: https://ourworldindata.org/covid-deaths

Techniquies used: 
- Joins
- Subqueries
- Case 
- Having clause
- Window functions
- Aggregate functions
- Temporary tables
- Views

Tables used:
- [PortfolioProject].[dbo].[CovidDeaths] = PortfolioProject..CovidDeaths (using the latter notation for simplicity)
- [PortfolioProject].[dbo].[CovidVaccination] = PortfolioProject..CovidVaccination
*/


-- WORLDWIDE: number of cases, deaths and fatality rate (ratio between deaths and cases)

SELECT 
	SUM(CONVERT(INT, new_deaths)) AS total_deaths, 
	SUM(new_cases) AS total_cases,
	ROUND(SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100, 4) AS fatality_rate
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL;

-------------------------------------------------------------------------------------------------------------------- 
/* 
WORLDWIDE: infection rate (percentage of the population infected with Covid-19)
Using CTE to calculate the total cases and population, which had been partitioned by location
*/

WITH world_cases_pop (total_cases, population)
AS
(
SELECT SUM(MAX(total_cases)) OVER (PARTITION BY location), SUM(MAX(population)) OVER (PARTITION BY location)
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location)
SELECT ROUND(SUM(total_cases) / SUM(population) * 100, 4) AS infection_rate
FROM world_cases_pop;

-------------------------------------------------------------------------------------------------------------------- 
/* 
WORLDWIDE: vaccination rate (percentage of the population that is fully vaccinnated)
Creating temporary tables to hold the total population and the total number of people fully vaccinated. This could've 
also been done using CTE's, as in the query above 
*/

-- Temporary table with the number of the world population:
DROP TABLE IF EXISTS #world_pop
CREATE TABLE #world_pop
(
	location nvarchar(250),
	population NUMERIC
)
INSERT INTO #world_pop
SELECT location, SUM(MAX(CONVERT(NUMERIC, population))) OVER (PARTITION BY location)
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location;

-- Temporary table with the number of people who have been fully vaccinated:
DROP TABLE IF EXISTS #world_fully_vacc
CREATE TABLE #world_fully_vacc
(
	location nvarchar(250),
	fully_vaccinated NUMERIC
)
INSERT INTO #world_fully_vacc
SELECT d.location, MAX(CONVERT(NUMERIC, people_fully_vaccinated))
FROM PortfolioProject..CovidVaccinations AS v
JOIN PortfolioProject..CovidDeaths AS d
	ON v.location = d.location 
	AND v.date = d.date
WHERE d.continent IS NOT NULL
GROUP BY d.location;

-- Getting the actual full vaccination rate (WORLDWIDE) after saving the intermediate numbers in temporary tables:
SELECT CAST(
	(SELECT SUM(fully_vaccinated) FROM #world_fully_vacc) / 
	(SELECT SUM(population) FROM #world_pop) * 100 AS DECIMAL (10,4)) AS word_full_vacc_rate;

-------------------------------------------------------------------------------------------------------------------- 
-- COUNTRY-LEVEL: finding percentage of the population that has been FULLY VACCINATED:  

SELECT 
	d.continent, 
	d.location, 
	CAST(d.date AS DATE) AS date, 
	d.population, 
	SUM(CONVERT(NUMERIC,v.people_fully_vaccinated)) AS people_fully_vaccinated, 
	ROUND(SUM(CONVERT(NUMERIC,v.people_fully_vaccinated) / population) * 100, 4) AS perc_fully_vaccinated
FROM PortfolioProject..CovidDeaths AS d
JOIN PortfolioProject..CovidVaccinations AS v
	ON d.location = v.location
	AND d.date = v.date
WHERE d.continent IS NOT NULL
GROUP BY d.continent, d.location, d.date, d.population
ORDER BY d.continent, d.location, d.date;

-------------------------------------------------------------------------------------------------------------------- 
/* 
COUNTRY-LEVEL: finding the number of cases, deaths and fatality rate at country-level. Also, classifying the
countries according to their fatality_rate (using CASE):
*/

SELECT 
	continent,
	location,
	SUM(CONVERT(INT, new_deaths)) AS total_deaths, 
	SUM(new_cases) AS total_cases,
	ROUND(SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100, 4) AS fatality_rate,
	CASE 
		WHEN  SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 < 2 THEN 'Low'
		WHEN  SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 >= 2 AND 
		      SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 <= 5 THEN 'Medium'
		WHEN  SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 > 5 THEN 'High'
		END AS fatality_rate_level
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent, location
ORDER BY fatality_rate DESC;

-------------------------------------------------------------------------------------------------------------------- 
 -- COUNTRY-LEVEL: finding the top 10 countries with the highest infection rates (using CTE):

WITH location_cases_pop (location, total_cases, population)
AS
(
SELECT location, SUM(MAX(total_cases)) OVER (PARTITION BY location), SUM(MAX(population)) OVER (PARTITION BY location)
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location)

SELECT TOP 10 location, ROUND(SUM(total_cases) / SUM(population) * 100, 4) AS infection_rate
FROM location_cases_pop
GROUP BY location
ORDER BY ROUND(SUM(total_cases) / SUM(population) * 100, 4) DESC;

-------------------------------------------------------------------------------------------------------------------- 
-- COUNTRY-LEVEL: finding the infection rate of specific countries: USA, Australia, and Brazil:

WITH location_cases_pop (location, total_cases, population)
AS
(
SELECT location, SUM(MAX(total_cases)) OVER (PARTITION BY location), SUM(MAX(population)) OVER (PARTITION BY location)
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location)

SELECT location, ROUND(SUM(total_cases) / SUM(population) * 100, 4) AS infection_rate
FROM location_cases_pop
WHERE location IN ('United States', 'Australia', 'Brazil')
GROUP BY location
ORDER BY ROUND(SUM(total_cases) / SUM(population) * 100, 4) DESC;

-------------------------------------------------------------------------------------------------------------------- 
-- AT COUNTRY LEVEL: returning all the countries with infection rate greater than or equal to 25% (using the HAVING clause):

WITH location_cases_pop (location, total_cases, population)
AS
(
SELECT location, SUM(MAX(total_cases)) OVER (PARTITION BY location), SUM(MAX(population)) OVER (PARTITION BY location)
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location)

SELECT location, population, total_cases, ROUND(SUM(total_cases) / SUM(population) * 100, 4) AS infection_rate
FROM location_cases_pop
GROUP BY location, population, total_cases
HAVING ROUND(SUM(total_cases) / SUM(population) * 100, 4) >= 25
ORDER BY ROUND(SUM(total_cases) / SUM(population) * 100, 4) DESC;

-------------------------------------------------------------------------------------------------------------------- 
-- COUNTRY-LEVEL: the 10 countries with the highest fatality rates (ratio between deaths and cases):

SELECT TOP 10
	location,
	SUM(CONVERT(INT, new_deaths)) AS total_deaths, 
	SUM(new_cases) AS total_cases,
	ROUND(SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100, 4) AS fatality_rate
From PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY fatality_rate DESC;

-------------------------------------------------------------------------------------------------------------------- 
-- AT CONTINENT_LEVEL: number of deaths, cases, and fatality rate broken down by continents:

SELECT
	continent,
	SUM(CONVERT(INT, new_deaths)) AS total_deaths, 
	SUM(new_cases) AS total_cases,
	ROUND(SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100, 4) AS fatality_rate
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY fatality_rate DESC;

-------------------------------------------------------------------------------------------------------------------- 
-- Examples of views created for reporting purposes (using COALESCE to replace 'NULL' with 0):

-- 1st view:
CREATE VIEW fully_vaccinated AS
SELECT 
	d.continent, 
	d.location, 
	CAST(d.date AS DATE) AS date, 
	d.population, 
	COALESCE(SUM(CONVERT(NUMERIC,v.people_fully_vaccinated)), 0) AS people_fully_vaccinated, 
	ROUND(COALESCE(SUM(CONVERT(NUMERIC,v.people_fully_vaccinated) / population) * 100, 0), 4) AS perc_fully_vaccinated
FROM PortfolioProject..CovidDeaths AS d
JOIN PortfolioProject..CovidVaccinations AS v
	ON d.location = v.location
	AND d.date = v.date
WHERE d.continent IS NOT NULL
GROUP BY d.continent, d.location, d.date, d.population;


-- 2nd view:
CREATE VIEW infection_fatality_rate AS 
SELECT 
	continent,
	location,
	population,
	COALESCE(SUM(new_cases), 0) AS total_cases,
	ROUND(COALESCE(SUM(new_cases), 0) / population * 100, 4) AS infection_rate,
	COALESCE(SUM(CONVERT(INT, new_deaths)), 0) AS total_deaths,
	ROUND(COALESCE(SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100, 0), 4) AS fatality_rate,
	CASE 
		WHEN  SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 < 2 THEN 'Low'
		WHEN  SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 >= 2 AND 
		      SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 <= 5 THEN 'Medium'
		WHEN  SUM(CONVERT(INT, new_deaths)) / SUM(new_cases) * 100 > 5 THEN 'High'
		ELSE 'None'
		END AS fatality_rate_level
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY continent, location, population;