/*
 * Title: Ad-Hoc Attrition Predicting Data Query 2022-2023
 * Author: Kenny Mai
 * Date: 4/27/2023
 * Description: 
 * 		Need to build a quick predictive model for inter-year scholar attrition.
 * 		This query takes some common features that affect attrition
 * 		during a snapshot of the year. To use this code for other 
 * 		time periods, the following should be updated: date_key in sch,
 * 		date_key in fdss, incident_dt in fs2, yr in co, date_key in out_sch,
 * 		date_key in return_sch, and cyc_end_date in stage2.
 * 
 * 		These data will go into the predictive modeling to estimate attrition,
 * 		hence this set has no attrited variable.
 * 
 * Last Modified By: Kenny Mai
 */
WITH
-- collecting a flag for the students who are enrolled and have attedance taken
said AS (
	SELECT
		dwh_scholar_id,
		sa_scholar_id
	FROM sacs.fact_daily_scholar_status
	WHERE date_key = '6/8/2022'
),
osis AS (
	SELECT 
		dwh_scholar_id,
		osis_student_id
	FROM sacs.dim_scholar
),
out_sch AS (
	SELECT 
		sa_scholar_id,	
		said.dwh_scholar_id,
		osis_student_id
	FROM said
	LEFT JOIN osis
	ON said.dwh_scholar_id = osis.dwh_scholar_id
),
-- determining whether or not scholar returned in the future year for at least 5 days
return_sch AS (
	SELECT
		sa_scholar_id,
		SUM(CASE WHEN attendance_status IN ('P') THEN 1 ELSE 0 END) AS present_days,
		CASE WHEN present_days < 5 THEN 1 ELSE 0 END AS attrited
	FROM sacs.fact_daily_scholar_status
	WHERE date_key >= '8/3/2022'
	AND date_key <='6/8/2023'
	GROUP BY 1
),
-- creating flag if the scholar did not return that next school year
attrit AS (
	SELECT
		out_sch.sa_scholar_id,
		CASE WHEN attrited IS NULL THEN 1 ELSE attrited END AS attrited
	FROM out_sch
	LEFT JOIN return_sch
	ON out_sch.sa_scholar_id = return_sch.sa_scholar_id
),
-- attendance data, restricted time period for relevant observations
-- contains a row for each day in the year the child was present for
fdss AS (
	SELECT
		date_key, 
		dwh_scholar_id,
		sa_scholar_id,
		attendance_status,
		excused
    FROM sacs.fact_daily_scholar_status		
    WHERE date_key <= '6/8/2022'
    AND date_key >= '8/3/2021'
    AND attendance_status NOT IN ('N/A')
	AND sa_scholar_id IN (SELECT sa_scholar_id FROM out_sch)
),
-- first sa day to determine new or returning since  we don't keep historical new or returning statuses
dsoi AS (
	SELECT
		sa_scholar_id,
		first_sa_day
	FROM sacs.dim_scholar_other_info
	WHERE sa_scholar_id IN (SELECT sa_scholar_id FROM out_sch)
),
-- scholar demographics
dd AS (
	SELECT
		school_date,
		sa_scholar_id,
		gender,
		race_ethnicity,
		scholar_grade,
		school_name,
		ell_status,
		sped_status,
		frpl_status
	FROM prod_scholar_demographics.demographics_details
	WHERE sa_scholar_id IN (SELECT sa_scholar_id FROM out_sch)
	AND school_date = '6/8/2022'
),
-- scholar addresses
cd_dup AS (
	SELECT
		sa_scholar_id,
		address,
		ROW_NUMBER() OVER (PARTITION BY sa_scholar_id ORDER BY address) AS dupcnt_cd
	FROM prod_scholar_contact.prod_scholar_contact_details
	WHERE sa_scholar_id IN (SELECT sa_scholar_id FROM out_sch)
),
cd AS (
	SELECT 
		sa_scholar_id,
		address
	FROM cd_dup
	WHERE dupcnt_cd = 1
),
-- number of incidents recorded during time period, restricted to time duration of interest
fs1 AS (
	SELECT
		idnumber AS sa_scholar_id,
		COUNT(idnumber) AS count_rep
	FROM sacs.fact_suspension
	WHERE incidenttype_nm LIKE 'REPRIMAND%'
	AND incident_dt <= '6/8/2022'
	AND incident_dt >= '8/3/2021'
	GROUP BY 1
),
-- number of suspensions recorded during time period, restricted to time duration of interest
fs2 AS (
	SELECT
		idnumber AS sa_scholar_id,
		COUNT(idnumber) AS count_sus
	FROM sacs.fact_suspension
	WHERE incidenttype_nm LIKE 'SUSPENSION%'
	AND incident_dt <= '6/8/2022'
	AND incident_dt >= '8/3/2021'
	GROUP BY 1
),
-- critical outlier data, taking the overall outlier flag, not the individual ones, a few dupes, but identical
co_dup AS (
	SELECT 
		dwh_scholar_id, 
		regular_outlier, 
		EXTRACT(YEAR FROM CAST(run_date AS DATE)) AS yr,
		CASE 
			WHEN regular_outlier IS TRUE 
				THEN '1' 
			ELSE '0' 
		END AS critical_outlier,
		ROW_NUMBER() OVER (PARTITION BY dwh_scholar_id ORDER BY dwh_scholar_id) AS dupcnt_co
	FROM raw_data_science.crit_outliers_interyear_attrition
	WHERE yr = 2022
	GROUP BY 1,2,3
),
co AS (
	SELECT
		*
	FROM co_dup
	WHERE dupcnt_co = 1
),
-- consolidating rows for attendance data, ends with a single row per scholar
stage1 AS (
    SELECT 
    	dwh_scholar_id,    
    	sa_scholar_id,
        min(date_key) AS cyc_start_date,
        max(date_key) AS cyc_end_date,
        SUM(CASE WHEN attendance_status IN ('P') THEN 1 ELSE 0 END) AS present_days,
        SUM(CASE WHEN attendance_status IN ('T') THEN 1 ELSE 0 END) AS tardy_days,
        SUM(CASE WHEN attendance_status IN ('T', 'P') THEN 1 ELSE 0 END) AS total_for_tardy, 
        SUM(CASE WHEN attendance_status IN ('A') AND excused = 'False' THEN 1 ELSE 0 END) AS absent_days,
        SUM(CASE WHEN attendance_status IN ('A') and excused = 'True' THEN 1 ELSE 0 END) AS excused_absent_days,
        (tardy_days+present_days+excused_absent_days+absent_days) AS total_days,
        CASE 
        	WHEN total_for_tardy > 0
        		THEN ROUND(tardy_days::FLOAT / total_for_tardy::FLOAT, 4) 
        	ELSE 0
        END AS tardy_percent,
        CASE
        	WHEN total_days > 0
        		THEN ROUND(absent_days::FLOAT / total_days::FLOAT, 4)
        	ELSE 0
        END AS absent_percent
    FROM fdss
    GROUP BY 1,2
),
-- adding all other features
stage2 AS (
	SELECT
		stage1.sa_scholar_id,
		first_sa_day,
		gender,
		race_ethnicity,
		scholar_grade,
		school_name,
		address,
		tardy_percent,
		absent_percent,
		count_rep,
		count_sus,
		ell_status,
		sped_status,
		frpl_status,
		critical_outlier,
		attrited
	FROM stage1
	LEFT JOIN fs1
	ON stage1.sa_scholar_id = fs1.sa_scholar_id
	LEFT JOIN fs2
	ON stage1.sa_scholar_id = fs2.sa_scholar_id
	LEFT JOIN dd
	ON stage1.sa_scholar_id = dd.sa_scholar_id
	LEFT JOIN co
	ON stage1.dwh_scholar_id = co.dwh_scholar_id
	LEFT JOIN cd
	ON stage1.sa_scholar_id = cd.sa_scholar_id
	LEFT JOIN attrit
	ON stage1.sa_scholar_id = attrit.sa_scholar_id
	LEFT JOIN dsoi
	ON stage1.sa_scholar_id = dsoi.sa_scholar_id
	ORDER BY 1
),
-- final cleanup. really important: the nulls for attrited are the intra-year attrits.
-- since we are only concerned about inter-year attrition for this ad-hoc request, 
-- we're not including these scholars in the final output. not efficient, but we can rewrite later
-- Bed-Stuy 2 merged with Bed-Stuy. Bed Stuy 1 and Fort Greene no longer exist
-- removed 12th graders because they're not coming back
stage3 AS (
	SELECT	
		sa_scholar_id,
		CASE 
			WHEN first_sa_day >= '6/22/2021'
				THEN 'New'
			ELSE 'Returning'
		END AS new_or_returning,
		gender,
		race_ethnicity,
		scholar_grade,
		CASE WHEN school_name = 'SA-BS2' THEN 'SA-BS' ELSE school_name END,
		address,
		tardy_percent,
		absent_percent,
		CASE 
			WHEN count_rep IS NULL 
				THEN 0 
			ELSE count_rep 
		END AS total_rep,
		CASE 
			WHEN count_sus IS NULL 
				THEN 0 
			ELSE count_sus 
		END AS total_sus,
		ell_status,
		sped_status,
		frpl_status,
		CASE
			WHEN critical_outlier = '1'
				THEN TRUE
			ELSE FALSE
		END AS critical_outlier,
		CAST(attrited AS BOOL)
	FROM stage2
	WHERE scholar_grade != '12'
)
SELECT * FROM stage3