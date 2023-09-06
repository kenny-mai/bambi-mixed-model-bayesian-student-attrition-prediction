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
	WHERE date_key = '6/6/2023'
),
osis AS (
	SELECT 
		dwh_scholar_id,
		osis_student_id,
		enrollment_status
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
    WHERE date_key <= '6/6/2023'
    AND date_key >= '8/3/2022'
    AND attendance_status NOT IN ('N/A')
	AND sa_scholar_id IN (SELECT sa_scholar_id FROM out_sch)
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
	WHERE school_date = '6/6/2023'
	AND sa_scholar_id IN (SELECT sa_scholar_id FROM out_sch)
),
dsoi AS (
	SELECT
		sa_scholar_id,
		new_or_returning
	FROM sacs.dim_scholar_other_info
	WHERE sa_scholar_id IN (SELECT sa_scholar_id FROM out_sch)
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
-- scholar movement, used to determine next grade and school
mp AS (
	SELECT 
		sa_scholar_id, 
		current_school, 
		CASE
			WHEN future_school = 'BS' THEN 'SA-BS'
			WHEN future_school = 'BSMS' THEN 'SA-BSMS'
			WHEN future_school = 'BX1MS' THEN 'SA-BX1MS'
			WHEN future_school = 'BX2' THEN 'SA-BX2'
			WHEN future_school = 'BX2MS' THEN 'SA-BX2MS'
			WHEN future_school = 'CH' THEN 'SA-CH'
			WHEN future_school = 'DPMS' THEN 'SA-DPMS'
			WHEN future_school = 'EFMS' THEN 'SA-EFMS'
			WHEN future_school = 'H2' THEN 'SA-H2'
			WHEN future_school = 'HE' THEN 'SA-HE'
			WHEN future_school = 'HNC' THEN 'SA-HNC'
			WHEN future_school = 'HSLA-HA' THEN 'SA-HSLA-HA'
			WHEN future_school = 'HSLA-MA' THEN 'SA-HSLA-MA'
			WHEN future_school = 'HYMS' THEN 'SA-HYMS'
			WHEN future_school = 'LAMS' THEN 'SA-LAMS'
			WHEN future_school = 'MWMS' THEN 'SA-MWMS'
			WHEN future_school = 'MYMS' THEN 'SA-MYMS'
			WHEN future_school = 'OZMS' THEN 'SA-OZMS'
			WHEN future_school = 'SG' THEN 'SA-SG'
			WHEN future_school = 'SGMS' THEN 'SA-SGMS'
			ELSE future_school
		END AS future_school,
		grade, 
		future_grade 
	FROM prod_scholar_movement_n_placement.prod_scholar_movement_n_placement_midyear_yoy_combined_transfer_detail
),
-- ms and hs placements, used to determine next grade and school
prfc AS (
	SELECT 
		id_number AS sa_scholar_id,
		school,
		CASE 
			WHEN projected_school IS NULL AND completion_status = 'Never Logged In' THEN 'No School - Marked Never Logged In'
			WHEN projected_school IS NULL AND completion_status = 'Not Returning' THEN 'No School - Marked Not Returning'
			WHEN projected_school IS NULL AND completion_status = 'Logged In' THEN 'No School - Marked Logged In'
			WHEN projected_school IS NULL AND completion_status = 'Returning' THEN 'No School - Marked Returning'
			WHEN projected_school = 'SA Ozone Park Middle School' THEN 'SA-OZMS'
			WHEN projected_school = 'SA Hudson Yards Middle School' THEN 'SA-HY'
			WHEN projected_school = 'SA Bronx 2 Middle School' THEN 'SA-BX2MS'
			WHEN projected_school = 'SA Far Rockaway Middle School' THEN 'SA-FRMS'
			WHEN projected_school = 'SA Bed-Stuy Middle School' THEN 'SA-BSMS'
			WHEN projected_school = 'SA Bronx Middle School' THEN 'SA-BXMS'
			WHEN projected_school = 'SA Harlem West' THEN 'SA-HW'
			WHEN projected_school = 'SA Bronx 1 Middle School' THEN 'SA-BX1MS'
			WHEN projected_school = 'SA High School of the Liberal Arts-Brooklyn' THEN 'SA-HSLA-BK'
			WHEN projected_school = 'SA Ditmas Park Middle School' THEN 'SA-DPMS'
			WHEN projected_school = 'SA Hamilton Heights Middle School' THEN 'SA-HHMS'
			WHEN projected_school = 'SA Harlem North Central' THEN 'SA-HNC'
			WHEN projected_school = 'SA High School of the Liberal Arts-Manhattan' THEN 'SA-HSLA-MA'
			WHEN projected_school = 'SA Midtown West Middle School' THEN 'SA-MWMS'
			WHEN projected_school = 'SA Lafayette Middle School' THEN 'SA-LAMS'
			WHEN projected_school = 'SA High School of the Liberal Arts-Harlem' THEN 'SA-HSLA-HA'
			WHEN projected_school = 'SA Springfield Gardens Middle School' THEN 'SA-SGMS'
			WHEN projected_school = 'SA East Flatbush Middle School' THEN 'SA-EFMS'
		WHEN projected_school = 'SA Harlem East' THEN 'SA-HE'
			ELSE projected_school
		END AS projected_school,
		completion_status
	FROM prod_ms_hs_placement.prod_scholar_ms_hs_placements_ranking_form_completion
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
	AND incident_dt <= '6/6/2023'
	AND incident_dt >= '8/3/2022'
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
	WHERE yr = 2023
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
-- adding all other features, using placements for next schools first, then filling nulls with midyear movement
stage2 AS (
	SELECT
		stage1.sa_scholar_id,
		gender,
		race_ethnicity,
		scholar_grade,
		future_grade,
		school_name,
		CASE WHEN projected_school IS NULL THEN future_school ELSE projected_school END AS future_school,
		address,
		tardy_percent,
		absent_percent,
		count_rep,
		count_sus,
		ell_status,
		sped_status,
		frpl_status,
		critical_outlier,
		new_or_returning
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
	LEFT JOIN mp
	ON stage1.sa_scholar_id = mp.sa_scholar_id
	LEFT JOIN prfc
	ON stage1.sa_scholar_id = prfc.sa_scholar_id
	LEFT JOIN dsoi
	ON stage1.sa_scholar_id = dsoi.sa_scholar_id
	ORDER BY 1
),
-- final cleanup. really important: the nulls for attrited are the intra-year attrits.
-- since we are only concerned about inter-year attrition for this ad-hoc request, 
-- we're not including these scholars in the final output. not efficient, but we can rewrite later
-- hard coding next grades to keep them strings
-- Bed-Stuy 2 merged with Bed-Stuy. Bed Stuy 1 and Fort Greene no longer exist
-- scholars going up a grade and staying within their school coded in
-- removed 12th graders because they're not coming back
stage3 AS (
	SELECT	
		sa_scholar_id,
		gender,
		race_ethnicity,
		scholar_grade,
		CASE 
			WHEN future_grade IS NULL AND scholar_grade = 'K' THEN '1'
			WHEN future_grade IS NULL AND scholar_grade = '1' THEN '2'
			WHEN future_grade IS NULL AND scholar_grade = '2' THEN '3'
			WHEN future_grade IS NULL AND scholar_grade = '3' THEN '4'
			WHEN future_grade IS NULL AND scholar_grade = '4' THEN '5'
			WHEN future_grade IS NULL AND scholar_grade = '5' THEN '6'
			WHEN future_grade IS NULL AND scholar_grade = '6' THEN '7'
			WHEN future_grade IS NULL AND scholar_grade = '7' THEN '8'
			WHEN future_grade IS NULL AND scholar_grade = '8' THEN '9'
			WHEN future_grade IS NULL AND scholar_grade = '9' THEN '10'
			WHEN future_grade IS NULL AND scholar_grade = '10' THEN '11'
			WHEN future_grade IS NULL AND scholar_grade = '11' THEN '12'
			WHEN future_grade IS NULL AND scholar_grade = '12' THEN 'G'
			ELSE future_grade
		END AS future_grade,
		CASE WHEN school_name = 'SA-BS2' THEN 'SA-BS' ELSE school_name END,
		CASE WHEN future_school IS NULL THEN school_name ELSE future_school END AS future_school,
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
		new_or_returning
	FROM stage2
	WHERE scholar_grade != '12'
)
SELECT * FROM stage3