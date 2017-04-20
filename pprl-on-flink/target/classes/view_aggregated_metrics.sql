CREATE MATERIALIZED VIEW IF NOT EXISTS aggregated_metrics AS
	SELECT
		j.job_type AS job_type,
		j.job_name AS job_name,
		j.data_description AS data_description,
		j.parallelism AS parallelism,
		j.records AS records,
		COALESCE(j.hash_families, -1) AS hash_families,
		COALESCE(j.hash_functions, -1) AS hash_functions,		
		ROUND((AVG(j.execution_time) / 1000), 2) AS execution_time, 
		ROUND((STDDEV(j.execution_time) / 1000), 2) AS execution_time_stddev, 
		ROUND((MIN(j.execution_time) / 1000), 2) AS execution_time_min, 
		ROUND((MAX(j.execution_time) / 1000), 2) AS execution_time_max, 	
		ROUND(AVG(j.reduction_rate), 2) AS reduction_rate, 
		ROUND(STDDEV(j.reduction_rate), 2) AS reduction_rate_stddev, 
		ROUND(MIN(j.reduction_rate), 2) AS reduction_rate_min, 
		ROUND(MAX(j.reduction_rate), 2) AS reduction_rate_max, 		
		ROUND(AVG(j.pairs_Completeness), 2) AS pairs_completeness, 
		ROUND(STDDEV(j.pairs_Completeness), 2) AS pairs_completeness_stddev, 
		ROUND(MIN(j.pairs_Completeness), 2) AS pairs_completeness_min, 
		ROUND(MAX(j.pairs_Completeness), 2) AS pairs_completeness_max, 	
		ROUND(AVG(j.pairs_quality), 2) AS pairs_quality, 
		ROUND(STDDEV(j.pairs_quality), 2) AS pairs_quality_stddev, 
		ROUND(MIN(j.pairs_quality), 2) AS pairs_quality_min, 
		ROUND(MAX(j.pairs_quality), 2) AS pairs_quality_max, 
		ROUND(AVG(j.f_measure), 2) AS f_measure, 
		ROUND(STDDEV(j.f_measure), 2) AS f_measure_stddev, 
		ROUND(MIN(j.f_measure), 2) AS f_measure_min, 
		ROUND(MAX(j.f_measure), 2) AS f_measure_max 
		
		FROM 
		
			job_metrics j
			
		JOIN
			
		(SELECT DISTINCT
				k.job_type AS job_type, 
				k.job_name AS job_name, 
				k.data_description AS data_description,
				k.parallelism AS parallelism,
				k.records AS records, 
				k.hash_families AS hash_families,
				k.hash_functions AS hash_functions					
				FROM job_metrics k) l
			
			ON  j.job_type = l.job_type
			AND j.job_name = l.job_name
			AND j.data_description = l.data_description
			AND j.parallelism = l.parallelism
			AND j.records = l.records
			AND ((j.hash_families IS NULL AND l.hash_families IS NULL) OR j.hash_families = l.hash_families)
			AND ((j.hash_functions IS NULL AND l.hash_functions IS NULL) OR j.hash_functions = l.hash_functions) 
			GROUP BY 
			j.job_type, 
			j.job_name, 
			j.data_description, 
			j.parallelism,
			j.records, 
			j.hash_families,
			j.hash_functions;
