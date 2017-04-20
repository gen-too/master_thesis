package dbs.bigdata.flink.pprl.evaluation;

import java.io.Serializable;
import java.math.BigDecimal;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.Table;

import org.hibernate.annotations.DynamicInsert;

import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.Synchronize;

@NamedNativeQueries({
	@NamedNativeQuery(
		name = "refreshAggregatedJobMetrics",
		query = "REFRESH MATERIALIZED VIEW aggregated_metrics"
	),
	
	@NamedNativeQuery(
		name = "getAllAggregatedJobMetrics", 
		query = "SELECT * FROM aggregated_metrics",
		resultClass = AggregatedJobMetricByScenario.class
	),
	
	@NamedNativeQuery(
		name = "getAggregatedJobMetricsByScenario", 
		query =   "SELECT m.* "
				+ "FROM aggregated_metrics m "
				+ "WHERE m.job_type = :jobType "
				+ "AND m.job_name = :jobName "
				+ "AND m.data_description = :dataDescription "
				+ "AND m.parallelism = :parallelism "
				+ "AND m.records = :records "
				+ "AND m.hash_families = :hashFamilies"
				+ "AND m.hash_functions = :hashFunctions",
		resultClass = AggregatedJobMetricByScenario.class
	),
	
	@NamedNativeQuery(
		name = "getDistinctDataDescriptionsFromAggJobMetrics",
		query =   "SELECT DISTINCT m.data_description "
				+ "FROM aggregated_metrics m"
	),
	
	@NamedNativeQuery(
		name = "getDistinctParallismGradesFromAggJobMetrics",
		query =   "SELECT DISTINCT ABS(m.parallelism) "
				+ "FROM aggregated_metrics m "
				+ "ORDER BY ABS(m.parallelism) ASC"
	),
	
	@NamedNativeQuery(
		name = "getDistinctParallismGradesFromAggJobMetricsForDataDescr",
		query =   "SELECT DISTINCT ABS(m.parallelism) "
				+ "FROM aggregated_metrics m "
				+ "WHERE m.data_description = :dataDescription "
				+ "ORDER BY ABS(m.parallelism) ASC"
	),
	
	@NamedNativeQuery(
		name = "getDistinctNumberOfRecordsFromAggJobMetricsForDataDescr",
		query =   "SELECT DISTINCT m.records "
				+ "FROM aggregated_metrics m "
				+ "WHERE m.data_description = :dataDescription"
	),
	
	@NamedNativeQuery(
		name = "getDistinctNumberOfRecordsFromAggJobMetricsForJobNameAndDataDescrAndParallelism",
		query =   "SELECT DISTINCT m.records "
				+ "FROM aggregated_metrics m "
				+ "WHERE ABS(m.parallelism) = :parallelism "
				+ "AND m.data_description = :dataDescription "
				+ "AND m.job_name IN(:jobNames) "
				+ "AND m.records > 5000" //optional
	),

	// Change for considered Jobs
	@NamedNativeQuery(
		name = "getDistinctJobNamesFromAggJobMetricsForDataDescrAndParallelism",
		query =   "SELECT DISTINCT m.job_name "
				+ "FROM aggregated_metrics m "
				+ "WHERE ABS(m.parallelism) = :parallelism "
				+ "AND m.data_description = :dataDescription "
				+ "AND m.job_name LIKE '%PB' "
				+ "ORDER BY m.job_name ASC"
	),
	
	@NamedNativeQuery(
		name = "getDistinctJobNamesFromAggJobMetricsForDataDescr",
		query =   "SELECT DISTINCT m.job_name "
				+ "FROM aggregated_metrics m "
				+ "WHERE m.data_description = :dataDescription "
				+ "AND m.job_name LIKE '%PB' "
				+ "ORDER BY m.job_name ASC"
	),
	
	@NamedNativeQuery(
		name = "getMetricsFromAggJobMetrics",
		query =   "SELECT "
				+ "m.job_name, "
				+ "m.records, "
				+ "m.reduction_rate, "
				+ "m.reduction_rate_stddev, "
				+ "m.pairs_completeness, "
				+ "m.pairs_completeness_stddev, "
				+ "m.pairs_quality, "
				+ "m.pairs_quality_stddev, "
				+ "m.f_measure, "
				+ "m.f_measure_stddev "
				+ "FROM aggregated_metrics m "
				+ "WHERE ABS(m.parallelism) = :parallelism "
				+ "AND m.data_description = :dataDescription "
				+ "AND m.job_name IN (:jobNames) "
				+ "AND m.records > 50000 " //optional
				+ "ORDER BY m.records ASC, m.job_name ASC"
	),
	
	@NamedNativeQuery(
		name = "getMetricsFromAggJobMetricsForCertainNumberOfRecords",
		query =   "SELECT "
				+ "m.job_name, "
				+ "m.records, "
				+ "m.reduction_rate, "
				+ "m.reduction_rate_stddev, "
				+ "m.pairs_completeness, "
				+ "m.pairs_completeness_stddev, "
				+ "m.pairs_quality, "
				+ "m.pairs_quality_stddev, "
				+ "m.f_measure, "
				+ "m.f_measure_stddev "
				+ "FROM aggregated_metrics m "
				+ "WHERE ABS(m.parallelism) = :parallelism "
				+ "AND m.data_description = :dataDescription "
				+ "AND m.job_name IN (:jobNames) "
				+ "AND m.records = :records "
				+ "ORDER BY m.job_name ASC"
	),
	
	@NamedNativeQuery(
		name = "getExecTimeMetricFromAggJobMetrics",
		query =   "SELECT "
				+ "m.job_name, "
				+ "m.records, "
				+ "m.execution_time, "
				+ "m.execution_time_stddev, "
				+ "ABS(m.parallelism) "
				+ "FROM aggregated_metrics m "
				+ "WHERE ABS(m.parallelism) = :parallelism "
				+ "AND m.data_description = :dataDescription "
				+ "AND m.job_name IN (:jobNames) "
				+ "AND m.records > 50000 " //optional
				+ "ORDER BY m.records ASC, m.job_name ASC"
	),
	
	@NamedNativeQuery(
		name = "getExecTimeMetricFromAggJobMetricsForDataDescAndJobName",
		query =   "SELECT "
				+ "m.job_name, "
				+ "m.records, "
				+ "m.execution_time, "
				+ "m.execution_time_stddev, "
				+ "ABS(m.parallelism) "
				+ "FROM aggregated_metrics m "
				+ "WHERE m.data_description = :dataDescription "
				+ "AND m.job_name = :jobName "
				+ "AND m.records > 50000 " // optional
				+ "ORDER BY ABS(m.parallelism) ASC, m.job_name ASC, m.records ASC"
	),
	
	@NamedNativeQuery(
		name = "getExecTimeMetricFromAggJobMetricsForDataDescAndJobNameAndRecords",
		query =   "SELECT "
				+ "m.job_name, "
				+ "m.records, "
				+ "m.execution_time, "
				+ "m.execution_time_stddev, "
				+ "ABS(m.parallelism) "
				+ "FROM aggregated_metrics m "
				+ "WHERE m.data_description = :dataDescription "
				+ "AND m.job_name IN (:jobNames) "
				+ "AND m.records = :records "
				+ "ORDER BY ABS(m.parallelism) ASC, m.job_name ASC"
	)
})

/**
 * A Object of this class represents a record of the
 * aggregated_metrics view.
 * These object contains all metrics (quality and runtime)
 * with its minimum, maximum, average and standard deviation
 * values that are collected from multiple iterations of
 * a pprl flink job.
 * 
 * @author mfranke
 *
 */
@Entity
@Table(name = "aggregated_metrics")
@Immutable
@Synchronize(value = { "job_metrics" })
@DynamicInsert
public class AggregatedJobMetricByScenario implements Serializable{
	
	private static final long serialVersionUID = 82826935639220315L;

	@EmbeddedId
	private Scenario scenario;
	
	@Column(name = "execution_time", precision = 7, scale = 2, nullable = false)
	private BigDecimal executionTime;
	
	@Column(name = "execution_time_stddev", precision = 7, scale = 2)
	private BigDecimal executionTimeStdDev;
	
	@Column(name = "execution_time_min", precision = 7, scale = 2, nullable = false)
	private BigDecimal executionTimeMin;
	
	@Column(name = "execution_time_max", precision = 7, scale = 2, nullable = false)
	private BigDecimal executionTimeMax;
	
	
	@Column(name = "reduction_rate", precision = 5, scale = 2, nullable = false)
	private BigDecimal reductionRate;
	
	@Column(name = "reduction_rate_stddev", precision = 5, scale = 2)
	private BigDecimal reductionRateStdDev;
	
	@Column(name = "reduction_rate_min", precision = 5, scale = 2, nullable = false)
	private BigDecimal reductionRateMin;
	
	@Column(name = "reduction_rate_max", precision = 5, scale = 2, nullable = false)
	private BigDecimal reductionRateMax;
	
	
	@Column(name = "pairs_completeness", precision = 5, scale = 2, nullable = false)
	private BigDecimal pairsCompleteness;	
	
	@Column(name = "pairs_completeness_stddev", precision = 5, scale = 2)
	private BigDecimal pairsCompletenessStdDev;	
	
	@Column(name = "pairs_completeness_min", precision = 5, scale = 2, nullable = false)
	private BigDecimal pairsCompletenessMin;	
	
	@Column(name = "pairs_completeness_max", precision = 5, scale = 2, nullable = false)
	private BigDecimal pairsCompletenessMax;	
		
	
	@Column(name = "pairs_quality", precision = 5, scale = 2, nullable = false)
	private BigDecimal pairsQuality;
	
	@Column(name = "pairs_quality_stddev",precision = 5, scale = 2)
	private BigDecimal pairsQualityStdDev;
	
	@Column(name = "pairs_quality_min", precision = 5, scale = 2, nullable = false)
	private BigDecimal pairsQualityMin;
	
	@Column(name = "pairs_quality_max", precision = 5, scale = 2, nullable = false)
	private BigDecimal pairsQualityMax;
	
	
	@Column(name = "f_measure", precision = 5, scale = 2, nullable = false)
	private BigDecimal fMeasure;
	
	@Column(name = "f_measure_stddev", precision = 5, scale = 2)
	private BigDecimal fMeasureStdDev;
	
	@Column(name = "f_measure_min", precision = 5, scale = 2, nullable = false)
	private BigDecimal fMeasureMin;
	
	@Column(name = "f_measure_max", precision = 5, scale = 2, nullable = false)
	private BigDecimal fMeasureMax;
	
	
	public AggregatedJobMetricByScenario(){}


	public AggregatedJobMetricByScenario(String jobType, String jobName, String dataDescription, int parallelism,
			long numberOfRecords, Integer hashFamilies, Integer hashFunctions, BigDecimal executionTime,
			BigDecimal executionTimeStdDev, BigDecimal executionTimeMin, BigDecimal executionTimeMax,
			BigDecimal reductionRate, BigDecimal reductionRateStdDev, BigDecimal reductionRateMin,
			BigDecimal reductionRateMax, BigDecimal pairsCompleteness, BigDecimal pairsCompletenessStdDev,
			BigDecimal pairsCompletenessMin, BigDecimal pairsCompletenessMax, BigDecimal pairsQuality,
			BigDecimal pairsQualityStdDev, BigDecimal pairsQualityMin, BigDecimal pairsQualityMax, BigDecimal fMeasure,
			BigDecimal fMeasureStdDev, BigDecimal fMeasureMin, BigDecimal fMeasureMax) {
		this.scenario = new Scenario(jobType, jobName, dataDescription, parallelism, numberOfRecords, hashFamilies, hashFunctions);
		this.executionTime = executionTime;
		this.executionTimeStdDev = executionTimeStdDev;
		this.executionTimeMin = executionTimeMin;
		this.executionTimeMax = executionTimeMax;
		this.reductionRate = reductionRate;
		this.reductionRateStdDev = reductionRateStdDev;
		this.reductionRateMin = reductionRateMin;
		this.reductionRateMax = reductionRateMax;
		this.pairsCompleteness = pairsCompleteness;
		this.pairsCompletenessStdDev = pairsCompletenessStdDev;
		this.pairsCompletenessMin = pairsCompletenessMin;
		this.pairsCompletenessMax = pairsCompletenessMax;
		this.pairsQuality = pairsQuality;
		this.pairsQualityStdDev = pairsQualityStdDev;
		this.pairsQualityMin = pairsQualityMin;
		this.pairsQualityMax = pairsQualityMax;
		this.fMeasure = fMeasure;
		this.fMeasureStdDev = fMeasureStdDev;
		this.fMeasureMin = fMeasureMin;
		this.fMeasureMax = fMeasureMax;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((scenario == null) ? 0 : scenario.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		AggregatedJobMetricByScenario other = (AggregatedJobMetricByScenario) obj;
		if (scenario == null) {
			if (other.scenario != null) {
				return false;
			}
		} else if (!scenario.equals(other.scenario)) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("RoundedJobMetricsByScenario [");
		builder.append(scenario);
		builder.append("\n executionTime=");
		builder.append(executionTime);
		builder.append("\n executionTimeStdDev=");
		builder.append(executionTimeStdDev);
		builder.append("\n executionTimeMin=");
		builder.append(executionTimeMin);
		builder.append("\n executionTimeMax=");
		builder.append(executionTimeMax);
		builder.append("\n reductionRate=");
		builder.append(reductionRate);
		builder.append("\n reductionRateStdDev=");
		builder.append(reductionRateStdDev);
		builder.append("\n reductionRateMin=");
		builder.append(reductionRateMin);
		builder.append("\n reductionRateMax=");
		builder.append(reductionRateMax);
		builder.append("\n pairsCompleteness=");
		builder.append(pairsCompleteness);
		builder.append("\n pairsCompletenessStdDev=");
		builder.append(pairsCompletenessStdDev);
		builder.append("\n pairsCompletenessMin=");
		builder.append(pairsCompletenessMin);
		builder.append("\n pairsCompletenessMax=");
		builder.append(pairsCompletenessMax);
		builder.append("\n pairsQuality=");
		builder.append(pairsQuality);
		builder.append("\n pairsQualityStdDev=");
		builder.append(pairsQualityStdDev);
		builder.append("\n pairsQualityMin=");
		builder.append(pairsQualityMin);
		builder.append("\n pairsQualityMax=");
		builder.append(pairsQualityMax);
		builder.append("\n fMeasure=");
		builder.append(fMeasure);
		builder.append("\n fMeasureStdDev=");
		builder.append(fMeasureStdDev);
		builder.append("\n fMeasureMin=");
		builder.append(fMeasureMin);
		builder.append("\n fMeasureMax=");
		builder.append(fMeasureMax);
		builder.append("\n]");
		return builder.toString();
	}

	
	public void setScenario(Scenario scenario){
		this.scenario = scenario;
	}
	
	public Scenario getScenario(){
		return this.scenario;
	}

	public BigDecimal getExecutionTime() {
		return executionTime;
	}

	public void setExecutionTime(BigDecimal executionTime) {
		this.executionTime = executionTime;
	}

	public BigDecimal getExecutionTimeStdDev() {
		return executionTimeStdDev;
	}

	public void setExecutionTimeStdDev(BigDecimal executionTimeStdDev) {
		this.executionTimeStdDev = executionTimeStdDev;
	}

	public BigDecimal getExecutionTimeMin() {
		return executionTimeMin;
	}

	public void setExecutionTimeMin(BigDecimal executionTimeMin) {
		this.executionTimeMin = executionTimeMin;
	}

	public BigDecimal getExecutionTimeMax() {
		return executionTimeMax;
	}

	public void setExecutionTimeMax(BigDecimal executionTimeMax) {
		this.executionTimeMax = executionTimeMax;
	}

	public BigDecimal getReductionRate() {
		return reductionRate;
	}

	public void setReductionRate(BigDecimal reductionRate) {
		this.reductionRate = reductionRate;
	}

	public BigDecimal getReductionRateStdDev() {
		return reductionRateStdDev;
	}

	public void setReductionRateStdDev(BigDecimal reductionRateStdDev) {
		this.reductionRateStdDev = reductionRateStdDev;
	}

	public BigDecimal getReductionRateMin() {
		return reductionRateMin;
	}

	public void setReductionRateMin(BigDecimal reductionRateMin) {
		this.reductionRateMin = reductionRateMin;
	}

	public BigDecimal getReductionRateMax() {
		return reductionRateMax;
	}

	public void setReductionRateMax(BigDecimal reductionRateMax) {
		this.reductionRateMax = reductionRateMax;
	}

	public BigDecimal getPairsCompleteness() {
		return pairsCompleteness;
	}

	public void setPairsCompleteness(BigDecimal pairsCompleteness) {
		this.pairsCompleteness = pairsCompleteness;
	}

	public BigDecimal getPairsCompletenessStdDev() {
		return pairsCompletenessStdDev;
	}

	public void setPairsCompletenessStdDev(BigDecimal pairsCompletenessStdDev) {
		this.pairsCompletenessStdDev = pairsCompletenessStdDev;
	}

	public BigDecimal getPairsCompletenessMin() {
		return pairsCompletenessMin;
	}

	public void setPairsCompletenessMin(BigDecimal pairsCompletenessMin) {
		this.pairsCompletenessMin = pairsCompletenessMin;
	}

	public BigDecimal getPairsCompletenessMax() {
		return pairsCompletenessMax;
	}

	public void setPairsCompletenessMax(BigDecimal pairsCompletenessMax) {
		this.pairsCompletenessMax = pairsCompletenessMax;
	}

	public BigDecimal getPairsQuality() {
		return pairsQuality;
	}

	public void setPairsQuality(BigDecimal pairsQuality) {
		this.pairsQuality = pairsQuality;
	}

	public BigDecimal getPairsQualityStdDev() {
		return pairsQualityStdDev;
	}

	public void setPairsQualityStdDev(BigDecimal pairsQualityStdDev) {
		this.pairsQualityStdDev = pairsQualityStdDev;
	}

	public BigDecimal getPairsQualityMin() {
		return pairsQualityMin;
	}

	public void setPairsQualityMin(BigDecimal pairsQualityMin) {
		this.pairsQualityMin = pairsQualityMin;
	}

	public BigDecimal getPairsQualityMax() {
		return pairsQualityMax;
	}

	public void setPairsQualityMax(BigDecimal pairsQualityMax) {
		this.pairsQualityMax = pairsQualityMax;
	}

	public BigDecimal getfMeasure() {
		return fMeasure;
	}

	public void setfMeasure(BigDecimal fMeasure) {
		this.fMeasure = fMeasure;
	}

	public BigDecimal getfMeasureStdDev() {
		return fMeasureStdDev;
	}

	public void setfMeasureStdDev(BigDecimal fMeasureStdDev) {
		this.fMeasureStdDev = fMeasureStdDev;
	}

	public BigDecimal getfMeasureMin() {
		return fMeasureMin;
	}

	public void setfMeasureMin(BigDecimal fMeasureMin) {
		this.fMeasureMin = fMeasureMin;
	}

	public BigDecimal getfMeasureMax() {
		return fMeasureMax;
	}
	
	public void setfMeasureMax(BigDecimal fMeasureMax) {
		this.fMeasureMax = fMeasureMax;
	}
}	