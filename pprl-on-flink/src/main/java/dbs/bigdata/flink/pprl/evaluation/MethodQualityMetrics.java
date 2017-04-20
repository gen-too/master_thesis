package dbs.bigdata.flink.pprl.evaluation;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Set of (quality) metrics for a specific job and a certain number of records
 * (intrinsic also related to a data set and a degree of parallelism).
 * The metrics are reduction rate, pairs completeness, pairs quality and
 * their respective standard deviation.
 * 
 * @author mfranke
 *
 */
public class MethodQualityMetrics {

	private static final int ATTRIBUTE_COUNT = 10;
	
	private String jobName;
	private BigInteger numberOfRecords;
	private BigDecimal reductionRate;
	private BigDecimal reductionRateStdDev;
	private BigDecimal pairsCompleteness;
	private BigDecimal pairsCompletenessStdDev;
	private BigDecimal pairsQuality;
	private BigDecimal pairsQualityStdDev;
	private BigDecimal fMeasure;
	private BigDecimal fMeasureStdDev;
	
	public MethodQualityMetrics(){}
	
	public MethodQualityMetrics(String jobName, BigInteger numberOfRecords, BigDecimal reductionRate,
			BigDecimal reductionRateStdDev, BigDecimal pairsCompleteness, BigDecimal pairsCompletenessStdDev,
			BigDecimal pairsQuality, BigDecimal pairsQualityStdDev, BigDecimal fMeasure, BigDecimal fMeasureStdDev) {
		this.jobName = jobName;
		this.numberOfRecords = numberOfRecords;
		this.reductionRate = reductionRate;
		this.reductionRateStdDev = reductionRateStdDev;
		this.pairsCompleteness = pairsCompleteness;
		this.pairsCompletenessStdDev = pairsCompletenessStdDev;
		this.pairsQuality = pairsQuality;
		this.pairsQualityStdDev = pairsQualityStdDev;
		this.fMeasure = fMeasure;
		this.fMeasureStdDev = fMeasureStdDev;
	}

	public static MethodQualityMetrics from(Object[] object){
		if (object.length != ATTRIBUTE_COUNT){
			return null;
		}
		else{
			MethodQualityMetrics result = new MethodQualityMetrics();
			
			result.setJobName((String) object[0]);
			result.setNumberOfRecords((BigInteger) object[1]);
			result.setReductionRate((BigDecimal) object[2]);
			result.setReductionRateStdDev((BigDecimal) object[3]);
			result.setPairsCompleteness((BigDecimal) object[4]);
			result.setPairsCompletenessStdDev((BigDecimal) object[5]);
			result.setPairsQuality((BigDecimal) object[6]);
			result.setPairsQualityStdDev((BigDecimal) object[7]);
			result.setfMeasure((BigDecimal) object[8]);
			result.setfMeasureStdDev((BigDecimal) object[9]);
			
			return result;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MethodQualityMetrics [");
		builder.append("\n jobName=");
		builder.append(jobName);
		builder.append("\n numberOfRecords=");
		builder.append(numberOfRecords);
		builder.append("\n reductionRate=");
		builder.append(reductionRate);
		builder.append("\n reductionRateStdDev=");
		builder.append(reductionRateStdDev);
		builder.append("\n pairsCompleteness=");
		builder.append(pairsCompleteness);
		builder.append("\n pairsCompletenessStdDev=");
		builder.append(pairsCompletenessStdDev);
		builder.append("\n pairsQuality=");
		builder.append(pairsQuality);
		builder.append("\n pairsQualityStdDev=");
		builder.append(pairsQualityStdDev);
		builder.append("\n fMeasure=");
		builder.append(fMeasure);
		builder.append("\n fMeasureStdDev=");
		builder.append(fMeasureStdDev);
		builder.append("\n]");
		return builder.toString();
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public BigInteger getNumberOfRecords() {
		return numberOfRecords;
	}
	
	public String getNumberOfRecordsAsString() {
		return String.format("%,d", numberOfRecords).replace(',', '.');
	}

	public void setNumberOfRecords(BigInteger numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
	}

	public BigDecimal getReductionRate() {
		return reductionRate;
	}

	public void setReductionRate(BigDecimal reductionRate) {
		this.reductionRate = reductionRate;
	}

	public BigDecimal getReductionRateStdDev() {
		if (this.reductionRateStdDev == null){
			this.reductionRateStdDev = new BigDecimal(0);
		}
		return reductionRateStdDev;
	}

	public void setReductionRateStdDev(BigDecimal reductionRateStdDev) {
		this.reductionRateStdDev = reductionRateStdDev;
	}

	public BigDecimal getPairsCompleteness() {
		return pairsCompleteness;
	}

	public void setPairsCompleteness(BigDecimal pairsCompleteness) {
		this.pairsCompleteness = pairsCompleteness;
	}

	public BigDecimal getPairsCompletenessStdDev() {
		if (this.pairsCompletenessStdDev == null){
			this.pairsCompletenessStdDev = new BigDecimal(0);
		}
		return pairsCompletenessStdDev;
	}

	public void setPairsCompletenessStdDev(BigDecimal pairsCompletenessStdDev) {
		this.pairsCompletenessStdDev = pairsCompletenessStdDev;
	}

	public BigDecimal getPairsQuality() {
		return pairsQuality;
	}

	public void setPairsQuality(BigDecimal pairsQuality) {
		this.pairsQuality = pairsQuality;
	}

	public BigDecimal getPairsQualityStdDev() {
		if (this.pairsQualityStdDev == null){
			this.pairsQualityStdDev = new BigDecimal(0);
		}
		return pairsQualityStdDev;
	}

	public void setPairsQualityStdDev(BigDecimal pairsQualityStdDev) {
		this.pairsQualityStdDev = pairsQualityStdDev;
	}

	public BigDecimal getfMeasure() {
		return fMeasure;
	}

	public void setfMeasure(BigDecimal fMeasure) {
		this.fMeasure = fMeasure;
	}

	public BigDecimal getfMeasureStdDev() {
		if (this.fMeasureStdDev == null){
			this.fMeasureStdDev = new BigDecimal(0);
		}
		return fMeasureStdDev;
	}

	public void setfMeasureStdDev(BigDecimal fMeasureStdDev) {
		this.fMeasureStdDev = fMeasureStdDev;
	}
}