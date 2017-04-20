package dbs.bigdata.flink.pprl.evaluation;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Set of (time) metrics for a specific job and a certain number of records
 * and degree of parallelism (intrinsic also related to a data set).
 * The metric here is the execution time and the standard deviation of it.
 * 
 * @author mfranke
 *
 */
public class MethodRuntimeMetrics {

	private static final int ATTRIBUTE_COUNT = 5;
	
	private String jobName;
	private BigInteger numberOfRecords;
	private BigDecimal executionTime;
	private BigDecimal executionTimeStdDev;
	private Integer parallelism;
	
	public MethodRuntimeMetrics(){}
	
	public MethodRuntimeMetrics(String jobName, BigInteger numberOfRecords, BigDecimal executionTime,
			BigDecimal executionTimeStdDev, Integer parallelism) {
		this.jobName = jobName;
		this.numberOfRecords = numberOfRecords;
		this.executionTime = executionTime;
		this.executionTimeStdDev = executionTimeStdDev;
		this.parallelism = parallelism;
	}
	
	public static MethodRuntimeMetrics from(Object[] object){
		if (object.length != ATTRIBUTE_COUNT){
			return null;
		}
		else{
			MethodRuntimeMetrics result = new MethodRuntimeMetrics();
			
			result.setJobName((String) object[0]);
			result.setNumberOfRecords((BigInteger) object[1]);
			result.setExecutionTime((BigDecimal) object[2]);
			result.setExecutionTimeStdDev((BigDecimal) object[3]);
			result.setParallelism((Integer) object[4]);
			
			return result;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MethodRuntimeMetrics [");
		builder.append("\n jobName=");
		builder.append(jobName);
		builder.append("\n numberOfRecords=");
		builder.append(numberOfRecords);
		builder.append("\n executionTime=");
		builder.append(executionTime);
		builder.append("\n executionTimeStdDev=");
		builder.append(executionTimeStdDev);
		builder.append("\n parallelism=");
		builder.append(parallelism);
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

	public Integer getParallelism() {
		return parallelism;
	}

	public void setParallelism(Integer parallelism) {
		this.parallelism = parallelism;
	}
}