package dbs.bigdata.flink.pprl.job.common;

import java.util.Arrays;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import dbs.bigdata.flink.pprl.preprocessing.Person;
import dbs.bigdata.flink.pprl.utils.common.HashUtils;

/**
 * Base class for implementing a pprl job.
 * Contains the basic input parameters of a pprl job
 * running with flink.
 * 
 * @author mfranke
 *
 */
public abstract class PpprlJob{

	protected final ExecutionEnvironment env;
	protected String jobName;
	protected String[] dataFilePaths;
	protected String lineDelimiter;
	protected String fieldDelimiter;
	protected String includingFields;
	protected String[] fieldNames;
	protected double comparisonThreshold;
	protected final int parallelism;
	protected String outputPath;
		
	/**
	 * Creates a new pprl job.
	 * 
	 * @param config the {@link Configuration} of the job. 
	 * 		  Here the configuration is used to carry some details about the pprl job,
	 * 		  which are used later for evaluation.
	 */
	public PpprlJob(Configuration config){
		this(config, null, null, null, null, null, null, 0d, null);
	}
	
	/**
	 * Creates a new pprl job.
	 * 
	 * @param config the {@link Configuration} of the job. 
	 * 		  Here the configuration is used to carry some details about the pprl job,
	 * 		  which are used later for evaluation.
	 * @param jobName the name of the pprl job.
	 * @param dataFilePaths the path to the data files.
	 * @param lineDelimiter the line delimiter that separate different records in the data files.
	 * @param fieldDelimiter the field delimiter that separate different record attributs in the data files.
	 * @param includingFields String value in the form of "000101010" where 0/1 means (not) include the related attribute.
	 * @param fieldNames the {@link Person} field names to which the attributes should be mapped.
	 * @param comparisonThreshold the threshold value used for classification of matches and non-matches.
	 */
	public PpprlJob(Configuration config, String jobName, String[] dataFilePaths, String lineDelimiter, String fieldDelimiter, String includingFields,
			String[] fieldNames, double comparisonThreshold, String outputPath) {
		// set up the execution environment
		this.env =  ExecutionEnvironment.getExecutionEnvironment();

		this.setJobName(jobName);
		
		int parallelism = config.getInteger("parallelism", this.env.getParallelism());
		if (parallelism == -1){
			parallelism = this.env.getParallelism();
			config.setInteger("parallelism", parallelism);	
		}
		
		this.parallelism = parallelism;
		this.env.setParallelism(parallelism);
		
		this.env.getConfig().setGlobalJobParameters(config);
		
		this.dataFilePaths = dataFilePaths;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
		this.includingFields = includingFields;
		this.fieldNames = fieldNames;
		this.comparisonThreshold = comparisonThreshold;
		this.outputPath = outputPath;
	}
		
	/**
	 * Executes the pprl job.
	 * 
	 * @param iteration the iteration round of the job.
	 * @return a {@link JobExecutionResult} containing some execution metrics.
	 * @throws Exception
	 */
	public abstract JobExecutionResult runJob(int iteration) throws Exception;

	
	public ExecutionEnvironment getEnv() {
		return env;
	}
	
	public String getJobType(){
		return this.getClass().getSimpleName();
	}
	
	public String getJobName(){
		return this.jobName;
	}
	
	public void setJobName(String jobName){
		this.jobName = (jobName != null) ? jobName : this.getJobType();
	}

	protected String getDataFileHash(){
		return Long.toString(Math.abs(HashUtils.getSHALongHash(Arrays.toString(dataFilePaths))));
	}
	
	public String[] getDataFilePaths() {
		return dataFilePaths;
	}

	public void setDataFilePaths(String[] dataFilePaths) {
		this.dataFilePaths = dataFilePaths;
	}

	public String getLineDelimiter() {
		return lineDelimiter;
	}

	public void setLineDelimiter(String lineDelimiter) {
		this.lineDelimiter = lineDelimiter;
	}

	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}

	public String getIncludingFields() {
		return includingFields;
	}

	public void setIncludingFields(String includingFields) {
		this.includingFields = includingFields;
	}
	
	public String[] getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	public double getComparisonThreshold() {
		return comparisonThreshold;
	}

	public void setComparisonThreshold(double comparisonThreshold) {
		this.comparisonThreshold = comparisonThreshold;
	}

	public int getParallelism() {
		return parallelism;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}
}