package dbs.bigdata.flink.pprl.evaluation.rest;

import java.util.Arrays;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class representing the information carried within the flink
 * configuration of the job.
 * 
 * @author mfranke
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class JobConfig {

	@JsonProperty("pprlJob")
	private String jobType;
	
	@JsonProperty("comparisonThreshold")
	private double comparisonThreshold;
	
	@JsonProperty("parallelism")
	private int parallelism;
	
	@JsonProperty("numberOfMatches")
	private long numberOfMatches;
	
	@JsonProperty("dataFilePaths")
	private String[] dataFilePaths;

	@JsonProperty("numberOfHashesPerFamily")
	private Integer numberOfHashesPerFamily;
	
	@JsonProperty("numberOfHashFamilies")
	private Integer numberOfHashFamilies;
	
	@JsonProperty("dataDescription")
	private String dataDescription;
	
	@JsonProperty("iterations")
	private int iterations;
	
	
	public JobConfig(){}
	
	
	public String getJobType() {
		return jobType;
	}

	public void setJobType(String jobType) {
		this.jobType = jobType;
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

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public long getNumberOfMatches() {
		return numberOfMatches;
	}

	public void setNumberOfMatches(long numberOfMatches) {
		this.numberOfMatches = numberOfMatches;
	}

	public String[] getDataFilePaths() {
		return dataFilePaths;
	}

	public void setDataFilePaths(String dataFilePaths) {
		this.dataFilePaths = dataFilePaths.split("#");
	}
	
	public Integer getNumberOfHashesPerFamily() {
		return numberOfHashesPerFamily;
	}

	public void setNumberOfHashesPerFamily(Integer numberOfHashesPerFamily) {
		this.numberOfHashesPerFamily = numberOfHashesPerFamily;
	}

	public Integer getNumberOfHashFamilies() {
		return numberOfHashFamilies;
	}

	public void setNumberOfHashFamilies(Integer numberOfHashFamilies) {
		this.numberOfHashFamilies = numberOfHashFamilies;
	}
	
	public String getDataDescription() {
		return dataDescription;
	}

	public void setDataDescription(String dataDescription) {
		this.dataDescription = dataDescription;
	}

	public int getIterations() {
		return iterations;
	}

	public void setIterations(int iterations) {
		this.iterations = iterations;
	}


	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("JobConfig [");
		builder.append("\n jobName=");
		builder.append(jobType);
		builder.append("\n comparisonThreshold=");
		builder.append(comparisonThreshold);
		builder.append("\n dataDescription=");
		builder.append(dataDescription);
		builder.append("\n parallelism=");
		builder.append(parallelism);
		builder.append("\n numberOfMatches=");
		builder.append(numberOfMatches);
		builder.append("\n dataFilePaths=");
		builder.append(Arrays.toString(dataFilePaths));
		builder.append("\n numberOfHashesPerFamily=");
		builder.append(numberOfHashesPerFamily);
		builder.append("\n numberOfHashFamilies=");
		builder.append(numberOfHashFamilies);
		builder.append("\n iterations=");
		builder.append(iterations);
		builder.append("\n" + "]");
		return builder.toString();
	}
}