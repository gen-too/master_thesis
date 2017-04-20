package dbs.bigdata.flink.pprl.evaluation.rest;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class containing all exposed flink accumulator results.
 * 
 * @author mfranke
 */
@JsonIgnoreProperties({"job-accumulators", "tasks"})
public class JobAccumulatorResponse {

	@JsonProperty("user-task-accumulators")
	private List<JobAccumulator> jobAccumulators;
	
	public JobAccumulatorResponse(){}

	public List<JobAccumulator> getJobAccumulators() {
		return jobAccumulators;
	}

	public void setJobAccumulators(List<JobAccumulator> jobAccumulator) {
		this.jobAccumulators = jobAccumulator;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("JobAccumulatorResponse [");
		for (JobAccumulator jobAccu : jobAccumulators){
			builder.append("\n ");
			builder.append( jobAccu);
		}
		builder.append("\n" + "]");
		return builder.toString();
	}
	
	
}