package dbs.bigdata.flink.pprl.evaluation.rest;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class that represents the list of completed jobs
 * provided by the flink rest monitor.
 * 
 * @author mfranke
 */
public class JobResponse {

	@JsonProperty("jobs")
	private List<Job> jobs;
	
	public JobResponse(){}

	public List<Job> getJobs() {
		return jobs;
	}

	/**
	 * @return only the successfully finished jobs.
	 */
	public List<Job> getFinishedJobs(){
		List<Job> result = new ArrayList<Job>();
		
		jobs.forEach((job) -> {
			if (job.hasFinished()){
				result.add(job);
			}
		});
		
		return result;
	}
	
	/**
	 * @return only the failed jobs.
	 */
	public List<Job> getFailedJobs(){
		List<Job> result = new ArrayList<Job>();
		
		jobs.forEach((job) -> {
			if (job.hasFailed()){
				result.add(job);
			}
		});
		
		return result;
	}
	
	public void setJobs(List<Job> jobs) {
		this.jobs = jobs;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("JobResponse [");
		for (Job job : jobs){
			builder.append("\n ");
			builder.append(job);
		}
		builder.append("\n" + "]");
		return builder.toString();
	}
}