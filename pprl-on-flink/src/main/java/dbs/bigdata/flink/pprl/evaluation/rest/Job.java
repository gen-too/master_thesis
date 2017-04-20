package dbs.bigdata.flink.pprl.evaluation.rest;

/**
 * Class that provides basic information of a executed job
 * like its id, name, state, start/end time and duration.
 */
import java.sql.Timestamp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown=true)
public class Job {

	private static final String FINISHED_FLAG = "FINISHED";
	private static final String FAILED_FLAG = "FAILED";
	
	@JsonProperty("jid")
	private String id;
	
	@JsonProperty("name")
	private String name;
	
	@JsonProperty("state")
	private String state;
	
	@JsonProperty("start-time")
	private Timestamp startTime;
	
	@JsonProperty("end-time")
	private Timestamp endTime;
	
	@JsonProperty("duration")
	private Long executionTime;
	
	
	public Job(){}

	public boolean hasFinished(){
		return this.state.equalsIgnoreCase(FINISHED_FLAG);
	}
	
	public boolean hasFailed(){
		return this.state.equalsIgnoreCase(FAILED_FLAG);
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public Timestamp getStartTime() {
		return startTime;
	}

	public void setStartTime(Timestamp startTime) {
		this.startTime = startTime;
	}

	public Timestamp getEndTime() {
		return endTime;
	}
	
	public void setEndTime(Timestamp endTime) {
		this.endTime = endTime;
	}

	public Long getExecutionTime() {
		return executionTime;
	}

	public void setExecutionTime(Long executionTime) {
		this.executionTime = executionTime;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Job [");
		builder.append("\n id=");
		builder.append(id);
		builder.append("\n name=");
		builder.append(name);
		builder.append("\n state=");
		builder.append(state);
		builder.append("\n startTime=");
		builder.append(startTime);
		builder.append("\n endTime=");
		builder.append(endTime);
		builder.append("\n executionTime=");
		builder.append(executionTime);
		builder.append("\n]");
		return builder.toString();
	}
}