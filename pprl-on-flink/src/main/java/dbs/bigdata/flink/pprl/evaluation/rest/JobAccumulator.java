package dbs.bigdata.flink.pprl.evaluation.rest;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class that represents a flink accumulator result.
 * 
 * @author mfranke
 */
public class JobAccumulator {

	@JsonProperty("name")
	private String name;
	
	@JsonProperty("type")
	private String type;
	
	@JsonProperty("value")
	private long value;
	
	
	public JobAccumulator(){}

	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("JobAccumulator [name=");
		builder.append(name);
		builder.append(", type=");
		builder.append(type);
		builder.append(", value=");
		builder.append(value);
		builder.append("]");
		return builder.toString();
	}	
}