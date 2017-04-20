package dbs.bigdata.flink.pprl.evaluation.rest;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Wrapper class to extract the {@link JobConfig} from the 
 * flink rest monitor json result.
 * 
 * @author mfranke
 */
@JsonIgnoreProperties(ignoreUnknown=true)
public class JobConfigResponse {

	private JobConfig jobConfig;
	
	public JobConfigResponse(){}
	
	@JsonProperty("execution-config")
	public void setJobConfig(Map<String, Object> userConfig) {
		ObjectMapper objectMapper = new ObjectMapper();
		this.jobConfig = objectMapper.convertValue(userConfig.get("user-config"), JobConfig.class);
	}
	
	public JobConfig getJobConfig(){
		return this.jobConfig;
	}
	
	@Override
	public String toString() {
		return this.jobConfig.toString();
	}	
}