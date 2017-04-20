package dbs.bigdata.flink.pprl.evaluation.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dbs.bigdata.flink.pprl.evaluation.ExecutedJobMetric;

/**
 * Class that provides access to the flink rest monitor.
 * 
 * @author mfranke
 */
public class FlinkRestMonitorConnector {
	private static final Logger log = LoggerFactory.getLogger(FlinkRestMonitorConnector.class);
	
	private static final String COMPLETED_JOBS_PATH = "joboverview/completed";
	private static final String JOBS_PATH = "jobs/";
	private static final String ACCUMULATOR_PATH= "/accumulators";
	private static final String CONFIG_PATH = "/config";
	
	private final URL url;
	private Proxy proxy;
	
	/**
	 * Creates a new connector.
	 * 
	 * @param url the url of the flink rest interface.
	 * @throws MalformedURLException
	 */
	public FlinkRestMonitorConnector(String url) throws MalformedURLException{
		this.url = new URL(url);
		this.proxy = null;
	}
	
	/**
	 * Retrieves basic job information, the job configuration and the job accumulators for all completed jobs
	 * and combines all this information into {@link ExecutedJobMetric}s.
	 * 
	 * @return list of metrics, see {@link ExecutedJobMetric};
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public List<ExecutedJobMetric> getExecutedJobMetrics() throws JsonParseException, JsonMappingException, IOException{
		JobResponse jobs = this.getJobs();		
		if (jobs != null){
			log.info("retrieved (" + jobs.getJobs().size() + ") jobs: " + jobs);
			List<ExecutedJobMetric> result = new ArrayList<ExecutedJobMetric>();
			
			for (Job job : jobs.getFinishedJobs()){
				final String jobId = job.getId();
				log.info("process job with id: " + jobId);
				JobAccumulatorResponse jobAccumulators = this.getJobAccumulators(jobId);
				JobConfigResponse jobConfig = this.getJobConfiguration(jobId);
							
				ExecutedJobMetric jobMetric = 
					new ExecutedJobMetric(
							job, 
							jobAccumulators.getJobAccumulators(), 
							jobConfig.getJobConfig()
					);
		
				result.add(jobMetric);
			}
			
			return result;
		}
		else{
			return null;
		}
	}
	
	/**
	 * Retrieves basic job information, the job configuration and the job accumulators for the job
	 * related to the given job id and combines all this information.
	 * @param jobId the identifier of the job
	 * @return all information related to the job exposed by the flink rest monitor within a {@link ExecutedJobMetric}.
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public ExecutedJobMetric getExecutedJobMetric(String jobId) throws JsonMappingException, IOException{
		Job job = this.getJob(jobId);
		JobAccumulatorResponse jobAccumulators = this.getJobAccumulators(jobId);
		JobConfigResponse jobConfig = this.getJobConfiguration(jobId);
		
		ExecutedJobMetric jobMetric = new ExecutedJobMetric(job, jobAccumulators.getJobAccumulators(), jobConfig.getJobConfig());
		return jobMetric;
	}
	
	/**
	 * Retrieves the configuration of a executed job by the id of the job.
	 * @param jobId the identifier of the job.
	 * @return the job configuration within a {@link JobConfigResponse}.
	 * @throws IOException
	 */
	public JobConfigResponse getJobConfiguration(String jobId) throws IOException{
		final String path = JOBS_PATH + jobId + CONFIG_PATH;
		
		final String json = this.getRequest(path);
		
		if (json != null && !json.isEmpty()){	
			ObjectMapper mapper = new ObjectMapper();
	
			return mapper.readValue(json, JobConfigResponse.class);
		}
		else{
			return null;
		}
	}
	
	/**
	 * Retrieves the exposed accumulators of a executed job by the id of the job.
	 * @param jobId the identifier of the job.
	 * @return the list of exposed accumulators, see {@link JobAccumulatorResponse}.
	 * @throws IOException
	 */
	public JobAccumulatorResponse getJobAccumulators(String jobId) throws IOException{
		final String path = JOBS_PATH + jobId + ACCUMULATOR_PATH;
		
		final String json = this.getRequest(path);

		if (json != null && !json.isEmpty()){	
			ObjectMapper mapper = new ObjectMapper();
			
			return mapper.readValue(json, JobAccumulatorResponse.class);
		}
		else{
			return null;
		}
	}
	
	/**
	 * Retrieve information of all completed jobs.
	 * @return list of job information, see {@link Job}.
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public JobResponse getJobs() throws JsonMappingException, IOException{
		final String json = this.getRequest(COMPLETED_JOBS_PATH);
		
		if (json != null && !json.isEmpty()){
			ObjectMapper mapper = new ObjectMapper();
		
			return mapper.readValue(json, JobResponse.class);
		}
		else{
			return null;
		}
	}
	
	/**
	 * Get information to the job specified by the job id.
	 * @param jobId the identifier of the job.
	 * @return the job information, see {@link Job}.
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public Job getJob(String jobId) throws JsonMappingException, IOException{
		final String path = JOBS_PATH + jobId;
		final String json = this.getRequest(path);
		
		if (json != null && !json.isEmpty()){
			ObjectMapper mapper = new ObjectMapper();
		
			return mapper.readValue(json, Job.class);
		}
		else{
			return null;
		}
	}
	
	public void setProxy(String adress, int port, Proxy.Type type){
		this.proxy = new Proxy(type, new InetSocketAddress(adress, port));
	}
	
	public void setProxy(Proxy proxy){
		this.proxy = proxy;
	}
	
	private String getRequest(String path) throws IOException{
		final URL requestUrl = new URL(this.url + "/" + path);
					
		HttpURLConnection con = (proxy == null) ? 
				(HttpURLConnection) requestUrl.openConnection() : (HttpURLConnection) requestUrl.openConnection(this.proxy);
		
		con.setRequestMethod("GET");
		con.setRequestProperty("Accept", "application/json");
		con.connect();
				
		InputStream stream = con.getInputStream();
		
		BufferedReader in = new BufferedReader(new InputStreamReader(stream));
		
		String inputLine;
		StringBuffer jsonResult = new StringBuffer();
		
		while ((inputLine = in.readLine()) != null){
			jsonResult.append(inputLine);
		}
		
		in.close();
		
		con.disconnect();
		return jsonResult.toString();
	}
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws JsonMappingException, IOException{
		final String local = "http://localhost:8081";
		final String cluster = "";
		FlinkRestMonitorConnector rest = new FlinkRestMonitorConnector(cluster);
		//rest.setProxy("127.0.0.1", 1080, Proxy.Type.SOCKS);
		System.out.println(rest.getJobs());
	}
}
