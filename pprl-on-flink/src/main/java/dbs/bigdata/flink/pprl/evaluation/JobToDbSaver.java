package dbs.bigdata.flink.pprl.evaluation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import dbs.bigdata.flink.pprl.evaluation.rest.FlinkRestMonitorConnector;

/**
 * Class that loads the execution results via the {@link FlinkRestMonitorConnector}
 * and stores them into a database utilizing the {@link DbConnector}.
 * 
 * @author mfranke
 */
@SuppressWarnings("deprecation")
public class JobToDbSaver {
	
	private static final Logger log = LoggerFactory.getLogger(JobToDbSaver.class);
	
	private static final String PARAM_URL = "url";
	private static final String PARAM_JOB_ID = "jobId";

	private static final String PARAM_PROXY = "proxy";
	private static final String PARAM_PROXY_URL = "proxyUrl";
	private static final String PARAM_PROXY_PORT = "proxyPort";
	
	private final FlinkRestMonitorConnector restConnector;
	private final String jobId;
	
	/**
	 * Creates a new {@link JobToDbSaver}.
	 * @param url the url of the flink rest interface.
	 * @throws MalformedURLException
	 */
	public JobToDbSaver(String url) throws MalformedURLException{
		this(url, null);
	}
	
	/**
	 * Creates a new {@link JobToDbSaver}.
	 * @param url the url of the flink rest interface.
	 * @param jobId the identifier of the job that should be inserted into the database.
	 * @throws MalformedURLException
	 */
	public JobToDbSaver(String url, String jobId) throws MalformedURLException{
		this.restConnector = new FlinkRestMonitorConnector(url);
		this.jobId = jobId;
	}
	
	/**
	 * Creates a new {@link JobToDbSaver}.
	 * @param url the url of the flink rest interface.
	 * @param proxy defines if proxy if necessary to reach the url.
	 * @param jobId the identifier of the job that should be inserted into the database.
	 * @throws MalformedURLException
	 */
	public JobToDbSaver(String url, Proxy proxy, String jobId) throws MalformedURLException{
		this.restConnector = new FlinkRestMonitorConnector(url);
		this.restConnector.setProxy(proxy);
		this.jobId = jobId;
	}
	
	
	/**
	 * Convenience method to save either one job if a jobId was specified
	 * or to save all job results coming from the rest interface.
	 *  
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public void save() throws JsonParseException, JsonMappingException, IOException{
		if (this.jobId != null){
			this.saveJobMetricsInDb(this.jobId);
		}
		else{
			this.saveAllJobMetricsInDb();
		}
	}
	
	/**
	 * Saves all job results coming from the rest interface.
	 * 
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public void saveAllJobMetricsInDb() throws JsonParseException, JsonMappingException, IOException{	 
		final List<ExecutedJobMetric> jobMetrics = this.restConnector.getExecutedJobMetrics();
		log.info("Received " + jobMetrics.size() + " executed jobs");
		final DbConnector dbConnector = new DbConnector();
		dbConnector.insertMetrics(jobMetrics);
		dbConnector.close();
	}
	
	/**
	 * Retrieve the job results for the specified job and save them into the database.
	 * @param jobId
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	public void saveJobMetricsInDb(String jobId) throws JsonMappingException, IOException{
		final ExecutedJobMetric job = this.restConnector.getExecutedJobMetric(jobId);
		
		final DbConnector dbConnector = new DbConnector();
		dbConnector.insertMetric(job);
		dbConnector.close();
	}
	
	private static Options makeOptions(){
		Options options = new Options();
		options.addOption(new Option("u", PARAM_URL, true, ""));
		options.addOption(new Option("j", PARAM_JOB_ID, true, ""));
		options.addOption(new Option("p", PARAM_PROXY, true, ""));
		options.addOption(new Option("pu", PARAM_PROXY_URL, true, ""));
		options.addOption(new Option("pp", PARAM_PROXY_PORT, true, ""));
		
		return options;
	}
	
	private static JobToDbSaver parseOptions(String[] args) throws ParseException, MalformedURLException{
		CommandLineParser parser = new BasicParser();	
		CommandLine cmd = parser.parse(makeOptions(), args);

		final String url = cmd.getOptionValue(PARAM_URL);
		final String jobId = cmd.getOptionValue(PARAM_JOB_ID);
		final String proxyType = cmd.getOptionValue(PARAM_PROXY);
		final String proxyUrl = cmd.getOptionValue(PARAM_PROXY_URL);
		final int proxyPort = Integer.parseInt(cmd.getOptionValue(PARAM_PROXY_PORT, "-1"));
		
		if (proxyType != null && !proxyType.isEmpty()){
			Proxy.Type type = resolveProxyType(proxyType);
			Proxy proxy = new Proxy(type, new InetSocketAddress(proxyUrl, proxyPort));
			return new JobToDbSaver(url, proxy, jobId);
		}
		else{
			return new JobToDbSaver(url, jobId);
		}
	}
	
	private static Proxy.Type resolveProxyType(String proxy){
		return Proxy.Type.valueOf(proxy);
	}
	
	
	public static void main (String[] args) throws JsonParseException, JsonMappingException, IOException, ParseException{
		System.out.println("Started JobToDbSaver");
		JobToDbSaver saver = parseOptions(args);
		saver.save();
	}
}