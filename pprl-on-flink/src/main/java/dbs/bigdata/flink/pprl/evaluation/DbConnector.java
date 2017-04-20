package dbs.bigdata.flink.pprl.evaluation;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.persistence.PersistenceException;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.query.Query;
import org.hibernate.type.IntegerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dbs.bigdata.flink.pprl.utils.common.FileUtils;
import dbs.bigdata.flink.pprl.utils.common.Unused;

/**
 * Class that provides access to the database that stores the
 * results of the executed flink pprl jobs.
 * 
 * @author mfranke
 *
 */
public class DbConnector {

	private static final Logger log = LoggerFactory.getLogger(DbConnector.class);
	
	private static final String JDBC_DRIVER = "org.postgresql.Driver";
	private static final String HIBERNATE_DIALECT = "org.hibernate.dialect.ProgressDialect";
	private static final String HOST = "localhost";
	private static final String PORT = "5432";
	private static final String DATABASE_NAME = "master-thesis";	
	private static final String URL = "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DATABASE_NAME;
	private static final String USERNAME = "";
	private static final String PASSWORD = "";
	private static final int BATCH_SIZE = 25;
	
	private final SessionFactory sessionFactory;
		
	/**
	 * Creates a new database connector object.
	 * This constructor will connect to the database with the
	 * specified driver, url, user and password.
	 * It will also executes the sql scripts in the
	 * src/main/resources folder which are intend to use for
	 * creation of tables and views.
	 */
	public DbConnector(){
		Properties properties = new Properties();
	    properties.setProperty("hibernate.connection.driver_class", JDBC_DRIVER);
	    properties.setProperty("hibernate.connection.url", URL);
	    properties.setProperty("hibernate.connection.username", USERNAME);
	    properties.setProperty("hibernate.connection.password", PASSWORD);
	    properties.setProperty("hibernate.dialect", HIBERNATE_DIALECT);
//	    properties.setProperty("hibernate.hbm2ddl.auto", "update");
	    properties.setProperty("hibernate.jdbc.batch_size", "20");

	    Configuration config = new Configuration();
	    config.addAnnotatedClass(ExecutedJobMetric.class);
	    config.addAnnotatedClass(AggregatedJobMetricByScenario.class);
	    config.addAnnotatedClass(Scenario.class);
	    config.setProperties(properties);
	    
	    this.sessionFactory = config.buildSessionFactory();
	    this.executeSqlScripts();
	}
	
	private void executeSqlScripts(){
		File[] sqlScripts = FileUtils.getFilesByType("src/main/resources", ".sql");    
	    log.info("Sql scripts: " + Arrays.toString(sqlScripts));
	    
	    if (sqlScripts != null && sqlScripts.length > 0){
		    try {
				Session session = this.sessionFactory.openSession();
				session.beginTransaction();
			    
				for (File script : sqlScripts){
					final String sql = FileUtils.readFile(script);
					session.createNativeQuery(sql).executeUpdate();
				}
			    
			    session.getTransaction().commit();
			} 
		    catch (IOException e) {
		    	e.printStackTrace();
			}	    
	    }
	}
	
	private static boolean checkConstraintViolationException(PersistenceException e){
		Throwable cause = e;
		
		while(cause != null){
			if (cause instanceof ConstraintViolationException){
				return true;
			}
			else{
				cause = cause.getCause();
			}
		}
		return false;
	}
	
	public void close(){
		this.sessionFactory.close();
	}
	
	
	/** ========================= Refresh ========================= **/
	/**
	 * Manually refreshes the materialized view that aggregates the job results.
	 */
	public void refreshMaterializedView(){
		Session session = this.sessionFactory.openSession();
		this.refreshMaterializedView(session);
	}
	
	private void refreshMaterializedView(Session session){
		session.beginTransaction();
		session.getNamedNativeQuery("refreshAggregatedJobMetrics").executeUpdate();
		session.getTransaction().commit();
	}
	
	/** ========================= Insert ========================= **/
	/**
	 * Inserts the given {@link ExecutedJobMetric} in the database.
	 * 
	 * @param job the metrics of the executed job.
	 * @return the id of the newly inserted record or null if the insert fails.
	 */
	public Long insertMetric(ExecutedJobMetric job){
		Session session = this.sessionFactory.openSession();
		
		Long jobId = null;
		
		try{
			session.beginTransaction();
	    	jobId = (Long) session.save(job);
	    	session.getTransaction().commit();
	    	this.refreshMaterializedView(session);
		}
		catch(PersistenceException e){
			if(!checkConstraintViolationException(e)){
				e.printStackTrace();
			}
	    	if (session.getTransaction() != null && session.getTransaction().isActive()){
	    		session.getTransaction().rollback();
	    	}
		}
		finally{
			session.close();
		}
		
		return jobId;
	}
	
	/**
	 * Inserts multiple {@link ExecutedJobMetric}s in the database
	 * in batch mode.
	 * 
	 * @param jobs the metrics of the executed jobs.
	 */
	public void insertMetricsBatch(List<ExecutedJobMetric> jobs){
		Session session = this.sessionFactory.openSession();
		
		try{
			session.beginTransaction();
	    
			for (int i = 0; i < jobs.size(); i++){
				final ExecutedJobMetric job = jobs.get(i);
				session.save(job);
				
				if (i > 0 && i % BATCH_SIZE == 0){
					session.flush();
					session.clear();
				}
			}
			    	
			session.getTransaction().commit();
			this.refreshMaterializedView(session);
		}
		catch(PersistenceException e){
			if(!checkConstraintViolationException(e)){
				e.printStackTrace();
			}
	    	if (session.getTransaction() != null && session.getTransaction().isActive()){
	    		session.getTransaction().rollback();
	    	}
		}
		finally{
			session.close();
		}	
	}
	
	/**
	 * Inserts multiple {@link ExecutedJobMetric}s in the database.
	 * 
	 * @param jobs jobs the metrics of the executed jobs.
	 */
	public void insertMetrics(List<ExecutedJobMetric> jobs){
		Session session = this.sessionFactory.openSession();
		
		for (ExecutedJobMetric job : jobs){
			try{
				session.beginTransaction();
				session.save(job);
				session.getTransaction().commit();
			}
			catch(PersistenceException e){
				if(!checkConstraintViolationException(e)){
					e.printStackTrace();
				}
		    	if (session.getTransaction() != null && session.getTransaction().isActive()){
		    		session.getTransaction().rollback();
		    	}
			}
		}
		
		this.refreshMaterializedView(session);
	
		session.close();
	}
	
	/** ========================= SELECT Metrics ========================= **/
	/**
	 * Retrieve the metrics of a executed job by the given job id.
	 * 
	 * @param id the identification of the job.
	 * @return
	 */
	public ExecutedJobMetric getJobById(long id){
		Session session = this.sessionFactory.openSession();
		ExecutedJobMetric result = session.get(ExecutedJobMetric.class, id);
		
		session.close();
		
		return result;		
	}
	
	/**
	 * Retrieve all jobs by the same type.
	 * 
	 * @param type the type name.
	 * @return list of job metrics ({@link ExecutedJobMetric}).
	 */
	@SuppressWarnings("unchecked")
	public List<ExecutedJobMetric> getJobsByType(String type){
		Session session = this.sessionFactory.openSession();
		
		Query<ExecutedJobMetric> query = session
				.getNamedQuery("getJobsByType")
				.setParameter("type", type);
		
		List<ExecutedJobMetric> result = query.getResultList();
		
		session.close();
		
		return result;
	}
	
	/**
	 * Retrieve all jobs with that used the same data set.
	 * 
	 * @param dataDescription the identifier for the used data set.
	 * @return list of job metrics ({@link ExecutedJobMetric}).
	 */
	@SuppressWarnings("unchecked")
	public List<ExecutedJobMetric> getJobsByDataDescription(String dataDescription){
		Session session = this.sessionFactory.openSession();
		
		Query<ExecutedJobMetric> query = session
				.getNamedQuery("getJobsByDataDescription")
				.setParameter("dataDescription", dataDescription);
		
		List<ExecutedJobMetric> result = query.getResultList();
		
		session.close();
		
		return result;
	}
	
	/**
	 * Retrieve all jobs that are executed under the same conditions (i.e. scenario).
	 * 
	 * @param type the job type.
	 * @param jobName the job name.
	 * @param dataDescription the identifier for the used data set.
	 * @param parallelism the degree of parallelism.
	 * @param numberOfRecords the number of records.
	 * @param numberOfHashFamilies the number of hash families (may be null).
	 * @param numberOfHashesPerFamily the number of hashes per family (may be null).
	 * @return list of job metrics ({@link ExecutedJobMetric}).
	 */
	public List<ExecutedJobMetric> getJobsByScenario(String type, String jobName, String dataDescription, int parallelism, long numberOfRecords, 
			int numberOfHashFamilies, int numberOfHashesPerFamily){
		return 
			this.getJobsByScenario(
				new Scenario(type, jobName, dataDescription, parallelism, numberOfRecords, numberOfHashFamilies, numberOfHashesPerFamily)
			);
	}
	
	/**
	 * Retrieve all jobs that are executed under the same conditions (i.e. scenario).
	 * @param scenario the {@link Scenario} of the job.
	 * @return list of job metrics ({@link ExecutedJobMetric}).
	 */
	@SuppressWarnings("unchecked")
	public List<ExecutedJobMetric> getJobsByScenario(Scenario scenario){
		Session session = this.sessionFactory.openSession();
		
		Query<ExecutedJobMetric> query = session
	    		.getNamedQuery("getJobsByScenario")
	    		.setParameter("type", scenario.getJobType())
	    		.setParameter("name", scenario.getJobName())
	    		.setParameter("dataDescription", scenario.getDataDescription())
	    		.setParameter("parallelism", scenario.getParallelism())
				.setParameter("numberOfRecords", scenario.getNumberOfRecords())
				.setParameter("numberOfHashFamilies", scenario.getHashFamilies())
				.setParameter("numberOfHashesPerFamily", scenario.getHashFunctions());
		
		List<ExecutedJobMetric> result = query.getResultList();
		
		session.close();
		
		return result;
	}
	
	/**
	 * Retrieve all scenarios.
	 * @return list of {@link Scenario}s.
	 */
	@SuppressWarnings("unchecked")
	public List<Scenario> getDistinctScenarios(){
		Session session = this.sessionFactory.openSession();
		
		Query<Object[]> query = session.getNamedNativeQuery("getDistinctScenarios");
		List<Object[]> result = query.getResultList();
		
		session.close();
		
		List<Scenario> scenarios = new ArrayList<Scenario>();
		
		result.forEach((record) -> {			
			final String type = (String) record[0];
			final String name = (String) record[1];
			final String dataDescr = (String) record[2];
			final int parallelism = (int) record[3];
			final BigInteger records = (BigInteger) record[4];
			final Integer hashFamilies = (Integer) record[5];
			final Integer hashFunctions = (Integer) record[6];
			
			final Scenario scenario = new Scenario(type, name, dataDescr, parallelism, records.longValue(), hashFamilies, hashFunctions);
			
			scenarios.add(scenario);
		});
		
		return scenarios;
	}
	
	/** ====================== SELECT Aggregated Metrics ====================== **/
	/**
	 * Retrieves all aggregated metrics stored in the related materialized view.
	 * 
	 * @return a list of {@link AggregatedJobMetricByScenario}.
	 */
	@SuppressWarnings("unchecked")
	public List<AggregatedJobMetricByScenario> getAllAggregatedMetrics(){
		Session session = this.sessionFactory.openSession();
		
		Query<AggregatedJobMetricByScenario> query = session
				.getNamedNativeQuery("getAllAggregatedJobMetrics");
		
		List<AggregatedJobMetricByScenario> result = query.getResultList();
		
		session.close();
		
		return result;
	}
	
	
	/**
	 * Retrieves the aggregated metrics for a specific scenario.
	 * 
	 * @param jobName the name of the job.
	 * @param jobType the type of the job.
 	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @param numberOfRecords the number of records.
	 * @param numberOfHashFamilies the number of hash families (may be null).
	 * @param numberOfHashesPerFamily the number of hashes per family (may be null).
	 * @return the aggregated metrics for the specified scenario.
	 */
	@Unused
	public AggregatedJobMetricByScenario getAggregatedJobMetricByScenario(String jobName, String jobType, String dataDescription, int parallelism, 
			long numberOfRecords, Integer numberOfHashFamilies, Integer numberOfHashesPerFamily){
		final Scenario scenario = 
				new Scenario(jobType, jobName, dataDescription, parallelism, numberOfRecords, numberOfHashFamilies, numberOfHashesPerFamily);
		
		return this.getAggregatedJobMetricByScenario(scenario);
	}
	
	/**
	 * Retrieves the aggregated metric for a specific scenario.
	 * 
	 * @param scenario the scenario.
	 * @return the aggregated metrics for the specified scenario.
	 */
	@SuppressWarnings("unchecked")
	@Unused
	public AggregatedJobMetricByScenario getAggregatedJobMetricByScenario(Scenario scenario){
		Session session = this.sessionFactory.openSession();
		
		Query<AggregatedJobMetricByScenario> query = session
				.getNamedNativeQuery("getAggregatedJobMetricsByScenario")
				.setParameter("name", scenario.getJobName())
				.setParameter("type", scenario.getJobType())
				.setParameter("dataDescription", scenario.getDataDescription())
				.setParameter("parallelism", scenario.getParallelism())
			    .setParameter("numberOfRecords", scenario.getNumberOfRecords())
			    .setParameter("numberOfHashFamilies", scenario.getHashFamilies(), IntegerType.INSTANCE)
			    .setParameter("numberOfHashesPerFamily", scenario.getHashFunctions(), IntegerType.INSTANCE);		
		
		AggregatedJobMetricByScenario result = query.getSingleResult();
		
		session.close();
		
		return result;
	}
	
	/**
	 * Retrieves all used (distinct) data set identifiers.
	 * 
	 * @return a list of data set identifiers.
	 */
	@SuppressWarnings("unchecked")
	public List<String> getDistinctDataDescriptions(){
		Session session = this.sessionFactory.openSession();
		
		Query<String> query = session
				.getNamedNativeQuery("getDistinctDataDescriptionsFromAggJobMetrics");
		
		List<String> result = query.getResultList();
				
		return result;	
	}
	
	/**
	 * Retrieves all parallelism values that are used connection with a certain
	 * data set.
	 * 
	 * @param dataDescription the data set identifier.
	 * 
	 * @return a list of parallelism values.
	 */
	@SuppressWarnings("unchecked")
	public List<Integer> getDistinctParallismGradesFromAggJobMetricsForDataDescr(String dataDescription){
		Session session = this.sessionFactory.openSession();
		
		Query<Integer> query = session
				.getNamedNativeQuery("getDistinctParallismGradesFromAggJobMetricsForDataDescr")
				.setParameter("dataDescription", dataDescription);
		
		List<Integer> result = query.getResultList();
				
		return result;	
	}
	
	/**
	 * Retrieves all parallelism values (independent of a certain data set).
	 * 
	 * @return a list of parallelism values.
	 */
	@SuppressWarnings("unchecked")
	public List<Integer> getDistinctParallismGradesFromAggJobMetrics(){
		Session session = this.sessionFactory.openSession();
		
		Query<Integer> query = session
				.getNamedNativeQuery("getDistinctParallismGradesFromAggJobMetrics");
		
		List<Integer> result = query.getResultList();
				
		return result;	
	}
	
	/**
	 * Retrieves the different number of records that are used in connection with
	 * a specific data set.
	 * 
	 * @param dataDescription the identifier of the data set.
	 * @return a list of record numbers.
	 */
	@SuppressWarnings("unchecked")
	public List<BigInteger> getDistinctNumberOfRecordsFromAggJobMetricsForDataDescr(String dataDescription){
		Session session = this.sessionFactory.openSession();
		
		Query<BigInteger> query = session
				.getNamedNativeQuery("getDistinctNumberOfRecordsFromAggJobMetricsForDataDescr")
				.setParameter("dataDescription", dataDescription);
		
		List<BigInteger> result = query.getResultList();
						
		return result;	
	}
	
	/**
	 * Retrieves the different number of records that are used in connection with
	 * specific jobs and a degree of parallelism and for a certain data set.
	 * 
	 * @param jobNames a list of job names.
	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @return a list of record numbers.
	 */
	@SuppressWarnings("unchecked")
	public List<BigInteger> getDistinctNumberOfRecordsFromAggJobMetricsForJobNameAndDataDescrAndParallelism(
			List<String> jobNames, String dataDescription, int parallelism){
		Session session = this.sessionFactory.openSession();
		
		Query<BigInteger> query = session
				.getNamedNativeQuery("getDistinctNumberOfRecordsFromAggJobMetricsForJobNameAndDataDescrAndParallelism")
				.setParameter("dataDescription", dataDescription)
				.setParameter("parallelism", parallelism)
				.setParameterList("jobNames", jobNames);
		
		List<BigInteger> result = query.getResultList();
						
		return result;	
	}
	
	/**
	 * Retrieves all jobs that are executed "on" a certain data set.
	 * 
	 * @param dataDescription the identifier of the data set.
	 * @return a list of job names.
	 */
	@SuppressWarnings("unchecked")
	public List<String> getDistinctJobNamesFromAggJobMetricsForDataDescr(String dataDescription){
		Session session = this.sessionFactory.openSession();
		
		Query<String> query = session
				.getNamedNativeQuery("getDistinctJobNamesFromAggJobMetricsForDataDescr")
				.setParameter("dataDescription", dataDescription);
		
		List<String> result = query.getResultList();
						
		return result;	
	}
	
	/**
	 * Retrieves all jobs that are executed "on" a certain data set for a
	 * specific degree of parallelism.
	 * 
	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @return a list of job names.
	 */
	@SuppressWarnings("unchecked")
	public List<String> getDistinctJobNamesFromAggJobMetricsForDataDescrAndParallelism(String dataDescription,
			int parallelism){
		Session session = this.sessionFactory.openSession();
		
		Query<String> query = session
				.getNamedNativeQuery("getDistinctJobNamesFromAggJobMetricsForDataDescrAndParallelism")
				.setParameter("dataDescription", dataDescription)
				.setParameter("parallelism", parallelism);
		
		List<String> result = query.getResultList();
						
		return result;	
	}
	
	/**
	 * Retrieves aggregated quality metrics that are related to jobs that are executed "on" a certain data
	 * set, a specific degree of parallelism and to a certain job.
	 * 
	 * @param jobName the name of the job.
	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @return a list of aggregated metrics that contain only the quality metrics (RR, PC, PQ, FM), see {@link MethodQualityMetrics}.
	 */
	public List<MethodQualityMetrics> getMetricsFromAggJobMetrics(String jobName, String dataDescription, int parallelism){
		List<String> jobNames = new ArrayList<String>();
		jobNames.add(jobName);
		return this.getMetricsFromAggJobMetrics(jobNames, dataDescription, parallelism);
	}
	
	/**
	 * Retrieves aggregated quality metrics that are related to jobs that are executed "on" a certain data
	 * set, a specific degree of parallelism and to one or many jobs.
	 *  
	 * @param jobNames a list of job names.
	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @return a list of aggregated metrics that contain only the quality metrics (RR, PC, PQ, FM), see {@link MethodQualityMetrics}.
	 */
	@SuppressWarnings("unchecked")
	public List<MethodQualityMetrics> getMetricsFromAggJobMetrics(List<String> jobNames, String dataDescription, int parallelism){
		Session session = this.sessionFactory.openSession();
		
		Query<Object[]> query = session
				.getNamedNativeQuery("getMetricsFromAggJobMetrics")
				.setParameter("dataDescription", dataDescription)
				.setParameter("parallelism", parallelism)
				.setParameterList("jobNames", jobNames);
		
		List<Object[]> result = query.getResultList();
		
		List<MethodQualityMetrics> metrics = new ArrayList<MethodQualityMetrics>();
		
		result.forEach((record) -> {
			final MethodQualityMetrics methodQualityMetric = MethodQualityMetrics.from(record);
			metrics.add(methodQualityMetric);
		});
		
		return metrics;	
	}
	
	/**
	 * Retrieves aggregated quality metrics that are related to jobs that are executed "on" a certain data
	 * set, a specific degree of parallelism, a certain number of records and to one or many jobs.
	 *  
	 * @param jobNames a list of job names.
	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @param recordNum the number of records.
	 * @return a list of aggregated metrics that contain only the quality metrics (RR, PC, PQ, FM), see {@link MethodQualityMetrics}.
	 */
	@SuppressWarnings("unchecked")
	public List<MethodQualityMetrics> getMetricsFromAggJobMetrics(List<String> jobNames, String dataDescription, int parallelism,
			BigInteger recordNum){
		Session session = this.sessionFactory.openSession();
		
		Query<Object[]> query = session
				.getNamedNativeQuery("getMetricsFromAggJobMetricsForCertainNumberOfRecords")
				.setParameter("dataDescription", dataDescription)
				.setParameter("parallelism", parallelism)
				.setParameterList("jobNames", jobNames)
				.setParameter("records", recordNum);
		
		List<Object[]> result = query.getResultList();
		
		List<MethodQualityMetrics> metrics = new ArrayList<MethodQualityMetrics>();
		
		result.forEach((record) -> {
			final MethodQualityMetrics methodQualityMetric = MethodQualityMetrics.from(record);
			metrics.add(methodQualityMetric);
		});
		
		return metrics;	
	}
	
	/**
	 * Retrieves aggregated runtime metrics that are related to jobs that are executed "on" a certain data
	 * set, a specific degree of parallelism and to one or many jobs.
	 *  
	 * @param jobNames a list of job names.
	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @return a list of aggregated metrics that contain only the runtime metrics, see {@link MethodRuntimeMetrics}.
	 */
	@SuppressWarnings("unchecked")
	public List<MethodRuntimeMetrics> getExecTimeMetricFromAggJobMetrics(List<String> jobNames, String dataDescription, 
			int parallelism){
		Session session = this.sessionFactory.openSession();
		
		Query<Object[]> query = session
				.getNamedNativeQuery("getExecTimeMetricFromAggJobMetrics")
				.setParameter("dataDescription", dataDescription)
				.setParameter("parallelism", parallelism)
				.setParameterList("jobNames", jobNames);
		
		List<Object[]> result = query.getResultList();
		
		List<MethodRuntimeMetrics> metrics = new ArrayList<MethodRuntimeMetrics>();
		
		result.forEach((record) -> {
			final MethodRuntimeMetrics methodRuntimeMetric = MethodRuntimeMetrics.from(record);
			metrics.add(methodRuntimeMetric);
		});
		
		return metrics;	
	}
	
	/**
	 * Retrieves aggregated runtime metrics that are related to jobs that are executed "on" a certain data
	 * set and to a jobs.
	 *  
	 * @param jobName the job name.
	 * @param dataDescription the identifier of the data set.
	 * @return a list of aggregated metrics that contain only the runtime metrics, see {@link MethodRuntimeMetrics}.
	 */
	@SuppressWarnings("unchecked")
	public List<MethodRuntimeMetrics> getExecTimeMetricFromAggJobMetricsForDataDescAndJobName(String jobName, 
			String dataDescription){
		Session session = this.sessionFactory.openSession();
		
		Query<Object[]> query = session
				.getNamedNativeQuery("getExecTimeMetricFromAggJobMetricsForDataDescAndJobName")
				.setParameter("dataDescription", dataDescription)
				.setParameter("jobName", jobName);
		
		List<Object[]> result = query.getResultList();
		
		List<MethodRuntimeMetrics> metrics = new ArrayList<MethodRuntimeMetrics>();
		
		result.forEach((record) -> {
			final MethodRuntimeMetrics methodRuntimeMetric = MethodRuntimeMetrics.from(record);
			metrics.add(methodRuntimeMetric);
		});
		
		return metrics;	
	}
	
	/**
	 * Retrieves aggregated runtime metrics that are related to jobs that are executed "on" a certain data
	 * set, a certain number of records and to one or many jobs.
	 *  
	 * @param jobNames a list of job names.
	 * @param dataDescription the identifier of the data set.
	 * @param recordNum the number of records.
	 * @return a list of aggregated metrics that contain only the quality metrics (RR, PC, PQ, FM), see {@link MethodQualityMetrics}.
	 */
	@SuppressWarnings("unchecked")
	public List<MethodRuntimeMetrics> getExecTimeMetricFromAggJobMetricsForDataDescAndJobNameAndRecords(List<String> jobNames, 
			String dataDescription, BigInteger records){
		Session session = this.sessionFactory.openSession();
		
		Query<Object[]> query = session
				.getNamedNativeQuery("getExecTimeMetricFromAggJobMetricsForDataDescAndJobNameAndRecords")
				.setParameter("dataDescription", dataDescription)
				.setParameterList("jobNames", jobNames)
				.setParameter("records", records);
		
		List<Object[]> result = query.getResultList();
		
		List<MethodRuntimeMetrics> metrics = new ArrayList<MethodRuntimeMetrics>();
		
		result.forEach((record) -> {
			final MethodRuntimeMetrics methodRuntimeMetric = MethodRuntimeMetrics.from(record);
			metrics.add(methodRuntimeMetric);
		});
		
		return metrics;	
	}
	
}
