package dbs.bigdata.flink.pprl.evaluation;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedNativeQueries;
import javax.persistence.NamedNativeQuery;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import org.hibernate.annotations.Generated;
import org.hibernate.annotations.GenerationTime;
import org.hibernate.annotations.Type;

import dbs.bigdata.flink.pprl.evaluation.rest.Job;
import dbs.bigdata.flink.pprl.evaluation.rest.JobAccumulator;
import dbs.bigdata.flink.pprl.evaluation.rest.JobConfig;

@NamedQueries({
	@NamedQuery(
		name = "getJobsByType",
		query = "SELECT j "
				+ "FROM ExecutedJobMetric j "
				+ "WHERE j.type = :type"
	),
	
	@NamedQuery(
		name = "getJobsByDataDescription",
		query = "SELECT j "
				+ "FROM ExecutedJobMetric j "
				+ "WHERE j.dataDescription = :dataDescription"
	),
	
	@NamedQuery(
		name = "getJobsByScenario",
		query = "SELECT j "
				+ "FROM ExecutedJobMetric j "
				+ "WHERE j.type = :type "
				+ "AND j.jobName = :name "
				+ "AND j.dataDescription = :dataDescription "
				+ "AND j.parallelism = :parallelism "
				+ "AND j.numberOfRecords = :numberOfRecords "
				+ "AND ((:numberOfHashFamilies IS NULL AND j.numberOfHashFamilies IS NULL) OR (j.numberOfHashFamilies = :numberOfHashFamilies)) "
				+ "AND ((:numberOfHashesPerFamily IS NULL AND j.numberOfHashesPerFamily IS NULL) OR (j.numberOfHashesPerFamily = :numberOfHashesPerFamily))"
	)
})

@NamedNativeQueries({
	@NamedNativeQuery(
			name = "getDistinctScenarios", 
			query = "SELECT DISTINCT "
					+ "j.job_type AS job_type, "
					+ "j.job_name AS job_name, "
					+ "j.data_description AS data_description, "
					+ "j.parallelism AS parallelism, "
					+ "j.records AS records, "
					+ "j.hash_families AS hash_families, "
					+ "j.hash_functions AS hash_functions "					
					+ "FROM job_metrics j "
	)
})
	
/**
 * A Object of this class represents a record of
 * the job_metrics table.
 * A ExecutedJobMetric contains basic information of a
 * executed pprl flink job and all exposed metrics.
 * 
 * @author mfranke
 *
 */
@Entity
@Table(name = "job_metrics")
public class ExecutedJobMetric {
	public static final BigDecimal NANO_2_SECONDS = new BigDecimal("1000000000");
	public static final BigDecimal MILLIS_2_SECONDS = new BigDecimal("1000");
	public static final BigDecimal PERCENT_MULTIPLICAND = new BigDecimal("100");

	private static final int DEFAULT_OUTPUT_SCALE = 2;
	private static final int DEFAULT_ROUNDING_MODE = BigDecimal.ROUND_HALF_EVEN;
	
	private long jobId;
	private String flinkJid;
	private String jobName;
	private int iteration;
	private BigDecimal executionTime;
	private BigDecimal numberOfTrueMatches;
	private BigDecimal numerOfFalseMatches;
	private BigDecimal numberOfRecordTuples;
	private BigDecimal numberOfCalculatedMatches;
	private BigDecimal numberOfRecords;
	private BigDecimal numberOfCandidates;
	private BigDecimal numberOfMatches;
	private double comparisonThreshold;
	private int parallelism;
	private String[] dataFilePaths;
	private String type;
	private String dataDescription;
	private Integer numberOfHashFamilies;
	private Integer numberOfHashesPerFamily;
	private int numberOfIterations;
	private BigDecimal reductionRate;
	private BigDecimal pairsCompleteness;
	private BigDecimal pairsQuality;
	private BigDecimal fMeasure;
	
	public ExecutedJobMetric(){}

	/**
	 * Creates {@link ExecutedJobMetric}.
	 * 
	 * @param job the job the containing metrics are related to.
	 * @param accumulators a list of {@link JobAccumulator}s that contains the flink accumulator values.
	 * @param config the job configuration which contains information about the job parameters. 
	 */
	public ExecutedJobMetric(Job job, List<JobAccumulator> accumulators, JobConfig config){
		this.flinkJid = job.getId();
		
		final String[] splittedName = job.getName().split("#");
		this.jobName = splittedName[0];
		this.iteration = Integer.parseInt(splittedName[1]);
		
		this.executionTime = new BigDecimal(job.getExecutionTime());
		
		for (JobAccumulator accu : accumulators){
			final String accuName = accu.getName();
			final Long accuValue = accu.getValue();
			
			if(accuName.equals("false-match-counter")){
				this.numerOfFalseMatches = new BigDecimal(accuValue);
			}
			else if (accuName.equals("true-match-counter")){
				this.numberOfTrueMatches = new BigDecimal(accuValue);
			}
			else if (accuName.equals("record-tuple-counter")){
				this.numberOfRecordTuples = new BigDecimal(accuValue);
			}
			else if (accuName.equals("record-counter")){
				this.numberOfRecords = new BigDecimal(accuValue);
			}
			else if (accuName.equals("match-counter")){
				this.numberOfCalculatedMatches = new BigDecimal(accuValue);
			}
			else if (accuName.equals("candidate-counter")){
				this.numberOfCandidates = new BigDecimal(accuValue);
			}
		}
		
		this.numberOfMatches = new BigDecimal(config.getNumberOfMatches());
		this.comparisonThreshold = config.getComparisonThreshold();
		this.parallelism = config.getParallelism();
		this.dataFilePaths = config.getDataFilePaths();
		this.type = config.getJobType();
		this.dataDescription = config.getDataDescription();
		this.numberOfHashFamilies = config.getNumberOfHashFamilies();
		this.numberOfHashesPerFamily = config.getNumberOfHashesPerFamily();
		this.numberOfIterations = config.getIterations();
		this.reductionRate = this.calculateReductionRate();
		this.pairsCompleteness = this.calculatePairsCompleteness();
		this.pairsQuality = this.calculatePairsQuality();
		this.fMeasure = this.calculateFMeasure();
	}

	private BigDecimal calculateReductionRate(){
		final BigDecimal one = new BigDecimal("1");
		final BigDecimal subtrahend = this.numberOfCandidates
				.divide(this.numberOfRecordTuples, DEFAULT_OUTPUT_SCALE * 2, DEFAULT_ROUNDING_MODE);

		final BigDecimal rr = one.subtract(subtrahend);
		final BigDecimal rrInPercent = rr
				.multiply(PERCENT_MULTIPLICAND)
				.setScale(DEFAULT_OUTPUT_SCALE, DEFAULT_ROUNDING_MODE);
	
		return rrInPercent;
	}
	
	private BigDecimal calculateRawPairsCompleteness(){
		final BigDecimal pc = this.numberOfTrueMatches
				.divide(this.numberOfMatches, DEFAULT_OUTPUT_SCALE * 2, DEFAULT_ROUNDING_MODE);
		
		return pc;
	}
	
	private BigDecimal calculatePairsCompleteness(){
		final BigDecimal pcInPercent = this.calculateRawPairsCompleteness()
				.multiply(PERCENT_MULTIPLICAND)
				.setScale(DEFAULT_OUTPUT_SCALE, DEFAULT_ROUNDING_MODE);
		
		return pcInPercent;
	}
	private BigDecimal calculateRawPairsQuality(){
		final BigDecimal pq = this.numberOfTrueMatches
				.divide(this.numberOfCalculatedMatches, DEFAULT_OUTPUT_SCALE * 2, DEFAULT_ROUNDING_MODE);
		
		return pq;
	}
	
	private BigDecimal calculatePairsQuality(){
		final BigDecimal pcInPercent = this.calculateRawPairsQuality()
				.multiply(PERCENT_MULTIPLICAND)
				.setScale(DEFAULT_OUTPUT_SCALE, DEFAULT_ROUNDING_MODE);
		
		return pcInPercent;
	}
	
	private BigDecimal calculateRawFMeasure(){
		final BigDecimal two = new BigDecimal("2");
		final BigDecimal pc = this.calculateRawPairsCompleteness();
		final BigDecimal pq = this.calculateRawPairsQuality();
		
		final BigDecimal nom = two.multiply(pc.multiply(pq));
		final BigDecimal denom = pc.add(pq);
		
		final BigDecimal fmeasure = nom.divide(denom, DEFAULT_OUTPUT_SCALE * 2, DEFAULT_ROUNDING_MODE);
		
		return fmeasure;
	}
	
	private BigDecimal calculateFMeasure(){		
		final BigDecimal fmeasureInPercent = this.calculateRawFMeasure()
				.multiply(PERCENT_MULTIPLICAND)
				.setScale(DEFAULT_OUTPUT_SCALE, DEFAULT_ROUNDING_MODE);
		
		return fmeasureInPercent;
	}
	
	
	@Id
	@Column(name = "job_id", columnDefinition = "SERIAL NOT NULL")
	@GeneratedValue(strategy = GenerationType.IDENTITY) // AUTO if hibernate creates the tables
	@Generated(value = GenerationTime.INSERT)
	public long getJobId() {
		return jobId;
	}

	public void setJobId(long jobId) {
		this.jobId = jobId;
	}

	@Column(name = "job_name", nullable = false)
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	
	@Column(name = "flink_jid", nullable = false, unique = true)
	public String getFlinkJid() {
		return flinkJid;
	}

	public void setFlinkJid(String flinkJid) {
		this.flinkJid = flinkJid;
	}
	
	@Column(name = "job_type", nullable = false)
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
	@Column(name = "execution_time", nullable = false)
	public long getExecutionTime() {
		return executionTime.longValue();
	}

	public void setExecutionTime(long executionTime) {
		this.executionTime = new BigDecimal(executionTime);
	}

	@Column(name = "true_matches", nullable = false)
	public long getNumberOfTrueMatches() {
		return numberOfTrueMatches.longValue();
	}

	public void setNumberOfTrueMatches(long numberOfTrueMatches) {
		this.numberOfTrueMatches = new BigDecimal(numberOfTrueMatches);
	}

	@Column(name = "false_matches", nullable = false)
	public long getNumerOfFalseMatches() {
		return numerOfFalseMatches.longValue();
	}

	public void setNumerOfFalseMatches(long numerOfFalseMatches) {
		this.numerOfFalseMatches = new BigDecimal(numerOfFalseMatches);
	}
	
	@Column(name = "records", nullable = false)
	public long getNumberOfRecords() {
		return numberOfRecords.longValue();
	}

	public void setNumberOfRecords(long numberOfRecords) {
		this.numberOfRecords = new BigDecimal(numberOfRecords);
	}

	@Column(name = "record_tuples", nullable = false)
	public long getNumberOfRecordTuples() {
		return numberOfRecordTuples.longValue();
	}

	public void setNumberOfRecordTuples(long numberOfRecordTuples) {
		this.numberOfRecordTuples = new BigDecimal(numberOfRecordTuples);
	}

	@Column(name = "matches", nullable = false)
	public long getNumberOfMatches() {
		return numberOfMatches.longValue();
	}

	public void setNumberOfMatches(long numberOfMatches) {
		this.numberOfMatches = new BigDecimal(numberOfMatches);
	}

	@Column(name = "calculated_matches", nullable = false)
	public long getNumberOfCalculatedMatches() {
		return numberOfCalculatedMatches.longValue();
	}

	public void setNumberOfCalculatedMatches(long numberOfCalculatedMatches) {
		this.numberOfCalculatedMatches = new BigDecimal(numberOfCalculatedMatches);
	}

	@Column(name = "candidates", nullable = false)
	public long getNumberOfCandidates() {
		return numberOfCandidates.longValue();
	}

	public void setNumberOfCandidates(long numberOfCandidates) {
		this.numberOfCandidates = new BigDecimal(numberOfCandidates);
	}

	@Column(name = "threshold", nullable = false)
	public double getComparisonThreshold() {
		return comparisonThreshold;
	}

	public void setComparisonThreshold(double comparisonThreshold) {
		this.comparisonThreshold = comparisonThreshold;
	}

	@Column(name = "parallelism", nullable = false)
	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
	@Column(name = "iteration", nullable = false)
	public int getIteration() {
		return iteration;
	}

	public void setIteration(int iteration) {
		this.iteration = iteration;
	}

	@Column(name = "total_iterations", nullable = false)
	public int getNumberOfIterations() {
		return numberOfIterations;
	}
	
	public void setNumberOfIterations(int numberOfIterations) {
		this.numberOfIterations = numberOfIterations;
	}
	
	@Column(name = "hash_functions", nullable = true)
	public Integer getNumberOfHashesPerFamily() {
		return numberOfHashesPerFamily;
	}

	public void setNumberOfHashesPerFamily(Integer numberOfHashesPerFamily) {
		this.numberOfHashesPerFamily = numberOfHashesPerFamily;
	}
	
	@Column(name = "hash_families", nullable = true)
	public Integer getNumberOfHashFamilies() {
		return numberOfHashFamilies;
	}

	public void setNumberOfHashFamilies(Integer numberOfHashFamilies) {
		this.numberOfHashFamilies = numberOfHashFamilies;
	}
	
	@Column(name = "data_description", nullable = false)
	public String getDataDescription() {
		return dataDescription;
	}
	
	public void setDataDescription(String dataDescription) {
		this.dataDescription = dataDescription;
	}
	
	@Column(name = "data_files", columnDefinition = "TEXT ARRAY NOT NULL")
	@Type(type = "dbs.bigdata.flink.pprl.utils.common.StringArrayType")
	public String[] getDataFilePaths() {
		return dataFilePaths;
	}

	public void setDataFilePaths(String[] dataFilePaths) {
		this.dataFilePaths = dataFilePaths;
	}

	@Column(name = "reduction_rate", precision = 5, scale = 2, nullable = false)
	public BigDecimal getReductionRate() {
		return reductionRate;
	}

	public void setReductionRate(BigDecimal reductionRate) {
		this.reductionRate = reductionRate;
	}

	@Column(name = "pairs_completeness", precision = 5, scale = 2, nullable = false)
	public BigDecimal getPairsCompleteness() {
		return pairsCompleteness;
	}
	
	public void setPairsCompleteness(BigDecimal pairsCompleteness) {
		this.pairsCompleteness = pairsCompleteness;
	}
	
	@Column(name = "pairs_quality", precision = 5, scale = 2, nullable = false)
	public BigDecimal getPairsQuality(){
		return pairsQuality;
	}
	
	public void setPairsQuality(BigDecimal pairsQuality) {
		this.pairsQuality = pairsQuality;
	}

	@Column(name = "f_measure", precision = 5, scale = 2, nullable = false)
	public BigDecimal getfMeasure() {
		return fMeasure;
	}

	public void setfMeasure(BigDecimal fMeasure) {
		this.fMeasure = fMeasure;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ExecutedJobMetric [");
		builder.append("\n jobId=");
		builder.append(jobId);
		builder.append("\n flinkJid=");
		builder.append(flinkJid);
		builder.append("\n jobName=");
		builder.append(jobName);
		builder.append("\n iteration=");
		builder.append(iteration);
		builder.append("\n executionTime=");
		builder.append(executionTime);
		builder.append("\n numberOfTrueMatches=");
		builder.append(numberOfTrueMatches);
		builder.append("\n numerOfFalseMatches=");
		builder.append(numerOfFalseMatches);
		builder.append("\n numberOfRecordTuples=");
		builder.append(numberOfRecordTuples);
		builder.append("\n numberOfCalculatedMatches=");
		builder.append(numberOfCalculatedMatches);
		builder.append("\n numberOfRecords=");
		builder.append(numberOfRecords);
		builder.append("\n numberOfCandidates=");
		builder.append(numberOfCandidates);
		builder.append("\n numberOfMatches=");
		builder.append(numberOfMatches);
		builder.append("\n comparisonThreshold=");
		builder.append(comparisonThreshold);
		builder.append("\n parallelism=");
		builder.append(parallelism);
		builder.append("\n dataFilePaths=");
		builder.append(Arrays.toString(dataFilePaths));
		builder.append("\n type=");
		builder.append(type);
		builder.append("\n dataDescription=");
		builder.append(dataDescription);
		builder.append("\n numberOfHashFamilies=");
		builder.append(numberOfHashFamilies);
		builder.append("\n numberOfHashesPerFamily=");
		builder.append(numberOfHashesPerFamily);
		builder.append("\n numberOfIterations=");
		builder.append(numberOfIterations);
		builder.append("\n reductionRate=");
		builder.append(reductionRate);
		builder.append("\n pairsCompleteness=");
		builder.append(pairsCompleteness);
		builder.append("\n pairsQuality=");
		builder.append(pairsQuality);
		builder.append("\n fMeasure=");
		builder.append(fMeasure);
		builder.append("\n]");
		return builder.toString();
	}
}