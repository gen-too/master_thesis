package dbs.bigdata.flink.pprl.evaluation;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.hibernate.annotations.DynamicInsert;

/**
 * A Scenario defines base parameters under which a group of
 * similar jobs is executed. More precise one pprl flink job
 * can be executed with custom parameters related to the job
 * and with different data sets, different record numbers and
 * various parallelism values. Each such job is then executed
 * for a certain number of iterations to reduce the impact of
 * outliers. But however each execution happens within the same
 * scenario.
 * 
 * @author mfranke
 */
@Embeddable
@DynamicInsert
public class Scenario implements Serializable{
	
	private static final long serialVersionUID = 3351749416452940397L;

	@Column(name = "job_type", nullable = false)
	private String jobType;
	
	@Column(name = "job_name", nullable = false)
	private String jobName;
	
	@Column(name = "data_description", nullable = false)
	private String dataDescription;
	
	@Column(name = "parallelism", nullable = false)
	private int parallelism;

	@Column(name = "records", nullable = false)
	private long numberOfRecords;
	
	@Column(name = "hash_families",  columnDefinition = "INTEGER NOT NULL DEFAULT -1")
	private Integer hashFamilies;
	
	@Column(name = "hash_functions", columnDefinition = "INTEGER NOT NULL DEFAULT -1")
	private Integer hashFunctions;
	
	/**
	 * Default empty constructor.
	 */
	public Scenario(){}

	/**
	 * Creates a new scenario.
	 * 
	 * @param jobType the type of the job. The type is referring to the type of blocking method.
	 * @param jobName the name of the job.
	 * @param dataDescription the identifier of the data set.
	 * @param parallelism the degree of parallelism.
	 * @param numberOfRecords the number of records.
	 * @param hashFamilies the number of hash families (may be null).
	 * @param hashFunctions the number of hash functions per hash family (may be null).
	 */
	public Scenario(String jobType, String jobName, String dataDescription, int parallelism, long numberOfRecords,
			Integer hashFamilies, Integer hashFunctions) {
		this.jobType = jobType;
		this.jobName = jobName;
		this.dataDescription = dataDescription;
		this.parallelism = parallelism;
		this.numberOfRecords = numberOfRecords;
		this.hashFamilies = (hashFamilies == null) ? -1 : hashFamilies;
		this.hashFunctions = (hashFunctions == null) ? -1 : hashFunctions;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataDescription == null) ? 0 : dataDescription.hashCode());
		result = prime * result + ((hashFamilies == null) ? 0 : hashFamilies.hashCode());
		result = prime * result + ((hashFunctions == null) ? 0 : hashFunctions.hashCode());
		result = prime * result + ((jobName == null) ? 0 : jobName.hashCode());
		result = prime * result + ((jobType == null) ? 0 : jobType.hashCode());
		result = prime * result + (int) (numberOfRecords ^ (numberOfRecords >>> 32));
		result = prime * result + parallelism;
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Scenario other = (Scenario) obj;
		if (dataDescription == null) {
			if (other.dataDescription != null) {
				return false;
			}
		} 
		else if (!dataDescription.equals(other.dataDescription)) {
			return false;
		}
		if (hashFamilies == null) {
			if (other.hashFamilies != null) {
				return false;
			}
		} 
		else if (!hashFamilies.equals(other.hashFamilies)) {
			return false;
		}
		if (hashFunctions == null) {
			if (other.hashFunctions != null) {
				return false;
			}
		} 
		else if (!hashFunctions.equals(other.hashFunctions)) {
			return false;
		}
		if (jobName == null) {
			if (other.jobName != null) {
				return false;
			}
		}
		else if (!jobName.equals(other.jobName)) {
			return false;
		}
		if (jobType == null) {
			if (other.jobType != null) {
				return false;
			}
		} 
		else if (!jobType.equals(other.jobType)) {
			return false;
		}
		if (numberOfRecords != other.numberOfRecords) {
			return false;
		}
		if (parallelism != other.parallelism) {
			return false;
		}
		return true;
	}


	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("\n scenario(");
		builder.append("\n  jobType=");
		builder.append(jobType);
		builder.append("\n  jobName=");
		builder.append(jobName);
		builder.append("\n  dataDescription=");
		builder.append(dataDescription);
		builder.append("\n  parallelism=");
		builder.append(parallelism);
		builder.append("\n  numberOfRecords=");
		builder.append(numberOfRecords);
		builder.append("\n  hashFamilies=");
		builder.append(hashFamilies);
		builder.append("\n  hashFunctions=");
		builder.append(hashFunctions);
		builder.append("\n )");
		return builder.toString();
	}

	public String getJobType() {
		return jobType;
	}

	public void setJobType(String jobType) {
		this.jobType = jobType;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getDataDescription() {
		return dataDescription;
	}

	public void setDataDescription(String dataDescription) {
		this.dataDescription = dataDescription;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public long getNumberOfRecords() {
		return numberOfRecords;
	}

	public void setNumberOfRecords(long numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
	}

	public Integer getHashFamilies() {
		return hashFamilies;
	}

	public void setHashFamilies(Integer hashFamilies) {
		this.hashFamilies = (hashFamilies == null) ? -1 : hashFamilies;
	}

	public Integer getHashFunctions() {
		return hashFunctions;
	}

	public void setHashFunctions(Integer hashFunctions) {
		this.hashFunctions = (hashFunctions == null) ? -1 : hashFunctions;
	}
}