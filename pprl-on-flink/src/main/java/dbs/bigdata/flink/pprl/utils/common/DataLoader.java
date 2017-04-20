package dbs.bigdata.flink.pprl.utils.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;

import dbs.bigdata.flink.pprl.preprocessing.Person;

/**
 * Abstract class to load  data from a source file.
 * 
 * @author mfranke
 *
 * @param <T> type of objects in which each line (row) of the file will be transformed.
 * 
 */
public class DataLoader<T extends Object> {
	
	protected final Class<T> pojoClass;
	
	protected ExecutionEnvironment env;
	protected String dataFilePath;
	protected String lineDelimiter;
	protected String fieldDelimiter;
	protected String includingFields;
	protected String[] fieldNamesPojo;
	protected boolean ignoreFirstLine;
	
	
	/**
	 * Creates a new DataLoader.
	 * @param pojoClass 
	 */
	public DataLoader(Class<T> pojoClass){
		this.pojoClass = pojoClass;
	}


	/**
	 * Creates a new DataLoader.
	 * @param pojoClass the class of type in which each row will be transformed.
	 * @param env the ExecutionEnvironment of the flink job.
	 */
	public DataLoader(Class<T> pojoClass, ExecutionEnvironment env){
		this.pojoClass = pojoClass;
		this.env = env;
	}

	/**
	 * Creates a new DataLoader.
	 * @param pojoClass the class of type in which each row will be transformed.
	 * @param env the ExecutionEnvironment of the flink job.
	 * @param dataFilePath path to the data file.
	 * @param lineDelimiter character separator for the lines (i.e. rows).
	 * @param fieldDelimiter character separator for the fields (i.e. columns).
	 * @param includingFields String sequence containing 1s and 0s specifying if the column should be read or not.
	 * @param fieldNames the {@link Person} fields that corresponds to the includingFields.
	 * @param ignoreFirstLine boolean flag that indicates whether or not the first line contains only column labels.
	 */
	public DataLoader(Class<T> pojoClass, ExecutionEnvironment env, String dataFilePath, String lineDelimiter, String fieldDelimiter,
			String includingFields, String[] fieldNames, boolean ignoreFirstLine){
		this.pojoClass = pojoClass;
		this.env = env;
		this.dataFilePath = dataFilePath;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
		this.includingFields = includingFields;
		this.fieldNamesPojo = fieldNames;
		this.ignoreFirstLine = ignoreFirstLine;
	}
	
	/**
	 * Reads the data from the file into a DataSet<T> object.
	 * @return the DataSet<T> representation of the specified file.
	 */
	public DataSet<T> getData(){
		CsvReader reader = this.env.readCsvFile(this.dataFilePath);
				
		reader.lineDelimiter(this.lineDelimiter);
		reader.fieldDelimiter(this.fieldDelimiter);
		reader.includeFields(this.includingFields);
			
		if (this.ignoreFirstLine){
			reader.ignoreFirstLine();
		}
			
		return reader.pojoType(this.pojoClass, this.fieldNamesPojo);
	};
	
	public ExecutionEnvironment getEnv() {
		return env;
	}

	public void setEnv(ExecutionEnvironment env) {
		this.env = env;
	}

	public String getDataFilePath() {
		return dataFilePath;
	}

	public void setDataFilePath(String dataFilePath) {
		this.dataFilePath = dataFilePath;
	}

	public String getLineDelimiter() {
		return lineDelimiter;
	}

	public void setLineDelimiter(String lineDelimiter) {
		this.lineDelimiter = lineDelimiter;
	}

	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public void setFieldDelimiter(String fieldDelimiter) {
		this.fieldDelimiter = fieldDelimiter;
	}

	public String getIncludingFields() {
		return includingFields;
	}

	public void setIncludingFields(String includingFields) {
		this.includingFields = includingFields;
	}

	public String[] getFieldNames() {
		return fieldNamesPojo;
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNamesPojo = fieldNames;
	}

	public boolean isIgnoreFirstLine() {
		return ignoreFirstLine;
	}

	public void setIgnoreFirstLine(boolean ignoreFirstLine) {
		this.ignoreFirstLine = ignoreFirstLine;
	}
}