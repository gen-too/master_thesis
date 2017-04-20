package dbs.bigdata.flink.pprl.preprocessing;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;
import dbs.bigdata.flink.pprl.utils.common.CsvDelimiter;
import dbs.bigdata.flink.pprl.utils.common.DataLoader;


/**
 * Preprocessor that transforms records of a data set into bloom filters.
 * 
 * @author mfranke
 *
 */
public class PersonInputToBloomFilterPreProcessor {
			
	private static final String DIR_BLOOM_FILTER = "bloom_filter/";
	
	private static final String FILE_PHONETIC_CLEAN = "phonetic_clean";
	private static final String FILE_PHONETIC = "phonetic_coded";
	private static final String FILE_PHONETIC_SALT = "phonetic_coded_salt";
	private static final String FILE_PHONETIC_DUAL = "phonetic_dual";
	
	private static final String DIR_PHONETIC_CLEAN = FILE_PHONETIC_CLEAN +"/";
	private static final String DIR_PHONETIC = FILE_PHONETIC + "/";
	private static final String DIR_PHONETIC_SALT = FILE_PHONETIC_SALT + "/";
	private static final String DIR_PHONETIC_DUAL = FILE_PHONETIC_DUAL + "/";
	
	
	
	
	private static final String ORG_SUFFIX = "org";
	private static final String DUP_SUFFIX = "dup";
	
	private final ExecutionEnvironment env;
	private String dataFilePath;
	private String lineDelimiter;
	private String fieldDelimiter;
	private String includingFields;
	private String[] personFields;
	private boolean ignoreFirstLine;
	private boolean withCharacterPadding;
	private int bloomFilterSize;
	private int bloomFilterHashes;
	private int nGramValue;
	private String outputFile;
	private String filename;
	private String phoneticKeyAttribute;
	private String phoneticCodec;
	private String phoneticSaltAttribute;
	private String phoneticCodecForSalt;
	private int bfSizePhoneticCode;
	private int bfHashesPhoneticCode;
	private boolean isDatasetGenerated;
	
	public PersonInputToBloomFilterPreProcessor(ExecutionEnvironment env){
		// set up the execution environment
		this.env =  ExecutionEnvironment.getExecutionEnvironment();	
	}
	
	public PersonInputToBloomFilterPreProcessor(String dataFilePath, String lineDelimiter, String fieldDelimiter,
			String includingFields, String[] personFields, boolean ignoreFirstLine, boolean withCharacterPadding,
			int bloomFilterSize, int bloomFilterHashes, int nGramValue, String outputFile, String filename, String phoneticKeyAttribute, String phoneticCodec,
			String phoneticSaltAttribute, String phoneticCodecForSalt, int bfSizePhoneticCode, int bfHashesPhoneticCode, boolean isDatasetGenerated){
		
		this.env = ExecutionEnvironment.getExecutionEnvironment();
		this.dataFilePath = dataFilePath;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
		this.includingFields = includingFields;
		this.personFields = personFields;
		this.ignoreFirstLine = ignoreFirstLine;
		this.withCharacterPadding = withCharacterPadding;
		this.bloomFilterSize = bloomFilterSize;
		this.bloomFilterHashes = bloomFilterHashes;
		this.nGramValue = nGramValue;
		this.outputFile = outputFile;
		this.filename = filename;
		this.phoneticKeyAttribute = phoneticKeyAttribute;
		this.phoneticCodec = phoneticCodec;
		this.phoneticSaltAttribute = phoneticSaltAttribute;
		this.phoneticCodecForSalt = phoneticCodecForSalt;
		this.bfSizePhoneticCode = bfSizePhoneticCode;
		this.bfHashesPhoneticCode = bfHashesPhoneticCode;
		this.isDatasetGenerated = isDatasetGenerated;
	}
	
	private String constructOutputFile(String dir){
		return this.constructOutputFile(dir, null, null);
	}
	
	private String constructOutputFile(String dir, String fileSuffix){
		return this.constructOutputFile(dir, fileSuffix, null);
	}
	
	private String constructOutputFile(String dir, String fileSuffix, String suffix){
		StringBuilder builder = new StringBuilder();
		
		builder.append(this.outputFile);
		builder.append(dir);
		builder.append(this.filename);
		builder.append((fileSuffix != null && !fileSuffix.isEmpty()) ? "_" + fileSuffix : "");
		builder.append((suffix != null && !suffix.isEmpty()) ? "_" + suffix : "");
		builder.append(".csv");
		
		return builder.toString();
	}
	
	/**
	 * Transforms all records of a data set into bloom filters.
	 * The result is written as a csv-file under the specified location.
	 * 
	 * @throws Exception
	 */
	public void processOnlyBloomFilter() throws Exception{
		DataSet<Tuple2<String, BloomFilter>> data = this.getIdBloomFilterPairs();
		
		if (isDatasetGenerated){
			DataSet<Tuple2<String, BloomFilter>> orgData = data.filter(new IdSuffixFilter(ORG_SUFFIX, true));
			
			DataSet<Tuple2<String, BloomFilter>> dupData = data.filter(new IdSuffixFilter(ORG_SUFFIX, false));
			
			orgData = orgData.map(new IdCleaner());
			
			dupData = dupData.map(new IdCleaner());
			
			orgData
				.writeAsCsv(
					constructOutputFile(DIR_BLOOM_FILTER, null, ORG_SUFFIX),
					CsvDelimiter.NEW_LINE_DELIMITER, 
					CsvDelimiter.COMMA_DELIMITER, 
					WriteMode.OVERWRITE)
				.setParallelism(1);
			
			dupData
				.writeAsCsv(
					constructOutputFile(DIR_BLOOM_FILTER, null, DUP_SUFFIX),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		else{
			data
				.writeAsCsv(
					constructOutputFile(DIR_BLOOM_FILTER),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		
		env.execute();
	}	
	
	/**
	 * Transforms all records of a data set into bloom filters with a clean (uncoded) phonetic key.
	 * The result is written as a csv-file under the specified location.
	 * 
	 * @throws Exception
	 */
	public void processWithCleanPhoneticEncoding() throws Exception{
		DataSet<Tuple3<String, Person, BloomFilter>> bloomFilter = this.getPersonBloomFilterPairs();
		
		DataSet<Tuple4<String, Person, String, BloomFilter>> bloomFilterWithPhoneticCode 
			= buildPhoneticEncoding(env, bloomFilter);
		
		DataSet<Tuple3<String, String, BloomFilter>> data = bloomFilterWithPhoneticCode.project(0,2,3);
		
		if (isDatasetGenerated){	
			DataSet<Tuple3<String, String, BloomFilter>> orgData = data.filter(new IdSuffixFilterPhonetic(ORG_SUFFIX, true));
			
			DataSet<Tuple3<String, String, BloomFilter>> dupData = data.filter(new IdSuffixFilterPhonetic(ORG_SUFFIX, false));
			
			orgData = orgData.map(new IdCleanerPhonetic());
			
			dupData = dupData.map(new IdCleanerPhonetic());
			
			orgData
				.writeAsCsv(
					constructOutputFile(DIR_PHONETIC_CLEAN, FILE_PHONETIC_CLEAN, ORG_SUFFIX),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER, 
					WriteMode.OVERWRITE)
				.setParallelism(1);
			
			dupData
				.writeAsCsv(
					constructOutputFile(DIR_PHONETIC_CLEAN, FILE_PHONETIC_CLEAN, DUP_SUFFIX),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		else{
			data
				.writeAsCsv(
					constructOutputFile(DIR_PHONETIC_CLEAN, FILE_PHONETIC_CLEAN),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		
		env.execute();
	}
	
	/**
	 * Transforms all records of a data set into bloom filters with a phonetic key coded as bloom filter.
	 * The result is written as a csv-file under the specified location.
	 * 
	 * @param withSalt boolean flag specifies whether or not a salt should used for the coding of the phonetic key.
	 * @throws Exception
	 */
	public void processWithCodedPhoneticEncoding(boolean withSalt) throws Exception{	
		final String fileDir = (withSalt) ? DIR_PHONETIC_SALT : DIR_PHONETIC; 
		final String file = (withSalt) ? FILE_PHONETIC_SALT : FILE_PHONETIC;
		
		DataSet<Tuple3<String, Person, BloomFilter>> bloomFilter = this.getPersonBloomFilterPairs();
				
		DataSet<Tuple4<String, Person, String, BloomFilter>> bloomFilterWithPhoneticCode 
			= buildPhoneticEncoding(env, bloomFilter);
		
		DataSet<Tuple3<String, String, BloomFilter>> codedData = 
				bloomFilterWithPhoneticCode
				.map(new PhoneticCodeAnonymizer(
							withSalt, 
							this.phoneticSaltAttribute,
							this.bfHashesPhoneticCode,
							this.bfSizePhoneticCode
					)
				)
				.withParameters(PhoneticCodeAnonymizer.createEncodingConfiguration(this.phoneticCodecForSalt));
		
		
		if (isDatasetGenerated){
			DataSet<Tuple3<String, String, BloomFilter>> orgCodedData = codedData.filter(new IdSuffixFilterPhonetic(ORG_SUFFIX, true));
			
			DataSet<Tuple3<String, String, BloomFilter>> dupCodedData = codedData.filter(new IdSuffixFilterPhonetic(ORG_SUFFIX, false));
			
			orgCodedData = orgCodedData.map(new IdCleanerPhonetic());
			
			dupCodedData = dupCodedData.map(new IdCleanerPhonetic());
			
			
			orgCodedData
				.writeAsCsv(
					constructOutputFile(fileDir, file, ORG_SUFFIX),	
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		
			dupCodedData
				.writeAsCsv(
					constructOutputFile(fileDir, file, DUP_SUFFIX),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		else{
			codedData
				.writeAsCsv(
					constructOutputFile(fileDir, file),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		
		env.execute();
	}
	
	/**
	 * Transforms all records of a data set into bloom filters with a phonetic key coded as bloom filter once with and once without salting.
	 * The result is written as a csv-file under the specified location.
	 * 
	 * @param withSalt boolean flag specifies whether or not a salt should used for the coding of the phonetic key.
	 * @throws Exception
	 */
	public void processWithBothCodedAndSaltedPhoneticEncoding() throws Exception{
		DataSet<Tuple3<String, Person, BloomFilter>> bloomFilter = this.getPersonBloomFilterPairs();
		
		DataSet<Tuple4<String, Person, String, BloomFilter>> bloomFilterWithPhoneticCode 
			= buildPhoneticEncoding(env, bloomFilter);
		
		DataSet<Tuple4<String, String, String, BloomFilter>> codedData = 
				bloomFilterWithPhoneticCode
				.map(new PhoneticCodeDualAnonymizer(
							this.phoneticSaltAttribute,
							this.bfHashesPhoneticCode,
							this.bfSizePhoneticCode
					)
				)
				.withParameters(PhoneticCodeAnonymizer.createEncodingConfiguration(this.phoneticCodecForSalt));
		
		
		if (isDatasetGenerated){
			DataSet<Tuple4<String, String, String, BloomFilter>> orgCodedData = codedData.filter(new IdSuffixFilterPhoneticDual(ORG_SUFFIX, true));
			
			DataSet<Tuple4<String, String, String, BloomFilter>> dupCodedData = codedData.filter(new IdSuffixFilterPhoneticDual(ORG_SUFFIX, false));
			
			orgCodedData = orgCodedData.map(new IdCleanerPhoneticDual());
			
			dupCodedData = dupCodedData.map(new IdCleanerPhoneticDual());
			
			
			orgCodedData
				.writeAsCsv(
					constructOutputFile(DIR_PHONETIC_DUAL, "", ORG_SUFFIX),	
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		
			dupCodedData
				.writeAsCsv(
					constructOutputFile(DIR_PHONETIC_DUAL, "", DUP_SUFFIX),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		else{
			codedData
				.writeAsCsv(
					constructOutputFile(DIR_PHONETIC_DUAL, ""),
					CsvDelimiter.NEW_LINE_DELIMITER,
					CsvDelimiter.COMMA_DELIMITER,
					WriteMode.OVERWRITE)
				.setParallelism(1);
		}
		
		env.execute();
	}
	
	private DataSet<Tuple2<String, BloomFilter>> getIdBloomFilterPairs() throws Exception{
		return this.getPersonBloomFilterPairs().project(0,2);
	}
	
	private DataSet<Tuple3<String, Person, BloomFilter>> getPersonBloomFilterPairs() throws Exception{
		// csv-file --> (Person)
		DataSet<Person> data = loadDataFromCsvFiles(env);
		
		data.writeAsText(this.outputFile + filename + "_persons.txt", WriteMode.OVERWRITE).setParallelism(1);
		
		// (Person) --> (Person, Bloom Filter)
		DataSet<Tuple2<Person, BloomFilter>> bloomFilter = buildBloomFilter(env, data);
			
		// (Person, Bloom Filter) --> (Id, Person, Bloom Filter)
		DataSet<Tuple3<String, Person, BloomFilter>> result = bloomFilter.map(new PersonIdExpander());
		
		result.writeAsText(this.outputFile + filename + "_bloom_filters.txt", WriteMode.OVERWRITE).setParallelism(1);
		
		return result;
	}
	
	private DataSet<Person> loadDataFromCsvFiles(ExecutionEnvironment env) throws Exception{
		
		DataLoader<Person> personLoader = new DataLoader<Person>(
				Person.class,
				env,
				this.dataFilePath,
				this.lineDelimiter,
				this.fieldDelimiter,
				this.includingFields,
				this.personFields,
				this.ignoreFirstLine
		);
				
		DataSet<Person> persons = personLoader.getData();
		
		DataSet<Person> personsWithId = persons.map(new PersonAddIdMapper());
				
		return personsWithId;
	}
	
		
	private DataSet<Tuple2<Person, BloomFilter>> buildBloomFilter(ExecutionEnvironment env,
		DataSet<Person> data) throws Exception{
			
		DataSet<Tuple2<Person, List<String>>> tokens = 
				data.map(new NGramListTokenizer(this.nGramValue, this.withCharacterPadding));
		
		tokens.writeAsText(this.outputFile + filename + "_tokens.txt", WriteMode.OVERWRITE).setParallelism(1);
					
		return tokens.map(new TokenListToBloomFilterMapper(this.bloomFilterSize, this.bloomFilterHashes));
	}
	
	
	private DataSet<Tuple4<String, Person, String, BloomFilter>> buildPhoneticEncoding(ExecutionEnvironment env, 
			DataSet<Tuple3<String, Person, BloomFilter>> data) throws ClassNotFoundException{		
		return data
				.map(new PhoneticEncoder(this.phoneticKeyAttribute))
				.withParameters(PhoneticEncoder.createEncodingConfiguration(this.phoneticCodec));	
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

	public String[] getPersonFields() {
		return personFields;
	}

	public void setPersonFields(String[] personFields) {
		this.personFields = personFields;
	}

	public boolean isIgnoreFirstLine() {
		return ignoreFirstLine;
	}

	public void setIgnoreFirstLine(boolean ignoreFirstLine) {
		this.ignoreFirstLine = ignoreFirstLine;
	}

	public boolean isWithCharacterPadding() {
		return withCharacterPadding;
	}

	public void setWithCharacterPadding(boolean withCharacterPadding) {
		this.withCharacterPadding = withCharacterPadding;
	}

	public int getBloomFilterSize() {
		return bloomFilterSize;
	}

	public void setBloomFilterSize(int bloomFilterSize) {
		this.bloomFilterSize = bloomFilterSize;
	}

	public int getBloomFilterHashes() {
		return bloomFilterHashes;
	}

	public void setBloomFilterHashes(int bloomFilterHashes) {
		this.bloomFilterHashes = bloomFilterHashes;
	}

	public int getnGramValue() {
		return nGramValue;
	}

	public void setnGramValue(int nGramValue) {
		this.nGramValue = nGramValue;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public ExecutionEnvironment getEnv() {
		return env;
	}

	public String getPhoneticCodec() {
		return phoneticCodec;
	}

	public void setPhoneticCodec(String phoneticCodec) {
		this.phoneticCodec = phoneticCodec;
	}

	public String getPhoneticCodecForSalt() {
		return phoneticCodecForSalt;
	}

	public void setPhoneticCodecForSalt(String phoneticCodecForSalt) {
		this.phoneticCodecForSalt = phoneticCodecForSalt;
	}

	public int getBfSizePhoneticCode() {
		return bfSizePhoneticCode;
	}

	public void setBfSizePhoneticCode(int bfSizePhoneticCode) {
		this.bfSizePhoneticCode = bfSizePhoneticCode;
	}

	public int getBfHashesPhoneticCode() {
		return bfHashesPhoneticCode;
	}

	public void setBfHashesPhoneticCode(int bfHashesPhoneticCode) {
		this.bfHashesPhoneticCode = bfHashesPhoneticCode;
	}

	public String getPhoneticKeyAttribute() {
		return phoneticKeyAttribute;
	}

	public void setPhoneticKeyAttribute(String phoneticKeyAttribute) {
		this.phoneticKeyAttribute = phoneticKeyAttribute;
	}

	public String getPhoneticSaltAttribute() {
		return phoneticSaltAttribute;
	}

	public void setPhoneticSaltAttribute(String phoneticSaltAttribute) {
		this.phoneticSaltAttribute = phoneticSaltAttribute;
	}

	public boolean isDatasetGenerated() {
		return isDatasetGenerated;
	}

	public void setDatasetGenerated(boolean isDatasetGenerated) {
		this.isDatasetGenerated = isDatasetGenerated;
	}
}