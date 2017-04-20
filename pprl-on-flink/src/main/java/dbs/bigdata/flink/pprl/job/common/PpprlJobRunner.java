package dbs.bigdata.flink.pprl.job.common;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dbs.bigdata.flink.pprl.job.lsh.LshPpprlJob;
import dbs.bigdata.flink.pprl.job.nestedloop.NestedLoopPpprlJob;
import dbs.bigdata.flink.pprl.job.phonetic.PhoneticPpprlJob;
import dbs.bigdata.flink.pprl.utils.common.CsvDelimiter;

/**
 * Executor class to start a certain p3rl flink job.
 * 
 * @author mfranke
 *
 */
@SuppressWarnings("deprecation")
public class PpprlJobRunner {	
	private static final Logger log = LoggerFactory.getLogger(PpprlJobRunner.class);
	
	public static final String PARAM_PPRL_JOB = "pprlJob";
	public static final String PARAM_JOB_NAME = "jobName";
	public static final String PARAM_DATA_DESCRIPTION = "dataDescription";
	public static final String PARAM_FILE_PATHS = "dataFilePaths";
	public static final String PARAM_NUM_MATCHES = "numberOfMatches";
	public static final String PARAM_LINE_DELIMITER = "lineDelimiter";
	public static final String PARAM_FIELD_DELIMITER  = "fieldDelimiter";
	public static final String PARAM_INCLUDING_FIELDS = "includingFields";
	public static final String PARAM_FIELD_NAMES = "fieldNames";
	public static final String PARAM_LSH_RANGE = "valueRangeLsh";
	public static final String PARAM_HASH_FAMILIES = "numberOfHashFamilies";
	public static final String PARAM_HASH_FUNCTIONS = "numberOfHashesPerFamily";
	public static final String PARAM_THRESHOLD = "comparisonThreshold";
	public static final String PARAM_PARALLELISM = "parallelism";
	public static final String PARAM_ITERATIONS = "iterations";
	public static final String PARAM_OUTPUT = "output";
	
	private int iterations;
	
	public PpprlJobRunner(){
		this.iterations = 0;
	}
	
	private static Options makeOptions(){
		Options options = new Options();

		options.addOption(new Option("j", PARAM_PPRL_JOB, true, ""));
		
		options.addOption(new Option("n", PARAM_JOB_NAME, true, ""));
		
		options.addOption(new Option("d", PARAM_DATA_DESCRIPTION, true, ""));
		
		Option dataFilePathsOtion = new Option("d", PARAM_FILE_PATHS, true, "");
		dataFilePathsOtion.setArgs(10);
		options.addOption(dataFilePathsOtion);
				
		options.addOption(new Option("m", PARAM_NUM_MATCHES, true, ""));
		
		options.addOption(new Option("l", PARAM_LINE_DELIMITER, true, ""));
		options.addOption(new Option("f", PARAM_FIELD_DELIMITER, true, ""));
		options.addOption(new Option("a", PARAM_INCLUDING_FIELDS, true, ""));
		
		Option fieldNamesOption = new Option("A", PARAM_FIELD_NAMES, true, "");
		fieldNamesOption.setArgs(InputTuple.FIELDS);		
		options.addOption(fieldNamesOption);
		
		options.addOption(new Option("s", PARAM_LSH_RANGE, true, ""));
		options.addOption(new Option("h", PARAM_HASH_FAMILIES, true, ""));
		options.addOption(new Option("H", PARAM_HASH_FUNCTIONS, true, ""));
		options.addOption(new Option("t", PARAM_THRESHOLD, true, ""));
		
		options.addOption(new Option("p", PARAM_PARALLELISM, true, ""));
		
		options.addOption(new Option("i", PARAM_ITERATIONS, true, ""));
		
		options.addOption(new Option("o", PARAM_OUTPUT, true, ""));
		
		return options;
	}
		
	private PpprlJob parseOptions(String[] args) throws ClassNotFoundException, ParseException{
		return this.parseOptions(makeOptions(), args);
	}
	
	private PpprlJob parseOptions(Options options, String[] args) throws ParseException, ClassNotFoundException{
		CommandLineParser parser = new BasicParser();	
		CommandLine cmd = parser.parse(options, args);

		final String pprlJobType = cmd.getOptionValue(PARAM_PPRL_JOB, "");
		
		final String jobName = cmd.getOptionValue(PARAM_JOB_NAME, null);
		
		final String dataDescription = cmd.getOptionValue(PARAM_DATA_DESCRIPTION, "");
		
		final String[] dataFilePaths = cmd.getOptionValues(PARAM_FILE_PATHS);	
				
		final long numberOfMatches = Long.parseLong(cmd.getOptionValue(PARAM_NUM_MATCHES, "0"));
	
		String lineDelimiter = cmd.getOptionValue(PARAM_LINE_DELIMITER, "");
		String fieldDelimiter = cmd.getOptionValue(PARAM_FIELD_DELIMITER, "");
		final String includingFields = cmd.getOptionValue(PARAM_INCLUDING_FIELDS, "");
		final String[] fieldNames = cmd.getOptionValues(PARAM_FIELD_NAMES);
		
		final int valueRangeLsh = Integer.parseInt(cmd.getOptionValue(PARAM_LSH_RANGE, "0"));
		final int numberOfHashFamilies = Integer.parseInt(cmd.getOptionValue(PARAM_HASH_FAMILIES, "0"));
		final int numberOfHashesPerFamily = Integer.parseInt(cmd.getOptionValue(PARAM_HASH_FUNCTIONS, "0"));
		
		final double comparisonThreshold = Double.parseDouble(cmd.getOptionValue(PARAM_THRESHOLD, "0"));
				
		final int parallelism = Integer.parseInt(cmd.getOptionValue(PARAM_PARALLELISM, "-1"));
		
		final String outputPath = cmd.getOptionValue(PARAM_OUTPUT, "");
		
		this.iterations = Integer.parseInt(cmd.getOptionValue(PARAM_ITERATIONS, "1"));
		
		if (lineDelimiter.equals("tab")){
			lineDelimiter = CsvDelimiter.TAB_DELIMITER;
		}
		else if (lineDelimiter.equals("line")){
			lineDelimiter = CsvDelimiter.NEW_LINE_DELIMITER;
		}
		else if(lineDelimiter.equals("comma")){
			lineDelimiter = CsvDelimiter.COMMA_DELIMITER;
		}
		
		if (fieldDelimiter.equals("tab")){
			fieldDelimiter = CsvDelimiter.TAB_DELIMITER;
		}
		else if (fieldDelimiter.equals("line")){
			fieldDelimiter = CsvDelimiter.NEW_LINE_DELIMITER;
		}
		else if(fieldDelimiter.equals("comma")){
			fieldDelimiter = CsvDelimiter.COMMA_DELIMITER;
		}
						
		Configuration config = new Configuration();
		config.setString(PARAM_PPRL_JOB, pprlJobType);
		config.setString(PARAM_DATA_DESCRIPTION, dataDescription);
		
		StringBuilder files = new StringBuilder();
		for (String file : dataFilePaths){
			files.append(file);
			files.append("#");
		}
		files.deleteCharAt(files.length()-1);
		
		config.setString(PARAM_FILE_PATHS, files.toString());
		
		config.setLong(PARAM_NUM_MATCHES, numberOfMatches);
		config.setDouble(PARAM_THRESHOLD, comparisonThreshold);
		config.setInteger(PARAM_PARALLELISM, parallelism);
		config.setInteger(PARAM_ITERATIONS, this.iterations);

		if (pprlJobType.equals(LshPpprlJob.class.getSimpleName())){
			config.setInteger(PARAM_HASH_FAMILIES, numberOfHashFamilies);
			config.setInteger(PARAM_HASH_FUNCTIONS, numberOfHashesPerFamily);
			
			LshPpprlJob job = new LshPpprlJob(config);
			job.setJobName(jobName);
			job.setDataFilePaths(dataFilePaths);
			job.setLineDelimiter(lineDelimiter);
			job.setFieldDelimiter(fieldDelimiter);
			job.setIncludingFields(includingFields);
			job.setFieldNames(fieldNames);
			job.setValueRangeLsh(valueRangeLsh);
			job.setNumberOfHashFamilies(numberOfHashFamilies);
			job.setNumberOfHashesPerFamily(numberOfHashesPerFamily);
			job.setComparisonThreshold(comparisonThreshold);
			job.setOutputPath(outputPath);
			
			return job;
		}
		else if (pprlJobType.equals(PhoneticPpprlJob.class.getSimpleName())){
			
			PhoneticPpprlJob job = new PhoneticPpprlJob(config);
			job.setJobName(jobName);
			job.setDataFilePaths(dataFilePaths);
			job.setLineDelimiter(lineDelimiter);
			job.setFieldDelimiter(fieldDelimiter);
			job.setIncludingFields(includingFields);
			job.setFieldNames(fieldNames);
			job.setComparisonThreshold(comparisonThreshold);
			job.setOutputPath(outputPath);
			
			return job;		
		}
		else if(pprlJobType.equals(NestedLoopPpprlJob.class.getSimpleName())){
			NestedLoopPpprlJob job = new NestedLoopPpprlJob(config);
			job.setJobName(jobName);
			job.setDataFilePaths(dataFilePaths);
			job.setLineDelimiter(lineDelimiter);
			job.setFieldDelimiter(fieldDelimiter);
			job.setIncludingFields(includingFields);
			job.setFieldNames(fieldNames);
			job.setComparisonThreshold(comparisonThreshold);
			job.setOutputPath(outputPath);
			
			return job;	
		}
		else{
			throw new ClassNotFoundException();
		}
	}
	
	public void run(PpprlJob job) throws Exception{
		if (job != null){
			for (int i = 0; i < this.iterations; i++){
				final JobExecutionResult result = job.runJob(i);
				log.info("Job finished with " + result.getNetRuntime() + "ms");
			}
		}
	}
	
	public static void main(String[] args) throws Exception {	
		/* use of a property file:
		final String propertiesFile = "parameter_config.properties";
		ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFile);
		
		String personField = parameter.get("personFields");
		String[] personFields = personField.split("#");
		
		for (int i = 0; i < personFields.length; i++) System.out.println(personFields[i]);
		*/
		log.info("Parsing command line options...");
		
		final PpprlJobRunner jobRunner = new PpprlJobRunner();
		final PpprlJob job = jobRunner.parseOptions(args);
		jobRunner.run(job);
	}
}