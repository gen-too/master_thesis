package dbs.bigdata.flink.pprl.preprocessing;


import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import dbs.bigdata.flink.pprl.utils.common.CsvDelimiter;


/**
 * Runner class for the {@link PersonInputToBloomFilterPreProcessor}.
 * Reads all necessary parameters from the command line and transforms
 * the pre-processing.
 * 
 * @author mfranke
 *
 */
@SuppressWarnings("deprecation")
public class PreProcessorRunner {

	public static final String PARAM_DATA_FILE_PATH 			= "dataFilePath";
	public static final String PARAM_LINE_DELIMITER 			= "lineDelimiter";
	public static final String PARAM_FIELD_DELIMITER 			= "fieldDelimiter";
	public static final String PARAM_INCLUDING_FIELDS 			= "includingFields";
	public static final String PARAM_PERSON_FIELDS 				= "personFields";
	public static final String PARAM_IGNORE_FIRST_LINE 			= "ignoreFirstLine";
	public static final String PARAM_WITH_CHAR_PADDING 			= "withCharacterPadding";
	public static final String PARAM_BLOOM_FILTER_SIZE 			= "bloomFilterSize";
	public static final String PARAM_BLOOM_FILTER_HASHES 		= "bloomFilterHashes";
	public static final String PARAM_N_GRAM_VALUE 				= "nGramValue";
	public static final String PARAM_PHONETIC_KEY_ATTRIBUTE 	= "phoneticKeyAttribute";
	public static final String PARAM_PHONETIC_CODEC 			= "phoneticCodec";
	public static final String PARAM_PHONETIC_SALT_ATTRIBUTE 	= "phoneticSaltAttribute";
	public static final String PARAM_PHONETIC_CODEC_SALT 		= "phoneticCodecForSalt";
	public static final String PARAM_BF_SIZE_PHONETIC_CODE 		= "bfSizePhoneticCode";
	public static final String PARAM_BF_HASHES_PHONETIC_CODE	= "bfHashesPhoneticCode";
	public static final String PARAM_IS_DATASET_GENERATED 		= "isDatasetGenerated";
	
	public static final String PHONETIC_ENCODER_PREFIX = "org.apache.commons.codec.language.";
	
	
	private static Options makeOptions(){
		Options options = new Options();

		options.addOption(new Option(PARAM_DATA_FILE_PATH, true, ""));
		options.addOption(new Option(PARAM_LINE_DELIMITER, true, ""));
		options.addOption(new Option(PARAM_FIELD_DELIMITER, true, ""));
		options.addOption(new Option(PARAM_INCLUDING_FIELDS, true, ""));
		
		Option personFieldOption = new Option(PARAM_PERSON_FIELDS, true, "");
		personFieldOption.setArgs(Person.FIELDS);
		
		options.addOption(personFieldOption);
		options.addOption(new Option(PARAM_IGNORE_FIRST_LINE, false, ""));
		options.addOption(new Option(PARAM_WITH_CHAR_PADDING, false, ""));
		options.addOption(new Option(PARAM_BLOOM_FILTER_SIZE, true, ""));
		options.addOption(new Option(PARAM_BLOOM_FILTER_HASHES, true, ""));
		options.addOption(new Option(PARAM_N_GRAM_VALUE, true, ""));				
		options.addOption(new Option(PARAM_PHONETIC_KEY_ATTRIBUTE, true, ""));
		options.addOption(new Option(PARAM_PHONETIC_CODEC, true, ""));
		options.addOption(new Option(PARAM_PHONETIC_SALT_ATTRIBUTE, true, ""));
		options.addOption(new Option(PARAM_PHONETIC_CODEC_SALT, true, ""));
		options.addOption(new Option(PARAM_BF_SIZE_PHONETIC_CODE, true, ""));
		options.addOption(new Option(PARAM_BF_HASHES_PHONETIC_CODE, true, ""));
		options.addOption(new Option(PARAM_IS_DATASET_GENERATED, false, ""));
		
		return options;
	}
	
	private static PersonInputToBloomFilterPreProcessor parseOptions(String[] args) throws ParseException{		
		CommandLineParser parser = new BasicParser();	
		CommandLine cmd = parser.parse(makeOptions(), args);
			
		final String dataFilePath = cmd.getOptionValue(PARAM_DATA_FILE_PATH);		
		String lineDelimiter = cmd.getOptionValue(PARAM_LINE_DELIMITER);
		String fieldDelimiter = cmd.getOptionValue(PARAM_FIELD_DELIMITER);
		final String includingFields = cmd.getOptionValue(PARAM_INCLUDING_FIELDS);
		final String[] personFields = cmd.getOptionValues(PARAM_PERSON_FIELDS);
		final boolean ignoreFirstLine = cmd.hasOption(PARAM_IGNORE_FIRST_LINE);
		final boolean withCharacterPadding = cmd.hasOption(PARAM_WITH_CHAR_PADDING);
		final int bloomFilterSize = Integer.valueOf(cmd.getOptionValue(PARAM_BLOOM_FILTER_SIZE));
		final int bloomFilterHashes = Integer.valueOf(cmd.getOptionValue(PARAM_BLOOM_FILTER_HASHES));
		final int nGramValue = Integer.valueOf(cmd.getOptionValue(PARAM_N_GRAM_VALUE));
		
		final String phoneticCodec = PHONETIC_ENCODER_PREFIX + cmd.getOptionValue(PARAM_PHONETIC_CODEC);
		final String phoneticCodecForSalt = PHONETIC_ENCODER_PREFIX + cmd.getOptionValue(PARAM_PHONETIC_CODEC_SALT);
		final int bfSizePhoneticCode = Integer.valueOf(cmd.getOptionValue(PARAM_BF_SIZE_PHONETIC_CODE));
		final int bfHashesPhoneticCode = Integer.valueOf(cmd.getOptionValue(PARAM_BF_HASHES_PHONETIC_CODE));
				
		final String phoneticKeyAttribute = cmd.getOptionValue(PARAM_PHONETIC_KEY_ATTRIBUTE);
		final String phoneticSaltAttribute = cmd.getOptionValue(PARAM_PHONETIC_SALT_ATTRIBUTE);
		
		final boolean isDatasetGenerated = cmd.hasOption(PARAM_IS_DATASET_GENERATED);
		
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
		
		
		final int pathNameSeparator = dataFilePath.lastIndexOf("/") + 1;
		final String filePath = dataFilePath.substring(0, pathNameSeparator);
		String fileName = dataFilePath.substring(pathNameSeparator, dataFilePath.length());
		
		if (fileName.endsWith(".csv") || fileName.endsWith(".txt")){
			fileName = fileName.substring(0, fileName.length() - 4);
		}
				
		final String outputFile = filePath + "Coded/" + fileName + "/";

		return 
			new PersonInputToBloomFilterPreProcessor(
				dataFilePath,
				lineDelimiter,
				fieldDelimiter,
				includingFields,
				personFields,
				ignoreFirstLine,
				withCharacterPadding,
				bloomFilterSize,
				bloomFilterHashes,
				nGramValue,
				outputFile,
				fileName,
				phoneticKeyAttribute,
				phoneticCodec,
				phoneticSaltAttribute,
				phoneticCodecForSalt,
				bfSizePhoneticCode,
				bfHashesPhoneticCode,
				isDatasetGenerated
			);
	}

	
	public static void main(String[] args) throws Exception{
		PersonInputToBloomFilterPreProcessor pp = parseOptions(args);
		
		pp.processWithBothCodedAndSaltedPhoneticEncoding();
		
		/*
		pp.processOnlyBloomFilter();
		pp.processWithCleanPhoneticEncoding();
		pp.processWithCodedPhoneticEncoding(false);
		pp.processWithCodedPhoneticEncoding(true);
		*/
	}
}