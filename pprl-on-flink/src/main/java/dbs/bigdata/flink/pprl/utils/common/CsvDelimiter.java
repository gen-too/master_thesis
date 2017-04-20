package dbs.bigdata.flink.pprl.utils.common;

/**
 * Exchange class for common csv delimiter characters.
 * 
 * @author mfranke
 *
 */
public final class CsvDelimiter {
	
	private CsvDelimiter(){
		throw new RuntimeException();
	}
	
	public static final String TAB_DELIMITER = "\t";
	public static final String NEW_LINE_DELIMITER = "\n";
	public static final String COMMA_DELIMITER = ",";
}
