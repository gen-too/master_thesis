package dbs.bigdata.flink.pprl.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class for cleaning the id of records from generated data set.
 * The id of a record is in the form rec-id-[org,dup].
 * The map function extracts the id and returns the record.
 * Equal to {@link IdCleaner} but the Tuple3 implementation.
 * 
 * @author mfranke
 *
 */
public class IdCleanerPhonetic implements MapFunction<Tuple3<String, String, BloomFilter>, Tuple3<String, String, BloomFilter>> {

	private static final long serialVersionUID = 3084145116483095359L;
	private static final String SEPATOR_CHARACTER = "-";
	

	@Override
	public Tuple3<String, String, BloomFilter> map(Tuple3<String, String, BloomFilter> value) throws Exception {
		final String id = value.f0;
		
		final String cleanId = id.split(SEPATOR_CHARACTER)[1];
		
		return new Tuple3<String, String, BloomFilter>(cleanId, value.f1, value.f2);
	}
}