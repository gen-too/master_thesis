package dbs.bigdata.flink.pprl.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class for cleaning the id of records from generated data set.
 * The id of a record is in the form rec-id-[org,dup].
 * The map function extracts the id and returns the record.
 * 
 * @author mfranke
 *
 */
public class IdCleaner implements MapFunction<Tuple2<String, BloomFilter>, Tuple2<String, BloomFilter>> {

	private static final long serialVersionUID = 4942555605666891828L;

	private static final String SEPATOR_CHARACTER = "-";
	
	@Override
	public Tuple2<String, BloomFilter> map(Tuple2<String, BloomFilter> value) throws Exception {
		final String id = value.f0;
		
		final String cleanId = id.split(SEPATOR_CHARACTER)[1];
		
		return new Tuple2<String, BloomFilter>(cleanId, value.f1);
	}
}