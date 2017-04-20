package dbs.bigdata.flink.pprl.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Map function for extracting the id of the person into the (person, bloom filter)-tuple.
 * 
 * @author mfranke
 *
 */
public class PersonIdExpander implements MapFunction<Tuple2<Person, BloomFilter>, Tuple3<String, Person, BloomFilter>>{

	private static final long serialVersionUID = -7937222459503703534L;

	@Override
	public Tuple3<String, Person, BloomFilter> map(Tuple2<Person, BloomFilter> value) throws Exception {
		return new Tuple3<String, Person, BloomFilter>(value.f0.getId(), value.f0, value.f1);
	}
}