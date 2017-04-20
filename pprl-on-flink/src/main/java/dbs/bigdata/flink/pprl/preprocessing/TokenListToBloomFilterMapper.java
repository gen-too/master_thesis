package dbs.bigdata.flink.pprl.preprocessing;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Adds a set of tokens (the tokenized qid attributes related to a person) into a bloom filter.
 * 
 * @author mfranke
 *
 */
public class TokenListToBloomFilterMapper implements 
	MapFunction<Tuple2<Person, List<String>>, Tuple2<Person, BloomFilter>> {

	private static final long serialVersionUID = -3771766587993456849L;
	private int size;
	private int hashes;
	
	/**
	 * Creates a new {@link TokenListToBloomFilterMapper}.
	 * 
	 * @param size size of the resulting bloom filter.
	 * @param hashes number of hash functions that should be used.
	 */
	public TokenListToBloomFilterMapper(int size, int hashes){
		this.size = size;
		this.hashes = hashes;
	}
	
	@Override
	public Tuple2<Person, BloomFilter> map(Tuple2<Person, List<String>> value) throws Exception {
		
		final Person person = value.f0;			
		BloomFilter bf = new BloomFilter(this.size, this.hashes);
			
		for (String token : value.f1){
			bf.addElement(token);
		}
		
		return new Tuple2<Person, BloomFilter>(person, bf);
	}
}