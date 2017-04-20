package dbs.bigdata.flink.pprl.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Filters person id by the suffix.
 * 
 * @author mfranke
 *
 */
public class IdSuffixFilter implements FilterFunction<Tuple2<String, BloomFilter>> {

	private static final long serialVersionUID = 3590173996043206149L;

	private String suffix;
	private boolean match;
	
	/**
	 * Creates a new IdSuffixFilter.
	 * 
	 * @param suffix the suffix for the filter operation.
	 * @param match boolean flag that specifies whether or not the id should or should not match the suffix.
	 */
	public IdSuffixFilter(String suffix, boolean match){
		this.suffix = suffix;
		this.match = match;
	}
	
	@Override
	public boolean filter(Tuple2<String, BloomFilter> value) throws Exception {
		if (match){
			return value.f0.endsWith(this.suffix);
		}
		else{
			return !value.f0.endsWith(this.suffix);
		}
	}
}