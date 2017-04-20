package dbs.bigdata.flink.pprl.preprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Filters person id by the suffix. 
 * Tuple4 implementation of the {@link IdSuffixFilterPhonetic} or rather {@link IdSuffixFilter}.
 * 
 * @author mfranke
 *
 */
public class IdSuffixFilterPhoneticDual implements FilterFunction<Tuple4<String, String, String, BloomFilter>> {

	private static final long serialVersionUID = 3590173996043206149L;

	private String suffix;
	private boolean match;
	
	/**
	 * Creates a new IdSuffixFilterPhonetic.
	 * 
	 * @param suffix the suffix for the filter operation.
	 * @param match boolean flag that specifies whether or not the id should or should not match the suffix.
	 */
	public IdSuffixFilterPhoneticDual(String suffix, boolean match){
		this.suffix = suffix;
		this.match = match;
	}
	
	@Override
	public boolean filter(Tuple4<String, String, String, BloomFilter> value) throws Exception {
		if (match){
			return value.f0.endsWith(this.suffix);
		}
		else{
			return !value.f0.endsWith(this.suffix);
		}
	}
}