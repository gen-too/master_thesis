package dbs.bigdata.flink.pprl.utils.lsh;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Interface for a BitSetHashfunction. Such a hash function accepts a {@link BitSet}
 * as input and returns T.
 * 
 * @author mfranke
 *
 */
public interface BitSetHashFunction<T> extends Serializable{

	/**
	 * Hashes a {@link BitSet} into a boolean value.
	 * 
	 * @param bitset
	 * 		-> the {@link BitSet} to hash.
	 * 
	 * @return
	 * 		-> a boolean value.
	 */
	public T hash(BitSet bitset);
}
