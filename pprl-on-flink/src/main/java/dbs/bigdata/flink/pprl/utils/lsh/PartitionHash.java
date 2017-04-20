package dbs.bigdata.flink.pprl.utils.lsh;

import java.util.BitSet;

import dbs.bigdata.flink.pprl.utils.common.Unused;

/**
 * Class that implements a partition hash function.
 * A partition hash function is a {@link BitSetHashFunction} which returns
 * a {@link BitSet} object as hash value. The hash (bit set) is calculated by
 * taking a consecutive partition from the input bit set. 
 * 
 * @author mfranke
 *
 */
@Unused
public class PartitionHash implements BitSetHashFunction<BitSet>{

	private static final long serialVersionUID = 5440718490349603546L;

	private int startIndex;
	private int endIndex;
	
	/**
	 * Creates a new {@link PartitionHash} object.
	 * 
	 * @param startIndex
	 * 		-> defines the start index of the partition to select from a bit set.
	 * 
	 * @param endIndex
	 * 		-> defines the end index of the partition to select from a bit set.
	 */
	public PartitionHash(int startIndex, int endIndex) {
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}
	
	/**
	 * Hashes a bit set into a {@link BitSet} hash value by
	 * taking the specified partition from the given bit set.
	 */
	@Override
	public BitSet hash(BitSet bitset) {
		if (this.endIndex >= bitset.cardinality()){
			return null;
		}
		else{
			return bitset.get(startIndex, endIndex);	
		}
	}

}