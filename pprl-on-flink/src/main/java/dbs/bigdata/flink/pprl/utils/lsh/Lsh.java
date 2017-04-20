package dbs.bigdata.flink.pprl.utils.lsh;

import java.util.BitSet;
import java.util.List;

import dbs.bigdata.flink.pprl.job.lsh.LshKey;
import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class, which implements the LSH for blocking.
 * 
 * @author mfranke
 * 
 * @param <T>
 * 		-> the type of hash function which is a {@link BitSetHashFunction}
 * 		   that returns a boolean value.
 */
public class Lsh<T extends BitSetHashFunction<Boolean>>{
	
	private BloomFilter bloomFilter;
	
	private HashFamilyGroup<T, Boolean> hashFamilyGroup;
	
	
	/**
	 * Creates a new {@link Lsh} object.
	 * 
	 * @param bloomFilter
	 * 		-> the bloom filter to be blocked.
	 * 
	 * @param hashFamilyGroup
	 * 		-> a group a hash families which are used to build the blocking keys for the
	 * 		   bloom filter.
	 */
	public Lsh(BloomFilter bloomFilter, HashFamilyGroup<T, Boolean> hashFamilyGroup){
		this.bloomFilter = bloomFilter;
		this.hashFamilyGroup = hashFamilyGroup;
	}
	
	/**
	 * Calculates the blocking keys for the specified bloom filter for the
	 * defined hash family group.
	 * 
	 * @return
	 * 		-> the blocking keys in form of a {@link BitSet} array.
	 */
	public LshKey[] calculateKeys(){
		int keyCount = hashFamilyGroup.getNumberOfHashFamilies();
		LshKey[] keys = new LshKey[keyCount];
		
		List<List<Boolean>> hashValues = this.hashFamilyGroup.calculateHashes(this.bloomFilter.getBitset());
		
		for (int i = 0; i < hashValues.size(); i++){
			keys[i] = this.calculateKey(hashValues.get(i), i);
		}
				
		return keys;
	}
	
	private LshKey calculateKey(List<Boolean> hashValues, Integer id){
		int hashCount = hashValues.size();
		
		BitSet key = new BitSet(hashValues.size());
		
		for (int i = 0; i < hashCount; i++){
			Boolean hash = hashValues.get(i);
			key.set(i, hash);
		}
		
		final LshKey lshKey = new LshKey(id, key);
		
		return lshKey;
	}
	
}