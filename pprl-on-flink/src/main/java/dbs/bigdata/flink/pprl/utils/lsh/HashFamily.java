package dbs.bigdata.flink.pprl.utils.lsh;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

/**
 * Class that represents a hash family for {@link BitSetHashFunction}s.
 * A hash family is a set of similar hash functions.
 * 
 * @author mfranke
 *
 * @param <T>
 * 		-> the type of hash functions. The type has to imlement to {@link BitSetHashFunction} 
 * 		   interface.
 *
 * @param <U>
 * 		-> the return type of the hash functions.
 */
public class HashFamily<T extends BitSetHashFunction<U>, U> implements Serializable {

	private static final long serialVersionUID = 7730577349251809858L;
	private List<T> hashFunctions;
	
	/**
	 * Creates a new {@link HashFamily} object.
	 */
	public HashFamily(){
		this.hashFunctions = new ArrayList<T>();
	}
	
	/**
	 * Creates a new {@link HashFamily} object.
	 * 
	 * @param hashFunctions
	 * 		-> a {@link List} of hash functions from type T.
	 */
	public HashFamily(List<T> hashFunctions){
		this.hashFunctions = hashFunctions;
	}
	
	public List<T> getHashFunctions(){
		return this.hashFunctions;
	}
	
	public void setHashFunctions(List<T> hashFunctions){
		this.hashFunctions = hashFunctions;
	}
	
	public void addHashFunction(T hashFunction){
		this.hashFunctions.add(hashFunction);
	}
	
	public int getNumberOfHashFunctions(){
		return this.hashFunctions.size();
	}
	
	/**
	 * Calculates the hash values for all hash functions in this hash family.
	 * 
	 * @param bitset
	 * 		-> the {@link BitSet} object to hash.
	 * 
	 * @return
	 * 		-> {@link List} of hash values U.
	 */
	public List<U> calculateHashes(BitSet bitset){
		List<U> hashes = new ArrayList<U>();
		
		for (int i = 0; i < this.hashFunctions.size(); i++){
			U hashValue = this.hashFunctions.get(i).hash(bitset);
			hashes.add(hashValue);
		}
		
		return hashes;
	}
	

	/**
	 * Generates a family of {@link IndexHash} functions with random indices.
	 * 
	 * @param numberOfHashesPerFamily
	 * 		-> count of hash functions in this hash family.
	 * 
	 * @param valueRange
	 * 		-> the range in which the positions / indices should be selected.
	 * 
	 * @return
	 * 		-> the generated index hash family.
	 */
	public static HashFamily<IndexHash, Boolean> generateRandomIndexHashFamily(
			int numberOfHashesPerFamily, int valueRange) {
		HashFamily<IndexHash, Boolean> hashFamily = 
				new HashFamily<IndexHash, Boolean>();
		
		Random rnd = new Random();
		ArrayList<Integer> indices = new ArrayList<Integer>();
		
		for (int i = 0; i < numberOfHashesPerFamily; i++){
			Integer randomIndex = rnd.nextInt(valueRange);
			
			while (indices.contains(randomIndex)){
				randomIndex = rnd.nextInt(valueRange);
			}
			
			indices.add(randomIndex);
			
			IndexHash indexHash = new IndexHash(randomIndex);

			hashFamily.addHashFunction(indexHash);
		}
		
		return hashFamily;
	}
}