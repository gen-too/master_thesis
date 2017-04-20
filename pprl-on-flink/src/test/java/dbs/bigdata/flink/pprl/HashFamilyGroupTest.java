package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.utils.lsh.HashFamily;
import dbs.bigdata.flink.pprl.utils.lsh.HashFamilyGroup;
import dbs.bigdata.flink.pprl.utils.lsh.IndexHash;

/**
 * Class for testing the {@link HashFamilyGroup} implementation.
 * 
 * @author mfranke
 *
 */
public class HashFamilyGroupTest {

	private HashFamilyGroup<IndexHash, Boolean> hashGroup;
	private int numberOfFamilies = 10;
	private int numberOfHashesPerFamily = 100;
	private int valueRange = 1000;
	
	
	@Before
	public void init(){
		this.hashGroup = new HashFamilyGroup<IndexHash, Boolean>();
	}
	
	@Test
	public void test() {
		assertEquals(0, this.hashGroup.getNumberOfHashFamilies());
		
		this.hashGroup.addHashFamily(HashFamily.generateRandomIndexHashFamily(10, 1000));
		this.hashGroup.addHashFamily(HashFamily.generateRandomIndexHashFamily(10, 1000));
		
		assertEquals(2, this.hashGroup.getNumberOfHashFamilies());
	}
	
	@Test
	public void testRandomGenerationOfAHashFamilyGroup(){
		this.hashGroup = HashFamilyGroup.generateRandomIndexHashFamilyGroup(
				numberOfFamilies, numberOfHashesPerFamily, valueRange);
		
		assertEquals(numberOfFamilies, this.hashGroup.getNumberOfHashFamilies());
		
		List<HashFamily<IndexHash, Boolean>> hashFamilyList = this.hashGroup.getHashFamilies();
		assertEquals(numberOfFamilies, hashFamilyList.size());
		
		for (int i = 0; i < hashFamilyList.size(); i++){
			assertEquals(numberOfHashesPerFamily, hashFamilyList.get(i).getNumberOfHashFunctions());
		}
	}
	
	@Test
	public void testCalculateHashValues(){
		this.hashGroup = HashFamilyGroup.generateRandomIndexHashFamilyGroup(
				numberOfFamilies, numberOfHashesPerFamily, valueRange);
		
		BitSet bitset = new BitSet(this.valueRange);
		bitset.set(35, 64);
		bitset.set(123);
		bitset.set(245);
		
		List<List<Boolean>> hashValues = this.hashGroup.calculateHashes(bitset);
		
		assertNotNull(hashValues);
		
		assertEquals(this.numberOfFamilies, hashValues.size());
		
		for (int i = 0; i < hashValues.size(); i++){
			List<Boolean> hashListAtFamily = hashValues.get(i);
			assertEquals(this.numberOfHashesPerFamily, hashListAtFamily.size());
		}
	}

}	
