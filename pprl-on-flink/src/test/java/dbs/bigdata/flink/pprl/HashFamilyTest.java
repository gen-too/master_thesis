package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import java.util.BitSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.utils.lsh.HashFamily;
import dbs.bigdata.flink.pprl.utils.lsh.IndexHash;

/**
 * Class for testing the {@link HashFamily} implementation.
 * 
 * @author mfranke
 *
 */
public class HashFamilyTest {

	private HashFamily<IndexHash, Boolean> hashFamily;
	private BitSet bitset;
	private int numberOfHashesPerFamily = 10;
	private int valueRange = 1000;
	
	
	@Before
	public void init(){
		this.hashFamily = new HashFamily<IndexHash, Boolean>();
		this.bitset = new BitSet(this.valueRange);
		this.bitset.set(1, 10);
		this.bitset.set(100, 1000);
	}
	
	@Test
	public void test() {
		assertNotNull(this.hashFamily);
		assertEquals(0, this.hashFamily.getNumberOfHashFunctions());
		
		IndexHash indexHash1 = new IndexHash(1);
		IndexHash indexHash2 = new IndexHash(2);
		
		this.hashFamily.addHashFunction(indexHash1);
		this.hashFamily.addHashFunction(indexHash2);
		
		assertEquals(2, this.hashFamily.getNumberOfHashFunctions());
	}

	@Test
	public void testGenerationOfRandomHashFamily(){
		this.hashFamily = HashFamily.generateRandomIndexHashFamily(numberOfHashesPerFamily, valueRange);
		
		assertNotNull(this.hashFamily);
		assertEquals(numberOfHashesPerFamily, this.hashFamily.getNumberOfHashFunctions());
		
		List<IndexHash> hashList = this.hashFamily.getHashFunctions();
		assertFalse(hashList.isEmpty());
		
		IndexHash aHashFunction = hashList.get(0);
		
		assertEquals(bitset.get(aHashFunction.getPosition()), aHashFunction.hash(this.bitset).booleanValue());
	}
	
	@Test
	public void testCalculateHashes(){
		this.hashFamily = HashFamily.generateRandomIndexHashFamily(numberOfHashesPerFamily, valueRange);
		
		List<Boolean> hashValues = this.hashFamily.calculateHashes(this.bitset);
		
		assertEquals(hashValues.size(), this.numberOfHashesPerFamily);
	}
}
