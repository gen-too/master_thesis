package dbs.bigdata.flink.pprl;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class for testing the {@link BloomFilter} implementation.
 * 
 * @author mfranke
 */
public class BloomFilterTest {

	private BloomFilter bloomFilter;
	private final int size = 100;
	private final int hashes = 4;
	
	@Before
	public void initializeBloomFilter(){
		this.bloomFilter = new BloomFilter(size, hashes);
	}
	
	@Test
	public void testAddElement(){
		assertTrue(this.bloomFilter.getBitset().isEmpty());
		int[] positions = this.bloomFilter.addElement("test");
		assertFalse(this.bloomFilter.getBitset().isEmpty());
		assertEquals(positions.length, this.bloomFilter.getHashFunctions());
	}
	
	@Test
	public void testEquals(){
		assertFalse(this.bloomFilter.equals(new BloomFilter(99,4)));
		BloomFilter otherBloomFilter = new BloomFilter(100,4);
		this.bloomFilter.addElement("foo");
		otherBloomFilter.addElement("bar");
		assertFalse(this.bloomFilter.equals(otherBloomFilter));
		otherBloomFilter.clear();
		otherBloomFilter.addElement("foo");
		assertTrue(this.bloomFilter.equals(otherBloomFilter));
	}
	
	@Test
	public void testMerge(){
		this.bloomFilter.clear();
		this.bloomFilter.addElement("foo");
		this.bloomFilter.addElement("bar");
		
		BloomFilter otherBloomFilter = new BloomFilter(10,4);
		assertNull(this.bloomFilter.merge(otherBloomFilter));
		
		otherBloomFilter = new BloomFilter(this.size, this.hashes);
		otherBloomFilter.addElement("abc");
		
		BloomFilter mergedBf = this.bloomFilter.merge(otherBloomFilter);
		assertNotNull(mergedBf);	
	}	
}