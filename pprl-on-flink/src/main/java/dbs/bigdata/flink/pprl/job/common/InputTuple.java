package dbs.bigdata.flink.pprl.job.common;

import antlr.collections.impl.BitSet;
import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class that represents a row read from a csv file.
 * This class is necessary because the {@link BloomFilter} uses
 * a {@link BitSet} which is not a valid Flink pojo. Therefore
 * it is not possible to construct the Bloom Filter objects
 * directly when the csv-file is read.
 * 
 * @author mfranke
 *
 */
public class InputTuple {

	public static final String ID_ATTRIBUTE = "id";
	public static final String DATASET_IDENTIFIER_ATTRIBUTE = "dataSetId";
	public static final String BLOOM_FILTER_ATTRIBUTE = "bloomFilterBits";
	public static final String BLOCKING_KEY_ATTRIBUTE = "blockingKeyBits";
	
	public static final int FIELDS = 4;
	
	private String id;
	private String dataSetId;
	private String bloomFilterBits;
	private String blockingKeyBits;
	
	/**
	 * Empty default constructor.
	 */
	public InputTuple(){
		this(null, null, null, null);
	}
	
	/**
	 * Creates a new {@link InputTuple}.
	 * @param id the id of the person the following data is related to.
	 * @param dataSetId the data set identifier of the tuple.
	 * @param bloomFilterBits the bits of the Bloom Filter.
	 * @param blockingKeyBits the bits of the blocking key.
	 */
	public InputTuple(String id, String dataSetId, String bloomFilterBits, String blockingKeyBits){
		this.id = id;
		this.dataSetId = dataSetId;
		this.bloomFilterBits = bloomFilterBits;
		this.blockingKeyBits = blockingKeyBits;
	}


	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("InputUnit [id=");
		builder.append(id);
		builder.append(", dataSetId=");
		builder.append(dataSetId);
		builder.append(", bloomFilterBits=");
		builder.append(bloomFilterBits);
		builder.append(", blockingKeyBits=");
		builder.append(blockingKeyBits);
		builder.append("]");
		return builder.toString();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getDataSetId() {
		return dataSetId;
	}

	public void setDataSetId(String dataSetId) {
		this.dataSetId = dataSetId;
	}

	public String getBloomFilterBits() {
		return bloomFilterBits;
	}

	public void setBloomFilterBits(String bloomFilterBits) {
		this.bloomFilterBits = bloomFilterBits;
	}

	public String getBlockingKeyBits() {
		return blockingKeyBits;
	}

	public void setBlockingKeyBits(String blockingKeyBits) {
		this.blockingKeyBits = blockingKeyBits;
	}
}