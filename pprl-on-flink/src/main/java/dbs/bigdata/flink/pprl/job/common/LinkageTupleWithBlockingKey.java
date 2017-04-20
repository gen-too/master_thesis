package dbs.bigdata.flink.pprl.job.common;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Special case of a {@link LinkageTuple} that contains an additional 
 * blocking key value which should be used in the blocking step.
 * 
 * @author mfranke
 *
 */
public class LinkageTupleWithBlockingKey extends LinkageTuple{

	protected BloomFilter blockingKey;
	
	/**
	 * Empty default constructor.
	 */
	public LinkageTupleWithBlockingKey(){
		super();
		this.blockingKey = null;
	}
	
	public LinkageTupleWithBlockingKey(String id, String dataSetId, BloomFilter bloomFilter, BloomFilter blockingKey){
		super(id, dataSetId, bloomFilter);
		this.blockingKey = blockingKey;
	}

	/**
	 * Parses a {@link InputTuple} into a {@link LinkageTupleWithBlockingKey} object.
	 * @param inputTuple the {@link InputTuple}.
	 * @return the corresponding {@link LinkageTupleWithBlockingKey}.
	 */
	public static LinkageTupleWithBlockingKey from(InputTuple inputTuple){
		LinkageTupleWithBlockingKey linkageTuple = new LinkageTupleWithBlockingKey();
		
		linkageTuple.setId(inputTuple.getId());
		linkageTuple.setDataSetId(inputTuple.getDataSetId());
		
		BloomFilter bf = BloomFilter.from(inputTuple.getBloomFilterBits());
		linkageTuple.setBloomFilter(bf);
		
		BloomFilter blockingKey = BloomFilter.from(inputTuple.getBlockingKeyBits());
		linkageTuple.setBlockingKey(blockingKey);
		
		return linkageTuple;		
	}
		
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[id=");
		builder.append(id);
		builder.append(", dId=");
		builder.append(dataSetId);
		builder.append(", bKey=");
		builder.append(blockingKey);
		builder.append(", bF=");
		builder.append(bloomFilter);
		builder.append("]");
		return builder.toString();
	}

	public BloomFilter getBlockingKey() {
		return blockingKey;
	}

	public void setBlockingKey(BloomFilter blockingKey) {
		this.blockingKey = blockingKey;
	}
}