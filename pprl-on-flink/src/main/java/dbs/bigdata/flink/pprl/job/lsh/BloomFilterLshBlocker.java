package dbs.bigdata.flink.pprl.job.lsh;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.job.common.LinkageTuple;
import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;
import dbs.bigdata.flink.pprl.utils.lsh.HashFamilyGroup;
import dbs.bigdata.flink.pprl.utils.lsh.IndexHash;
import dbs.bigdata.flink.pprl.utils.lsh.Lsh;

/**
 * Class for building blocks from the bloom filter.
 * Therefore the LshBlocker hashes pieces of the bloom filter
 * and generates a blocking key. 
 * 
 * @author mfranke
 *
 */
public class BloomFilterLshBlocker 
	implements FlatMapFunction<LinkageTuple, Tuple2<LshKey, LinkageTupleWithLshKeys>>{

	
	private static final long serialVersionUID = 5273583064581285374L;

	private HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup;
	
	public BloomFilterLshBlocker(HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup){
		this.hashFamilyGroup = hashFamilyGroup;
	}
	
	/**
	 * Transformation of (Id, {@link BloomFilter}) tuples
	 * into (KeyId, KeyValue, {@link LinkageTupleWithLshKeys}).
	 * This transformation executes the first blocking step.
	 */
	@Override
	public void flatMap(LinkageTuple value, Collector<Tuple2<LshKey, LinkageTupleWithLshKeys>> out) throws Exception {
					
		final Lsh<IndexHash> lsh = new Lsh<IndexHash>(value.getBloomFilter(), this.hashFamilyGroup);
		final LshKey[] lshKeys = lsh.calculateKeys();
		
		LinkageTupleWithLshKeys bfWithKeys = new LinkageTupleWithLshKeys(value, lshKeys);
		
		for (int i = 0; i < lshKeys.length; i++){
			out.collect(
				new Tuple2<LshKey, LinkageTupleWithLshKeys>(
					lshKeys[i],
					bfWithKeys
				)
			);
		}
	}
}