package dbs.bigdata.flink.pprl.job.phonetic;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;


import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import dbs.bigdata.flink.pprl.job.common.CandidateLinkageTuple;
import dbs.bigdata.flink.pprl.job.common.LinkageTupleWithBlockingKey;

/**
 * Reduce function implementation to build all pairs of record with the
 * same blocking key value.
 * 
 * @author mfranke
 *
 */
public class CandidateBuilder extends RichGroupReduceFunction<LinkageTupleWithBlockingKey, Tuple1<CandidateLinkageTuple>> {

	private static final long serialVersionUID = -4774714857455001825L;

	private LongCounter candidateCounter = new LongCounter();
	
	@Override
	public void open(Configuration parameters) throws Exception {
		getRuntimeContext().addAccumulator("candidate-counter", this.candidateCounter);
	}
	
	@Override
	public void reduce(Iterable<LinkageTupleWithBlockingKey> values,
			Collector<Tuple1<CandidateLinkageTuple>> out) throws Exception {
		
		List<LinkageTupleWithBlockingKey> list = this.cacheValues(values);
		
		for (int i = 0; i < list.size(); i++){
			final LinkageTupleWithBlockingKey first = list.get(i);
			
			for (int j = i + 1; j < list.size(); j++){
				final LinkageTupleWithBlockingKey second = list.get(j);
								
				if (!first.dataSetIdEquals(second)){
					this.candidateCounter.add(1L);
					out.collect(new Tuple1<CandidateLinkageTuple>(new CandidateLinkageTuple(first, second)));
				}
			}
		}		
	}
	
	private List<LinkageTupleWithBlockingKey> cacheValues(Iterable<LinkageTupleWithBlockingKey> values){
		List<LinkageTupleWithBlockingKey> list = new ArrayList<LinkageTupleWithBlockingKey>();
		
		for (LinkageTupleWithBlockingKey value : values){
			list.add(value);
		}
		
		return list;
	}
}