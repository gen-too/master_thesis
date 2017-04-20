package dbs.bigdata.flink.pprl.job.lsh;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * Class for removing duplicate candidate pairs for different blocking keys.
 * More precisely if the blocking key values of two bloom filter for a blocking key smaller than
 * the "current" are equal then the candidate pair is removed. This is because the candidate
 * pair is then compared for an other key and so the comparison here would be unnecessary. 
 * 
 * @author mfranke
 *
 */
public class DuplicateCandidateRemover extends RichFilterFunction<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> {

	private static final long serialVersionUID = 47428697482093145L;

	private LongCounter candidateCounter = new LongCounter();
	
	@Override
	public void open(Configuration parameters) throws Exception {
		getRuntimeContext().addAccumulator("candidate-counter", this.candidateCounter);
	}
	
	/**
	 * Filter out duplicate candidate bloom filter pairs (pairs that are compared already in an other
	 * block (an other key)).
	 */
	@Override
	public boolean filter(Tuple2<LshKey, CandidateLinkageTupleWithLshKeys> value) throws Exception {		
		int keyId = value.f0.getId();

		LinkageTupleWithLshKeys first = value.f1.getCandidateOne();
		LinkageTupleWithLshKeys second = value.f1.getCandidateTwo();
	
		while (keyId >= 1){	
			keyId--;
			LshKey firstLshKey = first.getLshKeyAt(keyId);
			LshKey secondLshKey = second.getLshKeyAt(keyId);
			
			if (firstLshKey.equals(secondLshKey)){
				return false;
			}		
		}
		this.candidateCounter.add(1L);
		return true;	
	}
}