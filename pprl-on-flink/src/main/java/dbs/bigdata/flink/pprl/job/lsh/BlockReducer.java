package dbs.bigdata.flink.pprl.job.lsh;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * GroupReducer for building candidate pairs that have the same value for a specific {@link LshKey}.
 * @author mfranke
 *
 */
public class BlockReducer implements GroupReduceFunction<Tuple2<LshKey, LinkageTupleWithLshKeys>, 
			Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> {

	
	private static final long serialVersionUID = -788582704739580670L;


	@Override
	public void reduce(Iterable<Tuple2<LshKey, LinkageTupleWithLshKeys>> values,
			Collector<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> out) throws Exception {
		
		ArrayList<Tuple2<LshKey, LinkageTupleWithLshKeys>> valueList = this.cacheValues(values);
		
		LshKey lshKey = valueList.get(0).f0;
			
		for (int i = 0; i < valueList.size(); i++){
			for (int j = i + 1; j < valueList.size(); j++){
				
				Tuple2<LshKey, LinkageTupleWithLshKeys> first = valueList.get(i);
				Tuple2<LshKey, LinkageTupleWithLshKeys> second = valueList.get(j);
				
				final LinkageTupleWithLshKeys firstTuple = first.f1;
				final LinkageTupleWithLshKeys secondTuple = second.f1;
				
				if(!firstTuple.dataSetIdEquals(secondTuple)){
//					final LshKey firstLshKey = first.f0;
//					final LshKey secondLshKey = second.f0;
					
//					if (firstLshKey.equals(secondLshKey)){
						CandidateLinkageTupleWithLshKeys candidate =
								new CandidateLinkageTupleWithLshKeys(firstTuple, secondTuple);
						
						out.collect(new Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>(lshKey, candidate));
//					}	
				}
			}
		}
	}

	private ArrayList<Tuple2<LshKey, LinkageTupleWithLshKeys>> cacheValues(
			Iterable<Tuple2<LshKey, LinkageTupleWithLshKeys>> values){
		
		ArrayList<Tuple2<LshKey, LinkageTupleWithLshKeys>> result = 
			new ArrayList<Tuple2<LshKey, LinkageTupleWithLshKeys>>();
		
		for (Tuple2<LshKey, LinkageTupleWithLshKeys> value : values) {
			result.add(value);
		}
		
		return result;
	}
}