package dbs.bigdata.flink.pprl.job.common;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class CrossedInputTupleToCanditeLinkageTupleMapper extends RichMapFunction<Tuple2<InputTuple, InputTuple>, Tuple1<CandidateLinkageTuple>> {

	private static final long serialVersionUID = -7329202076169046050L;

	private LongCounter candidateCounter = new LongCounter();
	
	@Override
	public void open(Configuration parameters) throws Exception {
		getRuntimeContext().addAccumulator("candidate-counter", this.candidateCounter);
	}

	@Override
	public Tuple1<CandidateLinkageTuple> map(Tuple2<InputTuple, InputTuple> value) throws Exception {
		final InputTuple input1 = value.f0;
		final InputTuple input2 = value.f1;
		
		final LinkageTuple output1 = LinkageTuple.from(input1);
		final LinkageTuple output2 = LinkageTuple.from(input2);
		
		this.candidateCounter.add(1L);
		
		return new Tuple1<CandidateLinkageTuple>(new CandidateLinkageTuple(output1, output2));
	}
}