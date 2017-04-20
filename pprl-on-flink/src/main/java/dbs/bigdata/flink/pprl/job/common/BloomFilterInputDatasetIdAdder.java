package dbs.bigdata.flink.pprl.job.common;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * Map function that adds a data set identification to each {@link InputTuple}
 * for saving the relationship of each such tuple to its data source.
 * Furthermore two accumulators count the number of records for each data set.
 * 
 * @author mfranke
 *
 */
public class BloomFilterInputDatasetIdAdder extends RichMapFunction<InputTuple, InputTuple> {

	private static final long serialVersionUID = -5042423733488788461L;

	private String dataSetIdentifier;
	private LongCounter recordCounter = new LongCounter(); 
	private RecordTupleCounter recordTupleCounter = new RecordTupleCounter();
	
	public BloomFilterInputDatasetIdAdder(String dataSetIdentifier){
		this.dataSetIdentifier = dataSetIdentifier;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		final RuntimeContext context = getRuntimeContext();
		context.addAccumulator("record-counter", this.recordCounter);
		context.addAccumulator("record-tuple-counter", this.recordTupleCounter);
	}
	
	@Override
	public InputTuple map(InputTuple value) throws Exception {
		this.recordCounter.add(1L);
		this.recordTupleCounter.add(this.dataSetIdentifier);
		value.setDataSetId(this.dataSetIdentifier);
		return value;
	}

}