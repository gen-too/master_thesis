package dbs.bigdata.flink.pprl.job.common;

import java.util.Collection;
import java.util.HashMap;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * Flink Accumulator that tracks the number of records contained in
 * the input files. This value is used for evaluation of the jobs.
 * @author mfranke
 *
 */
public class RecordTupleCounter implements Accumulator<String, Long> {

	private static final long serialVersionUID = 2344451081382365581L;

	private HashMap<String, Long> internalValue;
	
	public RecordTupleCounter() {
		this.internalValue = new HashMap<String, Long>();
	}
	
	@Override
	public void add(String value) {
		this.internalValue.merge(value, 1L, (x, y) -> { return x + y; });
	}

	@Override
	public Long getLocalValue() {
		final Collection<Long> values = this.internalValue.values();
		
		Long result = 1L;
		for (Long value : values){
			result *= value;
		}
		
		return result;
	}

	@Override
	public void resetLocal() {
		this.internalValue.clear();		
	}

	@Override
	public void merge(Accumulator<String, Long> other) {
		if (other instanceof RecordTupleCounter){
			final RecordTupleCounter otherCounter = (RecordTupleCounter) other;
			final HashMap<String, Long> otherMap = otherCounter.internalValue;
			
			otherMap.forEach(
					(k, v) -> this.internalValue.merge(k, v, (x, y) -> { return x + y; }));
		}
	}

	@Override
	public Accumulator<String, Long> clone() {
		RecordTupleCounter result = new RecordTupleCounter();
		result.internalValue = new HashMap<String, Long>(this.internalValue);
		return result;
	}
}