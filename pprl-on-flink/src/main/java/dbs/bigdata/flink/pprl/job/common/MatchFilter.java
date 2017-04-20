package dbs.bigdata.flink.pprl.job.common;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * Class for evaluating the found matches into
 * true matches and false matches. The accumulators
 * calculated it thereby are used for evaluation of
 * the different jobs.
 * 
 * @author mfranke
 *
 */
public class MatchFilter extends RichFilterFunction<LinkageResult> {

	private static final long serialVersionUID = -2111757275840761150L;

	private boolean trueMatches;
	private LongCounter trueMatchCounter = new LongCounter();;
	private LongCounter falseMatchCounter = new LongCounter();;
	
	public MatchFilter(boolean trueMatches){
		this.trueMatches = trueMatches;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		final RuntimeContext context = getRuntimeContext();
		context.addAccumulator("true-match-counter", this.trueMatchCounter);
		context.addAccumulator("false-match-counter", this.falseMatchCounter);
	}
	
	@Override
	public boolean filter(LinkageResult value) throws Exception {
		final boolean sameId = value.getTuple1().getId().equals(value.getTuple2().getId());
		final boolean sameDatasetId = value.getTuple1().getDataSetId().equals(value.getTuple2().getDataSetId());
		
		if (sameId && !sameDatasetId){
			this.trueMatchCounter.add(1L);
			return trueMatches;
		}
		else{
			this.falseMatchCounter.add(1L);
			return !trueMatches;
		}
	}
}