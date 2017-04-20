package dbs.bigdata.flink.pprl.job.common;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;



/**
 * Class for calculating similarity of a {@link CandidateLinkageTuple}.
 * 
 * @author mfranke
 *
 */
public class SimilarityCalculater<T extends CandidateLinkageTuple> extends 
	RichFlatMapFunction<Tuple1<T>, LinkageResult> {

//	private final Logger log = LoggerFactory.getLogger(SimilarityCalculater.class);
	private static final long serialVersionUID = -5284383227828157168L;
	
	private double threshold;
	private LongCounter matchCounter = new LongCounter();
	
	/**
	 * Creates a new {@link SimilarityCalculater}.
	 * @param threshold the threshold for the similarity value. 
	 * 		  A pair with a similarity above the threshold is classified as match.
	 */
	public SimilarityCalculater(double threshold){
		this.threshold = threshold;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
		getRuntimeContext().addAccumulator("match-counter", this.matchCounter);
	}
	
	/**
	 * Transformation of {@link CandidateLinkageTuple}s into a set of (Id1, Id2) tuples.
	 * These tuples are the ids of the person that matched.
	 */
	@Override
	public void flatMap(Tuple1<T> value, Collector<LinkageResult> out) throws Exception {

		final Double similarityValue = 
				// value.f0.calculateJaccardSimilarity();
				value.f0.calculateDiceSimilarity();
						
		if (similarityValue >= this.threshold){
			this.matchCounter.add(1L);
			out.collect(new LinkageResult(value.f0.getCandidateOne(), value.f0.getCandidateTwo(), similarityValue));
		}
	}
}