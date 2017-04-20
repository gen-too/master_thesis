package dbs.bigdata.flink.pprl.job.phonetic;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dbs.bigdata.flink.pprl.job.common.CandidateLinkageTuple;
import dbs.bigdata.flink.pprl.job.common.LinkageInputPreProcessor;
import dbs.bigdata.flink.pprl.job.common.LinkageResult;
import dbs.bigdata.flink.pprl.job.common.LinkageTupleWithBlockingKey;
import dbs.bigdata.flink.pprl.job.common.MatchFilter;
import dbs.bigdata.flink.pprl.job.common.PpprlJob;
import dbs.bigdata.flink.pprl.job.common.SimilarityCalculater;

/**
 * Pprl job that uses phonetic blocking.
 * 
 * @author mfranke
 */
public class PhoneticPpprlJob extends PpprlJob{
		
	private static final String OUTPUT_PREFIX = PhoneticPpprlJob.class.getSimpleName() + "/";
	
	private final Logger log = LoggerFactory.getLogger(PhoneticPpprlJob.class);
	
	public PhoneticPpprlJob(Configuration config){
		super(config);	
	}
	
	public PhoneticPpprlJob(Configuration config, String jobName, String[] dataFilePaths, String lineDelimiter, String fieldDelimiter, String includingFields, String[] fieldNames,
			double comparisonThreshold, String outputPath) {
		super(config, jobName, dataFilePaths, lineDelimiter, fieldDelimiter, includingFields, fieldNames, comparisonThreshold, outputPath);
	}
		
	public JobExecutionResult runJob(int iteration) throws Exception{
		log.info("run job");
		
		LinkageInputPreProcessor bfProcessor = 
				new LinkageInputPreProcessor(
						this.env, 					
						this.lineDelimiter, 
						this.fieldDelimiter,
						this.includingFields,
						this.fieldNames,
						this.dataFilePaths
				);
		
		final String datasetPathExtension = this.getDataFileHash() + "/";
		final String ouput = this.outputPath + OUTPUT_PREFIX + datasetPathExtension;
		
		
		DataSet<LinkageTupleWithBlockingKey> linkageTuples = bfProcessor.getLinkageTuplesWithBlockingKey();		
		
		DataSet<Tuple1<CandidateLinkageTuple>> candidates = buildCandidatePairs(linkageTuples);
		
		
		DataSet<LinkageResult> matchingPairs = this.calculateSimilarity(candidates);
		matchingPairs.writeAsText(ouput + "matchingPairs", WriteMode.OVERWRITE);

		
		DataSet<LinkageResult> trueMatches = matchingPairs.filter(new MatchFilter(true));
		trueMatches.writeAsText(ouput + "trueMatches", WriteMode.OVERWRITE);
		
		
		return this.env.execute(this.getJobName()+ "#" + iteration);		
	}
	
	private DataSet<Tuple1<CandidateLinkageTuple>> buildCandidatePairs(DataSet<LinkageTupleWithBlockingKey> linkageTuples){
		return linkageTuples
			.groupBy(new KeySelector<LinkageTupleWithBlockingKey, long[]>(){
				private static final long serialVersionUID = 5757734615244675456L;
	
				@Override
				public long[] getKey(LinkageTupleWithBlockingKey value) throws Exception {
					return value.getBlockingKey().getBitset().toLongArray();
				}
				
			})
			.reduceGroup(new CandidateBuilder());
	}
	
	private DataSet<LinkageResult> calculateSimilarity(DataSet<Tuple1<CandidateLinkageTuple>> data){
		return data.flatMap(new SimilarityCalculater<CandidateLinkageTuple>(this.comparisonThreshold));
	}
}	