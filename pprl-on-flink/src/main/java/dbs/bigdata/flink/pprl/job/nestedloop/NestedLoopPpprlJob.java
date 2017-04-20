package dbs.bigdata.flink.pprl.job.nestedloop;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dbs.bigdata.flink.pprl.job.common.CandidateLinkageTuple;
import dbs.bigdata.flink.pprl.job.common.LinkageInputPreProcessor;
import dbs.bigdata.flink.pprl.job.common.LinkageResult;
import dbs.bigdata.flink.pprl.job.common.MatchFilter;
import dbs.bigdata.flink.pprl.job.common.PpprlJob;
import dbs.bigdata.flink.pprl.job.common.SimilarityCalculater;

/**
 * Standard pprl job where no blocking method is used.
 * Instead all possible bloom filter pairs are build
 * and compared.
 * 
 * @author mfranke
 *
 */
public class NestedLoopPpprlJob extends PpprlJob{
	
	private static final String OUTPUT_PREFIX = NestedLoopPpprlJob.class.getSimpleName() + "/";
	
	private final Logger log = LoggerFactory.getLogger(NestedLoopPpprlJob.class);
	
	public NestedLoopPpprlJob(Configuration config) {
		super(config);
	}

	public NestedLoopPpprlJob(Configuration config, String jobName, String[] dataFilePaths, String lineDelimiter, String fieldDelimiter,
			String includingFields, String[] fieldNames, double comparisonThreshold, String outputPath) {
		super(config, jobName, dataFilePaths, lineDelimiter, fieldDelimiter, includingFields, fieldNames, comparisonThreshold, outputPath);
	}

	@Override
	public JobExecutionResult runJob(int iteration) throws Exception {
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
		
		
		DataSet<Tuple1<CandidateLinkageTuple>> candidates = bfProcessor.getLinkageTuplesAsCandidates();
		
		
		DataSet<LinkageResult> matchingPairs = this.calculateSimilarity(candidates);
		matchingPairs.writeAsText(ouput + "matchingPairs", WriteMode.OVERWRITE);

		
		DataSet<LinkageResult> trueMatches = matchingPairs.filter(new MatchFilter(true));
		trueMatches.writeAsText(ouput + "trueMatches", WriteMode.OVERWRITE);
		
		
		return this.env.execute(this.getJobName()+ "#" + iteration);
	}
	
	private DataSet<LinkageResult> calculateSimilarity(DataSet<Tuple1<CandidateLinkageTuple>> data){
		return data.flatMap(new SimilarityCalculater<CandidateLinkageTuple>(this.comparisonThreshold));
	}
}