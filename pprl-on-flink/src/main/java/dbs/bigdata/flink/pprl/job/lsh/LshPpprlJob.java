package dbs.bigdata.flink.pprl.job.lsh;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dbs.bigdata.flink.pprl.job.common.LinkageInputPreProcessor;
import dbs.bigdata.flink.pprl.job.common.LinkageResult;
import dbs.bigdata.flink.pprl.job.common.LinkageTuple;
import dbs.bigdata.flink.pprl.job.common.MatchFilter;
import dbs.bigdata.flink.pprl.job.common.PpprlJob;
import dbs.bigdata.flink.pprl.job.common.SimilarityCalculater;
import dbs.bigdata.flink.pprl.utils.lsh.HashFamilyGroup;
import dbs.bigdata.flink.pprl.utils.lsh.IndexHash;

/**
 * Pprl job that used lsh as blocking method.

 * @author mfranke
 */
public class LshPpprlJob extends PpprlJob{
	
	private static final String OUTPUT_PREFIX = LshPpprlJob.class.getSimpleName() + "/";
	
	private final Logger log = LoggerFactory.getLogger(LshPpprlJob.class);
	
	private int valueRangeLsh;
	private int numberOfHashFamilies;
	private int numberOfHashesPerFamily;
	
	public LshPpprlJob(Configuration config){
		super(config);
	}
	
	public LshPpprlJob(Configuration config, String jobName, String[] dataFilePaths, String lineDelimiter, String fieldDelimiter, String includingFields, String[] fieldNames,
			int valueRangeLsh, int numberOfHashFamilies, int numberOfHashesPerFamily, double comparisonThreshold, 
			String outputPath) {
		super(config, jobName, dataFilePaths, lineDelimiter, fieldDelimiter, includingFields, fieldNames, comparisonThreshold, outputPath);
		this.valueRangeLsh = valueRangeLsh;
		this.numberOfHashFamilies = numberOfHashFamilies;
		this.numberOfHashesPerFamily = numberOfHashesPerFamily;
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
		
		DataSet<LinkageTuple> linkageTuples = bfProcessor.getLinkageTuples();		
		
		DataSet<Tuple2<LshKey, LinkageTupleWithLshKeys>> keyBloomFilterPairs = calculateLshKeys(linkageTuples);			
		
		DataSet<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> keysWithCandidatePair = blockWithLshKeys(keyBloomFilterPairs);
			
		DataSet<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> distinctCandidatePairs = removeDuplicateCandidates(keysWithCandidatePair);
				
		
		DataSet<Tuple1<CandidateLinkageTupleWithLshKeys>> candidates = distinctCandidatePairs.project(1);
		DataSet<LinkageResult> matchingPairs = calculateSimilarity(candidates);
		matchingPairs.writeAsText(ouput +"matchingPairs", WriteMode.OVERWRITE);
		
		
		DataSet<LinkageResult> trueMatches = matchingPairs.filter(new MatchFilter(true));
		trueMatches.writeAsText(ouput + "trueMatches", WriteMode.OVERWRITE);
		
		return this.env.execute(this.getJobName()+ "#" + iteration);	
	}

	private DataSet<Tuple2<LshKey, LinkageTupleWithLshKeys>> calculateLshKeys(DataSet<LinkageTuple> data){
		// build blocks (use LSH for this) of bloom filters, where matches are supposed
				
		HashFamilyGroup<IndexHash, Boolean> hashFamilyGroup = 
				HashFamilyGroup.generateRandomIndexHashFamilyGroup(
						this.numberOfHashFamilies,
						this.numberOfHashesPerFamily,
						this.valueRangeLsh
				);
				
		return data.flatMap(new BloomFilterLshBlocker(hashFamilyGroup));		
	}
	
	
	private DataSet<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> blockWithLshKeys(DataSet<Tuple2<LshKey, LinkageTupleWithLshKeys>> data){
		return data.groupBy("f0.id", "f0.bits").reduceGroup(new BlockReducer());
	}
	
	
	private DataSet<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> removeDuplicateCandidates(
			DataSet<Tuple2<LshKey, CandidateLinkageTupleWithLshKeys>> data){
		return data.filter(new DuplicateCandidateRemover());
	}
	
	
	private DataSet<LinkageResult> calculateSimilarity(DataSet<Tuple1<CandidateLinkageTupleWithLshKeys>> data){
		return data.flatMap(new SimilarityCalculater<CandidateLinkageTupleWithLshKeys>(this.comparisonThreshold));
	}

	public int getValueRangeLsh() {
		return valueRangeLsh;
	}

	public void setValueRangeLsh(int valueRangeLsh) {
		this.valueRangeLsh = valueRangeLsh;
	}

	public int getNumberOfHashFamilies() {
		return numberOfHashFamilies;
	}

	public void setNumberOfHashFamilies(int numberOfHashFamilies) {
		this.numberOfHashFamilies = numberOfHashFamilies;
	}

	public int getNumberOfHashesPerFamily() {
		return numberOfHashesPerFamily;
	}

	public void setNumberOfHashesPerFamily(int numberOfHashesPerFamily) {
		this.numberOfHashesPerFamily = numberOfHashesPerFamily;
	}
}	