package dbs.bigdata.flink.pprl.job.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import dbs.bigdata.flink.pprl.utils.common.DataLoader;

/**
 * Preprocessor that reads the content of multiple csv-files.
 * Each source file represents an other data source and therefore
 * is assigned to a dataset identification. Each row is transformed
 * into a {@link InputTuple} and later parsed into a {@link LinkageTuple}.
 * Every such input value is related to a person. The objects from
 * the different sources are merged into a {@link DataSet} which
 * is then processed.
 * 
 * @author mfranke
 *
 */
public class LinkageInputPreProcessor {

	private final ExecutionEnvironment env;
	private String[] dataFilePaths;
	private String lineDelimiter;
	private String fieldDelimiter;
	private String includingFields;
	private String[] fieldNames;
	
	
	public LinkageInputPreProcessor(ExecutionEnvironment env){
		this.env = env;
	}
	
	public LinkageInputPreProcessor(ExecutionEnvironment env, String lineDelimiter,
			String fieldDelimiter, String includingFields, String[] fieldNames, String... dataFilePaths){
		this.env = env;
		this.dataFilePaths = dataFilePaths;
		this.lineDelimiter = lineDelimiter;
		this.fieldDelimiter = fieldDelimiter;
		this.includingFields = includingFields;
		this.fieldNames = fieldNames;
	}
	
	public DataSet<LinkageTuple> getLinkageTuples() throws Exception{
		DataSet<InputTuple> bloomFilter = loadAndUnionData();
		DataSet<LinkageTuple> bfData = bloomFilter.map(new InputTupleToLinkageTupleMapper());
		return bfData;
	}
	
	public DataSet<LinkageTupleWithBlockingKey> getLinkageTuplesWithBlockingKey() throws Exception{
		DataSet<InputTuple> bloomFilter = loadAndUnionData();
		DataSet<LinkageTupleWithBlockingKey> bfData = bloomFilter.map(new InputTupleToLinkageTupleWithBlockingKeyMapper());
		return bfData;
	}
	
	public DataSet<Tuple1<CandidateLinkageTuple>> getLinkageTuplesAsCandidates() throws Exception{
		DataSet<Tuple2<InputTuple, InputTuple>> bloomFilter = this.loadAndCrossData();
		return bloomFilter.map(new CrossedInputTupleToCanditeLinkageTupleMapper());
	}
	
	private DataSet<InputTuple> loadAndUnionData(){
		List<DataSet<InputTuple>> allData = this.loadInputData();
		
		DataSet<InputTuple> bfInputData = allData.get(0);
		
		for (int i = 1; i < allData.size(); i++){
			bfInputData = bfInputData.union(allData.get(i));
		}
		
		return bfInputData;
	}
	
	private DataSet<Tuple2<InputTuple, InputTuple>> loadAndCrossData(){
		List<DataSet<InputTuple>> allData = this.loadInputData();
		
		DataSet<Tuple2<InputTuple, InputTuple>> crossedData = allData.get(0).cross(allData.get(1));
				
		return crossedData;
	}
	
	private List<DataSet<InputTuple>> loadInputData(){
		List<DataSet<InputTuple>> allData = new ArrayList<DataSet<InputTuple>>();
		
		DataLoader<InputTuple> bfLoader = 
			new DataLoader<InputTuple>(
				InputTuple.class, 
				this.env,
				null,
				this.lineDelimiter,
				this.fieldDelimiter,
				this.includingFields,
				this.fieldNames,
				false
			);

		
		for (int i = 0; i < this.dataFilePaths.length; i++){
			bfLoader.setDataFilePath(this.dataFilePaths[i]);
			
			DataSet<InputTuple> data = bfLoader.getData();
			
			data = data.map(new BloomFilterInputDatasetIdAdder(String.valueOf(i)));
			
			allData.add(data);
		}
		
		return allData;
	}
}