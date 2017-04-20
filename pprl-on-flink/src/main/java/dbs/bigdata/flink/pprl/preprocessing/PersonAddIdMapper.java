package dbs.bigdata.flink.pprl.preprocessing;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Adds to each person an id and a data set identifier
 * (if not declared).
 * 
 * @author mfranke
 */
public class PersonAddIdMapper implements MapFunction<Person, Person> {

	private static final long serialVersionUID = 1874396153396624032L;

	private String dataSetIdentifier;
	
	/**
	 * Returns a new AddIdMapper object.
	 */
	public PersonAddIdMapper(){
		this("");
	}
	
	/**
	 * Returns a new AddIdMapper object.
	 *
	 * @param dataSetIdentifier the identifier for objects of the same data set.
	 */ 
	public PersonAddIdMapper(String dataSetIdentifier){
		this.dataSetIdentifier = dataSetIdentifier;
	}
	
	/**
	 * Adds an id and a data set identifier to the person object
	 * (if these attributes are not set).
	 */
	@Override
	public Person map(Person value) throws Exception {
		final String id = value.getId();
		if (id == null || id.isEmpty()){
			final String hashId = Long.toString(value.hashValue());
			value.setId(hashId);
		}
		
		final String dataSetId = value.getDataSetIdentifier();
		if (dataSetId == null || dataSetId.isEmpty()){
			value.setDataSetIdentifier(dataSetIdentifier);
		}
		return value;
	}
}