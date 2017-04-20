package dbs.bigdata.flink.pprl.preprocessing;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class TypedPersonAttribute {

	private String value;
	private AttributeType type;
	
	public TypedPersonAttribute(){}
	
	public TypedPersonAttribute(String value, AttributeType type){
		this.value = value;
		this.type = type;
	}
	
	public String getStringValue() throws Exception{
		if (this.type == AttributeType.STRING_TYPE){
			return value;
		}
		else{
			throw new Exception();
		}
	}
	
	public Integer getIntegerValue() throws Exception{
		if (this.type == AttributeType.NUMBER_TYPE){
			return Integer.valueOf(value);
		}
		else{
			throw new Exception();
		}
	}
	
	public LocalDate getDateValue(String pattern) throws Exception{
		if (this.type == AttributeType.DATE_TYPE){
			final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
			return LocalDate.parse(value, formatter);
		}
		else{
			throw new Exception();
		}
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public AttributeType getType() {
		return type;
	}

	public void setType(AttributeType type) {
		this.type = type;
	}
}