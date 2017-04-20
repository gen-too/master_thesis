package dbs.bigdata.flink.pprl.preprocessing;

import java.lang.reflect.Field;

import org.apache.commons.codec.StringEncoder;
import org.apache.commons.codec.language.Soundex;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class for calculating the phonetic key of a certain attribute value for
 * a person. This phonetic key can later be used for blocking.
 * 
 * @author mfranke
 *
 */
public class PhoneticEncoder extends RichMapFunction<Tuple3<String, Person, BloomFilter>, Tuple4<String, Person, String, BloomFilter>> {

	public static final String ENCODING_PARAMETER = "codec";
	public static final String SOUNDEX_CODING = "org.apache.commons.codec.language.Soundex";
	public static final String METAPHONE_CODING = "org.apache.commons.codec.language.Metaphone";
	public static final String DOUBLE_METAPHONE_CODING = "org.apache.commons.codec.language.DoubleMetaphone";
	
	private static final long serialVersionUID = 3901784140203774158L;

	private StringEncoder codec;
	private String fieldName;
	
	/**
	 * Creates a new {@link PhoneticEncoder}.
	 * @param fieldName the {@link Person} field (attribute) that should be encoded phonetic. 
	 */
	public PhoneticEncoder(String fieldName){
		this.fieldName = fieldName;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {		
		Class<StringEncoder> c = parameters.getClass(ENCODING_PARAMETER, Soundex.class, StringEncoder.class.getClassLoader());
		this.codec = c.newInstance();
	}
	
	@Override
	public Tuple4<String, Person, String, BloomFilter> map(Tuple3<String, Person, BloomFilter> value) throws Exception {
		Field field = Person.class.getDeclaredField(this.fieldName);
		field.setAccessible(true);
		String fieldValue = (String) field.get(value.f1);
		
		final String phoneticCode = codec.encode(fieldValue);
		
		return new Tuple4<String, Person, String, BloomFilter>(
				value.f0,
				value.f1,
				phoneticCode,
				value.f2
		);
	}

	public static Configuration createEncodingConfiguration(String codec) throws ClassNotFoundException{
		Configuration config = new Configuration();
		Class<?> encodingClass = Class.forName(codec);
		config.setClass(ENCODING_PARAMETER, encodingClass);
		return config;
	}
	
	public static Configuration createEncodingConfiguration(Class<?> encodingClass){
		Configuration config = new Configuration();
		config.setClass(ENCODING_PARAMETER, encodingClass);
		return config;
	}
}