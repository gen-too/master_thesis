package dbs.bigdata.flink.pprl.preprocessing;

import java.lang.reflect.Field;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.StringEncoder;
import org.apache.commons.codec.language.Soundex;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class that encodes the (clean) phonetic key into a bloom filter.
 * To raise the privacy a salt value can used to make it harder
 * to get informations about the phonetic key.
 * 
 * @author mfranke
 *
 */
public class PhoneticCodeAnonymizer extends RichMapFunction<Tuple4<String, Person, String, BloomFilter>,
	Tuple3<String, String, BloomFilter>> {

	private static final long serialVersionUID = 5274869071353918513L;

	public static final String ENCODING_PARAMETER = "codec";
	public static final String SOUNDEX_CODING = "org.apache.commons.codec.language.Soundex";
	public static final String METAPHONE_CODING = "org.apache.commons.codec.language.Metaphone";
	public static final String DOUBLE_METAPHONE_CODING = "org.apache.commons.codec.language.DoubleMetaphone";
	
	private boolean withSalt;
	private String fieldName;
	private StringEncoder codec;
	private int hashFunctions;
	private int size;
	
	/**
	 * Creates a new {@link PhoneticCodeAnonymizer}.
	 * 
	 * @param withSalt boolean flag, specifying whether or not a salt value should be used.
	 * @param fieldName the name of the {@link Person} field that should be used as salt (may be null if no salt should be used).
	 * @param hashFunctions the number of hash functions that should be used.
	 * @param size the size of the resulting bloom filter.
	 */
	public PhoneticCodeAnonymizer(boolean withSalt, String fieldName, int hashFunctions, int size) {
		this.withSalt = withSalt;
		this.fieldName = fieldName;
		this.hashFunctions = hashFunctions;
		this.size = size;
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {		
		Class<StringEncoder> c = parameters.getClass(ENCODING_PARAMETER, Soundex.class, StringEncoder.class.getClassLoader());
		this.codec = c.newInstance();
	}
		
	@Override
	public Tuple3<String, String, BloomFilter> map(Tuple4<String, Person, String, BloomFilter> value)
			throws Exception {
		String phoneticCode = value.f2;

		if (this.withSalt){
			final String salt = this.calculateSalt(value.f1);
			phoneticCode = phoneticCode + salt;
		}
				
		BloomFilter anonymizedPhoneticCode = new BloomFilter(this.size, this.hashFunctions);
		
		anonymizedPhoneticCode.addElement(phoneticCode);
		
		return 
			new Tuple3<String, String, BloomFilter>(
				value.f0,
				anonymizedPhoneticCode.toString(),
				value.f3
			);
	}
	
	private String calculateSalt(Person person) throws EncoderException, NoSuchFieldException, SecurityException, 
		IllegalArgumentException, IllegalAccessException{
		Field field = Person.class.getDeclaredField(this.fieldName);
		field.setAccessible(true);
		String fieldValue = (String) field.get(person);
		
		return this.codec.encode(fieldValue);
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