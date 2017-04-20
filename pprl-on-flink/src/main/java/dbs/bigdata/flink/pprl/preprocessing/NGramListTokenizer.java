package dbs.bigdata.flink.pprl.preprocessing;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Class for splitting the attributes (i.e. quasi identifiers) of records of the data 
 * into a list of n-gram tokens.
 * 
 * @author mfranke
 */
public class NGramListTokenizer 
	implements MapFunction<Person, Tuple2<Person, List<String>>> {

	private static final long serialVersionUID = -8819111750339919196L;
	
	private static final String PADDING_CHARACTER = "#";
	
	private int nGram;
	private boolean withTokenPadding;
	
	/**
	 * @param ngram the size n of the n-grams
	 * 
	 * @param withTokenPadding if true, a padding character is used for the tokens.
	 * 
	 * @throws Exception an exception is thrown if the value of ngram is smaller then one.
	 */
	public NGramListTokenizer(int nGram, boolean withTokenPadding) throws Exception{
		if (nGram >= 1){
			this.nGram = nGram;
			this.withTokenPadding = withTokenPadding;
		}
		else{
			throw new Exception();
		}
	}
	
	/**
	 * Transform {@link Person} objects into a (Person, TokenList) tuple.
	 */
	@Override
	public Tuple2<Person, List<String>> map(Person value) throws Exception {
		
		List<String> tokenList = new ArrayList<String>();
		
		List<String> qids = value.getSpecifiedAttributeValues();
		
		for (String attribute : qids){
			String normalizedAttribute = this.normalizeString(attribute);
			
			if (!normalizedAttribute.isEmpty()){
				if(this.withTokenPadding){
					normalizedAttribute = this.padString(normalizedAttribute);
				}
				
				List<String> attributeTokens = this.tokenizeString(normalizedAttribute);
			
				tokenList.addAll(attributeTokens);
			}
		}
		return new Tuple2<Person, List<String>>(value, tokenList);
	}
	
	private String normalizeString(String value){
		String result = value.toLowerCase();
		result = result.replaceAll("\\s+", "");
		
		return result;
	}
	
	private String padString(String value){
		String result = value;
		
		for (int i = 1; i < this.nGram; i++){
			result = PADDING_CHARACTER + result + PADDING_CHARACTER;
		}
		
		return result;
	}
	
	private List<String> tokenizeString(String value){
		List<String> tokenList = new ArrayList<String>();
		
		String token = "";
		char[] chars = value.toCharArray();
		
		for (int i = 0; i <= chars.length - this.nGram; i++){
			for (int j = i; j < i + this.nGram; j++){
				token = token + chars[j];
			}
			
			tokenList.add(token);

			token = "";
		}
		
		return tokenList;
	}
	
	public int getNGramValue(){
		return this.nGram;
	}
	
	public void setNGramValue(int nGram){
		this.nGram = nGram;
	}

	public boolean isWithTokenPadding() {
		return withTokenPadding;
	}

	public void setWithTokenPadding(boolean withTokenPadding) {
		this.withTokenPadding = withTokenPadding;
	}
}