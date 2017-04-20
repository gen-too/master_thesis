package dbs.bigdata.flink.pprl.job.common;

import java.text.DecimalFormat;

/**
 * Class that represents a single linkage result, i.e.
 * found matching records with a similarity above the
 * specified threshold.
 * 
 * @author mfranke
 *
 */
public class LinkageResult {

	private LinkageTuple tuple1;
	private LinkageTuple tuple2;
	private Double similarity;
	
	public LinkageResult(){
		this(null, null, null);
	}
	
	public LinkageResult(LinkageTuple tuple1, LinkageTuple tuple2, Double similarity){
		this.tuple1 = tuple1;
		this.tuple2 = tuple2;
		this.similarity = similarity;
	}
	

	public boolean isTruePositive(){
		return this.tuple1.idEquals(tuple2);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("[");
		builder.append(tuple1.getId());
		builder.append(",");
		builder.append(tuple2.getId());
		builder.append(", sim=");
		builder.append(new DecimalFormat("#.##").format(similarity));
		builder.append("]");
		return builder.toString();
	}

	public LinkageTuple getTuple1() {
		return tuple1;
	}

	public void setTuple1(LinkageTuple tuple1) {
		this.tuple1 = tuple1;
	}

	public LinkageTuple getTuple2() {
		return tuple2;
	}

	public void setTuple2(LinkageTuple tuple2) {
		this.tuple2 = tuple2;
	}

	public Double getSimilarity() {
		return similarity;
	}

	public void setSimilarity(Double similarity) {
		this.similarity = similarity;
	}	
}