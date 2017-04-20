package dbs.bigdata.flink.pprl.job.common;

import dbs.bigdata.flink.pprl.utils.bloomfilter.BloomFilter;

/**
 * Class that represents a candidate pair.
 * 
 * @author mfranke
 */
public class CandidateLinkageTuple{

	protected LinkageTuple candidateOne;
	protected LinkageTuple candidateTwo;
	
	/**
	 * Empty default constructor.
	 */
	public CandidateLinkageTuple(){
		this(null, null);
	}
	
	/**
	 * Creates a new candidate pair.
	 * @param candidateOne the first of the pair.
	 * @param candidateTwo the second of the pair.
	 */
	public CandidateLinkageTuple(LinkageTuple candidateOne, LinkageTuple candidateTwo){
		this.candidateOne = candidateOne;
		this.candidateTwo = candidateTwo;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CandidateLinkageTuple [candidateOne=");
		builder.append(candidateOne);
		builder.append(", candidateTwo=");
		builder.append(candidateTwo);
		builder.append("]");
		return builder.toString();
	}
	
	public LinkageTuple getCandidateOne() {
		return candidateOne;
	}

	public void setCandidateOne(LinkageTuple candidateOne) {
		this.candidateOne = candidateOne;
	}

	public LinkageTuple getCandidateTwo() {
		return candidateTwo;
	}

	public void setCandidateTwo(LinkageTuple candidateTwo) {
		this.candidateTwo = candidateTwo;
	}

	/**
	 * Calculates the Jaccard similarity of this candidate pair.
	 * @return Jaccard similarity value.
	 */
	public Double calculateJaccardSimilarity() {
		final BloomFilter bf1 = candidateOne.getBloomFilter();
		final BloomFilter bf2 = candidateTwo.getBloomFilter();
		
		return bf1.calculateJaccardSimilarity(bf2);
	}

	/**
	 * Calculates the Dice similarity of this candidate pair.
	 * @return Dice similarity value.
	 */
	public Double calculateDiceSimilarity() {
		final BloomFilter bf1 = candidateOne.getBloomFilter();
		final BloomFilter bf2 = candidateTwo.getBloomFilter();
		
		return bf1.calculateDiceSimilarity(bf2);
	}
}