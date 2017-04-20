package dbs.bigdata.flink.pprl.job.lsh;

import dbs.bigdata.flink.pprl.job.common.CandidateLinkageTuple;
import dbs.bigdata.flink.pprl.job.common.LinkageTuple;

/**
 * Special case of a {@link CandidateLinkageTuple} that contains also
 * {@link LshKey}s.
 * 
 * @author mfranke
 *
 */
public class CandidateLinkageTupleWithLshKeys extends CandidateLinkageTuple{

	/**
	 * Empty default constructor.
	 */
	public CandidateLinkageTupleWithLshKeys(){
		this(null, null);
	}

	/**
	 * Creates a new {@link CandidateLinkageTupleWithLshKeys}.
	 * 
	 * @param candidateOne the first candidate.
	 * @param candidateTwo the second candidate.
	 */
	public CandidateLinkageTupleWithLshKeys(LinkageTupleWithLshKeys candidateOne, LinkageTupleWithLshKeys candidateTwo){
		super(candidateOne, candidateTwo);
	}

	@Override
	public LinkageTupleWithLshKeys getCandidateOne() {
		return (LinkageTupleWithLshKeys) candidateOne;
	}

	public void setCandidateOne(LinkageTupleWithLshKeys candidateOne) {
		this.candidateOne = candidateOne;
	}

	@Override
	public void setCandidateOne(LinkageTuple candidateOne){
		if (candidateOne instanceof LinkageTupleWithLshKeys){
			super.setCandidateOne(candidateOne);
		}
	}
	
	@Override
	public LinkageTupleWithLshKeys getCandidateTwo() {
		return (LinkageTupleWithLshKeys) candidateTwo;
	}

	public void setCandidateTwo(LinkageTupleWithLshKeys candidateTwo) {
		this.candidateTwo = candidateTwo;
	}

	@Override
	public void setCandidateTwo(LinkageTuple candidateTwo){
		if (candidateTwo instanceof LinkageTupleWithLshKeys){
			super.setCandidateTwo(candidateTwo);
		}
	}
}