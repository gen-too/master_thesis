package dbs.bigdata.flink.pprl.job.common;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Map Function to transform a {@link InputTuple} into a {@link LinkageTuple}.
 * @author mfranke
 *
 */
public class InputTupleToLinkageTupleMapper implements MapFunction<InputTuple, LinkageTuple> {

	private static final long serialVersionUID = 5544602085311537878L;

	@Override
	public LinkageTuple map(InputTuple value) throws Exception {
		return LinkageTuple.from(value);
	}

}
