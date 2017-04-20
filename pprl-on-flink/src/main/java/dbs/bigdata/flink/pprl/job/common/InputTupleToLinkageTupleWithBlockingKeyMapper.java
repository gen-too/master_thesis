package dbs.bigdata.flink.pprl.job.common;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Map function that transforms a {@link InputTuple} into a {@link LinkageTupleWithBlockingKey}.
 * @author mfranke
 *
 */
public class InputTupleToLinkageTupleWithBlockingKeyMapper implements MapFunction<InputTuple, LinkageTupleWithBlockingKey> {

	private static final long serialVersionUID = 5511602085311537878L;

	@Override
	public LinkageTupleWithBlockingKey map(InputTuple value) throws Exception {
		return LinkageTupleWithBlockingKey.from(value);
	}
}