package dbs.bigdata.flink.pprl.evaluation;

import java.util.List;

import dbs.bigdata.flink.pprl.utils.common.Unused;

@Unused
public class MetricsAggregator {

	private DbConnector dbCon;
	
	public MetricsAggregator(){}
	
	public void printAggregatedMetricsTable(){
		this.dbCon = new DbConnector();	
		final List<AggregatedJobMetricByScenario> aggregatedMetrics = dbCon.getAllAggregatedMetrics();
		System.out.println(aggregatedMetrics);		
		this.dbCon.close();
	}
	
	
	public static void main(String[] args) throws Exception{
		final MetricsAggregator agg = new MetricsAggregator();
		agg.printAggregatedMetricsTable();
	}
}