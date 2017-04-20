package dbs.bigdata.flink.pprl.evaluation.graphics;

import java.math.BigInteger;
import java.util.List;

import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.statistics.DefaultStatisticalCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import dbs.bigdata.flink.pprl.evaluation.MethodQualityMetrics;
import dbs.bigdata.flink.pprl.evaluation.MethodRuntimeMetrics;

@SuppressWarnings("unused")
public class DatasetBuilder {
	
	private static final String VS = " vs ";
	private static final String RR_LABEL = "Reduction-Rate";
	private static final String PC_LABEL = "Pairs-Completeness";
	private static final String PQ_LABEL = "Pairs-Quality";
	private static final String FM_LABEL = "F-Measure";
	private static final String RECORDS_LABEL = "#Records";
	private static final String RECORDS_COUNT_OFFSET = " Records";
	private static final String JOB_LABEL = "Verfahren";
	private static final String METRICS_LABEL = "Metriken";
	private static final String TIME_LABEL = "Zeit [s]";
	private static final String PARALLELISM_LABEL = "Flink-Job-Parallelität";
	private static final String PARALLELISM_LABEL_PREFIX = "Parallelität ";
	
	private static final String RR_VS_RECORDS_VS_METHOD_LABEL = RR_LABEL + VS + RECORDS_LABEL + VS + JOB_LABEL;
	private static final String RR_VS_METHOD_VS_RECORDS_LABEL = RR_LABEL + VS + JOB_LABEL + VS + RECORDS_LABEL;
	
	private static final String PC_VS_RECORDS_VS_METHOD_LABEL = PC_LABEL + VS + RECORDS_LABEL + VS + JOB_LABEL;
	private static final String PC_VS_METHOD_VS_RECORDS_LABEL = PC_LABEL + VS + JOB_LABEL + VS + RECORDS_LABEL;
	
	private static final String PQ_VS_RECORDS_VS_METHOD_LABEL = PQ_LABEL + VS + RECORDS_LABEL + VS + JOB_LABEL;
	private static final String PQ_VS_METHOD_VS_RECORDS_LABEL = PQ_LABEL + VS + JOB_LABEL + VS + RECORDS_LABEL;
	
	private static final String FM_VS_RECORDS_VS_METHOD_LABEL = FM_LABEL + VS + RECORDS_LABEL + VS + JOB_LABEL;
	private static final String FM_VS_METHOD_VS_RECORDS_LABEL = FM_LABEL + VS + JOB_LABEL + VS + RECORDS_LABEL;
	
	private static final String RECORDS_VS_METRICS_VS_METHOD_LABEL_OFFSET = VS + METRICS_LABEL + VS + JOB_LABEL;
	private static final String RECORDS_VS_METHOD_VS_METRICS_LABEL_OFFSET = VS + JOB_LABEL + VS + METRICS_LABEL;
	
	private static final String METHOD_VS_RECORDS_VS_METRICS_LABEL_OFFSET = VS + RECORDS_LABEL + VS + METRICS_LABEL;
	private static final String METHOD_VS_METRICS_VS_RECORDS_LABEL_OFFSET = VS + METRICS_LABEL + VS + RECORDS_LABEL;
		
	private static final String TIME_VS_RECORDS_VS_METHOD_LABEL = TIME_LABEL + VS + RECORDS_LABEL + VS + JOB_LABEL;
	
	private static final String TIME_VS_RECORDS_VS_PARALLELISM_LABEL = TIME_LABEL + VS + RECORDS_LABEL + VS + PARALLELISM_LABEL;
	private static final String TIME_VS_PARALLELISM_VS_RECORDS_LABEL = TIME_LABEL + VS + RECORDS_LABEL + VS + PARALLELISM_LABEL;
	
	private static final String TIME_VS_METHOD_VS_PARALLELISM_LABEL = TIME_LABEL + VS + JOB_LABEL + VS + PARALLELISM_LABEL;
	private static final String TIME_VS_PARALLELISM_VS_METHOD_LABEL = TIME_LABEL + VS + PARALLELISM_LABEL + VS + JOB_LABEL;
	
	
	/** === TYPE 1 (RR) ================================================= **/
	/**
	 * Builds a data set that oppose the number of records (x-axis) and used methods for the reduction rate metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetReductionRateVsRecordCountVsMethod(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addReductionRateVsRecordCountVsMethod(dataset, metric);
		});
			
		final ChartData data = new ChartData(dataset, RR_VS_RECORDS_VS_METHOD_LABEL, RR_LABEL, RECORDS_LABEL);
		return data;
	}
	
	private static void addReductionRateVsRecordCountVsMethod(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double rr = metric.getReductionRate().doubleValue();
		final double rrStdDev = metric.getReductionRateStdDev().doubleValue();
		final String rowKey = metric.getJobName();
		final String columnKey = metric.getNumberOfRecordsAsString();
				
		dataset.add(rr, rrStdDev, rowKey, columnKey);
	}
	
	/**
	 * Builds a data set that oppose the used methods (x-axis) and the number of records for the reduction rate metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetReductionRateVsMethodVsRecordCount(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addReductionRateVsMethodVsRecordCount(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, RR_VS_METHOD_VS_RECORDS_LABEL, RR_LABEL, JOB_LABEL);
		return data;
	}
	
	private static void addReductionRateVsMethodVsRecordCount(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double rr = metric.getReductionRate().doubleValue();
		final double rrStdDev = metric.getReductionRateStdDev().doubleValue();	
		final String rowKey = metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET;;
		final String columnKey = metric.getJobName();
		
		dataset.add(rr, rrStdDev, rowKey, columnKey);
	}

	
	/** === TYPE 1 (PC) ================================================= **/
	/**
	 * Builds a data set that oppose the number of records (x-axis) and used methods for the pairs completeness metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetPairsCompletenessVsRecordCountVsMethod(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addPairsCompletenessVsRecordCountVsMethod(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, PC_VS_RECORDS_VS_METHOD_LABEL, PC_LABEL, RECORDS_LABEL);
		return data;
	}
	
	private static void addPairsCompletenessVsRecordCountVsMethod(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double pc = metric.getPairsCompleteness().doubleValue();
		final double pcStdDev = metric.getPairsCompletenessStdDev().doubleValue();
		final String rowKey = metric.getJobName();
		final String columnKey = metric.getNumberOfRecordsAsString();
				
		dataset.add(pc, pcStdDev, rowKey, columnKey);
	}
	
	/**
	 * Builds a data set that oppose the used methods (x-axis) and the number of records for the pairs completeness metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetPairsCompletenessVsMethodVsRecordCount(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addPairsCompletenessVsMethodVsRecordCount(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, PC_VS_METHOD_VS_RECORDS_LABEL, PC_LABEL, JOB_LABEL);
		return data;
	}
	
	private static void addPairsCompletenessVsMethodVsRecordCount(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double pc = metric.getPairsCompleteness().doubleValue();
		final double pcStdDev = metric.getPairsCompletenessStdDev().doubleValue();	
		final String rowKey = metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET;;
		final String columnKey = metric.getJobName();
		
		dataset.add(pc, pcStdDev, rowKey, columnKey);
	}
	
	
	/** === TYPE 1 (PQ) ================================================= **/
	/**
	 * Builds a data set that oppose the number of records (x-axis) and used methods for the pairs quality metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetPairsQualityVsRecordCountVsMethod(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addPairsQualityVsRecordCountVsMethod(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, PQ_VS_RECORDS_VS_METHOD_LABEL, PQ_LABEL, RECORDS_LABEL);
		return data;
	}
	
	private static void addPairsQualityVsRecordCountVsMethod(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double pq = metric.getPairsQuality().doubleValue();
		final double pqStdDev = metric.getPairsQualityStdDev().doubleValue();
		final String rowKey = metric.getJobName();
		final String columnKey = metric.getNumberOfRecordsAsString();
				
		dataset.add(pq, pqStdDev, rowKey, columnKey);
	}
	
	/**
	 * Builds a data set that oppose the used methods (x-axis) and the number of records for the pairs quality metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetPairsQualityVsMethodVsRecordCount(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addPairsQualityVsMethodVsRecordCount(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, PQ_VS_METHOD_VS_RECORDS_LABEL, PQ_LABEL, JOB_LABEL);
		return data;
	}
	
	private static void addPairsQualityVsMethodVsRecordCount(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double pq = metric.getPairsQuality().doubleValue();
		final double pqStdDev = metric.getPairsQualityStdDev().doubleValue();	
		final String rowKey = metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET;;
		final String columnKey = metric.getJobName();
		
		dataset.add(pq, pqStdDev, rowKey, columnKey);
	}
	
	/** === TYPE 1 (FM) ================================================= **/
	/**
	 * Builds a data set that oppose the number of records (x-axis) and used methods for the f-measure metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetFMeasureVsRecordCountVsMethod(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addFMeasureVsRecordCountVsMethod(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, FM_VS_RECORDS_VS_METHOD_LABEL, FM_LABEL, RECORDS_LABEL);
		return data;
	}
	
	private static void addFMeasureVsRecordCountVsMethod(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double fm = metric.getfMeasure().doubleValue();
		final double fmStdDev = metric.getfMeasureStdDev().doubleValue();
		final String rowKey = metric.getJobName();
		final String columnKey = metric.getNumberOfRecordsAsString();
				
		dataset.add(fm, fmStdDev, rowKey, columnKey);
	}
	
	/**
	 * Builds a data set that oppose the used methods (x-axis) and the number of records for the f-measure metric (y-axis).
	 * 
	 * @param metrics list of quality metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetFMeasureVsMethodVsRecordCount(List<MethodQualityMetrics> metrics){
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addFMeasureVsMethodVsRecordCount(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, FM_VS_METHOD_VS_RECORDS_LABEL, FM_LABEL, JOB_LABEL);
		return data;
	}
	
	private static void addFMeasureVsMethodVsRecordCount(DefaultStatisticalCategoryDataset dataset, MethodQualityMetrics metric){
		final double fm = metric.getfMeasure().doubleValue();
		final double fmStdDev = metric.getfMeasureStdDev().doubleValue();	
		final String rowKey = metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET;
		final String columnKey = metric.getJobName();
		
		dataset.add(fm, fmStdDev, rowKey, columnKey);
	}
	
	
	/** === TYPE 2 ================================================= **/
	/**
	 * Builds a data set that oppose the metrics (x-axis) and the used methods for a certain number of records.
	 * 
	 * @param recordNumber the record number all metrics are related to.
	 * @param metrics list of quality metrics where each metric refers to the same number of records.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetRecordsVsMetricsVsMethod(BigInteger recordNumber, 
			List<MethodQualityMetrics> metrics){
		
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForRecordsVsMetricsVsMethod(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, recordNumber + RECORDS_VS_METRICS_VS_METHOD_LABEL_OFFSET, "", METRICS_LABEL);
		return data;
	}
	
	private static void addValueForRecordsVsMetricsVsMethod(DefaultStatisticalCategoryDataset dataset, 
			MethodQualityMetrics metric){
		/*dataset.add(
			metric.getReductionRate().doubleValue(),
			metric.getReductionRateStdDev().doubleValue(),				
			metric.getJobName(),
			RR_LABEL
		);*/
		dataset.add(
			metric.getPairsCompleteness().doubleValue(),
			metric.getPairsCompletenessStdDev().doubleValue(),
			metric.getJobName(),		
			PC_LABEL
		);
		dataset.add(
			metric.getPairsQuality().doubleValue(),
			metric.getPairsQualityStdDev().doubleValue(),
			metric.getJobName(),
			PQ_LABEL
		);
		dataset.add(
			metric.getfMeasure().doubleValue(),
			metric.getfMeasureStdDev().doubleValue(),
			metric.getJobName(),
			FM_LABEL
		);
	}
	
	//only metrics with same number of records !
	/**
	 * Builds a data set that oppose the used methods (x-axis) to the metrics for a certain number of records.
	 * 
	 * @param recordNumber the record number all metrics are related to.
	 * @param metrics list of quality metrics where each metric refers to the same number of records.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetRecordsVsMethodVsMetrics(BigInteger recordNumber, 
			List<MethodQualityMetrics> metrics){
		
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForRecordsVsMethodVsMetrics(dataset, metric);
		});
		
		final ChartData data = new ChartData(dataset, recordNumber + RECORDS_VS_METHOD_VS_METRICS_LABEL_OFFSET, "", JOB_LABEL);
		return data;
	}
	
	private static void addValueForRecordsVsMethodVsMetrics(DefaultStatisticalCategoryDataset dataset, 
			MethodQualityMetrics metric){
		/*dataset.add(
			metric.getReductionRate().doubleValue(),
			metric.getReductionRateStdDev().doubleValue(),				
			RR_LABEL,
			metric.getJobName()			
		);*/
		dataset.add(
			metric.getPairsCompleteness().doubleValue(),
			metric.getPairsCompletenessStdDev().doubleValue(),	
			PC_LABEL,
			metric.getJobName()
		);
		dataset.add(
			metric.getPairsQuality().doubleValue(),
			metric.getPairsQualityStdDev().doubleValue(),
			PQ_LABEL,
			metric.getJobName()		
		);
		dataset.add(
			metric.getfMeasure().doubleValue(),
			metric.getfMeasureStdDev().doubleValue(),
			FM_LABEL,
			metric.getJobName()			
		);
	}
	

	
	/** === TYPE 3 ================================================= **/
	/**
	 * Builds a data set that oppose the number of records (x-axis) and the metrics for a certain method.
	 * 
	 * @param jobName the name of the job all metrics are related to.
	 * @param metrics list of quality metrics where each metric belong to the same job.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetMethodVsRecordCountVsMetrics(String jobName,
			List<MethodQualityMetrics> metrics){
		
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForMethodVsRecordCountVsMetrics(dataset, metric);
		});
		
		final ChartData data = 
				new ChartData(dataset, jobName + METHOD_VS_RECORDS_VS_METRICS_LABEL_OFFSET, "", RECORDS_LABEL);
		return data;
	}
		
		
	private static void addValueForMethodVsRecordCountVsMetrics(DefaultStatisticalCategoryDataset dataset, 
			MethodQualityMetrics metric){
		/*dataset.add(
			metric.getReductionRate().doubleValue(),
			metric.getReductionRateStdDev().doubleValue(),
			RR_LABEL,
			metric.getNumberOfRecords()
		);*/
		dataset.add(
			metric.getPairsCompleteness().doubleValue(),
			metric.getPairsCompletenessStdDev().doubleValue(),
			PC_LABEL,
			metric.getNumberOfRecords()
		);
		dataset.add(
			metric.getPairsQuality().doubleValue(),
			metric.getPairsQualityStdDev().doubleValue(),
			PQ_LABEL,
			metric.getNumberOfRecords()
		);
		dataset.add(
			metric.getfMeasure().doubleValue(),
			metric.getfMeasureStdDev().doubleValue(),
			FM_LABEL,
			metric.getNumberOfRecords()
		);
	}
		
	/**
	 * Builds a data set that oppose the metrics (x-axis) and the number of records for a certain method.
	 * 
	 * @param jobName the name of the job all metrics are related to.
	 * @param metrics list of quality metrics where each metric belong to the same job.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetMethodVsMetricsVsRecordCount(String jobName,
			List<MethodQualityMetrics> metrics){
		
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForMethodVsMetricsVsRecordCount(dataset, metric);
		});
		
		final ChartData data = 
				new ChartData(dataset, jobName + METHOD_VS_METRICS_VS_RECORDS_LABEL_OFFSET, "", METRICS_LABEL);
		return data;
	}
		
	private static void addValueForMethodVsMetricsVsRecordCount(DefaultStatisticalCategoryDataset dataset, 
			MethodQualityMetrics metric){
		/*dataset.add(
			metric.getReductionRate().doubleValue(),
			metric.getReductionRateStdDev().doubleValue(),				
			metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET,
			RR_LABEL
		);*/
		dataset.add(
			metric.getPairsCompleteness().doubleValue(),
			metric.getPairsCompletenessStdDev().doubleValue(),
			metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET,		
			PC_LABEL
		);
		dataset.add(
			metric.getPairsQuality().doubleValue(),
			metric.getPairsQualityStdDev().doubleValue(),
			metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET,
			PQ_LABEL
		);
		dataset.add(
			metric.getfMeasure().doubleValue(),
			metric.getfMeasureStdDev().doubleValue(),
			metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET,
			FM_LABEL
		);
	}
	
	
	public static XYDataset buildSpeedUpDataset(){
		/*
		final XYSeries series1 = new XYSeries("LSH(5,4)");
        series1.add(1, 1.0);
        series1.add(2, 1.58);
        series1.add(4, 2.34);
        series1.add(8, 2.01);
        series1.add(16, 4.32);
        series1.add(32, 4.85);

        final XYSeries series2 = new XYSeries("LSH(5,29)");
        series2.add(1, 1.0);
        series2.add(2, 1.59);
        series2.add(4, 2.30);
        series2.add(8, 5.84);
        series2.add(16, 6.9);
        series2.add(32, 7.32);
        
        final XYSeries series3 = new XYSeries("LSH(5,2)");
        series3.add(1, 1.0);
        series3.add(2, 1.61);
        series3.add(4, 1.99);
        series3.add(8, 3.27);
        series3.add(16, 5.87);
        series3.add(32, 6.61);
        
        final XYSeries series4 = new XYSeries("LSH(5,12)");
        series4.add(1, 1.0);
        series4.add(2, 1.63);
        series4.add(4, 2.26);
        series4.add(8, 2.49);
        series4.add(16, 6.66);
        series4.add(32, 5.43);
        
        final XYSeries series5 = new XYSeries("LSH(20,6)");
        series5.add(1, 1.0);
        series5.add(2, 1.75);
        series5.add(4, 2.87);
        series5.add(8, 4.6);
        series5.add(16, 5.08);
        series5.add(32, 5.22);
        
        final XYSeries series6 = new XYSeries("LSH(20,18)");
        series6.add(1, 1.0);
        series6.add(2, 1.4);
        series6.add(4, 3);
        series6.add(8, 3.4);
        series6.add(16, 4.69);
        series6.add(32, 7.47);


        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series1);
        dataset.addSeries(series2);	
        dataset.addSeries(series3);	
        dataset.addSeries(series4);	
        dataset.addSeries(series5);	
        dataset.addSeries(series6);	
        */
		
//		/*
		final XYSeries series1 = new XYSeries("PB");
        series1.add(1, 1.0);
        series1.add(2, 1.29);
        series1.add(4, 1.42);
        series1.add(8, 2.66);
        series1.add(16, 2.69);
        series1.add(32, 2.88);

        final XYSeries series2 = new XYSeries("SPB");
        series2.add(1, 1.0);
        series2.add(2, 1.08);
        series2.add(4, 1.16);
        series2.add(8, 2.12);
        series2.add(16, 2.14);
        series2.add(32, 2.29);


        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series1);
        dataset.addSeries(series2);		
//        */
        
        
        return dataset;
	}
	
	/** === TYPE A ================================================= **/
	/**
	 * Builds a data set that oppose the number of records (x-axis) and the used method to the execution time of the methods.
	 * 
	 * @param metrics a list of runtime metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetTimeVsRecordsVsMethod(List<MethodRuntimeMetrics> metrics) {
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForTimeVsRecordsVsMethod(dataset, metric);
		});
		
		final ChartData data = 
				new ChartData(dataset, TIME_VS_RECORDS_VS_METHOD_LABEL, TIME_LABEL, RECORDS_LABEL);
		return data;
	}
	
	private static void addValueForTimeVsRecordsVsMethod(DefaultStatisticalCategoryDataset dataset, 
			MethodRuntimeMetrics metric){
		dataset.add(
			metric.getExecutionTime(), 
			metric.getExecutionTimeStdDev(), 
			metric.getJobName(), 
			metric.getNumberOfRecordsAsString()
		);
	}
	
	/**
	 * Builds a data set that oppose the number of records (x-axis) and the used method to the execution time of the methods.
	 * This data set does not contain the standard deviation.
	 * 
	 * @param metrics a list of runtime metrics.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetWithoutStdDevTimeVsRecordsVsMethod(List<MethodRuntimeMetrics> metrics) {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForTimeVsRecordsVsMethod(dataset, metric);
		});
		
		final ChartData data = 
				new ChartData(dataset, TIME_VS_RECORDS_VS_METHOD_LABEL, TIME_LABEL, RECORDS_LABEL);
		return data;
	}
	
	private static void addValueForTimeVsRecordsVsMethod(DefaultCategoryDataset dataset, 
			MethodRuntimeMetrics metric){
		
		dataset.addValue(
				metric.getExecutionTime(),  
				metric.getJobName(), 
				metric.getNumberOfRecordsAsString()
			);
	}

	/** === TYPE B ================================================= **/
	/**
	 * Builds a data set that oppose the number of records (x-axis) and the degree of parallelism to the execution time (y-axis).
	 * 
	 * @param jobName the name of the job (method) all metrics are related to.
	 * @param metrics a list of runtime metrics where each metric belong to the same job (method).
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetTimeVsRecordsVsParallelism(String jobName, List<MethodRuntimeMetrics> metrics) {
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForTimeVsRecordsVsParallelism(dataset, metric);
		});
		
		final ChartData data = 
			new ChartData(
					dataset,
					TIME_VS_RECORDS_VS_PARALLELISM_LABEL + " (" + jobName + ")", 
					TIME_LABEL, RECORDS_LABEL
			);
		
		return data;
	}
	
	private static void addValueForTimeVsRecordsVsParallelism(DefaultStatisticalCategoryDataset dataset, 
			MethodRuntimeMetrics metric){
		dataset.add(
			metric.getExecutionTime(), 
			metric.getExecutionTimeStdDev(), 
			PARALLELISM_LABEL_PREFIX + metric.getParallelism(), 
			metric.getNumberOfRecordsAsString()
		);
	}
	
	/**
	 * Builds a data set that oppose the degree of parallelism (x-axis) and the number of records to the execution time (y-axis).
	 * 
	 * @param jobName the name of the job (method) all metrics are related to.
	 * @param metrics a list of runtime metrics where each metric belong to the same job (method).
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetTimeVsParallelismVsRecords(String jobName, List<MethodRuntimeMetrics> metrics) {
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForTimeVsParallelismVsRecords(dataset, metric);
		});
		
		final ChartData data = 
			new ChartData(
					dataset,
					TIME_VS_PARALLELISM_VS_RECORDS_LABEL + " (" + jobName + ")", 
					TIME_LABEL, PARALLELISM_LABEL
			);
		
		return data;
	}
	
	private static void addValueForTimeVsParallelismVsRecords(DefaultStatisticalCategoryDataset dataset, 
			MethodRuntimeMetrics metric){
		dataset.add(
			metric.getExecutionTime(), 
			metric.getExecutionTimeStdDev(), 
			metric.getNumberOfRecordsAsString() + RECORDS_COUNT_OFFSET,
			metric.getParallelism()
		);
	}
	
	/** === TYPE C ================================================= **/
	/**
	 * Builds a data set that oppose the methods (x-axis) and the degree of parallelism to the execution time (y-axis).
	 * 
	 * @param records the number of records to which all metrics are related to.
	 * @param metrics a list of runtime metrics where each metric belong to the same number of records.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetTimeVsMethodVsParallelism(BigInteger records, List<MethodRuntimeMetrics> metrics) {
		DefaultStatisticalCategoryDataset dataset = new DefaultStatisticalCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForTimeVsMethodVsParallelism(dataset, metric);
		});
		
		final ChartData data = 
			new ChartData(
					dataset,
					TIME_VS_METHOD_VS_PARALLELISM_LABEL + " (" + records + ")", 
					TIME_LABEL, JOB_LABEL
			);
		
		return data;
	}
		
	private static void addValueForTimeVsMethodVsParallelism(DefaultStatisticalCategoryDataset dataset, 
			MethodRuntimeMetrics metric){
		dataset.add(
			metric.getExecutionTime(), 
			metric.getExecutionTimeStdDev(), 
			PARALLELISM_LABEL_PREFIX + metric.getParallelism(), 
			metric.getJobName()
		);
	}
	
	/**
	 * Builds a data set that oppose the degree of parallelism (x-axis) and the methods to the execution time (y-axis).
	 * 
	 * @param records the number of records to which all metrics are related to.
	 * @param metrics a list of runtime metrics where each metric belong to the same number of records.
	 * @return {@link ChartData} object that can be used to create a {@link JFreeChart} with the {@link ChartCreator}.
	 */
	public static ChartData buildDatasetTimeVsParallelismVsMethod(BigInteger records, List<MethodRuntimeMetrics> metrics) {
		DefaultCategoryDataset dataset = new DefaultCategoryDataset();
		
		metrics.forEach((metric) -> {
			addValueForTimeVsParallelismVsMethod(dataset, metric);
		});
		
		final ChartData data = 
			new ChartData(
					dataset,
					TIME_VS_PARALLELISM_VS_METHOD_LABEL + " (" + records + ")", 
					TIME_LABEL, PARALLELISM_LABEL
			);
		
		return data;
	}
		
	private static void addValueForTimeVsParallelismVsMethod(DefaultCategoryDataset dataset, 
			MethodRuntimeMetrics metric){
		dataset.addValue(
			metric.getExecutionTime(), 
//			metric.getExecutionTimeStdDev(),
			metric.getJobName(),
			metric.getParallelism()
		);
	}	
}