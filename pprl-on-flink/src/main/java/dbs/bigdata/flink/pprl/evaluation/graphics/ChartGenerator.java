package dbs.bigdata.flink.pprl.evaluation.graphics;

import java.awt.FontFormatException;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.List;

import org.jfree.chart.JFreeChart;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.itextpdf.text.DocumentException;

import dbs.bigdata.flink.pprl.evaluation.DbConnector;
import dbs.bigdata.flink.pprl.evaluation.MethodQualityMetrics;
import dbs.bigdata.flink.pprl.evaluation.MethodRuntimeMetrics;

/**
 * Class for generating evaluation charts from execution results of the
 * different pprl jobs and their parameters. Therefore it queries the database
 * for the specific metrics (with the {@link DbConnector}, builds the chart data sets 
 * (with the {@link DatasetBuilder} from the metrics and finally
 * creates ({@link ChartCreator}) and prints ({@link ChartPrinter}) all evaluation charts.
 * 
 * @author mfranke
 */
@SuppressWarnings("unused")
public class ChartGenerator {
	private static final Logger log = LoggerFactory.getLogger(ChartGenerator.class);
	
	public static final int DEFAULT_PARALLELISM_VALUE_FOR_QUALITY_METRICS = 32;
	
	private static final String PDF_EXTENSION = ".pdf";
	
	private static final String PARALLELISM = "parallelism";
	private static final String METHOD = "method";
	private static final String RECORDS = "records";
	private static final String METRIC = "metric";
	
	private static final String TIME_CHARTS_DIR = "time_charts/";
	
	private static final String TIME_VS_RECORD_VS_METHOD_BAR_CHARTS_DIR = "records-vs-methods-bar_charts/";
	private static final String TIME_VS_RECORD_VS_METHOD_LINE_CHARTS_DIR = "records-vs-methods-line_charts/";
	private static final String TIME_VS_RECORD_VS_METHOD_LINE_CHARTS_STDDEV_DIR = "records-vs-methods-line_charts_stddev/";
	
	private static final String TIME_VS_PARALLELISM_VS_RECORDS_CHARTS_DIR = "parallelism-vs-records-charts/";
	private static final String TIME_VS_RECORDS_VS_PARALLELISM_CHARTS_DIR = "records-vs-parallelisms-charts/";
	
	private static final String TIME_VS_PARALLELISM_VS_METHOD_CHARTS_DIR = "parallelism-vs-methods-charts/";
	private static final String TIME_VS_METHOD_VS_PARALLELISM_CHARTS_DIR = "methods-vs-parallelism-charts/";
	
	private static final String[] TIME_CHARTS_DIRECTORIES = {
			TIME_VS_RECORD_VS_METHOD_BAR_CHARTS_DIR,
			TIME_VS_RECORD_VS_METHOD_LINE_CHARTS_DIR,
			TIME_VS_RECORD_VS_METHOD_LINE_CHARTS_STDDEV_DIR,
			TIME_VS_PARALLELISM_VS_RECORDS_CHARTS_DIR,
			TIME_VS_RECORDS_VS_PARALLELISM_CHARTS_DIR,
			TIME_VS_PARALLELISM_VS_METHOD_CHARTS_DIR,
			TIME_VS_METHOD_VS_PARALLELISM_CHARTS_DIR
	};
	
	
	private static final String TIME_VS_RECORD_VS_METHOD_BAR_CHART = "time-records-vs-methods-bar_chart";
	private static final String TIME_VS_RECORD_VS_METHOD_LINE_CHART = "time-records-vs-methods-line_chart";
	private static final String TIME_VS_RECORD_VS_METHOD_LINE_CHART_STDDEV = "time-records-vs-methods-line_chart_stddev";
	
	private static final String TIME_VS_PARALLELISM_VS_RECORDS_CHART = "time-parallelism-vs-records-chart";
	private static final String TIME_VS_RECORDS_VS_PARALLELISM_CHART = "time-records-vs-parallelism-chart";
	
	private static final String TIME_VS_PARALLELISM_VS_METHOD_CHART = "time-parallelism-vs-methods-chart";
	private static final String TIME_VS_METHOD_VS_PARALLELISM_CHART = "time-methods-vs-parallelism-chart";
	
	
	private static final String QUALITY_CHARTS_DIR = "quality_charts/";
	
	private static final String METRIC_VS_RECORD_VS_METHOD_CHARTS_DIR = "records-vs-methods-charts/";
	private static final String METRIC_VS_METHOD_VS_RECORD_CHARTS_DIR = "methods-vs-records-charts/";
	
	private static final String METRIC_VS_METRIC_VS_METHOD_CHARTS_DIR = "metrics-vs-methods-charts/";
	private static final String METRIC_VS_METHOD_VS_METRIC_CHARTS_DIR = "methods-vs-metrics-charts/";
	
	private static final String METHOD_VS_METRIC_VS_RECORD_CHARTS_DIR = "metrics-vs-records-charts/";
	private static final String METHOD_VS_RECORD_VS_METRIC_CHARTS_DIR = "records-vs-metrics-charts/";
	
	private static final String[] QUALITY_CHARTS_DIRECTORIES = {
			METRIC_VS_RECORD_VS_METHOD_CHARTS_DIR,
			METRIC_VS_METHOD_VS_RECORD_CHARTS_DIR,
			METRIC_VS_METRIC_VS_METHOD_CHARTS_DIR,
			METRIC_VS_METHOD_VS_METRIC_CHARTS_DIR,
			METHOD_VS_METRIC_VS_RECORD_CHARTS_DIR,
			METHOD_VS_RECORD_VS_METRIC_CHARTS_DIR
	};
	
	
	private static final String METRIC_VS_RECORD_VS_METHOD_CHART = "records-vs-methods-chart";
	private static final String METRIC_VS_METHOD_VS_RECORD_CHART = "methods-vs-records-chart";
	
	private static final String METRIC_VS_METRIC_VS_METHOD_CHART = "metrics-vs-methods-chart";
	private static final String METRIC_VS_METHOD_VS_METRIC_CHART = "methods-vs-metrics-chart";
	
	private static final String METHOD_VS_METRIC_VS_RECORD_CHART = "metrics-vs-records-chart";
	private static final String METHOD_VS_RECORD_VS_METRIC_CHART = "records-vs-metrics-chart";
	
	
	private String baseDirectory;
	private String timeChartDirectory;
	private String qualityChartDirectory;
	
	private DbConnector db;
	
	private boolean printChartsBlackWhite;
	private int parallelismValueForQualityMetrics;
	private List<String> dataDescriptionList;
	
	/**
	 * Creates a new chart generator.
	 * 
	 * @param baseDirectory the base directory under which the graphics should be stored.
	 * @param printChartsBlackWhite flag that indicates whether or not the charts should be printed black white or in color.
	 */
	public ChartGenerator(String baseDirectory, boolean printChartsBlackWhite, int parallelismValueForQualityMetrics){
		this.db = new DbConnector();
		this.dataDescriptionList = db.getDistinctDataDescriptions();
		this.printChartsBlackWhite = printChartsBlackWhite;
		this.parallelismValueForQualityMetrics = parallelismValueForQualityMetrics;
		this.baseDirectory = baseDirectory;	
		this.timeChartDirectory = this.baseDirectory + TIME_CHARTS_DIR;	
		this.qualityChartDirectory = this.baseDirectory + QUALITY_CHARTS_DIR;	
			
		this.createDirectories();
	}
	
	private void createDirectories(){
		final File baseDir = new File(this.baseDirectory);
		baseDir.mkdirs();
		
		final File timeChartDir = new File(this.timeChartDirectory);
		timeChartDir.mkdir();
		
		final File qualityChartDir = new File(this.qualityChartDirectory);
		qualityChartDir.mkdir();
		
		this.createTimeChartDirectories();
		this.createQualityChartDirectories();		
	}
	
	private void createTimeChartDirectories(){
		for (String dirName : TIME_CHARTS_DIRECTORIES){
			for (String dataDescr : this.dataDescriptionList){	
				final String fullDirName = this.timeChartDirectory + getDirectoryForDataDescr(dirName, dataDescr);
				log.info("create directory: " + fullDirName);
				final File dir = new File(fullDirName);
				dir.mkdirs();
			}
		}
	}
	
	private void createQualityChartDirectories(){
		for (String dirName : QUALITY_CHARTS_DIRECTORIES){
			for (String dataDescr : this.dataDescriptionList){	
				final String fullDirName = this.qualityChartDirectory + getDirectoryForDataDescr(dirName, dataDescr);
				log.info("create directory: " + fullDirName);
				final File dir = new File(fullDirName);
				dir.mkdirs();
			}
		}
	}
	
	private File createTimeChartFile(String subDirectory, String name){
		return new File(this.timeChartDirectory + subDirectory + name + PDF_EXTENSION);
	}
	
	private File createQualityChartFile(String subDirectory, String name){
		return new File(this.qualityChartDirectory + subDirectory + name + PDF_EXTENSION);
	}
	
	private static String createDataDescrExtension(String dataDescription){
		return "[" + dataDescription + "]";
	}
	
	private static String getDirectoryForDataDescr(String dir, String dataDescription){
		return dir + dataDescription + "/";
	}
	
	private static String createChartInfoExtension(String info, Object o){
		return "(" + info + "=" + o.toString() + ")";
	}
	
	/**
	 * Triggers the chart generation for all types of charts for all data sets.
	 * 
	 * @throws IOException
	 * @throws DocumentException
	 */
	public void generateAllCharts() throws IOException, DocumentException{
		for (String dataDescription : this.dataDescriptionList){
//			this.generateExecutionTimeWithRecordsVsMethodCharts(dataDescription);
//			this.generateExecutionTimeWithRecordsVsParallelismCharts(dataDescription);
			this.generateExecutionTimeWithParallelismVsMethodCharts(dataDescription);
			
//			this.generateMetricsWithRecordsVsMethodCharts(dataDescription, parallelismValueForQualityMetrics);
//			this.generateMetricsWithMetricVsMethodCharts(dataDescription, parallelismValueForQualityMetrics);
//			this.generateMethodWithRecordsVsMetricCharts(dataDescription, parallelismValueForQualityMetrics);
		}		
		db.close();
	}
	
	/**
	 * Generates charts that opposes records and the used methods (x-axis) to the execution time (y-axis).
	 * This are the charts defined as type A.
	 * 
	 * @param dataDescription the identifier of the used data set.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public void generateExecutionTimeWithRecordsVsMethodCharts(String dataDescription) throws IOException, DocumentException{
		final List<Integer> parallelismValues = db.getDistinctParallismGradesFromAggJobMetricsForDataDescr(dataDescription);	
		
		for (final Integer parallelism : parallelismValues){
			final List<String> jobNames = 
					db.getDistinctJobNamesFromAggJobMetricsForDataDescrAndParallelism(dataDescription, parallelism);
			
			final List<MethodRuntimeMetrics> metrics = 
					db.getExecTimeMetricFromAggJobMetrics(jobNames, dataDescription, parallelism);
								
			this.createTimeVsRecordVsMethodBarChart(dataDescription, parallelism, metrics);			
			this.createTimeVsRecordVsMethodLineChart(dataDescription, parallelism, metrics, true);
			this.createTimeVsRecordVsMethodLineChart(dataDescription, parallelism, metrics, false);
		}
	}
	
	private void createTimeVsRecordVsMethodBarChart(String dataDescription, Integer parallelism, List<MethodRuntimeMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetTimeVsRecordsVsMethod(metrics);				
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, false, printChartsBlackWhite);		
		
		
		final String fileName = TIME_VS_RECORD_VS_METHOD_BAR_CHART + createChartInfoExtension(PARALLELISM, parallelism) + createDataDescrExtension(dataDescription);	
		final String dir = getDirectoryForDataDescr(TIME_VS_RECORD_VS_METHOD_BAR_CHARTS_DIR, dataDescription);	
		final File file = this.createTimeChartFile(dir, fileName);		
		
		ChartPrinter.printPdf(chart, file);
	}
	
	public void createSpeedUpChart() throws IOException, DocumentException{
		final XYDataset data = DatasetBuilder.buildSpeedUpDataset();
		final JFreeChart chart = ChartCreator.createSpeedUpLineChart(data);
		
		final File file = this.createTimeChartFile("", "Speedup(Phon)-a");
		ChartPrinter.printPdf(chart, file);
		db.close();
	}
	
	private void createTimeVsRecordVsMethodLineChart(String dataDescription, Integer parallelism, List<MethodRuntimeMetrics> metrics, boolean withStdDev)
			throws IOException, DocumentException{	
		final ChartData data = (withStdDev) ? 
					DatasetBuilder.buildDatasetTimeVsRecordsVsMethod(metrics) : 
					DatasetBuilder.buildDatasetWithoutStdDevTimeVsRecordsVsMethod(metrics);

		final JFreeChart chart = ChartCreator.createLineChart(data, true, printChartsBlackWhite);	
		
		final String nameOffset = (withStdDev) ?
				TIME_VS_RECORD_VS_METHOD_LINE_CHART_STDDEV :
				TIME_VS_RECORD_VS_METHOD_LINE_CHART;
		
		final String subDir = (withStdDev) ?
				TIME_VS_RECORD_VS_METHOD_LINE_CHARTS_STDDEV_DIR :
				TIME_VS_RECORD_VS_METHOD_LINE_CHARTS_DIR;
				
					
		final String fileName = nameOffset + createChartInfoExtension(PARALLELISM, parallelism) + createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(subDir, dataDescription);
		final File file = this.createTimeChartFile(dir, fileName);		
		
		ChartPrinter.printPdf(chart, file);
	}
	
	
	/**
	 * Generates charts that opposes the number of records and the degree of parallelism (x-axis) to the execution time (y-axis).
	 * This are the charts defined as type B.
	 *  
	 * @param dataDescription the identifier of the used data set.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public void generateExecutionTimeWithRecordsVsParallelismCharts(String dataDescription) throws IOException, DocumentException{
		final List<String> jobNames = db.getDistinctJobNamesFromAggJobMetricsForDataDescr(dataDescription);
		
		for (final String jobName : jobNames){
			final List<MethodRuntimeMetrics> metrics = 
					db.getExecTimeMetricFromAggJobMetricsForDataDescAndJobName(jobName, dataDescription);
			
			this.createTimeVsParallelismVsRecordsChart(dataDescription, jobName, metrics);
			this.createTimeVsRecordsVsParallelismChart(dataDescription, jobName, metrics);
		}
	}
	
	private void createTimeVsParallelismVsRecordsChart(String dataDescription, String jobName, List<MethodRuntimeMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetTimeVsParallelismVsRecords(jobName, metrics);		
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, false, printChartsBlackWhite);		
		final String fileName = TIME_VS_PARALLELISM_VS_RECORDS_CHART + createChartInfoExtension(METHOD, jobName) + createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(TIME_VS_PARALLELISM_VS_RECORDS_CHARTS_DIR, dataDescription);	
		final File file = this.createTimeChartFile(dir, fileName);		
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createTimeVsRecordsVsParallelismChart(String dataDescription, String jobName, List<MethodRuntimeMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetTimeVsRecordsVsParallelism(jobName, metrics);		
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, false, printChartsBlackWhite);		
		final String fileName = TIME_VS_RECORDS_VS_PARALLELISM_CHART + createChartInfoExtension(METHOD, jobName) + createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(TIME_VS_RECORDS_VS_PARALLELISM_CHARTS_DIR, dataDescription);
		final File file = this.createTimeChartFile(dir, fileName);		
		
		ChartPrinter.printPdf(chart, file);
	}
	
	
	/**
	 * Generates charts that opposes the degree of parallelism and the used methods (x-axis) to the execution time (y-axis).
	 * This are the charts defined as type C.
	 *  
	 * @param dataDescription the identifier of the used data set.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public void generateExecutionTimeWithParallelismVsMethodCharts(String dataDescription) throws IOException, DocumentException{
		final List<BigInteger> recordCounts = db.getDistinctNumberOfRecordsFromAggJobMetricsForDataDescr(dataDescription);
		
		final List<String> jobNames = db.getDistinctJobNamesFromAggJobMetricsForDataDescr(dataDescription);
		
		for (BigInteger records : recordCounts){
			final List<MethodRuntimeMetrics> metrics = 
					db.getExecTimeMetricFromAggJobMetricsForDataDescAndJobNameAndRecords(jobNames, dataDescription, records);
			
			this.createTimeVsMethodVsParallelismChart(dataDescription, records, metrics);
			this.createTimeVsParallelismVsMethodChart(dataDescription, records, metrics);
		}
	}
	
	private void createTimeVsMethodVsParallelismChart(String dataDescription, BigInteger records, List<MethodRuntimeMetrics> metrics) throws IOException, DocumentException{	
		final ChartData data = DatasetBuilder.buildDatasetTimeVsMethodVsParallelism(records, metrics);		
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, false, printChartsBlackWhite);		
		final String fileName = TIME_VS_METHOD_VS_PARALLELISM_CHART + createChartInfoExtension(RECORDS, records) + createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(TIME_VS_PARALLELISM_VS_METHOD_CHARTS_DIR, dataDescription);
		final File file = this.createTimeChartFile(dir, fileName);		
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createTimeVsParallelismVsMethodChart(String dataDescription, BigInteger records, List<MethodRuntimeMetrics> metrics) throws IOException, DocumentException{	
		final ChartData data = DatasetBuilder.buildDatasetTimeVsParallelismVsMethod(records, metrics);		
//		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, false, printChartsBlackWhite);		
		final JFreeChart chart = ChartCreator.createLineChart(data, true, printChartsBlackWhite);
		
		final String fileName = TIME_VS_PARALLELISM_VS_METHOD_CHART + createChartInfoExtension(RECORDS, records) + createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(TIME_VS_METHOD_VS_PARALLELISM_CHARTS_DIR, dataDescription);
		final File file = this.createTimeChartFile(dir, fileName);		
		
		ChartPrinter.printPdf(chart, file);
	}

	/**
	 * Generates charts that opposes the number of records and the used methods (x-axis) to the quality metrics i.e. RR, PC, PQ, FM (y-axis).
	 * This are the charts defined as type I.
	 * 
	 * @param dataDescription the identifier of the used data set.
	 * @param parallelism the degree of parallelism that should be used for the evaluation.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public void generateMetricsWithRecordsVsMethodCharts(String dataDescription, int parallelism) throws IOException, DocumentException {
		final List<String> jobNames = db.getDistinctJobNamesFromAggJobMetricsForDataDescrAndParallelism(dataDescription, parallelism);
		final List<MethodQualityMetrics> metrics = db.getMetricsFromAggJobMetrics(jobNames, dataDescription, parallelism);
		
		this.createMetricsVsRecordsVsMethodCharts(dataDescription, parallelism, metrics);
		this.createMetricsVsMethodVsRecordsCharts(dataDescription, parallelism, metrics);
	}
	
	private void createMetricsVsRecordsVsMethodCharts(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		this.createRRVsRecordsVsMethodChart(dataDescription, parallelism, metrics);
		this.createPCVsRecordsVsMethodChart(dataDescription, parallelism, metrics);
		this.createPQVsRecordsVsMethodChart(dataDescription, parallelism, metrics);
		this.createFMVsRecordsVsMethodChart(dataDescription, parallelism, metrics);
	}
	
	private void createRRVsRecordsVsMethodChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetReductionRateVsRecordCountVsMethod(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_RECORD_VS_METHOD_CHART +
				createChartInfoExtension("metric", "RR") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_RECORD_VS_METHOD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createPCVsRecordsVsMethodChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetPairsCompletenessVsRecordCountVsMethod(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_RECORD_VS_METHOD_CHART +
				createChartInfoExtension("metric", "PC") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_RECORD_VS_METHOD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createPQVsRecordsVsMethodChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetPairsQualityVsRecordCountVsMethod(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_RECORD_VS_METHOD_CHART +
				createChartInfoExtension("metric", "PQ") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_RECORD_VS_METHOD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createFMVsRecordsVsMethodChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetFMeasureVsRecordCountVsMethod(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_RECORD_VS_METHOD_CHART +
				createChartInfoExtension("metric", "FM") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_RECORD_VS_METHOD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createMetricsVsMethodVsRecordsCharts(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		this.createRRVsMethodVsRecordsChart(dataDescription, parallelism, metrics);
		this.createPCVsMethodVsRecordsChart(dataDescription, parallelism, metrics);
		this.createPQVsMethodVsRecordsChart(dataDescription, parallelism, metrics);
		this.createFMVsMethodVsRecordsChart(dataDescription, parallelism, metrics);
	}
	
	private void createRRVsMethodVsRecordsChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetReductionRateVsMethodVsRecordCount(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_METHOD_VS_RECORD_CHART +
				createChartInfoExtension(METRIC, "RR") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_METHOD_VS_RECORD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createPCVsMethodVsRecordsChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetPairsCompletenessVsMethodVsRecordCount(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_METHOD_VS_RECORD_CHART +
				createChartInfoExtension(METRIC, "PC") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_METHOD_VS_RECORD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createPQVsMethodVsRecordsChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetPairsQualityVsMethodVsRecordCount(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_METHOD_VS_RECORD_CHART +
				createChartInfoExtension(METRIC, "PQ") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_METHOD_VS_RECORD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createFMVsMethodVsRecordsChart(String dataDescription, int parallelism, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetFMeasureVsMethodVsRecordCount(metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_METHOD_VS_RECORD_CHART +
				createChartInfoExtension(METRIC, "FM") +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METRIC_VS_METHOD_VS_RECORD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	
	/**
	 * Generates charts that opposes the used methods to the metrics and vice versa.
	 * This are the charts defined as type II.
	 * 
	 * @param dataDescription the identifier of the used data set.
	 * @param parallelism the degree of parallelism that should be used for the evaluation.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public void generateMetricsWithMetricVsMethodCharts(String dataDescription, int parallelism) throws IOException, DocumentException {
		final List<String> jobNames = db.getDistinctJobNamesFromAggJobMetricsForDataDescrAndParallelism(dataDescription, parallelism);	
		
		final List<BigInteger> records = 
			db.getDistinctNumberOfRecordsFromAggJobMetricsForJobNameAndDataDescrAndParallelism(
				jobNames, 
				dataDescription, 
				parallelism
			);	
		
		for (BigInteger recordNum : records){
			final List<MethodQualityMetrics> metrics = db.getMetricsFromAggJobMetrics(jobNames, dataDescription, parallelism, recordNum);
			
			this.createMetricsWithMetricVsMethodChart(dataDescription, parallelism, recordNum, metrics);
			this.createMetricsWithMethodVsMetricChart(dataDescription, parallelism, recordNum, metrics);
		}
	}
	
	private void createMetricsWithMetricVsMethodChart(String dataDescription, int parallelism, BigInteger recordNumber, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetRecordsVsMetricsVsMethod(recordNumber, metrics);	
		final JFreeChart chart  = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_METRIC_VS_METHOD_CHART + 
				createChartInfoExtension(RECORDS, recordNumber) + 
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		
		final String dir = getDirectoryForDataDescr(METRIC_VS_METRIC_VS_METHOD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createMetricsWithMethodVsMetricChart(String dataDescription, int parallelism, BigInteger recordNumber, List<MethodQualityMetrics> metrics) throws IOException, DocumentException{
		final ChartData data = DatasetBuilder.buildDatasetRecordsVsMethodVsMetrics(recordNumber, metrics);
		final JFreeChart chart  = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		final String fileName = 
				METRIC_VS_METHOD_VS_METRIC_CHART +
				createChartInfoExtension(RECORDS, recordNumber) +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		
		final String dir = getDirectoryForDataDescr(METRIC_VS_METHOD_VS_METRIC_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	/**
	 * Generates charts that opposes the number of records to the metrics and vice versa.
	 * This are the charts defined as type III.
	 * 
	 * @param dataDescription the identifier of the used data set.
	 * @param parallelism the degree of parallelism that should be used for the evaluation.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public void generateMethodWithRecordsVsMetricCharts(String dataDescription, int parallelism) throws IOException, DocumentException {
		final List<String> jobNames = 
				db.getDistinctJobNamesFromAggJobMetricsForDataDescrAndParallelism(dataDescription, parallelism);
		
		for (String jobName : jobNames){
			final List<MethodQualityMetrics> metrics = 
					db.getMetricsFromAggJobMetrics(jobName, dataDescription, parallelism);
			
			this.createMethodWithMetricVsRecordsChart(dataDescription, parallelism, jobName, metrics);
			this.createMethodWithRecordsVsMetricChart(dataDescription, parallelism, jobName, metrics);
		}
	}
	
	private void createMethodWithMetricVsRecordsChart(String dataDescription, int parallelism, String jobName, List<MethodQualityMetrics> metrics) throws IOException, DocumentException {
		final ChartData data = DatasetBuilder.buildDatasetMethodVsMetricsVsRecordCount(jobName, metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);
		
		final String fileName = METHOD_VS_METRIC_VS_RECORD_CHART + createChartInfoExtension(METHOD, jobName) + createDataDescrExtension(dataDescription);
		final String dir = getDirectoryForDataDescr(METHOD_VS_METRIC_VS_RECORD_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	private void createMethodWithRecordsVsMetricChart(String dataDescription, int parallelism, String jobName, List<MethodQualityMetrics> metrics) throws IOException, DocumentException {
		final ChartData data = DatasetBuilder.buildDatasetMethodVsRecordCountVsMetrics(jobName, metrics);
		final JFreeChart chart = ChartCreator.createStatisticalBarChart(data, true, true, printChartsBlackWhite);	
		final String fileName = 
				METHOD_VS_RECORD_VS_METRIC_CHART +
				createChartInfoExtension(METHOD, jobName) +
				createChartInfoExtension(PARALLELISM, parallelism) +
				createDataDescrExtension(dataDescription);
		
		final String dir = getDirectoryForDataDescr(METHOD_VS_RECORD_VS_METRIC_CHARTS_DIR, dataDescription);
		final File file = this.createQualityChartFile(dir, fileName);
		
		ChartPrinter.printPdf(chart, file);
	}
	
	public static void main(String[] args) throws IOException, DocumentException, SQLException, FontFormatException{
		final String resultPath = "";
		ChartGenerator gen = new ChartGenerator(resultPath, true, ChartGenerator.DEFAULT_PARALLELISM_VALUE_FOR_QUALITY_METRICS);
//		gen.generateAllCharts();
		gen.createSpeedUpChart();
	}
	
}
