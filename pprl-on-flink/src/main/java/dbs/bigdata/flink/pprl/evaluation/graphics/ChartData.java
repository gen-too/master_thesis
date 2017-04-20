package dbs.bigdata.flink.pprl.evaluation.graphics;

import org.jfree.chart.JFreeChart;
import org.jfree.data.category.CategoryDataset;

/**
 * Class that contains the data set and informations for a chart.
 * Can be used with the {@link ChartCreator} to create custom {@link JFreeChart}s.
 * 
 * @author mfranke
 */
public class ChartData {

	public CategoryDataset dataset;
	public String title;
	public String yAxisLabel;
	public String xAxisLabel;

	/**
	 * Creates a new empty {@link ChartData} object.
	 */
	public ChartData(){}
	
	/**
	 * Creates a new {@link ChartData} object for the given parameters.
	 * 
	 * @param dataset the data basis.
	 * @param title the title for the chart.
	 * @param yAxisLabel the label for the y-axis.
	 * @param xAxisLabel the label for the x-axis.
	 */
	public ChartData(CategoryDataset dataset, String title, String yAxisLabel, String xAxisLabel) {
		this.dataset = dataset;
		this.title = title;
		this.yAxisLabel = yAxisLabel;
		this.xAxisLabel = xAxisLabel;
	}

	public CategoryDataset getDataset() {
		return dataset;
	}

	public void setDataset(CategoryDataset dataset) {
		this.dataset = dataset;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getyAxisLabel() {
		return yAxisLabel;
	}

	public void setyAxisLabel(String yAxisLabel) {
		this.yAxisLabel = yAxisLabel;
	}

	public String getxAxisLabel() {
		return xAxisLabel;
	}

	public void setxAxisLabel(String xAxisLabel) {
		this.xAxisLabel = xAxisLabel;
	}
}