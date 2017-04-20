package dbs.bigdata.flink.pprl.evaluation.graphics;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.GradientPaint;
import java.text.DecimalFormat;
import java.text.NumberFormat;


import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.annotations.XYAnnotation;
import org.jfree.chart.annotations.XYLineAnnotation;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.LogAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.NumberTickUnit;
import org.jfree.chart.axis.TickUnits;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.CategoryItemRenderer;
import org.jfree.chart.renderer.category.StandardBarPainter;
import org.jfree.chart.renderer.category.StatisticalBarRenderer;
import org.jfree.chart.renderer.category.StatisticalLineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.LegendTitle;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.GradientPaintTransformType;
import org.jfree.ui.HorizontalAlignment;
import org.jfree.ui.RectangleEdge;
import org.jfree.ui.RectangleInsets;
import org.jfree.ui.StandardGradientPaintTransformer;
import org.jfree.ui.VerticalAlignment;

/**
 * Class for creation of custom styled charts.
 * 
 * @author mfranke
 */
@SuppressWarnings("unused")
public class ChartCreator {

	private static final String STYLE_LINE = "line";   
	private static final String STYLE_DOT = "dot";
    private static final String STYLE_DASH_1 = "dash1";
    private static final String STYLE_DASH_2 = "dash2";
    private static final String STYLE_DASH_3 = "dash3";
    private static final String STYLE_DASH_4 = "dash4";
    private static final String STYLE_DASH_5 = "dash5";
    private static final String STYLE_DASH_6 = "dash6";
	
	private static final Color DEFAULT_BACKGROUND_COLOR = Color.white;
	private static final Color DEFAULT_PLOT_BACKGROUND_COLOR = Color.white;
	private static final Color DEFAULT_GRID_COLOR = Color.lightGray;
	private static final DecimalFormat PERCENT_FORMAT_IS = new DecimalFormat("##'%'");
	private static final NumberFormat PERCENT_FORMAT_TO = NumberFormat.getPercentInstance();
	
	
	/** ===================================== Common Customizations ================================= **/
	
	private static void customizeTitle(JFreeChart chart){
		final Font font = chart.getTitle().getFont().deriveFont(Font.PLAIN, 32);
		chart.getTitle().setFont(font);
	}
	
	private static void customizeAxes(JFreeChart chart, boolean showPercentOnNumberAxis){
//		final CategoryAxis xAxis = chart.getCategoryPlot().getDomainAxis();		
		final NumberAxis xAxis =(NumberAxis)  chart.getXYPlot().getDomainAxis();
		xAxis.setLabelFont(xAxis.getLabelFont().deriveFont(Font.PLAIN, 18));
		xAxis.setTickLabelFont(xAxis.getLabelFont().deriveFont(Font.PLAIN, 15));
		xAxis.setLabelPaint(Color.BLACK);
		xAxis.setTickLabelPaint(Color.BLACK);
		
				xAxis.setLowerBound(1);
		xAxis.setLowerMargin(0.02d);
		xAxis.setUpperMargin(0.02d);
//		xAxis.setCategoryMargin(0.2d);
		xAxis.setTickMarkPaint(Color.BLACK);
		xAxis.setAxisLinePaint(Color.BLACK);
		
		try{
//			final NumberAxis yAxis = (NumberAxis) chart.getCategoryPlot().getRangeAxis();	
			final NumberAxis yAxis = (NumberAxis) chart.getXYPlot().getRangeAxis();
			yAxis.setLabelFont(yAxis.getLabelFont().deriveFont(Font.PLAIN, 18));
			yAxis.setLabelPaint(Color.BLACK);
			yAxis.setTickLabelFont(yAxis.getLabelFont().deriveFont(Font.PLAIN, 15));
			yAxis.setTickLabelPaint(Color.BLACK);
			yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
			yAxis.setTickMarkPaint(Color.BLACK);
			yAxis.setAxisLinePaint(Color.BLACK);
			yAxis.setUpperBound(8);
			yAxis.setLowerBound(1);
			
			if (showPercentOnNumberAxis){
		        yAxis.setNumberFormatOverride(PERCENT_FORMAT_IS);     
		        yAxis.setUpperBound(100);
		        yAxis.setLowerBound(0);
		    }
		}
		catch(ClassCastException e){}
	}
	
	private static void customizeBackground(JFreeChart chart){
		chart.setBackgroundPaint(DEFAULT_BACKGROUND_COLOR);
		chart.setBorderVisible(false);
		
//		final CategoryPlot plot = chart.getCategoryPlot(); 	
		final XYPlot plot = chart.getXYPlot();
		plot.setOutlinePaint(Color.BLACK);
		plot.setBackgroundPaint(DEFAULT_PLOT_BACKGROUND_COLOR);
        plot.setDomainGridlinePaint(DEFAULT_GRID_COLOR);
        plot.setRangeGridlinePaint(DEFAULT_GRID_COLOR);
              
        chart.setPadding(new RectangleInsets(16,0,4,8));
	}
	
	private static void customizeLegend(JFreeChart chart) {
		final LegendTitle legend = chart.getLegend();
		legend.setItemPaint(Color.BLACK);
        legend.setPosition(RectangleEdge.BOTTOM);
        legend.setMargin(10d, 8d, 2d, 1d);
//        legend.setMargin(8d, 8d, 2d, 4d);
        legend.setItemFont(legend.getItemFont().deriveFont(15f));
	}
	
	/** ============================================== Bar Chart ================================== **/
	
	/**
	 * Creates a new statistical bar chart.
	 * 
	 * @param chartData	the data to represent in this chart.
	 * @param ignoreTitle flag indicating whether or not the title from the {@link ChartData} should be used.
	 * @param showPercentOnNumberAxis flag indicating whether or not the y-axis shows percentage values.
	 * @param blackWhite flag indicating whether or not the chart should print in black and white or with colors.
	 * @return the created chart.
	 */
	public static JFreeChart createStatisticalBarChart(ChartData chartData, boolean ignoreTitle, boolean showPercentOnNumberAxis, boolean blackWhite){
		return 
			createStatisticalBarChart(
				chartData.getDataset(), 
				showPercentOnNumberAxis, 
				(ignoreTitle) ? "" : chartData.getTitle(), 
				chartData.getxAxisLabel(), 
				chartData.getyAxisLabel(), 
				blackWhite
			);
	}
	
	/**
	 * Creates a new statistical bar chart.
	 * 
	 * @param statisticalCategoryDataset the data set to represent in this chart.
	 * @param showPercentOnNumberAxis flag indicating whether or not the y-axis shows percentage values.
	 * @param chartTitle the title of the chart.
	 * @param xAxisLabel the label for the x-axis.
	 * @param yAxisLabel the label for the y-axis.
	 * @param blackWhite flag indicating whether or not the chart should print in black and white or with colors.
	 * @return the created chart.
	 */
	public static JFreeChart createStatisticalBarChart(CategoryDataset statisticalCategoryDataset,
			boolean showPercentOnNumberAxis,
			String chartTitle, String xAxisLabel, String yAxisLabel, boolean blackWhite){
		
		final CategoryAxis xAxis = new CategoryAxis(xAxisLabel);

		final NumberAxis yAxis = new NumberAxis(yAxisLabel);
        // enable for log-scale
//		final LogAxis yAxis = new LogAxis(yAxisLabel);     
//        yAxis.setSmallestValue(1d);
//        yAxis.setRange(1d, 10000d);
//        yAxis.setBase(10);
//        yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
      
        final CategoryItemRenderer renderer = new StatisticalBarRenderer();
        
        final CategoryPlot plot = new CategoryPlot(
        		statisticalCategoryDataset, 
        		xAxis, 
        		yAxis, 
        		renderer
        );
        
		JFreeChart chart = new JFreeChart(chartTitle, JFreeChart.DEFAULT_TITLE_FONT, plot , true);

		customizeBarChart(chart, blackWhite, showPercentOnNumberAxis);
		return chart;
	}
		
	private static void customizeBarChart(JFreeChart chart, boolean blackWhite, boolean showPercentOnNumberAxis){
		customizeTitle(chart);
		customizeAxes(chart, showPercentOnNumberAxis);
		customizeBackground(chart);        
        customizeLegend(chart);
        
        CategoryPlot plot = chart.getCategoryPlot();
        BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setItemMargin(0.0);
        renderer.setBarPainter(new StandardBarPainter());
        
        if (blackWhite){
        	makeBarChartBlackWhite(renderer);
        }
	}
	
	private static void makeBarChartBlackWhite(BarRenderer renderer){
		renderer.setDrawBarOutline(true);
		
		renderer.setSeriesPaint(0, Color.WHITE);
        renderer.setSeriesPaint(1, Color.LIGHT_GRAY);
        renderer.setSeriesPaint(2, Color.DARK_GRAY);
        renderer.setSeriesPaint(3, Color.BLACK);
        renderer.setSeriesPaint(4, new GradientPaint(0, 0, Color.WHITE, 0, 0, Color.DARK_GRAY, true));
        renderer.setSeriesPaint(5, new GradientPaint(0, 0, Color.DARK_GRAY, 0, 0, Color.WHITE));
        renderer.setSeriesPaint(6, new GradientPaint(0, 0, Color.WHITE, 0, 0, Color.BLACK));
        renderer.setSeriesPaint(7, new GradientPaint(0, 0, Color.BLACK, 0, 0, Color.WHITE));
        
        
        renderer.setGradientPaintTransformer(
        	new StandardGradientPaintTransformer(
        		GradientPaintTransformType.VERTICAL
        	)
        );
	}
	
	
	/** ============================================== Line Chart ================================= **/
	
	public static JFreeChart createSpeedUpLineChart(XYDataset dataset){
		JFreeChart lineChart = 
				ChartFactory.createXYLineChart(
					"",
					"Flink-Job-Parallelität", 
					"Speedup", 
					dataset
				);
		customizeLineChart(lineChart, true);
		return lineChart;
	}
	
	/**
	 * Creates a new line chart.
	 * 
	 * @param chartData	the data to represent in this chart.
	 * @param ignoreTitle flag indicating whether or not the title from the {@link ChartData} should be used.
	 * @param blackWhite flag indicating whether or not the chart should print in black and white or with colors.
	 * @return the created chart.
	 */
	public static JFreeChart createLineChart(ChartData chartData, boolean ignoreTitle, boolean blackWhite){
		return 
			createLineChart(
				chartData.getDataset(), 
				(ignoreTitle) ? "" : chartData.getTitle(), 
				chartData.getxAxisLabel(), 
				chartData.getyAxisLabel(), 
				blackWhite
			);
	}
	
	/**
	 * Creates a new line chart.
	 * 
	 * @param categoryDataset the data set to represent in this chart.
	 * @param chartTitle the title of the chart.
	 * @param xAxisLabel the label for the x-axis.
	 * @param yAxisLabel the label for the y-axis.
	 * @param blackWhite flag indicating whether or not the chart should print in black and white or with colors.
	 * @return the created chart.
	 */
	public static JFreeChart createLineChart(CategoryDataset categoryDataset, String charTitle, String xAxisLabel, String yAxisLabel,
			boolean blackWhite){
		JFreeChart lineChart = 
			ChartFactory.createLineChart(
				charTitle, 
				xAxisLabel, 
				yAxisLabel, 
				categoryDataset
			);
		
		// enable for log-scale
//		final LogAxis yAxis = new LogAxis(yAxisLabel);     
//        yAxis.setSmallestValue(1d);
//        yAxis.setRange(1d, 1000000d);
//        yAxis.setBase(10);
//        yAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        
//        lineChart.getCategoryPlot().setRangeAxis(yAxis);
		
		customizeLineChart(lineChart, blackWhite);
		return lineChart;
	}

	
	private static void customizeLineChart(JFreeChart chart, boolean blackWhite){
		customizeTitle(chart);
		customizeAxes(chart, false);
        customizeBackground(chart);
        customizeLegend(chart);
        
        
//        StatisticalLineAndShapeRenderer renderer = new StatisticalLineAndShapeRenderer(true, true); 
//        CategoryPlot plot = chart.getCategoryPlot();
        XYPlot plot = chart.getXYPlot();
        
      XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        plot.setRenderer(renderer);
        
        renderer.setBaseShapesFilled(true);
        renderer.setBaseShapesVisible(true);
        renderer.setAutoPopulateSeriesStroke(false);

        if (blackWhite){
        	makeLineChartBlackWhite(renderer);
        }
	}
	
	private static void makeLineChartBlackWhite(XYLineAndShapeRenderer renderer){
		renderer.setBasePaint(Color.BLACK);
		renderer.setAutoPopulateSeriesPaint(false);
	
		renderer.setSeriesStroke(0, toStroke(STYLE_LINE)); 
		renderer.setSeriesStroke(1, toStroke(STYLE_DOT)); 
		renderer.setSeriesStroke(2, toStroke(STYLE_DASH_1)); 
		renderer.setSeriesStroke(3, toStroke(STYLE_DASH_2)); 
		renderer.setSeriesStroke(4, toStroke(STYLE_DASH_3)); 
		renderer.setSeriesStroke(5, toStroke(STYLE_DASH_4));
		renderer.setSeriesStroke(6, toStroke(STYLE_DASH_5)); 
		renderer.setSeriesStroke(7, toStroke(STYLE_DASH_6)); 		
	}
	  
	private static BasicStroke toStroke(String style) {
        BasicStroke result = null;
       
        if (style != null) {
            float lineWidth = 0.8f;
            float dot[] = {lineWidth};
   
            if (style.equalsIgnoreCase(STYLE_LINE)) {
                result = new BasicStroke(lineWidth);
            } 
            
            else if (style.equalsIgnoreCase(STYLE_DOT)) {
                result = 
                	new BasicStroke(
                		lineWidth, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 
                		2.0f, dot, 0.0f
                	);
            }
            else if (style.equalsIgnoreCase(STYLE_DASH_1)){
            	result = 
            		new BasicStroke(
            			lineWidth, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f, 
            			new float[] {12.0f, 5.0f}, 0.0f
            		);	
            }
            else if(style.equalsIgnoreCase(STYLE_DASH_2)){
            	result = 
                	new BasicStroke(
                		lineWidth, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 1.0f, 
                		new float[] {10.0f, 5.0f, 1.0f, 4.0f, 1.0f, 4.0f}, 0.0f
                	);
            }
            else if(style.equalsIgnoreCase(STYLE_DASH_3)){
            	result =         
            		new BasicStroke(
            			lineWidth, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                        1.0f, new float[] {6.0f, 6.0f}, 0.0f
                    );
            }
            else if (style.equalsIgnoreCase(STYLE_DASH_4)) {
                result = 
                	new BasicStroke(
                		lineWidth, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 
                		10.0f, new float[] {3f, 7.0f}, 0.0f
                	);
            }
            else if(style.equalsIgnoreCase(STYLE_DASH_5)){
            	result = 
            		new BasicStroke(
            			lineWidth, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                        1.0f, new float[] {0.5f, 3.0f}, 0.0f
                    );
            }
            else if(style.equalsIgnoreCase(STYLE_DASH_6)){
            	result = 
            		new BasicStroke(
            			lineWidth, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                        1.0f, new float[] {0.2f, 8.0f}, 0.0f
                    );
            }                   
        }
       
        return result;
    }
}