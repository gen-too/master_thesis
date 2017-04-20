package dbs.bigdata.flink.pprl.evaluation.graphics;

import java.awt.Graphics2D;
import java.awt.geom.Rectangle2D;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.jfree.chart.JFreeChart;

import com.itextpdf.awt.FontMapper;
import com.itextpdf.awt.PdfGraphics2D;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Rectangle;
import com.itextpdf.text.pdf.PdfContentByte;
import com.itextpdf.text.pdf.PdfTemplate;
import com.itextpdf.text.pdf.PdfWriter;

/**
 * Class for printing a {@link JFreeChart} to a pdf document.
 * 
 * @author mfranke
 */
public class ChartPrinter {
	public static final float DEFAULT_WIDTH = 600;
	public static final float DEFAULT_HEIGHT = 400;
	
	private static final String DEFAULT_CUSTOM_FONT = "src/main/resources/ubuntu-font-family-0.83/Ubuntu-R.ttf";
	public static final CustomFontMapper DEFAULT_FONT_MAPPER = new CustomFontMapper(DEFAULT_CUSTOM_FONT);
	
	
	//private static final Rectangle PAGE_SIZE_A4 = PageSize.A4;
	private static final float DEFAULT_PAGE_MARGIN_LEFT = 50;
	private static final float DEFAULT_PAGE_MARGIN_RIGHT = 50;
	private static final float DEFAULT_PAGE_MARGIN_TOP = 50;
	private static final float DEFAULT_PAGE_MARGIN_BOTTOM = 50;
	private static final String JFREECHART_CREATOR_TAG = "JFreeChart";
	
	
	/**
	 * Prints a {@link JFreeChart} to a pdf file.
	 * 
	 * @param chart the chart to print.
	 * @param filename the specification of the destination file.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public static void printPdf(JFreeChart chart, File filename) throws IOException, DocumentException{
		printPdf(chart, filename, DEFAULT_FONT_MAPPER, DEFAULT_WIDTH, DEFAULT_HEIGHT);
	}
	
	/**
	 * Prints a {@link JFreeChart} to a pdf file.
	 * 
	 * @param chart the chart to print.
	 * @param filename the specification of the destination file.
	 * @param mapper a {@link FontMapper}.
	 * @param width the width of the document.
	 * @param height the height of the document.
	 * @throws IOException
	 * @throws DocumentException
	 */
	public static void printPdf(JFreeChart chart, File filename, FontMapper mapper, float width, float height) 
			throws IOException, DocumentException {
		
		Rectangle pagesize = new Rectangle(width, height);
		
		Document document = 
			new Document(
				pagesize, 
				DEFAULT_PAGE_MARGIN_LEFT, 
				DEFAULT_PAGE_MARGIN_RIGHT, 
				DEFAULT_PAGE_MARGIN_TOP,
				DEFAULT_PAGE_MARGIN_BOTTOM
			);

        PdfWriter writer = PdfWriter.getInstance(document, new FileOutputStream(filename));
		document.addTitle(chart.getTitle().getText());
		document.addCreator(JFREECHART_CREATOR_TAG);
        document.open();

        PdfContentByte content = writer.getDirectContent();
        PdfTemplate template = content.createTemplate(width, height);
            
        Graphics2D g2d1 = new PdfGraphics2D(template, width, height, mapper);
        Rectangle2D r2d1 = new Rectangle2D.Double(0, 0, width, height);
       
        chart.draw(g2d1, r2d1);
        g2d1.dispose();
       
        content.addTemplate(template, 0, 0);

        document.close();
    }
}