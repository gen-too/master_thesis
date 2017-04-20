package dbs.bigdata.flink.pprl.evaluation.graphics;

import java.awt.Font;
import java.io.IOException;

import org.jfree.chart.JFreeChart;

import com.itextpdf.awt.FontMapper;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.pdf.BaseFont;

/**
 * Class that allows the usage of custom fonts in a {@link JFreeChart}.
 * 
 * @author mfranke
 */
public class CustomFontMapper implements FontMapper{

	private final String font;
	
	/**
	 * Creates a new {@link CustomFontMapper}.
	 * 
	 * @param font the path to the ttf-file of a font.
	 */
	public CustomFontMapper(String font) {
		this.font = font;
	}
	
	@Override
	public BaseFont awtToPdf(Font font) {
		try {
            return 
            	BaseFont.createFont(
            		this.font,
            		BaseFont.IDENTITY_H, 
            		BaseFont.EMBEDDED
            	);
            
        } 
        catch (DocumentException e) {
            e.printStackTrace();
        } 
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
	}

	@Override
	public Font pdfToAwt(BaseFont font, int size) {
		return null;
	}
	
}