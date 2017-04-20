package dbs.bigdata.flink.pprl.utils.common;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Utility class for reading files.
 * @author mfranke
 *
 */
public final class FileUtils {

	private FileUtils(){
		throw new RuntimeException();
	}
	
	/**
	 * Retrieves all files of the same type in the specified directory.
	 * 
	 * @param directoryPath the directory path.
	 * @param suffix the file suffix specifying the type of file (e.g. csv).
	 * @return
	 */
	public static File[] getFilesByType(String directoryPath, String suffix){
		final File dir = new File(directoryPath);
	    
		final File[] files = 
			dir.listFiles(
				new FilenameFilter(){
			    	public boolean accept(File dir, String filename){ 
			    		return filename.endsWith(suffix); 
			    	}
			    }
			);
		
		return files;
	}
	
	/**
	 * Reads a file into a String.
	 * @param file the file to read.
	 * @return the content of the file.
	 * @throws IOException
	 */
	public static String readFile(File file) throws IOException{
		return readFile(file, Charset.defaultCharset());
	}
	
	/**
	 * Reads a file into a String.
	 * @param file the file to read.
	 * @param encoding the encoding that should be used.
	 * @return the content of the file.
	 * @throws IOException
	 */
	public static String readFile(File file, Charset encoding) throws IOException{
		final String path = file.getPath();
		final byte[] bytes = Files.readAllBytes(Paths.get(path));
		return new String(bytes, encoding);
	}	
	
	/**
	 * Reads the file under the specified path into a String.
	 * @param path the path to the file.
	 * @return the content of the file.
	 * @throws IOException
	 */
	public static String readFile(String path) throws IOException{
		return readFile(path, Charset.defaultCharset());
	}
	
	/**
	 * Reads the file under the specified path into a String.
	 * @param path the path to the file.
	 * @param encoding the  encoding that should be used.
	 * @return the content of the file.
	 * @throws IOException
	 */
	public static String readFile(String path, Charset encoding) throws IOException{
		final byte[] bytes = Files.readAllBytes(Paths.get(path));
		return new String(bytes, encoding);
	}	
}