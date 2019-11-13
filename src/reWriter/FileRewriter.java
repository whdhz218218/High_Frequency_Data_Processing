package reWriter;

import java.io.*;
import java.util.HashMap;
import java.util.Random;

import taqDBReaders.GZFileUtils;
import taqDBReaders.ReadGZippedTAQTradesFile;

// This class reWrite binRT files into dat files
public class FileRewriter {
	
	private Random _randomGenerator=new Random();
	private File[] _files;
	//Filter the files whose names contain "trades"
	private FilenameFilter _tradesFilter = new FilenameFilter() {
	    public boolean accept(File file, String name) {
	        if (name.contains("trades")) {return true;} 
	        else {return false;}
	    }
	};	
	
	private HashMap<String, Short> _tickerToByteMap=new HashMap<String, Short>();
	private String _outputFolder;
	private String _outputPath;
	
	
	public File[] getFiles() {return _files;}
	public HashMap<String, Short> gettickerToByteMap() {return _tickerToByteMap;}
	public String getoutputPathName() {return _outputFolder;}
	public String getoutputPath() {return _outputPath;}
	
	
	public FileRewriter(String inputFolder, String outputFolder) throws Exception {
		_files = new File(inputFolder).listFiles(_tradesFilter);
		if (_files.length==0) {throw new Exception("No such files");}
		_outputFolder=outputFolder;
	}
	
	/** Replace each ticker with a unique ID number
	 * @param ticker
	 * @throws Exception
	 */
	public void tickerToByte(String ticker) throws Exception{
		if(_tickerToByteMap.containsKey(ticker)) {throw new Exception("No Repeated files for the same ticker");	}
		else {
			short tickerByte=(short) _randomGenerator.nextInt(Short.MAX_VALUE);
			_tickerToByteMap.put(ticker, tickerByte);
		}
	}
	
    /** Rewrite one binRT file into a dat file with known ticker name and input path
     * @param inputFilePathName
     * @param ticker
     * @throws IOException
     */
	public void Rewrite(String inputFilePathName, String ticker) throws IOException {
		// Read the binRT file into the GZippedTAQTradesFile reader 
		ReadGZippedTAQTradesFile readTAQTradesFile = new ReadGZippedTAQTradesFile(inputFilePathName);
		// Get a unique random number for each output file
		int random =_randomGenerator.nextInt(Integer.MAX_VALUE);
		_outputPath=_outputFolder+random+"_1.dat";
		DataOutputStream dataOutputStream = GZFileUtils.getGZippedFileOutputStream(_outputPath);
		
		// Get the size, price, and milliseconds from midnight for each record from the reader
		int nRecs = readTAQTradesFile.getNRecs();
		short tickerByte = _tickerToByteMap.get(ticker);
		long millisecondsFromEpoch = readTAQTradesFile.getSecsFromEpoch()*1000L; 
		
		for( int i = 0; i < nRecs; i++ ) {	
			// Write the new information to the designated file
			dataOutputStream.writeLong( millisecondsFromEpoch+ readTAQTradesFile.getMillisecondsFromMidnight (i));
			dataOutputStream.writeShort(tickerByte);
			dataOutputStream.writeInt(readTAQTradesFile.getSize(i));
			dataOutputStream.writeFloat(readTAQTradesFile.getPrice(i));
		}		
		//Flush and close the output stream
		dataOutputStream.flush();
		dataOutputStream.close();		
	}
		
	/** Rewrite all the binRT files with specified symbol to dat files and output 
	 * them to the output folder
	 * @throws Exception
	 */
	public void RewriteAll() throws Exception {
		String ticker=null;
		for(File file: _files) {
			ticker=file.getName().split("_")[0];
			tickerToByte(ticker);
			Rewrite(file.getPath(), ticker);		
		}
	}
	
	
	
		














}
