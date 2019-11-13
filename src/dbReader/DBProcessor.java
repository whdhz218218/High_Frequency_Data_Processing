package dbReader;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import dbReaderFramework.I_DBProcessor;
import dbReaderFramework.I_DBReader;
import taqDBReaders.GZFileUtils;

// This class merges two dat files and write into a third dat file

public class DBProcessor implements I_DBProcessor{
	
	private DataOutputStream _dataOutputStream;   // Output stream used to write merged files
	private Random _randomGenerator=new Random();
	private String _outputFolderName;  // Folder where the output files are
	private String _outputPath;        // Path of the output file
	private int _ver;                  // Version of the merged file
	
	
	public DBProcessor(String outputFolderName, int ver) throws IOException {
		_outputFolderName=outputFolderName;
		_ver=ver;
		_outputPath=_outputFolderName+_randomGenerator.nextInt(Integer.MAX_VALUE)+"_"+_ver+".dat";
		_dataOutputStream = GZFileUtils.getGZippedFileOutputStream(_outputPath);
	}
	
	public String getoutputPath() {return _outputPath;}
	
	/** DBManager uses to pass readers to a processor.
	 * @param sequenceNumber        
	 * @param numReadersWithNewData 
	 * @param readers  
	 */
	@Override
	public boolean processReaders(long sequenceNumber, int numReadersWithNewData, LinkedList<I_DBReader> readers) {

		DATTradesDBReader reader1 = (DATTradesDBReader) readers.getFirst();
		DATTradesDBReader reader2 = (DATTradesDBReader) readers.getLast();
	
		if( reader1.getLastSequenceNumberRead() == sequenceNumber ) {
			int nRecs = reader1.getNRecsRead();
			try {
				for( int i = 0; i < nRecs; i++ ) {
					_dataOutputStream.writeLong( reader1.getMilliSecondsFromTheEpoch(i));
					_dataOutputStream.writeShort( reader1.gettickerByteList(i));
					_dataOutputStream.writeInt(reader1.getSizeList(i));
					_dataOutputStream.writeFloat(reader1.getPriceList(i));
				}
			} catch (IOException e) {
				return false;
			}
		}
		if( reader2.getLastSequenceNumberRead() == sequenceNumber ) {
			int nRecs = reader2.getNRecsRead();
			try {
				for( int i = 0; i < nRecs; i++ ) {
					_dataOutputStream.writeLong( reader2.getMilliSecondsFromTheEpoch(i));
					_dataOutputStream.writeShort( reader2.gettickerByteList(i));
					_dataOutputStream.writeInt(reader2.getSizeList(i));
					_dataOutputStream.writeFloat(reader2.getPriceList(i));
				}
			} catch (IOException e) {
				return false;
			}
		}
		
		return true;
	}
	
	/** Stop this processor by closing files
	 * @throws Exception 
	 */
	@Override
	public void stop() throws Exception {
		_dataOutputStream.flush();
		_dataOutputStream.close();
		
	}
	
	
	
	

}
