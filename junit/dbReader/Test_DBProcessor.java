package dbReader;

import java.util.LinkedList;

import junit.framework.TestCase;
import reWriter.FileRewriter;
import dbReaderFramework.*;

public class Test_DBProcessor extends TestCase {
	
	private int ver=2;
	private FileRewriter _fileRewriter;
	
	private String _pathName = "trades\\20070620";
	private String _outputFolderPath="new\\";
	
	private LinkedList<I_DBReader> _readers = new LinkedList<I_DBReader>();
	private DBProcessor _dbProcessor;
	private DBProcessor _dbProcessor2;
	
	public Test_DBProcessor() throws Exception {	
		_dbProcessor = new DBProcessor(_outputFolderPath, ver);
		_dbProcessor2 = new DBProcessor(_outputFolderPath, ver);
		_fileRewriter = new FileRewriter(_pathName, _outputFolderPath);
	}
	
	
    public void testProcessReaders() throws Exception {
    	_fileRewriter.tickerToByte("GE"); 
    	_fileRewriter.Rewrite(_pathName+"\\GE_trades.binRT", "GE");
    	String path1=_fileRewriter.getoutputPath();  	
    	_fileRewriter.tickerToByte("IBM"); 
    	_fileRewriter.Rewrite(_pathName+"\\IBM_trades.binRT", "IBM");
    	String path2=_fileRewriter.getoutputPath();
    	
		DATTradesDBReader dbReader1 = new DATTradesDBReader(path1);		
		DATTradesDBReader dbReader2 = new DATTradesDBReader(path2);
	
		_readers.add(dbReader1);
		_readers.add(dbReader2);
		
		dbReader1.readChunk(1182346200000L);
		dbReader2.readChunk(1182346200000L);
		
		
		Boolean isNotFinished = _dbProcessor2.processReaders(1182346200000L, 2, _readers);
		_dbProcessor2.stop(); 
		
		String path3 = _dbProcessor2.getoutputPath();
		DATTradesDBReader dbReader3 = new DATTradesDBReader(path3);
		
		dbReader3.readChunk(1182346200000L);
		assertTrue(dbReader3.getrecCount() == 2);
		assertTrue(dbReader3.getNRecsRead() == 2);
		assertTrue(dbReader3.isFinished());
		assertTrue(dbReader3.getLastSequenceNumberRead() == 1182346200000L);
		assertTrue(isNotFinished);		
	}

}
