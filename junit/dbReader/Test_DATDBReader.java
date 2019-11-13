package dbReader;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import junit.framework.TestCase;

public class Test_DATDBReader extends TestCase {
	
	private String _outputFolderPath="new\\";
	
	private FilenameFilter _1Filter = new FilenameFilter() {
	    public boolean accept(File file, String name) {
	        if (name.contains("_1")) {return true;} 
	        else {return false;}
	    }
	};
	private File[] _files;
	private DATTradesDBReader _dbReader;
	
	
	public Test_DATDBReader() throws IOException {
		_files = new File(_outputFolderPath).listFiles(_1Filter);
        _dbReader = new DATTradesDBReader(_files[0].getPath());
	}
	
	public void testConstructor() {
		assertTrue(_dbReader.getrecCount() == 0);
		assertTrue(_dbReader.getNRecsRead() == 0);
		assertTrue( !_dbReader._isFinished);
		assertTrue( _dbReader.getLastSequenceNumberRead() == 0);
	}
	
	public void testReadChunk() throws IOException {
		_dbReader.readChunk(0);
		assertTrue(_dbReader.getrecCount() == 0);
		assertTrue(_dbReader.getNRecsRead() == 0);
		assertTrue( !_dbReader._isFinished);
		assertTrue( _dbReader.getLastSequenceNumberRead() == 0);		
	}

	public void testReadChunk1record() throws IOException {
		_dbReader.readChunk(1182346241000L);
		assertTrue(_dbReader.getrecCount() == 1);
		assertTrue( _dbReader.getNRecsRead() == 1);
		assertTrue( !_dbReader._isFinished);
		assertTrue( _dbReader.getLastSequenceNumberRead() == 1182346241000L);
		assertTrue( _dbReader.getSequenceNumber() == 1182346242000L);
	}
	
	
	public void testReadChunkall() throws IOException {
		_dbReader = new DATTradesDBReader(_files[0].getPath());
		_dbReader.readChunk(11823462410000L);
		System.out.println(_dbReader.getrecCount());
		assertTrue(_dbReader.getrecCount() == 23595);
		assertTrue( _dbReader.getNRecsRead() == 23595);
		assertTrue( _dbReader._isFinished);
		assertTrue( _dbReader.getLastSequenceNumberRead() == 11823462410000L);
	}
	
   

	 

}
