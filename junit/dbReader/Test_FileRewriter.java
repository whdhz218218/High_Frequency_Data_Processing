package dbReader;

import java.io.DataInputStream;

import junit.framework.TestCase;
import reWriter.FileRewriter;
import taqDBReaders.GZFileUtils;

public class Test_FileRewriter extends TestCase {
	
	private FileRewriter _fileRewriter;
	private String _inputFolder = "trades\\20070620";
	private String _outputFolder="new\\";
	
	public Test_FileRewriter() throws Exception {
		_fileRewriter = new FileRewriter(_inputFolder, _outputFolder);
	}
	
	public void testConstructor() {
		assertTrue(_fileRewriter.getFiles().length == 4);
	}
	
	public void testtickerToByte() throws Exception {
		_fileRewriter.tickerToByte("IBM"); 
		assertTrue(_fileRewriter.gettickerToByteMap().size() == 1);
	}
	
	public void testRewrite() throws Exception {
		_fileRewriter.tickerToByte("IBM"); 
		_fileRewriter.Rewrite(_inputFolder+"\\IBM_trades.binRT", "IBM");
		assertTrue(_fileRewriter.getoutputPathName().equals(_outputFolder));
		DataInputStream dataInputStream = GZFileUtils.getGZippedFileInputStream(_fileRewriter.getoutputPath());

		assertTrue(dataInputStream.readLong() == 1182346241000L);
		dataInputStream.readShort();
		assertTrue(dataInputStream.readInt() == 85200);
		assertTrue(dataInputStream.readFloat()==106.5);			
	}
	
	
	public void testRewriteAll() throws Exception {
		_fileRewriter.RewriteAll();
	}
	

}
