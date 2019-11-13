package merge;

import java.io.File;
import java.io.FilenameFilter;

import junit.framework.TestCase;

public class Test_Merge extends TestCase {
	
	private String _outputFolder="new\\";
	private String _inputFolder="trades\\20070620";
	private FilenameFilter _verFilter = new FilenameFilter() {
	    public boolean accept(File file, String name) {
	        if (name.contains("_1")) {return true;} 
	        else {return false;}
	    }
	};
	
	public void testMerge() throws Exception {
		File[] files = new File(_outputFolder).listFiles(_verFilter);
		Merge merge = new Merge(_outputFolder,_inputFolder);
		merge.setVer(2);
		merge.merge(files[0],files[1]);
	}
	

}
