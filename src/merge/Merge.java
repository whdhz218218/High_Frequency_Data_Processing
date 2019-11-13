package merge;

import java.io.File;
import java.io.FilenameFilter;
import java.util.LinkedList;

import dbReader.DATTradesDBReader;
import dbReader.DBProcessor;
import dbReaderFramework.*;
import reWriter.FileRewriter;

// This class rewrites all the binRT files to the dat files and merge all of them 
// into one for each date directory
public class Merge {
	
	private String _outputFolder;
	private int _ver;
	private String _inputFolder;
	//Filter the files whose names contain "_" and some specified version
	private FilenameFilter _verFilter = new FilenameFilter() {
	    public boolean accept(File file, String name) {
	        if (name.contains("_"+_ver)) {return true;} 
	        else {return false;}
	    }
	};
	//Filter the files whose names contain "_"
	private FilenameFilter _finalFilter = new FilenameFilter() {
	    public boolean accept(File file, String name) {
	        if (name.contains("_")) {return true;} 
	        else {return false;}
	    }
	};
	
	
	public Merge(String outputFolder, String inputFolder) {
		_outputFolder=outputFolder;
		_ver=1;
		_inputFolder= inputFolder;
	}
	
	public void setVer(int ver) {_ver=ver;}
	
	/** Rewrite all binRT files to .dat files to designated folder
	 */
	public void rewriteFiles() throws Exception {
		FileRewriter fileRewriter = new FileRewriter(_inputFolder, _outputFolder);
		fileRewriter.RewriteAll();
	}
	
	/** Merge two dat files into one dat file
	 * @param file1
	 * @param file2
	 * @throws Exception
	 */
	public void merge(File file1, File file2) throws Exception {
		LinkedList<I_DBReader> readers = new LinkedList<I_DBReader>();      		
		DATTradesDBReader dbReader1 = new DATTradesDBReader(file1.getPath());
		DATTradesDBReader dbReader2 = new DATTradesDBReader(file2.getPath());
		readers.add( dbReader1);
		readers.add( dbReader2);
		
		LinkedList<I_DBProcessor> processors = new LinkedList<I_DBProcessor>();
		DBProcessor dbProcessor = new DBProcessor( _outputFolder, _ver );
		processors.add(dbProcessor);
	
		MergeClock mergeClock = new MergeClock(readers, processors);
			
		DBManager dbManager = new DBManager(readers, processors, mergeClock);	
		dbManager.launch();
	}
	
	/** If there are even numbers of the files, Merge two dat files into one dat file. 
	 * But if there are odd numbers of files, wait until other files are merged into one file,
	 * then merge the last file and the merged file
	 * @throws Exception
	 */
	public void mergeAll() throws Exception {
		File[] files = new File(_outputFolder).listFiles(_verFilter);
		if(files.length % 2 == 0) {
			while(files.length>1) {
				_ver++;
				for(int i=0; i<files.length;i+=2) {
					merge(files[i], files[i+1]);
				}	
				files = new File(_outputFolder).listFiles(_verFilter);
			}
		}
		else {
			while(files.length>2) {
				_ver++;
				for(int i=0; i<files.length-1;i+=2) {
					merge(files[i], files[i+1]);
				}	
				files = new File(_outputFolder).listFiles(_verFilter);
			}
			files = new File(_outputFolder).listFiles(_finalFilter);
			if(files.length==2) {merge(files[0],files[1]);}
		}
		
	}
	


}
