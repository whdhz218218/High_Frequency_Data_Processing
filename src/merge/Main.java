package merge;

import java.io.File;

public class Main {

	public static void main(String[] args) throws Exception {
		

		String folder1 = "trades\\20070620";
		String folder2 = "trades\\20070621";
		String outputFolder="new\\";
		
		new File(outputFolder+"20070620").mkdir();
		new File(outputFolder+"20070621").mkdir();
		Merge merge1 = new Merge(outputFolder+"20070620\\",folder1);
		merge1.rewriteFiles();
		merge1.mergeAll();
		Merge merge2 = new Merge(outputFolder+"20070621\\",folder2);
		merge2.rewriteFiles();
		merge2.mergeAll();

	}
	

}
