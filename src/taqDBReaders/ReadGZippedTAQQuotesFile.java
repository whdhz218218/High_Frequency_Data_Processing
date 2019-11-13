package taqDBReaders;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/** This class reads the gzipped file in the one-file-per-day-per-stock format 
 * that was described in class and can also be read in the R statistical
 * programming language. 
 * 
 * <P> The entire file is read into memory. Individual fields
 * can be accessed with get methods. For the header, the get methods returns
 * the number of records and the number of seconds from the epoch - January 1,
 * 1970 - to midnight of the the day that is being read. For other
 * records, an index must be provided to retrieve a field - 0 retrieves
 * a field in record 0, 1 retrieves a field in record 1, and so on.
 * </P>
 * 
 * <P>
 * Header fields: number of seconds from epoch, number of records in file
 * </P>
 * 
 * <P>
 * Record fields - 1 per record: milliseconds from midnight of that day, size
 * of best bid, price of best bid, size of best offer, price of best offer.
 * </P>
 * 
 * @author Lee
 *
 */
public class ReadGZippedTAQQuotesFile {
	
	// Header fields

		protected int _secsFromEpoch;
		protected int _nRecs;

	// Record fields
		
		protected int   [] _millisecondsFromMidnight;
		protected int   [] _bidSize;
		protected float [] _bidPrice;
		protected int   [] _askSize;
		protected float [] _askPrice;

	public int getSecsFromEpoch () { return _secsFromEpoch; }
	public int getNRecs         () { return _nRecs;         }

	public int   getMillisecondsFromMidnight ( int index ) { return _millisecondsFromMidnight[ index ]; }
	public int   getBidSize                  ( int index ) { return _bidSize[ index ];                  }
	public float getBidPrice                 ( int index ) { return _bidPrice[ index ];                 }
	public int   getAskSize                  ( int index ) { return _askSize[ index ];                  }
	public float getAskPrice                 ( int index ) { return _askPrice[ index ];                 }
	
	/**
	 * Constructor - Opens a gzipped TAQ quotes file and reads entire contents into memory.
	 * 
	 * @param filePathName Name of gzipped TAQ quotes file to read
	 * @throws IOException 
	 */
	public ReadGZippedTAQQuotesFile( String filePathName ) throws IOException {
		
		// Open file 
		
			InputStream in = new GZIPInputStream( new FileInputStream( filePathName ) );
			DataInputStream dataInputStream = new DataInputStream( in );
			
		// Read and save header info

			_secsFromEpoch = dataInputStream.readInt();
			_nRecs = dataInputStream.readInt();
		
		// Allocate space for data
		
			_millisecondsFromMidnight = new int   [ _nRecs ];
			_bidSize                  = new int   [ _nRecs ];
			_bidPrice                 = new float [ _nRecs ];
			_askSize                  = new int   [ _nRecs ];
			_askPrice                 = new float [ _nRecs ];
			
		// Read all records into memory
			
			for( int i = 0; i < _nRecs; i++ )
				_millisecondsFromMidnight[ i ] = dataInputStream.readInt();

			for( int i = 0; i < _nRecs; i++ )
				_bidSize[ i ] = dataInputStream.readInt();

			for( int i = 0; i < _nRecs; i++ )
				_bidPrice[ i ] = dataInputStream.readFloat();

			for( int i = 0; i < _nRecs; i++ )
				_askSize[ i ] = dataInputStream.readInt();

			for( int i = 0; i < _nRecs; i++ )
				_askPrice[ i ] = dataInputStream.readFloat();
			
		// Finished reading - close the stream

			dataInputStream.close();

	}
	
	/**
	 * Example of using this class to read a TAQ quotes file and access
	 * individual records.
	 */
	public static void example1() {
		String f = "/Users/minddrill/TAQ/R/quotes/20070620/IBM_quotes.binRQ";
		try {
			
			// Read entire TAQ quotes file into memory

				ReadGZippedTAQQuotesFile taqQuotes = new ReadGZippedTAQQuotesFile( f );
				
			// Iterate over all records, writing the contents of each to the console
				
				int nRecs = taqQuotes.getNRecs();
				for( int i = 0; i < nRecs; i++ ) {
					System.out.println(
						taqQuotes.getMillisecondsFromMidnight( i )
						+ ","
						+ taqQuotes.getBidSize( i )
						+ ","
						+ taqQuotes.getBidPrice( i )
						+ ","
						+ taqQuotes.getAskSize( i )
						+ ","
						+ taqQuotes.getAskPrice( i )
					);
				}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public static void main( String[] args ) {
		example1();
	}
	
}
