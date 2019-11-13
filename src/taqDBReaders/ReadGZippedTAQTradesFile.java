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
 * of trade, price of trade.
 * </P>
 * 
 * @author Lee
 *
 */
public class ReadGZippedTAQTradesFile {
	
	// Header fields

		protected int _secsFromEpoch;
		protected int _nRecs;

	// Record fields
		
		protected int   [] _millisecondsFromMidnight;
		protected int   [] _size;
		protected float [] _price;

	// Get-ter methods for header fields

		public int getSecsFromEpoch () { return _secsFromEpoch; }
		public int getNRecs         () { return _nRecs;         }

	// Get-ter methods for record fields

		public int   getMillisecondsFromMidnight ( int index ) { return _millisecondsFromMidnight[ index ]; }
		public int   getSize                     ( int index ) { return _size[ index ];                     }
		public float getPrice                    ( int index ) { return _price[ index ];                    }
	
	/**
	 * Constructor - Opens a gzipped TAQ trades file and reads entire contents into memory.
	 * 
	 * @param filePathName Name of gzipped TAQ trades file to read
	 * @throws IOException 
	 */
	public ReadGZippedTAQTradesFile( String filePathName ) throws IOException {
		
		// Open file and get data input stream
		
			FileInputStream fileInputStream = new FileInputStream( filePathName );
			InputStream in = new GZIPInputStream( fileInputStream );
			DataInputStream dataInputStream = new DataInputStream( in );
			
		// Read and save header info

			_secsFromEpoch = dataInputStream.readInt();
			_nRecs = dataInputStream.readInt();
		
		// Allocate space for data
		
			_millisecondsFromMidnight = new int   [ _nRecs ];
			_size                     = new int   [ _nRecs ];
			_price                    = new float [ _nRecs ];
			
		// Read all records into memory
			
			for( int i = 0; i < _nRecs; i++ )
				_millisecondsFromMidnight[ i ] = dataInputStream.readInt();

			for( int i = 0; i < _nRecs; i++ )
				_size[ i ] = dataInputStream.readInt();

			for( int i = 0; i < _nRecs; i++ )
				_price[ i ] = dataInputStream.readFloat();

		// Finished reading - close the stream

			dataInputStream.close();

	}
	
	/**
	 * Example of using this class to read a TAQ trades file and access
	 * individual records.
	 */
	public static void example1() {
		String f = "/Users/minddrill/TAQ/testGZ/IBM_trades.binRT";
		try {
			
			// Read entire TAQ trades file into memory

				ReadGZippedTAQTradesFile taqTrades = new ReadGZippedTAQTradesFile( f );
				
			// Iterate over all records, writing the contents of each to the console
				
				int nRecs = taqTrades.getNRecs();
				for( int i = 0; i < nRecs; i++ ) {
					System.out.println(
						taqTrades.getMillisecondsFromMidnight( i )
						+ ","
						+ taqTrades.getSize( i )
						+ ","
						+ taqTrades.getPrice( i )
					);
				}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	/*
	public static void main( String[] args ) {
		example1();
	}
	*/
	
}
