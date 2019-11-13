package taqDBReaders;

import java.io.IOException;

import dbReaderFramework.I_DBReader;

/**
 * This class implements an I_DBReader interface for use by the DBManager class. Functionality:
 * 
 * <P>
 * 1) A {@link ReadGZippedTAQQuotesFile gzipped TAQ reader} class is used to read an entire day of data for
 * one stock into memory.
 * </P>
 *  
 * <P>
 * 2) This class allows a calling object to step through that data one chunk at
 * a time. A chunk is defined as a piece of data in which all of the records have
 * the same sequence number.
 * </P>
 * 
 * <P>
 * 3) After a chunk is read, this class allows a calling object to retrieve the
 * number of records read for that chunk, and the fields in each record.
 * </P>
 * 
 * @author Lee
 *
 */
public class TAQQuotesDBReader implements I_DBReader {
	
	protected ReadGZippedTAQQuotesFile _taq;                    // This is the object that contains in memory one day of taq trades data
	protected int                      _recCount;               // Index into record array of underlying ReadGZippedTAQQuotesFile object
	protected int                      _nRecsRead;              // Number of records read in last read
	protected boolean                  _isFinished;             // Flag set to true when this object stops iterating over taq data
	protected long                     _lastSequenceNumberRead; // Sequence number of the last chunk successfully read by this object
	
	public boolean isFinished() { return _isFinished; }
	
	public TAQQuotesDBReader( String filePathName ) throws IOException {
		_taq = new ReadGZippedTAQQuotesFile(filePathName);
		_recCount = 0;
		_nRecsRead = 0;
		_isFinished = false;
		_lastSequenceNumberRead = 0;
	}

	@Override
	public int readChunk( long targetSequenceNum ) {
		
		if( _isFinished )
			return 0;
		
		// Iterate over records until we hit last record in data or we hit
		// a sequence number that is higher than the target sequence number
		
		
			// Compute number of records in our TAQ file
			
				int maxRecs = _taq.getNRecs();
				
			// Start the counter at the last record
				
				int i = _recCount;
			
			// Loop until we get to the last record or the sequence number
			// is greater than target sequence number
				
				while( true ) {
					
					// Have we reached the last record in our data file?
					
						if( _recCount == maxRecs ) {
							
							// Yes, we're past the end of the records in our data file.
							// Set finished flag and exit reading loop.
							
								_isFinished = true; 
								break;
								
						}
						
					// Have we encountered a higher sequence number?
						
						if( getSequenceNumber() > targetSequenceNum )
							
							// Yes, new sequence number so this reader is finished. Exit reading loop.
							
								break; 
						
					// We are still below our target sequence number and there are
					// more records left in this reader. Increment counter of records 
					// and keep reading
					
						_recCount++;
						
				}
				
		// Save last sequence number that was read. This will be used later
		// to check whether a particular reader has new data.
		
			_lastSequenceNumberRead = targetSequenceNum;
			
		// Compute the number of records we just read, save it, and return it
			
			_nRecsRead = _recCount - i;
			return _nRecsRead;
			
	}
	
	// These methods give an external object access to the data that
	// was read in the last readChunk call
	
		public int getNRecsRead() { return _nRecsRead; }
		
		public int getMillisFromMidnight( int i ) {
			return _taq.getMillisecondsFromMidnight( _recCount - _nRecsRead + i );
		}
	
		public long getSequenceNum( int i ) {
			return _taq.getSecsFromEpoch() * 1000L + _taq.getMillisecondsFromMidnight( _recCount - _nRecsRead + i );
		}
	
		public int getBidSize( int i ) {
			return _taq.getBidSize( _recCount - _nRecsRead + i );
		}
		
		public float getBidPrice( int i ) {
			return _taq.getBidPrice( _recCount - _nRecsRead + i );
		}
	
		public int getAskSize( int i ) {
			return _taq.getAskSize( _recCount - _nRecsRead + i );
		}
		
		public float getAskPrice( int i ) {
			return _taq.getAskPrice( _recCount - _nRecsRead + i );
		}

	/**
	 * Return the sequence number of the record that this reader is currently
	 * pointing to and will step over when it is asked to read all of the 
	 * records it has for this sequence number.
	 * 
	 * return Current sequence number of this reader
	 */
	@Override
	public long getSequenceNumber() {
		int secFromEpoch = _taq.getSecsFromEpoch();
		int millisFromMidnight = _taq.getMillisecondsFromMidnight( _recCount );
		long sn = ((long)secFromEpoch) * 1000L + millisFromMidnight;
		return sn;
	}

	/**
	 * Stop reading and close all files. In this case, there's nothing to do
	 * because everything has already been read into memory and the file has
	 * been closed.
	 */
	@Override
	public void stop() {
		// Nothing to do here because file was closed in _taq
	}

	/** Get the number of second from midnight of January 1, 1970 to midnight of current day in data file.
	 * @return Number of seconds from "epoch"
	 */
	public long getSecondsFromEpoch() {
		return _taq.getSecsFromEpoch(); 
	}

	/**
	 * After a chunk of data - a piece of data in which all of the sequence numbers
	 * are the same - has been read, the sequence number of that data is saved
	 * and returned by this method.
	 * 
	 * @return Last sequence number read
	 */
	@Override
	public long getLastSequenceNumberRead() {
		return _lastSequenceNumberRead;
	}

}
