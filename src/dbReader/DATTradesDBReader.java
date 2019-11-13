package dbReader;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedList;

import dbReaderFramework.I_DBReader;
import taqDBReaders.GZFileUtils;
import taqDBReaders.ReadGZippedTAQTradesFile;

//After rewriting all the files to dat, this class read dat files for future usage
public class DATTradesDBReader implements I_DBReader {

	protected DataInputStream 		   _dataInputStream;
	protected int                      _recCount;     // Index into record array of underlying ReadGZippedTAQQuotesFile object
	protected int                      _nRecsRead;    // Number of records read in last read
	protected boolean                  _isFinished;
	protected long                     _lastSequenceNumberRead;
	protected LinkedList<Long>         _milliSecondsFromTheEpoch;
	protected LinkedList<Short>        _tickerByteList;
	protected LinkedList<Integer>      _sizeList;
	protected LinkedList<Float>        _priceList; 
	
	public boolean isFinished() { return _isFinished; }
	
	public DATTradesDBReader( String filePathName ) throws IOException {
		_dataInputStream = GZFileUtils.getGZippedFileInputStream(filePathName);
		_recCount = 0;
		_nRecsRead = 0;
		_isFinished = false;
		_lastSequenceNumberRead = 0;
		_milliSecondsFromTheEpoch = new LinkedList<>();
		_tickerByteList = new LinkedList<Short>();
		_sizeList = new LinkedList<Integer>();
		_priceList = new LinkedList<Float>(); 
	}
	
	/** Read all records with sequence numbers less than or equal to the specified sequence number
	 * @param targetSequenceNum
	 */
	@Override
	public int readChunk( long targetSequenceNum ){
		
		if( _isFinished )
			return 0;
		
		// Iterate over records until we hit last record in data or we hit
		// a sequence number that is higher than the target sequence number
		
			int i = _recCount;
			while( true ) {
				
				try {
					if( _dataInputStream.available() == 0 ) {
						_isFinished = true;
						break;
						}
				    if( getSequenceNumber() > targetSequenceNum ) {break;}
				    _tickerByteList.add(_dataInputStream.readShort());
				    _sizeList.add(_dataInputStream.readInt());
				    _priceList.add(_dataInputStream.readFloat());
					
				    } catch (IOException e) {e.printStackTrace();}
				_recCount++;
			}
			
		// Save the sequence number that was just read
			
			_lastSequenceNumberRead = targetSequenceNum;
			
		// Save number of records red and return it
			
			_nRecsRead = _recCount - i;
			return _nRecsRead;
			
	}

	// These methods give an external object access to the data that
	// was read in the last readChunk call
	
		public int getNRecsRead() { return _nRecsRead; }
		
		public long getMilliSecondsFromTheEpoch( int i ) {return _milliSecondsFromTheEpoch.get( _recCount - _nRecsRead + i );}

		public int gettickerByteList( int i ) {return _tickerByteList.get( _recCount - _nRecsRead + i );}
	
		public int getSizeList( int i ) {return _sizeList.get( _recCount - _nRecsRead + i );}
		
		public float getPriceList( int i ) {return _priceList.get( _recCount - _nRecsRead + i );}
		
		public int getrecCount() {return _recCount;}

	/**
	 * Return the sequence number of the record that this reader is currently
	 * pointing to and will step over when it is asked to read all of the 
	 * records it has for this sequence number.
	 * 
	 * return Current sequence number of this reader
	 * @throws Exception 
	 */
	@Override
	public long getSequenceNumber() {
		long sequenceNumber = 0;
		if(_milliSecondsFromTheEpoch.size() == _tickerByteList.size()) {
			try {
				sequenceNumber = _dataInputStream.readLong();
			} catch (IOException e) {
				e.printStackTrace();
			}
			_milliSecondsFromTheEpoch.add(sequenceNumber);
		}
		else
			sequenceNumber = _milliSecondsFromTheEpoch.getLast();		
		return sequenceNumber;
	}

	/**
	 * Stop reading and close all files. In this case, there's nothing to do
	 * because everything has already been read into memory and the file has
	 * been closed.
	 */
	@Override
	public void stop() {
		try {
			_dataInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Return the last sequence number for which records were successfully
	 * read by this reader.
	 */
	@Override
	public long getLastSequenceNumberRead() {
		return _lastSequenceNumberRead;
	}

}
