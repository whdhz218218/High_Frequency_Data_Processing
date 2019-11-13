package dbReaderFramework;


public class TimeClock implements I_DBClock {

	public static long ONE_MINUTE_IN_MILLIS = 60 * 1000;
	public static long ONE_HOUR_IN_MILLIS = 60 * ONE_MINUTE_IN_MILLIS;
	public static long ONE_DAY_IN_MILLIS = 24 * ONE_HOUR_IN_MILLIS;
	
	protected long _startTime;
	protected long _endTime;
	protected long _interval;
	protected long _currentTime;
	
	public TimeClock(
		long startTime,
		long endTime,
		long interval
	) {
		_startTime = startTime;
		_interval = interval; 
		_currentTime = _startTime - _interval;
		_endTime = endTime;
	}

	@Override
	public boolean isFinished() {
		return _currentTime >= _endTime;
	}
	
	@Override
	public long getNextSequenceNumber() {
		return _currentTime += _interval;
	}

}
