package muti.simple.dm;

/**
 * Location.
 *
 * @author Andrea Muti
 * created: 21 gen 2018
 *
 */
public class Locationa {

	private String vehicle_id;
	private long timestamp;
	private double latitude;
	private double longitude;
	
	/**
	 * Public Constructor.
	 */
	public Locationa() {
		super();
	}
	
	/**
	 * Public Constructor.
	 * 
	 * @param vehicle_id
	 * @param timestamp
	 * @param latitude
	 * @param longitude
	 */
	public Locationa(String vehicle_id, long timestamp, double latitude, double longitude) {
		super();
		this.vehicle_id = vehicle_id;
		this.timestamp = timestamp;
		this.latitude = latitude;
		this.longitude = longitude;
	}
	
	/**
	 * @return the vehicle_id
	 */
	public String getVehicleId() {
		return vehicle_id;
	}
	
	/**
	 * @param vehicle_id the vehicle_id to set
	 */
	public void setVehicleId(String vehicle_id) {
		this.vehicle_id = vehicle_id;
	}
	
	/**
	 * @return the timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}
	
	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	/**
	 * @return the latitude
	 */
	public double getLatitude() {
		return latitude;
	}
	
	/**
	 * @param latitude the latitude to set
	 */
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	
	/**
	 * @return the longitude
	 */
	public double getLongitude() {
		return longitude;
	}
	
	/**
	 * @param longitude the longitude to set
	 */
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(latitude);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(longitude);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		result = prime * result + ((vehicle_id == null) ? 0 : vehicle_id.hashCode());
		return result;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Locationa other = (Locationa) obj;
		if (Double.doubleToLongBits(latitude) != Double.doubleToLongBits(other.latitude))
			return false;
		if (Double.doubleToLongBits(longitude) != Double.doubleToLongBits(other.longitude))
			return false;
		if (timestamp != other.timestamp)
			return false;
		if (vehicle_id == null) {
			if (other.vehicle_id != null)
				return false;
		} else if (!vehicle_id.equals(other.vehicle_id))
			return false;
		return true;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Location [vehicle_id=" + vehicle_id + ", timestamp=" + timestamp + ", latitude=" + latitude
				+ ", longitude=" + longitude + "]";
	}
}