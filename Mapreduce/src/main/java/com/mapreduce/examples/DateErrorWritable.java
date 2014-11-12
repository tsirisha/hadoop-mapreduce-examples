package com.mapreduce.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

public class DateErrorWritable implements WritableComparable<DateErrorWritable>{
	private final static SimpleDateFormat formatter = new SimpleDateFormat( "yyyy-MM-dd' T 'HH:mm:ss.SSS" );
	private Date theDate;
	private String theError;
	public Date getDate()
	{
		return theDate;
	}
	
	public void setDate( Date aDate )
	{
		this.theDate = aDate;
	}

	public void readFields(DataInput aOut) throws IOException {
		// TODO Auto-generated method stub
		theDate=new Date(aOut.readLong());
		theError=aOut.readUTF();
		
	}

	public void write(DataOutput aIn) throws IOException {
		// TODO Auto-generated method stub
		aIn.writeLong(theDate.getTime());
		aIn.writeUTF(theError);
		
		
	}
	public String getTheError() {
		return theError;
	}

	public void setTheError(String theError) {
		this.theError = theError;
	}
	public String toString(){
		return formatter.format(theDate)+" "+theError;
	}
	@Override
	public int hashCode() {
		return theDate.hashCode() * 163 + theError.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof DateErrorWritable) {
			DateErrorWritable dw = (DateErrorWritable) o;
			return theDate.equals(dw.theDate) && theError.equals(dw.theError);
		}
		return false;
	}
	public int compareTo(DateErrorWritable aDateError) {
		// TODO Auto-generated method stub
		int cmp = theDate.compareTo(aDateError.theDate);
		if (cmp != 0) {
			return cmp;
		}
		return theError.compareTo(aDateError.theError);
	}

	
}
