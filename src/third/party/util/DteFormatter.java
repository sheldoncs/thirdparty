package third.party.util;

import java.io.FileInputStream;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

  

  public class DteFormatter {
	  private Date date; 
	  private String stripDate;
	  private String AM_PM;
	  private String day;
	  
	public String printDate(){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		date = new Date();
		
		
		String s = formatter.format(date);
		return s;
		
	}
	public void returnTime(String timeStr){
		
		String time="2:00";
		long msec1 = 0;
		 String str = "one-two-three";
		 String[] temp;  
		 /* delimiter */
		 String delimiter = ":";
		 /* given string will be split by the argument delimiter provided. */
		 temp = time.split(delimiter);
		 /* print substrings */
		 for(int i =0; i < temp.length ; i++){
		
			 if (i == 0 ){
				 msec1 = 60*Integer.parseInt(temp[i]);
			 } else {
				 msec1 = msec1 + 1000*Integer.parseInt(temp[i]);
			 }
			 
		 }
		 
		 //System.out.println(msec1);
		 
        date = new Date();
        Date date = new Date();

        long msec = date.getTime();

        msec = msec +  msec1;

        date.setTime(msec);

        System.out.println(date);
      
		
	}
	public String getStripDate(){
		return stripDate;
	}
	public String printDateSlash(){
		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");

		date = new Date();
		
		
		String s = formatter.format(date);
		return s;
		
	}
	public String printDateSQLServer(){
		SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");

		
		date = new Date();
		
		
		String s = formatter.format(date);
		stripDate = s.substring(0, 2)+s.substring(3, 5)+s.substring(8,10) + s.substring(11,13)+s.substring(14,16)+s.substring(17,19);
		System.out.println(stripDate);
		
		return s;
	}
	
	public String getSimpleDate(){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		date = new Date();
		
		String s = formatter.format(date);
		return s;
	}
	public String getFormattedDate(Date d){
		SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy");

		String s = formatter.format(d);
		return s;
	}
	public String printTime(){
		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm a E");

		date = new Date();
		
		String s = formatter.format(date);
		setAMPM(s);
		setDay(s);
		s = s.substring(0, s.indexOf(getAMPM())+2);
		
		return s;
		
	}
	private void setAMPM(String d){
		if (d.indexOf("AM")>= 0){
		    AM_PM = d.substring(d.indexOf("AM"), d.indexOf("AM")+2);
		} else if (d.indexOf("PM")>= 0) {
			AM_PM = d.substring(d.indexOf("PM"), d.indexOf("PM")+2);	
		}
	}
	public String getAMPM(){
		return AM_PM;
	}
	private void setDay(String d){
		day = d.substring(d.lastIndexOf(" ")+1, d.length());
	}
	public String getDay(){
		return day;
	}
	public String parseDate(String dStr){
		
		String s = "";
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		
		try {
		date = formatter.parse(dStr);
		s = formatter.format(date);
		
		}
		catch (ParseException e){
			e.printStackTrace();
		}

		return s;
		
	}
	public static void main(String[] args) throws RemoteException,SQLException {
		DteFormatter tb = new DteFormatter();
		
		tb.returnTime("");
		MessageLogger.out.println(tb.printDateSQLServer());
		
		System.out.println(tb.parseDate("2099-12-31"));
		
		System.out.println(tb.printTime());
		System.out.println(tb.getAMPM());
		System.out.println(tb.getDay());
		
		Properties endPoint = new Properties();
		try {
			if (!tb.getDay().trim().equals("Sun") && !tb.getDay().trim().equals("Sat")){ 
		      endPoint.load(new FileInputStream("D:\\Program Files\\Business Objects\\Tomcat\\StudentActiveDirectory\\StudentActiveDirectory\\WEB-INF\\endpoint.properties"));
		      MessageLogger.out.println(endPoint.getProperty("pm").toString().trim());
		      MessageLogger.out.println(tb.printTime().trim());
		      if (endPoint.getProperty("pm").toString().trim().equals(tb.printTime().trim())){
			    MessageLogger.out.println("Hi I am here Now");
		      }
			}
		}
		catch (Exception ex){
	       ex.printStackTrace();
	    }
	   	
	}
	
}
