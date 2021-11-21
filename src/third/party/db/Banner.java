package third.party.db;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Date;
import java.util.Scanner;

import third.party.util.DteFormatter;
import third.party.util.MessageLogger;

public class Banner {
	
	private static final int  SPRIDEN_ID = 0;
	private static final int  GOBTPAC_PIDM = 1;
	private static final int  SPRIDEN_FIRST_NAME = 2;
	private static final int  SPRIDEN_LAST_NAME = 3;
	private static final int  THIRD_PARTY_ID = 4;
	private static final int  NEW_THIRD_PARTY_ID =5;
	private static final int  CA_EMAIL_ADDRESS =6;
	private static final int  STATUS =7;
	private String strDate;
	
	protected Connection conn;
	public Banner (){
	   try {
			
			//jdbc:oracle:thin:@localhost:5500/globldb3
				Class.forName("oracle.jdbc.driver.OracleDriver");
//				String url = "jdbc:oracle:thin:@bandb-prod.ec.cavehill.uwi.edu:8000:PROD";
				String url = "jdbc:oracle:thin:@bandb-dev.ec.cavehill.uwi.edu:8003:TEST";
			//String url = "jdbc:oracle:thin:@hermes.cavehill.uwi.edu:1521:PRODCH";
				conn = DriverManager.getConnection(url, "svc_update", "e98ce36209");
//				conn = DriverManager.getConnection(url, "svc_ethink", "z38te47209");
				
				conn.setAutoCommit(true);
				
				
				
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException ex) {
				ex.printStackTrace();

			}
   }
	
	public boolean externalUserExist(String external_user, int pidm){
		boolean found=false;
		String selectStatement = "select * from gobtpac where gobtpac_external_user = ?";
		try {
			PreparedStatement prepStmt = conn.prepareStatement(selectStatement);

			prepStmt.setString(1, external_user);
			
			
			ResultSet rs = prepStmt.executeQuery();
			if (rs.next()) {
				found=true;
				}
			prepStmt.close();
			rs.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return found;
	}
	public void deleteFromGorpaud(String external_user) {
		String selectStatement = "delete from gorpaud where gorpaud_external_user = ? and gorpaud_chg_ind in ('I','P')";
		try {
			PreparedStatement prepStmt = conn.prepareStatement(selectStatement);

			prepStmt.setString(1, external_user);
			MessageLogger.out.println("delete 'GORPAUD' records");
			prepStmt.executeUpdate();

			prepStmt.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
    /*select
   max(vsize(gobtpac_external_user))
from
   gobtpac*/
	public void updateThirdPartyId(int pidm, String third_party_id) throws SQLException {

		CallableStatement callStmt = conn.prepareCall("{CALL gb_third_party_access.p_update(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
		callStmt.setLong(1, pidm);
		callStmt.setString(2, "N");
		callStmt.setString(3, "N");
		callStmt.setString(4, "SVC_UPDATE");
		callStmt.setString(5, null);
		callStmt.setString(6, null);
		callStmt.setString(7, third_party_id);
		callStmt.setString(8, null);
		callStmt.setString(9, null);
		callStmt.setString(10, null);
		callStmt.setString(11, null);
		callStmt.setString(12, null);
		callStmt.setString(13, null);
		callStmt.setString(14, "Y");
		callStmt.setString(15, "N");
		callStmt.setString(16, "N");
		callStmt.setString(17, "Y");
		callStmt.registerOutParameter(18, Types.VARCHAR);
		
		callStmt.executeQuery();
	    callStmt.close();
	}

	public void setFormattedDate () {
		DteFormatter formatter = new DteFormatter();
		strDate = formatter.printDate().replace(" ", "-");
		strDate = strDate.replace(":", "-");
		
	}
	private String getFormattedDate () {
	  return strDate;
	}
	public void setupLogger() {
		Path path = FileSystems.getDefault().getPath("").toAbsolutePath();
		System.out.println(path);
		
		String logFile = path + "\\logs\\logger_"+ getFormattedDate()+ ".txt";

        try {
        	  MessageLogger.setErr(new PrintStream(new FileOutputStream(new File(logFile))));
        	  MessageLogger.setOut(new PrintStream(new FileOutputStream(new File(logFile))));
        	  
        	  
        } catch(Exception e) {
              e.printStackTrace(MessageLogger.out);
        }    
	}
	
	public void prepareThirdPartyUpdates (Banner db) throws NumberFormatException, IOException {
		Path path = FileSystems.getDefault().getPath(".").toAbsolutePath();
		
		Scanner sc = new Scanner(new File(path+"\\csv\\updates\\matthew_jordan.csv"));  
		String splitBy = ",";
		int cnt = 0;
		int errorCntr = 0;
		int cntr = 0;
		String[] outFile = null;
		String[] errorFile =null;
		MessageLogger.out.println("Start time: " +System.currentTimeMillis());
		while (sc.hasNext())  //returns a boolean value  
		{  
			try {
			String line = sc.next();
			line = line.replaceAll("\\s", "");
			line =
		        Normalizer.normalize(line, Form.NFD)
		            .replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
			line=line.replace("'","");
			line= line.replace(" ", "");
//			line=line.replace("-", "");
			line=line.replace("?", "");
			String[] student = line.split(splitBy);
			outFile = student;
			errorFile = student;
			if (cnt > 0) {
				
				        System.out.println(cnt+ ","+student+", "+ student[0]+","+student[1]+","+student[2]+","+student[3]+","+student[4]+","+student[5]);
					
						String email = student[CA_EMAIL_ADDRESS];
						String thirdParty = email.substring(0, email.indexOf("@"));
						System.out.println(thirdParty);
						//remove comment when live
						
						if (!externalUserExist(thirdParty,Integer.parseInt(student[GOBTPAC_PIDM]))) {
							   cntr++;
							   System.out.println("cntr"+cntr);
								MessageLogger.out.println(student[SPRIDEN_ID]+","+student[SPRIDEN_FIRST_NAME]+","+student[SPRIDEN_LAST_NAME]);
							    db.deleteFromGorpaud(student[THIRD_PARTY_ID]);
							    db.deleteFromGorpaud(student[NEW_THIRD_PARTY_ID]);
							    MessageLogger.out.println("Removing Student Third Party record Third Party ID = "+ student[THIRD_PARTY_ID] + " from "+"Gorepaud");
								db.updateThirdPartyId(Integer.parseInt(student[GOBTPAC_PIDM]), student[NEW_THIRD_PARTY_ID]);	
								MessageLogger.out.println("Student "+ student[SPRIDEN_FIRST_NAME] + " " +student[SPRIDEN_LAST_NAME] + " Third Party ID updated from third party id = "+ student[THIRD_PARTY_ID] + " to new third party id = "+student[NEW_THIRD_PARTY_ID]);
								MessageLogger.out.println();
								MessageLogger.out.println();
						}
						
			 }
			} catch (Exception sqle) {
				
				MessageLogger.out.println(sqle.getMessage());
				//DteFormatter formatter = new DteFormatter();
				String fileName = path + "\\logs\\errorrecs_"+getFormattedDate() + ".txt";
			    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName,true));
			    errorCntr++;
			    writer.write("Error Counter = "+ errorCntr);
			    writer.newLine();
			    writer.write("SPRIDEN_ID"+","+"FIRST_NAME"+","+"LAST_NAME"+","+"THIRD_PARTY_ID"+","+"NEW_THIRD_PARTY_ID");
			    writer.newLine();
			    writer.write(errorFile[SPRIDEN_ID]+","+errorFile[SPRIDEN_FIRST_NAME]+","+errorFile[SPRIDEN_LAST_NAME]+","+errorFile[THIRD_PARTY_ID]+","+errorFile[NEW_THIRD_PARTY_ID]);
			    writer.newLine();
			    writer.write(sqle.getMessage());
			    writer.newLine();
			    writer.close();
				MessageLogger.out.println();
				MessageLogger.out.println();
			
			}
			cnt++;
			
		}   
		sc.close();  //closes the scanner
		MessageLogger.out.println("End Time: " +System.currentTimeMillis());

	}
	public static void main(String[] args) throws SQLException, IOException {
		System.out.println("Start time: " +System.currentTimeMillis());
		
//		long start = 1627333726767L;
//		
//		long end = 1627326859449L;
//		
//		double diff = (((start - end)/1000)/60)/60;
//		System.out.println(diff);
		
		 Banner db = new Banner();
		 db.setFormattedDate();
		 db.setupLogger();
		 db.prepareThirdPartyUpdates(db);
		 System.out.println("End time: " +System.currentTimeMillis());
		 
	}
	
	
}
