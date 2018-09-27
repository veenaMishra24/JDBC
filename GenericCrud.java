package com.ey.jdbc;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Scanner;

public class GenericCrud {
	private static Connection conn = null;
	private static Statement stmt = null;
	private static PreparedStatement pstmt = null;
	private static ResultSet rs = null;
	private static DatabaseMetaData dbmd = null;
	private static ResultSetMetaData rsmd = null;
	private String tableName = null;
	private String pkName = null;
	private String pkVlaue = null;
	private static Scanner userInput = new Scanner(System.in);

	public static void connect() throws Exception {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/xe", "hr", "hr");
		stmt = conn.createStatement();
	}

	public void ListTables() throws Exception {
		rs = stmt.executeQuery("select tname from tab where tabtype='TABLE'");
		System.out.println("List of Tables in HR Schema");
		while (rs.next()) {
			System.out.println(rs.getString(1));
		}
	}

	public void getTableName() throws Exception {
		// ListTables();
		System.out.println("Please Enter Table Name for CRUD Operation:");
		tableName = userInput.next();
	}

	public void read(String pkValue) throws Exception {
		pstmt = conn.prepareStatement("select * from " + tableName + " where " + pkName + "= ?");
		pstmt.setString(1, pkValue);
		rs = pstmt.executeQuery();
		if (rs.next()) {
			rsmd = rs.getMetaData();
			for (int i = 1; i <= rsmd.getColumnCount(); i++)
				System.out.print(rs.getString(i) + "\t");
		}
		System.out.println("");

	}

	public void readAll(String tableName) throws Exception {
		System.out.println("**********" + tableName + " Contents Start**********");
		getColumnNames();
		getColumnContents();
		System.out.println("**********" + tableName + " Contents Ends**********");
	}

	public ResultSetMetaData getColumnNames() throws Exception {
		rs = stmt.executeQuery("select * from " + tableName);
		rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		for (int i = 1; i <= columnCount; i++)
			System.out.print(rsmd.getColumnName(i) + "\t");
		System.out.println("");
		return rsmd;
	}

	public void getColumnContents() throws Exception {
		rs = stmt.executeQuery("select * from " + tableName);
		rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		while (rs.next()) {
			for (int i = 1; i <= columnCount; i++)
				System.out.print(rs.getString(i) + "\t");
			System.out.println("");
		}

	}

	public String getKey() throws Exception {
		dbmd = conn.getMetaData();
		String pKey = "";
//		rs = dbmd.getPrimaryKeys(null, "HR", tableName.toUpperCase());
//		if (rs.next())
//			pKey = rs.getString("COLUMN_NAME");
//		else
//			pKey = "No Primary Key Found for the given Table :" + tableName;

		rs = stmt.executeQuery("select * from " + tableName);
		rsmd = rs.getMetaData();
		if (rsmd != null)
			pKey = rsmd.getColumnName(1);
		pkName = pKey;
		return pKey;
	}

	public void insert() throws Exception {
		StringBuilder insertQuery = new StringBuilder("insert into ");
		insertQuery.append(tableName).append(" values (");
		getColumnNames();
		int colCount = rsmd.getColumnCount();
		for (int i = 1; i <= colCount; i++) {
			if (i == colCount)
				insertQuery.append("?)");
			else
				insertQuery.append("?,");
		}
		System.out.println("Insert Query :" + insertQuery);
		pstmt = conn.prepareStatement(insertQuery.toString());
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			System.out.println("Enter value for the column " + rsmd.getColumnName(i) + " of data type "
					+ rsmd.getColumnTypeName(i) + ":");
			if (rsmd.getColumnTypeName(i).equals("Number"))
				pstmt.setInt(i, userInput.nextInt());
			else
				pstmt.setString(i, userInput.next());
		}

		int insertStatus = 0;
		insertStatus = pstmt.executeUpdate();
		if (insertStatus != 0)
			System.out.println("1 Record Inserted Successfully! :)");
		else
			System.out.println("Record Not Inserted!!!!  Pls Check :(");

	}

	public void delete() throws Exception {
		int deleteStatus = 0;
		String deleteQuery = "delete from " + tableName + " where " + getKey() + " = ?";
		System.out.println("Enter " + getKey() + " of " + tableName + " to delete :");
		pkVlaue = userInput.next();
		System.out.println("Delete Query : " + deleteQuery);
		pstmt = conn.prepareStatement(deleteQuery);
		pstmt.setString(1, pkVlaue);
		deleteStatus = pstmt.executeUpdate();
		if (deleteStatus != 0)
			System.out.println("1 Record Deleted Successfully! :)");
		else
			System.out.println("Record Not Deleted!!!!  Pls Check :(");
	}

	public void update() throws Exception {
		int updateStatus = 0;
		StringBuilder updateQuery = new StringBuilder("update ");
		updateQuery.append(tableName).append(" set ");
		System.out.println("Enter " + getKey() + " value to Update :");
		pkVlaue = userInput.next();
		System.out.println("Enter Column Name to Update :");
		String columnName = "";
		columnName = userInput.next();
		updateQuery.append(columnName).append(" = ? where ").append(getKey()).append("=?");

		System.out.println("Enter Updated value for the selected Column:");
		String columnValue = "";
		columnValue = userInput.next();
		System.out.println("Update Query :" + updateQuery);
		pstmt = conn.prepareStatement(updateQuery.toString());
		pstmt.setString(1, columnValue);
		pstmt.setString(2, pkVlaue);
		updateStatus = pstmt.executeUpdate();
		if (updateStatus != 0)
			System.out.println("1 Record Updated Successfully! :)");
		else
			System.out.println("Record Not Updated!!!!  Pls Check :(");
	}

	public void closeAll() throws Exception {
		if (rs != null)
			rs.close();
		if (stmt != null)
			stmt.close();
		if (pstmt != null)
			pstmt.close();
		if (conn != null)
			conn.close();
	}

	public static void main(String[] args) throws Exception {
		int option = 0;
		GenericCrud.connect();
		while (option != 5) {
			GenericCrud obj = new GenericCrud();
			System.out.println("$$$$$$$$$$$$$$$$$$$$$Generic Oracle CRUD Program$$$$$$$$$$$$$$$$$$$$$");
			obj.ListTables();
			System.out.println(
					"\t\t 1. View Table Content. \n\t\t 2. Insert new Record. \n\t\t 3. Update Existing Record. \n\t\t 4. Delete a Record. \n\t\t 5. Quit");
			System.out.println("Enter any Option to Continue (1-5) :");
			option = userInput.nextInt();
			switch (option) {
			case 1:
				obj.getTableName();
				obj.readAll(obj.tableName);
				break;
			case 2:
				obj.getTableName();
				obj.readAll(obj.tableName);
				obj.insert();
				break;
			case 3:
				obj.getTableName();
				obj.readAll(obj.tableName);
				obj.update();
				break;
			case 4:
				obj.getTableName();
				obj.readAll(obj.tableName);
				obj.delete();
				break;
			case 5:
				obj.closeAll();
				System.out.println("Thanks for Using Generic CRUD Application. Program Ends Here!!!");
				System.exit(0);
				break;
			default:
				System.out.println("You Entered an invalid Option. Pls Try Again!!!");
				break;
			}
		}

	}

}
