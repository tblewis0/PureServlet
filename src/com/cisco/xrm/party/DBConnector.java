package com.cisco.xrm.party;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DBConnector {

	public static final int POST_MATCH = 0;
	public static final int PRE_MATCH = 1;
	public static final int FULL_MATCH = 2;
	public static final int DEFAULT_SORT = 0;
	public static final int ASC = 1;
	public static final int DESC = 2;

	private final String IMG_URL = "http://www.intenseschool.com/images/cisco-logo.png";
	private final String CISCO_IMG = "<center><img src=" + IMG_URL
			+ " height=200 width=300>\n";
	private final String XRM = "<h2>Cisco xRM Party Master API Search</h2></center>\n";
	private final String STYLE = "<style>\n\t.size{font-size:10px;}\n</style>\n";
	private final String FORM_RETURN_START = "<center><form name=return_search action=/xRM/party method=get>\n";
	private final String FORM_RETRUN = "\t<input type=submit value='Return to Search' style='width:25%;height:50px;font-size:25px;color:red;'>\n";
	private final String FORM_END = "</form></center>";
	private final String FORM = FORM_RETURN_START + FORM_RETRUN + FORM_END;
	private final String TABLE_START = CISCO_IMG + XRM + FORM + STYLE
			+ "<table border=1>\n";
	private final String TABLE_HEADER_START = "\t\t<th>";
	private final String TABLE_HEADER_END = "</th>\n";
	private final String TABLE_ROW_START = "\t<tr class=size>\n";
	private final String TABLE_ROW_END = "\t</tr>\n";
	private final String TABLE_DATA_START = "\t\t<td>";
	private final String TABLE_DATA_END = "</td>\n";
	private final String TABLE_END = "</table>";

	private final String[] COLUMNS = { "PARTY_ID", "PARTY_NAME", "KNOWN_AS",
			"ATTRIBUTE20", "DUNS_NUMBER", "EMPLOYEES_TOTAL", "SIC_CODE",
			"ALL_ADDRESS_LINES", "CITY", "STATE", "POSTAL_CODE",
			"COUNTRY_NAME", "VERTICAL_MARKET_TOP_DESC" };
	private Connection con, con2;
	private Statement st;
	private ResultSet rs;
	private JSONArray partyArr = new JSONArray();

	public DBConnector() {
		try {
			Class.forName("com.mysql.jdbc.Driver");

			/*
			 * con = DriverManager.getConnection(
			 * "jbdc:mysql://api-wizards.cisco.com:3306/apiwizards", "root",
			 * "Cisco123");
			 */
			con = DriverManager.getConnection(
					"jdbc:mysql://atom3.cisco.com:3306/reddb", "redteam",
					"redteam");
			st = con.createStatement();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private String setTableHeaders() {
		String headers = TABLE_ROW_START;
		for (String header : COLUMNS) {
			headers += TABLE_HEADER_START + header + TABLE_HEADER_END;
		}
		headers += TABLE_ROW_END;

		return headers;
	}

	public String copyDB() throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM xRM";
			rs = st.executeQuery(query);
			query = "TRUNCATE parties";
			// st2.executeUpdate(query);
			query = "INSERT INTO parties VALUES(?, ?)";
			PreparedStatement pstmt = null;
			while (rs.next()) {
				pstmt = con2.prepareStatement(query);
				String party_id = rs.getString("PARTY_ID");
				String party_name = rs.getString("PARTY_NAME");
				pstmt.setString(1, party_id);
				pstmt.setString(2, party_name);
				pstmt.executeUpdate();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return "Success";
	}

	public String getData() throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM xRM";
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = null;
				JSONArray a = new JSONArray();
				String log = "";
				for (String col : COLUMNS) {
					p = new Party();
					p.put(col, (rs.getString(col) != null ? rs.getString(col)
							: "NULL"));
					log += p.toString();
					a.put(p);
				}
				partyArr.put(a);
				System.out.println(log);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataInTable() throws JSONException {
		long startTime = System.nanoTime();
		String table = TABLE_START + setTableHeaders();
		try {
			String query = "SELECT * FROM xRM";
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				table += TABLE_ROW_START;
				for (String col : COLUMNS) {
					table += TABLE_DATA_START
							+ (rs.getString(col) != null ? rs.getString(col)
									: "<i><div align=right>NULL</div></i>") + TABLE_DATA_END;
				}
				table += TABLE_ROW_END;
			}
			table += TABLE_END;
			System.out.println(table);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return table + "\n<i><div align=right>Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}

	public String getDataById(String party_ID) throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM xRM WHERE PARTY_ID = " + party_ID;
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = null;
				JSONArray a = new JSONArray();
				String log = "";
				for (String col : COLUMNS) {
					p = new Party();
					p.put(col, (rs.getString(col) != null ? rs.getString(col)
							: "NULL"));
					log += p.toString();
					a.put(p);
				}
				partyArr.put(a);
				System.out.println(log);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByIdInTable(String party_ID) throws JSONException {
		long startTime = System.nanoTime();
		String table = TABLE_START + setTableHeaders();
		try {
			String query = "SELECT * FROM xRM WHERE PARTY_ID = " + party_ID;
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				table += TABLE_ROW_START;
				for (String col : COLUMNS) {
					table += TABLE_DATA_START
							+ (rs.getString(col) != null ? rs.getString(col)
									: "<i><div align=right>NULL</div></i>") + TABLE_DATA_END;
				}
				table += TABLE_ROW_END;
			}
			table += TABLE_END;
			System.out.println(table);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return table + "\n<i><div align=right>Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}

	public String getDataByLimit(String limit) throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM xRM LIMIT " + limit;
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = null;
				JSONArray a = new JSONArray();
				String log = "";
				for (String col : COLUMNS) {
					p = new Party();
					p.put(col, (rs.getString(col) != null ? rs.getString(col)
							: "NULL"));
					log += p.toString();
					a.put(p);
				}
				partyArr.put(a);
				System.out.println(log);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByLimitInTable(String limit) throws JSONException {
		long startTime = System.nanoTime();
		String table = TABLE_START + setTableHeaders();
		try {
			String query = "SELECT * FROM xRM LIMIT " + limit;
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				table += TABLE_ROW_START;
				for (String col : COLUMNS) {
					table += TABLE_DATA_START
							+ (rs.getString(col) != null ? rs.getString(col)
									: "<i><div align=right>NULL</div></i>") + TABLE_DATA_END;
				}
				table += TABLE_ROW_END;
			}
			table += TABLE_END;
			System.out.println(table);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return table + "\n<i><div align=right>Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}

	public String getDataByName(String party_name) throws JSONException {
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM xRM WHERE PARTY_NAME = '"
					+ party_name + "'";
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = null;
				JSONArray a = new JSONArray();
				String log = "";
				for (String col : COLUMNS) {
					p = new Party();
					p.put(col, (rs.getString(col) != null ? rs.getString(col)
							: "NULL"));
					log += p.toString();
					a.put(p);
				}
				partyArr.put(a);
				System.out.println(log);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByNameInTable(String party_name) throws JSONException {
		String table = TABLE_START + setTableHeaders();
		long startTime = System.nanoTime();
		try {
			String query = "SELECT * FROM xRM WHERE PARTY_NAME = '"
					+ party_name + "'";
			rs = st.executeQuery(query);
			System.out.println("Records from Database");
			while (rs.next()) {
				table += TABLE_ROW_START;
				for (String col : COLUMNS) {
					table += TABLE_DATA_START
							+ (rs.getString(col) != null ? rs.getString(col)
									: "<i><div align=right>NULL</div></i>") + TABLE_DATA_END;
				}
				table += TABLE_ROW_END;
			}
			table += TABLE_END;
			System.out.println(table);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return table + "\n<i><div align=right>Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}

	public String getDataByLike(String pattern, int match_type)
			throws JSONException {
		long startTime = System.nanoTime();
		String query = "SELECT * FROM xRM WHERE PARTY_NAME LIKE ?";
		int count = 0;
		String match = (match_type == POST_MATCH ? pattern + "%"
				: match_type == PRE_MATCH ? "%" + pattern : "%" + pattern + "%");
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, match);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = null;
				JSONArray a = new JSONArray();
				String log = "";
				for (String col : COLUMNS) {
					p = new Party();
					p.put(col, (rs.getString(col) != null ? rs.getString(col)
							: "NULL"));
					log += p.toString();
					a.put(p);
				}
				partyArr.put(a);
				System.out.println(log);
				count++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime
				+ " nanoseconds/" + seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByLikeInTable(String pattern, int match_type)
			throws JSONException {
		String table = TABLE_START + setTableHeaders();
		long startTime = System.nanoTime();
		String query = "SELECT * FROM xRM WHERE PARTY_NAME LIKE ?";
		int count = 0;
		String match = (match_type == POST_MATCH ? pattern + "%"
				: match_type == PRE_MATCH ? "%" + pattern : "%" + pattern + "%");
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, match);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
				table += TABLE_ROW_START;
				for (String col : COLUMNS) {
					table += TABLE_DATA_START
							+ (rs.getString(col) != null ? rs.getString(col)
									: "<i><div align=right>NULL</div></i>") + TABLE_DATA_END;
				}
				table += TABLE_ROW_END;
			}
			table += TABLE_END;
			System.out.println(table);
			count++;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime
				+ " nanoseconds/" + seconds + " seconds");
		return table + "\n<i><div align=right>Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}

	public String getDataByRegExp(String pattern) throws JSONException {
		long startTime = System.nanoTime();
		String query = "SELECT * FROM xRM WHERE PARTY_NAME REGEXP ?";
		int count = 0;
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, pattern);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = null;
				JSONArray a = new JSONArray();
				String log = "";
				for (String col : COLUMNS) {
					p = new Party();
					p.put(col, (rs.getString(col) != null ? rs.getString(col)
							: "NULL"));
					log += p.toString();
					a.put(p);
				}
				partyArr.put(a);
				System.out.println(log);
				count++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime
				+ " nanoseconds/" + seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByRegExpInTable(String pattern) throws JSONException {
		String table = TABLE_START + setTableHeaders();
		long startTime = System.nanoTime();
		String query = "SELECT * FROM xRM WHERE PARTY_NAME REGEXP ?";
		int count = 0;
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			pstmt.setString(1, pattern);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
				table += TABLE_ROW_START;
				for (String col : COLUMNS) {
					table += TABLE_DATA_START
							+ (rs.getString(col) != null ? rs.getString(col)
									: "<i><div align=right>NULL</div></i>") + TABLE_DATA_END;
				}
				table += TABLE_ROW_END;
			}
			table += TABLE_END;
			System.out.println(table);
			count++;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime
				+ " nanoseconds/" + seconds + " seconds");
		return table + "\n<i><div align=right>Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}

	public String getDataByOrderBy(int sort_type, String col)
			throws JSONException {
		long startTime = System.nanoTime();
		String sort = (sort_type == DEFAULT_SORT ? ""
				: sort_type == ASC ? "ASC" : "DESC");
		String query = "SELECT * FROM xRM ORDER BY " + col + " " + sort;
		int count = 0;
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
				JSONObject p = null;
				JSONArray a = new JSONArray();
				String log = "";
				for (String _col : COLUMNS) {
					p = new Party();
					p.put(_col,
							(rs.getString(_col) != null ? rs.getString(_col)
									: "NULL"));
					log += p.toString();
					a.put(p);
				}
				partyArr.put(a);
				System.out.println(log);
				count++;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime
				+ " nanoseconds/" + seconds + " seconds");
		return partyArr.toString(3);
	}

	public String getDataByOrderByInTable(int sort_type, String col)
			throws JSONException {
		String table = TABLE_START + setTableHeaders();
		long startTime = System.nanoTime();
		String sort = (sort_type == DEFAULT_SORT ? ""
				: sort_type == ASC ? "ASC" : "DESC");
		String query = "SELECT * FROM xRM ORDER BY " + col + " " + sort;
		int count = 0;
		PreparedStatement pstmt = null;
		try {
			pstmt = con.prepareStatement(query);
			rs = pstmt.executeQuery();
			System.out.println("Records from Database");
			while (rs.next()) {
				table += TABLE_ROW_START;
				for (String _col : COLUMNS) {
					table += TABLE_DATA_START
							+ (rs.getString(_col) != null ? rs.getString(_col)
									: "<i><div align=right>NULL</div></i>") + TABLE_DATA_END;
				}
				table += TABLE_ROW_END;
			}
			table += TABLE_END;
			System.out.println(table);
			count++;
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println(count + "\n" + "Time Elapsed " + totalTime
				+ " nanoseconds/" + seconds + " seconds");
		return table + "\n<i><div align=right>Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}
}
