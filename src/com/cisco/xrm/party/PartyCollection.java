package com.cisco.xrm.party;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

//import redis.clients.jedis.Jedis;

//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;

import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;

@Path("/party")
public class PartyCollection {
	private JSONArray partyArr = new JSONArray();
	DBConnector dbc = new DBConnector();
	private String inputFile = "C:/DFT/LDE/xRM/src/com/cisco/xrm/party/sample/Sample.xls";
	private String search_form = "C:/DFT/LDE/xRM/src/com/cisco/xrm/party/html/form.html";
	private File form_file = new File(search_form);
	@GET
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String searchDB() throws JSONException, FileNotFoundException {
		long startTime = System.nanoTime();
		Scanner readForm = new Scanner(form_file);
		String form = "";
		while (readForm.hasNext()) {
			form += readForm.nextLine() + "\n";
		}
		System.out.println(form);
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");
		return form + "\n<i><div align=right>Time Elapsed: " + totalTime + " nanoseconds/"
				+ seconds + " seconds</div></i>";
	}

	@POST
	@Path("/all")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbParty() throws JSONException {
		// return dbc.getData();
		return dbc.getDataInTable();
	}

	@POST
	@Path("/id")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyById_POST(@FormParam("party_ID") final String party_ID)
			throws JSONException {
		System.out.println(party_ID);
		return dbc.getDataByIdInTable(party_ID);
	}

	@GET
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/id")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByID_GET(@QueryParam("party_ID") final String party_ID)
			throws JSONException {
		return dbc.getDataByIdInTable(party_ID.toUpperCase());
	}

	@GET
	@Path("/limit/{limit}")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByLimit(@PathParam("limit") final String limit)
			throws JSONException {
		return dbc.getDataByLimitInTable(limit);
	}

	@POST
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/name")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByName_POST(
			@FormParam("party_name") final String party_name)
			throws JSONException {
		return dbc.getDataByNameInTable(party_name.toUpperCase());
	}

	@GET
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Path("/name")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByName_GET(
			@QueryParam("party_name") final String party_name)
			throws JSONException {
		return dbc.getDataByNameInTable(party_name.toUpperCase());
	}

	@GET
	@Path("/copy")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String copyDB() throws JSONException {
		return dbc.copyDB();
	}

	/**
	 * 
	 * @param pattern
	 * @param match
	 *            0 - Post; 1 - Pre; 2 - Full
	 * @return Data retrieved from database
	 * @throws JSONException
	 */
	@GET
	@Path("/names/like")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByLike_GET(@QueryParam("pattern") final String pattern,
			@QueryParam("match") final int match) throws JSONException {
		return dbc.getDataByLikeInTable(pattern.toUpperCase(), match);
	}
	
	@POST
	@Path("/names/like")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByLike_POST(@FormParam("pattern") final String pattern,
			@FormParam("match") final int match) throws JSONException {
		return dbc.getDataByLikeInTable(pattern.toUpperCase(), match);
	}

	@POST
	@Path("/names/regexp")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByRegExp_POST(@FormParam("pattern") final String pattern)
			throws JSONException {
		return dbc.getDataByRegExpInTable(pattern);
	}
	
	@GET
	@Path("/names/regexp")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByRegExp_GET(@QueryParam("pattern") final String pattern)
			throws JSONException {
		return dbc.getDataByRegExpInTable(pattern);
	}

	/**
	 * 
	 * @param sort
	 *            0 - Default; 1 - ASC; 2 - DESC
	 * @param col
	 *            column name
	 * @return Data retrieved from database
	 * @throws JSONException
	 */
	@POST
	@Path("/names/orderby")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByOrderBy_POST(@FormParam("sort") final int sort,
			@FormParam("col") final String col) throws JSONException {
		return dbc.getDataByOrderByInTable(sort, col);
	}
	

	@GET
	@Path("/names/orderby")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String dbPartyByOrderBy_GET(@QueryParam("sort") final int sort,
			@QueryParam("col") final String col) throws JSONException {
		return dbc.getDataByOrderByInTable(sort, col);
	}

	@GET
	@Path("/excel")
	@Produces({ MediaType.TEXT_HTML, MediaType.APPLICATION_JSON,
			MediaType.TEXT_PLAIN })
	public String excelRead() {
		long startTime = System.nanoTime();
		// Jedis jedis = new Jedis("localhost", 8080);
		// jedis.connect();

		File inputWkbk = new File(inputFile);
		Workbook w = null;
		try {
			w = Workbook.getWorkbook(inputWkbk);
		} catch (BiffException | IOException e) {
			e.printStackTrace();
		}
		Sheet sheet = w.getSheet(0);
		for (int i = 1; i < sheet.getRows(); i++) {
			JSONObject p = new Party();
			try {
				for (int j = 0; j < sheet.getColumns(); j++) {
					// jedis.set(sheet.getCell(j, 0).getContents(), sheet
					// .getCell(j, i).getContents());
					if (j == 5) {
						// System.out.println(jedis.get(sheet.getCell(j,
						// 0).getContents()));
					}
					p.put(sheet.getCell(j, 0).getContents(), sheet
							.getCell(j, i).getContents());
				}
				partyArr.put(p);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		System.out.println(partyArr.length());

		String json = "";
		/*
		 * Gson gson = new GsonBuilder().setPrettyPrinting().create(); try { for
		 * (int i = 0; i < partyArr.length(); i++) { json +=
		 * partyArr.getJSONObject(i).toString(3); } } catch (JSONException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); } //json +=
		 * "]"; json = json.replace("\\", "");
		 */

		try {
			json = partyArr.toString(3);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime - startTime;
		double seconds = (double) totalTime / 1000000000.0;
		System.out.println("Time Elapsed " + totalTime + " nanoseconds/"
				+ seconds + " seconds");

		return json;
	}

	/*
	 * @GET
	 * 
	 * @Produces(MediaType.TEXT_HTML) public JSONObject getPartyObject(int
	 * index) throws JSONException { return partyArr.getJSONObject(index); }
	 * 
	 * @GET
	 * 
	 * @Produces(MediaType.TEXT_HTML) public String
	 * getPartyName(@PathParam("index") final int index) throws JSONException {
	 * return partyArr.getJSONObject(index).get("PARTY_NAME").toString(); }
	 */
}
