package org.apache.hadoop.metadata.bridge.hivelineage.hook;

import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.google.gson.Gson;

public class HiveLineageInfoTest {
	public static String parseQuery(String query) throws SemanticException,
			ParseException {
		HiveLineageInfo lep = new HiveLineageInfo();
		lep.getLineageInfo(query);
		Gson gson = new Gson();
		String jsonOut = gson.toJson(lep.getHLBean());
		return jsonOut;
	}
}
