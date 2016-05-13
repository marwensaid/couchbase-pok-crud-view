package crud;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.concurrent.TimeUnit;

public class CRUD {
	public static void main(String args[]) {
		//initial connection to the cluster
		Cluster cluster = CouchbaseCluster.create("192.168.0.21");
		//retrieving the data bucket
		Bucket bucket = cluster.openBucket("test", 60, TimeUnit.SECONDS);
		
		//creating a new json object
		JsonObject person1 = JsonObject.empty()
				.put("firstname", "Patrick")
				.put("lastname", "Komon")
				.put("sex", "m")
				.put("age", 17);
		//creating a json document with the json object
		JsonDocument doc1 = JsonDocument.create("pkomon", person1);
		//inserts (and possible overwrites) the json document in the bucket
		JsonDocument response = bucket.upsert(doc1);
		
		JsonObject person2 = JsonObject.empty()
				.put("firstname", "Lukas")
				.put("lastname", "Meyer")
				.put("sex", "m")
				.put("age", 18);
		JsonDocument doc2 = JsonDocument.create("lmayer", person2);
		JsonDocument response2 = bucket.upsert(doc2);
		
		//retrieving a json document from the bucket
		JsonDocument pkomon = bucket.get("pkomon");
		if (pkomon == null) {
			System.err.println("Document not found!");
		} else {
			//extracting data from the document
			System.out.println("Age: " + pkomon.content().getInt("age"));
			//updating data from the document
			pkomon.content().put("age", 18);
			System.out.println("Age: " + pkomon.content().getInt("age"));
			//send update to the bucket
			JsonDocument updated = bucket.replace(pkomon);
		}
		
		JsonDocument lmayer = bucket.get("lmayer");
		if (lmayer == null) {
			System.err.println("Document not found!");
		} else {
			System.out.println("Last name: " + lmayer.content().getString("lastname"));
			lmayer.content().put("lastname", "Mayer");
			System.out.println("Last name: " + lmayer.content().getString("lastname"));
			JsonDocument updated = bucket.replace(lmayer);
		}
		//deleting a json document
//		bucket.remove("lmayer");
		//clsong the connection
		cluster.disconnect();
	}
}