package view;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.view.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class View {
	Cluster cluster;
	BucketManager bucketManager;
	Bucket bucket;
	public void getConnection(){
		cluster = CouchbaseCluster.create("192.168.0.21");
		bucket = cluster.openBucket("test", 60, TimeUnit.SECONDS);
		bucketManager = bucket.bucketManager();
	}
	public void createView(){
		//throws a desingdocumentalreadyexistsexception if the design document already exists
		DesignDocument designDoc = DesignDocument.create("persontest",
				Arrays.asList(
						DefaultView.create("by_name",
							"function (doc, meta) { emit(meta.id, [doc.firstname, doc.age]); }", "_count")
						));
		//dev view -> true parameter; production view -> no 2. parameter;
		bucketManager.insertDesignDocument(designDoc, true);
	}
	public void getView(){
		DesignDocument designDoc = bucketManager.getDesignDocument("persontest", true);
		System.out.println(designDoc.name() + " has " + designDoc.views().size() + " views");
		// Iterate over all production design documents
		List<DesignDocument> designDocs = bucketManager.getDesignDocuments(true);
		System.out.println("bucket 'test' has " + designDocs.size() + " design documents:");
		for (DesignDocument doc : designDocs) {
			System.out.println(doc.name() + " has " + doc.views().size() + " views");
		}
	}
	public void deleteView(){
		bucketManager.removeDesignDocument("persontest");
		bucketManager.removeDesignDocument("persontest",true);
	}
	private void updateView() {
		DesignDocument designDoc = bucketManager.getDesignDocument("persontest", true);
		//update the "by_country" view, adding a reduce
		designDoc.views().add(
			DefaultView.create("by_name", //reuse same name
					"function (doc, meta) { emit(meta.id, [doc.lastname, doc.age]); }", "_count")
			);
		//resend to server
		bucketManager.upsertDesignDocument(designDoc, true);
	}
	public void queryView(){
		ViewResult result = bucket.query(ViewQuery.from("persontest", "by_name").development(true));
		for (ViewRow row : result) {
		    System.out.println(row);
		}
	}
	public static void main(String args[]) {
		View view = new View();
		view.getConnection();
//		view.createView();
		view.getView();
		view.updateView();
		view.queryView();
//		view.deleteView();
		view.cluster.disconnect();
	}
}