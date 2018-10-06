package hadoop.dataproc;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.JobField;
import com.google.cloud.bigquery.BigQuery.JobListOption;
import com.google.cloud.bigquery.BigQuery.JobOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;

public class BigQueryJobs {

	public static Map<String, BigQuery> queryObjectMap = new HashMap<String, BigQuery>();
	
	
	//Method to create a dummy Job in BigQuery. Run this for testing your jobs.
	/* Modify the BQ dataset name, BQ table name and source URI of dataset
	 * in the Google Cloud Storage.
	 * */
	public static void createJob(BigQuery service) throws Exception {
		String datasetName = "test_dataset";
		String tableName = "keytable";
		String sourceUri = "gs://test-gcp-01/Datasets/SampleData.csv";
		TableId tableId = TableId.of(datasetName, tableName);
		// Table field definition
		Field[] fields = new Field[] { Field.of("data1", LegacySQLTypeName.STRING),
				Field.of("data2", LegacySQLTypeName.STRING), Field.of("data3", LegacySQLTypeName.STRING),
				Field.of("data4", LegacySQLTypeName.STRING) };
		// Table schema definition
		Schema schema = Schema.of(fields);
		LoadJobConfiguration configuration = LoadJobConfiguration.builder(tableId, sourceUri)
				.setFormatOptions(FormatOptions.json()).setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
				.setSchema(schema).build();
		// Load the table
		Job loadJob = service.create(JobInfo.of(configuration));
		loadJob = loadJob.waitFor();
		// Check the table
	}
	
	/* View the BigQuery job details
	 * provided the Job object
	 * */
	public static void viewJobDetails(Job job) {

		System.out.println("JOB STATUS - " + job.getStatus().getState());
		System.out.println("JOB ERRORS - " + job.getStatus().getError());
		System.out.println("JOB EXECUTION ERRORS" + job.getStatus().getExecutionErrors());

	}
	
	/* View BigQuery job details
	 * provided job id.
	 * */
	public static void viewJobDetails(String jobId) throws Exception {

		BigQuery query = initializeBQ("gcp-bigquery-demo", getCredentials());

		Job job = query.getJob(jobId, JobOption.fields(JobField.values()));

		viewJobDetails(job);
	}
	
	/* retrieve the BigQuery Job Object
	 * given the Job ID
	 * */
	public static Job getJob(String jobID) throws Exception {

		BigQuery query = initializeBQ("gcp-bigquery-demo", getCredentials());
		Job job = query.getJob(jobID, JobOption.fields(JobField.values()));
		return job;

	}
	
	/* Cancel BigQuery Job 
	 * provided the Job object
	 * */
	public static boolean cancelJob(Job job) {

		return job.cancel();

	}
	/* Cancel the BigQuery Job
	 * provided the Job ID
	 * */
	public static boolean cancelJob(String jobID) throws Exception {

		return cancelJob(getJob(jobID));
	}
	
	/* BigQuery cannot reload/repeat the job
	 * create another job from same job 
	 * provided the Job Object.
	 * */
	public static Job recreateJob(Job job) throws Exception {

		Job newJob = job.toBuilder().setJobId(null).build();
		BigQuery query = initializeBQ("gcp-bigquery-demo", getCredentials());
		Job tempJob = query.create(newJob, JobOption.fields(JobField.values()));
		// tempJob.wait();
		// Thread.sleep(10);
		return tempJob;

	}
	
	
	/* BigQuery cannot reload/repeat the job
	 * create another job from same job 
	 * provided the Job ID as string.
	 * */
	public static Job recreateJob(String jobID) throws Exception {

		return recreateJob(getJob(jobID));

	}
	
	
	/* Initialize the BigQuery service
	 * from the GCP projectID as string &
	 * GoogleCredentials loaded from service accounts json file.
	 * */
	public static BigQuery initializeBQ(String projectID, GoogleCredentials credentials) throws Exception {
		
		System.out.println(credentials.getRequestMetadata().get("project_id"));
		
		if (credentials == null) {
			throw new Exception("Account Crdentials are null");

		}
		if (queryObjectMap.containsKey(projectID)) {
			return queryObjectMap.get(projectID);
		}
		
		else {
			BigQuery query = BigQueryOptions.newBuilder().setProjectId(credentials.getRequestMetadata().get("project_id").toString()).build().getService();
			queryObjectMap.put(projectID, query);
			return query;

		}

	}
	
	
	/* Load the GoogleCredentials object
	 * from the service account json file
	 * stored as part of this project
	 * */
	public static GoogleCredentials getCredentials() throws Exception {

		GoogleCredentials credentials = null;
		InputStream credentialsPath = BigQueryJobs.class.getResourceAsStream("service-account.json");

		// FileInputStream serviceAccountStream = new
		// FileInputStream(credentialsPath);
		try {
			credentials = ServiceAccountCredentials.fromStream(credentialsPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return credentials;
	}

	//TODO main method for testing all stubs
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		BigQuery query = initializeBQ("gcp-bigquery-demo", getCredentials());
		com.google.api.gax.paging.Page<Job> page = query.listJobs(JobListOption.allUsers());
		// createJob(query);
		Iterator<Job> itr = page.getValues().iterator();

		while (itr.hasNext()) {
			Job job = itr.next();
			viewJobDetails(job);

			System.out.println(" ************** ");
			viewJobDetails(job.getJobId().getJob());
			//cancelJob(job.getJobId().getJob());
			System.out.println(" ************* Reload Job ");

			// Job jobObj = recreateJob(job.getJobId().getJob());
			// System.out.println("Old JOB ID - " + jobObj.getJobId().getJob());

		}

	}

}
