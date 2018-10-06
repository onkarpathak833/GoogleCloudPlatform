#INSTRUCTIONS

1. Run the java file BigQueryJobs.java to View status of jobs, Cancel Jobs and Rerun/Re-create the BigQuery jobs.
2. You will need Google Service Account credentials file in json format created from API & Services -> Credentials tab of Google Cloud Platform. Browse to the API & Services menu from the main Navigation Menu of GCP on top left corner.
3. Place this file inside resource folder of the java project or from where you want to load it in the Java code.
4. Make sure you have sufficient permissions assigned to your user while creating Service Account Credentials. e.g. BigQuery Admin, BigQuery Editor etc.
5. Package the jar using Maven Build utility and run the Main class in the Jar to execute the methods.
6. Methods in this utility can be exposed directly as an API to re-use in your application.


#NOTES

1. Use JDK version 1.7 and above (This code is tested on JDK Version 1.8).
2. BigQuery API Version 1.41.0 client libraries are used in this utility.