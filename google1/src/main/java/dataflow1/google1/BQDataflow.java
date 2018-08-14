package dataflow1.google1;

import java.util.Iterator;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.api.services.bigquery.model.TableRow;

public class BQDataflow {

	public static void main1(String[] args) {
		// TODO Auto-generated method stub

		PipelineOptions options = PipelineOptionsFactory.create();

		PipelineRunner<? extends PipelineResult> pipeline = DirectRunner.fromOptions(options).create();

		Pipeline p = Pipeline.create(options);

		String url = "jdbc:mysql://35.196.141.216:3306/test?verifyServerCertificate=false&useSSL=true";


		org.apache.beam.sdk.values.PCollection<TableRow> bqCollection = p.apply(BigQueryIO.read().fromQuery("SELECT Plate_ID ,Registration_State ,Issue_Date  FROM `[test_training_ds].[nyc_parking_tickets_2015]` LIMIT 1000"));

		DataSourceConfiguration config = DataSourceConfiguration.create("com.mysql.jdbc.Drive", url).withUsername("root").withPassword("Onkar171@");
		//bqCollection.apply(JdbcIO.write().withDataSourceConfiguration(config).withStatement("insert into dump values "));


		org.apache.beam.sdk.values.PCollection<String> out = bqCollection.apply(ParDo.of(new DoFn<TableRow, String>() {
			@ProcessElement
			public void processElement(ProcessContext c) {

				Iterator itr = c.element().keySet().iterator();
				while(itr.hasNext()){
					String key = itr.next().toString();
					String value = c.element().get(key).toString();
					c.output(key+" : "+value);
				}

			}
		}) );

		out.apply(TextIO.write().to("dataflow"));
		pipeline.run(p);
	}

}
