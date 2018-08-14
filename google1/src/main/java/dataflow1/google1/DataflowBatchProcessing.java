package dataflow1.google1;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;



public class DataflowBatchProcessing {
	
	
	public interface DataflowBatchOptions extends PipelineOptions {
		
		@Description("Set the input file.")
		
		@Default.String("gs://test-gcp-01/Datasets/Parking_Violations_Issued_-_Fiscal_Year_2015.csv")
		String getInputFile();
		void setInputFile(String file);
		
		@Description("Set the output file.")
		
		@Required
		String getOutputFile();
		void setOutputFile(String file);
	}
	
	
	
	

	public static void main(String[] args) throws FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		DataflowBatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowBatchOptions.class);
		
		Pipeline p = Pipeline.create(options);
		
		//org.apache.beam.sdk.values.PCollection<String> collection= p.apply(TextIO.read().from(options.getInputFile()));
		//org.apache.beam.sdk.values.PCollection<String> collection = p.apply(TextIO.read().from("gs://test-gcp-01/Datasets/Parking_Violations_Issued_-_Fiscal_Year_2015.csv"));
		p.apply(TextIO.read().from(options.getInputFile())).apply(ParDo.of(new DoFn<String, String>() {
			@ProcessElement
			
			public void processElement(ProcessContext context){
				String data = context.element();
				String allData[] = data.split(",");
				System.out.println(allData.length);
				
				String plateType = allData[3];
				String dateValue = allData[4];
				String monthValue = "";
				
				if(dateValue.split("/").length==3) {
					 monthValue = dateValue.split("/")[0].trim();
				}
				
			if(plateType.equals("COM") && monthValue.equals("10")){
				context.output(data);
			}
			}
		
		})).apply(TextIO.write().to(options.getOutputFile()));
		
		 p.run().waitUntilFinish();
		
		
	}

}
