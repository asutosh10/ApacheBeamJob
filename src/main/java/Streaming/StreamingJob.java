package Streaming;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.time.LocalDateTime;

public class StreamingJob {
    public static void main(String[] args) {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setJobName("streamingJob");
        pipelineOptions.setProject("vaulted-acolyte-376115");
        pipelineOptions.setRegion("us-east1");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setGcpTempLocation("gs://apache_project/tmp");
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        PCollection<String> messages = pipeline.apply(PubsubIO.readStrings().fromTopic("projects/vaulted-acolyte-376115/topics/MyTopic"));
        PCollection<TableRow> tableRows = messages.apply(ParDo.of(new DoFn<String, TableRow>() {
            @ProcessElement
            public void processing(@Element String message, OutputReceiver<TableRow> receiver) {
                TableRow tableRow = new TableRow();
                tableRow.set("message", message).set("timestamp", LocalDateTime.now().toString());
                receiver.output(tableRow);
            }

        }));
        tableRows.apply(BigQueryIO.writeTableRows().to("vaulted-acolyte-376115.myDataset.myTable")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        pipeline.run();



    }
}
