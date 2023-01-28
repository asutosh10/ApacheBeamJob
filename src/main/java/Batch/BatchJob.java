package Batch;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.util.Arrays;
import java.util.List;

public class BatchJob {
    public static void main(String[] args) {
        DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        pipelineOptions.setProject("vaulted-acolyte-376115");
        pipelineOptions.setJobName("batchJob3");
        pipelineOptions.setRunner(DataflowRunner.class);
        pipelineOptions.setRegion("us-east1");
        pipelineOptions.setGcpTempLocation("gs://apache_project/tmp");
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        final List<String> namesArray = Arrays.asList("messi", "rooney", "odeegard", "saka");
        PCollection<String> names = pipeline.apply(Create.of(namesArray));
        names.apply(TextIO.write().to("gs://apache_project/output").withSuffix(".txt"));
        names.apply(PubsubIO.writeStrings().to("projects/vaulted-acolyte-376115/topics/MyTopic"));
        pipeline.run().waitUntilFinish();

    }
}
