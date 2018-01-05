package org.sinmetal.beam.ironmegalodon;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Ironmegalodon {

    public interface IronmegalodonOptions extends GcpOptions {
    }

    public static void main(String[] args) {

        IronmegalodonOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                        .as(IronmegalodonOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from("gs://hoge/input")).apply(TextIO.write().to("gs://hoge/output"));
        p.run();
    }
}
