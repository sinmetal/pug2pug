package org.sinmetal.beam.examples.storage2datastore;

import com.google.cloud.language.v1.Token;
import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

/**
 * Created by sinmetal on 2017/09/01.
 */
public class StorageToDatastore {

    public interface CSVToDatastoreOptions extends GcpOptions {

        @Description("Input File Path. Example gs://hoge/hoge.csv")
        @Default.String("ga://hoge/data.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Input Category Master File Path. Example gs://hoge/category.csv")
        @Default.String("ga://hoge/category.csv")
        String getCategoryMasterInputFile();
        void setCategoryMasterInputFile(String value);
    }

    public static class NaturalLanguageApi extends PTransform<PCollection<String>, PCollection<List<Token>>> {
        @Override
        public PCollection<List<Token>> expand(PCollection<String> lines) {
            return lines.apply(ParDo.of(new NaturalLanguageApiFn()));
        }
    }

    public static class TokensToEntity extends PTransform<PCollection<List<Token>>, PCollection<Entity>> {
        @Override
        public PCollection<Entity> expand(PCollection<List<Token>> lines) {
            return lines.apply(ParDo.of(new NLTokenToEntityFn()));
        }
    }

    public static class CSVToDatastore extends PTransform<PCollection<String>, PCollection<Entity>> {
        @Override
        public PCollection<Entity> expand(PCollection<String> lines) {
            return lines.apply(ParDo.of(new CSVToEntityFn()));
        }
    }

    public static void main(String[] args) {
        CSVToDatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CSVToDatastoreOptions.class);
        Pipeline p = Pipeline.create(options);

        p.apply("Read Item Master", TextIO.read().from(options.getInputFile()))
                .apply("Call Natural Language Api", new NaturalLanguageApi())
                .apply("NL Tokens Transfer To Datastore Entity", new TokensToEntity())
                .apply(DatastoreIO.v1().write().withProjectId(options.getProject()));
                //.apply(TextIO.write().to("gs://input-sinmetal-dataflow/nl-results.json"));

        p.run();
    }
}
