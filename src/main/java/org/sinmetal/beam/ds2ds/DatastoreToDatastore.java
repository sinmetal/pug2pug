package org.sinmetal.beam.ds2ds;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.Query;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class DatastoreToDatastore {

    public interface DatastoreToDatastoreOptions extends GcpOptions {

        @Description("Input Datastore Project Id")
        @Default.String("input")
        String getInputProjectId();
        void setInputProjectId(String value);

        @Description("Output Datastore Project Id")
        @Default.String("output")
        String getOutputProjectId();
        void setOutputProjectId(String value);
    }

    public static class EntityMigration extends PTransform<PCollection<Entity>, PCollection<Entity>> {
        @Override
        public PCollection<Entity> expand(PCollection<Entity> entities) {
            return entities.apply(ParDo.of(new EntityMigrationFn()));
        }
    }

    public static void main(String[] args) {

        DatastoreToDatastoreOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DatastoreToDatastoreOptions.class);
        Pipeline p = Pipeline.create(options);

        Query query = Query.newBuilder().build();
        p.apply(DatastoreIO.v1().read().withProjectId(options.getInputProjectId()).withQuery(query))
                .apply(new EntityMigration())
                .apply(DatastoreIO.v1().write().withProjectId(options.getInputProjectId()));

        p.run();
    }
}
