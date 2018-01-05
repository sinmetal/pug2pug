package org.sinmetal.beam.examples.storage2datastore;

/**
 * Created by sinmetal on 2017/09/20.
 */

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;

public class CSVToEntityFn extends DoFn<String, Entity> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        String[] columns = c.element().split(",");

        Key.Builder keyBuilder = Key.newBuilder();
        Key.PathElement pathElement = keyBuilder.addPathBuilder().setKind("ItemForCategoryJoin").setId(Long.parseLong(columns[0])).build();
        Key key = keyBuilder.setPath(0, pathElement).build();

        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(key);
        entityBuilder.putProperties("Name", Value.newBuilder().setStringValue(columns[1]).build());
        entityBuilder.putProperties("CategoryId", Value.newBuilder().setIntegerValue(Integer.parseInt(columns[2])).build());
        entityBuilder.putProperties("Price", Value.newBuilder().setIntegerValue(Integer.parseInt(columns[3])).build());
        c.output(entityBuilder.build());
    }
}
