package org.sinmetal.beam.ds2ds;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.HashMap;
import java.util.Map;

/**
 * PropertyNameを変更するFn
 */
public class EntityMigrationFn extends DoFn<Entity, Entity> {

    private static Map<String, String> migrationPropertyNameMap = new HashMap<>();

    static {
        migrationPropertyNameMap.put("", "");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Entity currentEntity = c.element();
        Entity.Builder newEntityBuilder = Entity.newBuilder();
        newEntityBuilder.setKey(currentEntity.getKey());
        for (Map.Entry<String, Value> entry : currentEntity.getPropertiesMap().entrySet()) {
            newEntityBuilder.putProperties(migrationPropertyName(entry.getKey()), entry.getValue());
        }

        c.output(newEntityBuilder.build());
    }

    private String migrationPropertyName(String propertyName) {
        if (migrationPropertyNameMap.containsKey(propertyName)) {
            return migrationPropertyNameMap.get(propertyName);
        }
        return propertyName;
    }
}