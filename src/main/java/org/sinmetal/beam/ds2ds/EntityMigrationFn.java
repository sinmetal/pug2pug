package org.sinmetal.beam.ds2ds;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
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
        migrationPropertyNameMap.put("OrganizationId", "OrganizationID");
        migrationPropertyNameMap.put("Url", "URL");
        migrationPropertyNameMap.put("LogoUrl", "LogoURL");
        migrationPropertyNameMap.put("SlackPostUrl", "SlackPostURL");
        migrationPropertyNameMap.put("Owner.UserId", "Owner.UserID");
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Entity currentEntity = c.element();
        
        Key currentEntityKey = currentEntity.getKey();
        Key.Builder keyBuilder = Key.newBuilder();
        Key.PathElement currentKeyPath = currentEntityKey.getPath(0);
        Key.PathElement.Builder keyPathBuilder = Key.PathElement.newBuilder();
        keyPathBuilder.setKind(currentKeyPath.getKind());
        if (currentKeyPath.getId() != 0) {
            keyPathBuilder.setId(currentKeyPath.getId());
        } else {
            keyPathBuilder.setName(currentKeyPath.getName());
        }
        keyBuilder.addPath(keyPathBuilder.build());
        Key key = keyBuilder.build();

        Entity.Builder newEntityBuilder = Entity.newBuilder();
        newEntityBuilder.setKey(key);
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
