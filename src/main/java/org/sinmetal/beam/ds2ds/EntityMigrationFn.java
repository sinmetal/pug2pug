package org.sinmetal.beam.ds2ds;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.HashMap;
import java.util.List;
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

        // KeyにはApplicationIDが含まれているので、別GCP Projectに移行する時はKeyを作り直す必要がある
        Key newEntityKey = buildKey(currentEntityKey.getPathList());
        Entity.Builder newEntityBuilder = Entity.newBuilder();
        newEntityBuilder.setKey(newEntityKey);
        for (Map.Entry<String, Value> entry : currentEntity.getPropertiesMap().entrySet()) {
            newEntityBuilder.putProperties(migrationPropertyName(entry.getKey()), entry.getValue());
        }

        c.output(newEntityBuilder.build());
    }

    private Key buildKey(List<Key.PathElement> pathElements) {
        // TODO Namespanceの情報が入ってないような気がする
        Key.Builder keyBuilder = Key.newBuilder();
        for (Key.PathElement pathElement : pathElements) {
            Key.PathElement.Builder keyPathBuilder = Key.PathElement.newBuilder();
            keyPathBuilder.setKind(pathElement.getKind());
            if (pathElement.getId() != 0) {
                keyPathBuilder.setId(pathElement.getId());
            } else {
                keyPathBuilder.setName(pathElement.getName());
            }
            keyBuilder.addPath(keyPathBuilder.build());
        }
        return keyBuilder.build();
    }

    private String migrationPropertyName(String propertyName) {
        if (migrationPropertyNameMap.containsKey(propertyName)) {
            return migrationPropertyNameMap.get(propertyName);
        }
        return propertyName;
    }
}
