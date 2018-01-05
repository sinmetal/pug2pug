package org.sinmetal.beam.examples.storage2datastore;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

/**
 * Created by sinmetal on 2017/09/21.
 */
public class JoinCategoryMasterFn extends DoFn<Entity, Entity> {

    private final PCollectionView<Map<Integer, String>> categoryMasterView;

    public JoinCategoryMasterFn(PCollectionView<Map<Integer, String>> categoryMasterView) {
        this.categoryMasterView = categoryMasterView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Map<Integer, String> categoryMaster = c.sideInput(this.categoryMasterView);
        Entity entity = c.element();

        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder.setKey(entity.getKey());
        entityBuilder.putAllProperties(entity.getPropertiesMap());

        // set Category Name
        long categoryId = entity.getPropertiesMap().get("CategoryId").getIntegerValue();
        String categoryName = categoryMaster.get((int)categoryId);
        entityBuilder.putProperties("CategoryName", Value.newBuilder().setStringValue(categoryName).build());
        
        c.output(entityBuilder.build());
    }
}
