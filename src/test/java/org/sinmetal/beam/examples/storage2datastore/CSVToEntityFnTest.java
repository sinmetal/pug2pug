package org.sinmetal.beam.examples.storage2datastore;

import com.google.datastore.v1.Entity;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Created by sinmetal on 2017/09/20.
 */
@RunWith(JUnit4.class)
public class CSVToEntityFnTest {

    @Test
    public void testCSVToEntityFn() throws Exception {
        DoFnTester<String, Entity> extractCSVToEntityFn = DoFnTester.of(new CSVToEntityFn());

        List<Entity> entities = extractCSVToEntityFn.processBundle("1,GCPUG標準Tシャツ,1,1500");
        Assert.assertThat(entities.size(), CoreMatchers.is(1));
        Assert.assertThat(entities.get(0).getKey().getPath(0).getKind(), CoreMatchers.is("ItemForCategoryJoin"));
        Assert.assertThat(entities.get(0).getKey().getPath(0).getId(), CoreMatchers.is(1L));
        Assert.assertThat(entities.get(0).getPropertiesMap().get("Name").getStringValue(), CoreMatchers.is("GCPUG標準Tシャツ"));
        Assert.assertThat(entities.get(0).getPropertiesMap().get("CategoryId").getIntegerValue(), CoreMatchers.is(1L));
        Assert.assertThat(entities.get(0).getPropertiesMap().get("Price").getIntegerValue(), CoreMatchers.is(1500L));
    }
}
