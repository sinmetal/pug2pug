package org.sinmetal.beam.examples.storage2datastore;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Created by sinmetal on 2017/09/21.
 */
public class CSVToCategoryKVFn extends DoFn<String, KV<Integer, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String[] columns = c.element().split(",");

        // 0番目にID, 1番目にNameが入っているのが前提
        c.output(KV.of(Integer.parseInt(columns[0]),columns[1]));
    }
}
