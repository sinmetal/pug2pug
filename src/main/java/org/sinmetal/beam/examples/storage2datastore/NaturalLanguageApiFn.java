package org.sinmetal.beam.examples.storage2datastore;

import com.google.cloud.language.v1.AnalyzeSyntaxRequest;
import com.google.cloud.language.v1.AnalyzeSyntaxResponse;
import com.google.cloud.language.v1.Document;
import com.google.cloud.language.v1.Document.Type;
import com.google.cloud.language.v1.EncodingType;

import com.google.cloud.language.v1.LanguageServiceClient;
import com.google.cloud.language.v1.Token;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.List;


/**
 * Created by sinmetal on 2017/09/21.
 */
public class NaturalLanguageApiFn extends DoFn<String, List<Token>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        LanguageServiceClient language = null;
        try {
            language = LanguageServiceClient.create();
            // The text to analyze
            String text = c.element();
            Document doc = Document.newBuilder()
                    .setContent(text).setType(Type.PLAIN_TEXT).build();

            AnalyzeSyntaxRequest request = AnalyzeSyntaxRequest.newBuilder()
                    .setDocument(doc)
                    .setEncodingType(EncodingType.UTF16)
                    .build();

            AnalyzeSyntaxResponse response = language.analyzeSyntax(request);
            c.output(response.getTokensList());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (language != null) {
                try {
                    language.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
