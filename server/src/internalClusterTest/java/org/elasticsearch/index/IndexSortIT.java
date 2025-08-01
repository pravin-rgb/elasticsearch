/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;

public class IndexSortIT extends ESIntegTestCase {
    private static final XContentBuilder TEST_MAPPING = createTestMapping();

    private static XContentBuilder createTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("properties")
                .startObject("date")
                .field("type", "date")
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public void testIndexSort() {
        SortField dateSort = new SortedNumericSortField("date", SortField.Type.LONG, false);
        dateSort.setMissingValue(Long.MAX_VALUE);
        SortField numericSort = new SortedNumericSortField("numeric_dv", SortField.Type.INT, false);
        numericSort.setMissingValue(Integer.MAX_VALUE);
        SortField keywordSort = new SortedSetSortField("keyword_dv", false);
        keywordSort.setMissingValue(SortField.STRING_LAST);
        Sort indexSort = new Sort(dateSort, numericSort, keywordSort);
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "1")
                .putList("index.sort.field", "date", "numeric_dv", "keyword_dv")
        ).setMapping(TEST_MAPPING).get();
        for (int i = 0; i < 20; i++) {
            prepareIndex("test").setId(Integer.toString(i))
                .setSource("numeric_dv", randomInt(), "keyword_dv", randomAlphaOfLengthBetween(10, 20))
                .get();
        }
        flushAndRefresh();
        ensureYellow();
        assertSortedSegments("test", indexSort);
    }

    public void testIndexSortDateNanos() {
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "1")
                .put("index.sort.field", "@timestamp")
                .put("index.sort.order", "desc")
        ).setMapping("""
            {
              "properties": {
                "@timestamp": {
                  "type": "date_nanos"
                }
              }
            }
            """).get();

        flushAndRefresh();
        ensureYellow();

        SortField sf = new SortedNumericSortField("@timestamp", SortField.Type.LONG, true, SortedNumericSelector.Type.MAX);
        sf.setMissingValue(0L);
        Sort expectedIndexSort = new Sort(sf);
        assertSortedSegments("test", expectedIndexSort);
    }

    public void testInvalidIndexSort() {
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).putList("index.sort.field", "invalid_field"))
                .setMapping(TEST_MAPPING)
        );
        assertThat(exc.getMessage(), containsString("unknown index sort field:[invalid_field]"));

        exc = expectThrows(
            IllegalArgumentException.class,
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).putList("index.sort.field", "numeric"))
                .setMapping(TEST_MAPPING)
        );
        assertThat(exc.getMessage(), containsString("docvalues not found for index sort field:[numeric]"));

        exc = expectThrows(
            IllegalArgumentException.class,
            prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).putList("index.sort.field", "keyword"))
                .setMapping(TEST_MAPPING)
        );
        assertThat(exc.getMessage(), containsString("docvalues not found for index sort field:[keyword]"));
    }
}
