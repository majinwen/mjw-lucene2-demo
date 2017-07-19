package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.facets.FacetField;

public class WLPTopFieldDocCollector extends TopFieldDocCollector {

    private IndexReader reader;

    // 需要统计的字段
    private List<FacetField> facetFields = new ArrayList<FacetField>();

    public WLPTopFieldDocCollector(IndexReader reader, Sort sort, int nDoc)
            throws IOException {
        super(reader, sort, nDoc);
        this.reader = reader;
    }

    // 添加新的统计项
    public void addFacetField(FacetField facetField) {
        facetFields.add(facetField);
    }

    public void collect(int doc, float score) {
        super.collect(doc, score);

        for (int i=0, n=facetFields.size(); i<n; i++) {
            FacetField facetField = facetFields.get(i);
            facetField.collect(doc, score);
        }
    }

    public List<FacetField> getFacetFields() {
        return facetFields;
    }

    public Map<String, List<FacetField.FacetEntry>> getFacetValueMap() {
        Map<String, List<FacetField.FacetEntry>> facetValues = new HashMap<String, List<FacetField.FacetEntry>>();
        for (int i=0, n=facetFields.size(); i<n; i++) {
            FacetField facetField = facetFields.get(i);
            List<FacetField.FacetEntry> entries = facetField.getFacetResult();
            facetValues.put(facetField.field(), entries);
        }
        return facetValues;
    }
}
