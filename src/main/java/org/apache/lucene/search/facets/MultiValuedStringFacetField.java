package org.apache.lucene.search.facets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MultiValuedFieldCache;
import org.apache.lucene.search.UnInvertedField23;
import org.apache.lucene.search.UnInvertedField23.FacetTerms;

public class MultiValuedStringFacetField extends AbstractFacetField implements FacetField {

    IndexReader reader;

    String field;

    private UnInvertedField23 uif;

    private FacetTerms fterms;

    public MultiValuedStringFacetField(IndexReader reader, String fieldName) throws IOException {
        this.reader = reader;
        this.field = fieldName;
        uif = MultiValuedFieldCache.DEFAULT.getUnInvertedField23(reader, fieldName);
        MultiValuedFieldCache.DEFAULT.getStrings(reader, fieldName);
        fterms = uif.createFacetTerms();
    }

    public String field() {
        return field;
    }

    public void collect(int doc, float score) {
        uif.accumulateTermCount(doc, fterms);
    }

    public List<FacetEntry> getFacetResult() {
        return getFacetResult(1);
    }

    private List<FacetEntry> entries = null;
    public List<FacetEntry> getFacetResult(int minCount) {
        if(minCount == 1 && entries != null){
            return entries;
        }
        entries = new ArrayList<FacetEntry>();
        try {
            Iterator<Map.Entry<String, Integer>> iter = uif.iteratorFacetTerms(fterms, reader, minCount);
            while (iter.hasNext()) {
                Map.Entry<String, Integer> entry = iter.next();
                entries.add(new StringEntry(entry.getKey(), entry.getValue()));
            }
        } catch (IOException ioe) {
            // ignore it
        }
        return entries;
    }

    @Deprecated
    public Map<String, Integer> getFacetResultinHm() {
        return getFacetMap(1);
    }

    public Map<String, Integer> getFacetMap() {
        return getFacetMap(1);
    }

    public Map<String, Integer> getFacetMap(int minCount) {
        Map<String, Integer> result = new HashMap<String, Integer>();
        try {
            Iterator<Map.Entry<String, Integer>> iter = uif.iteratorFacetTerms(fterms, reader, minCount);
            while (iter.hasNext()) {
                Map.Entry<String, Integer> entry = iter.next();
                result.put(entry.getKey(), entry.getValue());
            }
        } catch (IOException ioe) {
            // ignore it
        }
        return result;
    }
}
