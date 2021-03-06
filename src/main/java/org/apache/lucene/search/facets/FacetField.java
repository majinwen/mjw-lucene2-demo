package org.apache.lucene.search.facets;

import java.util.List;

public interface FacetField {
    public String field();

    public void collect(int doc, float score);

    public List<FacetEntry> getFacetResult();

    public List<FacetEntry> getFacetResult(int minCount);

    interface FacetEntry<K,V> {
        /**
         * Returns the key corresponding to this entry.
         *
         * @return the key corresponding to this entry.
         * @throws IllegalStateException implementations may, but are not
         *         required to, throw this exception if the entry has been
         *         removed from the backing map
         */
        K getKey();

        /**
         * Returns the value corresponding to this entry.  If the mapping
         * has been removed from the backing map (by the iterator's
         * <tt>remove</tt> operation), the results of this call are undefined.
         *
         * @return the value corresponding to this entry.
         * @throws IllegalStateException implementations may, but are not
         *         required to, throw this exception if the entry has been
         *         removed from the backing map
         */
        V getValue();
    }
}
