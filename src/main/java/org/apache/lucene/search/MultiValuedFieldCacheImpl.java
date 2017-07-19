package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

class MultiValuedFieldCacheImpl extends ExtendedFieldCacheImpl implements MultiValuedFieldCache {
  // inherit javadocs
  public MultiValuedStringIndex getStringsIndex(IndexReader reader, String field)
      throws IOException {
    return (MultiValuedStringIndex) stringsIndexCache.get(reader, field);
  }

  private static final int DEFAULT_MULTI_VALUED_FIELD_LENGTH = 8;
  Cache stringsIndexCache = new Cache() {

    protected Object createValue(IndexReader reader, Object fieldKey)
        throws IOException {
      String field = ((String) fieldKey).intern();
      final int[][] tmpArrays = new int[reader.maxDoc()][];
      String[] mterms = new String[reader.maxDoc()+1];
      TermDocs termDocs = reader.termDocs();
      TermEnum termEnum = reader.terms (new Term (field, ""));
      int t = 0;  // current term number

      // an entry for documents that have no terms in this field
      // should a document with no terms be at top or bottom?
      // this puts them at the top - if it is changed, FieldDocSortedHitQueue
      // needs to change as well.
      mterms[t++] = null;

      try {
        do {
          Term term = termEnum.term();
          if (term==null || term.field() != field) break;

          // resize mterms
          if (t >= mterms.length) {
            int newLength = mterms.length * 2 -1;
            String[] newArray = new String[newLength];
            System.arraycopy(mterms, 0, newArray, 0, t);
            mterms = newArray;
          }
          mterms[t] = term.text();

          termDocs.seek (termEnum);
          while (termDocs.next()) {
            int doc = termDocs.doc();
            // tmpArrays[doc][0] store the item count of tmpArrays[doc][]
            if (tmpArrays[doc] == null) {
              tmpArrays[doc] = new int[DEFAULT_MULTI_VALUED_FIELD_LENGTH];
              tmpArrays[doc][0] = 0;
            } else if (tmpArrays[doc][0] == (tmpArrays[doc].length -1)){
              int newLength = (int) (tmpArrays[doc][0] * 2);
              int[] newArray = new int[newLength];
              System.arraycopy(tmpArrays[doc], 0, newArray, 0, tmpArrays[doc].length);
              tmpArrays[doc] = newArray;
            }
            tmpArrays[doc][0] ++;
            tmpArrays[doc][tmpArrays[doc][0]] = t;
          }

          t++;
        } while (termEnum.next());
      } finally {
        termDocs.close();
        termEnum.close();
      }

      int[][] retArray = null;
      if (t == 0) {
        // if there are no terms, make the term array
        // have a single null entry
        mterms = new String[1];
        retArray = new int[reader.maxDoc()][0];
      } else {
        if (t < mterms.length) {
          // if there are less terms than documents,
          // trim off the dead array space
          String[] terms = new String[t];
          System.arraycopy (mterms, 0, terms, 0, t);
          mterms = terms;
        }

        retArray = new int[reader.maxDoc()][];
        for (int i=0, n=reader.maxDoc(); i<n; i++) {
          int[] array = tmpArrays[i];
          if (array == null || array[0] == 0) {
            retArray[i] = new int[0];
          } else {
            int[] newArray = new int[array[0]];
            System.arraycopy(array, 1, newArray, 0, array[0]);
            retArray[i] = newArray;
          }
        }
      }

      MultiValuedStringIndex value = new MultiValuedStringIndex(retArray, mterms);
      return value;
    }
  };

  public UnInvertedField23 getUnInvertedField23(IndexReader reader, String field) throws IOException {
    return (UnInvertedField23)unInvertedFieldCache.get(reader, field);
  }

  Cache unInvertedFieldCache = new Cache() {
    protected Object createValue(IndexReader reader, Object fieldKey)
        throws IOException {
      String field = ((String) fieldKey).intern();
      return new UnInvertedField23(field, reader);
    }
  };

  public void purge(IndexReader reader) {
    unInvertedFieldCache.purge(reader);
    stringsIndexCache.purge(reader);
  }
}

