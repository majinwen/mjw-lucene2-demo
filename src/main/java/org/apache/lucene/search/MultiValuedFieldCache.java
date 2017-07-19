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

public interface MultiValuedFieldCache extends ExtendedFieldCache {

  /**
   * @Deprecated int[m][n] use more heap memory than int[m*n] up to 100M 
   *   in 64bit JVM. use UnInvertedField23.
   */
  @Deprecated
  public static class MultiValuedStringIndex {

    /** All the term values, in natural order. */
    public final String[] lookup;

    /** For each document, an index into the lookup array. */
    public final int[][] orders;

    /** Creates one of these objects */
    public MultiValuedStringIndex (int[][] values, String[] lookup) {
      this.orders = values;
      this.lookup = lookup;
    }
  }

  /** Expert: The cache used internally by facet classes. */
  public static MultiValuedFieldCache DEFAULT = new MultiValuedFieldCacheImpl();

  /** Checks the internal cache for an appropriate entry, and if none
   * is found reads the term values in <code>field</code> and returns
   * an array of them in natural order, along with an array telling
   * which element in the term array each document uses.
   * @param reader  Used to get field values.
   * @param field   Which field contains the strings.
   * @return Array of terms and index into the array for each document.
   * @throws IOException  If any error occurs.
   */
  @Deprecated
  public MultiValuedStringIndex getStringsIndex(IndexReader reader, String field)
  throws IOException;

  public UnInvertedField23 getUnInvertedField23(IndexReader reader, String field)
  throws IOException;
}
