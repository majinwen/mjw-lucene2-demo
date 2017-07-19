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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;


/**
 *
 * Final form of the un-inverted field:
 *   Each document points to a list of term numbers that are contained in that document.
 *
 *   Term numbers are in sorted order, and are encoded as variable-length deltas from the
 *   previous term number.  Real term numbers start at 2 since 0 and 1 are reserved.  A
 *   term number of 0 signals the end of the termNumber list.
 *
 *   There is a single int[maxDoc()] which either contains a pointer into a byte[] for
 *   the termNumber lists, or directly contains the termNumber list if it fits in the 4
 *   bytes of an integer.  If the first byte in the integer is 1, the next 3 bytes
 *   are a pointer into a byte[] where the termNumber list starts.
 *
 *   There are actually 256 byte arrays, to compensate for the fact that the pointers
 *   into the byte arrays are only 3 bytes long.  The correct byte array for a document
 *   is a function of it's id.
 *
 *   To save space and speed up faceting, any term that matches enough documents will
 *   not be un-inverted... it will be skipped while building the un-inverted field structure,
 *   and will use a set intersection method during faceting.
 *
 *   To further save memory, the terms (the actual string values) are not all stored in
 *   memory, but a TermIndex is used to convert term numbers to term values only
 *   for the terms needed after faceting has completed.  Only every 128th term value
 *   is stored, along with it's corresponding term number, and this is used as an
 *   index to find the closest term and iterate until the desired number is hit (very
 *   much like Lucene's own internal term index).
 *
 */
public class UnInvertedField23 {
  private static final Logger logger = Logger.getLogger(UnInvertedField23.class.getName());

  public static int TNUM_OFFSET=2;
  private static int BIG_TERMS_SHRESHOLD = 1000000;

  static class TopTerm {
    Term term;
    int termNum;

    long memSize() {
      return 8 +   // obj header
             8 + 8 +(term.text().length()<<1) +  //term
             4;    // int
    }
  }

  String field;
  int numTermsInField;
  int termsInverted;  // number of unique terms that were un-inverted
  long termInstances; // total number of references to term numbers
  final TermIndex ti;
  long memsz;
  int total_time;  // total time to uninvert the field
  int phase1_time;  // time for phase1 of the uninvert process
  final AtomicLong use = new AtomicLong(); // number of uses

  int[] index;
  byte[][] tnums = new byte[256][];
  int[] maxTermCounts;
  final Map<Integer,TopTerm> bigTerms = new LinkedHashMap<Integer,TopTerm>();


  public long memSize() {
    // can cache the mem size since it shouldn't change
    if (memsz!=0) return memsz;
    long sz = 8*8 + 32; // local fields
    sz += bigTerms.size() * 64;
    for (TopTerm tt : bigTerms.values()) {
      sz += tt.memSize();
    }
    if (index != null) sz += index.length * 4;
    if (tnums!=null) {
      for (byte[] arr : tnums)
        if (arr != null) sz += arr.length;
    }
    if (maxTermCounts != null)
      sz += maxTermCounts.length * 4;
    sz += ti.memSize();
    memsz = sz;
    return sz;
  }


  /** Number of bytes to represent an unsigned int as a vint. */
  static int vIntSize(int x) {
    if ((x & (0xffffffff << (7*1))) == 0 ) {
      return 1;
    }
    if ((x & (0xffffffff << (7*2))) == 0 ) {
      return 2;
    }
    if ((x & (0xffffffff << (7*3))) == 0 ) {
      return 3;
    }
    if ((x & (0xffffffff << (7*4))) == 0 ) {
      return 4;
    }
    return 5;
  }


  // todo: if we know the size of the vInt already, we could do
  // a single switch on the size
  static int writeInt(int x, byte[] arr, int pos) {
    int a;
    a = (x >>> (7*4));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    a = (x >>> (7*3));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    a = (x >>> (7*2));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    a = (x >>> (7*1));
    if (a != 0) {
      arr[pos++] = (byte)(a | 0x80);
    }
    arr[pos++] = (byte)(x & 0x7f);
    return pos;
  }



  public UnInvertedField23(String field, IndexReader reader) throws IOException {
    this.field = field;
    this.ti = new TermIndex(field);
    uninvert(reader);
  }


  private void uninvert(IndexReader reader) throws IOException {
    long startTime = System.currentTimeMillis();

    int maxDoc = reader.maxDoc();

    int[] index = new int[maxDoc];       // immediate term numbers, or the index into the byte[] representing the last number
    this.index = index;
    final int[] lastTerm = new int[maxDoc];    // last term we saw for this document
    final byte[][] bytes = new byte[maxDoc][]; // list of term numbers for the doc (delta encoded vInts)
    maxTermCounts = new int[1024];

    NumberedTermEnum te = ti.getEnumerator(reader);

    // threshold, over which we use set intersections instead of counting
    // to (1) save memory, and (2) speed up faceting.
    // Add 2 for testing purposes so that there will always be some terms under
    // the threshold even when the index is very small.
    int threshold = maxDoc / 20 + 2;

    // ע��: ����һ�д�����Ϊ�˱�����bigTerms�����term����ǰ�汾��UnInvertedField��֧�ֶ�bigTerms�Ĵ���
    // ��Solr��ֲUnInvertedField�Ĺ�����ȥ����SolrIndexSearcher����searcher�ܸ��������ļ���bigTerms��ÿ��term��docCount��
    threshold = Math.max(maxDoc + 1, BIG_TERMS_SHRESHOLD);

    // threshold = 2000000000; //////////////////////////////// USE FOR TESTING
    int[] docs = new int[1000];
    int[] freqs = new int[1000];

    // we need a minimum of 9 bytes, but round up to 12 since the space would
    // be wasted with most allocators anyway.
    byte[] tempArr = new byte[12];

    //
    // enumerate all terms, and build an intermediate form of the un-inverted field.
    //
    // During this intermediate form, every document has a (potential) byte[]
    // and the int[maxDoc()] array either contains the termNumber list directly
    // or the *end* offset of the termNumber list in it's byte array (for faster
    // appending and faster creation of the final form).
    //
    // idea... if things are too large while building, we could do a range of docs
    // at a time (but it would be a fair amount slower to build)
    // could also do ranges in parallel to take advantage of multiple CPUs

    // OPTIONAL: remap the largest df terms to the lowest 128 (single byte)
    // values.  This requires going over the field first to find the most
    // frequent terms ahead of time.

    for (;;) {
      Term t = te.term();
      if (t==null) break;

      int termNum = te.getTermNumber();

      if (termNum >= maxTermCounts.length) {
        // resize by doubling - for very large number of unique terms, expanding
        // by 4K and resultant GC will dominate uninvert times.  Resize at end if material
        int[] newMaxTermCounts = new int[maxTermCounts.length*2];
        System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, termNum);
        maxTermCounts = newMaxTermCounts;
      }

      int df = te.docFreq();
      if (df >= threshold) {
        TopTerm topTerm = new TopTerm();
        topTerm.term = t;
        topTerm.termNum = termNum;
        bigTerms.put(topTerm.termNum, topTerm);

        /// DocSet set = searcher.getDocSet(new TermQuery(topTerm.term));
        /// maxTermCounts[termNum] = set.size();
        maxTermCounts[termNum] = reader.docFreq(topTerm.term);

        te.next();
        continue;
      }

      termsInverted++;

      TermDocs td = te.getTermDocs();
      td.seek(te);
      for(;;) {
        int n = td.read(docs,freqs);
        if (n <= 0) break;

        maxTermCounts[termNum] += n;

        for (int i=0; i<n; i++) {
          termInstances++;
          int doc = docs[i];
          // add 2 to the term number to make room for special reserved values:
          // 0 (end term) and 1 (index into byte array follows)
          int delta = termNum - lastTerm[doc] + TNUM_OFFSET;
          lastTerm[doc] = termNum;
          int val = index[doc];

          if ((val & 0xff)==1) {
            // index into byte array (actually the end of
            // the doc-specific byte[] when building)
            int pos = val >>> 8;
            int ilen = vIntSize(delta);
            byte[] arr = bytes[doc];
            int newend = pos+ilen;
            if (newend > arr.length) {
              // We avoid a doubling strategy to lower memory usage.
              // this faceting method isn't for docs with many terms.
              // In hotspot, objects have 2 words of overhead, then fields, rounded up to a 64-bit boundary.
              // TODO: figure out what array lengths we can round up to w/o actually using more memory
              // (how much space does a byte[] take up?  Is data preceded by a 32 bit length only?
              // It should be safe to round up to the nearest 32 bits in any case.
              int newLen = (newend + 3) & 0xfffffffc;  // 4 byte alignment
              byte[] newarr = new byte[newLen];
              System.arraycopy(arr, 0, newarr, 0, pos);
              arr = newarr;
              bytes[doc] = newarr;
            }
            pos = writeInt(delta, arr, pos);
            index[doc] = (pos<<8) | 1;  // update pointer to end index in byte[]
          } else {
            // OK, this int has data in it... find the end (a zero starting byte - not
            // part of another number, hence not following a byte with the high bit set).
            int ipos;
            if (val==0) {
              ipos=0;
            } else if ((val & 0x0000ff80)==0) {
              ipos=1;
            } else if ((val & 0x00ff8000)==0) {
              ipos=2;
            } else if ((val & 0xff800000)==0) {
              ipos=3;
            } else {
              ipos=4;
            }

            int endPos = writeInt(delta, tempArr, ipos);
            if (endPos <= 4) {
              // value will fit in the integer... move bytes back
              for (int j=ipos; j<endPos; j++) {
                val |= (tempArr[j] & 0xff) << (j<<3);
              }
              index[doc] = val;
            } else {
              // value won't fit... move integer into byte[]
              for (int j=0; j<ipos; j++) {
                tempArr[j] = (byte)val;
                val >>>=8;
              }
              // point at the end index in the byte[]
              index[doc] = (endPos<<8) | 1;
              bytes[doc] = tempArr;
              tempArr = new byte[12];
            }

          }

        }

      }

      te.next();
    }

    numTermsInField = te.getTermNumber();
    te.close();

    // free space if outrageously wasteful (tradeoff memory/cpu) 

    if ((maxTermCounts.length - numTermsInField) > 1024) { // too much waste!
      int[] newMaxTermCounts = new int[numTermsInField];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, numTermsInField);
      maxTermCounts = newMaxTermCounts;
   }

    long midPoint = System.currentTimeMillis();

    if (termInstances == 0) {
      // we didn't invert anything
      // lower memory consumption.
      index = this.index = null;
      tnums = null;
    } else {

      //
      // transform intermediate form into the final form, building a single byte[]
      // at a time, and releasing the intermediate byte[]s as we go to avoid
      // increasing the memory footprint.
      //
      for (int pass = 0; pass<256; pass++) {
        byte[] target = tnums[pass];
        int pos=0;  // end in target;
        if (target != null) {
          pos = target.length;
        } else {
          target = new byte[4096];
        }

        // loop over documents, 0x00ppxxxx, 0x01ppxxxx, 0x02ppxxxx
        // where pp is the pass (which array we are building), and xx is all values.
        // each pass shares the same byte[] for termNumber lists.
        for (int docbase = pass<<16; docbase<maxDoc; docbase+=(1<<24)) {
          int lim = Math.min(docbase + (1<<16), maxDoc);
          for (int doc=docbase; doc<lim; doc++) {
            int val = index[doc];
            if ((val&0xff) == 1) {
              int len = val >>> 8;
              index[doc] = (pos<<8)|1; // change index to point to start of array
              if ((pos & 0xff000000) != 0) {
                // we only have 24 bits for the array index
                throw new IOException("Too many values for UnInvertedField faceting on field "+field);
              }
              byte[] arr = bytes[doc];
              bytes[doc] = null;        // IMPORTANT: allow GC to avoid OOM
              if (target.length <= pos + len) {
                int newlen = target.length;
                /*** we don't have to worry about the array getting too large
                 * since the "pos" param will overflow first (only 24 bits available)
                if ((newlen<<1) <= 0) {
                  // overflow...
                  newlen = Integer.MAX_VALUE;
                  if (newlen <= pos + len) {
                    throw new SolrException(400,"Too many terms to uninvert field!");
                  }
                } else {
                  while (newlen <= pos + len) newlen<<=1;  // doubling strategy
                }
                ****/
                while (newlen <= pos + len) newlen<<=1;  // doubling strategy                 
                byte[] newtarget = new byte[newlen];
                System.arraycopy(target, 0, newtarget, 0, pos);
                target = newtarget;
              }
              System.arraycopy(arr, 0, target, pos, len);
              pos += len + 1;  // skip single byte at end and leave it 0 for terminator
            }
          }
        }

        // shrink array
        if (pos < target.length) {
          byte[] newtarget = new byte[pos];
          System.arraycopy(target, 0, newtarget, 0, pos);
          target = newtarget;
          if (target.length > (1<<24)*.9) {
            logger.warn("Approaching too many values for UnInvertedField faceting on field '"+field+"' : bucket size=" + target.length);
          }
        }
        
        tnums[pass] = target;

        if ((pass << 16) > maxDoc)
          break;
      }
    }

    long endTime = System.currentTimeMillis();

    total_time = (int)(endTime-startTime);
    phase1_time = (int)(midPoint-startTime);

    logger.info("UnInverted multi-valued field " + toString());
  }

  public static class FacetTerms {
    int[] counts;
  }

  public FacetTerms createFacetTerms() {
    FacetTerms fterms = new FacetTerms();
    fterms.counts = new int[numTermsInField];
    return fterms;
  }

  public void accumulateTermCount(int doc, FacetTerms fterms) {
	if(null == index){
	  return;
	}
	int code = index[doc];
    if ((code & 0xff)==1) {
      int pos = code>>>8;
      int whichArray = (doc >>> 16) & 0xff;
      byte[] arr = tnums[whichArray];
      int tnum = 0;
      for(;;) {
        int delta = 0;
        for(;;) {
          byte b = arr[pos++];
          delta = (delta << 7) | (b & 0x7f);
          if ((b & 0x80) == 0) break;
        }
        if (delta == 0) break;
        tnum += delta - TNUM_OFFSET;
        fterms.counts[tnum]++;
      }
    } else {
      int tnum = 0;
      int delta = 0;
      for (;;) {
        delta = (delta << 7) | (code & 0x7f);
        if ((code & 0x80)==0) {
          if (delta==0) break;
          tnum += delta - TNUM_OFFSET;
          fterms.counts[tnum]++;
          delta = 0;
        }
        code >>>= 8;
      }
    }
  }


  public Iterator<Entry<String, Integer>> iteratorFacetTerms(final FacetTerms fterms, IndexReader reader, final int minCount) throws IOException {
    final NumberedTermEnum te = ti.getEnumerator(reader);

    Iterator<Entry<String, Integer>> iter = new Iterator<Entry<String, Integer>>() {
      int idx = 0;

      public boolean hasNext() {
        for (; idx < fterms.counts.length && fterms.counts[idx] < minCount; idx++);
        if (idx == fterms.counts.length) {
          try {
            te.close();
          } catch (IOException ioe) {
            // ignore it
          }
          return false;
        }
        return true;
      }

      public Entry<String, Integer> next() {
        final int pos = idx++;
        Entry<String, Integer> nv = new Entry<String, Integer>() {
            public String getKey() {
              try {
                return getTermText(te, pos);
              } catch (IOException ioe) {
                return null;
              }
            }

            public Integer getValue() {
              return fterms.counts[pos];
            }

            public Integer setValue(Integer value) {
              throw new UnsupportedOperationException();
            }
        };
        return nv;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
    return iter;
  }



  String getTermText(NumberedTermEnum te, int termNum) throws IOException {
    if (bigTerms.size() > 0) {
      // see if the term is one of our big terms.
      TopTerm tt = bigTerms.get(termNum);
      if (tt != null) {
        return tt.term.text();
      }
    }

    te.skipTo(termNum);
    return te.term().text();
  }

  public String toString() {
    return "{field=" + field
            + ",memSize="+memSize()
            + ",tindexSize="+ti.memSize()
            + ",time="+total_time
            + ",phase1="+phase1_time
            + ",nTerms="+numTermsInField
            + ",bigTerms="+bigTerms.size()
            + ",termInstances="+termInstances
            + ",uses="+use.get()
            + "}";
  }
}


// How to share TermDocs (int[] score[])???
// Hot to share TermPositions?
/***
class TermEnumListener {
  void doTerm(Term t) {
  }
  void done() {
  }
}
***/


class NumberedTermEnum extends TermEnum {
  protected final IndexReader reader;
  protected final TermIndex tindex;
  protected TermEnum tenum;
  protected int pos=-1;
  protected Term t;
  protected TermDocs termDocs;


  NumberedTermEnum(IndexReader reader, TermIndex tindex) throws IOException {
    this.reader = reader;
    this.tindex = tindex;
  }


  NumberedTermEnum(IndexReader reader, TermIndex tindex, String termValue, int pos) throws IOException {
    this.reader = reader;
    this.tindex = tindex;
    this.pos = pos;
    tenum = reader.terms(tindex.createTerm(termValue));
    setTerm();
  }

  public TermDocs getTermDocs() throws IOException {
    if (termDocs==null) termDocs = reader.termDocs(t);
    else termDocs.seek(t);
    return termDocs;
  }

  protected boolean setTerm() {
    t = tenum.term();
    if (t==null
            || t.field() != tindex.fterm.field()  // intern'd compare
            //|| (tindex.prefix != null && !t.text().startsWith(tindex.prefix,0)) )
            )
    {
      t = null;
      return false;
    }
    return true;
  }


  public boolean next() throws IOException {
    pos++;
    boolean b = tenum.next();
    if (!b) {
      t = null;
      return false;
    }
    return setTerm();  // this is extra work if we know we are in bounds...
  }

  public Term term() {
    return t;
  }

  public int docFreq() {
    return tenum.docFreq();
  }

  public void close() throws IOException {
    if (tenum!=null) tenum.close();
  }

  public boolean skipTo(String target) throws IOException {
    return skipTo(tindex.fterm.createTerm(target));
  }

  public boolean skipTo(Term target) throws IOException {
    // already here
    if (t != null && t.equals(target)) return true;

    int startIdx = Arrays.binarySearch(tindex.index,target.text());

    if (startIdx >= 0) {
      // we hit the term exactly... lucky us!
      if (tenum != null) tenum.close();
      tenum = reader.terms(target);
      pos = startIdx << TermIndex.intervalBits;
      return setTerm();
    }

    // we didn't hit the term exactly
    startIdx=-startIdx-1;

    if (startIdx == 0) {
      // our target occurs *before* the first term
      if (tenum != null) tenum.close();
      tenum = reader.terms(target);
      pos = 0;
      return setTerm();
    }

    // back up to the start of the block
    startIdx--;

    if ((pos >> TermIndex.intervalBits) == startIdx && t != null && t.text().compareTo(target.text())<=0) {
      // we are already in the right block and the current term is before the term we want,
      // so we don't need to seek.
    } else {
      // seek to the right block
      if (tenum != null) tenum.close();
      tenum = reader.terms(target.createTerm(tindex.index[startIdx]));
      pos = startIdx << TermIndex.intervalBits;
      setTerm();  // should be true since it's in the index
    }


    while (t != null && t.text().compareTo(target.text()) < 0) {
      next();
    }

    return t != null;
  }


  public boolean skipTo(int termNumber) throws IOException {
    int delta = termNumber - pos;
    if (delta < 0 || delta > TermIndex.interval || tenum==null) {
      int idx = termNumber >>> TermIndex.intervalBits;
      String base = tindex.index[idx];
      pos = idx << TermIndex.intervalBits;
      delta = termNumber - pos;
      if (tenum != null) tenum.close();
      tenum = reader.terms(tindex.createTerm(base));
    }
    while (--delta >= 0) {
      boolean b = tenum.next();
      if (b==false) {
        t = null;
        return false;
      }
      ++pos;
    }
    return setTerm();
  }

  /** The current term number, starting at 0.
   * Only valid if the previous call to next() or skipTo() returned true.
   */
  public int getTermNumber() {
    return pos;
  }
}


/**
 * Class to save memory by only storing every nth term (for random access), while
 * numbering the terms, allowing them to be retrieved later by number.
 * This is only valid when used with the IndexReader it was created with.
 * The IndexReader is not actually stored to facilitate caching by using it as a key in
 * a weak hash map.
 */
class TermIndex {
  final static int intervalBits = 7;  // decrease to a low number like 2 for testing
  final static int intervalMask = 0xffffffff >>> (32-intervalBits);
  final static int interval = 1 << intervalBits;

  final Term fterm; // prototype to be used in term construction w/o String.intern overhead
  String[] index;
  int nTerms;
  long sizeOfStrings;

  TermIndex(String field) {
    this(field, null);
  }

  TermIndex(String field, String prefix) {
    this.fterm = new Term(field, "");
  }

  Term createTerm(String termVal) {
    return fterm.createTerm(termVal);
  }

  NumberedTermEnum getEnumerator(IndexReader reader, int termNumber) throws IOException {
    NumberedTermEnum te = new NumberedTermEnum(reader, this);
    te.skipTo(termNumber);
    return te;
  }

  /* The first time an enumerator is requested, it should be used
     with next() to fully traverse all of the terms so the index
     will be built.
   */
  NumberedTermEnum getEnumerator(IndexReader reader) throws IOException {
    if (index==null) return new NumberedTermEnum(reader,this, "", 0) {
      ArrayList<String> lst;

      protected boolean setTerm() {
        boolean b = super.setTerm();
        if (b && (pos & intervalMask)==0) {
          String text = term().text();
          sizeOfStrings += text.length() << 1;
          if (lst==null) {
            lst = new ArrayList<String>();
          }
          lst.add(text);
        }
        return b;
      }

      public boolean skipTo(Term target) throws IOException {
        throw new UnsupportedOperationException();
      }

      public boolean skipTo(int termNumber) throws IOException {
        throw new UnsupportedOperationException();
      }

      public void close() throws IOException {
        nTerms=pos;
        super.close();
        index = lst!=null ? lst.toArray(new String[lst.size()]) : new String[0];
      }
    };
    else return new NumberedTermEnum(reader,this,"",0);
  }


  /**
   * Returns the approximate amount of memory taken by this TermIndex.
   * This is only an approximation and doesn't take into account java object overhead.
   *
   * @return
   * the approximate memory consumption in bytes
   */
  public long memSize() {
    // assume 8 byte references?
    return 8+8+8+8+(index.length<<3)+sizeOfStrings;
  }
}

