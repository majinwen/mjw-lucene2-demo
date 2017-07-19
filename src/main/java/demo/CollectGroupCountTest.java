package demo;

/**
 * Created by majinwen on 2017/7/19.
 */
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import org.apache.lucene.search.facets.FacetField;
import org.apache.lucene.search.facets.MultiValuedStringFacetField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;



public class CollectGroupCountTest {


    public static void main(String[] args) {
        Directory dir = new RAMDirectory();

        Document doc = new Document();
        doc.add(new Field("id", "binbin", Store.YES, Index.TOKENIZED));
        doc.add(new Field("string", "haha", Store.YES, Index.UN_TOKENIZED));
        doc.add(new Field("time", "20100801", Store.YES, Index.UN_TOKENIZED));
        doc.add(new Field("duplicate", "123456", Store.YES, Index.UN_TOKENIZED));

        Document doc1 = new Document();
        doc1.add(new Field("id", "yaoyao", Store.YES, Index.UN_TOKENIZED));
        doc1.add(new Field("string", "haha", Store.YES, Index.UN_TOKENIZED));
        doc1.add(new Field("time", "20100801", Store.YES, Index.UN_TOKENIZED));
        doc1.add(new Field("duplicate", "123456", Store.YES,Index.UN_TOKENIZED));

        Document doc11 = new Document();
        doc11.add(new Field("id", "liufeng", Store.YES, Index.UN_TOKENIZED));
        doc11.add(new Field("string", "haha", Store.YES, Index.UN_TOKENIZED));
        doc11.add(new Field("time", "20100801", Store.YES, Index.UN_TOKENIZED));
        doc11.add(new Field("duplicate", "123456", Store.YES,Index.UN_TOKENIZED));


        Document doc2 = new Document();
        doc2.add(new Field("id", "zhangjian", Store.YES, Index.UN_TOKENIZED));
        doc2.add(new Field("string", "haha", Store.YES, Index.UN_TOKENIZED));
        doc2.add(new Field("time", "20100801", Store.YES, Index.UN_TOKENIZED));
        doc2.add(new Field("duplicate", "123455", Store.YES,Index.UN_TOKENIZED));



        Document doc3 = new Document();
        doc3.add(new Field("id", "liweicheng", Store.YES, Index.UN_TOKENIZED));
        doc3.add(new Field("string", "haha", Store.YES, Index.UN_TOKENIZED));
        doc3.add(new Field("time", "20100801", Store.YES, Index.UN_TOKENIZED));
        doc3.add(new Field("duplicate", "123451", Store.YES,Index.UN_TOKENIZED));



        try {
            IndexWriter indexWriter = new IndexWriter(dir, new StandardAnalyzer(), true);
            indexWriter.addDocument(doc);
            indexWriter.addDocument(doc1);
            indexWriter.addDocument(doc11);
            indexWriter.addDocument(doc2);
            indexWriter.addDocument(doc3);
            indexWriter.close();


                  Sort ss = null;
         // 设置Collector：1)对指定数据项进行统计; 2)调整Doc的score值
            WLPTopFieldDocCollector collector = null;

            IndexReader reader = IndexReader.open(dir);

            System.out.println(reader.maxDoc());
            IndexSearcher indexSearcher = new IndexSearcher(reader);



            collector = new WLPTopFieldDocCollector(reader, Sort.INDEXORDER, 200);

            collector.addFacetField(new MultiValuedStringFacetField(reader, "duplicate"));



            Query query = new TermQuery(new Term("time", "20100801"));



            indexSearcher.search(query, collector);;


            Map<String, List<FacetField.FacetEntry>> res =  collector.getFacetValueMap();

            for (Map.Entry<String, List<FacetField.FacetEntry>>  entry : res.entrySet()) {

               System.out.println(entry.getKey());
               List<FacetField.FacetEntry> a = entry.getValue();
                int len = a.size();
                for(int i=0;i < len; i++){
                    FacetField.FacetEntry  b = a.get(i);
                    System.out.println(b.getKey() + "----" + b.getValue());
                }

            }


        } catch (CorruptIndexException e) {
            e.printStackTrace();
        } catch (LockObtainFailedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
