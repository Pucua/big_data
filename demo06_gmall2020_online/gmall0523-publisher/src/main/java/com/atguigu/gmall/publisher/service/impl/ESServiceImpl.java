package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.apache.lucene.queryparser.xml.builders.MatchAllDocsQueryBuilder;
import org.apache.lucene.search.TermAutomatonQuery;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author : Pucua
 * @date : 2023-10-11 20:24
 * @Desc :发布查询接口(日活总数)
 **/

@Service  // 将当前对象的创建交给Spring容器进行管理
public class ESServiceImpl implements ESService {

    // 将ES的客户端操作对象注入到Service中
    @Autowired
    JestClient jestClient;

    /*
       GET /gmall0523_dau_info_2020-10-24-query/_search
       {
         "query": {
           "match_all": {}
         }
       }
        */
    @Override
    public Long getDauTotal(String date) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new MatchAllQueryBuilder());
        String query = sourceBuilder.toString();
        String indexName = "gmall0523_dau_info_"+date+"-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();

        Long total = 0L;
        try {  // execute 编译+检查异常
            SearchResult result = jestClient.execute(search);
            total = result.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败");
        }
        return total;
    }

    /*
    GET /gmall0523_dau_info_2020-10-24-query/_search
    {
      "aggs": {
        "groupBy_hr": {
          "terms": {
            "field": "hr",
            "size": 24
          }
        }
      }
    }

     */
    @Override
    public Map<String, Long> getDauHour(String date) {
        Map<String, Long> hourMap = new HashMap<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("groupBy_hr", ValueType.LONG).field("hr").size(24);
//        AggregationBuilders.terms("groupBy_hr").field("hr").size(24)

        sourceBuilder.aggregation(termsAggregationBuilder);

        String query = sourceBuilder.toString();
        String indexName = "gmall0523_dau_info_"+date+"-query";
        Search search = new Search.Builder(query)
                .addIndex(indexName)
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            TermsAggregation termAgg = result.getAggregations().getTermsAggregation("groupBy_hr");
            if(termAgg != null){
                List<TermsAggregation.Entry> buckets = termAgg.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put(bucket.getKey(),bucket.getCount());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询异常");
        }
        return hourMap;
    }
}
