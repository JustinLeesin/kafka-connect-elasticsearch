package com.justin.connect.es;

import com.hannesstockner.connect.es.ElasticsearchSinkTask;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class TestSinkTask {
    ElasticsearchSinkTask entityElasticsearchSinkTask;
    @Before
    public void testBeforeClass() {
         entityElasticsearchSinkTask = new ElasticsearchSinkTask();
    }

    @Test
    public void testStart(){
        entityElasticsearchSinkTask.start(new HashMap<String, String>());
        System.out.println(entityElasticsearchSinkTask.client.listedNodes());
    }

}
