package com.hannesstockner.connect.es;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchSinkTask extends SinkTask {

 // private static final org.slf4j.Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);

  private String indexPrefix;
  private final String TYPE = "kafka";

  //private Client client;
  public TransportClient client;
  //Should handle resuming from a previous offset
  {
      Logger.getLogger("org.elasticsearch").setLevel(Level.DEBUG);
  }
  @Override
  public void start(Map<String, String> props) {
    final String esHost = props.get(ElasticsearchSinkConnector.ES_HOST);
    indexPrefix = props.get(ElasticsearchSinkConnector.INDEX_PREFIX);
    try {
      Settings settings = Settings.builder()
              .put("cluster.name", "cluster").put("client.transport.sniff", false).put("node.name","ct-62").build();
        System.out.println(settings.get("cluster.name"));
      client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("ct-62"), 9300));

   /*    client = TransportClient
       .builder()
        .build()
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(esHost), 9300));
*/
   IndicesAdminClient indicesAdminClient = client.admin().indices();
        indicesAdminClient.prepareCreate("twitter").get();
  /*    client
        .admin()
        .indices()
        .preparePutTemplate("kafka_to_es")
        .setTemplate(indexPrefix + "*")
        .addMapping(TYPE, new HashMap<String, Object>() {{
          put("date_detection", false);
          put("numeric_detection", false);
        }})
        .get();*/
    } catch (UnknownHostException ex) {
      throw new ConnectException("Couldn't connect to es host", ex);
    }
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord record : records) {
    //  log.info("Processing {}", record.value());

      //log.info(record.value().getClass().toString());

      client
        .prepareIndex(indexPrefix + record.topic(), TYPE)
        .setSource(record.value().toString())
        .get();
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
  }

  //should be synchronized. modified by lee
  @Override
  public synchronized void stop() {
    client.close();
  }

  @Override
  public String version() {
    return new ElasticsearchSinkConnector().version();
  }
}
