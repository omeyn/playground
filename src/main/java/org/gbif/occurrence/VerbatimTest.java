package org.gbif.occurrence;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.codehaus.jackson.map.ObjectMapper;

public class VerbatimTest {

  public static void main(String[] args) throws IOException {
    verbatimReadTest();
  }

  private static void serdeTest() throws IOException {
    UUID datasetKey = UUID.randomUUID();
    UUID orgKey = UUID.randomUUID();
    Date crawlDate = new Date();

    VerbatimOccurrence verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(datasetKey);
    verb.setPublishingOrgKey(orgKey);
    verb.setPublishingCountry(Country.DENMARK);
    verb.setProtocol(EndpointType.DWC_ARCHIVE);
    verb.setLastCrawled(crawlDate);

    for (DwcTerm term : DwcTerm.values()) {
      verb.setField(term, "I am " + term);
    }

    ObjectMapper mapper = new ObjectMapper();
    String json = mapper.writeValueAsString(verb);
    System.out.println(json);

    VerbatimOccurrence deser = mapper.readValue(json, VerbatimOccurrence.class);
  }

  private static void verbatimReadTest() throws IOException {
    Configuration config = HBaseConfiguration.create();
    HTablePool tablePool = new HTablePool(config, 1);
    OccurrencePersistenceService occService = new OccurrencePersistenceServiceImpl("appdev_occurrence", tablePool);
    VerbatimOccurrence verb = occService.getVerbatim(2164);
    System.out.println("Got verb " + verb);
    for (Map.Entry entry : verb.getFields().entrySet()) {
      System.out.println("Got field [" + entry.getKey() + "]=[" + entry.getValue() + ']');
    }
    System.out.println("fields has bor? " + verb.hasField(DwcTerm.basisOfRecord));
    System.out.println("bor is " + verb.getField(DwcTerm.basisOfRecord));
  }
}
