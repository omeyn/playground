package org.gbif.occurrence;

import java.io.IOException;

public class VerbatimTest {

  public static void main(String[] args) throws IOException, InterruptedException {
//    verbatimReadTest();
  }

//  private static void serdeTest() throws IOException {
//    UUID datasetKey = UUID.randomUUID();
//    UUID orgKey = UUID.randomUUID();
//    Date crawlDate = new Date();
//
//    VerbatimOccurrence verb = new VerbatimOccurrence();
//    verb.setKey(1);
//    verb.setDatasetKey(datasetKey);
//    verb.setPublishingOrgKey(orgKey);
//    verb.setPublishingCountry(Country.DENMARK);
//    verb.setProtocol(EndpointType.DWC_ARCHIVE);
//    verb.setLastCrawled(crawlDate);
//
//    for (DwcTerm term : DwcTerm.values()) {
//      verb.setVerbatimField(term, "I am " + term);
//    }
//
//    ObjectMapper mapper = new ObjectMapper();
//    String json = mapper.writeValueAsString(verb);
//    System.out.println(json);
//
//    VerbatimOccurrence deser = mapper.readValue(json, VerbatimOccurrence.class);
//  }
//
//  private static void verbatimReadTest() throws IOException, InterruptedException {
//    int poolSize = 20;
//    Configuration config = HBaseConfiguration.create();
//    HTablePool tablePool = new HTablePool(config, poolSize);
//    ExecutorService tp = Executors.newFixedThreadPool(poolSize);
//    OccurrencePersistenceService occService = new OccurrencePersistenceServiceImpl("appdev_occurrence", tablePool);
//    Thread.sleep(2000);
//    for (int i = 0; i < poolSize; i++) {
//      tp.submit(new VerbReader(occService, i));
////      Thread.sleep(1000);
//    }
//    tp.shutdown();
//    tp.awaitTermination(1, TimeUnit.MINUTES);
//
//
//    //    VerbatimOccurrence verb = occService.getVerbatim(2164);
//    //    System.out.println("Got verb " + verb);
//    //    for (Map.Entry entry : verb.getFields().entrySet()) {
//    //      System.out.println("Got field [" + entry.getKey() + "]=[" + entry.getValue() + ']');
//    //    }
//    //    System.out.println("fields has bor? " + verb.hasField(DwcTerm.basisOfRecord));
//    //    System.out.println("bor is " + verb.getField(DwcTerm.basisOfRecord));
//  }
//
//  private static class VerbReader implements Runnable {
//
//    private final OccurrenceService occService;
//    private final int threadId;
//
//    private VerbReader(OccurrenceService occService, int threadId) {
//      this.occService = occService;
//      this.threadId = threadId;
//    }
//
//    @Override
//    public void run() {
//      VerbatimOccurrence verb = occService.getVerbatim(2164);
//      //      System.out.println("Got verb " + verb);
//      //      for (Map.Entry entry : verb.getFields().entrySet()) {
//      //        System.out.println("Got field [" + entry.getKey() + "]=[" + entry.getValue() + ']');
//      //      }
//      System.out.println(
//        "Thread [" + threadId + "] term count [" + verb.getVerbatimFields().size() + "] fields has bor? " + verb
//          .hasVerbatimField(DwcTerm.basisOfRecord));
//      //      System.out.println("Thread [" + threadId + "] bor is " + verb.getField(DwcTerm.basisOfRecord));
//    }
//  }
}
