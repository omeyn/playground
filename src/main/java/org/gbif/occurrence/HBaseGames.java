package org.gbif.occurrence;

import java.io.IOException;

public class HBaseGames {

  public static void main(String[] args) throws IOException {
    HBaseGames instance = new HBaseGames();
//    instance.writeOccRow("oliver_test");
//    instance.testHex();
    String triplet = "4fa7b334-ce0d-4e88-aaae-2e0c138d049e|CLO|EBIRD_AK|OBS100167516|null";
    System.out.println(triplet.split("\\|")[3]);
  }

  private void testHex() {
    Integer key = 215464618;
    System.out.println(key + " as hex is " + hbaseHex(key));
  }

  private String hbaseHex(int key) {
    String rawHex = Integer.toHexString(key);
    //    \x00\x00\x00\x0A
    System.out.println("raw hex is " + rawHex);
    String hex = "";
    int lengthCount = 0;
    for (int i = rawHex.length()-1; i > -1; i--) {
      hex = rawHex.substring(i, i+1).toUpperCase() + hex;
      lengthCount++;
      if (lengthCount > 0 && lengthCount % 2 == 0) {
        hex = "\\x" + hex;
      }
    }

    if (lengthCount < 8) {
      for (int i=lengthCount; i < 8; i++) {
        hex = "0" + hex;
        lengthCount++;
        if (lengthCount > 0 && lengthCount % 2 == 0) {
          hex = "\\x" + hex;
        }
      }
    }

    return hex;
  }

//  private void writeOccRow(String tableName) throws IOException {
//    Configuration config = HBaseConfiguration.create();
//    HTablePool pool = new HTablePool(config, 1);
//
//    HTableInterface occTable = pool.getTable(Bytes.toBytes(tableName));
//
//    Put put = new Put(Bytes.toBytes(1));
//    put.add(Bytes.toBytes("o"), Bytes.toBytes(Columns.column(GbifInternalTerm.crawlId)), Bytes.toBytes(1));
//    occTable.put(put);
//
//    Occurrence occ = new Occurrence();
//    occ.setKey(1);
//    occ.setDatasetKey(UUID.randomUUID());
//    Date now = new Date();
//    occ.setLastInterpreted(now);
//    occ.setLastCrawled(now);
//    occ.setLastParsed(now);
//    for (Term term : DwcTerm.values()) {
//      occ.setVerbatimField(term, "I am " + term.simpleName());
//    }
//    for (Term term : DcTerm.values()) {
//      occ.setVerbatimField(term, "I am " + term.simpleName());
//    }
//    for (Term term : GbifTerm.values()) {
//      occ.setVerbatimField(term, "I am " + term.simpleName());
//    }
//    for (Term term : IucnTerm.values()) {
//      occ.setVerbatimField(term, "I am " + term.simpleName());
//    }
//
//    OccurrencePersistenceService occService = new OccurrencePersistenceServiceImpl(tableName, pool);
//    occService.update(occ);
//
//    Scan scan = new Scan();
//    ResultScanner results = occTable.getScanner(scan);
//    for (Result result : results) {
//      System.out.println("Got row key [" + Bytes.toInt(result.getRow()) + "] with [" + result.raw().length + "] kvs");
//    }
//
//  }

}
