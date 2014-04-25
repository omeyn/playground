package org.gbif.occurrence;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveFactory;
import org.gbif.dwc.text.StarRecord;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.utils.file.ClosableIterator;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EBirdUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(EBirdUpdater.class);

  private static final int THREAD_COUNT = 100;
  private static final String EBIRD_DATASET_KEY = "4fa7b334-ce0d-4e88-aaae-2e0c138d049e";
  private static final String DELIMITER = "|";

  public static void main(String[] args) {
    if (args.length != 3) {
      LOG.error("Usage: EBirdUpdater <lookup table name> <ebird table name> <full path to ebird dwca>");
      System.exit(1);
    }

    String lookupTableName = args[0];
    String ebirdTableName = args[1];
    File dwcaFile = new File(args[2]);

    Configuration config = HBaseConfiguration.create();
    HTablePool pool = new HTablePool(config, THREAD_COUNT);
    ExecutorService tp =
      new ThreadPoolExecutor(THREAD_COUNT, THREAD_COUNT, 1, TimeUnit.HOURS, new SynchronousQueue<Runnable>(),
        new ThreadPoolExecutor.CallerRunsPolicy());

    Archive archive = null;
    try {
      archive = ArchiveFactory.openArchive(dwcaFile);
    } catch (IOException e) {
      LOG.error("Couldn't open archive", e);
      System.exit(1);
    }
    ClosableIterator<StarRecord> iterator = archive.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      count++;
      StarRecord record = iterator.next();
      String catNum = record.core().value(DwcTerm.catalogNumber);
      String occId = record.core().value(DwcTerm.occurrenceID);
//      tp.submit(new LookupUpdater(pool, lookupTableName, ebirdTableName, catNum, occId));
      tp.submit(new LookupUpdater(pool, lookupTableName, ebirdTableName, catNum, catNum));

      if (count % 100000 == 0) {
        LOG.info("Processed [{}] records", count);
      }
    }
  }

  private static class LookupUpdater implements Runnable {

    private final HTablePool pool;
    private final String lookupTableName;
    private final String ebirdTableName;
    private final String catalogNumber;
    private final String occId;

    private LookupUpdater(HTablePool pool, String lookupTableName, String ebirdTableName, String catalogNumber,
      String occId) {
      this.pool = pool;
      this.lookupTableName = lookupTableName;
      this.ebirdTableName = ebirdTableName;
      this.catalogNumber = catalogNumber;
      this.occId = occId;
    }

    @Override
    public void run() {
      HTableInterface lookupTable = pool.getTable(Bytes.toBytes(lookupTableName));
      HTableInterface ebirdTable = pool.getTable(Bytes.toBytes(ebirdTableName));
      try {
        Get get = new Get(Bytes.toBytes(catalogNumber));
        Result result = ebirdTable.get(get);
        if (result != null) {
          byte[] rawId = result.getValue(Columns.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN));
          if (rawId != null) {
            // we found a match for this cat # - now write new occId entry
            String newKey = EBIRD_DATASET_KEY + DELIMITER + occId;
            LOG.info("would write new row key [{}] value [{}]", newKey, Bytes.toInt(rawId));
//            Put put = new Put(Bytes.toBytes(newKey));
//            put.add(Columns.CF, Bytes.toBytes(Columns.LOOKUP_KEY_COLUMN), rawId);
//            lookupTable.put(put);
          }
        }
      } catch (IOException e) {
        LOG.warn("Failed on get", e);
      } finally {
        try {
          lookupTable.close();
          ebirdTable.close();
        } catch (IOException e) {
          LOG.warn("Couldn't return table to pool - possible memory leak", e);
        }
      }
    }
  }
}
