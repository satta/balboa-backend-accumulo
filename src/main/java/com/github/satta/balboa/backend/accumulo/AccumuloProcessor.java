package com.github.satta.balboa.backend.accumulo;

import com.github.satta.balboa.backend.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.lexicoder.SequenceLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AccumuloProcessor implements InputProcessor {
    private final AccumuloClient client;
    private final SequenceLexicoder<String> lc;
    private final MultiTableBatchWriter w;
    private final BatchWriter n, d, r;
    private final LongCombiner.VarLenEncoder vle;

    public AccumuloProcessor(AccumuloClient client) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        this.client = client;
        AtomicReference<BatchWriterConfig> config = new AtomicReference<>(new BatchWriterConfig()
                .setMaxMemory(10000000)
                .setMaxLatency(5, TimeUnit.SECONDS)
                .setMaxWriteThreads(5));
        this.lc = new SequenceLexicoder<>(new StringLexicoder());
        this.vle = new LongCombiner.VarLenEncoder();
        w = client.createMultiTableBatchWriter(config.get());
        n = w.getBatchWriter("balboa_by_rrname");
        d = w.getBatchWriter("balboa_by_rdata");
        r = w.getBatchWriter("balboa_by_rrname_rev");
    }

    private Mutation mutationFill(byte[] row, Observation o) {
        Mutation m = new Mutation(row);
        m.at()
                .family("count")
                .qualifier("count")
                .visibility("public")
                .put(vle.encode((long) o.getCount()));
        m.at()
                .family("seen")
                .qualifier("first")
                .visibility("public")
                .put(vle.encode(Long.valueOf(o.getFirst_seen_ts())));
        m.at()
                .family("seen")
                .qualifier("last")
                .visibility("public")
                .put(vle.encode(Long.valueOf(o.getLast_seen_ts())));
        return m;
    }

    public void close() {
        try {
            this.w.close();
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(Observation o) throws BalboaException {
        LinkedList<String> rowList = new LinkedList<>();
        rowList.add(o.getRrname());
        rowList.add(o.getSensorid());
        rowList.add(o.getRrtype());
        rowList.add(o.getRdata());
        Mutation m = mutationFill(lc.encode(rowList), o);
        try {
            n.addMutation(m);
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
        rowList.clear();
        rowList.add(o.getRdata());
        rowList.add(o.getSensorid());
        rowList.add(o.getRrtype());
        rowList.add(o.getRrname());
        m = mutationFill(lc.encode(rowList), o);
        try {
            d.addMutation(m);
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
        rowList.clear();
        rowList.add(new StringBuilder(o.getRrname()).reverse().toString());
        rowList.add(o.getSensorid());
        rowList.add(o.getRrtype());
        rowList.add(o.getRdata());
        m = mutationFill(lc.encode(rowList), o);
        try {
            r.addMutation(m);
        } catch (MutationsRejectedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(DumpRequest d) throws BalboaException {
        throw new BalboaException("not implemented yet");
    }

    @Override
    public void handle(BackupRequest b) throws BalboaException {
        throw new BalboaException("not implemented yet");
    }

    private void handleRrnameQuery(Query q, ObservationStreamConsumer osc, Authorizations auths) {
        // default: exact search by rrname
        Range rng = Range.prefix((q.getRrname() + '\0'));
        String rrname = q.getRrname();
        String tableName = "balboa_by_rrname";
        if (rrname.contains("%")) {
            // wildcard search, suffix-anchored
            if (rrname.startsWith("%")) {
                rrname = StringUtils.substring(rrname, 1, rrname.length());
                rng = Range.prefix(new StringBuilder(rrname).reverse().toString());
                tableName = "balboa_by_rrname_rev";
            } else if (rrname.endsWith("%")) {
                // wildcard search, prefix-anchored
                rng = Range.prefix(StringUtils.substring(rrname, 0, rrname.length() - 1));
            } else {
                throw new BalboaException("wildcards are only supported at either the beginning or end of query");
            }
        }
        int i = 0;
        try (Scanner scan = client.createScanner(tableName, auths)) {
            scan.setRange(rng);
            // TODO: use custom iterators to make sure these two match in the correct locations
            // TODO: (e.g. via a RowFilter iterator)
            if (q.getSensorid() != null && !q.getSensorid().equals("")) {
                IteratorSetting sidSetting = new IteratorSetting(5, GrepIterator.class);
                GrepIterator.setTerm(sidSetting, '\0' + q.getSensorid() + '\0');
                scan.addScanIterator(sidSetting);
            }
            if (q.getRdata() != null && !q.getRdata().equals("")) {
                IteratorSetting sidSetting = new IteratorSetting(6, GrepIterator.class);
                GrepIterator.setTerm(sidSetting, '\0' + q.getRdata() + '\0');
                scan.addScanIterator(sidSetting);
            }
            RowIterator rowIterator = new RowIterator(scan);
            while (rowIterator.hasNext()) {
                if (i >= q.getLimit())
                    return;
                Iterator<Map.Entry<Key,Value>> row = rowIterator.next();
                Observation o = new Observation();
                Map.Entry<Key, Value> lkv = null;
                while (row.hasNext()) {
                    Map.Entry<Key, Value> kv = row.next();
                    Text thisCol = kv.getKey().getColumnQualifier();
                    switch (thisCol.toString()) {
                        case "count":
                            o.setCount(vle.decode(kv.getValue().get()).intValue());
                            break;
                        case "last":
                            o.setLast_seen(vle.decode(kv.getValue().get()).intValue());
                            break;
                        case "first":
                            o.setFirst_seen(vle.decode(kv.getValue().get()).intValue());
                            break;
                    }
                    lkv = kv;
                }
                assert lkv != null;
                byte[] thisRow = lkv.getKey().getRowData().getBackingArray();
                List<String> rowValues = lc.decode(thisRow);
                o.setRdata(rowValues.get(3));
                if (tableName.equals("balboa_by_rrname_rev")) {
                    o.setRrname(new StringBuilder(rowValues.get(0)).reverse().toString());
                } else {
                    o.setRrname(rowValues.get(0));
                }
                o.setRrtype(rowValues.get(2));
                o.setSensorid(rowValues.get(1));
                osc.submit(o);
                i++;
            }
        } catch (TableNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    private void handleRdataQuery(Query q, ObservationStreamConsumer osc, Authorizations auths) {
        Range rng;
        if (q.getSensorid() != null && !q.getSensorid().equals("")) {
            rng = Range.prefix((q.getRdata() + '\0' + q.getSensorid() + '\0'));
        } else {
            rng = Range.prefix((q.getRdata() + '\0'));
        }
        String tableName = "balboa_by_rdata";
        int i = 0;
        try (Scanner scan = client.createScanner(tableName, auths)) {
            scan.setRange(rng);
            if (q.getSensorid() != null && !q.getSensorid().equals("")) {
                IteratorSetting sidSetting = new IteratorSetting(5, GrepIterator.class);
                GrepIterator.setTerm(sidSetting, '\0' + q.getSensorid() + '\0');
                scan.addScanIterator(sidSetting);
            }
            RowIterator rowIterator = new RowIterator(scan);
            while (rowIterator.hasNext()) {
                if (i >= q.getLimit())
                    return;
                Iterator<Map.Entry<Key,Value>> row = rowIterator.next();
                Observation o = new Observation();
                Map.Entry<Key, Value> lkv = null;
                while (row.hasNext()) {
                    Map.Entry<Key, Value> kv = row.next();
                    Text thisCol = kv.getKey().getColumnQualifier();
                    switch (thisCol.toString()) {
                        case "count":
                            o.setCount(vle.decode(kv.getValue().get()).intValue());
                            break;
                        case "last":
                            o.setLast_seen(vle.decode(kv.getValue().get()).intValue());
                            break;
                        case "first":
                            o.setFirst_seen(vle.decode(kv.getValue().get()).intValue());
                            break;
                    }
                    lkv = kv;
                }
                assert lkv != null;
                byte[] thisRow = lkv.getKey().getRowData().getBackingArray();
                List<String> rowValues = lc.decode(thisRow);
                o.setRdata(rowValues.get(0));
                o.setRrtype(rowValues.get(2));
                o.setRrname(rowValues.get(3));
                o.setSensorid(rowValues.get(1));
                osc.submit(o);
                i++;
            }
        } catch (TableNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    public void handle(Query q, ObservationStreamConsumer osc) throws BalboaException {
        Authorizations auths = new Authorizations("public");
        if (q.getRrname() != null && !q.getRrname().equals("")) {
                handleRrnameQuery(q, osc, auths);
        } else if (q.getRdata() != null && !q.getRdata().equals("")) {
            handleRdataQuery(q, osc, auths);
        } else {
            throw new BalboaException("neither Rdata nor RRname given");
        }
    }
}
