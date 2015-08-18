package com.beeva.reportingcrm.flume.sink.hbase;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.apache.log4j.Logger;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

/**
 * A serializer for the AsyncHBaseSink, which splits the event body into
 * multiple columns and inserts them into a row whose key is available in the
 * headers
 */
public class SplittingSerializer implements AsyncHbaseEventSerializer {
	private byte[] table;
	private byte[] colFam;
	private Event currentEvent;
	private byte[][] columnNames;
	private final List<PutRequest> puts = new ArrayList<PutRequest>();
	private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
	private byte[] currentRowKey;
	private final byte[] eventCountCol = "eventCount".getBytes();
	protected static Logger log = Logger.getLogger(SplittingSerializer.class.getName());

	@Override
	public void configure(Context context) {
		log.info("ENTRANDO EN CONFIGURE 2");
		String cols = new String(context.getString("columns"));
		log.info((new StringBuilder()).append("COLS2 :: ").append(cols).toString());
		String names[] = cols.split(",");
		byte columnNames[][] = new byte[names.length][];
		int i = 0;
		String arr$[] = names;
		int len$ = arr$.length;
		for (int i$ = 0; i$ < len$; i$++) {
			String name = arr$[i$];
			columnNames[i++] = name.getBytes();
		}
	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// TODO Auto-generated method stub
		log.info("ENTRANDO EN CONFIGURE 1");
	}

	@Override
	public void initialize(byte[] table, byte[] cf) {
		log.info("ENTRANDO EN INITIALIZE");
		this.table = table;
		this.colFam = cf;
	}

	@Override
	public void setEvent(Event event) {
		log.info("ENTRANDO EN SET EVENT");
		currentEvent = event;
		String rowKeyStr = (String) currentEvent.getHeaders().get("rowKey");
		if (rowKeyStr == null) {
			//rowKeyStr = (new StringBuilder()).append("").append((new Date()).getTime()).toString();
			String eventStr = new String(currentEvent.getBody());
			log.info((new StringBuilder()).append("EVENTSTR :: ").append(eventStr).toString());
			if (null != eventStr && !"".equals(eventStr)){
				String uuid = eventStr.split(",")[0];
				rowKeyStr = (new StringBuilder()).append("").append(uuid).toString();
			}else{
				rowKeyStr = (new StringBuilder()).append("").append((new Date()).getTime()).toString();
			}
			//
			System.out.println((new StringBuilder()).append("ROWkeystr :: ").append(rowKeyStr).toString());
		}
		currentRowKey = rowKeyStr.getBytes();
		System.out.println((new StringBuilder()).append("currentRowKey :: ").append(currentRowKey).toString());
	}

	@Override
	public List<PutRequest> getActions() {
		log.info("ENTRANDO EN GET ACTIONS");
		String columns[] = new String[8];
		columns[0] = "des_domcopro";
		columns[1] = "subscriberKey";
		columns[2] = "email";
		columns[3] = "xti_sexo";
		columns[4] = "cod_idioma";
		columns[5] = "des_nompila";
		columns[6] = "event_definition_key";
		columns[7] = "contact_key";
		String cols1 = "des_domcopro,subscriberKey,email,xti_sexo,cod_idioma,des_nompila,event_definition_key,contact_key";
		String names[] = cols1.split(",");
		byte columnNames[][] = new byte[8][];
		int ii = 0;
		String arr$[] = names;
		int len$ = arr$.length;
		for (int i$ = 0; i$ < len$; i$++) {
			String name = arr$[i$];
			columnNames[ii++] = name.getBytes();
		}

		String eventStr = new String(currentEvent.getBody());
		log.info((new StringBuilder()).append("EVENTSTR :: ").append(eventStr).toString());
		String cols[] = eventStr.split(",");
		log.info((new StringBuilder()).append("COLS :: ").append(cols.toString()).toString());
		puts.clear();
		log.info((new StringBuilder()).append("table :: ").append(table.toString()).toString());
		log.info((new StringBuilder()).append("currentRowKey :: ").append(currentRowKey).toString());
		log.info((new StringBuilder()).append("colFam :: ").append(colFam.toString()).toString());
		for (int i = 0; i < cols.length; i++) {
			log.info((new StringBuilder()).append("columnNames[i] :: ").append(columnNames[i].toString()).toString());
			PutRequest req = new PutRequest(table, currentRowKey, colFam, columnNames[i], cols[i].getBytes());
			puts.add(req);
		}

		return puts;
	}

	@Override
	public List<AtomicIncrementRequest> getIncrements() {
		log.info("ENTRANDO EN GET INCREMENTS");
		incs.clear();
		incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), colFam, eventCountCol));
		return incs;
	}

	@Override
	public void cleanUp() {
		log.info("ENTRANDO EN CLEAN UP");
		table = null;
		colFam = null;
		currentEvent = null;
		columnNames = (byte[][]) null;
		currentRowKey = null;
	}

}