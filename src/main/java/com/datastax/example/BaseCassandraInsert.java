package com.datastax.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by angela on 4/30/16.
 */
public abstract class BaseCassandraInsert {
    protected String path="";
    public BaseCassandraInsert(String path){
        this.path=path;

    }
       public void connect(String node) throws IOException, ParseException {
        cluster = Cluster.builder()
                .addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }

        addData(cluster.connect());
    }
        public void close() {
        cluster.close();
    }

    protected abstract void addData(Session connect) throws IOException, ParseException;

    protected Cluster cluster;
    int toInt(String text){
        return  Integer.parseInt(text.trim());
    }
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");


    Date toDate(String text) throws ParseException {
        return  formatter.parse(text);
    }
    Date tmDate(String text) throws ParseException {
        Date date = formatter.parse("2012/11/11");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, Integer.parseInt(text));
        return calendar.getTime();
    }
}
