package com.datastax.example;

import au.com.bytecode.opencsv.CSVReader;
import com.datastax.driver.core.Session;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by angela on 4/30/16.
 */
public class DataTransformForBucket extends BaseCassandraInsert {





    public DataTransformForBucket(String path) {
        super(path);
    }

    public  void transform(String path) throws IOException, ParseException {
        FileWriter writer = new FileWriter("results/output1");
        CSVReader reader = new CSVReader(new FileReader(path));
        for( String[] arr: reader.readAll()) {

            StringBuffer buffer= new StringBuffer();

            buffer.append(toInt(arr[0])+",");
             buffer.append(arr[5]+",");
             buffer.append(arr[8]+",");
             buffer.append(arr[11]+",");
             buffer.append(toInt(arr[17])/10+"\n");
            writer.append(buffer.toString());
        }
        writer.close();

    }
    public static void main(String[] args) throws IOException, ParseException {
        DataTransformForBucket dataTransform = new DataTransformForBucket("");
         dataTransform.transform("/home/angela/Downloads/flights/flights_from_pg.csv");
    }

    protected void addData(Session connect) throws IOException, ParseException {

    }
}
