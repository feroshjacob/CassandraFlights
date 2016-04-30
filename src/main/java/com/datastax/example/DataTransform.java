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
public class DataTransform extends BaseCassandraInsert {




    private SimpleDateFormat oPFormat = new SimpleDateFormat( "yyyy-MM-dd H:m:sz");

    public DataTransform(String path) {
        super(path);
    }

    private String toCDate(Date date){

       return  oPFormat.format(date);
    }
    public  void transform(String path) throws IOException, ParseException {
        FileWriter writer = new FileWriter("results/output");
        CSVReader reader = new CSVReader(new FileReader(path));
        for( String[] arr: reader.readAll()) {

            StringBuffer buffer= new StringBuffer();

            buffer.append(toInt(arr[0])+",");
             buffer.append(toInt(arr[1])+",");
             buffer.append(toInt(arr[2])+",");
             buffer.append(toCDate(toDate(arr[3]))+",");
             buffer.append(toInt(arr[4])+",");
             buffer.append(arr[5]+",");
             buffer.append(toInt(arr[6])+",");
             buffer.append(toInt(arr[7])+",");
             buffer.append(arr[8]+",");
             buffer.append(arr[9]+",");
             buffer.append(arr[10]+",");
             buffer.append(arr[11]+",");
             buffer.append(arr[12]+",");
             buffer.append(arr[13]+",");
             buffer.append(toCDate(tmDate(arr[14]))+",");
             buffer.append(toCDate(tmDate(arr[15]))+",");
             buffer.append(toCDate(tmDate(arr[16]))+",");
             buffer.append(toCDate(tmDate(arr[17]))+",");
             buffer.append(toInt(arr[18])+"\n");
            writer.append(buffer.toString());
        }
        writer.close();

    }
    public static void main(String[] args) throws IOException, ParseException {
        DataTransform dataTransform = new DataTransform("");
         dataTransform.transform("/home/angela/Downloads/flights/flights_from_pg.csv");
    }

    @Override
    protected void addData(Session connect) throws IOException, ParseException {

    }
}
