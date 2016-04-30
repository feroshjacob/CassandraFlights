package com.datastax.example;

import au.com.bytecode.opencsv.CSVReader;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

/**
 * Created by angela on 4/30/16.
 */
public class TimeBucketApp extends BaseCassandraInsert {

    public TimeBucketApp(String path) {
        super(path);
    }

    @Override
    protected void addData(Session session) throws IOException, ParseException {


            CSVReader reader = new CSVReader(new FileReader(this.path));
            for( String[] arr: reader.readAll()) {
                try {
                    Statement statement = QueryBuilder.insertInto("flight", "FlightsByAirTime")
                            .value("ID", toInt(arr[0]))
                            .value("CARRIER",arr[5])
                            .value("ORIGIN",arr[8])
                            .value("DEST",arr[11])
                            .value("AIR_TIME",toInt(arr[17])/10);
                    //  System.out.println(statement.toString());
                    session.execute(statement);

                }catch (InvalidQueryException ex){
                    // ex.printStackTrace();
                    System.out.println("");
                    for(String element :arr){
                        System.out.print(element+"-");
                    }
                    System.out.println("");
                }

            }

        }




    public static void main(String[] args) throws IOException, ParseException {
        App client = new App(args[0]);
        client.connect("127.0.0.1");
        client.close();

    }
}
