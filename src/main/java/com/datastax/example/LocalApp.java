package com.datastax.example;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by angela on 4/30/16.
 */
public class LocalApp {
    public static void main(String[] args) throws IOException, ParseException {
        App client = new App("/home/angela/Downloads/flights/1.csv");
        client.connect("104.196.110.202");
        client.close();
    }

}
