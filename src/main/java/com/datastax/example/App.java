package com.datastax.example;

/**
 * Hello world!
 *
 */
    import au.com.bytecode.opencsv.CSVReader;
    import com.datastax.driver.core.*;
    import com.datastax.driver.core.exceptions.InvalidQueryException;
    import com.datastax.driver.core.querybuilder.QueryBuilder;

    import java.io.FileReader;
    import java.io.IOException;
    import java.text.ParseException;

public class App extends BaseCassandraInsert {


    public App(String path) {
        super(path);
    }


    protected void addData(Session session) throws IOException, ParseException {

        CSVReader reader = new CSVReader(new FileReader(this.path));
        for( String[] arr: reader.readAll()) {
        try {
            Statement statement = QueryBuilder.insertInto("flight", "FlightsByOriginAirport")
                    .value("ID", toInt(arr[0]))
                    .value("YEAR",toInt(arr[1]))
                    .value("DAY_OF_MONTH",toInt(arr[2]))
                    .value("FL_DATE",toDate(arr[3]))
                    .value("AIRLINE_ID",      toInt(arr[4]))
                    .value("CARRIER",arr[5])
                    .value("FL_NUM",toInt(arr[6]))
                    .value("ORIGIN_AIRPORT_ID",toInt(arr[7]))
                    .value("ORIGIN",arr[8])
                    .value("ORIGIN_CITY_NAME",arr[9])
                    .value("ORIGIN_STATE_ABR",arr[10])
                    .value("DEST",arr[11])
                    .value("DEST_CITY_NAME",arr[12])
                    .value("DEST_STATE_ABR",arr[13])
                    .value("DEP_TIME",tmDate(arr[14]))
                    .value("ARR_TIME",tmDate(arr[15]))
                    .value("ACTUAL_ELAPSED_TIME",tmDate(arr[16]))
                    .value("AIR_TIME",tmDate(arr[17]))
                    .value("DISTANCE",toInt(arr[18]));
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
