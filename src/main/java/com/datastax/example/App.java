package com.datastax.example;

/**
 * Hello world!
 *
 */
    import au.com.bytecode.opencsv.CSVReader;
    import com.datastax.driver.core.*;
    import com.datastax.driver.core.exceptions.InvalidQueryException;
    import com.datastax.driver.core.querybuilder.QueryBuilder;

    import java.io.FileNotFoundException;
    import java.io.FileReader;
    import java.io.IOException;
    import java.text.ParseException;
    import java.text.SimpleDateFormat;
    import java.util.Calendar;
    import java.util.Date;

public class App {

    private String path="";
    public App(String path){
        this.path=path;

    }
    private Cluster cluster;

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

    private int toInt(String text){
      return  Integer.parseInt(text.trim());
    }
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");


    private Date toDate(String text) throws ParseException {
      return  formatter.parse(text);
    }
    private Date tmDate(String text) throws ParseException {
        Date date = formatter.parse("2012/11/11");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, Integer.parseInt(text));
        return calendar.getTime();
    }

    private void addData(Session session) throws IOException, ParseException {

        CSVReader reader = new CSVReader(new FileReader(this.path));
        for( String[] arr: reader.readAll()) {
        try {
            /*
            session.execute("INSERT INTO flight.flights" +
                    "(ID, " +
                    "YEAR,DAY_OF_MONTH" +
                    ",FL_DATE," +
                            "AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID, ORIGIN, ORIGIN_CITY_NAME, " +
                            "ORIGIN_STATE_ABR, DEST, DEST_CITY_NAME, DEST_STATE_ABR, DEP_TIME, " +
                            "ARR_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME, DISTANCE) VALUES" +
                            " (?,?,?,?,?" +
                            ",?,?,?,?,?" +
                            ",?,?,?,?,?" +
                            ",?,?,?,?)",
                    toInt(arr[0]), toInt(arr[1]), toInt(arr[2]), toDate(arr[3]), toInt(arr[4]),
                    arr[5], toInt(arr[6]), toInt(arr[7]), arr[8], arr[9],
                    arr[10], arr[11], arr[12], arr[13], tmDate(arr[14]),
                    tmDate(arr[15]), tmDate(arr[16]), tmDate(arr[17]), arr[18]
            );
            */
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


    public void close() {
        cluster.close();
    }

    public static void main(String[] args) throws IOException, ParseException {
        App client = new App(args[0]);
        client.connect("127.0.0.1");
        client.close();
    }

}
