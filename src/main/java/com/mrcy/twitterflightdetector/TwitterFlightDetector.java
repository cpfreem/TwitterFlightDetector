package com.mrcy.twitterflightdetector;

import be.datablend.blueprints.impls.mongodb.MongoDBGraph;
import com.mrcy.flightinformationproducer.datascraper.AirportInfo;
import com.mrcy.flightinformationproducer.datascraper.FlightScheduleData;
import com.mrcy.flightinformationproducer.datascraper.FlightScheduleScraper;
import com.mrcy.flightinformationproducer.datascraper.ModesData;
import com.mrcy.flightinformationproducer.datascraper.ModesDataScraper;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONException;

/**
 * Based on Geolocation information and time stamp from Twitter predict the
 * possible flights a person took
 *
 */
public class TwitterFlightDetector {

    public static Graph graph = new MongoDBGraph("localhost", 27017);
    public static FlightScheduleScraper flightScheduleScraper = new FlightScheduleScraper();

    public TwitterFlightDetector() throws IOException, JSONException {
        //this.writeFlightSchedulesToMongo();
    }

    /*
     * This function will write the Flight Schedules for all aircraft found
     * by ModesDataScraper to Mongo
     */
    private void writeFlightSchedulesToMongo() throws IOException, JSONException {
        ModesDataScraper modesDataScraper = new ModesDataScraper();
        List<ModesData> modesData = modesDataScraper.getModesData();
        for (int i = 0; i < modesData.size(); i++) {
            String flightNumber = modesData.get(i).getFlightNumber();
            storeFlightInfo(flightNumber);
        }

        List<AirportInfo> airportsUsed = flightScheduleScraper.getAirportsInUse();
        storeAirportInfo(airportsUsed);

    }

    private void storeFlightInfo(String flightNumber) {
        List<FlightScheduleData> scheduleData;

        GraphQuery query = graph.query();
        query.has("OBJECT_TYPE", "FLIGHT_SCHEDULE");
        query.has("FLIGHT_NUMBER", flightNumber);

        Iterable<Vertex> vertices = query.vertices();

        // If flight schedule does not exist in database then store it
        if (!vertices.iterator().hasNext()) {
            System.out.println("Did not find schedule for: " + flightNumber + " requesting from flight aware");
            scheduleData = flightScheduleScraper.getFlightSchedules(flightNumber);
            Vertex v = graph.addVertex(null);
            v.setProperty("OBJECT_TYPE", "FLIGHT_SCHEDULE");
            v.setProperty("FLIGHT_NUMBER", flightNumber);
            List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
            for (int i = 0; i < scheduleData.size(); i++) {
                mapList.add(scheduleData.get(i).toMap());
                System.out.println(scheduleData.get(i).toString());
            }
            v.setProperty("FLIGHT_SCHEDULE_DATA", mapList);
        } else {
            System.out.println("Already stored flight schedule");
        }
    }

    private void storeAirportInfo(List<AirportInfo> airportsUsed) {
        for (int i = 0; i < airportsUsed.size(); i++) {
            String airportName = airportsUsed.get(i).getName();
            GraphQuery query = graph.query();
            query.has("OBJECT_TYPE", "AIRPORT");
            query.has("AIRPORT_NAME", airportName);

            Iterable<Vertex> vertices = query.vertices();

            // If airport does not exist in database then store it
            if (!vertices.iterator().hasNext()) {
                System.out.println("Did not find airport: " + airportName);
                Vertex v = graph.addVertex(null);
                v.setProperty("OBJECT_TYPE", "AIRPORT");
                v.setProperty("AIRPORT_NAME", airportName);
                List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
                mapList.add(airportsUsed.get(i).toMap());
                v.setProperty("AIRPORT_DATA", mapList);
                System.out.println(airportsUsed.get(i).toString());
            } else {
                System.out.println("Already stored Airport information");
            }
        }
    }

    /*
     * find airports
     * find flights between airports
     * narrow flights based on time
     * 
     * This would be better with elastic search and using a radius
     */
    public void getPossibleFlights(Map geoInfo1, Map geoInfo2) {
        System.out.println("You made it to getPossibleFlights");
        System.out.println(geoInfo1.toString());
        System.out.println(geoInfo2.toString());

        GraphQuery query = graph.query();
        query.has("OBJECT_TYPE", "AIRPORT");
        
        Map<String, Object> from = new HashMap();
        Map<String, Object> to = new HashMap();

        int count = 0;
        Iterable<Vertex> vertices = query.vertices();
        for (Vertex v : vertices) {
            List<Map<String, Object>> data = v.getProperty("AIRPORT_DATA");
            for (int i = 0; i < data.size(); i++) {
                Double lat = new Double(data.get(i).get("LATITUDE").toString());
                Double lon = new Double(data.get(i).get("LONGITUDE").toString());
                String name = data.get(i).get("NAME").toString();

                if (distance(lat, lon, new Double(geoInfo1.get("latitude").toString()), new Double(geoInfo1.get("longitude").toString()), "M") <= 5) {
                    System.out.println("This is a possible FROM airport");
                    from = data.get(i);
                    System.out.println("Name = " + data.get(i).get("NAME").toString()
                            + ", Lat = " + data.get(i).get("LATITUDE").toString()
                            + ", Lon = " + data.get(i).get("LONGITUDE").toString());
                }
                
                if (distance(lat, lon, new Double(geoInfo2.get("latitude").toString()), new Double(geoInfo2.get("longitude").toString()), "M") <= 5) {
                    System.out.println("This is a possible TO airport");
                    to = data.get(i);
                    System.out.println("Name = " + data.get(i).get("NAME").toString()
                            + ", Lat = " + data.get(i).get("LATITUDE").toString()
                            + ", Lon = " + data.get(i).get("LONGITUDE").toString());
                }
                
            }
            count++;
        }
        System.out.println("There are " + count + " airports.");
        
        GraphQuery flightSchedule = graph.query();
        flightSchedule.has("OBJECT_TYPE", "FLIGHT_SCHEDULE");
        
        count = 0;
        Iterable<Vertex> vertices2 = flightSchedule.vertices();
        for(Vertex flights : vertices2) {
            List<Map<String, Object>> data = flights.getProperty("FLIGHT_SCHEDULE_DATA");
            for(int i = 0; i < data.size(); i++) {
                String origin = data.get(i).get("ORIGIN").toString();
                String origin_lookup = "";
                if(origin.contains("(")) {
                    origin_lookup = origin.substring(origin.indexOf("(")+1, origin.indexOf(")"));
                    origin_lookup = origin_lookup.substring(1, origin_lookup.length());
                }

                String dest = data.get(i).get("DESTINATION").toString();
                String dest_lookup = "";
                if(dest.contains("(")) {
                    dest_lookup = dest.substring(dest.indexOf("(")+1, dest.indexOf(")"));
                    dest_lookup = dest_lookup.substring(1, dest_lookup.length());
                }
                
                Long depart_timestamp = new Long(data.get(i).get("DEPARTURE_TIME").toString());
                Long geo1_timestamp = new Long(geoInfo1.get("timestamp").toString());
                
                Long arrive_timestamp = new Long(data.get(i).get("ARRIVAL_TIME").toString());
                Long geo2_timestamp = new Long(geoInfo2.get("timestamp").toString());
                
                String from_name = from.get("NAME").toString();
                String to_name = to.get("NAME").toString();
                if(from_name.equals(origin_lookup) && 
                   to_name.equals(dest_lookup) &&
                   (depart_timestamp-geo1_timestamp) <= 1800000 &&
                   (geo2_timestamp-arrive_timestamp) <= 600000) {
                    System.out.println(data.get(i).toString());
                }
            }
            count++;
        }
        System.out.println("There are " + count + "flight numbers");
    }

    private static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;
        if (unit.equals("K")) {
            dist = dist * 1.609344;
        } else if (unit.equals("N")) {
            dist = dist * 0.8684;
        }
        return (dist);
    }

    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
        return (rad * 180 / Math.PI);
    }

    public static void main(String[] args) throws IOException, JSONException {
        System.out.println("TwitterFlightDetector!");

        Map geo1 = new HashMap();
        Map geo2 = new HashMap();

        geo1.put("latitude", 39.8617);
        geo1.put("longitude", -104.6731);
        geo1.put("timestamp", 1379090780000L);

        geo2.put("latitude", 42.2125);
        geo2.put("longitude", -83.3533);
        geo2.put("timestamp", 1379100400000L);

        System.out.println("distance = " + distance(39.85029262, -104.67378616, 46.91980019, -96.82563128, "M"));
        TwitterFlightDetector tfd = new TwitterFlightDetector();
        tfd.getPossibleFlights(geo1, geo2);

//        System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, "M") + " Miles\n");
//        System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, "K") + " Kilometers\n");
//        System.out.println(distance(32.9697, -96.80322, 29.46786, -98.53506, "N") + " Nautical Miles\n");
    }
}
