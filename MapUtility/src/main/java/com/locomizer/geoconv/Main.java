package com.locomizer.geoconv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.locationtech.jts.geom.*;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


@SuppressWarnings({"ConstantConditions", "Duplicates"})
public class Main {
    public static final String JSON = "json";
    public static final String KML = "kml";
    public static final String H_3 = "h3(";

    public static final String INDEX = "index";
    public static final String NAME = "name";
    public static final String ADDRESS = "address";
    public static final String ID = "id";
    public static final String DESCRIPTION = "description";
    public static final String PHONE_NUMBER = "phoneNumber";

    public static final String UNDERSCORE = "_";

    public static final Character COMMA = ',';
    public static final String COMMA_STR = COMMA.toString();

    private static final GeometryFactory FACTORY = new GeometryFactory();

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            printHelpAndExit();
        }

        String in = null, out = null;
        int[] resolutions = {-1, -1};
        try {
            in = args[0].toLowerCase();
            if (!in.equals(JSON) && !in.equals(KML) && !in.startsWith(H_3)) {
                printHelpAndExit();
            }

            out = args[1].toLowerCase();
            if (!out.equals(JSON) && !out.equals(KML) && !out.startsWith(H_3)) {
                printHelpAndExit();
            }
        } catch (Exception e) {
            printHelpAndExit();
        }

        if (in.equals(out) || (in.startsWith(H_3) && out.startsWith(H_3))) {

            printHelpAndExit();
        }

        List<String> columns = null;

        if (in.startsWith(H_3)) {
            columns = Arrays.stream(args[0].substring(3, args[0].length() - 1).split(COMMA_STR))
                    .map(String::trim)
                    .collect(Collectors.toList());

            if (columns.isEmpty() || !columns.contains(INDEX)) {
                printHelpAndExit();
            }
        }

        if (out.startsWith(H_3)) {
            columns = Arrays.stream(args[1].substring(3, args[1].length() - 1).split(COMMA_STR))
                    .map(String::trim)
                    .collect(Collectors.toList());

            if (columns.isEmpty() || !columns.contains(INDEX)) {
                printHelpAndExit();
            }

            try {
                Pattern resPattern = Pattern.compile("(\\d+)");

                Matcher m = resPattern.matcher(columns.get(0));
                if (m.find()) {
                    resolutions[0] = Integer.parseInt(m.group(1));
                    if (m.find()) {
                        resolutions[1] = Integer.parseInt(m.group(1));
                    }
                } else {
                    printHelpAndExit();
                }
            } catch (Exception e) {
                printHelpAndExit();
            }

            for (int resolution : resolutions) {
                if (resolution > 15) {
                    printHelpAndExit();
                }
            }

            Arrays.sort(resolutions);

            columns.remove(0);
        }

        File inFile = new File(args[2]);
        File outFile = new File(args[3]);
        if (!inFile.isFile() || !inFile.canRead() || (outFile.isFile() && !outFile.canWrite()) || (!outFile.isFile() && outFile.exists())) {
            printHelpAndExit();
        }


        String input = new String(Files.readAllBytes(inFile.toPath()));
        Map<Geometry, Map<String, Object>> geometries = new ConcurrentHashMap<>();


        if (JSON.equals(in)) {
            GeoJSON json = GeoJSONFactory.create(input);
            GeoJSONReader reader = new GeoJSONReader();

            if (json instanceof Feature) {
                Feature feature = (Feature) json;

                feature(geometries, reader, feature);
            } else if (json instanceof FeatureCollection) {
                FeatureCollection collection = (FeatureCollection) json;

                Arrays.stream(collection.getFeatures()).parallel()
                        .forEach(feature -> feature(geometries, reader, feature));
            } else {
                throw new RuntimeException("Input JSON root element was neither Feature nor FeatureCollection");
            }
        }

        if (KML.equals(in)) {
            de.micromata.opengis.kml.v_2_2_0.Kml kml = de.micromata.opengis.kml.v_2_2_0.Kml.unmarshal(input);

            de.micromata.opengis.kml.v_2_2_0.Feature feature = kml.getFeature();

            if (feature instanceof de.micromata.opengis.kml.v_2_2_0.Placemark) {
                de.micromata.opengis.kml.v_2_2_0.Placemark p = (de.micromata.opengis.kml.v_2_2_0.Placemark) feature;

                placemark(geometries, p);
            } else {
                List<de.micromata.opengis.kml.v_2_2_0.Container> containers = new ArrayList<>();
                container(containers, feature);

                containers.parallelStream()
                        .forEach(container -> {
                            if (container instanceof de.micromata.opengis.kml.v_2_2_0.Document) {
                                de.micromata.opengis.kml.v_2_2_0.Document d = (de.micromata.opengis.kml.v_2_2_0.Document) container;

                                for (de.micromata.opengis.kml.v_2_2_0.Feature f : d.getFeature()) {
                                    if (f instanceof de.micromata.opengis.kml.v_2_2_0.Placemark) {
                                        de.micromata.opengis.kml.v_2_2_0.Placemark p = (de.micromata.opengis.kml.v_2_2_0.Placemark) f;

                                        placemark(geometries, p);
                                    }
                                }
                            }

                            if (container instanceof de.micromata.opengis.kml.v_2_2_0.Folder) {
                                de.micromata.opengis.kml.v_2_2_0.Folder fo = (de.micromata.opengis.kml.v_2_2_0.Folder) container;

                                for (de.micromata.opengis.kml.v_2_2_0.Feature f : fo.getFeature()) {
                                    if (f instanceof de.micromata.opengis.kml.v_2_2_0.Placemark) {
                                        de.micromata.opengis.kml.v_2_2_0.Placemark p = (de.micromata.opengis.kml.v_2_2_0.Placemark) f;

                                        placemark(geometries, p);
                                    }
                                }
                                containers.add(fo);
                            }
                        });
            }
        }

        if (in.startsWith(H_3)) {
            H3Core h3core = H3Core.newInstance();

            try (CSVParser parser = new CSVParser(new StringReader(input), CSVFormat.EXCEL.withDelimiter(COMMA))) {
                List<String> _columns = columns;

                StreamSupport.stream(parser.spliterator(), true)
                        .forEach(rec -> {
                            Map<String, Object> props = new HashMap<>();

                            Long hash = null;
                            for (int i = _columns.size(); i > 0; ) {
                                String col = _columns.get(--i);

                                if (!col.equals(UNDERSCORE)) {
                                    String c = rec.get(i);
                                    if (col.equals(INDEX)) {
                                        hash = Long.parseLong(c, 16);
                                    }
                                    props.put(col, c);
                                }
                            }

                            List<GeoCoord> geo = h3core.h3ToGeoBoundary(hash);
                            geo.add(geo.get(0));

                            List<Coordinate> cl = new ArrayList<>();
                            geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                            Polygon polygon = FACTORY.createPolygon(cl.toArray(new Coordinate[0]));

                            geometries.put(polygon, props);
                        });
            }
        }

        if (out.startsWith(H_3)) {
            H3Core h3core = H3Core.newInstance();

            Map<Long, Map<String, Object>> hashes = new ConcurrentHashMap<>();

            int curLev, maxLev;
            if (resolutions[0] < 0) {
                curLev = maxLev = resolutions[1];
            } else {
                curLev = resolutions[0];
                maxLev = resolutions[1];
            }

            Map<Geometry, Map<String, Object>> gmtrs = geometries;

            for (; curLev <= maxLev; curLev++) {
                final int _level = curLev;

                gmtrs = gmtrs.entrySet().parallelStream()
                        .map(res -> {
                            Geometry geometry = res.getKey();
                            Map<String, Object> props = res.getValue();

                            if (geometry instanceof Polygon) {
                                Polygon p = (Polygon) geometry;

                                List<GeoCoord> gco = new ArrayList<>();
                                LinearRing shell = (LinearRing) p.getExteriorRing();
                                for (Coordinate c : shell.getCoordinates()) {
                                    gco.add(new GeoCoord(c.y, c.x));
                                }

                                List<LinearRing> holes = new ArrayList<>();

                                List<List<GeoCoord>> gci = new ArrayList<>();
                                for (int i = p.getNumInteriorRing(); i > 0; ) {
                                    List<GeoCoord> gcii = new ArrayList<>();
                                    LinearRing hole = (LinearRing) p.getInteriorRingN(--i);

                                    for (Coordinate c : hole.getCoordinates()) {
                                        gcii.add(new GeoCoord(c.y, c.x));
                                    }
                                    gci.add(gcii);

                                    if (_level != maxLev) {
                                        holes.add(hole);
                                    }
                                }

                                Set<Long> polyfill = new HashSet<>(h3core.polyfill(gco, gci, _level));
                                for (Long hash : polyfill) {
                                    if (_level == maxLev) {
                                        List<Long> neigh = h3core.kRing(hash, 1);
                                        hashes.put(hash, props);
                                        neigh.forEach(n -> hashes.putIfAbsent(n, props));
                                    } else {
                                        if (polyfill.containsAll(h3core.kRing(hash, 1))) {
                                            if (_level != maxLev) {
                                                List<GeoCoord> geo = h3core.h3ToGeoBoundary(hash);
                                                geo.add(geo.get(0));

                                                List<Coordinate> cl = new ArrayList<>();
                                                geo.forEach(c -> cl.add(new Coordinate(c.lng, c.lat)));

                                                Collections.reverse(cl);
                                                LinearRing hole = FACTORY.createLinearRing(cl.toArray(new Coordinate[0]));
                                                holes.add(hole);
                                            }

                                            hashes.put(hash, props);
                                        }
                                    }
                                }

                                if (_level != maxLev) {
                                    geometry = FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0]));
                                }
                            }

                            if (_level == maxLev) {
                                if (geometry instanceof Point) {
                                    Coordinate c = geometry.getCoordinate();

                                    Long pointfill = h3core.geoToH3(c.y, c.x, _level);
                                    hashes.put(pointfill, props);
                                }
                            }

                            return new AbstractMap.SimpleEntry<>(geometry, props);
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            }

            try (CSVPrinter printer = new CSVPrinter(new BufferedWriter(new FileWriter(outFile), 4096 * 1024), CSVFormat.EXCEL.withDelimiter(COMMA))) {
                for (Map.Entry<Long, Map<String, Object>> h : hashes.entrySet()) {
                    for (String col : columns) {
                        if (INDEX.equals(col)) {
                            printer.print(Long.toHexString(h.getKey()));
                        } else {
                            printer.print(h.getValue().get(col));
                        }
                    }
                    printer.println();
                }
            }
        }

        if (KML.equals(out)) {
            de.micromata.opengis.kml.v_2_2_0.Kml kml = new de.micromata.opengis.kml.v_2_2_0.Kml();
            List<de.micromata.opengis.kml.v_2_2_0.Feature> kmlCollection = kml.createAndSetDocument().getFeature();

            Queue<de.micromata.opengis.kml.v_2_2_0.Placemark> pms = new ConcurrentLinkedQueue<>();
            geometries.entrySet().parallelStream()
                    .forEach(res -> {
                        de.micromata.opengis.kml.v_2_2_0.Placemark pm = new de.micromata.opengis.kml.v_2_2_0.Placemark();
                        pms.add(pm);

                        de.micromata.opengis.kml.v_2_2_0.ExtendedData ed = pm.createAndSetExtendedData();

                        res.getValue().forEach((k, v) -> {
                            switch (k) {
                                case NAME: {
                                    pm.setName(String.valueOf(v));
                                    break;
                                }
                                case ADDRESS: {
                                    pm.setAddress(String.valueOf(v));
                                    break;
                                }
                                case ID: {
                                    pm.setId(String.valueOf(v));
                                    break;
                                }
                                case DESCRIPTION: {
                                    pm.setDescription(String.valueOf(v));
                                    break;
                                }
                                case PHONE_NUMBER: {
                                    pm.setPhoneNumber(String.valueOf(v));
                                    break;
                                }
                                default:
                                    ed.createAndAddData(String.valueOf(v)).setName(k);
                            }
                        });

                        Geometry geo = res.getKey();
                        if (geo instanceof Polygon) {
                            de.micromata.opengis.kml.v_2_2_0.Polygon pg = pm.createAndSetPolygon();

                            List<de.micromata.opengis.kml.v_2_2_0.Coordinate> lc = pg
                                    .createAndSetOuterBoundaryIs()
                                    .createAndSetLinearRing()
                                    .createAndSetCoordinates();

                            for (Coordinate c : ((Polygon) geo).getExteriorRing().getCoordinates()) {
                                lc.add(new de.micromata.opengis.kml.v_2_2_0.Coordinate(c.getX(), c.getY()));
                            }

                            for (int i = ((Polygon) geo).getNumInteriorRing(); i > 0; i--) {
                                List<de.micromata.opengis.kml.v_2_2_0.Coordinate> lci = pg
                                        .createAndAddInnerBoundaryIs()
                                        .createAndSetLinearRing()
                                        .createAndSetCoordinates();

                                for (Coordinate c : ((Polygon) geo).getInteriorRingN(i - 1).getCoordinates()) {
                                    lci.add(new de.micromata.opengis.kml.v_2_2_0.Coordinate(c.getX(), c.getY()));
                                }
                            }
                        }
                        if (geo instanceof Point) {
                            de.micromata.opengis.kml.v_2_2_0.Point pt = pm.createAndSetPoint();

                            List<de.micromata.opengis.kml.v_2_2_0.Coordinate> lc = pt.createAndSetCoordinates();

                            Coordinate c = geo.getCoordinate();
                            lc.add(new de.micromata.opengis.kml.v_2_2_0.Coordinate(c.getX(), c.getY()));
                        }
                    });

            kmlCollection.addAll(pms);

            kml.marshal(outFile);
        }

        if (JSON.equals(out)) {
            GeoJSONWriter writer = new GeoJSONWriter();

            Queue<Feature> fl = new ConcurrentLinkedQueue<>();

            geometries.entrySet().parallelStream()
                    .forEach(e -> fl.add(new Feature(writer.write(e.getKey()), e.getValue())));

            FeatureCollection fc = writer.write(new ArrayList<>(fl));

            ObjectMapper om = new ObjectMapper();

            om.writeValue(outFile, fc);
        }
    }

    private static void printHelpAndExit() {
        System.err.println("Call syntax:\n" +
                "   java -jar locomizer-geoconv.jar input output /path/to/input/file /path/to/output/file\n" +
                "Inputs:\n" +
                "   * json for GeoJSON\n" +
                "   * kml for KML\n" +
                "   * h3(attributes) for a properly quoted and escaped CSV of H3 indexes with attributes\n" +
                "Outputs:\n" +
                "   * json\n" +
                "   * kml\n" +
                "   * h3(resolution,attributes)\n" +
                "General notes:\n" +
                "   * output file will be overwritten without a prompt\n" +
                "   * input and output formats must be different\n" +
                "   * all geometries are extracted from their grouping wrappers such as features or folders\n" +
                "     and flattened to polygons (preserving any holes) and points\n" +
                "GeoJSON notes:\n" +
                "   * supported geometry types are Polygon, Point, MultiPolygon and MultiPoint\n" +
                "KML notes:\n" +
                "   * supported geometry types are Polygon, Point, and LinearRing inside a Placemark\n" +
                "   * supported attributes are name, address, id, description, phoneNumber,\n" +
                "     while all other will be treated as extended data\n" +
                "H3 notes:\n" +
                "   * attributes is a comma-separated list of arbitrary but unique attribute names\n" +
                "   * the only mandatory attribute is index which is treated as a hexadecimal string\n" +
                "   * use _ (a single underscore) to skip a column\n" +
                "   * resolution is an integer in the range of 0 to 15\n" +
                "Example 1:\n" +
                "  Assume we need to cover an GeoJSON map of a country with h3 indices level 6\n" +
                "  and then save resulting coverage as a KML file. This is a two-step process\n" +
                "  1) java -jar locomizer-geoconv.jar json h3(6,index) /mnt/storage/Belarus.json /mnt/storage/Belarus-h3-6.csv\n" +
                "  2) java -jar locomizer-geoconv.jar h3(index) kml /mnt/storage/Belarus-h3-6.csv /mnt/storage/Belarus-h3-6.kml\n" +
                "Example 2:\n" +
                "  Assume we need to extract h3 indices level 10 from an KML file along with names and descriptions\n" +
                "  java -jar locomizer-geoconv.jar kml h3(10,index,name,description) /mnt/storage/Moscow-districts.json /mnt/storage/Moscow-districts-h3-10.csv\n"
        );

        System.exit(1);
    }

    private static void feature(Map<Geometry, Map<String, Object>> result, GeoJSONReader reader, Feature
            feature) {
        Geometry geometry = reader.read(feature.getGeometry());

        Map<String, Object> fp = feature.getProperties();

        if ((geometry instanceof Polygon) || (geometry instanceof Point)) {
            result.put(geometry, fp);
        }

        if (geometry instanceof MultiPolygon) {
            for (int n = geometry.getNumGeometries(); n > 0; ) {
                Polygon p = (Polygon) geometry.getGeometryN(--n);
                result.put(p, fp);
            }
        }

        if (geometry instanceof MultiPoint) {
            for (int n = geometry.getNumGeometries(); n > 0; ) {
                Point p = (Point) geometry.getGeometryN(--n);
                result.put(p, fp);
            }
        }
    }

    private static void container
            (List<de.micromata.opengis.kml.v_2_2_0.Container> containers, de.micromata.opengis.kml.v_2_2_0.Feature
                    feature) {
        if (feature instanceof de.micromata.opengis.kml.v_2_2_0.Document) {
            de.micromata.opengis.kml.v_2_2_0.Document d = (de.micromata.opengis.kml.v_2_2_0.Document) feature;

            for (de.micromata.opengis.kml.v_2_2_0.Feature f : d.getFeature()) {
                if (f instanceof de.micromata.opengis.kml.v_2_2_0.Container) {
                    container(containers, f);
                }
            }
            containers.add(d);
        }

        if (feature instanceof de.micromata.opengis.kml.v_2_2_0.Folder) {
            de.micromata.opengis.kml.v_2_2_0.Folder fo = (de.micromata.opengis.kml.v_2_2_0.Folder) feature;

            for (de.micromata.opengis.kml.v_2_2_0.Feature f : fo.getFeature()) {
                if (f instanceof de.micromata.opengis.kml.v_2_2_0.Container) {
                    container(containers, f);
                }
            }
            containers.add(fo);
        }
    }

    private static void placemark
            (Map<Geometry, Map<String, Object>> result, de.micromata.opengis.kml.v_2_2_0.Placemark pm) {
        Map<String, Object> properties = new HashMap<>();
        de.micromata.opengis.kml.v_2_2_0.ExtendedData ed = pm.getExtendedData();
        if (ed != null) {
            List<de.micromata.opengis.kml.v_2_2_0.Data> d = ed.getData();
            if (d != null) {
                for (de.micromata.opengis.kml.v_2_2_0.Data dd : d) {
                    properties.put(dd.getName(), dd.getValue());
                }
            }
        }
        String v = pm.getName();
        if (v != null) {
            properties.put(NAME, v);
        }
        v = pm.getAddress();
        if (v != null) {
            properties.put(ADDRESS, v);
        }
        v = pm.getId();
        if (v != null) {
            properties.put(ID, v);
        }
        v = pm.getDescription();
        if (v != null) {
            properties.put(DESCRIPTION, v);
        }
        v = pm.getPhoneNumber();
        if (v != null) {
            properties.put(PHONE_NUMBER, v);
        }

        de.micromata.opengis.kml.v_2_2_0.Geometry g = pm.getGeometry();
        if (g != null) {
            if (g instanceof de.micromata.opengis.kml.v_2_2_0.MultiGeometry) {
                de.micromata.opengis.kml.v_2_2_0.MultiGeometry mg = (de.micromata.opengis.kml.v_2_2_0.MultiGeometry) g;

                for (de.micromata.opengis.kml.v_2_2_0.Geometry gg : mg.getGeometry()) {
                    geometry(result, gg, properties);
                }
            } else {
                geometry(result, g, properties);
            }
        }
    }

    private static void geometry
            (Map<Geometry, Map<String, Object>> result, de.micromata.opengis.kml.v_2_2_0.Geometry
                    geometry, Map<String, Object> properties) {
        if ((geometry instanceof de.micromata.opengis.kml.v_2_2_0.Polygon)) {
            de.micromata.opengis.kml.v_2_2_0.Polygon p = (de.micromata.opengis.kml.v_2_2_0.Polygon) geometry;

            List<de.micromata.opengis.kml.v_2_2_0.Coordinate> lc = p
                    .getOuterBoundaryIs()
                    .getLinearRing()
                    .getCoordinates();

            List<Coordinate> lco = new ArrayList<>();
            for (de.micromata.opengis.kml.v_2_2_0.Coordinate c : lc) {
                lco.add(new Coordinate(c.getLongitude(), c.getLatitude()));
            }
            LinearRing lro = FACTORY.createLinearRing(lco.toArray(new Coordinate[0]));

            List<LinearRing> lri = new ArrayList<>();
            List<de.micromata.opengis.kml.v_2_2_0.Boundary> lb = p.getInnerBoundaryIs();
            for (de.micromata.opengis.kml.v_2_2_0.Boundary b : lb) {
                lco = new ArrayList<>();
                lc = b.getLinearRing().getCoordinates();
                for (de.micromata.opengis.kml.v_2_2_0.Coordinate c : lc) {
                    lco.add(new Coordinate(c.getLongitude(), c.getLatitude()));
                }
                lri.add(FACTORY.createLinearRing(lco.toArray(new Coordinate[0])));
            }

            Polygon res = FACTORY.createPolygon(lro, lri.toArray(new LinearRing[0]));
            result.put(res, properties);
        }

        if ((geometry instanceof de.micromata.opengis.kml.v_2_2_0.LinearRing)) {
            List<de.micromata.opengis.kml.v_2_2_0.Coordinate> lc = ((de.micromata.opengis.kml.v_2_2_0.LinearRing) geometry)
                    .getCoordinates();

            List<Coordinate> lco = new ArrayList<>();
            for (de.micromata.opengis.kml.v_2_2_0.Coordinate c : lc) {
                lco.add(new Coordinate(c.getLongitude(), c.getLatitude()));
            }
            LinearRing lro = FACTORY.createLinearRing(lco.toArray(new Coordinate[0]));

            Polygon res = FACTORY.createPolygon(lro);
            result.put(res, properties);
        }

        if (geometry instanceof de.micromata.opengis.kml.v_2_2_0.Point) {
            de.micromata.opengis.kml.v_2_2_0.Coordinate c = ((de.micromata.opengis.kml.v_2_2_0.Point) geometry)
                    .getCoordinates()
                    .get(0);

            Coordinate cc = new Coordinate(c.getLongitude(), c.getLatitude());

            Point res = FACTORY.createPoint(cc);
            result.put(res, properties);
        }
    }
}
