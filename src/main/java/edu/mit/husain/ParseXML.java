package edu.mit.husain;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import javafx.util.Pair;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.err;
import static java.lang.System.out;

/**
 * Created by husain on 6/6/14.
 */
public class ParseXML {

    public static final String REC_END = "</REC>";
    public static final String REC_START = "<REC ";

    final static Set<String> allSubjects = Sets.newConcurrentHashSet();
    final static Set<String> allUids = Sets.newConcurrentHashSet();
    final static Set<String> allCountries = Sets.newConcurrentHashSet();
    final static Set<String> allAuthorNames = Sets.newConcurrentHashSet();
//    final static ConcurrentMap<String, Set<String>> subjectsOfUid = new MapMaker()
//            .concurrencyLevel(16)
//            .initialCapacity(1000)
//            .makeMap();
//    final static ConcurrentMap<String, Set<String>> countryOfUid = new MapMaker()
//            .concurrencyLevel(16)
//            .initialCapacity(1000)
//            .makeMap();
//    final static ConcurrentMap<String, Set<String>> emailAddressOfUid = new MapMaker()
//            .concurrencyLevel(16)
//            .initialCapacity(1000)
//            .makeMap();
//    final static ConcurrentMap<String, Set<String>> woSNameOfUid = new MapMaker()
//            .concurrencyLevel(16)
//            .initialCapacity(1000)
//            .makeMap();



    public static void main(String... filenames) throws Exception {

        checkArgument(filenames.length >= 1, "what's the input file?");
        progressMonitor.start();

        List<X> xes = Lists.newArrayList(filenames).parallelStream().map(ParseXML::toXMatrix).collect(Collectors.toList());

        err.print("done parsing all files... summing");
        final X finalX = new X(1900);
        xes.forEach((x) -> finalX.add(x));


        List<String> sortedSubjects = Lists.newArrayList();
        sortedSubjects.addAll(allSubjects);
        sortedSubjects.sort((c1, c2) -> c1.compareToIgnoreCase(c2));
        sqlOut(sortedSubjects);
        out.println("delete from x;");
        for (int year : finalX.authorSubjectOfYear.keySet()) {
            sqlOut(sortedSubjects, year, finalX.authorSubjectOfYear.get(year));
        }
        sqlOutAuthorCountryTable(finalX);
        err.println("done! here are all the countries we saw:");
        err.println(Joiner.on("\n").join(allCountries));
        progressMonitor.stop();
    }

    private static void sqlOutAuthorCountryTable(final X x) {
        final X.PairHistogram authorCountry =x.authorCountry;
        out.println("delete from author_country;");
        for(final String authorName:authorCountry.primaryKeySet()){
            String sql="INSERT INTO author_country\n"+
                    "(author,country)\n"+
                    "VALUES('"+authorName.replace("'","")+"', '"+authorCountry.maxOtherKeyFor(authorName).replace("'","")+"');";
            out.println(sql);

        }
    }


    private static void sqlOut(final List<String> sortedSubjects, final int year, X.PairHistogram authorSubject) {
        for (Pair<String, String> key : authorSubject.keySetAsPair()) {
            StringBuffer sql = new StringBuffer("INSERT INTO x\n");
            final String authorName =key.getKey();
            final String subject = key.getValue();
            final int sPos = sortedSubjects.indexOf(subject);
            sql.append("(author, year, s" + sPos + ")\n");
            sql.append("VALUES ('" + authorName.replace("'","") + "', " + year + ", " + authorSubject.valueOf(authorName,subject) + ");");
            out.println(sql);
        }

    }

    private static void sqlOut(final List<String> sortedSubjects) {
        out.println("delete from subject_s;");
        for (int i = 0; i < sortedSubjects.size(); i++) {
            out.println("INSERT INTO subject_s (subject, s) VALUES ('" + sortedSubjects.get(i).replace("'","") + "'," + i + ");");
        }
    }


    private static X toXMatrix(final String filename) {
        final File inputFile = new File(checkNotNull(filename));
        int year = Integer.parseInt(filename.substring(3, 7));
        final X x = new X(year);
        final X result= getXFromBufferedReader(bufferedReaderOfFile(inputFile), x, year);
        err.println("\t done processing file="+filename);
        return result;
    }


    private static BufferedReader bufferedReaderOfFile(final File file) {
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = Files.newReader(checkNotNull(file), Charset.defaultCharset());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return checkNotNull(bufferedReader);
    }

    private static X getXFromBufferedReader(BufferedReader tmpFileReader, final X Xbatch, final int year) {
        try {

        final DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        final XPath xpath = XPathFactory.newInstance().newXPath();

        StringBuffer recordTxt = new StringBuffer(44000);// average size of document is about 22k
        String line;
            while ((line = tmpFileReader.readLine()) != null) {
                if (line.contains(REC_START)) {
                    recordTxt.setLength(0);
                }
                recordTxt.append(line);
                recordTxt.append('\n');
                if (line.contains(REC_END)) {//done with this record
                    processXmlRecord(Xbatch, recordTxt.toString(), year,db,xpath);
                    recordTxt.setLength(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            err.println("IO error on processingThread=" + Thread.currentThread().getName());
            Throwables.propagate(e);
        }
        return Xbatch;
    }


//    key data structures, concurrent!:


    /**
     * key processing Method
     *  @param x         where we add our results
     * @param xmlRecord string of the xml representing the TR data of a publication
     * @param db
     * @param xpath
     */
    private static void processXmlRecord(final X x, final String xmlRecord, int year,final DocumentBuilder db,final XPath xpath) throws Exception {
////       bunch of sanity checks:
//        checkArgument(xmlRecord.contains(REC_START) && xmlRecord.contains(REC_END), "valid xml file???!!!\n\n" + xmlRecord);
//        checkArgument(xmlRecord.contains("<UID>"), "valid xml file???!!!\n\n" + xmlRecord);

        db.reset();
        xpath.reset();

        final InputSource source = new InputSource(new StringReader(xmlRecord));
        final Document document = db.parse(source);


        final String uid = xpath.evaluate("REC/UID", document);
        final List<String> subjects = listOfStringsFromXPath(xpath, "//subject[@ascatype='traditional']", document);
        final List<String> countries = listOfStringsFromXPath(xpath, "//addresses//country", document);
//        final List<String> emailAddresses = listOfStringsFromXPath(xpath, "//email_addr", document);
        final List<String> names = listOfStringsFromXPath(xpath, "//wos_standard", document);

        allUids.add(uid);
        allSubjects.addAll(subjects);
        allCountries.addAll(countries);
        allAuthorNames.addAll(names);

        //the following assumes we only see a UID once in the different threads so we don't have to worry about syncing
//        countryOfUid.put(uid, countries);
//        subjectsOfUid.put(uid, subjects);
//        emailAddressOfUid.put(uid, emailAddresses);
//        woSNameOfUid.put(uid, names);

        x.appendToMatrix(subjects, countries, names, year);
    }

    final private static List<String> listOfStringsFromXPath(final XPath xpath, final String xpathPattern, final Document document) throws XPathExpressionException {
        final NodeList result = (NodeList) xpath.evaluate(xpathPattern, document, XPathConstants.NODESET);
        final List<String> propertySet = Lists.newArrayList();
        for (int i = 0; i < result.getLength(); i++) {
            propertySet.add(result.item(i).getTextContent().toLowerCase());
        }
        return propertySet;
    }

    static Thread progressMonitor = new Thread() {
        @Override
        public void run() {
            while (true) {
                err.print("\tAvailable memory: " + Runtime.getRuntime().totalMemory() / (1024 * 1024 * 1024) + " GB");
                err.println("\tprogress...\t " + allUids.size() / 1_000_000. + "Million Documents processed:");
                err.println(allCountries.size() + "countries\t" + allSubjects.size() + "subjects, and\t" +allAuthorNames.size()/1_000_000. + " Million authors");
                try {
                    Thread.sleep(1000 * 3 * 60);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };


}
