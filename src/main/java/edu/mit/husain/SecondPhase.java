package edu.mit.husain;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.err;

/**
 * 2nd phase analysis based off of results of running ParseXML
 * Created by husain@mit.edu on 7/20/14.
 */
public class SecondPhase {

    static volatile long recordsProcessed = 0;
    static AuthorCountryDB authorCountryDB;
    public static final String SUB_SUBJECT_FILE = "/tmp/subSubjectUids.csv";

    /**
     * read in one year at a time. year.xml and automatically write out year.sql for results.
     *
     * @param yearBounds start and stop years
     * @throws Exception
     */
    public static void main(String... yearBounds) throws Exception {
        Preconditions.checkArgument(yearBounds.length == 2);

        err.println("loading sub-subject map from " + SUB_SUBJECT_FILE);
        SubSubject.main(new String[]{SUB_SUBJECT_FILE});

        progressMonitor.start();
        authorCountryDB = new AuthorCountryDB();

        IntStream.rangeClosed(Integer.parseInt(yearBounds[0]), Integer.parseInt(yearBounds[1])).parallel() //eg. 1980..2013
                .mapToObj(SecondPhase::readYearXml)
                .forEach(SecondPhase::writeYearStats);

        sqlOutAllSubjects();

        progressMonitor.stop();

    }

    private static void sqlOutAllSubjects() {
        File output = new File("allSubjects.sql");
        Subject.allSubjects.keySet().forEach((s) -> {
            try {
                Files.append(
                        "INSERT INTO subject_hash (subject, hash) VALUES ('" + s.replace("'", "") + "'," + s.hashCode() + ");\n"
                        , output, Charset.defaultCharset());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });


    }

    /**
     * write results into YYYY.sql files
     *
     * @param yearlyStatistics
     */
    private static void writeYearStats(YearlyStatistics yearlyStatistics) {
        try {
            BufferedWriter oWriter = Files.newWriter(new File(Integer.toString(yearlyStatistics.getYear()) + ".sql"), Charset.defaultCharset());
            for (String sql : yearlyStatistics.enumerateValuesAsSql()) {
                oWriter.write(sql);
            }
            oWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static YearlyStatistics readYearXml(int inputYear) {
        YearlyStatistics stats = new YearlyStatistics(inputYear);
        final String fileName = Integer.toString(inputYear) + ".xml";
        BufferedReader inputBuffer = ParseXML.bufferedReaderOfFile(new File(fileName));
        return getYearlyStatisticsFromBufferedReader(inputBuffer, stats);
    }


    private static YearlyStatistics getYearlyStatisticsFromBufferedReader(final BufferedReader inputBuffer, final YearlyStatistics stats) {
        try {

            final DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            final XPath xpath = XPathFactory.newInstance().newXPath();

            final StringBuilder recordTxt = new StringBuilder(44000);// average size of document is about 22k
            String line;
            while ((line = inputBuffer.readLine()) != null) {
                if (line.contains(ParseXML.REC_START)) {
                    recordTxt.setLength(0);
                }
                recordTxt.append(line).append('\n');
                if (line.contains(ParseXML.REC_END)) {//done with this record
                    addXmlRecordToStats(stats, recordTxt.toString(), db, xpath);
                    recordsProcessed++;
                    recordTxt.setLength(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            err.println("IO error on processingThread=" + Thread.currentThread().getName());
            Throwables.propagate(e);
        }
        return stats;
    }

    /**
     * MAIN RECORD PROCESSOR!
     * this method calculates the countries first directly in the record and then via authors
     *
     * @see #findCountriesViaAuthors(javax.xml.xpath.XPath, org.w3c.dom.Document, int)
     */
    private static void addXmlRecordToStats(final YearlyStatistics stats, final String xmlRecord, final DocumentBuilder db, final XPath xpath) throws Exception {
        db.reset();
        xpath.reset();

        final InputSource source = new InputSource(new StringReader(xmlRecord));
        final Document document = db.parse(source);

        final String uid = xpath.evaluate("REC/UID", document);
        final List<Subject> subjects = Subject.fromStringList(listOfStringsFromXPath(xpath, "//subject[@ascatype='traditional']", document));
        subjects.addAll(subSubjectsOfUid(uid));
        final List<Country> countries = Country.from(listOfStringsFromXPath(xpath, "//addresses//country", document));
        if (countries.size() == 0) {
            countries.addAll(findCountriesViaAuthors(xpath, document, stats.getYear()));
        }
        stats.incSubjectsAndCountryStats(subjects, countries);
    }

    private static Collection<Subject> subSubjectsOfUid(final String uid) {
        checkNotNull(uid);
        final String key = "WOS:" + uid;
        if (!SubSubject.documentSubjectMap.containsKey(key)) {
            return Collections.EMPTY_SET;
        }

        return Collections2.transform(SubSubject.documentSubjectMap.get(key),
                (subjSubjectString) -> Subject.from(subjSubjectString));
    }


    private static List<Country> findCountriesViaAuthors(final XPath xpath, final Document document, int year) throws XPathExpressionException {
        final List<String> authorNames = listOfStringsFromXPath(xpath, "//wos_standard", document);
        List<Country> result;
        for (String author : authorNames) {
            Optional<Country> countryOptional = authorCountryDB.get(author, year);
            if (countryOptional.isPresent()) {
                result = Lists.newArrayListWithExpectedSize(1);
                result.add(countryOptional.get());
                return result;
            }
        }
        return Collections.emptyList();
    }

    private static List<String> listOfStringsFromXPath(final XPath xpath, final String xpathPattern, final Document document) throws XPathExpressionException {
        final NodeList result = (NodeList) xpath.evaluate(xpathPattern, document, XPathConstants.NODESET);
        final List<String> propertyList = Lists.newArrayListWithCapacity(result.getLength());
        for (int i = 0; i < result.getLength(); i++) {
            propertyList.add(result.item(i).getTextContent().toLowerCase());
        }
        return propertyList;
    }


    private static Thread progressMonitor = new Thread() {
        @Override
        public void run() {
            while (true) {
                err.print("\tAvailable memory: " + Runtime.getRuntime().totalMemory() / (1024 * 1024 * 1024) + " GB");
                err.println("\tprogress...\t " + recordsProcessed / 1_000_000. + "Million Documents processed:");
                try {
                    Thread.sleep(1000 * 2 * 60);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };


}
