package edu.mit.husain;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.System.err;

/**
 * 2nd phase analysis based off of results of running ParseXML
 * Created by husain on 7/20/14.
 */
public class SecondPhase {

    static volatile long recordsProcessed = 0;
    static AuthorCountryDB authorCountryDB;

    /**
     * read in one year at a time. year.xml and automatically write out year.sql for results.
     *
     * @param argv start and stop years
     * @throws Exception
     */
    public static void main(String... argv) throws Exception {
        Preconditions.checkArgument(argv.length == 2);
        progressMonitor.start();
        authorCountryDB = new AuthorCountryDB();

        IntStream.range(Integer.parseInt(argv[0]), Integer.parseInt(argv[1])).parallel() //eg. 1980..2012
                .mapToObj(SecondPhase::readYearXml)
                .forEach(SecondPhase::writeYearStats);

        progressMonitor.stop();

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


    private static YearlyStatistics getYearlyStatisticsFromBufferedReader(BufferedReader inputBuffer, final YearlyStatistics stats) {
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


    private static void addXmlRecordToStats(final YearlyStatistics stats, final String xmlRecord, final DocumentBuilder db, final XPath xpath) throws Exception {
        db.reset();
        xpath.reset();

        final InputSource source = new InputSource(new StringReader(xmlRecord));
        final Document document = db.parse(source);

        final List<Subject> subjects = Subject.from(listOfStringsFromXPath(xpath, "//subject[@ascatype='traditional']", document));
        final List<Country> countries = Country.from(listOfStringsFromXPath(xpath, "//addresses//country", document));
        if (countries.size() == 0) {
            countries.addAll(findCountriesViaAuthors(xpath, document, stats.getYear()));
        }
        stats.incSubjectsAndCountryStats(subjects, countries);
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
