import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
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
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.IntStream;

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
    static X x = new X();


    public static void main(String... args) throws Exception {
        checkArgument(args.length >= 1, "what's the input file?");

        final File inputFile = new File(args[0]);
        final int numberOfWorkingFiles = Runtime.getRuntime().availableProcessors();
        checkArgument(numberOfWorkingFiles > 1, "number of files to split has to be grater than 1");
        final long minFileSize = inputFile.length() / numberOfWorkingFiles;
        int outFileNum = 0;
        BufferedWriter outputSink = newOutputFile(outFileNum);
        long outPos = 0;
        boolean write = false;
        try (BufferedReader bufferedReader = Files.asCharSource(inputFile, Charset.defaultCharset()).openBufferedStream()) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.contains(REC_START)) {
                    write = true;
                }
                if (write) {
                    outputSink.write(line);
                    outputSink.newLine();
                    outPos += ((long) (line.length()) + 1L);
                }
                if (outPos > minFileSize && line.contains(REC_END)) {
                    outputSink.close();
                    outPos = 0;
                    outFileNum++;
                    outputSink = newOutputFile(outFileNum);
                }
                if (line.contains(REC_END)) {
                    write = false;
                }
            }
//            need to make sur ethe output stream closed before closing the input stream...
            outputSink.close();
        } catch (IOException e) {
            e.printStackTrace();
            err.println("problem parsing input file, file" + inputFile.getAbsolutePath());
        }


        parseTmpFilesInParallel(outFileNum);

        csvDump(countryOfUid);

    }

    private static void csvDump(final ConcurrentMap<String, Set<String>> emailAddressOfUid) {
        emailAddressOfUid.forEach((s, strings) -> {
            out.print(s + '|');
            out.println(Joiner.on('|').join(strings));
        });
    }

    private static void parseTmpFilesInParallel(final int lastFileNum) throws Exception {
        IntStream.rangeClosed(0, lastFileNum).parallel()
                .mapToObj(ParseXML::newTmpReader)
                .forEach(ParseXML::processTmpInputBuffer);
    }

    private static void processTmpInputBuffer(BufferedReader tmpFileReader) {
        StringBuffer recordTxt = new StringBuffer(44000);// average size of document is about 22k
        String line;
        try {
            while ((line = tmpFileReader.readLine()) != null) {
                recordTxt.append(line);
                recordTxt.append('\n');
                if (line.contains(REC_END)) {//done with this record
                    processRecord(recordTxt.toString());
                    recordTxt.setLength(0);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            err.println("IO error on thread " + Thread.currentThread().getName());
            Throwables.propagate(e);
        }
    }


    private static java.io.BufferedWriter newOutputFile(int outFileNum) throws FileNotFoundException {
        return Files.newWriter(getFile(outFileNum), Charset.defaultCharset());
    }

    private static File getFile(int outFileNum) {
        return new File("/tmp/" + Integer.toString(outFileNum) + ".xml");
    }

    private static BufferedReader newTmpReader(int i) {
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = Files.newReader(getFile(i), Charset.defaultCharset());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return checkNotNull(bufferedReader);
    }


//    key data structures, concurrent!:

    static Set<String> allSubjects = Sets.newConcurrentHashSet();
    static Set<String> allUids = Sets.newConcurrentHashSet();
    static Set<String> allCountries = Sets.newConcurrentHashSet();
    static Set<String> allEmailAddresses = Sets.newConcurrentHashSet();
    static Set<String> allAuthorNames = Sets.newConcurrentHashSet();
    static ConcurrentMap<String, Set<String>> subjectsOfUid = new MapMaker()
            .concurrencyLevel(8)
            .initialCapacity(1000)
            .makeMap();
    static ConcurrentMap<String, Set<String>> countryOfUid = new MapMaker()
            .concurrencyLevel(8)
            .initialCapacity(1000)
            .makeMap();
    static ConcurrentMap<String, Set<String>> emailAddressOfUid = new MapMaker()
            .concurrencyLevel(8)
            .initialCapacity(1000)
            .makeMap();
    static ConcurrentMap<String, Set<String>> woSNameOfUid = new MapMaker()
            .concurrencyLevel(8)
            .initialCapacity(1000)
            .makeMap();


    /**
     * key processing Method
     *
     * @param xmlRecord string of the xml representing the TR data of a publication
     */
    private static void processRecord(String xmlRecord) throws Exception {
//       bunch of sanity checks:
        checkArgument(xmlRecord.contains(REC_START) && xmlRecord.contains(REC_END), "valid xml file???!!!\n\n" + xmlRecord);
        checkArgument(xmlRecord.contains("<UID>WOS:"), "valid xml file???!!!\n\n" + xmlRecord);

        InputSource source = new InputSource(new StringReader(xmlRecord));

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document document = db.parse(source);

        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();

        final String uid = checkNotNull(xpath.evaluate("REC/UID", document));
        final Set<String> subjects = setOfStringsFromXPath(xpath, "//subject[@ascatype='traditional']", document);
        final Set<String> countries = setOfStringsFromXPath(xpath, "//addresses//country", document);
        final Set<String> emailAddresses = setOfStringsFromXPath(xpath, "//email_addr", document);
        final Set<String> names = setOfStringsFromXPath(xpath, "//wos_standard", document);

        allUids.add(uid);
        allSubjects.addAll(subjects);
        allCountries.addAll(countries);
        allEmailAddresses.addAll(emailAddresses);
        allAuthorNames.addAll(names);

        //the following assumes we only see a UID once in the different threads so we don't have to worry about syncing
        countryOfUid.put(uid, countries);
        subjectsOfUid.put(uid, subjects);
        emailAddressOfUid.put(uid, emailAddresses);
        woSNameOfUid.put(uid, names);

        x.appnedToExportMatrix(subjects, countries, names);
    }

    private static Set<String> setOfStringsFromXPath(XPath xpath, final String xpathPattern, final Document document) throws XPathExpressionException {
        checkNotNull(xpathPattern);
        checkNotNull(document);
//        multipel values:

        NodeList result = (NodeList) xpath.evaluate(xpathPattern, document, XPathConstants.NODESET);
        final SortedSet<String> propertySet = Sets.newTreeSet();
        for (int i = 0; i < result.getLength(); i++) {
            propertySet.add(checkNotNull(result.item(i).getTextContent()));
        }


        return (propertySet);

    }

}
