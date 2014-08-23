package edu.mit.husain;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.err;
import static java.lang.System.out;

/**
 * Created by husain on 8/15/14.
 */
public class SubSubject {

    static HashBasedTable<String, String, Double> phi = HashBasedTable.create(400, 400);
    static Map<String, Set<String>> subjectDocumentMap = Maps.newHashMapWithExpectedSize(400);
    static Map<String, Set<String>> documentSubjectMap = Maps.newHashMapWithExpectedSize(5500000);
    static public int linesProcessed = 0;

    /**
     * read in one year at a time. year.xml and automatically write out year.sql for results.
     *
     * @param argv start and stop years
     * @throws Exception
     */
    public static void main(String... argv) throws Exception {
        Preconditions.checkArgument(argv.length == 1, "1 arg which is the name of the file to process");
        progressMonitor.start();
        err.println("loading input file...");
        Files.readLines(new File(argv[0]), Charset.defaultCharset(), new LineProcessor<Integer>() {
            final Splitter splitter = Splitter.on(",WOS:");

            @Override
            public boolean processLine(String line) throws IOException {
                linesProcessed++;
                Iterator<String> splitLine = splitter.split(line).iterator();
                final String subSubject = splitLine.next().replace('"', ' ');
                final String document = splitLine.next();
                Set<String> documents = subjectDocumentMap.getOrDefault(subSubject, Sets.newHashSet());
                Set<String> subSubjects = documentSubjectMap.getOrDefault(document, Sets.newHashSet());
                documents.add(document);
                subSubjects.add(subSubject);
                subjectDocumentMap.put(subSubject, documents);
                documentSubjectMap.put(document, subSubjects);
                return true;
            }

            @Override
            public Integer getResult() {
                return null;
            }
        });

        err.println("calculating phi...");
        Set<String> allSubjects = subjectDocumentMap.keySet();
        for (String row : allSubjects)
            for (String col : allSubjects) {
                phi.put(row, col, corr(row, col));
            }

        err.println("outputting...");
        //output all subjects
        out.print("allSubSubjects={\"");
        out.println(Joiner.on("\",\n\"").join(allSubjects) + "\"};");
        //phi matrix:
        for (Table.Cell<String, String, Double> cell : phi.cellSet()) {
            out.format("subSubjectPhi[\"" + cell.getRowKey() + "\",\"" + cell.getColumnKey() + "\"]=%3.16f;\n", cell.getValue());//subSubjectPhi["subject_i","subject_j"]=0.342;
        }

        progressMonitor.stop();

    }

    /**
     * corollation between subjects 1 and 2
     *
     * @return
     */
    private static double corr(String s1, String s2) {
        Set<String> docs1 = checkNotNull(subjectDocumentMap.get(s1)), docs2 = checkNotNull(subjectDocumentMap.get(s2));
        Set<String> intersection = Sets.intersection(docs1, docs2);
        return (double) intersection.size() / Math.sqrt(docs1.size() * docs2.size() + 1e-15);

    }


    private static Thread progressMonitor = new Thread() {
        @Override
        public void run() {
            while (true) {
                err.print("\tAvailable memory: " + Runtime.getRuntime().totalMemory() / (1024 * 1024 * 1024) + " GB");
                err.println("\tprogress...\t " + linesProcessed / 1_000_000. + "Million Lines processed:");
                try {
                    Thread.sleep(1000 * 2 * 60);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    };


}
