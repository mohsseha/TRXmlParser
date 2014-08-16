package edu.mit.husain;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.abs;
import static java.lang.System.err;

/**
 * Created by husain on 7/21/14.
 */
public class AuthorCountryDB {
    final static String AUTHOR_COUNTRY_DB = "/tmp/author_country_year.PipeDelimitedFile";

    private HashBasedTable<String, Integer, Country> db;

    public AuthorCountryDB() {
        err.print("loading " + AUTHOR_COUNTRY_DB + "...");
        final Splitter splitter = Splitter.on('|');
        try {
            db = Files.readLines(new File(AUTHOR_COUNTRY_DB), Charset.defaultCharset(), new LineProcessor<HashBasedTable<String, Integer, Country>>() {
                final HashBasedTable<String, Integer, Country> result = HashBasedTable.create(8_000_000, 1);
                final AtomicInteger integer = new AtomicInteger();

                @Override
                public boolean processLine(final String line) throws IOException {
                    final List<String> parts = splitter.splitToList(line);
                    //        tordo, p|france|1980
                    integer.incrementAndGet();
                    synchronized (result) {
                        result.put(parts.get(0), Integer.parseInt(parts.get(2)), Country.from(parts.get(1)));
                    }
                    if (integer.get() % 10_000_000 == 0)
                        err.println("\tlines processed==" + integer.get() + "countries=" + Country.size());
                    return true;
                }

                @Override
                public HashBasedTable<String, Integer, Country> getResult() {
                    return result;
                }
            });

        } catch (IOException e) {
            err.println("\nhad problem reading file " + AUTHOR_COUNTRY_DB);
            e.printStackTrace();
        }
        err.println("finished loading author country relation table");
    }

    final Optional<Country> get(final String author, final int year) {
        Country result = db.get(author, year);
        if (result == null && db.row(author).size() > 0) {
            List<Integer> sortedYears = Lists.newArrayList(db.row(author).keySet());
            sortedYears.sort((i1, i2) -> (abs(i1 - year) - abs(i2 - year)));
            result = db.get(author, sortedYears.get(0));//closest year
        }
        return Optional.fromNullable(result);
    }
}
