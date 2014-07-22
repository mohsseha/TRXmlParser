package edu.mit.husain;

import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import static java.lang.Math.abs;
import static java.lang.System.err;

/**
 * Created by husain on 7/21/14.
 */
public class AuthorCountryDB {
    final String AUTHOR_COUNTRY_DB = "/tmp/author_country_year.PipeDelimitedFile";

    public static final AuthorCountryDB singleton = new AuthorCountryDB();
    private final HashBasedTable<String, Integer, Country> db = HashBasedTable.create(36054863, 10);

    private AuthorCountryDB() {
        err.print("loading " + AUTHOR_COUNTRY_DB + "...");
        final Splitter splitter = Splitter.on('|');
        try {
            List<String> rows = Files.readLines(new File(AUTHOR_COUNTRY_DB), Charset.defaultCharset());
            for (String row : rows) {
                final List<String> parts = splitter.splitToList(row);
                //        tordo, p|france|1980
                db.put(parts.get(0), Integer.parseInt(parts.get(2)), Country.from(parts.get(1)));
            }
        } catch (IOException e) {
            err.println("\nhad problem reading file " + AUTHOR_COUNTRY_DB);
            e.printStackTrace();
        }
        err.println("finished loading.");
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
