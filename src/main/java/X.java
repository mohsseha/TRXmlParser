import com.google.common.base.Objects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import java.util.Set;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.err;

/**
 * Created by husain on 6/12/14.
 */
public class X {
    Logger log = Logger.getAnonymousLogger();
    private Table<String, String, Double> nameSubjectTable = HashBasedTable.create();
    private Table<String, String, Integer> nameCountryTable = HashBasedTable.create();

    public X() {
    }


    public void appnedToExportMatrix(Set<String> subjects, Set<String> countries, Set<String> names) {
        checkArgument(names.size() > 0 && subjects.size() > 0);
        final double inc = 1.0 / ((double) names.size() * subjects.size());
        for (String name : names) {
            for (String subject : subjects) {
                incrementXby(name, subject, inc);
            }
        }
        if (countries.size() == 0) {
            log.fine("no found ... ");
            err.print("DEBUG: DELME no countries");
            return;
        }
        for (String name : names) {
            for (String country : countries) {
                incrementAX(name, country);
            }
        }
    }

    private void incrementAX(final String name, final String country) {
//        int current = getNameCountry(name, country);
//        nameSubjectTable.put(name, subject, current + 1);
    }

    synchronized private void incrementXby(final String name, final String subject, final double inc) {
        double current = getNameSubject(name, subject);
        nameSubjectTable.put(name, subject, current + inc);
    }

    private double getNameSubject(final String name, final String subject) {
        return Objects.firstNonNull(nameSubjectTable.get(name, subject), 0.0);
    }
}
