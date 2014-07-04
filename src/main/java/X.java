import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.System.err;

/**
 * Created by husain on 6/12/14.
 */
public class X {
    Logger log = Logger.getAnonymousLogger();

    Map<Integer, PairHistogram> authorSubjectOfYear = Maps.newConcurrentMap();
    final PairHistogram authorCountry=new PairHistogram();

    private X() {
    }

    public X(final int year) {
        authorSubjectOfYear.put(year,new PairHistogram());
    }


    public void appendToMatrix(Set<String> subjects, Set<String> countries, Set<String> authors, final int year) {
        double inc = 1.0 / ((double) authors.size() * subjects.size());
        for (String authorName : authors) {
            for (String subject : subjects) {
                authorSubjectOfYear.get(year).incPairBy(authorName, subject, inc);
            }
        }
        inc = 1.0 / ((double) authors.size() * countries.size());
        for (String authorName : authors) {
            for (String country : countries) {
                authorCountry.incPairBy(authorName, country, inc);
            }
        }
    }

    /**
     * meant to run in series
     * @param otherX
     */
    synchronized public void add(final X otherX) {
        //first add the year histogram
        Set<Integer> otherYears = otherX.authorSubjectOfYear.keySet();
        Set<Integer> unionYears=Sets.union(this.authorSubjectOfYear.keySet(), otherYears);
        Set<Integer> intersectionYears=Sets.intersection(this.authorSubjectOfYear.keySet(), otherYears);
        for(int year: unionYears){
            final PairHistogram otherXPairHistogramOfYear = otherX.authorSubjectOfYear.get(year);
            if(intersectionYears.contains(year)){//need to add
                this.authorSubjectOfYear.get(year).appendAndAdd(otherXPairHistogramOfYear);
            }else if(otherYears.contains(year)){//copy them over from the other year
                this.authorSubjectOfYear.put(year, otherXPairHistogramOfYear);
            }
        }
        //now add the author country histogram
        this.authorCountry.appendAndAdd(otherX.authorCountry);
    }

    class PairHistogram {
        ConcurrentMap<Pair<String, String>, Double> histogram = new MapMaker()
                .concurrencyLevel(16)
                .initialCapacity(1000)
                .makeMap();

        public void incPairBy(String s1, String s2, final double delta) {
            final Pair key = new Pair(s1, s2);
            histogram.compute(key, (k, v) -> (v == null) ? delta : v + delta);

        }

        /**
         * not meant to run in parallel
         * @param otherHistogram
         */
        synchronized void appendAndAdd(final PairHistogram otherHistogram) {
            for(Pair<String,String> oKey:otherHistogram.histogram.keySet()){
                final Double otherVal = otherHistogram.histogram.get(oKey);
                this.histogram.compute(oKey, (k, v) -> (v == null) ? otherVal : v + otherVal);
            }
        }
    }
}
