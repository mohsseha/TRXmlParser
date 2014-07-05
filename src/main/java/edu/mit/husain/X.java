package edu.mit.husain;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import javafx.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by husain on 6/12/14.
 */
public class X {
    Logger log = Logger.getAnonymousLogger();

    Map<Integer, PairHistogram> authorSubjectOfYear = Maps.newConcurrentMap();
    final PairHistogram authorCountry = new PairHistogram();

    private X() {
    }

    public X(final int year) {
        authorSubjectOfYear.put(year, new PairHistogram());
    }


    public void appendToMatrix(final List<String> subjects,final List<String> countries,final List<String> authors, final int year) {
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
     *
     * @param otherX
     */
    synchronized public void add(final X otherX) {
        //first add the year histogram
        Set<Integer> otherYears = otherX.authorSubjectOfYear.keySet();
        Set<Integer> unionYears = Sets.union(this.authorSubjectOfYear.keySet(), otherYears);
        Set<Integer> intersectionYears = Sets.intersection(this.authorSubjectOfYear.keySet(), otherYears);
        for (int year : unionYears) {
            final PairHistogram otherXPairHistogramOfYear = otherX.authorSubjectOfYear.get(year);
            if (intersectionYears.contains(year)) {//need to add
                this.authorSubjectOfYear.get(year).appendAndAddOtherHistogram(otherXPairHistogramOfYear);
            } else if (otherYears.contains(year)) {//copy them over from the other year
                this.authorSubjectOfYear.put(year, otherXPairHistogramOfYear);
            }
        }
        //now add the author country histogram
        this.authorCountry.appendAndAddOtherHistogram(otherX.authorCountry);
    }


    class PairHistogram {
        private ConcurrentMap<String, ConcurrentMap<String, Double>> histogram = new MapMaker()
                .concurrencyLevel(16 * 4)//guess only
                .initialCapacity(1000)
                .makeMap();

        public void incPairBy(String s1, String s2, final double delta) {
            synchronized (this) {
                final double currentValue = valueOf(s1, s2);//makes sure that an internal Map exists
                histogram.get(s1).put(s2, currentValue + delta);//can't fail since line above created internal array
            }
        }

        public double valueOf(final String s1, final String s2) {
            if (histogram.get(s1) == null) {
                histogram.put(s1, new MapMaker()
                                .initialCapacity(100)
                                .makeMap()
                );
            }
            return histogram.get(s1).getOrDefault(s2, 0.0);
        }

        /**
         * not meant to run in parallel
         *
         * @param otherHistogram
         */
        synchronized void appendAndAddOtherHistogram(final PairHistogram otherHistogram) {
            for (final Pair<String, String> oKey : otherHistogram.keySetAsPair()) {
                final String s1 = oKey.getKey();
                final String s2 = oKey.getValue();
                final double thisVal = this.valueOf(s1, s2);
                final double otherVal = otherHistogram.valueOf(s1, s2);
                this.histogram.get(s1).put(s2, thisVal + otherVal);
            }
        }

        public List<Pair<String, String>> keySetAsPair() {
            int size = 5 * histogram.keySet().size();
            List<Pair<String, String>> result = Lists.newArrayListWithExpectedSize(size);
            for (String s1 : histogram.keySet()) {
                for (String s2 : histogram.get(s1).keySet()) {
                    result.add(new Pair<>(s1, s2));
                }
            }
            return result;
        }

        public Set<String> primaryKeySet() {
            return histogram.keySet();
        }

        public String maxOtherKeyFor(final String primaryKey) {
            final ConcurrentMap<String, Double> innerMap = histogram.get(primaryKey);
            final Set<String> secondaryKeys = innerMap.keySet();
            if (innerMap == null || secondaryKeys.size() == 0) {
                return "UNKNOWN";
            }
            String result = secondaryKeys.iterator().next();
            for (String candidate : secondaryKeys) {
                if (valueOf(primaryKey, candidate) > valueOf(primaryKey, result)) {
                    result = candidate;
                }
            }
            return result;
        }
    }
}
