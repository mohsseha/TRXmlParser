package edu.mit.husain;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;

import java.util.List;

/**
 * Created by husain on 7/20/14.
 */
public class YearlyStatistics {
    public int getYear() {
        return year;
    }

    private HashBasedTable<Country, Subject, Double> xCountrySubject = HashBasedTable.create(400, 251);

    int year;

    public YearlyStatistics(int i) {
        year = i;
    }

    synchronized public void incSubjectsAndCountryStats(List<Subject> subjects, List<Country> countries) { //should not matter since this should only be called by a single thread.
        Preconditions.checkNotNull(subjects);
        Preconditions.checkNotNull(countries);
        final double inc = (1.0 / subjects.size()) * (1.0 / countries.size());
        for (Subject subject : subjects)
            for (Country country : countries) {
                double current = Objects.firstNonNull(xCountrySubject.get(country, subject), 0.0);
                xCountrySubject.put(country, subject, current + inc);
            }
    }

    public Iterable<String> enumerateValuesAsSql() {
        return Iterables.transform(xCountrySubject.cellSet(),
                (cell) -> "INSERT INTO year_country_subject_x (year,country, subject_hash,x) VALUES (" + this.getYear() + ",'" +
                        cell.getRowKey().countryName + "'," + cell.getColumnKey().subjectName.hashCode() + "," + cell.getValue() + ");\n");

    }


}
