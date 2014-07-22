package edu.mit.husain;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by husain on 7/20/14.
 */
final public class Country {
    public static ConcurrentMap<String, Country> allCountries = Maps.newConcurrentMap();
    final String countryName;

    private Country(final String countryName) {
        this.countryName = countryName;
        allCountries.put(countryName, this);
    }

    public static Country from(final String name) {
        Country country = allCountries.get(name);
        if (country == null) {
            country = new Country(name);
        }
        return country;
    }

    public static List<Country> from(final List<String> strings) {
        return Lists.transform(strings, (s) -> (from(s)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Country country = (Country) o;

        return countryName.equals(country.countryName);

    }

    @Override
    public int hashCode() {
        return countryName.hashCode();
    }
}
