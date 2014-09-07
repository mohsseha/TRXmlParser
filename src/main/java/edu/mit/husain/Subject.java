package edu.mit.husain;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by husain on 7/20/14.
 */
final public class Subject {


    public static ConcurrentMap<String, Subject> allSubjects = Maps.newConcurrentMap();
    final String subjectName;

    private Subject(final String subjectName) {
        this.subjectName = subjectName;
        allSubjects.put(subjectName, this);
    }

    public static Subject from(final String name) {
        Subject subject = allSubjects.get(name);
        if (subject == null) {
            subject = new Subject(name);
        }
        return subject;
    }

    public static List<Subject> fromStringList(final List<String> strings) {
        return Lists.newArrayList(Lists.transform(strings, (s) -> (from(s))));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Subject subject = (Subject) o;

        return subjectName.equals(subject.subjectName);

    }

    @Override
    public int hashCode() {
        return subjectName.hashCode();
    }
}
