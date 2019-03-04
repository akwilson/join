package com.wilsonware.join;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Merges two {@link Iterable} objects in to one.
 * <p>
 * The {@link #createJoin} and {@link #createMerge} factory functions return an {@link Iterable}
 * to consume the merged objects. Objects in the underlying {@code Iterable}s are expected to arrive
 * sorted by the same key property. Keys are identified by the {@code MergeJoin} using either natural
 * ordering or a provided {@link Comparator} object.
 * </p>
 * @see <a href="http://use-the-index-luke.com/sql/join/sort-merge-join">Sort-Merge Join Reference</a>
 */
public class MergeJoin {
    private MergeJoin() {}

    /**
     * Such boilerplate. Much nonsense.
     *
     * @param iter an {@link Iterable}
     * @param <T>  the type in the {@code Iterable}
     * @return     a {@link Stream} of T
     */
    private static <T> Stream<T> i2s(Iterator<T> iter) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED), false);
    }

    /**
     * Creates a new merge {@link Iterable}. Uses natural ordering to identify and compare keys. The
     * underlying values implement the {@link Comparable} interface.
     *
     * @param left  the left source to merge, sorted by a key
     * @param right the right source to merge, sorted by the same key
     * @param <T>   the type of the underlying objects
     * @return      an {@code Iterable} to consume the merged objects
     */
    public static <T extends Comparable<T>> Iterable<T> createMerge(Iterable<T> left, Iterable<T> right) {
        return () -> i2s(new MergeIterator<>(left, right, null)).flatMap(MergedRow::stream).iterator();
    }

    /**
     * Creates a new merge {@link Iterable}. Uses the supplied {@link Comparable} to identify and compare keys.
     *
     * @param left     the left source to merge, sorted by a key
     * @param right    the right source to merge, sorted by the same key
     * @param comparer the {@code Comparator} to identify and sort keys in the underlying objects
     * @param <T>      the type of the underlying objects
     * @return         an {@code Iterable} to consume the merged objects
     */
    public static <T> Iterable<T> createMerge(Iterable<T> left, Iterable<T> right, Comparator<T> comparer) {
        return () -> i2s(new MergeIterator<>(left, right, comparer)).flatMap(MergedRow::stream).iterator();
    }

    /**
     * Creates a new join {@link Iterable}. Uses natural ordering to identify and compare keys. The
     * underlying values implement the {@link Comparable} interface.
     *
     * @param left  the left source to join, sorted by a key
     * @param right the right source to join, sorted by the same key
     * @param <T>   the type of the underlying objects
     * @return      an {@code Iterable} of {@link MergedRow} objects to consume the joined objects
     */
    public static <T extends Comparable<T>> Iterable<MergedRow<T>> createJoin(Iterable<T> left, Iterable<T> right) {
        return () -> new MergeIterator<>(left, right, null);
    }

    /**
     * Creates a new join {@link Iterable}. Uses the supplied {@link Comparable} to identify and compare keys.
     *
     * @param left     the left source to join, sorted by a key
     * @param right    the right source to join, sorted by the same key
     * @param comparer the {@code Comparator} to identify and sort keys in the underlying objects
     * @param <T>      the type of the underlying objects
     * @return         an {@code Iterable} of {@link MergedRow} objects to consume the joined objects
     */
    public static <T> Iterable<MergedRow<T>> createJoin(Iterable<T> left, Iterable<T> right, Comparator<T> comparer) {
        return () -> new MergeIterator<>(left, right, comparer);
    }

    /**
     * Performs the merge-join. Consumes values from each {@link Iterable} and compares
     * them by key. Returns the lowest value identified and consumes from the corresponding
     * {@code Iterable} on the next iteration.
     *
     * @param <T> the type of the underlying object
     */
    private static class MergeIterator<T> implements Iterator<MergedRow<T>> {
        private final Iterator<T> left;
        private final Iterator<T> right;
        private final Comparator<T> comparer;

        private MergedRow<T> result;
        private T leftVal;
        private T rightVal;
        private boolean goLeft = true;
        private boolean goRight = true;

        public MergeIterator(Iterable<T> left, Iterable<T> right, Comparator<T> comparer) {
            this.left = left.iterator();
            this.right = right.iterator();
            this.comparer = comparer;
        }

        private int compareVals(T left, T right) {
            if (left == null) {
                return 1;
            }

            if (right == null) {
                return -1;
            }

            if (comparer != null) {
                return comparer.compare(left, right);
            }

            @SuppressWarnings("unchecked")
            Comparable<T> comp = (Comparable<T>)left;
            return comp.compareTo(right);
        }

        @Override
        public boolean hasNext() {
            if (goLeft) {
                leftVal = left.hasNext() ? left.next() : null;
            }

            if (goRight) {
                rightVal = right.hasNext() ? right.next() : null;
            }

            if (leftVal == null && rightVal == null) {
                result = null;
                return false;
            }

            int res = compareVals(leftVal, rightVal);
            if (res < 0) {
                // item1 wins, consume from list1 next iteration
                result = new MergedRow<T>(leftVal, null);
                goLeft = true;
                goRight = false;
            } else if (res > 0) {
                // item2 wins, consume from list2 next iteration
                result = new MergedRow<T>(null, rightVal);
                goLeft = false;
                goRight = true;
            } else {
                // both keys equal, consume from both lists
                result = new MergedRow<T>(leftVal, rightVal);
                goLeft = true;
                goRight = true;
            }

            return true;
        }

        @Override
        public MergedRow<T> next() {
            if (result == null) {
                throw new NoSuchElementException("No values left to merge");
            }

            return result;
        }
    }

    /**
     * Represents a row in the merged data set. Either (but not both) values can be null.
     *
     * @param <T> the type of the underlying merged objects
     */
    public static class MergedRow<T> {
        private final T left;
        private final T right;

        MergedRow(T left, T right) {
            this.left = left;
            this.right = right;
        }

        /**
         * @return the value in the left source
         */
        public T getLeft() {
            return left;
        }

        /**
         * @return the value in the right source
         */
        public T getRight() {
            return right;
        }

        Stream<T> stream() {
            return Stream.of(getLeft(), getRight()).filter(Objects::nonNull);
        }
    }
}
