package com.wilsonware.join;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.wilsonware.join.MergeJoin.MergedRow;

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for the {@link MergeJoin} class.
 */
public class MergeJoinTest {
    @Test
    public void testMerge() {
        List<String> list1 = ImmutableList.of("AAA", "CCC", "FFF", "YYY", "ZZZ");
        List<String> list2 = ImmutableList.of("BBB", "DDD", "EEE", "QQQ", "XXX");

        Iterable<String> res = MergeJoin.createMerge(list1, list2);
        assertThat(res, contains("AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "QQQ", "XXX", "YYY", "ZZZ"));
    }

    @Test
    public void testOverlapping() {
        List<String> list1 = ImmutableList.of("AAA", "AAA", "FFF", "QQQ", "ZZZ");
        List<String> list2 = ImmutableList.of("AAA", "DDD", "EEE", "QQQ", "XXX");

        Iterable<String> res = MergeJoin.createMerge(list1, list2);
        assertThat(res, contains("AAA", "AAA", "AAA", "DDD", "EEE", "FFF", "QQQ", "QQQ", "XXX", "ZZZ"));
    }

    @Test
    public void testUneven() {
        List<String> list1 = ImmutableList.of("AAA", "AAA", "FFF", "QQQ", "ZZZ");
        List<String> list2 = ImmutableList.of("AAA", "DDD");

        Iterable<String> res = MergeJoin.createMerge(list1, list2);
        assertThat(res, contains("AAA", "AAA", "AAA", "DDD", "FFF", "QQQ", "ZZZ"));

        res = MergeJoin.createMerge(list2, list1);
        assertThat(res, contains("AAA", "AAA", "AAA", "DDD", "FFF", "QQQ", "ZZZ"));
    }

    @Test
    public void testWithComparator() {
        List<String> list1 = ImmutableList.of("A", "CCC", "FFFFF", "BBBBBB");
        List<String> list2 = ImmutableList.of("ZZ", "PPPP");

        Iterable<String> res = MergeJoin.createMerge(list1, list2, Comparator.comparingInt(String::length));
        assertThat(res, contains("A", "ZZ", "CCC", "PPPP", "FFFFF", "BBBBBB"));
    }

    @Test
    public void testJoin() {
        List<String> list1 = ImmutableList.of("AAA", "BBB", "FFF", "QQQ", "ZZZ");
        List<String> list2 = ImmutableList.of("AAA", "DDD", "EEE", "QQQ", "XXX");

        Iterable<MergedRow<String>> res = MergeJoin.createJoin(list1, list2);
        List<MergedRow<String>> test = StreamSupport.stream(res.spliterator(), false).collect(Collectors.toList());
        assertThat(test.get(0).getLeft(), equalTo("AAA"));
        assertThat(test.get(0).getRight(), equalTo("AAA"));
        assertThat(test.get(1).getLeft(), equalTo("BBB"));
        assertNull(test.get(1).getRight());
        assertNull(test.get(2).getLeft());
        assertThat(test.get(2).getRight(), equalTo("DDD"));
        assertNull(test.get(3).getLeft());
        assertThat(test.get(3).getRight(), equalTo("EEE"));
        assertThat(test.get(4).getLeft(), equalTo("FFF"));
        assertNull(test.get(4).getRight());
        assertThat(test.get(5).getLeft(), equalTo("QQQ"));
        assertThat(test.get(5).getRight(), equalTo("QQQ"));
        assertNull(test.get(6).getLeft());
        assertThat(test.get(6).getRight(), equalTo("XXX"));
        assertThat(test.get(7).getLeft(), equalTo("ZZZ"));
        assertNull(test.get(7).getRight());
    }
}
