# Join two sorted collections

The `MergeJoin` class will merge two sorted collections in to a single sorted iterable

* The input collections must be sorted.
* Only one pass is required for the merge i.e. runs in O(n)

```java
class Test {
    public static void main(String[] args) {
        var list1 = List.of("AAA", "CCC", "FFF");
        var list2 = List.of("BBB", "DDD", "EEE");
    
        Iterable<String> res = MergeJoin.createMerge(list1, list2);
        for (String str : res) {
            System.out.println(str);
        }
    }
}
```

Results in

```text
AAA
BBB
CCC
DDD
EEE
FFF
```

Two collections of objects can also be joined. See unit tests for more details.