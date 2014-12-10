Code Style
=====

Our code style is enforced using custom formatting options in intellij idea. Intellij will automatically pick up this code style and apply it. However you may wish to format changes lines to ensure you are following the style.

BasicDocumentRevision
ALongClassName

For reference the style looks like:

```java
class AClass implements InterfaceA, InterfaceB {

    private static final String aLongString = "this is a string that extends beyond" +
            " the line limit";

    private long aMethodWithAReallyLongSignature(ALongClassName aLongClassName,
                                                       List<String> list,
                                                       Long aLong,
                                                       AnotherLongClass anotherLongClass,
                                                       Map<String, Object> amap) {

        ALongClassName aVariable = aLongClassName.doThing(aLongClassName.getId(), list.get(0));

        for (int i = 1; i < revisions.size(); i++) {
            //do something here
        }

        if (anotherLongClass != null) {
            return anotherLongClass.getReference() + aLongClassName.getId();
        } else {
            return null;
        }
    }
}
```