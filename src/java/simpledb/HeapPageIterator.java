package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
    
    private Iterator<Tuple> tupleIter;


    public HeapPageIterator(ArrayList<Tuple> t) {
        tupleIter = t.listIterator();
    }
     @Override
    public boolean hasNext() {
        return tupleIter.hasNext();
    }

    @Override
    public Tuple next() {
        return tupleIter.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}