package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        ArrayList<TDItem> td_items = new ArrayList<TDItem>();
        for (int i = 0; i < numFields(); i ++) {
            td_items.add(new TDItem(types[i], names[i]));
        }
        return td_items.listIterator();
    }

    private static final long serialVersionUID = 1L;
    private Type[] types;
    private String[] names;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (fieldAr == null) {
            this.types = typeAr;
            this.names = new String[types.length];
            for (int i = 0; i < typeAr.length; i++) {
                this.names[i] = "null";
            }
        } else if (fieldAr.length < typeAr.length) {
            this.types = typeAr;
            this.names = new String[types.length];
            for (int i = 0; i < fieldAr.length; i++) {
                this.names[i] = fieldAr[i];
            }
            for (int i = fieldAr.length; i < typeAr.length; i++) {
                this.names[i] = "null";
            }
        } else {
            this.types = typeAr;
            this.names = fieldAr;
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.types = typeAr;
        this.names = new String[types.length];
        for (int i = 0; i < names.length; i++) {
            this.names[i] = "null";
        }
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return types.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= numFields()) {
            throw new NoSuchElementException();
        }
        return names[i];
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= numFields()) {
            throw new NoSuchElementException();
        }
        return types[i];
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < names.length; i++) {
            if (names[i].equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (Type t : types) {
            size += t.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) { 
        Type[] td3types = new Type[td1.numFields() + td2.numFields()];
        String[] td3names = new String[td1.numFields() + td2.numFields()];

        for (int i = 0; i < td1.numFields(); i++) {
            td3types[i] = td1.getFieldType(i);
            td3names[i] = td1.getFieldName(i);
        }

        for (int i = 0; i < td2.numFields(); i++) {
            td3types[i+td1.numFields()] = td2.getFieldType(i);
            td3names[i+td1.numFields()] = td2.getFieldName(i);
        }

        return new TupleDesc(td3types, td3names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TupleDesc)) {
            return false;
        }

        TupleDesc otd = (TupleDesc) o;
        
        if (otd.numFields() != this.numFields()) {
            return false;
        }

        for (int i = 0; i < otd.numFields(); i++) {
            if (!otd.getFieldType(i).equals(this.getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String str = "";
        for (int i = 0; i < types.length; i++) {
            str += types[i];
            str += "[" + i + "](";
            str += names[i];
            str += "[" + i + "]), ";
        }
        return str.substring(0, str.length() - 2);
    }
}
