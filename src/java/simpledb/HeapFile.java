package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // bytes to skip
        int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
        byte[] page = new byte[BufferPool.PAGE_SIZE];

        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.skipBytes(offset);
            raf.read(page);
            return new HeapPage((HeapPageId) pid, page);
        }
        catch (Exception e) {
            System.out.println(e);
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (Math.ceil(file.length() / BufferPool.PAGE_SIZE));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        try {
            return new HeapFileIterator(tid, this);
        }
        catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    /*
    OKAY HENRY IGNORE ALL THIS ASDFASDFASDF WE HAVE TO RETURN DbFileIterator NOT Iterator<Tuple>
    public DbFileIterator iterator(TransactionId tid) {
        // each page has iterator over tuples already, so get each pages iterator and add tuples?
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Iterator<Tuple> pageIterator;

        for (int i = 0; i < this.numPages(); i++) {
            pageIterator = pageIterator(tid, i);
            //for (Tuple t : pageIterator) {
            //    tuples.add(t);
            //}
            while (pageIterator.hasNext()) {
                tuples.add(pageIterator.next());
            }
        }

        return tuples.iterator();
    }

    private Iterator<Tuple> pageIterator(TransactionId tid, int pageNumber) {
        HeapPageId pid = new HeapPageId(this.getId(), pageNumber);
        HeapPage page = (HeapPage) BufferPool.getPage(tid, pid, null);
        return page.iterator();
    }
    */

}

