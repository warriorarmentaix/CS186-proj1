package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId tid;
    private HeapFile file;
    
    private boolean open;
    private int currentPage;
    private Iterator<Tuple> currentPageIter;


    public HeapFileIterator(TransactionId tid, HeapFile f) {
        // DON'T LET CONSTRUCTOR DO ANYTHING THAT THROWS EXCEPTION OR MUCH SCARY
        this.tid = tid;
        this.file = f;
    }

    private void setCurrentIterator() throws DbException, TransactionAbortedException {
        //HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(file.getId(), currentPage), null);
        HeapPageId pid = new HeapPageId(file.getId(), currentPage);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
        currentPageIter = page.iterator();
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() throws DbException, TransactionAbortedException {
        open = true;
        currentPage = 0;
        setCurrentIterator();
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (open) {
            if (currentPageIter.hasNext()) {
                return true;
            }
            else if (currentPage + 1 < file.numPages()) {
                currentPage++;
                setCurrentIterator();
                if (currentPageIter.hasNext()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!open) {
            throw new NoSuchElementException();
        }
        if (hasNext()) {
            return currentPageIter.next();
        }
        throw new NoSuchElementException();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        open = false;
        currentPageIter = null;
    }
} 