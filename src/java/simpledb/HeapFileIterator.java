package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId tid;
    private HeapFile file;
    private ArrayList<Tuple> tuples;
    private int index;
    private boolean open;

    public HeapFileIterator(TransactionId tid, HeapFile f) {
        // DON'T THROW EXCEPTIONS IN CONSTRUCTOR IT MAKES EVERYTHING SCARY AND BAD
        this.tid = tid;
        this.file = f;
        tuples = new ArrayList<Tuple>();
        index = 0;
        open = false;
    }

    private void initTuples() throws DbException, TransactionAbortedException {
        Iterator<Tuple> iter;
        HeapPageId pid;
        HeapPage page;

        for (int i = 0; i < file.numPages(); i++) {
            pid = new HeapPageId(file.getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            iter = page.iterator();

            while (iter.hasNext()) {
                tuples.add(iter.next());
            }
        }
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() throws DbException, TransactionAbortedException {
        initTuples();
        index = 0;
        open = true;
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return (open && (index < tuples.size()));
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            Tuple tup = tuples.get(index);
            index++;
            return tuples.get(index);
        }
        else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        open = false;
    }
} 