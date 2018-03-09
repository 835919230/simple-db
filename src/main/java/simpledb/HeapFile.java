package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private File heapFile;

    private TupleDesc tupleDesc;

    private int pageNumbers;

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        heapFile = f;
        tupleDesc = td;

        pageNumbers = numPages();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int[] pageInfo = pid.serialize();
        try {
            RandomAccessFile raf = new RandomAccessFile(heapFile, "rw");
            int eachPageSize = BufferPool.getPageSize();
            long pos = 0; // start of the file
            long fileLength = raf.length();
            int pageNo = 0;
            while (pos < fileLength) {
                raf.seek(pos);
                byte[] data = HeapPage.createEmptyPageData();
                int nRead = raf.read(data);
                if (nRead <= -1) {
                    System.err.println("reading problem...read data is under zero, please check");
                    pos += (long)eachPageSize;
                    continue;
                }
                if (pageNo == pageInfo[1] && getId() == pageInfo[0]) {
                    // find the correct page and then return this page
//                    System.out.printf("page with pageId: %s, %s is found.", getId(), pageNo);
//                    System.out.println();
                    return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
                }
                pos += (long)eachPageSize;
                pageNo++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("file not exists in this file");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("system ioe exception!");
        }
        throw new IllegalArgumentException("please check that page with pageid: "+pid+" exists");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo = page.getId().getPageNumber();
        RandomAccessFile raf = new RandomAccessFile(heapFile, "rw");
        raf.seek(pageNo*BufferPool.getPageSize());
        raf.write(page.getPageData());

        raf.close();
        pageNumbers = numPages();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (heapFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // acquire a lock
        ArrayList<Page> dirtyPages = new ArrayList<>();
        readWriteLock.writeLock().lock();

        int N = numPages();
        boolean alreadyInsert = false;
        for (int i = 0; i < N; i++) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() <= 0) {
                continue;
            }
            page.insertTuple(t);
                // no need to write this page at once, leave the mass to BufferPool
            alreadyInsert = true;
            dirtyPages.add(page);
            // write success, break the loop
            break;
        }

        if (!alreadyInsert) {
            HeapPageId heapPageId = new HeapPageId(getId(), N);
            HeapPage page = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            page.insertTuple(t);
            // new page should write at once
            writePage(page);
            dirtyPages.add(page);
        }
        //release write lock
        readWriteLock.writeLock().unlock();
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyPages = new ArrayList<>();
        readWriteLock.writeLock().lock();
        RecordId recordId = t.getRecordId();
        if (recordId != null) {
            PageId pageId = recordId.getPageId();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            page.deleteTuple(t);
            dirtyPages.add(page);
        }
        readWriteLock.writeLock().unlock();
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(tid);
    }

    public class HeapFileIterator extends AbstractDbFileIterator {
        /**
         * index for page iterator
         */
        private int pageIndex;

        private Iterator<Tuple> itr;

        private TransactionId transactionId;

        private int tableId;

        private boolean opened;

        public HeapFileIterator(TransactionId id)  {
            transactionId = id;
            tableId = HeapFile.this.getId();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapFile.this.pageNumbers = numPages();
            if (isPageOver())
                return;
            PageId pageId = new HeapPageId(tableId, pageIndex);
            Page page = Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);

            HeapPage heapPage = (HeapPage) page;
            itr = heapPage.iterator();

            setOpened(true);
        }

        private void setOpened(boolean opened) {
            this.opened = opened;
        }

        private boolean isOpened() {
            return opened;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            pageIndex = 0;
            open();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (isPageOver())
                return null;
            if (!itr.hasNext()) {
                increasePageIndex();
                open();
                return readNext();
            }
            Tuple next = itr.next();
            if (next == null)
                increasePageIndex();
            return next;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return readNext();
        }

        protected int increasePageIndex() {
            return ++pageIndex;
        }

        protected boolean isPageOver() {
            return pageIndex >= numPages();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpened() || isPageOver())
                return false;

            while (!itr.hasNext() && !isPageOver()) {
                increasePageIndex();
                open();
            }
            return !isPageOver() && itr!=null&& itr.hasNext();
        }
    }

}

