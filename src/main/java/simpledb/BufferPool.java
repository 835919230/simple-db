package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private LinkedHashMap<PageId, Page> pageMap;

    private static final Lock lock = new ReentrantLock();

    private final AtomicInteger atomicNumPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pageMap = new LinkedHashMap<>(DEFAULT_PAGES, 1.1F, false);
        atomicNumPages = new AtomicInteger(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Page page = pageMap.get(pid);
        if (page == null) {
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            if (page == null) {
                System.out.println("no page with pid: " + pid);
                throw new DbException("no page with pid: " + pid);
            }
            // lock to ensure thread safe
            synchronized (BufferPool.class) {
                if (pageMap.size() >= DEFAULT_PAGES) {
                    evictPage();
                }
                pageMap.put(pid, page);
            }
        }
        // TODO: 2018/2/17 to finish the logic of reading page
        // TODO: 2018/2/17 facing problem: what's the usage of @param tid, the Permissions seem to be useless

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);

        ArrayList<Page> dirtyPages = databaseFile.insertTuple(tid, t);
        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            pageMap.put(dirtyPage.getId(), dirtyPage);
        }
//        flushAllPages();
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        if (recordId!=null) {
            int tableId = recordId.getPageId().getTableId();
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
            synchronized (BufferPool.class) {
                ArrayList<Page> dirtyPages = databaseFile.deleteTuple(tid, t);
                for (Page dirtyPage : dirtyPages) {
                    pageMap.put(dirtyPage.getId(), dirtyPage);
                }
            }
            return;
        }
        // if recordId doesn't exist, loop all the cache table ids
        Iterator<Integer> integerIterator = Database.getCatalog().tableIdIterator();
        while (integerIterator.hasNext()) {
            Integer tableId = integerIterator.next();
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
            synchronized (BufferPool.class) {
                ArrayList<Page> dirtyPages = databaseFile.deleteTuple(tid, t);
                for (Page dirtyPage : dirtyPages) {
                    pageMap.put(dirtyPage.getId(), dirtyPage);
                }
            }

        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        Set<Map.Entry<PageId, Page>> entries = pageMap.entrySet();
        for (Map.Entry<PageId, Page> entry : entries) {
            Page page = entry.getValue();
            if (page.isDirty() != null) {
                int tableId = page.getId().getTableId();
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
                databaseFile.writePage(page);
                page.markDirty(false, null);
            }
        }
        pageMap.clear();
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        Page page = pageMap.get(pid);
        if (page.isDirty() != null) {
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            try {
                databaseFile.writePage(page);
                page.markDirty(false, null);
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (!pageMap.containsKey(pid))
            return;
        Page page = pageMap.get(pid);
        int tableId = page.getId().getTableId();
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
        databaseFile.writePage(page);
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // remove the longest existing page
        Map.Entry<PageId, Page> entry = pageMap.entrySet().iterator().next();
        pageMap.remove(entry.getKey());
    }

}
