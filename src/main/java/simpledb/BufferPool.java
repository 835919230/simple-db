package simpledb;

import java.io.*;
import java.util.*;
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

    private final LinkedHashMap<PageId, Page> pageMap;

    private final LockManagerImpl lockManager;

    private final int maxPageNum;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pageMap = new LinkedHashMap<>(numPages, 1.1F, false);
        lockManager = LockManagerImpl.create();
        maxPageNum = numPages;
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
        lockManager.acquireLock(tid, pid, perm);
        Page page = null;
            page = pageMap.get(pid);
            if (page == null) {
                page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                if (page == null) {
                    System.out.println("no page with pid: " + pid);
                    throw new DbException("no page with pid: " + pid);
                }
                // lock to ensure thread safe
                synchronized (BufferPool.class) {
                    if (pageMap.size() >= maxPageNum) {
                        evictPage();
                    }
                    pageMap.put(pid, page);
                }
            }
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
        try {
            flushPage(pid);
            lockManager.releasePage(tid, pid);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
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
        if (commit)
            flushPages(tid);
        lockManager.releasePages(tid);
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
                synchronized (BufferPool.class) {
                    pageMap.put(dirtyPage.getId(), dirtyPage);
                }
            }
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
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        if (recordId!=null) {
                int tableId = recordId.getPageId().getTableId();
                System.out.println("buffer pool delete tuple: " + tableId + ", " + recordId.getTupleNumber());
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);

                ArrayList<Page> dirtyPages = databaseFile.deleteTuple(tid, t);
                synchronized (BufferPool.class) {
                    for (Page dirtyPage : dirtyPages) {
                        pageMap.put(dirtyPage.getId(), dirtyPage);
                    }
                }
                return;
        }
        // if recordId doesn't exist, loop all the cache table ids
        throw new TransactionAbortedException();
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        flushPages(null);
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
        if (page != null && page.isDirty() != null) {
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
        System.out.println(tid);
        Set<Map.Entry<PageId, Page>> entries = pageMap.entrySet();
        for (Map.Entry<PageId, Page> entry : entries) {
            Page page = entry.getValue();
            if (page.isDirty() != null && page.isDirty().equals(tid)) {
                int tableId = page.getId().getTableId();
                DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableId);
                databaseFile.writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the oldest page that isn't lock by another transaction to disk to ensure no steal.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // remove the longest existing page and not lock by another transaction
        boolean allDirty = true;
        Iterator<Map.Entry<PageId, Page>> it = pageMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<PageId, Page> next = it.next();
            PageId pageId = next.getKey();
            if (next.getValue().isDirty() != null) {
                continue;
            }
                try {
                    flushPage(pageId);
                    allDirty = false;
                } catch (IOException e) {
                    throw new DbException("can't flush page with pid: " + pageId);
                }
                pageMap.remove(pageId);
                break;
        }
        if (!allDirty) {
//            throw new DbException("can't evictPage because all pages are locked");
            it = pageMap.entrySet().iterator();
            Map.Entry<PageId, Page> next = it.next();
            try {
                flushPage(next.getKey());
                pageMap.remove(next.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
