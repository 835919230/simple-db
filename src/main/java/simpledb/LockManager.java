package simpledb;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by HeXi on 2018/3/11.
 */
public class LockManager {

    private ConcurrentMap<PageId, TransactionId> pageIdTxIdMap = new ConcurrentHashMap<>();
    private ConcurrentMap<TransactionId, List<PageId>> txIdPageIdMap = new ConcurrentHashMap<>();

    private ConcurrentMap<PageId, ReentrantLock> exclusiveLockMap = new ConcurrentHashMap<>();

    private static final Object sharedLock = new Object();

    private HashSet<PageId> pageInSharedLock = new HashSet<>();
    private HashSet<PageId> pageInExclusiveLock = new HashSet<>();

    private ConcurrentMap<TransactionId, LinkedList<TransactionId>> waitingTxMap = new ConcurrentHashMap<>();
    private ConcurrentMap<TransactionId, LinkedList<Thread>> waitingThread = new ConcurrentHashMap<>();

    public ReentrantLock getExclusiveLock(PageId pageId) {
        ReentrantLock lock = exclusiveLockMap.get(pageId);
        if (lock == null) {
            lock = new ReentrantLock();
            exclusiveLockMap.put(pageId, lock);
        }
        return lock;
    }

    public Object getSharedLock(PageId pageId) {
        return sharedLock;
    }

    public void acquireLock(TransactionId txId, PageId pid, Permissions perm) throws TransactionAbortedException, DbException  {
        if (perm == Permissions.READ_ONLY) {
            while (!acquireSharedLock(txId, pid));
            System.out.println(Thread.currentThread());
        } else if (perm == Permissions.READ_WRITE) {
            while (!acquireExcelusiveLock(txId, pid));
            System.out.println(Thread.currentThread());
        } else {
            throw new DbException("illegal Permission.");
        }
    }

    public synchronized void releasePage(TransactionId tid, PageId pid) {
        List<PageId> pageIdList = txIdPageIdMap.get(tid);
        pageIdTxIdMap.remove(pid);
        pageInSharedLock.remove(pid);
        pageInExclusiveLock.remove(pid);
        pageIdList.remove(pid);

        wakeupWaitingThread(tid);
    }

    public synchronized void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        if (!commit)
            return;

        List<PageId> pageIdList = txIdPageIdMap.get(tid);
        for (PageId pageId : pageIdList) {
            pageIdTxIdMap.remove(pageId);
            pageInSharedLock.remove(pageId);
            pageInExclusiveLock.remove(pageId);
        }

        wakeupWaitingThread(tid);

        pageIdList.clear();
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        List<PageId> pageIds = txIdPageIdMap.get(tid);
        return pageIds != null && pageIds.contains(p);
    }

    public boolean isLockByAnotherTransaction(PageId pageId) {
        TransactionId transactionId = pageIdTxIdMap.get(pageId);
        return transactionId != null;
    }

    private boolean acquireExcelusiveLock(TransactionId tid, PageId pid) {
        Object lock = getLock(pid);
        TransactionId transactionId = pageIdTxIdMap.get(pid);
        while (true) {
            synchronized (lock) {
                transactionId = pageIdTxIdMap.get(pid);
                if ((!pageInExclusiveLock.contains(pid) && !pageInSharedLock.contains(pid))
                        || (transactionId != null && transactionId.equals(tid))) {
                    synchronized (LockManager.class) {
                        pageInExclusiveLock.add(pid);
                    }
                    pageIdTxIdMap.put(pid, tid);
                    List<PageId> pageIdList = getPageIdList(tid);
                    pageIdList.add(pid);

                    if (transactionId != null) {
                        LinkedList<TransactionId> waitingTxIdList = waitingTxMap.get(transactionId);
                        if (waitingTxIdList != null)
                            waitingTxIdList.remove(tid);
                    }
                    return true;
                }
                addWaitingTransaction(transactionId, tid);
                addWaitingThread(transactionId);
                LockSupport.park();
            }
        }
    }

    private List<PageId> getPageIdList(TransactionId tid) {
        List<PageId> pageIdList = txIdPageIdMap.get(tid);
        if (pageIdList == null) {
            pageIdList = new LinkedList<>();
            txIdPageIdMap.put(tid, pageIdList);
        }
        return pageIdList;
    }
    private boolean acquireSharedLock(TransactionId tid, PageId pid) {
        Object lock = getLock(pid);
        TransactionId transactionId = pageIdTxIdMap.get(pid);
        while (true) {
            synchronized (lock) {
                transactionId = pageIdTxIdMap.get(pid);
                if (!pageInExclusiveLock.contains(pid)) {
                    synchronized (LockManager.class) {
                        pageInSharedLock.add(pid);
                    }
                    pageIdTxIdMap.put(pid, tid);
                    List<PageId> pageIdList = getPageIdList(tid);
                    pageIdList.add(pid);

                    if (transactionId != null) {
                        LinkedList<TransactionId> waitingTxIdList = waitingTxMap.get(transactionId);
                        if (waitingTxIdList != null)
                            waitingTxIdList.remove(tid);
                    }
                    return true;
                }
                addWaitingTransaction(transactionId, tid);
                addWaitingThread(transactionId);
                LockSupport.park();
            }
        }
    }

    private ConcurrentMap<PageId, Object> lockMap = new ConcurrentHashMap<>();

    private Object getLock(PageId pageId) {
        Object lock = lockMap.get(pageId);
        if (lock == null) {
            lock = new Object();
            lockMap.put(pageId, lock);
        }
        return lock;
    }

    private void wakeupWaitingThread(TransactionId tid) {
        LinkedList<Thread> threads = waitingThread.get(tid);
        while (threads != null && threads.size() > 0) {
            Thread first = threads.removeFirst();
            waitingTxMap.get(tid).removeFirst();
            System.out.println("thread name: "+first.getName()+" ,begin to wake up");
            LockSupport.unpark(first);
        }
    }

    private void recordPageIdAndTxId(TransactionId txId, PageId pageId, Permissions perm)
            throws TransactionAbortedException, DbException {
        boolean done = false;
        while (!done) {
            checkDeadLock(txId, pageId, perm);
            TransactionId transactionId = pageIdTxIdMap.get(pageId);
            // 是不同的事务 而且 已经锁住的事务至少有读的权限， 然后当前事务又要求有写权限的，阻塞
            if (transactionId != null && !transactionId.equals(txId)
                    && hasReadPermission(transactionId, pageId)
                    && perm == Permissions.READ_WRITE) {
                // locked by another transaction
                synchronized (LockManager.class) {
                    addWaitingThread(transactionId);

                    addWaitingTransaction(transactionId, txId);
                }
                System.out.println("thread name: "+Thread.currentThread().getName()+" ,tid: "+txId+" ,begin to block");
                LockSupport.park();
                done = false;
            } else {
                done = acquireLockAgain(txId, pageId, perm);
            }
        }
    }

    private synchronized void checkDeadLock(TransactionId curTid, PageId pageId, Permissions perm) throws TransactionAbortedException {
        TransactionId transactionId = pageIdTxIdMap.get(pageId);
        if (transactionId == null || transactionId.equals(curTid) || perm == Permissions.READ_ONLY)
            return;
        addWaitingTransaction(transactionId, curTid);
        if (innerCheckDeadLock(transactionId))
            throw new TransactionAbortedException();
        // if no exceptions threw out, it will be safe
    }

    private synchronized boolean innerCheckDeadLock(TransactionId transactionId) throws TransactionAbortedException {
        boolean rel = false;
        if (transactionId == null)
            return false;
        LinkedList<TransactionId> tidLinkedList = waitingTxMap.get(transactionId);
        if (tidLinkedList == null)
            return false;
        LinkedList<TransactionId> queue = new LinkedList<>();
        queue.addAll(tidLinkedList);
        // bfs
        while (queue.size() > 0) {
            TransactionId first = queue.removeFirst();
            if (transactionId.equals(first))
                return true;
            LinkedList<TransactionId> linkedList = waitingTxMap.get(first);
            if (linkedList != null) {
                queue.addAll(linkedList);
            }
        }
        return false;
    }

    private synchronized void addWaitingThread(TransactionId transactionId) {
        LinkedList<Thread> waitingThreadList = waitingThread.get(transactionId);
        if (waitingThreadList == null) {
            waitingThreadList = new LinkedList<>();
            waitingThread.put(transactionId, waitingThreadList);
        }
        if (!waitingThreadList.contains(Thread.currentThread()))
            waitingThread.get(transactionId).add(Thread.currentThread());
    }

    private synchronized void addWaitingTransaction(TransactionId transactionId, TransactionId waitingTxId) {
        LinkedList<TransactionId> waitingTxIdList = waitingTxMap.get(transactionId);
        if (waitingTxIdList == null) {
            waitingTxIdList = new LinkedList<>();
            waitingTxMap.put(transactionId, waitingTxIdList);
        }
        if (!waitingTxIdList.contains(waitingTxId)) {
            System.out.println("transactionId: "+transactionId+", add waitingTid: "+waitingTxId);
            waitingTxMap.get(transactionId).add(waitingTxId);
        }
    }

    private boolean acquireLockAgain(TransactionId txId, PageId pageId, Permissions perm) {
        boolean done = false;
        // double check lock to ensure data correctness
        // 数据页没有被其他事务锁住，或者被同一个事务锁住，或者是要求读锁的
        if (pageIdTxIdMap.get(pageId) == null || txId.equals(pageIdTxIdMap.get(pageId)) || perm == Permissions.READ_ONLY ) {
            System.out.println("current thread: "+Thread.currentThread().getName());
            synchronized (LockManager.class) {
                // 数据页没有被其他事务锁住，或者被同一个事务锁住，或者是要求读锁的
                if (pageIdTxIdMap.get(pageId) == null || txId.equals(pageIdTxIdMap.get(pageId)) || perm == Permissions.READ_ONLY) {
//                    if (!hasReadPermission(txId, pageId) && !hasWritePermission(txId, pageId)) {
//                        System.out.println("thread currentThread: "+Thread.currentThread().getName()+" has no permissions");
//                        return false;
//                    }
                    if (perm == Permissions.READ_WRITE) {
                        pageInExclusiveLock.add(pageId);
                    }
                    // sharedLock肯定要加
                    pageInSharedLock.add(pageId);
                    pageIdTxIdMap.put(pageId, txId);
                    List<PageId> pageIdList = txIdPageIdMap.get(txId);
                    if (pageIdList == null) {
                        pageIdList = new LinkedList<>();
                        txIdPageIdMap.put(txId, pageIdList);
                    }
                    pageIdList.add(pageId);
                    System.out.println("current thread: "+Thread.currentThread().getName()+" succeed in lock race, transctionId: "+txId);
                    done = true;
                }
            }
        }
        return done;
    }

    private boolean hasReadPermission(TransactionId txId, PageId pageId) {
        return hasWritePermission(txId, pageId) || !pageInExclusiveLock.contains(pageId);
    }

    private boolean hasWritePermission(TransactionId txId, PageId pageId) {
        if (!pageInExclusiveLock.contains(pageId))
            return true;
        TransactionId transactionId = pageIdTxIdMap.get(pageId);
        return transactionId != null && transactionId.equals(txId);
    }

}
