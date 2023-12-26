package tech.bsdb.io;

import java.io.Closeable;
import java.io.IOException;

public class Native {

    static {
            NativeUtils.loadLibraryFromJar(System.mapLibraryName("bsdbjni"));
    }
    public static class Uring implements Closeable {
        // holder of pointer of io_uring instance
        private long _ring;

        static {
            NativeUtils.loadLibraryFromJar(System.mapLibraryName("bsdbjni"));
            initIDs();
        }

        public Uring(int queueDepth, long flags){
            int ret = initUring(queueDepth, flags);
            if (ret < 0) {
                throw new RuntimeException("initialize Uring failed with ret :" + ret);
            }
        }

        private native static final void initIDs();

        // int io_uring_queue_init(unsigned entries, struct io_uring *ring, unsigned flags)
        private native  int initUring(int queueDepth, long flags);

        //void io_uring_queue_exit(struct io_uring *ring)
        public native void exitUring();

        @Override
        public void close() throws IOException {
            exitUring();
        }


        /**
         * -------------- REGISTER --------------------
         **/
        // extern int io_uring_register_buffers(struct io_uring *ring, const struct iovec *iovecs, unsigned nr_iovecs)
        // extern int io_uring_unregister_buffers(struct io_uring *ring)
        // extern int io_uring_register_files(struct io_uring *ring, const int *files, unsigned nr_files);
        // extern int io_uring_unregister_files(struct io_uring *ring);
        public native  int registerBuffers(long[] buffers, long[] bufferLengths); // 0 success

        public native  int unregisterBuffers(); // 0 success

        public native  int registerFiles(int[] fds);

        public native  int unregisterFiles();

        public native  int availableSQ();

        /**
         * ------------- PREPARE READ/WRITE -----------
         **/
        // static inline void io_uring_prep_read(struct io_uring_sqe *sqe, int fd, void *buf, unsigned nbytes, off_t offset)
        // static inline void io_uring_prep_readv(struct io_uring_sqe *sqe, int fd, const struct iovec *iovecs, unsigned nr_vecs, off_t offset)
        // static inline void io_uring_prep_read_fixed(struct io_uring_sqe *sqe, int fd, void *buf, unsigned nbytes, off_t offset, int buf_index)

        // static inline void io_uring_prep_write(struct io_uring_sqe *sqe, int fd, void *buf, unsigned nbytes, off_t offset)
        // static inline void io_uring_prep_writev(struct io_uring_sqe *sqe, int fd, const struct iovec *iovecs, unsigned nr_vecs, off_t offset)
        // static inline void io_uring_prep_write_fixed(struct io_uring_sqe *sqe, int fd, const void *buf, unsigned nbytes, off_t offset, int buf_index)
        //
        // static inline void io_uring_prep_fsync(struct io_uring_sqe *sqe, int fd, unsigned fsync_flags)
        public native  int prepareRead(long reqId, long flags, int fd, long buf, int bytes, long offset);

        public native  int prepareReadM(long[] reqId, long flags, int fd, long[] buf, int[] bytes, long[] offset, int from, int to);

        public native  int prepareReads(long reqId, long flags, int fd, long[] buf, int[] bytes, long offset);

        public native  int prepareReadFixed(long reqId, long flags, int fd, long buf, int bytes, long offset, int bufIndex);

        public native  int prepareWrite(long reqId, long flags, int fd, long buf, int bytes, long offset);

        public native  int prepareWrites(long reqId, long flags, int fd, long[] buf, int[] bytes, long offset);

        public native  int prepareWriteFixed(long reqId, long flags, int fd, long buf, int bytes, long offset, int bufIndex);

        public native  int prepareFsync(long reqId, long flags, int fd, long syncFlags);

        /**
         * ------------------------ REQUEST / RESPONSE ------------------
         **/
        // extern struct io_uring_sqe *io_uring_get_sqe(struct io_uring *ring);
        // static inline void io_uring_sqe_set_flags(struct io_uring_sqe *sqe, unsigned flags)
        // static inline void io_uring_sqe_set_data(struct io_uring_sqe *sqe, void *data)
        //
        // static inline void *io_uring_cqe_get_data(struct io_uring_cqe *cqe)
        //
        // extern int io_uring_submit(struct io_uring *ring);
        // extern int io_uring_submit_and_wait(struct io_uring *ring, unsigned wait_nr);
        //
        // extern int io_uring_wait_cqe_timeout(struct io_uring *ring, struct io_uring_cqe **cqe_ptr, struct __kernel_timespec *ts);
        // static inline int io_uring_wait_cqe(struct io_uring *ring, struct io_uring_cqe **cqe_ptr)
        // static inline int io_uring_wait_cqe_nr(struct io_uring *ring, struct io_uring_cqe **cqe_ptr, unsigned wait_nr)
        //
        // unsigned io_uring_peek_batch_cqe(struct io_uring *ring, struct io_uring_cqe **cqes, unsigned count);
        // static inline int io_uring_peek_cqe(struct io_uring *ring, struct io_uring_cqe **cqe_ptr)
        //
        // static inline void io_uring_cqe_seen(struct io_uring *ring, struct io_uring_cqe *cqe)
        // static inline void io_uring_cq_advance(struct io_uring *ring, unsigned nr)
        public native  int submit();

        public native  int submitAndWait(int waitNr);

        public native  int waitCQEntryTimeout(long[] reqIds, long[] retCodes, long millis);

        public native  int waitCQEntries(long[] reqIds, long[] retCodes, int waitNr);

        public native  int peekCQEntries(long[] reqIds, long[] retCodes, int count);

        public native  void advanceCQ(int nr);

        public static class Flags {
            //#define IORING_SETUP_IOPOLL     (1U << 0)       /* io_context is polled */
            //#define IORING_SETUP_SQPOLL     (1U << 1)       /* SQ poll thread */
            //#define IORING_SETUP_SQ_AFF     (1U << 2)       /* sq_thread_cpu is valid */
            //#define IORING_SETUP_CQSIZE     (1U << 3)       /* app defines CQ size */
            //#define IORING_SETUP_CLAMP      (1U << 4)       /* clamp SQ/CQ ring sizes */
            //#define IORING_SETUP_ATTACH_WQ  (1U << 5)       /* attach to existing wq */
            public static final int IOPOLL = (1 << 0);
            public static final int SQPOLL = (1 << 1);
            public static final int SQ_AFF = (1 << 2);
            public static final int CQSIZE = (1 << 3);
            public static final int CLAMP = (1 << 4);
            public static final int ATTACH_WQ = (1 << 5);
        }
    }

    /** file open and buffer allocate **/
    public native static int open(String fileName, int flags);
    public native static long allocateAligned(int size, int align);
    public native static void free(long address);

    public native static long pread(int fd, long position, long bufAddr, int size);
    public native static int close(int fd);


    /** flags for open **/
    public static final int O_RDONLY = 00;
    public static final int O_WRONLY = 01;
    public static final int O_RDWR = 02;
    //	If the file does not exist it will be created. The owner (user ID) of the file is set to the effective user ID of the process. The group ownership (group ID) is set either to the effective group ID of the process or to the group ID of the parent directory (depending on filesystem type and mount options, and the mode of the parent directory, see, e.g., the mount options bsdgroups and sysvgroups of the ext2 filesystem, as described in mount(8)).
    public static final int O_CREAT = 0100;
    //If the file already exists and is a regular file and the open mode allows writing (i.e., is O_RDWR or O_WRONLY) it will be truncated to length 0. If the file is a FIFO or terminal device file, the O_TRUNC flag is ignored. Otherwise the effect of O_TRUNC is unspecified.
    public static final int O_TRUNC = 01000;
    //	Try to minimize cache effects of the I/O to and from this file. In general this will degrade performance, but it is useful in special situations, such as when applications do their own caching. File I/O is done directly to/from user space buffers. The I/O is synchronous, i.e., at the completion of a read(2) or write(2), data is guaranteed to have been transferred. Under Linux 2.4 transfer sizes, and the alignment of user buffer and file offset must all be multiples of the logical block size of the file system. Under Linux 2.6 alignment must fit the block size of the device.
    //A semantically similar (but deprecated) interface for block devices is described in raw(8).
    public static final int O_DIRECT = 040000;
    //The file is opened for synchronous I/O. Any write()s on the resulting file descriptor will block the calling process until the data has been physically written to the underlying hardware. But see RESTRICTIONS below.
    public static final int O_SYNC = 04000000;


    //	The file is opened in append mode. Before each write(), the file offset is positioned at the end of the file, as if with lseek(). O_APPEND may lead to corrupted files on NFS file systems if more than one process appends data to a file at once. This is because NFS does not support appending to a file, so the client kernel has to simulate it, which can’t be done without a race condition.
    public static final int O_APPEND = 02000;
    //Enable signal-driven I/O: generate a signal (SIGIO by default, but this can be changed via fcntl(2)) when input or output becomes possible on this file descriptor. This feature is only available for terminals, pseudo-terminals, sockets, and (since Linux 2.6) pipes and FIFOs. See fcntl(2) for further details.
    public static final int O_ASYNC	= 020000;
    public static final int O_CLOEXEC = 02000000;
    //	If pathname is not a directory, cause the open to fail. This flag is Linux-specific, and was added in kernel version 2.1.126, to avoid denial-of-service problems if opendir(3) is called on a FIFO or tape device, but should not be used outside of the implementation of opendir.
    public static final int O_DIRECTORY	= 0200000;
    public static final int O_DSYNC	= 010000;
    //	When used with O_CREAT, if the file already exists it is an error and the open() will fail. In this context, a symbolic link exists, regardless of where it points to. O_EXCL is broken on NFS file systems; programs which rely on it for performing locking tasks will contain a race condition. The solution for performing atomic file locking using a lockfile is to create a unique file on the same file system (e.g., incorporating hostname and pid), use link(2) to make a link to the lockfile. If link() returns 0, the lock is successful. Otherwise, use stat(2) on the unique file to check if its link count has increased to 2, in which case the lock is also successful.
    public static final int O_EXCL = 0200;
    //	(LFS) Allow files whose sizes cannot be represented in an off_t (but can be represented in an off64_t) to be opened.
    public static final int O_LARGEFILE =	0;
    //Since Linux 2.6.8) Do not update the file last access time (st_atime in the inode) when the file is read(2). This flag is intended for use by indexing or backup programs, where its use can significantly reduce the amount of disk activity. This flag may not be effective on all filesystems. One example is NFS, where the server maintains the access time.
    public static final int O_NOATIME = 01000000;
    //If pathname refers to a terminal device — see tty(4) — it will not become the process’s controlling terminal even if the process does not have one.
    public static final int O_NOCTTY = 0400;
    //	If pathname is a symbolic link, then the open fails. This is a FreeBSD extension, which was added to Linux in version 2.1.126. Symbolic links in earlier components of the pathname will still be followed.
    public static final int O_NOFOLLOW = 0400000;
    //	When possible, the file is opened in non-blocking mode. Neither the open() nor any subsequent operations on the file descriptor which is returned will cause the calling process to wait. For the handling of FIFOs (named pipes), see also fifo(7). For a discussion of the effect of O_NONBLOCK in conjunction with mandatory file locks and with file leases, see fcntl(2).
    public static final int O_NONBLOCK = 04000;
    public static final int O_PATH = 010000000;
    public static final int O_TMPFILE = (020000000 | 0200000);


    public static final int S_IRWXU = 00700;// user (file owner) has read, write and execute permission
    public static final int S_IRUSR = 00400;// user has read permission
    public static final int S_IWUSR = 00200;// user has write permission
    public static final int S_IXUSR = 00100;// user has execute permission
    public static final int S_IRWXG = 00070;// group has read, write and execute permission
    public static final int S_IRGRP = 00040;// group has read permission
    public static final int S_IWGRP = 00020;// group has write permission
    public static final int S_IXGRP = 00010;// group has execute permission
    public static final int S_IRWXO = 00007;// others have read, write and execute permission
    public static final int S_IROTH = 00004;// others have read permission
    public static final int S_IWOTH = 00002;// others have write permission
    public static final int S_IXOTH = 00001;// others have execute permission
}
