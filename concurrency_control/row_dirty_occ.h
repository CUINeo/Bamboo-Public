#pragma once 

class table_t;
class Catalog;
class txn_man;

#if CC_ALG == DIRTY_OCC
#define LOCK_BIT (1UL << 63)

class Row_dirty_occ {
public:
    void            init(row_t * row);
    RC              access(txn_man * txn, TsType type, row_t * local_row);

    bool            validate(ts_t tid);
    void            write(row_t * data, uint64_t tid);
    void            write_to_stashed(row_t * data, uint64 tid);

    bool            is_stashed_locked();
    void            lock();
    void            lock_stashed();
    void            release();
    void            release_stashed();
    
    bool            try_lock();
    void            assert_lock() { assert(_tid & LOCK_BIT); }

    uint64_t        get_tid();

private:
    volatile ts_t   _tid;
    row_t *         _row;
};

#endif
