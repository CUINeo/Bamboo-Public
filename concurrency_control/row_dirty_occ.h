#pragma once 

class table_t;
class Catalog;
class txn_man;

#if CC_ALG == DIRTY_OCC

#define LOCK_BIT (1UL << 63)

class Row_dirty_occ {
public:
    void                init(row_t * row);

    RC                  access(txn_man * txn, TsType type, row_t * local_row);
    void                inc_temp();
    bool                validate(ts_t tid, bool in_write_set);
    void                dirty_write(row_t * data, ts_t tid);
    void                write(row_t * data, ts_t tid);

    bool                is_hotspot() { return _temp >= DR_THRESHOLD; }

    void                lock();
    void                release();
    void                lock_stashed();
    void                release_stashed();
    
    bool                try_lock();
    void                assert_lock() { assert(_tid & LOCK_BIT); }

    ts_t                get_tid();

private:
    volatile ts_t       _tid;
    volatile ts_t       _stashed_tid;
    row_t *             _row;
    row_t *             _stashed_row;
    uint64_t            _temp;
    pthread_mutex_t *   _latch;
};

#endif
