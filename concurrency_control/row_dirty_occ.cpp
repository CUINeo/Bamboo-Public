#include "txn.h"
#include "row.h"
#include "row_dirty_occ.h"
#include "mem_alloc.h"

#if CC_ALG == DIRTY_OCC

// This function initializes the data structure
void Row_dirty_occ::init(row_t * row) {
    _tid = 0;
    _stashed_tid = 0;
    _row = row;
    _stashed_row = NULL;
    _latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
    pthread_mutex_init(_latch, NULL);
    _temp = 0;
}

// This function performs a dirty read or a clean read depending on the temperature
RC Row_dirty_occ::access(txn_man * txn, TsType type, row_t * local_row) {
    if (_temp >= DR_THRESHOLD && _stashed_row) {
        // Dirty read
        // Read the latest uncommitted data
        latch_stashed();
        local_row->copy(_stashed_row);
        txn->last_tid = _stashed_tid;
        release_stashed();
    } else {
        // Clean read
        // Read the latest committed data
        ts_t v = 0;
        ts_t v2 = 1;
        while (v2 != v) {
            v = _tid;
            while (v & LOCK_BIT) {
                PAUSE
                v = _tid;
            }
            local_row->copy(_row);
            COMPILER_BARRIER
            v2 = _tid;
        }
        txn->last_tid = v & (~LOCK_BIT);
    }
    return RCOK;
}

// This function performs a validation on the current row
bool Row_dirty_occ::validate(ts_t tid, bool in_write_set) {
    ts_t v = _tid;
    if (in_write_set) {
        // If the row is in the write set, then the row has already been locked
        bool ret = (tid == (v & (~LOCK_BIT)));
        if (ret == false) {
            _temp += 1;
        }
        return ret;
    }
    if (v & LOCK_BIT) {
        _temp += 1;
        return false;
    } else if (tid != (v & (~LOCK_BIT))) {
        _temp += 1;
        return false;
    } else {
        _temp -= 1;
        return true;
    }
}

// This function is invoked to perform a dirty write
void Row_dirty_occ::dirty_write(row_t * data, ts_t tid) {
    latch_stashed();
    _stashed_row->copy(data);
    _stashed_tid = tid;
    release_stashed();
}

// This function is invoked to update rows of successfully committed transactions
void Row_dirty_occ::write(row_t * data, ts_t tid) {
    _row->copy(data);
    ts_t v = _tid;
	M_ASSERT(v & LOCK_BIT, "tid=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n",
            tid, (v & LOCK_BIT), (v & (~LOCK_BIT)));
    _tid = (tid | LOCK_BIT);
}

void Row_dirty_occ::lock() {
    ts_t v = _tid;
    while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid, v, v | LOCK_BIT)) {
		PAUSE
		v = _tid;
	}
}

void Row_dirty_occ::release() {
    assert_lock();
    _tid &= (~LOCK_BIT);
}

void Row_dirty_occ::latch_stashed() {
    pthread_mutex_lock(_latch);
}

void Row_dirty_occ::release_stashed() {
    pthread_mutex_unlock(_latch);
}

bool Row_dirty_occ::try_lock() {
    ts_t v = _tid;
    if (v & LOCK_BIT) {
        return false;
    }
    return __sync_bool_compare_and_swap(&_tid, v, (v | LOCK_BIT));
}

ts_t Row_dirty_occ::get_tid() {
    return _tid & (~LOCK_BIT);
}

#endif