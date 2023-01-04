#include <random>
#include <cmath>
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
    _temp = 0;

    _latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
    pthread_mutex_init(_latch, NULL);
}

// This function performs a dirty read or a clean read depending on the temperature
RC Row_dirty_occ::access(txn_man * txn, TsType type, row_t * local_row) {
    if (_temp >= DR_THRESHOLD && _stashed_row) {
        // Dirty read
        // Read the latest uncommitted data
        ts_t v = 0;
        ts_t v2 = 1;
        while (v2 != v) {
            v = _stashed_tid;
            while (v & LOCK_BIT) {
                PAUSE
                v = _stashed_tid;
            }
            local_row->copy(_stashed_row);
            COMPILER_BARRIER
            v2 = _stashed_tid;
        }
        txn->last_tid = v & (~LOCK_BIT);
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

// This function increments the current temperature by possibility of 1/2^(_temp)
void Row_dirty_occ::inc_temp() {
    // TODO: implement more efficient probability algorithm
    if ((double)rand() / RAND_MAX <= 1 / pow(2, _temp)) {
        uint64_t temp = _temp;
        while (!__sync_bool_compare_and_swap(&_temp, temp, temp + 1)) {
            PAUSE
            temp = _temp;
        }
    }
}

// This function performs a validation on the current row
bool Row_dirty_occ::validate(ts_t tid, bool in_write_set) {
    ts_t v = _tid;
    if (in_write_set) {
        // If the row is in the write set, then the row has already been locked
        // TODO: figure out why transactions with dirty write cannot commit
        bool ret = (tid == (v & (~LOCK_BIT)));
        if (ret == false) {
            inc_temp();
        }
        return ret;
    }
    if (v & LOCK_BIT) {
        inc_temp();
        return false;
    } else if (tid != (v & (~LOCK_BIT))) {
        inc_temp();
        return false;
    }
    return true;
}

// This function is invoked to perform a dirty write
void Row_dirty_occ::dirty_write(row_t * data, ts_t tid) {
    lock_stashed();
    if (_stashed_row) {
        // Free the previous stashed row
        mem_allocator.free(_stashed_row, sizeof(row_t));
    }
    _stashed_row = data;
    _stashed_tid = tid | LOCK_BIT;
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

void Row_dirty_occ::lock_stashed() {
    ts_t v = _stashed_tid;
    while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_stashed_tid, v, v | LOCK_BIT)) {
        PAUSE
        v = _stashed_tid;
    }
}

void Row_dirty_occ::release_stashed() {
    _stashed_tid &= (~LOCK_BIT);
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