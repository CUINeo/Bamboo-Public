#include "txn.h"
#include "row.h"
#include "row_dirty_occ.h"

#if CC_ALG == DIRTY_OCC

// This function appends txn to the end of dep_txns
void txn_man::register_dep(txn_man * txn) {
    Dependent * new_dep_txn = new Dependent();
    new_dep_txn->_txn = txn;
    new_dep_txn->_next = NULL;

    // Use latch to protect the linked list
    pthread_mutex_lock(dep_latch);
    if (dep_txns == NULL) {
        dep_txns = new_dep_txn;
    } else {
        Dependent * ptr = dep_txns;
        while (ptr->_next) {
            ptr = ptr->_next;
        }
        ptr->_next = new_dep_txn;
    }
    pthread_mutex_unlock(dep_latch);
}

RC txn_man::validate_dirty_occ() {
    RC rc = RCOK;

    // Wait for all dependent transactions to commit or abort first
    do {
        if (aborted) {
            // Clear all dirty writes
            for (int rid = 0; rid < row_cnt; rid++) {
                Access * access = accesses[rid];
                if (access->type == WR && access->orig_row->manager->is_hotspot()) {
                    access->orig_row->manager->clear_stashed(txn_id);
                }
            }
            // Notify all dependents to abort
            Dependent * ptr = dep_txns;
            while (ptr) {
                ptr->_txn->aborted = true;
                ptr = ptr->_next;
            }
            cleanup(Abort);
            return Abort;
        }
        PAUSE
    } while (dep_cnt > 0);

    // Get the write set and the read set
    int write_set[wr_cnt];
    int cur_wr_idx = 0;
    int read_set[row_cnt - wr_cnt];
    int cur_rd_idx = 0;
    for (int rid = 0; rid < row_cnt; rid++) {
        if (accesses[rid]->type == WR)
            write_set[cur_wr_idx++] = rid;
        else
            read_set[cur_rd_idx++] = rid;
    }

    // Bubble sort the write set to be in the primary key order
    for (int i = 0; i < wr_cnt - 1; i++) {
        for (int j = 0; j < wr_cnt - i - 1; j++) {
            if (accesses[write_set[j]]->orig_row->get_primary_key() >
                accesses[write_set[j + 1]]->orig_row->get_primary_key()) {
                int tmp = write_set[j];
                write_set[j] = write_set[j + 1];
                write_set[j + 1] = tmp;
            }
        }
    }

	// Lock tuples in the lock set in the primary key order
    int num_locks = 0;
    bool done = false;
    if (_validation_no_wait) {
        while (!done) {
            num_locks = 0;
            for (int i = 0; i < wr_cnt; i++) {
                row_t * row = accesses[write_set[i]]->orig_row;
                if (!row->manager->try_lock()) {
                    break;
                }
                row->manager->assert_lock();
                num_locks++;
                if (!row->manager->validate(accesses[write_set[i]]->tid, true)) {
                    abort_cnt_write_set++;
                    rc = Abort;
                    goto final;
                }
            }
            if (num_locks == wr_cnt) {
                done = true;
            } else {
                for (int i = 0; i < num_locks; i++) {
                    accesses[write_set[i]]->orig_row->manager->release();
                }
            }
            PAUSE
        }
    } else {
        for (int i = 0; i < wr_cnt; i++) {
            row_t * row = accesses[write_set[i]]->orig_row;
            row->manager->lock();
            num_locks++;
            if (!row->manager->validate(accesses[write_set[i]]->tid, true)) {
                rc = Abort;
                goto final;
            }
        }
    }

	// Validate tuples in the read set
    for (int i = 0; i < row_cnt - wr_cnt; i++) {
        Access * access = accesses[read_set[i]];
        if (!access->orig_row->manager->validate(access->tid, false)) {
            abort_cnt_read_set++;
            rc = Abort;
            goto final;
        }
    }

final:
    if (rc == Abort) {
        // Release all locks held
        for (int i = 0; i < num_locks; i++) {
            accesses[write_set[i]]->orig_row->manager->release();
        }
        // Clear all dirty writes
        for (int i = 0; i < wr_cnt; i++) {
            Access * access = accesses[write_set[i]];
            if (access->orig_row->manager->is_hotspot()) {
                access->orig_row->manager->clear_stashed(txn_id);
            }
        }
        // Notify all dependents to abort
        Dependent * ptr = dep_txns;
        while (ptr) {
            ptr->_txn->aborted = true;
            ptr = ptr->_next;
        }
        cleanup(rc);
    } else {
        for (int i = 0; i < wr_cnt; i++) {
            Access * access = accesses[write_set[i]];
            // Use txn_id to distinguish among different transactions
            access->orig_row->manager->write(access->data, txn_id);
            access->orig_row->manager->release();
            // Clear all dirty writes
            if (access->orig_row->manager->is_hotspot()) {
                access->orig_row->manager->clear_stashed(txn_id);
            }
        }
        // Decrease dep_cnt for all dependents
        Dependent * ptr = dep_txns;
        while (ptr) {
            ptr->_txn->dep_cnt -= 1;
            ptr = ptr->_next;
        }
        cleanup(rc);
    }
    return rc;
}

#endif