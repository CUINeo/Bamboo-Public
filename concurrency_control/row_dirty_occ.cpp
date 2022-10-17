#include "txn.h"
#include "row.h"
#include "row_dirty_occ.h"
#include "mem_alloc.h"

#if CC_AL == DIRTY_OCC

void Row_dirty_occ::init(row_t * row) {
    _row = row;
    _tid = 0;
}

RC Row_dirty_occ::access(txn_man * txn, TsType type, row_t * local_row) {

}

#endif