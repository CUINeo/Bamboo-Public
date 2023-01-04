#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_dirty_occ.h"
#include "mem_alloc.h"
#include "query.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
    txn_man::init(h_thd, h_wl, thd_id);
    _wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
    RC rc;
    ycsb_query * m_query = (ycsb_query *) query;
    ycsb_wl * wl = (ycsb_wl *) h_wl;
    itemid_t * m_item = NULL;
#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
    int access_id;
    retire_threshold = (uint32_t) floor(m_query->request_cnt * (1 - g_last_retire));
#else
    row_cnt = 0;
#endif

    // if long txn and not rerun aborted txn, generate queries
    if (unlikely(m_query->is_long && !(m_query->rerun))) {
        uint64_t starttime = get_sys_clock();
        m_query->gen_requests(h_thd->get_thd_id(), h_wl);
        DEC_STATS(h_thd->get_thd_id(), run_time, get_sys_clock() - starttime);
    }

    for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
        ycsb_request * req = &m_query->requests[rid];
        int part_id = wl->key_to_part( req->key );
        bool finish_req = false;
        UInt32 iteration = 0;
        while ( !finish_req ) {
            if (iteration == 0) {
                m_item = index_read(_wl->the_index, req->key, part_id);
            }
#if INDEX_STRUCT == IDX_BTREE
            else {
                _wl->the_index->index_next(get_thd_id(), m_item);
                if (m_item == NULL)
                    break;
            }
#endif
            row_t * row = ((row_t *)m_item->location);
            row_t * row_local;
            access_t type = req->rtype;
            //printf("[txn-%lu] start %d requests at key %lu\n", get_txn_id(), rid, req->key);
            row_local = get_row(row, type);
            if (row_local == NULL) {
                rc = Abort;
                goto final;
            }
#if CC_ALG == BAMBOO && (THREAD_CNT != 1)
            access_id = row_cnt - 1;
#endif

            // Computation //
            // Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
//                  for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                        int fid = 0;
                        char * data = row_local->get_data();
                        __attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
//                  }
                } else {
                    assert(req->rtype == WR);
//					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
                        int fid = 0;
#if (CC_ALG == BAMBOO) || (CC_ALG == WOUND_WAIT) || (CC_ALG == DIRTY_OCC) || (CC_ALG == SILO) || (CC_ALG == TICTOC) || (CC_ALG == OCC)
                        char * data = row_local->get_data();
#else
                        char * data = row->get_data();
#endif
                        *(uint64_t *)(&data[fid * 10]) = 0;
#if CC_ALG == DIRTY_OCC
                        // If the row is a hotspot, perform a dirty read
                        if (row->manager->is_hotspot()) {
                            row_t * tmp_row = (row_t *) _mm_malloc(sizeof(row_t), 64);
                            tmp_row->init(MAX_TUPLE_SIZE);
                            tmp_row->copy(row_local);
                            tmp_row->table = row_local->get_table();
                            row->manager->dirty_write(tmp_row, txn_id);
                        }
#endif
//					}
                } 
            }

            iteration ++;
            if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
                finish_req = true;
#if (CC_ALG == BAMBOO) && (THREAD_CNT != 1)
            // retire write txn
            if (finish_req && (req->rtype == WR) && (rid <= retire_threshold)) {
            	//printf("[txn-%lu] retire %d requests\n", get_txn_id(), rid);
                if (retire_row(access_id) == Abort)
                  return finish(Abort);
            }
#endif
        }
    }
    rc = RCOK;
final:
    rc = finish(rc);
    return rc;
}

