#include "row.h"
#include "txn.h"
#include "row_ww.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_ww::init(row_t * row) {
	_row = row;
	// owners is a single linked list, each entry/node contains info like lock type, prev/next
	owners = NULL;
	// waiter is a double linked list. two ptrs to the linked lists
	waiters_head = NULL;
	waiters_tail = NULL;
	owner_cnt = 0;
	waiter_cnt = 0;

#if SPINLOCK
	latch = new pthread_spinlock_t;
	pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#else
	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
#endif
	
	lock_type = LOCK_NONE;
	blatch = false;
}

RC Row_ww::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_ww::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == WOUND_WAIT);
	RC rc;
	LockEntry * en;
	// get part id
	//int part_id =_row->get_part_id();

	#if DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif

	if (g_thread_cnt > 1){
		if (g_central_man)
			// if using central manager
			glob_manager->lock_row(_row);
		else {
			#if SPINLOCK
			pthread_spin_lock( latch );
			#else
			pthread_mutex_lock( latch );
			#endif
		}
	}

	#if DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug1, get_sys_clock() - starttime);
	INC_STATS(txn->get_thd_id(), debug10, 1);
	#endif

	// each thread has at most one owner of a lock
	assert(owner_cnt <= g_thread_cnt);
	// each thread has at most one waiter
	assert(waiter_cnt < g_thread_cnt);

#if DEBUG_ASSERT
	if (owners != NULL)
		assert(lock_type == owners->type); 
	else 
		assert(lock_type == LOCK_NONE);
	en = owners;
	UInt32 cnt = 0;
	while (en) {
		assert(en->txn->get_thd_id() != txn->get_thd_id());
		cnt ++;
		en = en->next;
	}
	assert(cnt == owner_cnt);
	en = waiters_head;
	cnt = 0;
	while (en) {
		cnt ++;
		en = en->next;
	}
	assert(cnt == waiter_cnt);
#endif

	
	if (owner_cnt == 0) {
			// if owner is empty, grab the lock
		LockEntry * entry = get_entry();
		entry->type = type;
		entry->txn = txn;
		STACK_PUSH(owners, entry);
		owner_cnt ++;
		lock_type = type;
		txn->lock_ready = true;
			rc = RCOK;
	} else {
			en = owners;
			LockEntry * prev = NULL;
			while (en != NULL) {
				if (en->txn->get_ts() > txn->get_ts() && conflict_lock(lock_type, type)) {
						// step 1 - figure out what need to be done when aborting a txn
						// ask thread to abort
					if (txn->wound_txn(en->txn) == COMMITED){
						// this txn is wounded by other txns.. 
						if (owner_cnt == 0)
							bring_next();
						rc = Abort;
						goto final;
					}
					// remove from owner
					if (prev)
							prev->next = en->next;
					else {
						if (owners == en)
							owners = en->next;
					}
					// free en
					return_entry(en);
					// update count
					owner_cnt--;
					if (owner_cnt == 0)
						lock_type = LOCK_NONE;
				} else {
						prev = en;
				}
				en = en->next;
			}

		// insert to wait list
		// insert txn to the right position
		// the waiter list is always in timestamp order
		LockEntry * entry = get_entry();
		entry->txn = txn;
		entry->type = type;
		en = waiters_head;
		while ((en != NULL) && (txn->get_ts() > en->txn->get_ts()))
			en = en->next;
		if (en) {
			LIST_INSERT_BEFORE(en, entry);
			if (en == waiters_head)
				waiters_head = entry;
		} else
			LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
		waiter_cnt ++;
		txn->lock_ready = false;
		rc = WAIT;
		bring_next();

		// if brought in owner return acquired lock
		en = owners;
		while(en){
			if (en->txn == txn) {
				rc = RCOK;
				break;
			}
			en = en->next;
		}
	}
final:
	if (g_central_man)
		glob_manager->release_row(_row);
	else {
		#if SPINLOCK
		pthread_spin_unlock( latch );
		#else
		pthread_mutex_unlock( latch );
		#endif
	}

	return rc;
}


RC Row_ww::lock_release(txn_man * txn) {

	#if DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif

	if (g_thread_cnt > 1) {
		if (g_central_man)
			glob_manager->lock_row(_row);
		else {
			#if SPINLOCK
			pthread_spin_lock( latch );
			#else
			pthread_mutex_lock( latch );
			#endif
		}
	}

	#if DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug6, get_sys_clock() - starttime);
	#endif

	// Try to find the entry in the owners
	LockEntry * en = owners;
	LockEntry * prev = NULL;

	while ((en != NULL) && (en->txn != txn)) {
		prev = en;
		en = en->next;
	}
	if (en) { // find the entry in the owner list
		if (prev) 
			prev->next = en->next;
		else{ 
			if (owners == en)
				owners = en->next;
		}
		return_entry(en);
		owner_cnt --;
		if (owner_cnt == 0)
			lock_type = LOCK_NONE;
	} else {
		// Not in owners list, try waiters list.
		en = waiters_head;
		while (en != NULL && en->txn != txn)
			en = en->next;
		if (en) {
			LIST_REMOVE(en);
			if (en == waiters_head)
				waiters_head = en->next;
			if (en == waiters_tail)
				waiters_tail = en->prev;
			return_entry(en);
			waiter_cnt --;
		}
	}

	bring_next();
	ASSERT((owners == NULL) == (owner_cnt == 0));

	if (g_central_man)
		glob_manager->release_row(_row);
	else {
		#if SPINLOCK
		pthread_spin_unlock( latch );
		#else
		pthread_mutex_unlock( latch );
		#endif
	}

	return RCOK;
}

bool Row_ww::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
		else if (l1 == LOCK_EX || l2 == LOCK_EX)
			return true;
	else
		return false;
}

LockEntry * Row_ww::get_entry() {
	LockEntry * entry = (LockEntry *) 
		mem_allocator.alloc(sizeof(LockEntry), _row->get_part_id());
	return entry;
}
void Row_ww::return_entry(LockEntry * entry) {
	mem_allocator.free(entry, sizeof(LockEntry));
}

void
Row_ww::bring_next() {
		LockEntry * entry;
	// If any waiter can join the owners, just do it!
	while (waiters_head && (owners == NULL || !conflict_lock(owners->type, waiters_head->type) )) {
		LIST_GET_HEAD(waiters_head, waiters_tail, entry);
		STACK_PUSH(owners, entry);
		owner_cnt ++;
		waiter_cnt --;
		ASSERT(entry->txn->lock_ready == 0);
		entry->txn->lock_ready = true;
		lock_type = entry->type;
	}
		ASSERT((owners == NULL) == (owner_cnt == 0));
}
