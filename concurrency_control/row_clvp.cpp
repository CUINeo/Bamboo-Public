#include "row.h"
#include "txn.h"
#include "row_clvp.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_clvp::init(row_t * row) {
	_row = row;
	// owners is a single linked list, each entry/node contains info like lock type, prev/next
	owners = NULL;
	owners_tail = NULL;
	// waiter is a double linked list. two ptrs to the linked lists
	waiters_head = NULL;
	waiters_tail = NULL;
	// retired is a linked list, the next of tail is the head of owners
	retired = NULL;
	retired_tail = NULL;
	owner_cnt = 0;
	waiter_cnt = 0;
	retired_cnt = 0;

	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);

	blatch = false;
}

RC Row_clvp::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_clvp::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == CLV);
	CLVLockEntry * en;

	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// each thread has at most one owner of a lock
	assert(owner_cnt <= g_thread_cnt);
	// each thread has at most one waiter
	assert(waiter_cnt < g_thread_cnt);

#if DEBUG_ASSERT
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

	RC rc = WAIT;
	RC status = check_abort(type, txn, retired, false);
	if (status == Abort) {
		rc = Abort;
		bring_next();
		goto final;
	} else if (status == WAIT) {
		// check owners
		if (check_abort(type, txn, owners, true) == Abort) {
			rc = Abort;
			bring_next();
			goto final;
		}
	} else {
		// owners should be all aborted and becomes empty
		while(owners) {
			en = owners;
			en->txn->set_abort();
			return_entry(en);
			owners = owners->next;
		}
		owners = NULL;
		owners_tail = NULL;
		owner_cnt = 0;
	}

	insert_to_waiters(type, txn);
	#if DEBUG_ASSERT
	if(owners)
		assert(owners->prev == NULL);
	if(retired){
		assert(retired->prev==NULL);
		if (owners)
		assert(retired->txn!=owners->txn);
		if (waiters_head)
		assert(retired->txn != waiters_head->txn);
	}
	#endif
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

final:
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}

RC Row_clvp::lock_retire(txn_man * txn) {

	if (g_central_man)
		glob_manager->lock_row(_row);
	else
		pthread_mutex_lock( latch );

	RC rc = RCOK;
	// Try to find the entry in the owners and remove
	CLVLockEntry * entry = remove_if_exists_in_owner(txn);
	if (entry == NULL) {
		// may be already wounded by others, or self is aborted
		assert(txn->status == ABORTED);
		rc = Abort;
		goto final;
	}

#if DEBUG_ASSERT
	assert_notin_list(retired, retired_tail, retired_cnt, entry->txn);
#endif
	// increment barriers if conflict
	if (retired_tail) {
		if (conflict_lock(retired_tail->type, entry->type)) {
			entry->delta = true;
			txn->increment_commit_barriers();
		} else {
			entry->is_cohead = retired_tail->is_cohead;
		}
	} else {
		entry->is_cohead = true;
	}

	// append entry to retired
	RETIRED_LIST_PUT_TAIL(retired, retired_tail, entry);
	retired_cnt++;

#if DEBUG_CLV
	printf("[row_clv] move txn %lu from owners to retired type %d of row %lu\n",
			txn->get_txn_id(), entry->type, _row->get_row_id());
#endif
#if DEBUG_ASSERT
	assert_in_list(retired, retired_tail, retired_cnt, entry->txn);

#endif

final:
	// bring next owners from waiters
	bring_next();
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}

RC Row_clvp::lock_release(txn_man * txn, RC rc) {
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// Try to find the entry in the retired
	RC status = remove_if_exists_in_retired(txn, rc == Abort);
	if (status == RCOK) {
		// owners should be all aborted and becomes empty
		CLVLockEntry * en;
		while(owners) {
			en = owners;
			en->txn->set_abort();
			return_entry(en);
			owners = owners->next;
		}
		owners_tail = NULL;
		owners = NULL;
		owner_cnt = 0;
	} else if (status == ERROR) {
		// need to check owners
		CLVLockEntry * en = remove_if_exists_in_owner(txn);
		if (en == NULL) {
			// Not in owners list, try waiters list.
			CLVLockEntry * en = waiters_head;
		 	while (en != NULL && en->txn != txn)
				en = en->next;
			if (en) {
				LIST_REMOVE(en);
				if (en == waiters_head)
						waiters_head = en->next;
				if (en == waiters_tail)
						waiters_tail = en->prev;
				return_entry(en);
				waiter_cnt--;
				#if DEBUG_CLV
				printf("[row_clv] rm txn %lu from waiters of row %lu\n", txn->get_txn_id(), _row->get_row_id());
				#endif
				#if DEBUG_ASSERT
				assert_notin_list(waiters_head, waiters_tail, waiter_cnt, txn);
				#endif
			}
		} else {
			return_entry(en);
		}
	} 

	// WAIT - done releasing with is_abort = true
	// FINISH - done releasing with is_abort = false
	bring_next();

	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return RCOK;
}

void
Row_clvp::bring_next() {
	

	CLVLockEntry * entry;
	// If any waiter can join the owners, just do it!
	while (waiters_head) {
		if ((owners == NULL) || (!conflict_lock(owners->type, waiters_head->type))) {
			LIST_GET_HEAD(waiters_head, waiters_tail, entry);
			#if DEBUG_ASSERT
			// aseert no conflicts
			if (has_conflicts_in_list(owners, entry))
				assert(false);
			#endif
			QUEUE_PUSH(owners, owners_tail, entry);
			#if DEBUG_ASSERT
			if (owner_cnt > 0)
				assert(!conflict_lock(owners->type, entry->type));
			#endif
			owner_cnt ++;
			waiter_cnt --;
			ASSERT(entry->txn->lock_ready == 0);
			entry->txn->lock_ready = true;
			#if DEBUG_CLV
			printf("[row_clv] bring %lu from waiters to owners of row %lu\n",
					entry->txn->get_txn_id(), _row->get_row_id());
			#endif
			#if DEBUG_ASSERT
			assert_in_list(owners, owners_tail, owner_cnt, entry->txn);
			assert_notin_list(waiters_head, waiters_tail, waiter_cnt, entry->txn);
			#endif
		} else
			break;
	}
	ASSERT((owners == NULL) == (owner_cnt == 0));
}

bool Row_clvp::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
		else if (l1 == LOCK_EX || l2 == LOCK_EX)
			return true;
	else
		return false;
}

bool Row_clvp::conflict_lock_entry(CLVLockEntry * l1, CLVLockEntry * l2) {
	if (l1 == NULL || l2 == NULL)
		return false;
	return conflict_lock(l1->type, l2->type);
}

CLVLockEntry * Row_clvp::get_entry() {
	CLVLockEntry * entry = (CLVLockEntry *) mem_allocator.alloc(sizeof(CLVLockEntry), _row->get_part_id());
	entry->prev = NULL;
	entry->next = NULL;
	entry->delta = false;
	entry->is_cohead = false;
	entry->txn = NULL;
	return entry;
}

void Row_clvp::return_entry(CLVLockEntry * entry) {
	mem_allocator.free(entry, sizeof(CLVLockEntry));
}

void
Row_clvp::insert_to_waiters(lock_t type, txn_man * txn) {
	CLVLockEntry * entry = get_entry();
	entry->txn = txn;
	entry->type = type;
	CLVLockEntry * en = waiters_head;
	while (en != NULL)
	{
		if (txn->get_ts() < en->txn->get_ts())
			break;
		en = en->next;
	}
	if (en) {
		LIST_INSERT_BEFORE(en, entry);
		if (en == waiters_head)
			waiters_head = entry;
	} else {
		LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
	}
	waiter_cnt ++;
	txn->lock_ready = false;
#if DEBUG_CLV
	printf("[row_clv] add txn %lu type %d to waiters of row %lu\n",
			txn->get_txn_id(), type, _row->get_row_id());
#endif
#if DEBUG_ASSERT
	assert_in_list(waiters_head, waiters_tail, waiter_cnt, txn);
#endif
}

RC
Row_clvp::check_abort(lock_t type, txn_man * txn, CLVLockEntry * list, bool is_owner) {
	CLVLockEntry * en = list;
	CLVLockEntry * prev = NULL;
	while (en != NULL) {
		if (en->txn->status == ABORTED || (conflict_lock(en->type, type) && (en->txn->get_ts() > txn->get_ts())) ) {
			if (txn->wound_txn(en->txn) == ERROR) {
				#if DEBUG_CLV
				printf("[row_clv] detected txn %lu is aborted when "
				"trying to wound others on row %lu\n", txn->get_txn_id(),  _row->get_row_id());
				#endif
				return Abort;
			}
			#if DEBUG_CLV
			printf("[row_clv] txn %lu abort txn %lu on row %lu\n", txn->get_txn_id(), 
				en->txn->get_txn_id(), _row->get_row_id());
			#endif

			if (is_owner) {
				#if DEBUG_ASSERT
				assert_in_list(owners, owners_tail, owner_cnt, en->txn);
				#endif
				#if DEBUG_CLV
				printf("[row_clv] txn %lu rm another txn %lu from owners of row %lu\n", 
					txn->get_txn_id(), en->txn->get_txn_id(), _row->get_row_id());
				#endif
				QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
				#if DEBUG_ASSERT
				assert_notin_list(owners, owners_tail, owner_cnt, en->txn);
				if(owners)
					assert_in_list(owners, owners_tail, owner_cnt, owners);
				#endif
			} else {
				// in retired, need to remove & abort descendants of en as well
				RC rc =  remove_descendants(en, txn, type);
				return_entry(en);
				return rc;
			}
		} else {
			prev = en;
		}
		en = en->next;
	}
	#if DEBUG_ASSERT
	if(owners)
		assert_in_list(owners, owners_tail, owner_cnt, owners);
	#endif
	return WAIT;
}

RC
Row_clvp::remove_descendants(CLVLockEntry * en, txn_man * txn, lock_t type) {
	CLVLockEntry * to_return = NULL;
	#if DEBUG_ASSERT
		assert_in_list(retired, retired_tail, retired_cnt, en->txn);
	#endif
	// 1. remove self
	LIST_RM(retired, retired_tail, en, retired_cnt);

	#if DEBUG_ASSERT
		assert_notin_list(retired, retired_tail, retired_cnt, en->txn);
	#endif
	#if DEBUG_CLV
	printf("[row_clv] rm aborted txn %lu from retired of row %lu\n", 
			en->txn->get_txn_id(), _row->get_row_id());
	#endif

	CLVLockEntry * prev = en;
	// 2. remove from next conflict till end
	en = en->next;
	// delta = false: has no delta, 0
	while(en && (en->delta == 0)) {
		// for lock_get only: not depend on detected aborts, but still need to check if conflict with current txn
		if (txn && conflict_lock(en->type, type) && (en->txn->get_ts() > txn->get_ts())) {
			if (txn->wound_txn(en->txn) == ERROR) {
				if (conflict_lock_entry(en->prev, en)) {
					en->delta = true;
				}
				#if DEBUG_CLV
				printf("[row_clv] detected txn %lu is aborted when "
				"trying to wound others on row %lu\n", txn->get_txn_id(),  _row->get_row_id());
				#endif
				// already removed, has to release
				//return_entry(prev);
				return Abort;
			}
			LIST_RM(retired, retired_tail, en, retired_cnt);

			#if DEBUG_CLV
			printf("[row_clv] txn %lu abort txn %lu on row %lu\n", txn->get_txn_id(), 
				en->txn->get_txn_id(), _row->get_row_id());
			printf("[row_clv] rm aborted txn %lu from retired of row %lu\n", 
			en->txn->get_txn_id(), _row->get_row_id());
			#endif
			to_return = en;
			en = en->next;
			return_entry(to_return);
		} else {
			if (conflict_lock_entry(en->prev, en)) {
				en->delta = true;
			}
			en = en->next;
		}
	}

	#if DEBUG_ASSERT
		assert_notin_list(retired, retired_tail, retired_cnt, prev->txn);
	#endif
	
	if (en) {
		LIST_RM_SINCE(retired, retired_tail, en);
	} else {
		// has no conflicting entry after removed
		// 3. if owners do not conflict with removed entry
		if (!conflict_lock_entry(prev, owners)) {
			//return_entry(prev);
			return WAIT;
		}
	}

	// 4. abort from next conflict (en) till end
	while(en) {
		to_return = en;
		en->txn->set_abort();
		retired_cnt--;

		#if DEBUG_CLV
		printf("[row_clv] rm aborted txn %lu from retired of row %lu\n", 
			en->txn->get_txn_id(), _row->get_row_id());
		#endif

		en = en->next;
		return_entry(to_return);
	}
	// 5.need to abort all owners as well, and txn can definitely hold the lock
	//return_entry(prev);
	return RCOK;
}

RC
Row_clvp::remove_if_exists_in_retired(txn_man * txn, bool is_abort) {
	CLVLockEntry * en = retired;
	//CLVLockEntry * to_return = NULL;
	RC status = ERROR;

	while (en != NULL) {
		/*if (en->txn->status == ABORTED) {
			// in retired, need to remove anything behind
			#if DEBUG_CLV
			if (!is_abort) {
			printf("decide to abort descendants of %lu for row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
			print_list(retired, retired_tail, retired_cnt);
			}
			#endif
			status = remove_descendants(en, NULL, LOCK_NONE);
			to_return = en;
			en = en->prev;
			// continue until target txn is aborted
		}
		else */ if (en->txn->get_txn_id() == txn->get_txn_id()) {
			break;
		}
		en = en->next;
		/*
		if (to_return) {
			return_entry(to_return);
			to_return = NULL;
		}*/
	}
	if (en) { // find the entry in the retired list
		if (!is_abort) {
			#if DEBUG_CLV
			printf("[row_clv] rm txn %lu from retired of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
			#endif
			update_entry(en);
			LIST_RM(retired, retired_tail, en, retired_cnt);
			#if DEBUG_ASSERT
			assert_notin_list(retired, retired_tail, retired_cnt, txn);
			#endif
			// FINISH: find and removed
			//if (status == ERROR)
			status = FINISH;
		} else {
			// RCOK (owners should all be aborted)  or WAIT  
			//if (status == WAIT)
			status = remove_descendants(en, NULL, LOCK_NONE);
		}
		return_entry(en);
	}
	// did not find, need to keep working
	#if DEBUG_ASSERT
	if (status == RCOK)
		assert_notin_list(retired, retired_tail, retired_cnt, txn);
	#endif
	return status;
}


CLVLockEntry * 
Row_clvp::remove_if_exists_in_owner(txn_man * txn) {
	#if DEBUG_ASSERT
	if(owners)
		assert_in_list(owners, owners_tail, owner_cnt, owners);
	#endif
	CLVLockEntry * en = owners;
	CLVLockEntry * prev = NULL;
	//CLVLockEntry * to_return = NULL;

	while (en != NULL) {
		/*if (en->txn->status == ABORTED) {
			// just remove the entry and continue, no one is depending on it
			QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
			to_return = en;
			#if DEBUG_CLV
			printf("[row_clv] rm txn %lu from owners of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
			#endif
			#if DEBUG_ASSERT
			assert_notin_list(owners, owners_tail, owner_cnt, txn);
			#endif
		}*/
		if (en->txn->get_txn_id() == txn->get_txn_id()) {
			/*if (to_return == en) {
				return_entry(to_return);
				return NULL;
			}*/
			break;
		}
		prev = en;
		en = en->next;
		/*if (to_return) {
			return_entry(to_return);
			to_return = NULL;
		}*/
	}
	
	if (en) { // find the entry in the retired list
		#if DEBUG_CLV
		printf("[row_clv] rm txn %lu from owners of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
		#endif
		QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
		#if DEBUG_ASSERT
		assert_notin_list(owners, owners_tail, owner_cnt, txn);
		#endif
		// find and return
		return en;
	}
	// did not find
	return NULL;
}

void
Row_clvp::update_entry(CLVLockEntry * en) {
	CLVLockEntry * entry;
	if (en->prev) {
		if (en->next) {
			if (en->next->delta == true) {
				if (!conflict_lock_entry(en->prev, en->next)) {
					// both are SH
					en->next->delta = false;
					// change delta, need to check cohead
					if (en->prev->is_cohead) {
						entry = en->next;
						while(entry && (entry->delta == false)) {
							entry->is_cohead = true;
							entry->txn->decrement_commit_barriers();
							entry = entry->next;
						}
					} // else, not cohead, nothing to do
				}
			} else {
				en->next->delta = en->delta;
			}
		} else {
			// has no next, nothing needs to be updated
		}
	} else {
		// has no previous, en = head
		if (en->next) {
			#if DEBUG_ASSERT
			assert(en == retired);
			#endif
			// has next entry
			// en->next->is_cohead = true;
			if (en->next->delta) {
				en->next->delta = false;
				entry = en->next;
				while(entry && (entry->delta == false)) {
					entry->is_cohead = true;
					entry->txn->decrement_commit_barriers();
					entry = entry->next;
				}
			} // else (R)RR, no changes
		} else {
			// has no next entry, never mind
		}
	}
	assert(retired || retired->is_cohead);
}

void
Row_clvp::print_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt) {
	CLVLockEntry * en = list;
	int count = 0;
	while(en){
		printf("(%lu, %d) -> ", en->txn->get_txn_id(), en->type);
		en = en->next;
		count += 1;
	}
	if (tail) {
		printf("expected cnt: %d, real cnt: %d, expected tail: %lu\n", cnt, count, 
		tail->txn->get_txn_id());
	} else {
		printf("expected cnt: %d, real cnt: %d, expected tail is null\n", cnt, count);
	}
}


void
Row_clvp::assert_notin_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn) {
	CLVLockEntry * en = list;
	CLVLockEntry * prev = NULL;
	int count = 0;
	while(en){
		if(txn->get_txn_id() == en->txn->get_txn_id())
			printf("ERROR: %lu is already in row %lu\n", txn->get_txn_id(), _row->get_row_id());
		assert(txn->get_txn_id() != en->txn->get_txn_id());
		prev = en;
		en = en->next;
		count += 1;
	}
	if (count != cnt){
		print_list(list, tail, cnt);
	}
	assert(count == cnt);
	assert(tail == prev);
}

void
Row_clvp::assert_in_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn) {
	CLVLockEntry * en = list;
	CLVLockEntry * prev = NULL;
	int count = 0;
	bool in = false;
	while(en){
		if(txn->get_txn_id() == en->txn->get_txn_id()) {
			if (in) {
				print_list(owners, owners_tail, owner_cnt);
				assert(false);
			}
			in = true;
		}
		prev = en;
		en = en->next;
		count += 1;
	}
	#if DEBUG_CLV
	if (tail != prev)
		print_list(list, tail, cnt);
	#endif
	// assert(tail->txn->get_txn_id() == txn->get_txn_id());
	assert(in);
	assert(tail == prev);
	assert(count == cnt);
}

void
Row_clvp::assert_in_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt,CLVLockEntry * l) {
	CLVLockEntry * en = list;
	CLVLockEntry * prev = NULL;
	txn_man * txn = l->txn;
	int count = 0;
	bool in = false;
	while(en){
		if(txn->get_txn_id() == en->txn->get_txn_id()) {
			if (in) {
				print_list(owners, owners_tail, owner_cnt);
				assert(false);
			}
			in = true;
		}
		prev = en;
		en = en->next;
		count += 1;
	}
	#if DEBUG_CLV
	if (tail != prev)
		print_list(list, tail, cnt);
	#endif
	// assert(tail->txn->get_txn_id() == txn->get_txn_id());
	assert(in);
	assert(tail == prev);
	assert(count == cnt);
}

bool
Row_clvp::has_conflicts_in_list(CLVLockEntry * list, CLVLockEntry * entry) {
	CLVLockEntry * en;
	en = list;
	while(en) {
		if (conflict_lock(en->type, entry->type)) {
			return true;
		}
		en = en->next;
	}
	return false;
}
