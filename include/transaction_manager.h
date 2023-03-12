#ifndef __TRANSACTION_MANAGER_H__
#define __TRANSACTION_MANAGER_H__

#include <lock_manager.h>
#include <vector>
#include <map>
#include <set>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include "bpt.h"
#include "buf.h"
#include <log.h>

using namespace std;

#define NUM_TRX (int)1e6

typedef struct trx_t trx_t;
typedef struct lock_t lock_t;

struct trx_t{
	lock_t* first;
};


int trx_begin(void);
int trx_commit(int trx_id);
void trx_wait (int trx_front, int trx_back);
void trx_unwait(int trx_front, int trx_back);
int trx_traversal(int here); 
int detect_deadlock();
int trx_acquire(int trx_id, lock_t* acquired_lock);
int trx_abort(int trx_id);
void trx_backup(int trx_id, int table_id, int key,int page_num, int offset, char * prev_val);
void trx_unlock(); 
#endif
