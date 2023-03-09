#ifndef __LOCK_MANAGER_H__
#define __LOCK_MANAGER_H__

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <utility>
#include <iostream>
#include <map>
#include <transaction_manager.h>

using namespace std;

typedef struct lock_t lock_t;
typedef struct entry_t entry_t;

struct entry_t{
	lock_t * tail;
	lock_t * head;
	int table_id;
	int64_t key;
	map <int, lock_t*> mp;
};

struct lock_t {
	lock_t* prev;
	lock_t* next;
	lock_t* waiting_lock;
	entry_t* sentinel;
	int is_waiting; 
	pthread_cond_t cond;
	int lock_mode; 
	lock_t* trx_next;
	int owner_trx;
	int is_aborted; 
};


/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode,int offset);
int lock_release(lock_t* lock_obj);

#endif 
