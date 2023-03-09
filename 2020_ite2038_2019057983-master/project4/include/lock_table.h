#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <utility>
#include <iostream>
#include <map>

typedef struct entry_t entry_t;
typedef struct lock_t lock_t;

using namespace std;
/* APIs for lock table */
//int get_hash_id (int table_id, int64_t key);
int init_lock_table();
lock_t* lock_acquire(int table_id, int64_t key);
int lock_release(lock_t* lock_obj);

#endif /* __LOCK_TABLE_H__ */
