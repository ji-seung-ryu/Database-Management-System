#include <lock_table.h>

struct entry_t{
	lock_t * tail;
	lock_t * head;
	int table_id;
	int64_t key;
};

struct lock_t {
	lock_t* prev;
	lock_t* next;
	entry_t* sentinel;
	pthread_cond_t cond;
};

typedef struct entry_t entry_t;
typedef struct lock_t lock_t;

map <pair<int,int64_t>,entry_t*> id_hash;
pthread_mutex_t lock_table_latch;

int
init_lock_table()
{
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	return 0;
}

lock_t*
lock_acquire(int table_id, int64_t key)
{
//	printf("in fac %d %d\n", table_id, key);
	pthread_mutex_lock(&lock_table_latch);

	pair<int,int64_t> u = {table_id,key};
	entry_t* entry = NULL;
	lock_t* ret= (lock_t*) malloc (sizeof(lock_t));
	
	if (id_hash.find(u) == id_hash.end()){
		entry = (entry_t*) malloc (sizeof(entry_t));
		entry->table_id = table_id, entry->key = key;		
		
		lock_t* head = (lock_t*) malloc(sizeof(lock_t));
		lock_t* tail = (lock_t*) malloc(sizeof(lock_t)); 
		tail->prev = head, head->next = tail;
		tail->sentinel = entry, head->sentinel= entry;
		
		//ret->sentinel = entry;
		entry->head = head, entry->tail = tail;
		id_hash[u] = entry;
	}
	else entry = id_hash[u];
	
	ret->sentinel = entry;
	ret->cond = PTHREAD_COND_INITIALIZER;
	ret->prev = ret->sentinel->tail->prev;
	ret->sentinel->tail->prev->next = ret;
	ret->next = ret->sentinel->tail;
	//ret-> next = entry[hash].tail;
	ret->sentinel->tail->prev = ret;
	//entry[hash].tail->prev = ret;

	ret->sentinel->table_id = table_id;
	ret->sentinel->key = key;
	

	while (ret->sentinel->head->next != ret){
		pthread_cond_wait(&ret->cond, &lock_table_latch);
	}

	pthread_mutex_unlock(&lock_table_latch);
	return ret;
}

int
lock_release(lock_t* lock_obj)
{
	pthread_mutex_lock(&lock_table_latch);

	if (lock_obj->next != lock_obj->sentinel->tail) {		
		pthread_cond_signal(&lock_obj->next->cond);
	}

	lock_obj->next->prev = lock_obj->prev;
	lock_obj->prev->next = lock_obj->next;
	free(lock_obj);

	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}
