#include <lock_manager.h>


map <pair<int,int64_t>,entry_t*> id_hash;
pthread_mutex_t lock_table_latch= PTHREAD_MUTEX_INITIALIZER;

int
init_lock_table()
{
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	return 0;
}

lock_t*
lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode,int offset)
{
//	cout<<trx_id<<" "<<key<<"\n";
	pthread_mutex_lock(&lock_table_latch);

	printf("in lock acq trx:%d tid:%d key:%d\n", trx_id,table_id,key);

//	printf("%d in key %d\n",trx_id,key);
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


//	if (entry->mp.find(trx_id) != entry->mp.end()){
//		pthread_mutex_unlock(&lock_table_latch);
//		return entry->mp[trx_id];
//	}
	// if ret is already locked with highere mode -> return ;;;
	lock_t * lock = entry->head->next;

//	while (lock != entry->tail){
//		cout<<"trx : "<<lock->owner_trx<<" ";
//		lock = lock->next;
//	}
//	cout<<"\n";


	ret->sentinel = entry;
	ret->cond = PTHREAD_COND_INITIALIZER;
	ret->prev = ret->sentinel->tail->prev;
	ret->sentinel->tail->prev->next = ret;
	ret->next = ret->sentinel->tail;
	ret->sentinel->tail->prev = ret;

	ret->sentinel->table_id = table_id;
	ret->sentinel->key = key;

	ret->owner_trx = trx_id; 
	ret->lock_mode = lock_mode; 
	ret->is_waiting = 1; 
	ret->trx_next = NULL;
	ret->is_aborted = 0;

	lock = ret->sentinel->head->next; 	
	
	// hanging lock in my trx 
	// if detect deadlock, immediatly retun NULL
	
//	pthread_mutex_unlock(&lock_table_latch);
	if(trx_acquire (trx_id, ret) == -1){
		pthread_mutex_unlock(&lock_table_latch);
		trx_abort(trx_id); 
		return NULL;
	}



	//wating until acquire

	//when acquire?
	// 1. head->next == ret
	// 2. ret->prev, ret are shared mode and, ret->prev acquire lock
	// 3. ret->prev, ret are same trx, ret->prev acquire lock
	// 4. ret aleardy acquire lock

//	cout<<"acquire "<<trx_id<<"\n";
	lock_t * r = ret->sentinel->head->next;
	while (r!= ret->sentinel->tail){
		
		cout<<r->owner_trx<<" "<<"["<<r->lock_mode<<"]"<<" "<<"("<<r->is_waiting<<")"<<"\n";
		r = r->next;
	}
	cout<<"\n";

	while (ret->sentinel->head->next != ret && !(ret->lock_mode == 0 && ret->prev->lock_mode == 0 && ret->prev->is_waiting == 0) ){
		if (ret->prev->is_waiting == 0 && ret->prev->owner_trx == trx_id) break;

		if (ret->is_waiting ==0 ) break; 
		pthread_cond_wait(&ret->cond, &lock_table_latch);
		
	}

	cout<<"acquire lock trx"<< trx_id<<" tid:"<<table_id<<" key:"<<key<<"\n";
//	lock acquired!
//	entry->mp[trx_id] = ret; 
	ret->is_waiting = 0; 

/*	leaf_page * lp = (leaf_page*) buf_read_page(table_id, offset);

	int i=0; 
	for (i=0;;i++) {
		if (lp->records[i].key == key) break; 
	}
	char * prev_val = lp->records[i].value; 
	release_page_latch(table_id, offset); 
	trx_backup(trx_id, table_id, key,offset, prev_val);*/
//	printf("%d out key %d\n",trx_id,key);

	pthread_mutex_unlock(&lock_table_latch);
	return ret;
}

int
lock_release(lock_t* lock_obj)
{
	pthread_mutex_lock(&lock_table_latch);

	cout<<"release "<<lock_obj->owner_trx<<"\n";
/*	lock_t * r = lock_obj->sentinel->head->next;
	cout<<"release "<<lock_obj->owner_trx<<"\n";
	while (r!= lock_obj->sentinel->tail){
		
		cout<<r->owner_trx<<" "<<"["<<r->lock_mode<<"]"<<" "<<"("<<r->is_waiting<<")"<<"\n";
		r = r->next;
	}
	cout<<"\n";*/
/*
	if (lock_obj ->prev == lock_obj->sentinel->head){
		pthread_cond_signal(&lock_obj->next->cond);
		lock_obj->next->is_waiting = 0;
	}
*/
	lock_t * l =lock_obj->next;

	lock_obj->next->prev = lock_obj->prev;
	lock_obj->prev->next = lock_obj->next;
	free(lock_obj);



	while (l != l->sentinel->tail && l->is_waiting == 1){
		if (l->prev == l->sentinel->head){
			pthread_cond_signal(&l->cond);
			l->is_waiting = 0; 
		}
		else if (l->prev->is_waiting == 0 && l->prev->owner_trx == l->owner_trx){
			pthread_cond_signal(&l->cond);
			l->is_waiting = 0; 
		}
		
		else if (l->prev->is_waiting == 0 && l->prev->lock_mode == 0 && l->lock_mode == 0){
			pthread_cond_signal(&l->cond);
			l->is_waiting = 0; 
		}
		else break; 


		l = l->next;
	}

	/*
	// if lock_obj is head next, lock_obj-> next must recieve signal
	// and then, when nested locks are both shared -> recieve signal
	if (lock_obj->prev == lock_obj->sentinel->head){
		lock_t * next_obj = lock_obj->next;

		if (next_obj != lock_obj->sentinel->tail){
			pthread_cond_signal(&next_obj->cond);
			next_obj->is_waiting = 0; 
		
			next_obj = next_obj->next;
			while (next_obj != lock_obj->sentinel->tail){
				pthread_cond_signal(&next_obj->cond);
				next_obj->is_waiting = 0; 
				if (next_obj->lock_mode ==1) break;
				next_obj = next_obj->next;
			}
		}
		

	}

	//if lock_ob -> prev is shared and acquired, 
	// lock_obj->next can acquired (they are both shared)

	if ((lock_obj->prev->lock_mode == 0 && lock_obj->prev->is_waiting == 0)){
		lock_t * next_obj = lock_obj->next;
		while (next_obj != lock_obj->sentinel->tail){
			if (next_obj->lock_mode ==1 ) break;
			pthread_cond_signal(&next_obj->cond);
			next_obj->is_waiting = 0; 
			next_obj = next_obj->next; 
		}
	}


	//if lock_ob -> prev is shared and acquired, 
	// lock_obj->next can acquired (they have same trx_id)
	
	if (lock_obj->prev->is_waiting ==0){
		lock_t* next_obj = lock_obj->next;
		while (next_obj != lock_obj->sentinel->tail){
			if (next_obj-> owner_trx != lock_obj->prev->owner_trx) break;
			pthread_cond_signal(&next_obj->cond);
			next_obj->is_waiting = 0 ; 
			next_obj = next_obj -> next;

		}

	}
*/


	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}
