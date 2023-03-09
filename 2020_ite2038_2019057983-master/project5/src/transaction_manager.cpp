#include <transaction_manager.h>



int trx_cnt = 0;
pthread_mutex_t trx_manager_latch= PTHREAD_MUTEX_INITIALIZER;
map <int,trx_t* >trx_hash;
map <int,int> trx_visit, trx_check; 
multiset<int> waiting_trx[NUM_TRX];
//vector<char *> pre_record[NUM_TRX];
map <tuple<int,int,int> , tuple<int,char * > > pre_record;
vector <int> cycle; 

int trx_begin(void){
	pthread_mutex_lock(&trx_manager_latch);
	trx_cnt += 1;

	trx_t* trx = (trx_t*) malloc(sizeof(trx_t));
	trx->first = NULL;
	trx_hash[trx_cnt] = trx;

	int ret= trx_cnt;
	pthread_mutex_unlock(&trx_manager_latch);
	return ret; 
}

int trx_commit(int trx_id){
	pthread_mutex_lock(&trx_manager_latch);
	
//	cout<<"commit "<<trx_id<<"\n";

	// if trx_id not exist before commit, OMG 
	// when programs run nomarly, not execute
	if (trx_hash.find(trx_id) == trx_hash.end()){
//		cout<<"it shouldnt occur\n";
		pthread_mutex_unlock(&trx_manager_latch);
		return -1;
	}

	trx_t* trx= trx_hash[trx_id];
	lock_t* lock = trx->first;

	// in vector, there are lock which will be committed.
	vector <lock_t*> lock_vec;
	while (lock != NULL){
		lock_vec.push_back(lock);
		lock = lock->trx_next;
	}

	bool beforeLock = true; 
/*
	// before comitting, remove the edges between conflicting locks
	for (int i=0;i<(int)lock_vec.size();i++) {
		lock = lock_vec[i]->sentinel->head->next;
		while (lock != lock_vec[i]->sentinel->tail){
			if ((lock_vec[i]->lock_mode | lock->lock_mode) == 1 && lock_vec[i]->owner_trx != lock->owner_trx){
				
				if (beforeLock) trx_unwait(lock->owner_trx, lock_vec[i]->owner_trx);
				else trx_unwait(lock_vec[i]->owner_trx, lock->owner_trx);
			}

			if (lock == lock_vec[i]) beforeLock = false; 
			lock = lock->next;
		}
	}
*/

	trx_hash.erase(trx_id);
	pthread_mutex_unlock(&trx_manager_latch);

	// all lock in vector release 
	for (int i=0;i<(int)lock_vec.size();i++) lock_release(lock_vec[i]); 
//	pthread_mutex_lock(&trx_manager_latch);

	// bye bye, trx_id 
//	pthread_mutex_unlock(&trx_manager_latch);
	return 0; 
}
void trx_wait (int trx_front, int trx_back){
	// lock that trx_id is trx_back is waiting lock that trx_id is trx_front  
	if (trx_front == trx_back) {
		return;
	}
	waiting_trx[trx_back].insert(trx_front);
}

void trx_unwait (int trx_front,int trx_back){
	// lock that trx_id is trx_back is !not! waiting lock that trx_id is trx_front  
	if (trx_front == trx_back) {
		return;
	}
	// if there are no trx_front in front of trx_back, return 
	if (waiting_trx[trx_back].end() == waiting_trx[trx_back].find(trx_front)){
		return;
	}
	waiting_trx[trx_back].erase(waiting_trx[trx_back].find(trx_front));
}

// just dfs, and if here is visited, detect cycle (= deadlock)
int trx_traversal(int here){
	// trx_check is true -> here is what i checked before, but there is no cycle
	// trx_visit is true -> here is what i visited before, and visit again ( = cycle) 
	if (trx_check[here]) {
		return -1;
	}
	if (trx_visit[here]) {
		return here;
	}	
	trx_visit[here] = 1;

	int val = -1;
	
	multiset<int> ms = waiting_trx[here]; 
	
	// visit adjacent next
	// if val != -1, then cycle exist 

	for (auto next = ms.begin() ; next!= ms.end(); next++) val = max(val,trx_traversal(*next)); 

	trx_check[here] = 1;
	trx_visit[here] = 0;
	if (val != -1) cycle.push_back(here);

	if (val == here) return -1;
	return val;	
}
int detect_deadlock(){
	// clear past data
	trx_visit.clear(); 
	trx_check.clear();

	vector <vector <int> > ret;

	// 1 to trx_cnt needs to check 

	for (int i=1;i<= trx_cnt;i++){
		cycle.clear();
		// already check or already commit, continue;
		if (trx_check[i] == 1 || trx_hash.find(i) == trx_hash.end()){
			continue;
		}
		// else traversal!
		trx_traversal(i);
		
		// if cycle not emtpy, store cycle in ret 
		if (!cycle.empty()) ret.push_back(cycle);		
	}	

	// if ret is not empty, deadlock detection! 
	if (!ret.empty()) {
//		cout<<"deadlock detection\n";	
		return -1;
	}
	else {
//		cout<<"deadlock nondetection\n";
		return 0;
	}
}

int trx_acquire(int trx_id, lock_t* acquired_lock){
	pthread_mutex_lock(&trx_manager_latch);

	cout<<"in trx_acquire trx "<<trx_id<<" tid:"<<acquired_lock->sentinel->table_id<<" key: "<<acquired_lock->sentinel->key<<"\n";
	// not exist my transaction 
	// when programs normally run, not execute
	if (trx_hash.find(trx_id) == trx_hash.end()){
//		cout<<"it shouldnt occur\n";
		pthread_mutex_unlock(&trx_manager_latch);
		return -1;
	}

	trx_t* trx = trx_hash[trx_id];
	
	lock_t * tmp = trx->first;

	trx->first = acquired_lock;
	acquired_lock->trx_next = tmp;
	
	// hang lock in trx!
	// hanged position is the last trx_next 
	/*
	if (trx->first == NULL) trx->first = acquired_lock;
	
	else{ 
		lock_t* lock = trx->first;
		while (lock->trx_next !=NULL){
			lock = lock->trx_next;
		}
		lock->trx_next = acquired_lock;
	}
	
	l = acquired_lock->sentinel->head->next;
*/
	// add wait edge between conflicting locks (at least one lock exclusive  && not my trx)	

	/*while (l != acquired_lock){
		if ((l->lock_mode | acquired_lock->lock_mode) == 1 && l->owner_trx != acquired_lock->owner_trx){
			trx_wait(l->owner_trx, acquired_lock->owner_trx);
		}
		l = l->next;
	}

	// if detect deadlock , return -1 
	if (detect_deadlock() == -1){
		pthread_mutex_unlock(&trx_manager_latch);
		return -1; 
	}
	*/
	pthread_mutex_unlock(&trx_manager_latch);
	return 0;
}

void trx_unlock(){
	pthread_mutex_unlock(&trx_manager_latch);
}
void trx_abort(int trx_id){
	pthread_mutex_lock(&trx_manager_latch);
	trx_t * trx = trx_hash[trx_id];
	lock_t * lock = trx->first;

	vector <lock_t*> lock_vec;

	// in lock_vec, there are lock will be aborted . 

	while (lock != NULL){
		lock_vec.push_back(lock); 
		lock->is_aborted = 1; 
		lock = lock->trx_next;
	}

	// before aborting, remove edge between conflicting locks

	for (int i=0;i<(int)lock_vec.size();i++){
		lock = lock_vec[i]->sentinel->head->next;
		bool beforeLock = true; 
		while (lock != lock_vec[i]->sentinel->tail){
			if ((lock_vec[i]->lock_mode | lock->lock_mode) == 1 && lock_vec[i]->owner_trx != lock->owner_trx){
				
				if (beforeLock) trx_unwait(lock->owner_trx, lock_vec[i]->owner_trx);
				else trx_unwait(lock_vec[i]->owner_trx, lock->owner_trx);
			}

			if (lock == lock_vec[i]) beforeLock = false; 
			lock = lock->next;
		}
	}	

	trx_hash.erase(trx_id);
	pthread_mutex_unlock(&trx_manager_latch); 
	
	// following codes are to backup 

	for (int i=0;i<(int)lock_vec.size()-1;i++){		
		
		tuple<int,int,int> u(trx_id,lock_vec[i]->sentinel->table_id, lock_vec[i]->sentinel->key);

		if (pre_record.find(u) == pre_record.end()){
//			cout<<"no pre data\n";
			return;
		}

		// in the pre_record, the backup data are stored. you can use it and modify the page information
		tuple<int,char*> w = pre_record[u];
		int offset = get<0>(w);
		char * pre_data = get<1>(w);
		
		//BACKUP 

		leaf_page * c = (leaf_page*)buf_read_page(lock_vec[i]->sentinel->table_id, offset);
		strcpy(c->records[lock_vec[i]->sentinel->key].value, pre_data); 
		buf_write_page(lock_vec[i]->sentinel->table_id, offset, (page_t*)c);
		release_page_latch(lock_vec[i]->sentinel->table_id, offset);


		lock_release(lock_vec[i]); 
	}

	// the last lock doesnt need to backup , because the lock do not backup.
	// before backup, deadlock detects! 
	lock_release(lock_vec.back());
}

void trx_backup(int trx_id,int table_id, int key, int offset, char* prev_val){
	tuple<int,int,int> u(trx_id,table_id,key);
	tuple<int,char*> w(offset,prev_val);

	// you can find offset, prev_val with trx_id, table_id, key
	// i store datas with map and tuple
	if (pre_record.find(u) == pre_record.end()){
		pre_record[u] = w;
	}

//	tuple<int,char*> t;
//	t = pre_record[u];

	pthread_mutex_unlock(&trx_manager_latch);
}
