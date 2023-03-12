#include <transaction_manager.h>

int trx_cnt = 0;
pthread_mutex_t trx_manager_latch = PTHREAD_MUTEX_INITIALIZER;
map<int, trx_t *> trx_hash;
map<int, int> trx_visit, trx_check;
multiset<int> waiting_trx[NUM_TRX];

map<tuple<int, int, int>, tuple<int, char *, int>> pre_record;
vector<int> cycle;

int trx_begin(void)
{
	pthread_mutex_lock(&trx_manager_latch);
	trx_cnt += 1;

	trx_t *trx = (trx_t *)malloc(sizeof(trx_t));
	trx->first = NULL;
	trx_hash[trx_cnt] = trx;

	int ret = trx_cnt;
	pthread_mutex_unlock(&trx_manager_latch);
	log_BCR_write(ret, 0);
	return ret;
}

int trx_commit(int trx_id)
{
	pthread_mutex_lock(&trx_manager_latch);

	// if trx_id not exist before commit, OMG 
	// when programs run nomarly, not execute
	if (trx_hash.find(trx_id) == trx_hash.end())
	{
		pthread_mutex_unlock(&trx_manager_latch);
		return -1;
	}

	trx_t *trx = trx_hash[trx_id];
	lock_t *lock = trx->first;

	// in vector, there are lock which will be committed.
	vector<lock_t *> lock_vec;
	while (lock != NULL)
	{
		lock_vec.push_back(lock);
		lock = lock->trx_next;
	}

	bool beforeLock = true;

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

	trx_hash.erase(trx_id);
	pthread_mutex_unlock(&trx_manager_latch);

	for (int i = 0; i < (int)lock_vec.size(); i++)
		lock_release(lock_vec[i]);

	log_BCR_write(trx_id, 2);

	return 0;
}
void trx_wait(int trx_front, int trx_back)
{
	// lock that trx_id is trx_back is waiting lock that trx_id is trx_front  
	if (trx_front == trx_back)
	{
		return;
	}
	waiting_trx[trx_back].insert(trx_front);
}

void trx_unwait(int trx_front, int trx_back)
{
	// lock that trx_id is trx_back is !not! waiting lock that trx_id is trx_front  
	if (trx_front == trx_back)
	{
		return;
	}

	// if there are no trx_front in front of trx_back, return 
	if (waiting_trx[trx_back].end() == waiting_trx[trx_back].find(trx_front))
	{
		return;
	}
	waiting_trx[trx_back].erase(waiting_trx[trx_back].find(trx_front));
}

// just dfs, and if here is visited, detect cycle (= deadlock)
int trx_traversal(int here)
{

	// trx_check is true -> here is what i checked before, but there is no cycle
	// trx_visit is true -> here is what i visited before, and visit again ( = cycle) 

	if (trx_check[here])
	{
		return -1;
	}
	if (trx_visit[here])
	{
		return here;
	}
	trx_visit[here] = 1;

	int val = -1;

	multiset<int> ms = waiting_trx[here];

	for (auto next = ms.begin(); next != ms.end(); next++)
		val = max(val, trx_traversal(*next));

	trx_check[here] = 1;
	trx_visit[here] = 0;
	if (val != -1)
		cycle.push_back(here);

	if (val == here)
		return -1;
	return val;
}
int detect_deadlock()
{

	trx_visit.clear();
	trx_check.clear();

	vector<vector<int>> ret;

	for (int i = 1; i <= trx_cnt; i++)
	{
		cycle.clear();

		if (trx_check[i] == 1 || trx_hash.find(i) == trx_hash.end())
		{
			continue;
		}

		trx_traversal(i);

		if (!cycle.empty())
			ret.push_back(cycle);
	}

	if (!ret.empty())
	{

		return -1;
	}
	else
	{

		return 0;
	}
}

int trx_acquire(int trx_id, lock_t *acquired_lock)
{
	pthread_mutex_lock(&trx_manager_latch);

	if (trx_hash.find(trx_id) == trx_hash.end())
	{

		pthread_mutex_unlock(&trx_manager_latch);
		return -1;
	}

	trx_t *trx = trx_hash[trx_id];

	lock_t *tmp = trx->first;

	trx->first = acquired_lock;
	acquired_lock->trx_next = tmp;

	lock_t* l = acquired_lock->sentinel->head->next;

	// add wait edge between conflicting locks (at least one lock exclusive  && not my trx)	
	while (l != acquired_lock){
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
	


	pthread_mutex_unlock(&trx_manager_latch);
	return 0;
}

void trx_unlock()
{
	pthread_mutex_unlock(&trx_manager_latch);
}
int trx_abort(int trx_id)
{
	pthread_mutex_lock(&trx_manager_latch);
	log_BCR_write(trx_id, 3);
	trx_t *trx = trx_hash[trx_id];
	lock_t *lock = trx->first;

	vector<lock_t *> lock_vec;

	while (lock != NULL)
	{
		lock_vec.push_back(lock);
		lock->is_aborted = 1;
		lock = lock->trx_next;
	}

	trx_hash.erase(trx_id);
	pthread_mutex_unlock(&trx_manager_latch);

	for (int i = 0; i < (int)lock_vec.size(); i++)
	{

		tuple<int, int, int> u(trx_id, lock_vec[i]->sentinel->table_id, lock_vec[i]->sentinel->key);

		if (pre_record.find(u) == pre_record.end())
		{

			return 0;
		}

		tuple<int, char *, int> w = pre_record[u];
		int page_num = get<0>(w);
		char *pre_data = get<1>(w);
		int offset = get<2>(w);

		int idx = (offset - 128) / sizeof(leaf_record);

		leaf_page *c = (leaf_page *)buf_read_page(lock_vec[i]->sentinel->table_id, page_num);
		strcpy(c->records[idx].value, pre_data);
		buf_write_page(lock_vec[i]->sentinel->table_id, page_num, (page_t *)c);
		release_page_latch(lock_vec[i]->sentinel->table_id, page_num);

		log_compensate_write(trx_id, 3, lock_vec[i]->sentinel->table_id, page_num, offset, 120, c->records[idx].value, pre_data, 0);

		lock_release(lock_vec[i]);
	}
	return trx_id;
}

void trx_backup(int trx_id, int table_id, int key, int page_num, int offset, char *prev_val)
{
	pthread_mutex_lock(&trx_manager_latch);
	tuple<int, int, int> u(trx_id, table_id, key);
	tuple<int, char *, int> w(page_num, prev_val, offset);

	if (pre_record.find(u) == pre_record.end())
	{
		pre_record[u] = w;
	}

	pthread_mutex_unlock(&trx_manager_latch);
}
