#include <lock_manager.h>
#include <transaction_manager.h>
#include "buf.h"
#include "bpt.h"
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>

#define NUM_THREADS 100

void* thread_func1(void* arg){
	int trx_id1 = trx_begin();
	char * val1 =(char *) malloc(100);


	if (db_find(1,122,val1,trx_id1) == -1 ) return NULL;

	if (db_update(1,122,"xx",trx_id1)== -1) return NULL;

	if (db_find(1,122,val1,trx_id1) == -1 ) return NULL;
	
	cout<<val1<<"\n";
	if (db_update(1,122,"tt",trx_id1)== -1) return NULL;
	
	if (db_update(1,122,"zz",trx_id1)== -1) return NULL;

	trx_commit(trx_id1);
	return NULL;
}


int main(){
	pthread_t threads[NUM_THREADS];
 
	init_lock_table();
	init_db(5);
	
	cout<<open_table("d.txt")<<"\n";




	for (int i=0;i<NUM_THREADS;i++){
		

	
		pthread_create(&threads[i],NULL,thread_func1,NULL);
	}

	long ret;
	for (int i=0;i<NUM_THREADS;i++){
		pthread_join(threads[i], (void**)&ret);
	
	}
	close_table(1);

	return 0;
}



