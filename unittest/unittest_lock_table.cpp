#include <lock_manager.h>
#include <transaction_manager.h>
#include "buf.h"
#include "bpt.h"
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <log.h>

#define NUM_THREADS 1000

void* thread_func1(void* arg){
	int trx_id1 = trx_begin();
	char * val1 =(char *) malloc(100);


//	if (db_find(1,122,val1,trx_id1) == -1 ) return NULL;
cout<< "db_find "<<trx_id1<<"\n";
	if (db_update(1,50,"xx",trx_id1)== -1) return NULL;

//	if (db_find(1,122,val1,trx_id1) == -1 ) return NULL;
	
//	cout<<val1<<"\n";
//	if (db_update(1,122,"tt",trx_id1)== -1) return NULL;
	
//	if (db_update(1,122,"zz",trx_id1)== -1) return NULL;
cout<< "db_commit "<<trx_id1<<"\n";
	trx_commit(trx_id1);
	return NULL;
}

void* thread_func2(void* arg){
	int trx_id1 = trx_begin();
	char * val1 =(char *) malloc(100);


//	if (db_find(1,122,val1,trx_id1) == -1 ) return NULL;
	cout<< "db_find "<<trx_id1<<"\n";
	if (db_find(1,50,val1,trx_id1)== -1) return NULL;

//	if (db_find(1,122,val1,trx_id1) == -1 ) return NULL;
	
//	cout<<val1<<"\n";
//	if (db_update(1,122,"tt",trx_id1)== -1) return NULL;
	
//	if (db_update(1,122,"zz",trx_id1)== -1) return NULL;
	cout<< "db_commit "<<trx_id1<<"\n";
	trx_commit(trx_id1);
	return NULL;
}

int main(){
	pthread_t threads[NUM_THREADS];
 
	init_lock_table();
	init_db(100,0,100,"logfile.data","logmsg.txt");
	 
	//log_BCR_write(1,1);
	

	int fd = open_table("DATA1");
	
	for (int i=1;i<=100;i++) db_insert(fd, i,"tt"); 
	

	//cout<<open_table("d.txt")<<"\n";

	


	for (int i=0;i<NUM_THREADS;i++){
			
		if (i%2) pthread_create(&threads[i],NULL,thread_func1,NULL);
		else pthread_create(&threads[i],NULL,thread_func2,NULL);
	}

	long ret;
	for (int i=0;i<NUM_THREADS;i++){
		pthread_join(threads[i], (void**)&ret);
	
	}
	fclose(logmsg_fp);
	close(log_fd);
	close_table(fd);

	return 0;
}



