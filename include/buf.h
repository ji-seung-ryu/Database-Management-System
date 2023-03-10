#ifndef BUF_H
#define BUF_H
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "file.h"
#define TABLE_NUM 15

typedef struct buffer{
	page_t* frame;
	int table_id;
	int page_num;
	bool is_dirty;
	bool is_pinned;
	int next_LRU;
	int prev_LRU;
	pthread_mutex_t page_latch;
}buffer;
int open_table(char* pathname);
int shutdown_db();
int close_table(int table_id);
int get_id (int table_id, int page_num);
int init_db(int num_buf);
int find_buf(int tid);
page_t* buf_read_page(int tid, int page_num);
void buf_write_page(int tid,int page_num,page_t * src);
void set_pin (int tid,int page_num);
void unset_pin (int tid,int page_num);
void release_page_latch(int tid,int page_num);

#endif
