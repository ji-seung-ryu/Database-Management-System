#include <iostream>
#include <string.h>
#include "buf.h"
#include <map>

using namespace std;
buffer *B;
map <int, int> page_buf;
// 1e6 * table_id + page_num -> buf

int head, tail;
int get_id (int table_id, int page_num) {
	return (int)1e6 * table_id + page_num;
}
int init_db (int num_buf){
	B = (buffer *)malloc(sizeof(buffer) * (num_buf+2));
	memset(B,0,sizeof(buffer)* num_buf);

	for (int i=0;i<=num_buf+1;i++){
		B[i].frame = (page_t*) malloc(PAGE_BYTE);
		B[i].next_LRU = i+1;
		B[i].prev_LRU = i-1;
		B[i].page_num = -1;
	}
	head = 0;
	tail = num_buf+1;
}

int find_buf(int tid){
	
	int ret;

	for (int i=tail;i!= head;){
//		printf("%d ",i);
		if (i!= tail && !B[i].is_pinned){
			ret= i;
			break;
		}
		i= B[i].prev_LRU;
	}
	//for (int i=tail;i!=head; i=B[i].prev_LRU) printf("%d ",i);
	//printf("\n");
	//for (int i=head; i!= tail; i=B[i].next_LRU) printf("%d ",i);
//	printf("ret: %d\n", ret);
	B[B[ret].prev_LRU].next_LRU = B[ret].next_LRU;
	B[B[ret].next_LRU].prev_LRU = B[ret].prev_LRU;
	
	B[B[head].next_LRU].prev_LRU = ret;
	B[ret].next_LRU= B[head].next_LRU;
	B[ret].prev_LRU= head;
	B[head].next_LRU = ret;

//	B[ret].table_id = tid;
	B[head].next_LRU = ret; 

	if (B[ret].is_dirty) {
		file_write_page(B[ret].table_id, B[ret].page_num, B[ret].frame);
		B[ret].is_dirty = false;
	}
	
	if (B[ret].page_num != -1 ){
		page_buf[get_id(B[ret].table_id,B[ret].page_num)] = 0;
	} 
	return ret;	
}
page_t* buf_read_page (int tid, int page_num){
	
//page_buf mmodify
	int possible_buf;
	
//	printf("buf: %d \n", page_buf[get_id(tid,page_num)]);
	// no buf for page_num
	if(page_buf[get_id(tid, page_num)]== 0){
		possible_buf = find_buf(tid);
		page_buf[get_id(tid,page_num)] = possible_buf; 
		B[possible_buf].page_num =page_num;
		B[possible_buf].table_id = tid;
		file_read_page(tid, page_num, B[possible_buf].frame);
//		B[possible_buf].is_pinned = true;
	}
	// has buf for page_num 
	else{
		possible_buf= page_buf[get_id(tid,page_num)];
//		B[possible_buf].is_pinned = true;
	}

	return B[possible_buf].frame;
}

void buf_write_page(int tid, int page_num, page_t * src){
	int possible_buf;

//	printf("%d %d %d\n", tid, page_num , page_buf[get_id(tid, page_num)]);
	// no buf for page_num
	if(page_buf[get_id(tid, page_num)]== 0){
		possible_buf = find_buf(tid);
		page_buf[get_id(tid,page_num)]= possible_buf;
		B[possible_buf].page_num =page_num;
		B[possible_buf].table_id = tid;
		B[possible_buf].frame = src;
//		B[possible_buf].is_dirty = 1;
	}
	// has buf for page_num 
	else{
		possible_buf= page_buf[get_id(tid,page_num)];
		B[possible_buf].frame = src; 
//		printf("here %d", possible_buf);
	}
	B[possible_buf].is_dirty = 1; 
//	B[possible_buf].is_pinned = false;
//	printf("table_id %d page_num %d buffer %d\n",tid, page_num, possible_buf);
}

void set_pin (int tid, int page_num ) {
	int possible_buf = page_buf[get_id(tid,page_num)];
	B[possible_buf].is_pinned = true; 
}

void unset_pin (int tid,int page_num){
	int possible_buf = page_buf[get_id(tid,page_num)];
	B[possible_buf].is_pinned = false; 
}
int shutdown_db(){
	for (int i=1;i<tail;i++){
		if (B[i].is_dirty) {
	//		printf("%d ",i);
			file_write_page(B[i].table_id, B[i].page_num, B[i].frame);
		}
// problem about is_dirty 
	}
	for (int i=1;i<=TABLE_NUM;i++) close(i+2);
// close?
	return 0;
}

int close_table(int table_id){
	for (int i=1;i<tail;i++){
		if (B[i].is_dirty && B[i].table_id == table_id){
			file_write_page(B[i].table_id, B[i].page_num,B[i].frame);
		}
	}
	close(id_fd[table_id]);
	return 0;
}
