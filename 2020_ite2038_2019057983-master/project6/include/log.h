#ifndef LOG_H
#define LOG_H

#define log_BCR_size 28
#define log_update_size 288
#define log_compensate_size 296
 
#include <iostream>
#include <vector> 
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "file.h"
#include <map> 
#include "buf.h"

using namespace std;

typedef struct update_log update_log;
typedef struct compensate_log compensate_log; 
typedef struct BCR_log BCR_log; // for begin, commit, rollback

#pragma pack (push, 1)

struct update_log{
	int log_size;
	int64_t lsn;
	int64_t prev_lsn;
	int trx_id;
	int type;
	int table_id;
	int page_num;
	int offset;
	int data_len;
	char old_image[120];
	char new_image[120];
};

struct compensate_log{
	int log_size;	
	int64_t lsn;
	int64_t prev_lsn;
	int trx_id;
	int type;
	int table_id;
	int page_num;
	int offset;
	int data_len;
	char old_image[120];
	char new_image[120];
	int64_t next_undo_lsn;
};

struct BCR_log{
	int log_size;
	int64_t lsn;
	int64_t prev_lsn;
	int trx_id;
	int type;
};

#pragma pack(pop)
void log_BCR_write (int trx_id, int type);
int log_update_write(int trx_id, int type, int table_id, int page_num, int offset, int data_len, char* old_image, char* new_image);
void log_compensate_write(int trx_id, int type, int table_id, int page_num, int offset, int data_len, char* old_image, char* new_image,int64_t next_undo_lsn);
void log_write_disk (int until_lsn,int type);
void log_analysis ();
void log_redo(int redo_tot);
void log_undo(int undo_tot);
#endif
