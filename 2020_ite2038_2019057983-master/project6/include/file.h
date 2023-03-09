#ifndef FILE_H
#define FILE_H
#include <stdint.h>
#include <stdio.h>
#include "bpt.h"
#include <log.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

void make_free_page(int table_id, pagenum_t from);
pagenum_t file_alloc_page(int table_id, header_page* header);

void file_free_page(int table_id, pagenum_t pagenum);

void file_read_page(int table_id, pagenum_t pagenum,page_t* dest);
void* file_read_log(int64_t offset,  int* log_size); 

void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);
void file_write_log(int log_id, int64_t offset, void* src,int log_size); 

#endif
