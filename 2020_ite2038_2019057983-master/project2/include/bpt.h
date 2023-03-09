#pragma once

#include <stdio.h>

typedef uint64_t pagenum_t;

#define PAGE_BYTE 4096
#define RESERVED_PAGE 104
#define MAX_VALUE_SIZE 120
#define LEAF_ORDER 32
#define BRANCH_ORDER 249

typedef struct leaf_record{
	int64_t key;	
	char value[MAX_VALUE_SIZE];
}leaf_record;

typedef struct internal_record{
	int64_t key,page_num;
}internal_record;

typedef struct header_page{
	pagenum_t free_page_num;
	pagenum_t root_page_num;
	pagenum_t number_of_pages;
	char reserved[PAGE_BYTE - 3*sizeof(pagenum_t)];
}header_page;

typedef struct free_page{
	pagenum_t next_free_page_num;
	char reserved[PAGE_BYTE - sizeof(pagenum_t)];
}free_page;

typedef struct leaf_page{
	pagenum_t parent_page_num;
	int is_leaf;
	int num_of_keys;
	char reserved[RESERVED_PAGE];
	pagenum_t right_sibling_num;
	leaf_record records[LEAF_ORDER-1];
}leaf_page;

typedef struct internal_page{
	pagenum_t parent_page_num;
	int is_leaf;
	int num_of_keys;
	char reserved[RESERVED_PAGE];
	pagenum_t one_more_pagenum;
	internal_record records[BRANCH_ORDER-1];
}internal_page;

typedef struct page_t{
	int is_header, is_leaf,is_internal,is_free;
	pagenum_t free_page_num;
	pagenum_t root_page_num;
	pagenum_t number_of_pages;
	pagenum_t next_free_page_num;
	pagenum_t parent_page_num;
	int num_of_keys;
	pagenum_t right_sibling_num;
	leaf_record leaf_records[LEAF_ORDER-1];
	pagenum_t one_more_pagenum;
	pagenum_t next;
	pagenum_t page_num;
	internal_record internal_records[BRANCH_ORDER-1];
}page_t;

extern int fd;
extern int ever_page_num;
extern page_t header;
int length_to_root(pagenum_t here);
int open_table(char* pathname);
void db_print_leaves();
void print_all( );

int db_insert_into_node_after_splitting(page_t root, page_t old_node,pagenum_t old_node_offset, int left_index, int key, page_t right,pagenum_t right_offset);
int db_insert_into_node(page_t root, page_t n, pagenum_t n_offset,int left_index, int key, pagenum_t right_offset); 
void db_insert_into_leaf_after_splitting(page_t root, page_t leaf,pagenum_t leaf_offset, int64_t  key, char * value);
void db_insert_into_leaf(page_t leaf,pagenum_t leaf_offset, int key, char* value );
int db_insert_into_parent(page_t root, page_t left, pagenum_t left_offset,int key, page_t right,pagenum_t right_offset);
int db_get_left_index(page_t parent, int64_t left_key);
int db_insert_into_new_root(page_t left,pagenum_t left_offset, int key, page_t right,pagenum_t right_offset);

int open_table(char* pathname);
void db_print_leaves();
int db_insert (int64_t key, char * value);
int db_start_new_tree(int64_t key, char * value);
int64_t db_find_leaf(int key);

int db_adjust_root(page_t root);

int db_find (int64_t key, char * ret_val);
int db_delete_entry(pagenum_t key_offset, int key);
int db_coalesce_nodes(page_t root, page_t n, page_t neighbor,pagenum_t neighbor_offset,int neighbor_index,int k_prime);
int db_remove_entry_from_node(page_t* n, int key);
int db_get_neighbor_index(page_t n, pagenum_t n_offset);
int db_delete(int64_t key);

