#ifndef BPT_H
#define BPT_H
#include <stdio.h>

typedef uint64_t pagenum_t;

#define PAGE_BYTE 4096
#define RESERVED_PAGE 104
#define MAX_VALUE_SIZE 120
#define LEAF_ORDER 32
#define BRANCH_ORDER 249

#pragma pack (push, 1)

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
	pagenum_t page_lsn; 
	char reserved[PAGE_BYTE - 4*sizeof(pagenum_t)];
}header_page;

typedef struct free_page{
	pagenum_t next_free_page_num;
	char first_reserved[16];
	pagenum_t page_lsn; 
	char second_reserved[PAGE_BYTE -16-8 -sizeof(pagenum_t)];
}free_page;

typedef struct leaf_page{
	pagenum_t parent_page_num;
	int is_leaf;
	int num_of_keys;
	char first_reserved[8];
	pagenum_t page_lsn; 
	char second_reserved[RESERVED_PAGE-16];
	pagenum_t right_sibling_num;
	leaf_record records[LEAF_ORDER-1];
}leaf_page;

typedef struct internal_page{
	pagenum_t parent_page_num;
	int is_leaf;
	int num_of_keys;
	char first_reserved[8];
	pagenum_t page_lsn; 
	char reserved[RESERVED_PAGE-16];
	pagenum_t one_more_pagenum;
	internal_record records[BRANCH_ORDER-1];
}internal_page;

typedef struct page_t{
	char contents[PAGE_BYTE];
}page_t;

#pragma pack(pop)
extern int fd; 
extern int id_fd[15];


int length_to_root(int table_id, header_page* header, pagenum_t here);

void print_all(int table_id);

int db_insert_into_node_after_splitting(int table_id, header_page* header, internal_page* old_node,pagenum_t old_node_offset, int left_index, int key, internal_page* right,pagenum_t right_offset);
int db_insert_into_node(int table_id, header_page * header, internal_page* n, pagenum_t n_offset,int left_index, int key, pagenum_t right_offset); 
void db_insert_into_leaf_after_splitting(int table_id, header_page * header, leaf_page* leaf,pagenum_t leaf_offset, int64_t  key, char * value);
void db_insert_into_leaf(int table_id, leaf_page* leaf,pagenum_t leaf_offset, int key, char* value );
int db_insert_into_parent(int table_id, header_page* header, internal_page* left, pagenum_t left_offset,int key, internal_page* right,pagenum_t right_offset);
int db_get_left_index(internal_page * parent, int64_t left_key);
int db_insert_into_new_root(int table_id, header_page* header, internal_page* left,pagenum_t left_offset, int key, internal_page right,pagenum_t right_offset);

int open_table(char* pathname);

int db_insert (int table_id, int64_t key, char * value);
int db_start_new_tree(int table_id, header_page* header, int64_t key, char * value);
int64_t db_find_leaf(int table_id, int key,header_page* header);

int db_adjust_root(int table_id, header_page* header, internal_page* root);

int db_find (int table_id, int64_t key, char * ret_val,int trx_id);
int db_update(int table_id, int64_t key,char* ret_val,int trx_id);
int db_delete_entry(int table_id,header_page* header, pagenum_t key_offset, int key);
int db_coalesce_nodes(int table_id, header_page* header, internal_page* n, pagenum_t n_offset, internal_page* neighbor,pagenum_t neighbor_offset,int neighbor_index,int k_prime);
int db_remove_entry_from_node(int table_id, internal_page* n, int key);
int db_get_neighbor_index(int table_id, internal_page* n, pagenum_t n_offset);
int db_delete(int table_id, int64_t key);
#endif

