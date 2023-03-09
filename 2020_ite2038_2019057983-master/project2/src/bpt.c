#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "bpt.h"
#include <fcntl.h>
#include "file.h"

#define MAX 10000
int front=-1;
int rear=-1;
int q[MAX];
int table_id;

int IsEmpty(void){
    if(front==rear)//front와 rear가 같으면 큐는 비어있는 상태
        return 1;
    else return 0;
}
int IsFull(void){
    int tmp=(rear+1)%MAX; //원형 큐에서 rear+1을 MAX로 나눈 나머지값이
    if(tmp==front)//front와 같으면 큐는 가득차 있는 상태
        return 1;
    else
        return 0;
}
void addq(int value){
    if(IsFull())
        printf("Queue is Full.\n");
    else{
         rear = (rear+1)%MAX;
         q[rear]=value;
        }

}
int deleteq(){
    if(IsEmpty())
        printf("Queue is Empty.\n");
    else{
        front = (front+1)%MAX;
        return q[front];
    }
}

int open_table(char* pathname){
	if((fd = open(pathname,O_CREAT|O_RDWR|O_SYNC,0644)) ==-1){
		printf("open error");		
		return -1;
	}

	memset(&header,0,sizeof(page_t));
	header.is_header =1;
	file_read_page(0,&header);
	if (header.number_of_pages == 0) header.number_of_pages =1;
	return table_id++; 
}

void db_print_leaves() {
    int i;
	page_t c;
	memset(&c,0,sizeof(page_t));
	file_read_page(header.root_page_num, &c);
	while (!c.is_leaf){
		file_read_page(c.one_more_pagenum,&c);
   
	}
	while (1) {
        for (i = 0; i < c.num_of_keys; i++) {
            printf("%ld ", c.leaf_records[i].key);
        }
		printf(" <%ld>", c.page_num);
        if (c.right_sibling_num != 0) {
            printf(" | ");
			c.is_leaf= 1;
			file_read_page(c.right_sibling_num,&c);
        }
        else
            break;
    }
	printf("\n");
}

void print_all( ) {
	page_t root;
	memset(&root,0,sizeof(page_t));
	file_read_page(header.root_page_num, &root);
	page_t n;
	
	int i = 0;
    int rank = 0;
    int new_rank = 0;
	addq(header.root_page_num);
	
	while(!IsEmpty() ) {
		memset(&n,0,sizeof(page_t));
       	file_read_page( deleteq(),&n);
		
		if (n.parent_page_num != 0 ) {
			page_t parent;
            memset(&parent,0,sizeof(page_t));
			file_read_page(n.parent_page_num, &parent);
			new_rank = length_to_root(n.page_num);
            if (parent.one_more_pagenum == n.page_num&& new_rank != rank) {
                rank = new_rank;
                printf("\n");
            }
        }
		if (n.is_leaf){
            for (i = 0; i < n.num_of_keys; i++) {
                printf("%ld ", n.leaf_records[i].key);
			}
        }
		
		else{
			for (int i=0;i<n.num_of_keys;i++){
				printf("%ld ", n.internal_records[i].key);

			}		
       
			if (n.one_more_pagenum!=0)	addq(n.one_more_pagenum);
            for (i = 0; i < n.num_of_keys; i++)
                addq(n.internal_records[i].page_num);

		}
		
		printf(" <%ld>", n.page_num);
        printf("| ");
    }
    printf("\n");
}

int length_to_root(pagenum_t here ) {
    if (here == header.root_page_num){
		return 0;
	}
	page_t p;
	memset(&p,0,sizeof(page_t));
	file_read_page(here,&p);

	return 1+ length_to_root(p.parent_page_num);
}


int db_insert_into_node_after_splitting(page_t root, page_t old_node,pagenum_t old_node_offset, int left_index,int key, page_t right, pagenum_t right_offset) {

    int i, j, split, k_prime;
    page_t new_node, child;
	memset(&new_node, 0, sizeof(page_t)), memset(&child, 0,sizeof(page_t));
	new_node.is_internal = 1; 
	pagenum_t new_node_offset= file_alloc_page();

	internal_record * tmp = (internal_record *) malloc(sizeof(internal_record)*(BRANCH_ORDER+1));

    for (i = 0, j = 0; i < old_node.num_of_keys; i++, j++) {
        if (j == left_index) j++;
        tmp[j].key = old_node.internal_records[i].key;
		tmp[j].page_num = old_node.internal_records[i].page_num;
    }

	tmp[left_index].key = key;
	tmp[left_index].page_num = right_offset;

    split = BRANCH_ORDER/2;
    old_node.num_of_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_node.internal_records[i].page_num = tmp[i].page_num;
        old_node.internal_records[i].key = tmp[i].key;
        old_node.num_of_keys++;
    }
    
	k_prime = tmp[split-1].key;
	new_node.one_more_pagenum = tmp[split-1].page_num;
	for (++i, j = 0; i < BRANCH_ORDER; i++, j++) {     
        new_node.internal_records[j].page_num = tmp[i].page_num;
        new_node.internal_records[j].key = tmp[i].key;
        new_node.num_of_keys++;
    }

    new_node.parent_page_num = old_node.parent_page_num;


    for (i = 0; i <= new_node.num_of_keys; i++) {

		pagenum_t child_offset;
       	if (i!=0) child_offset = new_node.internal_records[i-1].page_num;
		else child_offset = new_node.one_more_pagenum;
		file_read_page(child_offset,&child);
        child.parent_page_num = new_node_offset;
		file_write_page(child_offset,&child);
    }
		
	free(tmp);
	
	file_write_page(new_node_offset, &new_node);
	file_write_page(old_node_offset, &old_node);
    return db_insert_into_parent(root, old_node,old_node_offset, k_prime, new_node,new_node_offset);
}


int db_insert_into_new_root(page_t left,pagenum_t left_offset, int key, page_t right,pagenum_t right_offset) {
	page_t root;
	memset(&root,0,sizeof(page_t));
	root.is_internal = 1;
	root.is_leaf= 0;
	root.internal_records[0].key = key;
	root.one_more_pagenum = left_offset;
    root.internal_records[0].page_num = right_offset;	 
    root.num_of_keys++;
    root.parent_page_num = 0;
   
	header.root_page_num = file_alloc_page();
	left.parent_page_num = header.root_page_num;
    right.parent_page_num = header.root_page_num;

	file_write_page(header.root_page_num,&root);
	file_write_page(left_offset, &left);
	file_write_page(right_offset,&right);	
    return 0;
}

///* Helper function used in
//_parent
// * to find the index which is located left
// if one_more_pagenum return -1

int db_get_left_index(page_t parent, int64_t left_key) {

    int left_index = 0;

    while (left_index < parent.num_of_keys && parent.internal_records[left_index].key <left_key )	left_index+=1;

    return left_index;
}

int db_insert_into_node(page_t root, page_t n, pagenum_t n_offset,int left_index, int key, pagenum_t right_offset) {
    int i;

    for (i = n.num_of_keys; i > left_index; i--) {
        n.internal_records[i] = n.internal_records[i-1];
    }
    n.internal_records[left_index].page_num = right_offset;
    n.internal_records[left_index].key = key;
    n.num_of_keys++;

	file_write_page(n_offset,&n);
    return 0;
}
 

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */

int db_insert_into_parent(page_t root, page_t left, pagenum_t left_offset,int key, page_t right,pagenum_t right_offset) {
    int left_index;
    page_t parent;
	memset(&parent, 0, sizeof(page_t));
	parent.is_internal= 1;
	pagenum_t parent_offset= left.parent_page_num;
	
	/* Case: new root. */

    if (parent_offset == 0)
        return db_insert_into_new_root(left,left_offset, key,right,right_offset);

    
	file_read_page(parent_offset, &parent);
	/* Case: leaf or node. (Remainder of
     * function body.)
     */

    /* Find the parent's pointer to the left
     * node.
     */

    left_index = db_get_left_index(parent, key);

    /* Simple case: the new key fits into the node.
     */

    if (parent.num_of_keys < BRANCH_ORDER - 1){
		db_insert_into_node(root, parent, parent_offset, left_index, key, right_offset);
		file_write_page(left_offset,&left);
		file_write_page(right_offset,&right);
		return 0;

	}
    /* Harder case:  split a node in order
     * to preserve the B+ tree properties.
     */
	
   db_insert_into_node_after_splitting(root, parent,parent_offset, left_index, key, right,right_offset);
   return 0;	
}


void db_insert_into_leaf(page_t leaf,pagenum_t leaf_offset, int key, char* value ) {
	
    int i, insertion_point;
	
    insertion_point = 0;
    while (insertion_point < leaf.num_of_keys && leaf.leaf_records[insertion_point].key < key)
        insertion_point++;

    for (i = leaf.num_of_keys; i > insertion_point; i--) {
        leaf.leaf_records[i].key = leaf.leaf_records[i-1].key;
        strcpy(leaf.leaf_records[i].value, leaf.leaf_records[i-1].value);
    }

    leaf.leaf_records[insertion_point].key = key;
	strcpy(leaf.leaf_records[insertion_point].value, value);
    leaf.num_of_keys++;

	file_write_page(leaf_offset,&leaf);
    return;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
void db_insert_into_leaf_after_splitting(page_t root, page_t leaf,pagenum_t leaf_offset, int64_t key, char * value) {

    page_t new_leaf;
	memset(&new_leaf,0,sizeof(page_t));
	new_leaf.is_leaf = 1;

	pagenum_t new_leaf_offset = file_alloc_page();

    int insertion_index, split, new_key, i, j;

	leaf_record * tmp = (leaf_record *) malloc(sizeof(leaf_record)*(LEAF_ORDER+1));	
    insertion_index = 0;
    while (insertion_index < leaf.num_of_keys && leaf.leaf_records[insertion_index].key < key)
        insertion_index++;
	
    for (i = 0, j = 0; i < leaf.num_of_keys; i++, j++) {
        if (j == insertion_index) j++;
        tmp[j].key = leaf.leaf_records[i].key;
		strcpy(tmp[j].value, leaf.leaf_records[i].value);
    }

    tmp[insertion_index].key = key;
	strcpy(tmp[insertion_index].value, value);

    leaf.num_of_keys = 0;

    
   split = LEAF_ORDER/2;
    for (i = 0; i < split; i++) {
        strcpy(leaf.leaf_records[i].value,tmp[i].value);
        leaf.leaf_records[i].key = tmp[i].key;
        leaf.num_of_keys++;
    }

    for (i = split, j = 0; i < LEAF_ORDER; i++, j++) {
        strcpy(new_leaf.leaf_records[j].value,tmp[i].value);
        new_leaf.leaf_records[j].key = tmp[i].key;
        new_leaf.num_of_keys++;
    }


	new_leaf.right_sibling_num =  leaf.right_sibling_num;
	leaf.right_sibling_num = new_leaf_offset;

    new_leaf.parent_page_num = leaf.parent_page_num;
    new_key = new_leaf.leaf_records[0].key;

	file_write_page(new_leaf_offset, &new_leaf);
	file_write_page(leaf_offset, &leaf);

	free(tmp);

	db_insert_into_parent(root, leaf, leaf_offset, new_key, new_leaf,new_leaf_offset);
}


int db_start_new_tree(int64_t key, char * value){
	page_t root;
	memset(&root,0,sizeof(page_t));
	root.is_leaf =1;
	root.leaf_records[0].key= key, strcpy(root.leaf_records[0].value,value);
	root.right_sibling_num = 0;
	root.num_of_keys= 1;
	root.is_leaf =1; root.parent_page_num = 0;

	pagenum_t root_offset = file_alloc_page();
	header.root_page_num = root_offset;

	file_write_page(root_offset, &root);
	return 0;
}
//  If success, return 0. Otherwise, return non-zero

int db_insert (int64_t key, char * value){

	page_t root;
	memset(&root,0,sizeof(page_t));
	file_read_page(header.root_page_num, &root);
	
    /* The current implementation ignores
     * duplicates.
     */
	char * tmp = (char *)malloc(sizeof(char)*150);
    if (db_find(key, tmp) == 0){
     	printf("already exist\n");
		file_write_page(0,&header);
		return -1;
	}
	

	if (root.num_of_keys == 0){
		db_start_new_tree(key, value);
		file_write_page(0,&header);
		return 0;
	}

    int64_t leaf_offset =db_find_leaf(key);
	page_t leaf;
	memset(&leaf,0,sizeof(page_t));
	leaf.is_leaf = 1;
	file_read_page(leaf_offset, &leaf);
    /* Case: leaf has room for key and pointer.
     */

    if (leaf.num_of_keys < LEAF_ORDER - 1) {
		db_insert_into_leaf(leaf, leaf_offset,key, value);
		file_write_page(0,&header);
    	return 0;
	}


    /* Case:  leaf must be split.
     */

   	db_insert_into_leaf_after_splitting(root, leaf,leaf_offset, key, value);
	file_write_page(0,&header);
	return 0;
	}
/* 
 * Returns the leaf pagenum containing the given key.
 * if there is no tree, return 0;
 */
int64_t db_find_leaf(int key) {
	int i = 0;
	
	if (header.root_page_num == 0) return -1;

	pagenum_t offset = header.root_page_num;
    page_t c;
	memset(&c,0,sizeof(page_t));
	file_read_page(offset,&c);

    while (!c.is_leaf) {
        i = 0;

        while (i < c.num_of_keys) {
            if (key >= c.internal_records[i].key) i++;
            else break;
        }

		if (i==0) offset = c.one_more_pagenum;
		else offset = c.internal_records[i-1].page_num;
		c.is_free= 0, c.is_internal =0;
    	file_read_page(offset, &c);
    }

    return offset; 
}
// if find, return 0;
// else return -1;
int db_find (int64_t key, char * ret_val){

	pagenum_t offset= db_find_leaf(key);
    
	if (offset == -1 ) return -1;
	int i=0;
	page_t c;
	memset(&c,0,sizeof(page_t));
	c.is_leaf = 1;
	file_read_page(offset,&c);
	
    for (i = 0; i < c.num_of_keys; i++)
        if (c.leaf_records[i].key == key) break;
    if (i == c.num_of_keys)
        return -1;
    else{
		strcpy(ret_val, c.leaf_records[i].value);
        return 0;
	}

}

int db_adjust_root(page_t root){
	page_t new_root;
	memset(&new_root, 0,sizeof(page_t));
	if (root.num_of_keys > 0 ) return 0;

	if (!root.is_leaf) {
		file_read_page(root.one_more_pagenum, &new_root);
		new_root.parent_page_num =0;

	}
	else {
		new_root.page_num = file_alloc_page();
	}
	header.root_page_num = new_root.page_num;
	file_write_page(new_root.page_num, &new_root);

	return 0;
}

int db_get_neighbor_index(page_t n, pagenum_t n_offset){
	int i;

	page_t parent;
	memset(&parent,0,sizeof(page_t));
	file_read_page(n.parent_page_num, &parent);

	if (parent.one_more_pagenum == n_offset) return -1;

	for (i=0;i<parent.num_of_keys;i++){
		if (parent.internal_records[i].page_num == n_offset) return i;
	}

}

int db_remove_entry_from_node(page_t* n, int key){
	int i=0;

	if (n->is_leaf) while (n->leaf_records[i].key != key) i++;
	if (n->is_internal) while(n->internal_records[i].key != key) i++; 

	for (++i; i<n->num_of_keys; i++){
		if (n->is_leaf){
			n->leaf_records[i-1].key = n->leaf_records[i].key;
			strcpy(n->leaf_records[i-1].value, n->leaf_records[i].value);
		}
		if (n->is_internal){
			n->internal_records[i-1].key = n->internal_records[i].key;
			n->internal_records[i-1].page_num = n->internal_records[i].page_num;
		}
	}

	n->num_of_keys --; 

	return 0;
}

int db_coalesce_nodes(page_t root, page_t n, page_t neighbor,pagenum_t neighbor_offset,int neighbor_index,int k_prime){

	page_t tmp;
	pagenum_t temp;
	int i,j,neighbor_insertion_index,n_end;

	if (neighbor_index== -1) {
		temp =neighbor_offset;
		neighbor_offset= n.page_num;
		n.page_num =temp;
		tmp = n;
		n= neighbor;
		neighbor = tmp;
	}

	neighbor_insertion_index = neighbor.num_of_keys;

	if (!n.is_leaf){

		neighbor.internal_records[neighbor_insertion_index].key = k_prime;
		neighbor.internal_records[neighbor_insertion_index].page_num = n.one_more_pagenum;
		neighbor.num_of_keys += 1;

		n_end = n.num_of_keys;

		for (i=neighbor_insertion_index+1,j=0;j<n_end;i++,j++){
			neighbor.internal_records[i].key= n.internal_records[j].key;
			neighbor.internal_records[i].page_num = n.internal_records[j].page_num;
			neighbor.num_of_keys +=1;
			n.num_of_keys -=1;
		}

		memset(&tmp,0,sizeof(page_t));

		for (i=0;i<neighbor.num_of_keys;i++){
			file_read_page(neighbor.internal_records[i].page_num, &tmp);
			tmp.parent_page_num = neighbor_offset;
			file_write_page(neighbor.internal_records[i].page_num,&tmp);
		}

		file_read_page(neighbor.one_more_pagenum, &tmp);
		tmp.parent_page_num = neighbor_offset;
		file_write_page(neighbor.one_more_pagenum,&tmp);


	}

	else{
		for (i= neighbor_insertion_index, j=0;j<n.num_of_keys;i++, j++){
			neighbor.leaf_records[i].key= n.leaf_records[j].key;
			strcpy(neighbor.leaf_records[i].value, n.leaf_records[j].value); 
			neighbor.num_of_keys+=1;
		}
		neighbor.right_sibling_num = n.right_sibling_num;
		
	}

	file_write_page(neighbor_offset, &neighbor);
	file_write_page(n.page_num,&n);
	db_delete_entry(n.parent_page_num, k_prime);

	return 0;

}

int db_delete_entry(pagenum_t key_offset, int key){
	page_t root;
	page_t n;
	page_t neighbor;
	page_t parent;
	memset(&root,0,sizeof(page_t)), memset(&n,0,sizeof(page_t));
	memset(&neighbor,0,sizeof(page_t)), memset(&parent, 0,sizeof(page_t));

	int neighbor_index, k_prime_index, k_prime;

	file_read_page(header.root_page_num, &root);
	file_read_page(key_offset, &n);

	db_remove_entry_from_node(&n,key);
	file_write_page(key_offset, &n);

	if (key_offset == header.root_page_num) return db_adjust_root(n);


	if (n.num_of_keys != 0) {		
		return 0;
	}

	file_read_page(n.parent_page_num , &parent);
	neighbor_index = db_get_neighbor_index(n,key_offset);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = parent.internal_records[k_prime_index].key;
	if (neighbor_index ==-1){
		file_read_page(parent.internal_records[0].page_num, &neighbor); 
	}
	else{
		if (neighbor_index == 0) file_read_page(parent.one_more_pagenum, &neighbor);
		else file_read_page(parent.internal_records[neighbor_index-1].page_num, &neighbor);
	}
	if (neighbor.num_of_keys == BRANCH_ORDER -1) {
		file_write_page(key_offset,&n);
		return 0;
	}

	return db_coalesce_nodes(root,n, neighbor,neighbor.page_num,neighbor_index,k_prime);
}
// if success, return 0
// else return -1
int db_delete(int64_t key){

	char * key_record =(char *)malloc(150);
	int exist= db_find(key, key_record);
	pagenum_t leaf_offset= db_find_leaf(key);

	if (exist != -1 && leaf_offset !=0 ){
		db_delete_entry(leaf_offset,key);
		file_write_page(0,&header);
		return 0;
	}
	file_write_page(0,&header);
	return -1;

}



