#define _GNU_SOURCE
#include <queue>
#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "bpt.h"
#include <fcntl.h>
#include <lock_manager.h>
#include <log.h>
#include "buf.h"
#include <map>
#define MAX 10000

using namespace std;

int front = -1;
int rear = -1;
int q[MAX];

int IsEmpty(void)
{
	if (front == rear)
		return 1;
	else
		return 0;
}
int IsFull(void)
{
	int tmp = (rear + 1) % MAX;
	if (tmp == front)
		return 1;
	else
		return 0;
}
void addq(int value)
{
	if (IsFull())
		printf("Queue is Full.\n");
	else
	{
		rear = (rear + 1) % MAX;
		q[rear] = value;
	}
}
int deleteq()
{
	if (IsEmpty())
		printf("Queue is Empty.\n");
	else
	{
		front = (front + 1) % MAX;
		return q[front];
	}
}

void print_all(int table_id)
{

	internal_page *n;
	header_page *header = (header_page *)buf_read_page(table_id, 0);
	internal_page *root = (internal_page *)buf_read_page(table_id, header->root_page_num);

	int i = 0;
	int rank = 0;
	int new_rank = 0;
	addq(header->root_page_num);

	while (!IsEmpty())
	{

		pagenum_t dq = deleteq();
		n = (internal_page *)buf_read_page(table_id, dq);

		if (n->parent_page_num != 0)
		{

			internal_page *parent = (internal_page *)buf_read_page(table_id, n->parent_page_num);

			new_rank = length_to_root(table_id, header, dq);

			if (parent->one_more_pagenum == dq && new_rank != rank)
			{
				rank = new_rank;
				printf("\n");
			}
		}
		if (n->is_leaf)
		{
			leaf_page *lp = (leaf_page *)n;
			for (i = 0; i < lp->num_of_keys; i++)
			{
				printf("%ld ", lp->records[i].key);
			}
		}

		else
		{
			for (int i = 0; i < n->num_of_keys; i++)
			{
				printf("%ld ", n->records[i].key);
			}

			if (n->one_more_pagenum != 0)
				addq(n->one_more_pagenum);
			for (i = 0; i < n->num_of_keys; i++)
				addq(n->records[i].page_num);
		}

		printf(" <%ld> [%ld]", dq, n->parent_page_num);
		printf("| ");
	}
	buf_write_page(table_id, header->root_page_num, (page_t *)root);
	buf_write_page(table_id, 0, (page_t *)header);
	printf("finish print all\n");
}

int length_to_root(int table_id, header_page *header, pagenum_t here)
{

	if (here == header->root_page_num)
	{
		return 0;
	}
	internal_page *p = (internal_page *)buf_read_page(table_id, here);

	return 1 + length_to_root(table_id, header, p->parent_page_num);
}

int db_insert_into_node_after_splitting(int table_id, header_page *header, internal_page *old_node, pagenum_t old_node_offset, int left_index, int key, internal_page *right, pagenum_t right_offset)
{

	int i, j, split, k_prime;
	internal_page *new_node = (internal_page *)malloc(PAGE_BYTE);
	internal_page *child = (internal_page *)malloc(PAGE_BYTE);
	memset(new_node, 0, PAGE_BYTE), memset(child, 0, PAGE_BYTE);
	pagenum_t new_node_offset = file_alloc_page(table_id, header);

	internal_record *tmp = (internal_record *)malloc(sizeof(internal_record) * (BRANCH_ORDER + 1));

	for (i = 0, j = 0; i < old_node->num_of_keys; i++, j++)
	{
		if (j == left_index)
			j++;
		tmp[j].key = old_node->records[i].key;
		tmp[j].page_num = old_node->records[i].page_num;
	}

	tmp[left_index].key = key;
	tmp[left_index].page_num = right_offset;

	split = BRANCH_ORDER / 2;
	old_node->num_of_keys = 0;
	for (i = 0; i < split - 1; i++)
	{
		old_node->records[i].page_num = tmp[i].page_num;
		old_node->records[i].key = tmp[i].key;
		old_node->num_of_keys++;
	}

	k_prime = tmp[split - 1].key;
	new_node->one_more_pagenum = tmp[split - 1].page_num;
	for (++i, j = 0; i < BRANCH_ORDER; i++, j++)
	{
		new_node->records[j].page_num = tmp[i].page_num;
		new_node->records[j].key = tmp[i].key;
		new_node->num_of_keys++;
	}

	new_node->parent_page_num = old_node->parent_page_num;

	for (i = 0; i <= new_node->num_of_keys; i++)
	{

		pagenum_t child_offset;
		if (i != 0)
			child_offset = new_node->records[i - 1].page_num;
		else
			child_offset = new_node->one_more_pagenum;

		release_page_latch(table_id, child_offset);
		child = (internal_page *)buf_read_page(table_id, child_offset);

		child->parent_page_num = new_node_offset;
		buf_write_page(table_id, child_offset, (page_t *)child);

		release_page_latch(table_id, child_offset);
	}

	free(tmp);

	buf_write_page(table_id, new_node_offset, (page_t *)new_node);
	db_insert_into_parent(table_id, header, old_node, old_node_offset, k_prime, new_node, new_node_offset);
	return 0;
}

int db_get_left_index(internal_page *parent, int64_t left_key)
{

	int left_index = 0;

	while (left_index < parent->num_of_keys && parent->records[left_index].key < left_key)
		left_index += 1;

	return left_index;
}

int db_insert_into_node(int table_id, header_page *header, internal_page *n, pagenum_t n_offset, int left_index, int key, pagenum_t right_offset)
{

	int i;

	for (i = n->num_of_keys; i > left_index; i--)
	{
		n->records[i] = n->records[i - 1];
	}
	n->records[left_index].page_num = right_offset;
	n->records[left_index].key = key;
	n->num_of_keys++;

	return 0;
}

int db_insert_into_new_root(int table_id, header_page *header, internal_page *left, pagenum_t left_offset, int key, internal_page *right, pagenum_t right_offset)
{
	internal_page *root = (internal_page *)malloc(PAGE_BYTE);
	memset(root, 0, PAGE_BYTE);
	root->records[0].key = key;
	root->one_more_pagenum = left_offset;
	root->records[0].page_num = right_offset;
	root->num_of_keys++;
	root->parent_page_num = 0;

	header->root_page_num = file_alloc_page(table_id, header);
	left->parent_page_num = header->root_page_num;
	right->parent_page_num = header->root_page_num;

	buf_write_page(table_id, header->root_page_num, (page_t *)root);

	return 0;
}
int db_insert_into_parent(int table_id, header_page *header, internal_page *left, pagenum_t left_offset, int key, internal_page *right, pagenum_t right_offset)
{
	int left_index;
	pagenum_t parent_offset = left->parent_page_num;

	/* Case: new root. */

	if (parent_offset == 0)
		return db_insert_into_new_root(table_id, header, left, left_offset, key, right, right_offset);

	internal_page *parent = (internal_page *)buf_read_page(table_id, parent_offset);

	/* Case: leaf or node. (Remainder of
	 * function body.)
	 */

	/* Find the parent's pointer to the left
	 * node.
	 */

	left_index = db_get_left_index(parent, key);

	/* Simple case: the new key fits into the node.
	 */

	if (parent->num_of_keys < BRANCH_ORDER - 1)
	{
		db_insert_into_node(table_id, header, parent, parent_offset, left_index, key, right_offset);

		buf_write_page(table_id, parent_offset, (page_t *)parent);
		release_page_latch(table_id, parent_offset);

		return 0;
	}
	/* Harder case:  split a node in order
	 * to preserve the B+ tree properties.
	 */

	db_insert_into_node_after_splitting(table_id, header, parent, parent_offset, left_index, key, right, right_offset);
	buf_write_page(table_id, parent_offset, (page_t *)parent);
	release_page_latch(table_id, parent_offset);

	return 0;
}
/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
void db_insert_into_leaf_after_splitting(int table_id, header_page *header, leaf_page *leaf, pagenum_t leaf_offset, int64_t key, char *value)
{

	leaf_page *new_leaf = (leaf_page *)malloc(PAGE_BYTE);
	memset(new_leaf, 0, PAGE_BYTE);
	new_leaf->is_leaf = 1;

	pagenum_t new_leaf_offset = file_alloc_page(table_id, header);

	int insertion_index, split, new_key, i, j;

	leaf_record *tmp = (leaf_record *)malloc(sizeof(leaf_record) * (LEAF_ORDER + 1));
	insertion_index = 0;
	while (insertion_index < leaf->num_of_keys && leaf->records[insertion_index].key < key)
		insertion_index++;

	for (i = 0, j = 0; i < leaf->num_of_keys; i++, j++)
	{
		if (j == insertion_index)
			j++;
		tmp[j].key = leaf->records[i].key;
		strcpy(tmp[j].value, leaf->records[i].value);
	}

	tmp[insertion_index].key = key;
	strcpy(tmp[insertion_index].value, value);

	leaf->num_of_keys = 0;

	split = LEAF_ORDER / 2;
	for (i = 0; i < split; i++)
	{
		strcpy(leaf->records[i].value, tmp[i].value);
		leaf->records[i].key = tmp[i].key;
		leaf->num_of_keys++;
	}

	for (i = split, j = 0; i < LEAF_ORDER; i++, j++)
	{
		strcpy(new_leaf->records[j].value, tmp[i].value);
		new_leaf->records[j].key = tmp[i].key;
		new_leaf->num_of_keys++;
	}

	new_leaf->right_sibling_num = leaf->right_sibling_num;
	leaf->right_sibling_num = new_leaf_offset;

	new_leaf->parent_page_num = leaf->parent_page_num;
	new_key = new_leaf->records[0].key;

	free(tmp);

	buf_write_page(table_id, new_leaf_offset, (page_t *)new_leaf);
	db_insert_into_parent(table_id, header, (internal_page *)leaf, leaf_offset, new_key, (internal_page *)new_leaf, new_leaf_offset);
}

void db_insert_into_leaf(int table_id, leaf_page *leaf, pagenum_t leaf_offset, int key, char *value)
{
	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->num_of_keys && leaf->records[insertion_point].key < key)
		insertion_point++;

	for (i = leaf->num_of_keys; i > insertion_point; i--)
	{
		leaf->records[i].key = leaf->records[i - 1].key;
		strcpy(leaf->records[i].value, leaf->records[i - 1].value);
	}

	leaf->records[insertion_point].key = key;
	strcpy(leaf->records[insertion_point].value, value);
	leaf->num_of_keys++;

	return;
}

int db_start_new_tree(int table_id, header_page *header, int64_t key, char *value)
{
	leaf_page *root = (leaf_page *)malloc(PAGE_BYTE);
	memset(root, 0, PAGE_BYTE);
	root->records[0].key = key, strcpy(root->records[0].value, value);
	root->right_sibling_num = 0;
	root->num_of_keys = 1;
	root->parent_page_num = 0;
	root->is_leaf = 1;

	pagenum_t root_offset = file_alloc_page(table_id, header);
	header->root_page_num = root_offset;

	buf_write_page(table_id, root_offset, (page_t *)root);

	return 0;
}

int db_insert(int table_id, int64_t key, char *value)
{

	header_page *header = (header_page *)buf_read_page(table_id, 0);

	/* The current implementation ignores
	 * duplicates.
	 */
	char *tmp = (char *)malloc(sizeof(char) * 150);
	/*    if (db_find(table_id, key, tmp,0)== 0){
			printf("already exist\n");
			buf_write_page(table_id,0,(page_t*)header);
			release_page_latch(table_id, 0);
			return -1;
		}*/

	if (header->root_page_num == 0)
	{

		db_start_new_tree(table_id, header, key, value);

		buf_write_page(table_id, 0, (page_t *)header);
		release_page_latch(table_id, 0);
		return 0;
	}

	pagenum_t leaf_offset = db_find_leaf(table_id, key, header);
	leaf_page *leaf = (leaf_page *)buf_read_page(table_id, leaf_offset);

	/* Case: leaf has room for key and pointer.
	 */

	if (leaf->num_of_keys < LEAF_ORDER - 1)
	{
		db_insert_into_leaf(table_id, leaf, leaf_offset, key, value);
		buf_write_page(table_id, 0, (page_t *)header);
		buf_write_page(table_id, leaf_offset, (page_t *)leaf);

		release_page_latch(table_id, 0);
		release_page_latch(table_id, leaf_offset);

		return 0;
	}

	/* Case:  leaf must be split.
	 */

	db_insert_into_leaf_after_splitting(table_id, header, leaf, leaf_offset, key, value);
	buf_write_page(table_id, 0, (page_t *)header);
	buf_write_page(table_id, leaf_offset, (page_t *)leaf);

	release_page_latch(table_id, 0);
	release_page_latch(table_id, leaf_offset);

	return 0;
}
/*
 * Returns the leaf pagenum containing the given key.
 * if there is no tree, return 0;
 */
int64_t db_find_leaf(int table_id, int key, header_page *header)
{

	int i = 0;
	if (header->root_page_num == 0)
		return -1;

	pagenum_t offset = header->root_page_num;

	internal_page *c = (internal_page *)buf_read_page(table_id, offset);

	while (!c->is_leaf)
	{
		i = 0;

		while (i < c->num_of_keys)
		{
			if (key >= c->records[i].key)
				i++;
			else
				break;
		}
		release_page_latch(table_id, offset);

		if (i == 0)
			offset = c->one_more_pagenum;
		else
			offset = c->records[i - 1].page_num;

		c = (internal_page *)buf_read_page(table_id, offset);
	}

	release_page_latch(table_id, offset);
	return offset;
}

int db_find(int table_id, int64_t key, char *ret_val, int trx_id)
{

	header_page *header = (header_page *)buf_read_page(table_id, 0);
	release_page_latch(table_id, 0);

	pagenum_t offset = db_find_leaf(table_id, key, header);

	if (offset == -1)
		return -1;
	int i = 0;

	leaf_page *c = (leaf_page *)buf_read_page(table_id, offset);

	for (i = 0; i < c->num_of_keys; i++)
	{
		if (c->records[i].key == key)
			break;
	}

	if (i == c->num_of_keys)
	{
		release_page_latch(table_id, offset);
		return -1;
	}
	else
	{
		release_page_latch(table_id, offset);
		lock_t *lock = lock_acquire(table_id, c->records[i].key, trx_id, 0, offset);
		c = (leaf_page *)buf_read_page(table_id, offset);

		strcpy(ret_val, c->records[i].value);
		release_page_latch(table_id, offset);
		return 0;
	}
}
int db_update(int table_id, int64_t key, char *values, int trx_id)
{

	header_page *header = (header_page *)buf_read_page(table_id, 0);
	release_page_latch(table_id, 0);
	pagenum_t offset = db_find_leaf(table_id, key, header);

	if (offset == -1)
	{

		return -1;
	}
	int i = 0;
	leaf_page *c = (leaf_page *)buf_read_page(table_id, offset);

	for (i = 0; i < c->num_of_keys; i++)
	{

		if (c->records[i].key == key)
			break;
	}

	if (i == c->num_of_keys)
	{
		release_page_latch(table_id, offset);
		return -1;
	}
	else
	{
		int page_lsn = log_update_write(trx_id, 1, table_id, offset, 128 + sizeof(leaf_record) * i, MAX_VALUE_SIZE, c->records[i].value, values);
		release_page_latch(table_id, offset);

		trx_backup(trx_id, table_id, key, offset, 128 + sizeof(leaf_record) * i, c->records[i].value);

		lock_t *lock = lock_acquire(table_id, c->records[i].key, trx_id, 1, offset);
		strcpy(c->records[i].value, values);
		c->page_lsn = page_lsn;
		c = (leaf_page *)buf_read_page(table_id, offset);
		buf_write_page(table_id, offset, (page_t *)c);
		release_page_latch(table_id, offset);

		return 0;
	}
}
int db_adjust_root(int table_id, header_page *header, internal_page *root)
{
	internal_page *new_root;

	if (root->num_of_keys > 0)
		return 0;

	if (!root->is_leaf)
	{

		new_root = (internal_page *)buf_read_page(table_id, root->one_more_pagenum);
		new_root->parent_page_num = 0;
		header->root_page_num = root->one_more_pagenum;
	}
	else
	{
		new_root = (internal_page *)malloc(PAGE_BYTE);
		memset(new_root, 0, PAGE_BYTE);
		header->root_page_num = file_alloc_page(table_id, header);
	}

	buf_write_page(table_id, header->root_page_num, (page_t *)new_root);
	unset_pin(table_id, header->root_page_num);

	return 0;
}

int db_get_neighbor_index(int table_id, internal_page *n, pagenum_t n_offset)
{
	int i;

	internal_page *parent = (internal_page *)buf_read_page(table_id, n->parent_page_num);

	if (parent->one_more_pagenum == n_offset)
		return -1;

	for (i = 0; i < parent->num_of_keys; i++)
	{
		if (parent->records[i].page_num == n_offset)
			return i;
	}

	buf_write_page(table_id, n->parent_page_num, (page_t *)parent);
}

int db_remove_entry_from_node(int table_id, internal_page *n, pagenum_t n_offset, int key)
{
	int i = 0;

	if (n->is_leaf)
	{
		leaf_page *m = (leaf_page *)n;
		while (m->records[i].key != key)
			i++;

		for (++i; i < m->num_of_keys; i++)
		{
			m->records[i - 1].key = m->records[i].key;
			strcpy(m->records[i - 1].value, m->records[i].value);
		}
		m->num_of_keys--;
		buf_write_page(table_id, n_offset, (page_t *)m);
	}
	else
	{
		while (n->records[i].key != key)
			i++;

		for (++i; i < n->num_of_keys; i++)
		{
			n->records[i - 1].key = n->records[i].key;
			n->records[i - 1].page_num = n->records[i].page_num;
		}
		n->num_of_keys--;
		buf_write_page(table_id, n_offset, (page_t *)n);
	}

	return 0;
}

int db_delete_entry(int table_id, header_page *header, pagenum_t key_offset, int key)
{

	internal_page *neighbor;
	internal_page *parent;

	int neighbor_index, k_prime_index, k_prime;
	pagenum_t neighbor_offset;

	internal_page *n = (internal_page *)buf_read_page(table_id, key_offset);
	set_pin(table_id, key_offset);
	db_remove_entry_from_node(table_id, n, key_offset, key);

	if (key_offset == header->root_page_num)
		return db_adjust_root(table_id, header, n);

	if (n->num_of_keys != 0)
	{
		return 0;
	}

	parent = (internal_page *)buf_read_page(table_id, n->parent_page_num);
	neighbor_index = db_get_neighbor_index(table_id, n, key_offset);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = parent->records[k_prime_index].key;
	if (neighbor_index == -1)
	{

		neighbor = (internal_page *)buf_read_page(table_id, parent->records[0].page_num);
		neighbor_offset = parent->records[0].page_num;
	}
	else
	{
		if (neighbor_index == 0)
		{

			neighbor = (internal_page *)buf_read_page(table_id, parent->one_more_pagenum);
			neighbor_offset = parent->one_more_pagenum;
		}
		else
		{

			neighbor = (internal_page *)buf_read_page(table_id, parent->records[neighbor_index - 1].page_num);
			neighbor_offset = parent->records[neighbor_index - 1].page_num;
		}
	}

	if (neighbor->num_of_keys == BRANCH_ORDER - 1)
	{

		buf_write_page(table_id, key_offset, (page_t *)n);
		unset_pin(table_id, key_offset);
		return 0;
	}

	db_coalesce_nodes(table_id, header, n, key_offset, neighbor, neighbor_offset, neighbor_index, k_prime);
	unset_pin(table_id, key_offset);
	buf_write_page(table_id, key_offset, (page_t *)n);

	return 0;
}

int db_coalesce_nodes(int table_id, header_page *header, internal_page *n, pagenum_t n_offset, internal_page *neighbor, pagenum_t neighbor_offset, int neighbor_index, int k_prime)
{

	internal_page *tmp;
	pagenum_t temp;
	int i, j, neighbor_insertion_index, n_end;

	if (neighbor_index == -1)
	{
		temp = neighbor_offset;
		neighbor_offset = n_offset;
		n_offset = temp;
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	neighbor_insertion_index = neighbor->num_of_keys;

	if (!n->is_leaf)
	{

		neighbor->records[neighbor_insertion_index].key = k_prime;
		neighbor->records[neighbor_insertion_index].page_num = n->one_more_pagenum;
		neighbor->num_of_keys += 1;

		n_end = n->num_of_keys;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++)
		{
			neighbor->records[i].key = n->records[j].key;
			neighbor->records[i].page_num = n->records[j].page_num;
			neighbor->num_of_keys += 1;
			n->num_of_keys -= 1;
		}

		for (i = 0; i < neighbor->num_of_keys; i++)
		{

			tmp = (internal_page *)buf_read_page(table_id, neighbor->records[i].page_num);
			tmp->parent_page_num = neighbor_offset;

			buf_write_page(table_id, neighbor->records[i].page_num, (page_t *)tmp);
		}

		tmp = (internal_page *)buf_read_page(table_id, neighbor->one_more_pagenum);
		tmp->parent_page_num = neighbor_offset;

		buf_write_page(table_id, neighbor->one_more_pagenum, (page_t *)tmp);

		buf_write_page(table_id, neighbor_offset, (page_t *)neighbor);

		buf_write_page(table_id, n_offset, (page_t *)n);
		db_delete_entry(table_id, header, n->parent_page_num, k_prime);
	}

	else
	{
		leaf_page *m = (leaf_page *)n;
		leaf_page *meighbor = (leaf_page *)neighbor;

		for (i = neighbor_insertion_index, j = 0; j < m->num_of_keys; i++, j++)
		{
			meighbor->records[i].key = m->records[j].key;
			strcpy(meighbor->records[i].value, m->records[j].value);
			meighbor->num_of_keys += 1;
		}

		meighbor->right_sibling_num = m->right_sibling_num;

		buf_write_page(table_id, neighbor_offset, (page_t *)meighbor);

		buf_write_page(table_id, n_offset, (page_t *)m);
		db_delete_entry(table_id, header, m->parent_page_num, k_prime);
	}

	return 0;
}

int db_delete(int table_id, int64_t key)
{

	char *key_record = (char *)malloc(150);
	int exist = db_find(table_id, key, key_record, 0);
	pagenum_t leaf_offset = db_find_leaf(table_id, key, 0);
	header_page *header = (header_page *)buf_read_page(table_id, 0);
	set_pin(table_id, 0);

	if (exist != -1 && leaf_offset != 0)
	{
		db_delete_entry(table_id, header, leaf_offset, key);
		buf_write_page(table_id, 0, (page_t *)header);
		unset_pin(table_id, 0);

		return 0;
	}

	buf_write_page(table_id, 0, (page_t *)header);
	unset_pin(table_id, 0);
	return -1;
}
