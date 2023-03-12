#include <iostream>
#include <string.h>
#include "buf.h"
#include <map>

using namespace std;
buffer *B;
map<pair<int, int>, int> page_buf;
map<int, int> is_open;
pthread_mutex_t buffer_manager_latch;
int table_id;
int fd;
int id_fd[15];
map<string, int> path_tid;

int head, tail;

int open_table(char *pathname)
{
	if ((fd = open(pathname, O_CREAT | O_RDWR | O_SYNC, 0644)) == -1)
	{
		printf("open error");
		return -1;
	}

	int ret;

	int past_tid;
	if (path_tid[pathname])
	{
		past_tid = path_tid[pathname];
		id_fd[past_tid] = fd;
		ret = past_tid;
	}
	else
	{
		if (table_id >= 10)
			return -1;
		path_tid[pathname] = ++table_id;
		id_fd[table_id] = fd;
		ret = table_id;
	}

	header_page *header = (header_page *)buf_read_page(ret, 0);
	if (header->number_of_pages == 0)
		header->number_of_pages = 1;
	buf_write_page(ret, 0, (page_t *)header);

	release_page_latch(ret, 0);

	is_open[ret] = 1;

	return ret;
}

int get_id(int table_id, int page_num)
{
	return (int)1e6 * table_id + page_num;
}
int init_db(int num_buf)
{
	buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;
	B = (buffer *)malloc(sizeof(buffer) * (num_buf + 2));
	memset(B, 0, sizeof(buffer) * num_buf);

	for (int i = 0; i <= num_buf + 1; i++)
	{
		B[i].frame = (page_t *)malloc(PAGE_BYTE);
		B[i].next_LRU = i + 1;
		B[i].prev_LRU = i - 1;
		B[i].page_num = -1;
		B[i].page_latch = PTHREAD_MUTEX_INITIALIZER;
		B[i].is_dirty = 0;
	}

	head = 0;
	tail = num_buf + 1;
}

int find_buf(int tid)
{

	int ret;

	ret = B[tail].prev_LRU;

	pthread_mutex_lock(&B[B[tail].prev_LRU].page_latch);

	B[B[ret].prev_LRU].next_LRU = B[ret].next_LRU;
	B[B[ret].next_LRU].prev_LRU = B[ret].prev_LRU;

	B[B[head].next_LRU].prev_LRU = ret;
	B[ret].next_LRU = B[head].next_LRU;
	B[ret].prev_LRU = head;
	B[head].next_LRU = ret;

	if (B[ret].is_dirty)
	{
		file_write_page(B[ret].table_id, B[ret].page_num, B[ret].frame);
		B[ret].is_dirty = false;
	}

	if (B[ret].page_num != -1)
	{
		pair<int, int> u = {B[ret].table_id, B[ret].page_num};
		page_buf.erase(u);
	}

	return ret;
}
page_t *buf_read_page(int tid, int page_num)
{

	pthread_mutex_lock(&buffer_manager_latch);
	int possible_buf;

	pair<int, int> u = {tid, page_num};

	if (page_buf.find(u) == page_buf.end())
	{
		possible_buf = find_buf(tid);
		page_buf[u] = possible_buf;

		pthread_mutex_unlock(&buffer_manager_latch);
		B[possible_buf].page_num = page_num;
		B[possible_buf].table_id = tid;
		file_read_page(tid, page_num, B[possible_buf].frame);
	}

	else
	{
		possible_buf = page_buf[u];
		pthread_mutex_lock(&B[possible_buf].page_latch);
		pthread_mutex_unlock(&buffer_manager_latch);
	}

	return B[possible_buf].frame;
}

void buf_write_page(int tid, int page_num, page_t *src)
{

	int possible_buf;

	pair<int, int> u = {tid, page_num};
	if (page_buf.find(u) == page_buf.end())
	{
		possible_buf = find_buf(tid);
		page_buf[u] = possible_buf;

		B[possible_buf].page_num = page_num;
		B[possible_buf].table_id = tid;
		B[possible_buf].frame = src;

		pthread_mutex_unlock(&B[possible_buf].page_latch);
	}

	else
	{
		possible_buf = page_buf[u];

		B[possible_buf].frame = src;
	}
	B[possible_buf].is_dirty = 1;
}

void release_page_latch(int tid, int page_num)
{

	pair<int, int> u = {tid, page_num};
	if (page_buf.end() == page_buf.find(u))
	{
		return;
	}
	else
	{
		int possible_buf = page_buf[u];

		pthread_mutex_unlock(&B[possible_buf].page_latch);
	}
}
void set_pin(int tid, int page_num)
{
	pthread_mutex_lock(&buffer_manager_latch);
	pair<int, int> u = {tid, page_num};
	int possible_buf = page_buf[u];
	pthread_mutex_lock(&B[possible_buf].page_latch);
	pthread_mutex_unlock(&buffer_manager_latch);
	B[possible_buf].is_pinned = true;
	pthread_mutex_unlock(&B[possible_buf].page_latch);
}

void unset_pin(int tid, int page_num)
{
	pthread_mutex_lock(&buffer_manager_latch);
	pair<int, int> u = {tid, page_num};
	int possible_buf = page_buf[u];
	pthread_mutex_lock(&B[possible_buf].page_latch);
	pthread_mutex_unlock(&buffer_manager_latch);
	B[possible_buf].is_pinned = false;
	pthread_mutex_unlock(&B[possible_buf].page_latch);
}
int shutdown_db()
{
	pthread_mutex_lock(&buffer_manager_latch);
	for (int i = 1; i < tail; i++)
	{
		pthread_mutex_lock(&B[i].page_latch);
		if (B[i].is_dirty)
		{

			file_write_page(B[i].table_id, B[i].page_num, B[i].frame);
		}
		pthread_mutex_unlock(&B[i].page_latch);
	}
	for (int i = 1; i <= TABLE_NUM; i++)
	{
		if (is_open[i])
		{
			close(id_fd[i]);
			is_open[i] = 0;
		}
	}
	pthread_mutex_unlock(&buffer_manager_latch);
	return 0;
}

int close_table(int table_id)
{
	pthread_mutex_lock(&buffer_manager_latch);
	for (int i = 1; i < tail; i++)
	{
		pthread_mutex_lock(&B[i].page_latch);
		if (B[i].is_dirty && B[i].table_id == table_id)
		{
			file_write_page(B[i].table_id, B[i].page_num, B[i].frame);
		}
		pthread_mutex_unlock(&B[i].page_latch);
	}
	close(id_fd[table_id]);
	is_open[table_id] = 0;
	pthread_mutex_unlock(&buffer_manager_latch);
	return 0;
}
