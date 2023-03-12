#include "file.h"
#include "buf.h"
#include <memory.h>
#include <stdlib.h>
void make_free_page(int table_id, header_page *header, pagenum_t from)
{
	header->number_of_pages += 100;
	free_page *new_free_page = (free_page *)malloc(PAGE_BYTE);
	memset(new_free_page, 0, PAGE_BYTE);
	header->free_page_num = from;

	for (int i = from; i < from + 100; i++)
	{
		if (i != from + 99)
			new_free_page->next_free_page_num = i + 1;
		else
			new_free_page->next_free_page_num = 0;

		file_write_page(table_id, i, (page_t *)new_free_page);
	}

	return;
}

pagenum_t file_alloc_page(int table_id, header_page *header)
{

	pagenum_t ret = header->free_page_num;

	if (ret == 0)
	{
		ret = header->number_of_pages;
		make_free_page(table_id, header, header->number_of_pages);
	}

	free_page *first_free_page = (free_page *)malloc(PAGE_BYTE);
	memset(first_free_page, 0, PAGE_BYTE);

	file_read_page(table_id, ret, (page_t *)first_free_page);
	header->free_page_num = first_free_page->next_free_page_num;

	return ret;
}

void file_free_page(int table_id, pagenum_t pagenum)
{
	header_page *header = (header_page *)malloc(PAGE_BYTE);
	memset(header, 0, PAGE_BYTE);
	file_read_page(table_id, pagenum, (page_t *)header);

	free_page *new_free_page = (free_page *)malloc(PAGE_BYTE);
	memset(new_free_page, 0, PAGE_BYTE);

	pagenum_t tmp = header->free_page_num;
	header->free_page_num = pagenum;
	new_free_page->next_free_page_num = tmp;

	file_write_page(table_id, pagenum, (page_t *)new_free_page);
	file_write_page(table_id, 0, (page_t *)header);
}

void file_read_page(int table_id, pagenum_t pagenum, page_t *dest)
{
	pread(id_fd[table_id], dest, PAGE_BYTE, pagenum * PAGE_BYTE);
}

void file_write_page(int table_id, pagenum_t pagenum, const page_t *src)
{
	pwrite(id_fd[table_id], src, PAGE_BYTE, pagenum * PAGE_BYTE);
	fsync(fd);
}
