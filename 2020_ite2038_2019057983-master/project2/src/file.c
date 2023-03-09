#include "file.h"
#include <memory.h>
void make_free_page(pagenum_t from){
	header.number_of_pages += 1000;
	page_t new_free_page;
	memset(&new_free_page,0,sizeof(page_t));
	header.free_page_num = from;
	
	for (int i=from;i<from+1000;i++){
		new_free_page.is_free= 1;
		if (i!= from+1000) new_free_page.next_free_page_num = i+1;
		else new_free_page.next_free_page_num =0;

		file_write_page(i,&new_free_page);
	}

	
	file_write_page(0,&header);
	return;
}

pagenum_t file_alloc_page(){
	pagenum_t ret= header.free_page_num;
	
	if (ret== 0){
		ret = header.number_of_pages;
		make_free_page(header.number_of_pages);
		file_read_page(0,&header);
	}

	page_t first_free_page;
	memset(&first_free_page,0,sizeof(page_t));
	first_free_page.is_free= 1;

	file_read_page(ret,&first_free_page);
	header.free_page_num=first_free_page.next_free_page_num;
	file_write_page(0,&header);
	return ret;

}


void file_free_page(pagenum_t pagenum){
	page_t new_free_page;
	memset(&new_free_page,0,sizeof(page_t));
	new_free_page.is_free= 1;
	
	pagenum_t tmp= header.free_page_num;
	header.free_page_num = pagenum;
	new_free_page.next_free_page_num = tmp;
	
	file_write_page(pagenum, &new_free_page);
	file_write_page(0,&header);
}

void file_read_page(pagenum_t pagenum,page_t* dest){
	dest->page_num= pagenum;
	if (dest->is_header) {
		pread(fd, &(dest->free_page_num), sizeof(pagenum_t), pagenum*PAGE_BYTE);
		pread(fd,&(dest->root_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE + 8);
		pread(fd,&(dest->number_of_pages),sizeof(pagenum_t),pagenum*PAGE_BYTE + 16);
	}
	else if (dest->is_free) pread(fd,&(dest->next_free_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE);
	else {
		pread(fd,&(dest->is_leaf),sizeof(int),pagenum*PAGE_BYTE+8);		
		if (dest->is_leaf) {
			dest->is_leaf = 1, dest->is_internal = 0;
			pread(fd,&(dest->parent_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE);
			pread(fd,&(dest->is_leaf),sizeof(int),pagenum*PAGE_BYTE+8);		
			pread(fd,&(dest->num_of_keys),sizeof(int),pagenum*PAGE_BYTE+12);		
			pread(fd,&(dest->right_sibling_num),sizeof(pagenum_t),pagenum*PAGE_BYTE+120);				
			pread(fd,dest->leaf_records,(LEAF_ORDER-1)*sizeof(leaf_record),pagenum*PAGE_BYTE+128);	
		}
		else{
			dest->is_internal= 1, dest->is_leaf= 0;
			pread(fd,&(dest->parent_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE);
			pread(fd,&(dest->is_leaf),sizeof(int),pagenum*PAGE_BYTE+8);		
			pread(fd,&(dest->num_of_keys),sizeof(int),pagenum*PAGE_BYTE+12);		
			pread(fd,&(dest->one_more_pagenum),sizeof(pagenum_t),pagenum*PAGE_BYTE+120);				
			pread(fd,dest->internal_records,(BRANCH_ORDER-1)*sizeof(internal_record),pagenum*PAGE_BYTE+128);
		}
	}	
}


	void file_write_page(pagenum_t pagenum, const page_t* src){	
	if (src->is_header) {
		pwrite(fd, &(src->free_page_num), sizeof(pagenum_t), pagenum*PAGE_BYTE);
		pwrite(fd,&(src->root_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE + 8);
		pwrite(fd,&(src->number_of_pages),sizeof(pagenum_t),pagenum*PAGE_BYTE + 16);
	}
	else if (src->is_free) pwrite(fd,&(src->next_free_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE);
	else if (src->is_leaf) {
		
		pwrite(fd,&(src->parent_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE);
		pwrite(fd,&(src->is_leaf),sizeof(int),pagenum*PAGE_BYTE+8);		
			pwrite(fd,&(src->num_of_keys),sizeof(int),pagenum*PAGE_BYTE+12);		
			pwrite(fd,&(src->right_sibling_num),sizeof(pagenum_t),pagenum*PAGE_BYTE+120);				
            pwrite(fd,src->leaf_records,(LEAF_ORDER-1)*sizeof(leaf_record),pagenum*PAGE_BYTE+128);	
		}
	else{
			
			pwrite(fd,&(src->parent_page_num),sizeof(pagenum_t),pagenum*PAGE_BYTE);
			pwrite(fd,&(src->is_leaf),sizeof(int),pagenum*PAGE_BYTE+8);		
			pwrite(fd,&(src->num_of_keys),sizeof(int),pagenum*PAGE_BYTE+12);		
			pwrite(fd,&(src->one_more_pagenum),sizeof(pagenum_t),pagenum*PAGE_BYTE+120);				
            pwrite(fd,src->internal_records,(BRANCH_ORDER-1)*sizeof(internal_record),pagenum*PAGE_BYTE+128);
		}
	fsync(fd);

	}	


	

	


