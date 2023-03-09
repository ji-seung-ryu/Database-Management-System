#include "file.h"
#include "bpt.h"
#include <stdlib.h>
#include <string.h>

// MAIN
int fd;
int ever_page_num;
page_t header;
int main( int argc, char ** argv ) {	
	open_table(argv[1]);
	
	for (int i=1300;i<=1700;i++){

		db_delete(i);
	}		
	print_all();

	
	close(fd);

	return 0;   
}
