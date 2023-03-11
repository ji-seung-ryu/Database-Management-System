#include <log.h>

int64_t cur_lsn = 0;
int64_t log_offset = 0;
int64_t last_disk_lsn = -1;
vector<void *> log_buf;
vector<void *> past_log;
map<int, int> is_winner;
pthread_mutex_t log_manager_latch = PTHREAD_MUTEX_INITIALIZER;
vector<int> prev_trx;
int64_t past_offset = 0;

void log_BCR_write(int trx_id, int type)
{
	pthread_mutex_lock(&log_manager_latch);
	BCR_log *bcr = (BCR_log *)malloc(sizeof(BCR_log));

	memset(bcr, 0, sizeof(BCR_log));
	bcr->trx_id = trx_id;
	bcr->type = type;
	bcr->lsn = cur_lsn++;

	bcr->log_size = log_BCR_size;
	log_buf.push_back((void *)bcr);
	if (type == 2 || type == 3)
	{
		log_write_disk(bcr->lsn, type);
		return;
	}
	pthread_mutex_unlock(&log_manager_latch);
}

int log_update_write(int trx_id, int type, int table_id, int page_num, int offset, int data_len, char *old_image, char *new_image)
{
	pthread_mutex_lock(&log_manager_latch);
	update_log *ulog = (update_log *)malloc(sizeof(update_log));
	memset(ulog, 0, sizeof(update_log));
	ulog->trx_id = trx_id;
	ulog->type = type;
	ulog->table_id = table_id;
	ulog->page_num = page_num;
	ulog->offset = offset;
	ulog->data_len = data_len;
	strcpy(ulog->old_image, old_image);
	strcpy(ulog->new_image, new_image);
	ulog->lsn = cur_lsn++;
	ulog->log_size = log_update_size;

	log_buf.push_back((void *)ulog);
	pthread_mutex_unlock(&log_manager_latch);
	return ulog->lsn;
}

void log_compensate_write(int trx_id, int type, int table_id, int page_num, int offset, int data_len, char *old_image, char *new_image, int64_t next_undo_lsn)
{
	pthread_mutex_lock(&log_manager_latch);
	compensate_log *clog = (compensate_log *)malloc(sizeof(compensate_log));
	memset(clog, 0, sizeof(compensate_log));
	clog->trx_id = trx_id;
	clog->type = type;
	clog->table_id = table_id;
	clog->page_num = page_num;
	clog->offset = offset;
	clog->data_len = data_len;
	strcpy(clog->old_image, old_image);
	strcpy(clog->new_image, new_image);
	clog->lsn = cur_lsn++;
	clog->log_size = log_compensate_size;
	clog->next_undo_lsn = next_undo_lsn;

	log_buf.push_back((void *)clog);
	pthread_mutex_unlock(&log_manager_latch);
}

void log_write_disk(int until_lsn, int type)
{
	if (type != 2 && type != 3)
		pthread_mutex_lock(&log_manager_latch);
	for (int i = last_disk_lsn + 1; i <= until_lsn; i++)
	{
		cout << i << " ";

		compensate_log *l = (compensate_log *)log_buf[i];

		file_write_log(log_fd, log_offset, log_buf[i], l->log_size);

		log_offset += l->log_size;
		last_disk_lsn += 1;
	}

	pthread_mutex_unlock(&log_manager_latch);
}

void log_analysis()
{
	pthread_mutex_lock(&log_manager_latch);
	fprintf(logmsg_fp, "[ANALYSIS] Analysis pass start\n");

	int64_t past_offset = 0;

	while (true)
	{
		int *log_size = (int *)malloc(sizeof(int));
		void *dest = file_read_log(past_offset, log_size);
		if (dest == NULL)
			break;
		past_offset += *log_size;

		past_log.push_back(dest);

		if (*log_size == log_BCR_size)
		{
			BCR_log *bcr = (BCR_log *)dest;
			if (bcr->type == 0)
				prev_trx.push_back(bcr->trx_id);
			if (bcr->type == 2 || bcr->type == 3)
			{

				is_winner[bcr->trx_id] = 1;
			}
		}
	}

	fprintf(logmsg_fp, "[ANALYSIS] Analysis success. Winner:");

	for (int trx_id : prev_trx)
	{
		if (is_winner[trx_id])
		{
			fprintf(logmsg_fp, " %d", trx_id);
		}
	}
	fprintf(logmsg_fp, ", Loser:");

	for (int trx_id : prev_trx)
	{
		if (!is_winner[trx_id])
		{
			fprintf(logmsg_fp, " %d", trx_id);
		}
	}
	fprintf(logmsg_fp, "\n");

	pthread_mutex_unlock(&log_manager_latch);
}

void log_redo(int redo_tot)
{
	pthread_mutex_lock(&log_manager_latch);

	int redo_cnt = 0;
	fprintf(logmsg_fp, "[REDO] Redo pass start\n");

	for (int i = 0; i < (int)past_log.size(); i++)
	{
		int *log_size = (int *)malloc(sizeof(int));
		void *dest = past_log[i];
		*log_size = ((compensate_log *)dest)->log_size;

		if (dest == NULL)
			break;
		if (redo_cnt == redo_tot)
			break;

		past_offset += *log_size;
		redo_cnt += 1;

		if (((BCR_log *)dest)->type == 0)
		{
			fprintf(logmsg_fp, "LSN %lu [BEGIN] Transaction id %d\n", past_offset, ((BCR_log *)dest)->trx_id);
		}
		if (((update_log *)dest)->type == 1)
		{

			if (is_winner[((update_log *)dest)->trx_id])
			{

				char str[100];
				sprintf(str, "DATA%d", ((update_log *)dest)->table_id);
				cout << str << "\n";
				open_table(str);
				leaf_page *lp = (leaf_page *)buf_read_page(((update_log *)dest)->table_id, ((update_log *)dest)->page_num);

				if (lp->page_lsn > ((update_log *)dest)->lsn)
				{
					fprintf(logmsg_fp, "LSN %lu [CONSIDER_REDO] Transaction id %d redo \n", past_offset, ((update_log *)dest)->trx_id);

					release_page_latch(((update_log *)dest)->table_id, ((update_log *)dest)->page_num);
					continue;
				}

				fprintf(logmsg_fp, "LSN %lu [UPDATE] Transaction id %d redo apply\n", past_offset, ((update_log *)dest)->trx_id);

				int idx = (((update_log *)dest)->offset - 128) / 128;
				strcpy(lp->records[idx].value, ((update_log *)dest)->new_image);
				lp->page_lsn = ((update_log *)dest)->lsn;
				buf_write_page(((update_log *)dest)->table_id, ((update_log *)dest)->page_num, (page_t *)lp);
				release_page_latch(((update_log *)dest)->table_id, ((update_log *)dest)->page_num);
			}
		}
		if (((BCR_log *)dest)->type == 2)
		{
			fprintf(logmsg_fp, "LSN %lu [COMMIT] Transaction id %d\n", past_offset, ((BCR_log *)dest)->trx_id);
		}
		if (((BCR_log *)dest)->type == 3)
		{
			fprintf(logmsg_fp, "LSN %lu [ROLLBACK] Transaction id %d\n", past_offset, ((BCR_log *)dest)->trx_id);
		}
		if (((compensate_log *)dest)->type == 4)
		{

			if (is_winner[((compensate_log *)dest)->trx_id])
			{

				char str[100];
				sprintf(str, "DATA%d", ((compensate_log *)dest)->table_id);

				open_table(str);
				leaf_page *lp = (leaf_page *)buf_read_page(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num);

				if (lp->page_lsn > ((compensate_log *)dest)->lsn)
				{
					fprintf(logmsg_fp, "LSN %lu [CONSIDER_REDO] Transaction id %d redo \n", past_offset, ((compensate_log *)dest)->trx_id);

					release_page_latch(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num);
					continue;
				}

				fprintf(logmsg_fp, "LSN %lu [CLR] next undo lsn %lu\n", past_offset, ((compensate_log *)dest)->next_undo_lsn);

				int idx = (((compensate_log *)dest)->offset - 128) / 128;
				strcpy(lp->records[idx].value, ((compensate_log *)dest)->new_image);
				lp->page_lsn = ((compensate_log *)dest)->lsn;
				buf_write_page(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num, (page_t *)lp);
				release_page_latch(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num);
			}
		}
	}

	fprintf(logmsg_fp, "[REDO] Redo pass end\n");

	pthread_mutex_unlock(&log_manager_latch);
}
void log_undo(int undo_tot)
{
	pthread_mutex_lock(&log_manager_latch);
	fprintf(logmsg_fp, "[UNDO] Undo pass start\n");

	int undo_cnt = 0;

	for (int i = (int)past_log.size() - 1; i >= 0; i--)
	{
		int *log_size = (int *)malloc(sizeof(int));
		void *dest = past_log[i];
		*log_size = ((compensate_log *)dest)->log_size;

		if (dest == NULL)
			break;
		if (undo_cnt == undo_tot)
			break;

		past_offset -= *log_size;
		undo_cnt += 1;

		if (((BCR_log *)dest)->type == 0)
		{
			fprintf(logmsg_fp, "LSN %lu [BEGIN] Transaction id %d\n", past_offset, ((BCR_log *)dest)->trx_id);
		}
		if (((update_log *)dest)->type == 1)
		{

			if (!is_winner[((update_log *)dest)->trx_id])
			{

				char str[100];
				sprintf(str, "DATA%d", ((update_log *)dest)->table_id);

				open_table(str);
				leaf_page *lp = (leaf_page *)buf_read_page(((update_log *)dest)->table_id, ((update_log *)dest)->page_num);

				if (lp->page_lsn < ((update_log *)dest)->lsn)
				{

					release_page_latch(((update_log *)dest)->table_id, ((update_log *)dest)->page_num);
					continue;
				}
				fprintf(logmsg_fp, "LSN %lu [UPDATE] Transaction id %d undo apply\n", past_offset, ((update_log *)dest)->trx_id);

				int idx = (((update_log *)dest)->offset - 128) / 128;
				strcpy(lp->records[idx].value, ((update_log *)dest)->old_image);
				lp->page_lsn = ((update_log *)dest)->lsn;
				buf_write_page(((update_log *)dest)->table_id, ((update_log *)dest)->page_num, (page_t *)lp);
				release_page_latch(((update_log *)dest)->table_id, ((update_log *)dest)->page_num);
			}
		}
		if (((BCR_log *)dest)->type == 2)
		{
			fprintf(logmsg_fp, "LSN %lu [COMMIT] Transaction id %d\n", past_offset, ((BCR_log *)dest)->trx_id);
		}
		if (((BCR_log *)dest)->type == 3)
		{
			fprintf(logmsg_fp, "LSN %lu [ROLLBACK] Transaction id %d\n", past_offset, ((BCR_log *)dest)->trx_id);
		}
		if (((compensate_log *)dest)->type == 4)
		{
			if (!is_winner[((compensate_log *)dest)->trx_id])
			{

				char str[100];
				sprintf(str, "DATA%d", ((compensate_log *)dest)->table_id);

				open_table(str);
				leaf_page *lp = (leaf_page *)buf_read_page(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num);

				if (lp->page_lsn < ((compensate_log *)dest)->lsn)
				{

					release_page_latch(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num);
					continue;
				}
				fprintf(logmsg_fp, "LSN %lu [CLR] next undo lsn %lu\n", past_offset, ((compensate_log *)dest)->next_undo_lsn);

				int idx = (((compensate_log *)dest)->offset - 128) / 128;
				strcpy(lp->records[idx].value, ((compensate_log *)dest)->old_image);
				lp->page_lsn = ((compensate_log *)dest)->lsn;
				buf_write_page(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num, (page_t *)lp);
				release_page_latch(((compensate_log *)dest)->table_id, ((compensate_log *)dest)->page_num);
			}
		}
	}
	fprintf(logmsg_fp, "[UNDO] Undo pass end\n");

	pthread_mutex_unlock(&log_manager_latch);
}
