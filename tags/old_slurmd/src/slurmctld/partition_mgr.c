/*****************************************************************************\
 *  partition_mgr.c - manage the partition information of slurm
 *	Note: there is a global partition list (part_list) and
 *	time stamp (last_part_update)
 *****************************************************************************
 *  Copyright (C) 2002 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Moe Jette <jette@llnl.gov> et. al.
 *  UCRL-CODE-2002-040.
 *  
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://www.llnl.gov/linux/slurm/>.
 *  
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *  
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *  
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
\*****************************************************************************/

#ifdef HAVE_CONFIG_H
#  include <config.h>
#endif

#include <ctype.h>
#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <src/common/hostlist.h>
#include <src/common/list.h>
#include <src/common/pack.h>
#include <src/common/xstring.h>
#include <src/slurmctld/locks.h>
#include <src/slurmctld/slurmctld.h>

#define BUF_SIZE 1024

struct part_record default_part;	/* default configuration values */
List part_list = NULL;			/* partition list */
char default_part_name[MAX_NAME_LEN];	/* name of default partition */
struct part_record *default_part_loc = NULL;	/* location of default partition */
time_t last_part_update;		/* time of last update to partition records */

static	int	build_part_bitmap (struct part_record *part_record_point);
static	void	dump_part_state (struct part_record *part_record_point, Buf buffer);
static	uid_t 	*get_groups_members (char *group_names);
static	uid_t 	*get_group_members (char *group_name);
static	time_t	get_group_tlm (void);
static	void	list_delete_part (void *part_entry);
static	int	uid_list_size (uid_t *uid_list_ptr);


/*
 * build_part_bitmap - update the total_cpus, total_nodes, and node_bitmap for the specified 
 *	partition, also reset the partition pointers in the node back to this partition.
 * input: part_record_point - pointer to the partition
 * output: returns 0 if no error, errno otherwise
 * global: node_record_table_ptr - pointer to global node table
 * NOTE: this does not report nodes defined in more than one partition. this is checked only  
 *	upon reading the configuration file, not on an update
 */
int 
build_part_bitmap (struct part_record *part_record_point) 
{
	int i, update_nodes;
	char  *this_node_name ;
	bitstr_t *old_bitmap;
	struct node_record *node_record_point;	/* pointer to node_record */
	hostlist_t host_list;

	part_record_point->total_cpus = 0;
	part_record_point->total_nodes = 0;

	if (part_record_point->node_bitmap == NULL) {
		part_record_point->node_bitmap = (bitstr_t *) bit_alloc (node_record_count);
		if (part_record_point->node_bitmap == NULL)
			fatal("bit_alloc memory allocation failure");
		old_bitmap = NULL;
	}
	else {
		old_bitmap = bit_copy (part_record_point->node_bitmap);
		bit_nclear (part_record_point->node_bitmap, 0, node_record_count-1);
	}

	if (part_record_point->nodes == NULL) {		/* no nodes in partition */
		if (old_bitmap)				/* leave with empty bitmap */
			bit_free (old_bitmap);
		return 0;
	}

	if ( (host_list = hostlist_create (part_record_point->nodes)) == NULL) {
		if (old_bitmap)
			bit_free (old_bitmap);
		error ("hostlist_create error on %s, %m", part_record_point->nodes);
		return ESLURM_INVALID_NODE_NAME;
	}

	while ( (this_node_name = hostlist_shift (host_list)) ) {
		node_record_point = find_node_record (this_node_name);
		if (node_record_point == NULL) {
			error ("build_part_bitmap: invalid node specified %s", this_node_name);
			free (this_node_name);
			if (old_bitmap)
				bit_free (old_bitmap);
			hostlist_destroy (host_list);
			return ESLURM_INVALID_NODE_NAME;
		}	
		part_record_point->total_nodes++;
		part_record_point->total_cpus += node_record_point->cpus;
		node_record_point->partition_ptr = part_record_point;
		if (old_bitmap) 
			bit_clear (old_bitmap,
			      (int) (node_record_point - node_record_table_ptr));
		bit_set (part_record_point->node_bitmap,
			    (int) (node_record_point - node_record_table_ptr));
		free (this_node_name);
	}
	hostlist_destroy (host_list);

	/* unlink nodes removed from the partition */
	if (old_bitmap) {
		update_nodes = 0;
		for (i = 0; i < node_record_count; i++) {
			if (bit_test (old_bitmap, i) == 0)
				continue;
			node_record_table_ptr[i].partition_ptr = NULL;
			update_nodes = 1;
		}
		bit_free (old_bitmap);
		if (update_nodes)
			last_node_update = time (NULL);
	}			

	return 0;
}


/* 
 * create_part_record - create a partition record
 * output: returns a pointer to the record or NULL if error
 * global: default_part - default partition parameters
 *         part_list - global partition list
 * NOTE: the record's values are initialized to those of default_part
 * NOTE: allocates memory that should be xfreed with delete_part_record
 */
struct part_record * 
create_part_record (void) 
{
	struct part_record *part_record_point;

	last_part_update = time (NULL);

	part_record_point =
		(struct part_record *) xmalloc (sizeof (struct part_record));

	strcpy (part_record_point->name, "DEFAULT");
	part_record_point->max_time = default_part.max_time;
	part_record_point->max_nodes = default_part.max_nodes;
	part_record_point->root_only = default_part.root_only;
	part_record_point->state_up = default_part.state_up;
	part_record_point->shared = default_part.shared;
	part_record_point->total_nodes = default_part.total_nodes;
	part_record_point->total_cpus = default_part.total_cpus;
	part_record_point->node_bitmap = NULL;
	part_record_point->magic = PART_MAGIC;

	if (default_part.allow_groups) {
		part_record_point->allow_groups =
			(char *) xmalloc (strlen (default_part.allow_groups) + 1);
		strcpy (part_record_point->allow_groups,
			default_part.allow_groups);
	}
	else
		part_record_point->allow_groups = NULL;

	if (default_part.nodes) {
		part_record_point->nodes =
			(char *) xmalloc (strlen (default_part.nodes) + 1);	
		strcpy (part_record_point->nodes, default_part.nodes);
	}
	else
		part_record_point->nodes = NULL;

	if (list_append (part_list, part_record_point) == NULL)
		fatal ("create_part_record: unable to allocate memory");

	return part_record_point;
}


/* 
 * delete_part_record - delete record for partition with specified name
 * input: name - name of the desired node, delete all partitions if pointer is NULL 
 * output: return 0 on success, errno otherwise
 * global: part_list - global partition list
 */
int 
delete_part_record (char *name) 
{
	int i;

	last_part_update = time (NULL);
	if (name == NULL)
		i = list_delete_all (part_list, &list_find_part,
				     "universal_key");
	else
		i = list_delete_all (part_list, &list_find_part, name);
	if ((name == NULL) || (i != 0))
		return 0;

	error ("delete_part_record: attempt to delete non-existent partition %s", name);
	return ENOENT;
}


/* dump_all_part_state - save the state of all partitions to file */
int
dump_all_part_state ( void )
{
	ListIterator part_record_iterator;
	struct part_record *part_record_point;
	int error_code = 0, log_fd;
	char *old_file, *new_file, *reg_file;
	/* Locks: Read partition */
	slurmctld_lock_t part_read_lock = { READ_LOCK, NO_LOCK, NO_LOCK, READ_LOCK };
	Buf buffer = init_buf(BUF_SIZE*16);

	/* write header: time */
	pack_time  (time (NULL), buffer);

	/* write partition records to buffer */
	lock_slurmctld (part_read_lock);
	part_record_iterator = list_iterator_create (part_list);		
	while ((part_record_point = (struct part_record *) list_next (part_record_iterator))) {
		if (part_record_point->magic != PART_MAGIC)
			fatal ("dump_all_part_state: data integrity is bad");

		dump_part_state (part_record_point, buffer);
	}			
	list_iterator_destroy (part_record_iterator);
	unlock_slurmctld (part_read_lock);

	/* write the buffer to file */
	old_file = xstrdup (slurmctld_conf.state_save_location);
	xstrcat (old_file, "/part_state.old");
	reg_file = xstrdup (slurmctld_conf.state_save_location);
	xstrcat (reg_file, "/part_state");
	new_file = xstrdup (slurmctld_conf.state_save_location);
	xstrcat (new_file, "/part_state.new");
	lock_state_files ();
	log_fd = creat (new_file, 0600);
	if (log_fd == 0) {
		error ("Can't save state, error creating file %s, %m", new_file);
		error_code = errno;
	}
	else {
		if (write (log_fd, get_buf_data(buffer), get_buf_offset(buffer)) != 
							get_buf_offset(buffer)) {
			error ("Can't save state, error writing file %s, %m", new_file);
			error_code = errno;
		}
		close (log_fd);
	}
	if (error_code) 
		(void) unlink (new_file);
	else {	/* file shuffle */
		(void) unlink (old_file);
		(void) link (reg_file, old_file);
		(void) unlink (reg_file);
		(void) link (new_file, reg_file);
		(void) unlink (new_file);
	}
	xfree (old_file);
	xfree (reg_file);
	xfree (new_file);
	unlock_state_files ();

	free_buf (buffer);
	return 0;
}

/*
 * dump_part_state - dump the state of a specific partition to a buffer
 * part_record_point (I) - pointer to partition for which information is requested
 * buffer (I/O) - location to store data, pointers automatically advanced
 */
void 
dump_part_state (struct part_record *part_record_point, Buf buffer) 
{
	uint16_t default_part_flag;

	if (default_part_loc == part_record_point)
		default_part_flag = 1;
	else
		default_part_flag = 0;

	packstr (part_record_point->name, buffer);
	pack32  (part_record_point->max_time, buffer);
	pack32  (part_record_point->max_nodes, buffer);

	pack16  (default_part_flag, buffer);
	pack16  ((uint16_t)part_record_point->root_only, buffer);
	pack16  ((uint16_t)part_record_point->shared, buffer);

	pack16  ((uint16_t)part_record_point->state_up, buffer);
	packstr (part_record_point->allow_groups, buffer);
	packstr (part_record_point->nodes, buffer);
}

/*
 * load_part_state - load the partition state from file, recover from slurmctld restart.
 *	execute this after loading the configuration file data.
 */
int
load_part_state ( void )
{
	char *part_name, *allow_groups, *nodes, *state_file, *data;
	uint32_t time, max_time, max_nodes;
	uint16_t name_len, def_part_flag, root_only, shared, state_up;
	struct part_record *part_ptr;
	uint32_t data_size = 0;
	int data_allocated, data_read = 0, error_code = 0;
	int state_fd;
	Buf buffer;

	/* read the file */
	state_file = xstrdup (slurmctld_conf.state_save_location);
	xstrcat (state_file, "/part_state");
	lock_state_files ();
	state_fd = open (state_file, O_RDONLY);
	if (state_fd < 0) {
		info ("No partition state file (%s) to recover", state_file);
		error_code = ENOENT;
	}
	else {
		data_allocated = BUF_SIZE;
		data = xmalloc(data_allocated);
		while ((data_read = read (state_fd, &data[data_size], BUF_SIZE)) == BUF_SIZE) {
			data_size += data_read;
			data_allocated += BUF_SIZE;
			xrealloc(data, data_allocated);
		}
		data_size += data_read;
		close (state_fd);
		if (data_read < 0) 
			error ("Error reading file %s: %m", state_file);
	}
	xfree (state_file);
	unlock_state_files ();

	buffer = create_buf (data, data_size);
	if (data_size > sizeof (time_t))
		unpack_time (&time, buffer);

	while (remaining_buf (buffer) > 0) {
		safe_unpackstr_xmalloc (&part_name, &name_len, buffer);
		safe_unpack32 (&max_time, buffer);
		safe_unpack32 (&max_nodes, buffer);
		safe_unpack16 (&def_part_flag, buffer);
		safe_unpack16 (&root_only, buffer);
		safe_unpack16 (&shared, buffer);
		safe_unpack16 (&state_up, buffer);
		safe_unpackstr_xmalloc (&allow_groups, &name_len, buffer);
		safe_unpackstr_xmalloc (&nodes, &name_len, buffer);

		/* validity test as possible */
		if ((def_part_flag > 1) || 
		    (root_only > 1) || 
		    (shared > SHARED_FORCE) || 
		    (state_up > 1)) {
			error ("Invalid data for partition %s: def_part_flag=%u, root_only=%u, shared=%u, state_up=%u",
				part_name, def_part_flag, root_only, shared, state_up);
			error ("No more partition data will be processed from the checkpoint file");
			if (part_name)
				xfree (part_name);
			error_code = EINVAL;
			break;		
		}

		/* find record and perform update */
		part_ptr = list_find_first (part_list, &list_find_part, part_name);

		if (part_ptr) {
			part_ptr->max_time = max_time;
			part_ptr->max_nodes = max_nodes;
			if (def_part_flag) {
				strcpy (default_part_name, part_name);
				default_part_loc = part_ptr;	
			}
			part_ptr->root_only = root_only;
			part_ptr->shared = shared;
			part_ptr->state_up = state_up;
			if (part_ptr->allow_groups)
				xfree (part_ptr->allow_groups);
			part_ptr->allow_groups = allow_groups;
			if (part_ptr->nodes)
				xfree (part_ptr->nodes);
			part_ptr->nodes = nodes;
		} else {
			info ("load_part_state: partition %s removed from configuration file.", 
				part_name);
		}

		if (part_name)
			xfree (part_name);
	}

	free_buf (buffer);
	return error_code;

unpack_error:
	error ("Incomplete partition data checkpoint file.  State not completely restored");
	free_buf (buffer);
	return EFAULT;
}

/* 
 * find_part_record - find a record for partition with specified name
 * input: name - name of the desired partition 
 * output: return pointer to node partition or null if not found
 * global: part_list - global partition list
 */
struct part_record *
find_part_record (char *name){
	return list_find_first (part_list, &list_find_part, name);
}


/* 
 * init_part_conf - initialize the default partition configuration values and create 
 *	a (global) partition list. 
 * this should be called before creating any partition entries.
 * output: return value - 0 if no error, otherwise an error code
 * global: default_part - default partition values
 *         part_list - global partition list
 */
int 
init_part_conf () 
{
	last_part_update = time (NULL);

	strcpy (default_part.name, "DEFAULT");
	default_part.max_time = INFINITE;
	default_part.max_nodes = INFINITE;
	default_part.root_only = 0;
	default_part.state_up = 1;
	default_part.shared = SHARED_NO;
	default_part.total_nodes = 0;
	default_part.total_cpus = 0;
	if (default_part.nodes)
		xfree (default_part.nodes);
	default_part.nodes = (char *) NULL;
	if (default_part.allow_groups)
		xfree (default_part.allow_groups);
	default_part.allow_groups = (char *) NULL;
	if (default_part.allow_uids)
		xfree (default_part.allow_uids);
	default_part.allow_uids = (uid_t *) NULL;
	if (default_part.node_bitmap)
		bit_free (default_part.node_bitmap);
	default_part.node_bitmap = (bitstr_t *) NULL;

	if (part_list)		/* delete defunct partitions */
		(void) delete_part_record (NULL);
	else
		part_list = list_create (&list_delete_part);

	if (part_list == NULL) 
		fatal ("init_part_conf: list_create can not allocate memory");
		

	strcpy (default_part_name, "");
	default_part_loc = (struct part_record *) NULL;

	return 0;
}

/*
 * list_delete_part - delete an entry from the global partition list, 
 *	see common/list.h for documentation
 * global: node_record_count - count of nodes in the system
 *         node_record_table_ptr - pointer to global node table
 */
void 
list_delete_part (void *part_entry) 
{
	struct part_record *part_record_point;	/* pointer to part_record */
	int i;

	part_record_point = (struct part_record *) part_entry;
	for (i = 0; i < node_record_count; i++) {
		if (node_record_table_ptr[i].partition_ptr != part_record_point)
			continue;
		node_record_table_ptr[i].partition_ptr = NULL;
	}			
	if (part_record_point->allow_groups)
		xfree (part_record_point->allow_groups);
	if (part_record_point->allow_uids)
		xfree (part_record_point->allow_uids);
	if (part_record_point->nodes)
		xfree (part_record_point->nodes);
	if (part_record_point->node_bitmap)
		bit_free (part_record_point->node_bitmap);
	xfree (part_entry);
}


/*
 * list_find_part - find an entry in the partition list, see common/list.h for documentation,
 *	key is partition name or "universal_key" for all partitions 
 * global- part_list - the global partition list
 */
int 
list_find_part (void *part_entry, void *key) 
{
	if (strcmp (key, "universal_key") == 0)
		return 1;

	if (strncmp (((struct part_record *) part_entry)->name, 
	    (char *) key, MAX_NAME_LEN) == 0)
		return 1;

	return 0;
}


/* 
 * pack_all_part - dump all partition information for all partitions in 
 *	machine independent form (for network transmission)
 * input: buffer_ptr - location into which a pointer to the data is to be stored.
 *                     the calling function must xfree the storage.
 *         buffer_size - location into which the size of the created buffer is in bytes
 *         update_time - dump new data only if partition records updated since time 
 *                       specified, otherwise return empty buffer
 * output: buffer_ptr - the pointer is set to the allocated buffer.
 *         buffer_size - set to size of the buffer in bytes
 *         update_time - set to time partition records last updated
 * global: part_list - global list of partition records
 * NOTE: the buffer at *buffer_ptr must be xfreed by the caller
 * NOTE: change PART_STRUCT_VERSION in common/slurmlib.h whenever the format changes
 * NOTE: change slurm_load_part() in api/part_info.c whenever the data format changes
 */
void 
pack_all_part (char **buffer_ptr, int *buffer_size, time_t * update_time) 
{
	ListIterator part_record_iterator;
	struct part_record *part_record_point;
	int parts_packed, tmp_offset;
	Buf buffer;

	buffer_ptr[0] = NULL;
	*buffer_size = 0;
	if (*update_time == last_part_update)
		return;

	buffer = init_buf (BUF_SIZE*16);

	/* write haeader: version and time */
	parts_packed = 0 ;
	pack32  ((uint32_t) parts_packed, buffer);
	pack_time  (last_part_update, buffer);

	/* write individual partition records */
	part_record_iterator = list_iterator_create (part_list);		
	while ((part_record_point = 
		(struct part_record *) list_next (part_record_iterator))) {
		if (part_record_point->magic != PART_MAGIC)
			fatal ("pack_all_part: data integrity is bad");

		pack_part(part_record_point, buffer);
		parts_packed ++ ;
	}			

	list_iterator_destroy (part_record_iterator);

	/* put the real record count in the message body header */	
	tmp_offset = get_buf_offset (buffer);
	set_buf_offset (buffer, 0);
	pack32  ((uint32_t) parts_packed, buffer);
	set_buf_offset (buffer, tmp_offset);

	*update_time = last_part_update;
	*buffer_size = get_buf_offset (buffer);
	buffer_ptr[0] = xfer_buf_data (buffer);
}


/* 
 * pack_part - dump all configuration information about a specific partition in 
 *	machine independent form (for network transmission)
 * dump_part_ptr (I) - pointer to partition for which information is requested
 * buffer (I/O) - buffer in which data is place, pointers automatically updated
 * global: default_part_loc - pointer to the default partition
 * NOTE: if you make any changes here be sure to make the corresponding 
 *	changes to load_part_config in api/partition_info.c
 */
void 
pack_part (struct part_record *part_record_point, Buf buffer) 
{
	uint16_t default_part_flag;
	char node_inx_ptr[BUF_SIZE];

	if (default_part_loc == part_record_point)
		default_part_flag = 1;
	else
		default_part_flag = 0;

	packstr (part_record_point->name, buffer);
	pack32  (part_record_point->max_time, buffer);
	pack32  (part_record_point->max_nodes, buffer);
	pack32  (part_record_point->total_nodes, buffer);

	pack32  (part_record_point->total_cpus, buffer);
	pack16  (default_part_flag, buffer);
	pack16  ((uint16_t)part_record_point->root_only, buffer);
	pack16  ((uint16_t)part_record_point->shared, buffer);

	pack16  ((uint16_t)part_record_point->state_up, buffer);
	packstr (part_record_point->allow_groups, buffer);
	packstr (part_record_point->nodes, buffer);
	if (part_record_point->node_bitmap) {
		bit_fmt (node_inx_ptr, BUF_SIZE, part_record_point->node_bitmap);
		packstr (node_inx_ptr, buffer);
	}
	else
		packstr ("", buffer);
}


/* 
 * update_part - update a partition's configuration data
 * global: part_list - list of partition entries
 *	last_part_update - update time of partition records
 */
int 
update_part (update_part_msg_t * part_desc ) 
{
	int error_code, i;
	struct part_record *part_ptr;

	if ((part_desc -> name == NULL ) ||
			(strlen (part_desc->name ) >= MAX_NAME_LEN)) {
		error ("update_part: invalid partition name  %s", part_desc->name);
		return ESLURM_INVALID_PARTITION_NAME ;
	}			

	error_code = 0;
	part_ptr = list_find_first (part_list, &list_find_part, part_desc->name);

	if (part_ptr == NULL) {
		error ("update_part: partition %s does not exist, being created.",
				part_desc->name);
		part_ptr = create_part_record ();
		strcpy(part_ptr->name, part_desc->name );
	}			

	last_part_update = time (NULL);
	if (part_desc->max_time != NO_VAL) {
		info ("update_part: setting max_time to %d for partition %s",
				part_desc->max_time, part_desc->name);
		part_ptr->max_time = part_desc->max_time;
	}			

	if (part_desc->max_nodes != NO_VAL) {
		info ("update_part: setting max_nodes to %d for partition %s",
				part_desc->max_nodes, part_desc->name);
		part_ptr->max_nodes = part_desc->max_nodes;
	}			

	if (part_desc->root_only != (uint16_t) NO_VAL) {
		info ("update_part: setting root_only to %d for partition %s",
				part_desc->root_only, part_desc->name);
		part_ptr->root_only = part_desc->root_only;
	}			

	if (part_desc->state_up != (uint16_t) NO_VAL) {
		info ("update_part: setting state_up to %d for partition %s",
				part_desc->state_up, part_desc->name);
		part_ptr->state_up = part_desc->state_up;
	}			

	if (part_desc->shared != (uint16_t) NO_VAL) {
		info ("update_part: setting shared to %d for partition %s",
				part_desc->shared, part_desc->name);
		part_ptr->shared = part_desc->shared;
	}			

	if ((part_desc->default_part == 1) && 
	     (strcmp(default_part_name, part_desc->name) != 0)) {
		info ("update_part: changing default partition from %s to %s",
				default_part_name, part_desc->name);
		strcpy (default_part_name, part_desc->name);
		default_part_loc = part_ptr;
	}			

	if (part_desc->allow_groups != NULL) {
		if (part_ptr->allow_groups)
			xfree (part_ptr->allow_groups);
		i = strlen(part_desc->allow_groups) + 1;
		part_ptr->allow_groups = xmalloc(i);
		strcpy ( part_ptr->allow_groups , part_desc->allow_groups ) ;
		info ("update_part: setting allow_groups to %s for partition %s",
				part_desc->allow_groups, part_desc->name);
		if (part_ptr->allow_uids)
			xfree (part_ptr->allow_uids);
		part_ptr->allow_uids = get_groups_members (part_desc->allow_groups);
	}			

	if (part_desc->nodes != NULL) {
		char *backup_node_list;
		backup_node_list = part_ptr->nodes;
		i = strlen(part_desc->nodes) + 1;
		part_ptr->nodes = xmalloc(i);
		strcpy ( part_ptr->nodes , part_desc->nodes ) ;

		error_code = build_part_bitmap (part_ptr);
		if (error_code) {
			if (part_ptr->nodes)
				xfree (part_ptr->nodes);
			part_ptr->nodes = backup_node_list;
		}
		else {
			info ("update_part: setting nodes to %s for partition %s",
				part_desc->nodes, part_desc->name);
			if (backup_node_list)
				xfree(backup_node_list);
		}
	}
			
	return error_code;
}


/* validate_group - validate that the submit uid is authorized to run in this partition */
int
validate_group (struct part_record *part_ptr, uid_t submit_uid)
{
	int i;

	if (part_ptr->allow_groups == NULL)
		return 1;	/* all users allowed */
	if ( (submit_uid == 0) || (submit_uid = getuid ()) )
		return 1;	/* super-user can run anywhere */

	if (part_ptr->allow_uids == NULL)
		return 0;	/* no non-super-users in the list */
	for (i=0; part_ptr->allow_uids[i]; i++) {
		if (part_ptr->allow_uids[i] == submit_uid)
			return 1;
	}
	return 0;		/* not in this group's list */

}

/* load_part_uid_allow_list - for every partition reload the allow_uid list if "force" 
 *	is true or the GROUP_FILE has changed */
void
load_part_uid_allow_list ( int force )
{
	static time_t last_update_time;
	time_t temp_time;
	ListIterator part_record_iterator;
	struct part_record *part_record_point;

	temp_time = get_group_tlm();
	if ( (force == 0) && (temp_time == last_update_time) )
		return;
	debug ("Updating partition uid access list");
	last_update_time = temp_time;
	last_part_update = time (NULL);

	part_record_iterator = list_iterator_create (part_list);		
	while ((part_record_point = (struct part_record *) list_next (part_record_iterator))) {
		if (part_record_point->allow_uids)
			xfree (part_record_point->allow_uids);
		part_record_point->allow_uids = get_groups_members (part_record_point->allow_groups);
	}
	list_iterator_destroy (part_record_iterator);
}


/* get_groups_members - indentify the users in a comma delimited list of group names
 * Returns a zero terminated list of its UIDs or NULL on error 
 * NOTE: User root has implicitly access to every group (the zero terminating uid)
 * NOTE: The caller must xfree non-NULL return values */
uid_t *
get_groups_members (char *group_names)
{
	int *group_uids = NULL;
	int *temp_uids = NULL;
	int i, j, k;
	char *tmp_names, *name_ptr, *one_group_name;

	if (group_names == NULL)
		return NULL;

	i = strlen(group_names) + 1;
	tmp_names = xmalloc (i);
	strcpy (tmp_names, group_names);

	one_group_name = strtok_r (tmp_names, ",", &name_ptr);
	while ( one_group_name ) {
		temp_uids = get_group_members (one_group_name);
		if (group_uids) {
			/* concatenate the uid_lists and free the new one */
			i = uid_list_size (group_uids);
			j = uid_list_size (temp_uids);
			xrealloc (group_uids, sizeof (uid_t) * (i+j+1));
			for (k=0; k<=j; k++)
				group_uids[i+k] = temp_uids[k];
			xfree (temp_uids);
		} 
		else
			group_uids = temp_uids;
		one_group_name = strtok_r (NULL, ",", &name_ptr);
	}
	xfree (tmp_names);

	return group_uids;
}

/* get_group_members - indentify the users in a given group name
 * Returns a zero terminated list of its UIDs or NULL on error 
 * NOTE: User root has implicitly access to every group (the zero terminating uid)
 * NOTE: The caller must xfree non-NULL return values */
uid_t *
get_group_members (char *group_name)
{
	struct group *group_struct_ptr;
	struct passwd *user_pw_ptr;
	int i, j;
	int *group_uids = NULL;
	int uid_cnt = 0;

	group_struct_ptr = getgrnam(group_name);   /* Note: static memory, do not free */
	if (group_struct_ptr == NULL) {
		error ("Could not find configured group %s\n", group_name);
		setgrent ();
		return NULL;
	}

	for (i=0; ; i++) {
		if (group_struct_ptr->gr_mem[i] == NULL)
			break;
	}
	uid_cnt = i;
	group_uids = (int *) xmalloc (sizeof (uid_t) * (uid_cnt + 1));
	memset (group_uids, 0, (sizeof (uid_t) * (uid_cnt + 1)));

	j = 0;
	for (i=0; i<uid_cnt; i++) {
		user_pw_ptr = getpwnam (group_struct_ptr->gr_mem[i]);
		if (user_pw_ptr) {
			if (user_pw_ptr->pw_uid)
				group_uids[j++] = user_pw_ptr->pw_uid;
		}
		else
			error ("Could not find user %s in configured group %s\n", 
				group_struct_ptr->gr_mem[i], group_name);
		setpwent ();
	}

	setgrent ();
	return group_uids;
}

/* get_group_tlm - return the time of last modification for the GROUP_FILE (/etc/group) */
time_t 
get_group_tlm (void)
{
	struct stat stat_buf;

	if (stat (GROUP_FILE, &stat_buf)) {
		error ("Can't stat file %s %m", GROUP_FILE);
		return (time_t) 0;
	}
	return stat_buf.st_mtime;
}

#if EXTREME_LOGGING
/* print_group_members - print the members of a uid list */
void 
print_group_members (uid_t *uid_list)
{
	int i;

	if (uid_list) {
		for (i=0; uid_list[i]; i++) {
			debug3 ("%u", (unsigned int) uid_list[i]);
		}
	}
	printf ("\n\n");
}
#endif

/* uid_list_size - return the count of uid's in a zero terminated list */
int	
uid_list_size (uid_t *uid_list_ptr)
{
	int i;

	if (uid_list_ptr == NULL)
		return 0;

	for (i=0; ; i++) {
		if (uid_list_ptr[i] == 0)
			break;
	}

	return i;
}
