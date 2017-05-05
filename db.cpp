#include <iostream>
#include <fstream>

#include <string>
#include <sstream>
#include <limits.h>

#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>

using namespace std;


#define DISK_FILE "db.bin"
#define DELETED INT_MIN

// Basic Configuration
//#define MEMORY_SIZE 1024*1024*256 // 128
#define PAGE_SIZE MEMORY_SIZE * 2
#define THREAD_NUM 1
#define FILE_NUM 1

// Bitmap
#define BUCKET_SIZE (sizeof(uint64_t))
#define MAX_BIT (BUCKET_SIZE * BUCKET_NUM)
#define BUCKET_COUNT 1000
#define SEED 1000

// HashTable
#define HASH_BUCKET_SIZE 4 // 65536


#define SET_BIT(bloom_filter, bits) (bloom_filter[bits / BUCKET_SIZE] |= 1 << (bits % BUCKET_SIZE))
#define GET_BIT(bloom_filter, bits) (bloom_filter[bits / BUCKET_SIZE] & (1 << (bits % BUCKET_SIZE)))


//INT_MAX: 2147483647
//INT_MIN: -2147483648

double time1;
struct timeval start_time, end_time;



struct tree_node;
struct tri_node;

int curThread = 0;

struct key_value
{
	int key;
	int value;
};

struct tree_node
{	struct key_value;
	struct tree_node* left, *right;
};


struct tri_node 
{	bool is_minus, is_end;
	struct tri_node* next[10];
};

struct page_header
{
	int count;
	int prev;
	//int min;
	//int max;
};


struct page_header_tree
{	int axis;
	int count;
	int smaller;
	int bigger;
};

struct page_hashtable_bucket
{	int bucketnum;
	int bucketcount;
	int* list;
};

struct hashtable_st {
   int size;
   struct key_value_h **table;
};

struct key_value_h {
   int key;
   int value;
   struct key_value_h *next;
};


struct hashtable_st* hashtable;

char lsm_memory[MEMORY_SIZE];

pthread_t tid[THREAD_NUM];

char page_in[PAGE_SIZE- sizeof(struct page_header)];

bool flush_table[MEMORY_SIZE / sizeof(struct key_value)] = {0};

int cur_mem_size = 0;
int all_elements = 0;
char page_buffer[PAGE_SIZE];
int page_count = 0;

int read_total = 0, write_total = 0;

pthread_rwlock_t lock[FILE_NUM];

fstream db;

uint64_t* bloom_filter;
int BUCKET_NUM = (10 > MEMORY_SIZE / sizeof(struct key_value) / 1000 ? 10 : MEMORY_SIZE / sizeof(struct key_value) / 1000);

//db.seekg (blk_number * BLKSIZE);  // Error!
//db.read (buffer, 100);

//db.seekp(streampos pos);
//db.write (buffer, 100);

//myFile.close();

inline void* parse(void* line);

uint32_t Hash(int key, uint32_t seed) {

	return (seed && 0xffffffff) ^ key;

}


//int heap[1000000], curHeapSize;




struct hashtable_st *HashTable_Init(int size)
{
   struct hashtable_st *hashtable;
   if( size < 1 )
      return NULL;

   hashtable = (struct hashtable_st*)malloc(sizeof(struct hashtable_st));

   hashtable->table = (struct key_value_h**)malloc( sizeof(struct key_value_h * ) * size );

   int i;
   for( i = 0; i < size; i++ ) {
      hashtable->table[i] = NULL;
   }

   hashtable->size = size;
	cur_mem_size = size;

   return hashtable;
}

int HashTable_Hash(struct hashtable_st *hashtable, int key)
{  return key % hashtable->size;
}

struct key_value_h* HashTable_NewBucket(int key, int value )
{
   struct key_value_h *newbucket = (struct key_value_h*)malloc(sizeof(struct key_value_h));

   newbucket->next = NULL;
   newbucket->key = key;
   newbucket->value = value;

   return newbucket;
}


void HashTable_Put(struct hashtable_st *hashtable, int key, int value)
{
   struct key_value_h *next, *last, *newbucket;

   int bucket = HashTable_Hash(hashtable, key);

   next = hashtable->table[bucket];

	char isInitial = 1;
   while(next && key > next->key)
   {  last = next;
      next = next->next;
		isInitial = 0;
   }
   if(next && key == next->key) // an update
      next->value = value;
   else // a new insertion
   {  newbucket = HashTable_NewBucket(key, value);
		if (isInitial == 0)
			cur_mem_size += sizeof(struct key_value_h);
		all_elements++;

      if(next == hashtable->table[bucket])
      {  newbucket->next = next;
         hashtable->table[bucket] = newbucket;
      }
      else if (!next)
      {  last->next = newbucket;
      }
      else
      {  newbucket->next = next;
         last->next = newbucket;
      }
   }
}

int HashTable_Get(struct hashtable_st *hashtable, int key) 
{  int bucket;
   struct key_value_h *pair;
   bucket = HashTable_Hash(hashtable, key);
   pair = hashtable->table[bucket];

   while( pair != NULL && key > pair->key)
      pair = pair->next;
  
   if( pair == NULL || key != pair->key)
      return DELETED; 
   else
      return pair->value;
}  


void HashTable_Delete(struct hashtable_st *hashtable, int key)
{  int bucket;
   struct key_value_h *pair = NULL, *prev = NULL;
   bucket = HashTable_Hash(hashtable, key);
   pair = hashtable->table[bucket];

   while(pair != NULL && key > pair->key)
   {  prev = pair;
      pair = pair->next;
   }

   if(pair != NULL && key == pair->key)
   {  if (prev) // middle bucket
      {  prev->next = pair->next;
			cur_mem_size -= sizeof(struct key_value_h);
			free(pair);
      }
		else // head bucket
			prev->next = NULL;

		all_elements--;
		if (all_elements < 0 || cur_mem_size < 0)
		{	printf("Hashtable size negative (elements = %d, memory = %d\n", all_elements, cur_mem_size);
			exit(0);
		}
   }
}






/*Initialize Heap*/


/*
void HeapInit() {
    heap[0] = -INT_MAX;
    curHeapSize = 0;
}

void HeapPut(struct key_value element) 
{	heap[heapSize++] = element;
	int cur = heapSize;
	while (heap[cur / 2] > element) 
	{	heap[cur] = heap[cur / 2];
      cur /= 2;
   }
   heap[cur] = element;
}
*/


uint64_t* BloomInit() {
   return (uint64_t*)calloc(BUCKET_COUNT, sizeof(uint64_t));
}

void BloomPut(uint64_t* bloom, int key) {
   uint32_t val = SEED;
   for (int i = 0; i < BUCKET_COUNT; i++) {
      val = Hash(key, SEED);
      //SET_BIT(bloom,val);
   }
}

bool BloomGet(uint64_t* bloom, int key) {
   uint32_t val = SEED;

   for (int i = 0; i < BUCKET_COUNT; i++) {
      val = Hash(key, SEED);
      //if (!GET_BIT(bloom,val)) {
      //   return false;
      //}
   }
   return true;
}


void Merge_Hashtable(struct key_value* memory_array)
{

	int db_position = PAGE_SIZE * (page_count - 1);
//printf("DB position: %d\n", db_position);
	if (page_count != 0 )
	{	db.seekp (db_position);  // last write page pointer
		db.seekg (db_position);  // last read page pointer
	}
	struct page_header header, write_header;
	//key_value_array;

	if (page_count != 0)	
	{	db.read ((char*)&header, sizeof(struct page_header)); // read page header
		db.read ((char*)&page_in, header.count); // read contents

		db.seekp (db_position);  // last write page pointer
		db.seekg (db_position);  // last read page pointer
	}
	else // initial page creation
	{	header.count = 0;
		header.prev = -1;

		db.seekp (0);  // last write page pointer
		db.seekg (0);  // last read page pointer
		page_count++;
		//cout << "Merge start" << endl;
	}


	//	cout << "Reading header: " << header.count << ", " << header.prev << "(" << db_position << ")" << endl;

	//int temp_buckets = (PAGE_SIZE - sizeof(struct page_header)) / sizeof(struct key_value_h) + MEMORY_SIZE;
	struct key_value_h* temp = new struct key_value_h[MEMORY_SIZE / sizeof(struct key_value_h) + 1];
	int cur_page_bucket = 0;
	int cur_memory_bucket = 0;
	int max_memory_bucket = cur_mem_size / sizeof(struct key_value_h) - 1; // inclusive
	int max_page_bucket = header.count - 1;
	int i = 0;	
	bool isFirstLoop = true;



	while (max_memory_bucket >= cur_memory_bucket || max_page_bucket >= cur_page_bucket)
	{
		//for (i = 0; i < temp_buckets; i++) 
		{	// page array is running out
			//if (max_memory_bucket < cur_memory_bucket && max_page_bucket < cur_page_bucket)
			//	break;
			if (max_memory_bucket > cur_memory_bucket && memory_array[cur_memory_bucket].value == DELETED)
			{	
				cur_memory_bucket++;
				continue;
			}
			if (max_page_bucket > cur_page_bucket && ((struct key_value*)page_in)[cur_page_bucket].value == DELETED) 
			{	
				cur_page_bucket++;
				continue;
			}

			if (max_memory_bucket >= cur_memory_bucket && max_page_bucket < cur_page_bucket)
			{	temp[i].key = memory_array[cur_memory_bucket].key;	
				temp[i++].value = memory_array[cur_memory_bucket].value;
				cur_memory_bucket++;
//printf("Temp1: %d[%d]\n",temp[i-1], i-1); 
			}
			// memory array is running out
			else if (max_memory_bucket < cur_memory_bucket && max_page_bucket >= cur_page_bucket)
			{	temp[i].key = ((struct key_value*)page_in)[cur_page_bucket].key;	
				temp[i++].value = ((struct key_value*)page_in)[cur_page_bucket].value;
				cur_page_bucket++;
//printf("Temp2: %d[%d]\n",temp[i-1], i-1); 
			}
			// both running out, we are done with merging
			//else if (max_memory_bucket < cur_memory_bucket && max_page_bucket < cur_page_bucket)
			//{	break;
			//}
			// memory array is the next smallest
			else if (memory_array[cur_memory_bucket].key < ((struct key_value*)page_in)[cur_page_bucket].key)
			{	temp[i].key = memory_array[cur_memory_bucket].key;	
				temp[i++].value = memory_array[cur_memory_bucket].value;
				cur_memory_bucket++;
//printf("Temp3: %d[%d]\n",temp[i-1], i-1); 
			}
			// page array is the next smallest
			else 
			{	temp[i].key = ((struct key_value*)page_in)[cur_page_bucket].key;	
				temp[i++].value = ((struct key_value*)page_in)[cur_page_bucket].value;
				cur_page_bucket++;
//printf("Temp4: %d[%d]\n",temp[i-1], i-1); 
			}
		}
		//db.flush();
		//db.seekg(0);
		//db.seekp(0);

//header.count = -100;
//header.prev = -100;
		//db.read ((char*)&header, sizeof(struct page_header)); // read page header
		//cout << " Just read header: " << header.count << ", " << header.prev << endl;

//temp[0].key = 100;
		//db.read ((char*)(temp), 4); // read page header
		//cout << "just read contents: " << temp[0].key << " " << temp[0].value << " " << temp[1].key << " " << temp[1].value << endl;
	}
	i;
	struct key_value_h* temp_write = temp;	
	int write_count = i;
	if (PAGE_SIZE < i*sizeof(struct key_value) + sizeof(struct page_header))
	{	
		//cout << "Exceed : i=" << i << ", " << i*sizeof(struct key_value) + sizeof(struct page_header) << endl; 
		write_header.count = (PAGE_SIZE - sizeof(struct page_header)) / sizeof(key_value);
		write_header.prev = header.prev;

		page_count++;
		db.write((char*)&write_header, sizeof(struct page_header));
		db.write((char*)temp_write, write_header.count * sizeof(struct key_value));
		temp_write = temp_write + write_header.count;
		write_count -= write_header.count;
		header.prev = db_position;


		//cout << "Writing header: " << write_header.count << ", " << write_header.prev << "(" << db_position << ")" << endl;
		//cout << "Writing contents ( " << write_header.count * sizeof(struct key_value)<< ") " << temp[0].key << ", " << temp[0].value << endl;
	}
	
	write_header.count = write_count;
	write_header.prev = header.prev;
		//if (page_count == 0)
		//	write_header.prev = -1;

//printf ("writing count: %d, temp_write: %d, %d \n", write_header.count, temp_write[0], temp_write[1]);
//		cout << "Writing header: " << write_header.count << ", " << write_header.prev << "(" << db_position << ")" << endl;
//		cout << "Writing contents ( " << write_header.count * sizeof(struct key_value)<< ") " << temp[0].key << ", " << temp[0].value << endl;

		//if (!isFirstLoop)		
		//	page_count++;
		//else
		//{	db.seekp (db_position);  // last write page pointer
		//	db.seekg (db_position);  // last read page pointer
		//}


//db.seekp (0);  // last write page pointer
//db.seekg (0);  // last read page pointer


		db.write((char*)&write_header, sizeof(struct page_header));
		db.write((char*)temp_write, write_header.count * sizeof(struct key_value));

//db.flush();
//header.count = -10;
//header.prev = -100;
//db.seekp (0);  // last write page pointer
//db.seekg (0);  // last read page pointer
//db.read ((char*)&header, sizeof(struct page_header)); // read page header
		//db.read ((char*)&page_in, header.count * sizeof(struct key_value)); // read contents

//cout << "Reading header: " << header.count << ", " << header.prev << "(" << db_position << ")" << endl;
	
//	header.prev = db_position;
//		header.count = 0;
		//isFirstLoop = false;


	cur_mem_size = 0;

	delete[] temp;
}

void Merge_Array(struct key_value* memory_array)
{

	int db_position = PAGE_SIZE * (page_count - 1);
//printf("DB position: %d\n", db_position);
	if (page_count != 0 )
	{	db.seekp (db_position);  // last write page pointer
		db.seekg (db_position);  // last read page pointer
	}
	struct page_header header, write_header;
	//key_value_array;

	if (page_count != 0)	
	{	db.read ((char*)&header, sizeof(struct page_header)); // read page header
		db.read ((char*)&page_in, header.count * sizeof(struct key_value)); // read contents

		db.seekp (db_position);  // last write page pointer
		db.seekg (db_position);  // last read page pointer
	}
	else
	{	header.count = 0;
		header.prev = -1;

		db.seekp (0);  // last write page pointer
		db.seekg (0);  // last read page pointer
		page_count++;
		//cout << "Merge start" << endl;
	}


	//	cout << "Reading header: " << header.count << ", " << header.prev << "(" << db_position << ")" << endl;

	int temp_buckets = (PAGE_SIZE - sizeof(struct page_header)) / sizeof(struct key_value) + MEMORY_SIZE;
	struct key_value* temp = new struct key_value[temp_buckets];
	int cur_page_bucket = 0;
	int cur_memory_bucket = 0;
	int max_memory_bucket = cur_mem_size / sizeof(struct key_value) - 1; // inclusive
	int max_page_bucket = header.count - 1;
	int i = 0;	
	bool isFirstLoop = true;
	while (max_memory_bucket >= cur_memory_bucket || max_page_bucket >= cur_page_bucket)
	{
		//for (i = 0; i < temp_buckets; i++) 
		{	// page array is running out
			//if (max_memory_bucket < cur_memory_bucket && max_page_bucket < cur_page_bucket)
			//	break;
			if (max_memory_bucket > cur_memory_bucket && memory_array[cur_memory_bucket].value == DELETED)
			{	
				cur_memory_bucket++;
				continue;
			}
			if (max_page_bucket > cur_page_bucket && ((struct key_value*)page_in)[cur_page_bucket].value == DELETED) 
			{	
				cur_page_bucket++;
				continue;
			}

			if (max_memory_bucket >= cur_memory_bucket && max_page_bucket < cur_page_bucket)
			{	temp[i].key = memory_array[cur_memory_bucket].key;	
				temp[i++].value = memory_array[cur_memory_bucket].value;
				cur_memory_bucket++;
//printf("Temp1: %d[%d]\n",temp[i-1], i-1); 
			}
			// memory array is running out
			else if (max_memory_bucket < cur_memory_bucket && max_page_bucket >= cur_page_bucket)
			{	temp[i].key = ((struct key_value*)page_in)[cur_page_bucket].key;	
				temp[i++].value = ((struct key_value*)page_in)[cur_page_bucket].value;
				cur_page_bucket++;
//printf("Temp2: %d[%d]\n",temp[i-1], i-1); 
			}
			// both running out, we are done with merging
			//else if (max_memory_bucket < cur_memory_bucket && max_page_bucket < cur_page_bucket)
			//{	break;
			//}
			// memory array is the next smallest
			else if (memory_array[cur_memory_bucket].key < ((struct key_value*)page_in)[cur_page_bucket].key)
			{	temp[i].key = memory_array[cur_memory_bucket].key;	
				temp[i++].value = memory_array[cur_memory_bucket].value;
				cur_memory_bucket++;
//printf("Temp3: %d[%d]\n",temp[i-1], i-1); 
			}
			// page array is the next smallest
			else 
			{	temp[i].key = ((struct key_value*)page_in)[cur_page_bucket].key;	
				temp[i++].value = ((struct key_value*)page_in)[cur_page_bucket].value;
				cur_page_bucket++;
//printf("Temp4: %d[%d]\n",temp[i-1], i-1); 
			}
		}
		//db.flush();
		//db.seekg(0);
		//db.seekp(0);

//header.count = -100;
//header.prev = -100;
		//db.read ((char*)&header, sizeof(struct page_header)); // read page header
		//cout << " Just read header: " << header.count << ", " << header.prev << endl;

//temp[0].key = 100;
		//db.read ((char*)(temp), 4); // read page header
		//cout << "just read contents: " << temp[0].key << " " << temp[0].value << " " << temp[1].key << " " << temp[1].value << endl;
	}
	i;
	struct key_value* temp_write = temp;	
	int write_count = i;
	if (PAGE_SIZE < i*sizeof(struct key_value) + sizeof(struct page_header))
	{	
		//cout << "Exceed : i=" << i << ", " << i*sizeof(struct key_value) + sizeof(struct page_header) << endl; 
		write_header.count = (PAGE_SIZE - sizeof(struct page_header)) / sizeof(key_value);
		write_header.prev = header.prev;

		page_count++;
		db.write((char*)&write_header, sizeof(struct page_header));
		db.write((char*)temp_write, write_header.count * sizeof(struct key_value));
		temp_write = temp_write + write_header.count;
		write_count -= write_header.count;
		header.prev = db_position;


		//cout << "Writing header: " << write_header.count << ", " << write_header.prev << "(" << db_position << ")" << endl;
		//cout << "Writing contents ( " << write_header.count * sizeof(struct key_value)<< ") " << temp[0].key << ", " << temp[0].value << endl;
	}
	
	write_header.count = write_count;
	write_header.prev = header.prev;
		//if (page_count == 0)
		//	write_header.prev = -1;

//printf ("writing count: %d, temp_write: %d, %d \n", write_header.count, temp_write[0], temp_write[1]);
//		cout << "Writing header: " << write_header.count << ", " << write_header.prev << "(" << db_position << ")" << endl;
//		cout << "Writing contents ( " << write_header.count * sizeof(struct key_value)<< ") " << temp[0].key << ", " << temp[0].value << endl;

		//if (!isFirstLoop)		
		//	page_count++;
		//else
		//{	db.seekp (db_position);  // last write page pointer
		//	db.seekg (db_position);  // last read page pointer
		//}


//db.seekp (0);  // last write page pointer
//db.seekg (0);  // last read page pointer


		db.write((char*)&write_header, sizeof(struct page_header));
		db.write((char*)temp_write, write_header.count * sizeof(struct key_value));

//db.flush();
//header.count = -10;
//header.prev = -100;
//db.seekp (0);  // last write page pointer
//db.seekg (0);  // last read page pointer
//db.read ((char*)&header, sizeof(struct page_header)); // read page header
		//db.read ((char*)&page_in, header.count * sizeof(struct key_value)); // read contents

//cout << "Reading header: " << header.count << ", " << header.prev << "(" << db_position << ")" << endl;
	
//	header.prev = db_position;
//		header.count = 0;
		//isFirstLoop = false;


	cur_mem_size = 0;

	delete[] temp;
}

void Update_Array(struct key_value* memory_array, int key, int value, bool isDelete)
{
	int insert_index = 0;
	int cur_mem_elements = cur_mem_size / sizeof(struct key_value);

	for (insert_index = 0; insert_index < cur_mem_elements; insert_index++)
	{	
		if (memory_array[insert_index].key == key) // it's an update 
		{	insert_index = insert_index;
			break;
		}
		else if (memory_array[insert_index].key < key)
			continue;
		else
		{	for (int j = cur_mem_elements - 1; j >= insert_index; j--)
			{	memory_array[j + 1] = memory_array[j];  // shift all elements one right
			}
			//cur_mem_size += sizeof(struct key_value);
			break;
		}
	}
	// Insertion bucket is found
	if (!isDelete) // insertion or update
	{	
		if (memory_array[insert_index].key != key) // a new insertion
		{	all_elements++;
			cur_mem_size += sizeof(struct key_value);
			BloomPut(bloom_filter, key);
		}
		memory_array[insert_index].key = key;
		memory_array[insert_index].value = value;
		// if we can't insert another element in the future, flush to the disk
		if (MEMORY_SIZE < cur_mem_size + sizeof(struct key_value)) 
		{	
			Merge_Array(memory_array);
			free(bloom_filter);
			bloom_filter = BloomInit();
		}
	}
	else if (memory_array[insert_index].value != DELETED) // a new deletion	
	{	memory_array[insert_index].value = DELETED;
		all_elements--;
	}	
}

void Update_Hashtable(struct key_value* memory_array, int key, int value, bool isDelete)
{
   int insert_index = 0;
   int cur_mem_elements = cur_mem_size / sizeof(struct key_value_h);

	if (!isDelete)
		HashTable_Put(hashtable, key, value);
	else
		HashTable_Delete(hashtable, key);
		
	if (MEMORY_SIZE < cur_mem_size + sizeof(struct key_value_h))
      {
         Merge_Hashtable(memory_array);
         free(bloom_filter);
         bloom_filter = BloomInit();
      }
}




void Update_Heap(struct key_value* memory_array, int key, int value, bool isDelete)
{
	int insert_index = 0;
	int cur_mem_elements = cur_mem_size / sizeof(struct key_value);

	for (insert_index = 0; insert_index < cur_mem_elements; insert_index++)
	{	
		if (memory_array[insert_index].key == key) // it's an update 
		{	insert_index = insert_index;
			break;
		}
		else if (memory_array[insert_index].key < key)
			continue;
		else
		{	for (int j = cur_mem_elements - 1; j >= insert_index; j--)
			{	memory_array[j + 1] = memory_array[j];  // shift all elements one right
			}
			//cur_mem_size += sizeof(struct key_value);
			break;
		}
	}
	// Insertion bucket is found
	if (!isDelete) // insertion or update
	{	
		if (memory_array[insert_index].key != key) // a new insertion
		{	all_elements++;
			cur_mem_size += sizeof(struct key_value);
			BloomPut(bloom_filter, key);
		}
		memory_array[insert_index].key = key;
		memory_array[insert_index].value = value;
		// if we can't insert another element in the future, flush to the disk
		if (MEMORY_SIZE < cur_mem_size + sizeof(struct key_value)) 
		{	
			Merge_Array(memory_array);
			free(bloom_filter);
			bloom_filter = BloomInit();
		}
	}
	else if (memory_array[insert_index].value != DELETED) // a new deletion	
	{	memory_array[insert_index].value = DELETED;

		all_elements--;
	}	
}


bool Heap_Search(struct key_value* mem_array, int key, int array_bucket_num)
{
	int step = array_bucket_num;
	int cur = step / 2;
	step /= 2;
	
	if (array_bucket_num == 0)
		return false;

	while (true)
	{
		if (mem_array[cur].key < key)		
		{	step /= 2;
			if (step == 0)
				break; // not found
			cur += step;
		}
		else if (mem_array[cur].key > key)
		{	step /= 2;
			if (step == 0)
				break; // not found
			cur -= step;
		}
		else // found the value, but it might have been deleted 
		{	if (mem_array[cur].value != DELETED)
				cout << mem_array[cur].value << endl;
			return true;
		}
	}
	if (cur - 1 >= 0 && mem_array[cur - 1].key == key && mem_array[cur - 1].value != DELETED)
	{	cout << mem_array[cur - 1].value << endl;
		return true;
	}
	if (cur + 1 < array_bucket_num && mem_array[cur + 1].key == key && mem_array[cur + 1].value != DELETED)
	{	cout << mem_array[cur + 1].value << endl;
		return true;
	}
	return false;
}



void put(int index, int value)
{
	Update_Array((struct key_value*)(lsm_memory), index, value, false);
	// Memory is full, so flush memory into disk
	
	return;
}


bool Hash_Search()
{



}

void Hash_Range_Search()
{
}


bool Array_Binary_Search(struct key_value* mem_array, int key, int array_bucket_num)
{
	int step = array_bucket_num;
	int cur = step / 2;
	step /= 2;
	
	if (array_bucket_num == 0)
		return false;

	while (true)
	{
		if (mem_array[cur].key < key)		
		{	step /= 2;
			if (step == 0)
				break; // not found
			cur += step;
		}
		else if (mem_array[cur].key > key)
		{	step /= 2;
			if (step == 0)
				break; // not found
			cur -= step;
		}
		else // found the value, but it might have been deleted 
		{	if (mem_array[cur].value != DELETED)
				cout << mem_array[cur].value << endl;
			return true;
		}
	}
	if (cur - 1 >= 0 && mem_array[cur - 1].key == key && mem_array[cur - 1].value != DELETED)
	{	cout << mem_array[cur - 1].value << endl;
		return true;
	}
	if (cur + 1 < array_bucket_num && mem_array[cur + 1].key == key && mem_array[cur + 1].value != DELETED)
	{	cout << mem_array[cur + 1].value << endl;
		return true;
	}
	return false;
}



void Get_Array(int key)
{

	// Search Bloom Filter ************************



	// ************************

	bool isFound = false;
	if (BloomGet(bloom_filter, key))
		isFound = Array_Binary_Search((struct key_value*)lsm_memory, key, cur_mem_size / sizeof (struct key_value));
	if (isFound)
	{	return;
	}
	// search the disk
	if (page_count == 0)
		return;


	db.seekg (PAGE_SIZE * (page_count - 1));  // last read page pointer

	struct page_header header, write_header;
	//key_value_array;
	
	while (true)
	{	
		db.read ((char*)&header, sizeof(struct page_header)); // read page header
		db.read ((char*)&page_in, header.count * sizeof(struct page_header)); // read contents
//printf("Still...\n");
	//if (BloomGet(bloom, key))
		isFound = Array_Binary_Search((struct key_value*)page_in, key, header.count);
		if (isFound)
		{	return;
		}
		if (header.prev == -1) // not found even in the disk
			return;
		
		db.seekg(header.prev); // move the disk pointer to the past block
	}
printf("don\n");
}

void get(int key)
{
	if (all_elements == 0)
		return;

	Get_Array(key);	
}


void Array_Range_Search(struct key_value* mem_array, int lower, int upper, int array_bucket_num)
{
	// scan and print all matching ones
	for (int i = 0; i < array_bucket_num; i++)
	{	
		if (mem_array[i].key >= lower && mem_array[i].key < upper && mem_array[i].value != DELETED)
			cout << mem_array[i].key << ":" << mem_array[i].value << " ";
	}
	cout << endl;
}


int Array_Print(struct key_value* mem_array, int array_bucket_num, int level)
{
	// scan and print all matching ones
	for (int i = 0; i < array_bucket_num; i++)
	{	if (mem_array[i].value != DELETED)
			cout << mem_array[i].key << ":" << mem_array[i].value << ":L" << level << " ";
	}
}

void range(int lower, int upper)
{
	// scan the memory, first
	Array_Range_Search((struct key_value*)lsm_memory, lower, upper, cur_mem_size / sizeof (struct key_value)); 

	// scan the whole disk
	if (page_count == 0)
		return;


	db.seekg (PAGE_SIZE * (page_count - 1));  // last read page pointer
//db.seekg (0);  // last read page pointer
	struct page_header header, write_header;
	
	while (true)
	{	

		db.read ((char*)&header, sizeof(struct page_header)); // read page header

		db.read ((char*)&page_in, header.count * sizeof(struct page_header)); // read contents
//cout << "range read: " << header.count << " " << header.prev << endl;
		Array_Range_Search((struct key_value*)page_in, lower, upper, header.count);
		if (header.prev == -1) // end of the disk
			return;
//cout << "HERE!" << endl;	
		db.seekg(header.prev); // move the disk pointer to the past block
	}
}

void del(int index)
{
	if (all_elements == 0)
		return;

	Update_Array((struct key_value*)(lsm_memory), index, DELETED, true);
}

void load(string filename)
{
	string line;
//exit(0);
	ifstream myfile(filename.c_str());


	if (myfile.is_open())
	{

		gettimeofday(&start_time,NULL);

		int ops = 0;
		while ( getline (myfile,line) )
		{ 
			//int err = pthread_create(&(tid[curThread++ % THREAD_NUM]), NULL, &parse, (void*)line.c_str());
			const char* cstr = line.c_str();
			parse((void*)cstr);
			ops++;
			//pthread_join(tid[(curThread- 1) % THREAD_NUM], NULL);
			//parse(line);
		}
		gettimeofday(&end_time,NULL);
	double delay = ((end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec))/1000000.0;
		//printf("%f sec (%d reads, %d writes), %d items, %f op/s\n", delay, read_total, write_total, all_elements, ops/delay);
		      printf("%d op/s\n", (int)(ops/delay));
		myfile.close();
  }

		//printf("%f sec\n", ((end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec))/1000000.0);
}

void stat()
{
	cout << endl << "Total Pairs: " << all_elements << endl << endl;
	int num = Array_Print((struct key_value*)lsm_memory, cur_mem_size / sizeof (struct key_value), 1);
	cout << endl << "----- LV1: " << num << endl << endl;
	struct page_header header;
	int level = 2;

	if (page_count == 0)
		return;

	db.seekg (PAGE_SIZE * (page_count - 1));  // last read page pointer
	db.seekp (PAGE_SIZE * (page_count - 1));  // last write page pointer
	while (true)
	{	
		num = 0;
//cout << "Filepos: " << PAGE_SIZE * (page_count - 1) << endl;
		db.read ((char*)&header, sizeof(struct page_header)); // read page header
		db.read ((char*)&page_in, header.count * sizeof(struct key_value)); // read contents
//cout << header.prev << " " << page_count << " " <<  endl;

//cout << "Header: " << header.prev << " " << header.count << " " << " " << ((struct key_value*)(page_in))[0].key << endl;
//return;
		num += Array_Print((struct key_value*)page_in, header.count, 2);
//cout << header.prev << endl;
		cout << endl;

		cout << "----- LV" << level++ << ": " << num << endl << endl;;

		if (header.prev == -1) // end of the disk
			break;
//cout << header.prev << " " << header.count << endl;
		db.seekg(header.prev); // move the disk pointer to the past block

//string a;
//getline(cin, a);	

	}
}

inline void* parse(void* arg)
{	stringstream ss;
	int index, value;
	string filename, command;
	string line((char*)arg);
	//cout << "Command: " << line << endl;
	ss.str(line);
	ss >> command;

	if (command.length() != 1)
	{	cout << "Usage: " << endl << "p [int] [int]" << endl << "g [int]" << endl << "r [int] [int]" << endl 
			<< "d [int]" << endl << "l filename" << endl << "s" << endl; 
	}
	else if (command[0] == 'p')
	{	ss >> index;
		ss >> value;
		pthread_rwlock_wrlock(&lock[0]);
		put(index, value);
		write_total++;
		pthread_rwlock_unlock(&lock[0]);
	}
	else if (command[0] == 'g')
	{	ss >> index;
		pthread_rwlock_wrlock(&lock[0]);
		get(index);
		read_total++;
		pthread_rwlock_unlock(&lock[0]);
	}
	else if (command[0] == 'r')
	{	ss >> index;
		ss >> value;
		pthread_rwlock_rdlock(&lock[0]);
		range(index, value);
		read_total++;
		pthread_rwlock_unlock(&lock[0]);
	}
	else if (command[0] == 'd')
	{	ss >> index;
		pthread_rwlock_wrlock(&lock[0]);
		del(index);
		write_total++;
		pthread_rwlock_unlock(&lock[0]);
	}
	else if (command[0] == 'l')
	{	ss >> filename;
		load(filename);
	}
	else if (command[0] == 's')
	{	stat();
	}
	else
	{	cout << "Usage: " << endl << "p [int] [int]" << endl << "g [int]" << endl << "r [int] [int]" << endl 
			<< "d [int]" << endl << "l filename" << endl << "s" << endl; 
	}
	//cout << command << " " << index << " " << value << endl;
}

int main()
{
	string line;
	string filename;
	int index, value;
	string command;

	bloom_filter = BloomInit();

	hashtable = HashTable_Init(HASH_BUCKET_SIZE); //HASHTABLE
	
	db.open("db.bin", fstream::in | fstream::out | fstream::binary | fstream::trunc);
	//myfile << "Writing this to a file.\n";
	//db_file.close();	

	for (int i = 0; i < FILE_NUM; i++)
	{	if (pthread_rwlock_init(&lock[i], NULL) != 0)
		{	printf("\n mutex init failed\n");
			exit(0);
		}
	}

	while (true)
	{	
		getline(cin, line);
		const char* cstr = line.c_str();
		parse((void*)cstr);
	}
	return 0;
}
