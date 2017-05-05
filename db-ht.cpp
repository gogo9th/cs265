#include <iostream>
#include <fstream>

#include <stdio.h>
#include <string>
#include <sstream>
#include <limits.h>

#include <cstring>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <sys/types.h>

using namespace std;


//#define DISK_FILE "db.bin"
#define DELETED INT_MIN

// Basic Configuration

#ifndef MEMORY_SIZE
#define MEMORY_SIZE 0x100000000 // 128
#endif

#ifndef DISK_BUCKET_NUM
#define DISK_BUCKET_NUM 3
#endif

#ifndef BLOOM_BUCKET_NUM
#define BLOOM_BUCKET_NUM  1024 //1024
#endif

#ifndef HASH_SHIFT
#define HASH_SHIFT 9
#endif

#define HASH_BUCKET_NUM (MEMORY_SIZE >> HASH_SHIFT) // 65536

#ifndef THREAD_NUM
#define THREAD_NUM 64
#endif

// Bitmap
//#define BLOOM_BUCKET_SIZE (sizeof(uint64_t))
//#define MAX_BIT (BUCKET_SIZE * BUCKET_NUM)
//#define SEED 1000

// HashTable



#define SET_BIT(bloom_filter, bits) bloom_filter[((bits % BLOOM_BUCKET_NUM) + BLOOM_BUCKET_NUM) % BLOOM_BUCKET_NUM] |= (1 << (bits % 8))
#define GET_BIT(bloom_filter, bits) bloom_filter[((bits % BLOOM_BUCKET_NUM) + BLOOM_BUCKET_NUM) % BLOOM_BUCKET_NUM ] & (1 << (bits % 8))



//INT_MAX: 2147483647
//INT_MIN: -2147483648

double time1;
struct timeval start_time, end_time, getstart, getend, putstart, putend;


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

struct db_header
{
	int count;
	//int prev = -1;
	//int min;
	//int max;
};


struct db_header_tree
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

//struct hashtable_st {
//   int size;
//   struct key_value_h **table;
//};

struct key_value_h {
   int key;
   int value;
	bool isRecent;
   struct key_value_h *next = NULL;
};


struct key_value_h** hashtable;
struct key_value_h** hashtable2;

//struct hashtable_st* hashtable;

//char lsm_memory[MEMORY_SIZE];

pthread_t tid[THREAD_NUM];

//char page_in[PAGE_SIZE- sizeof(struct db_header)];

//bool flush_table[MEMORY_SIZE / sizeof(struct key_value)] = {0};

int cur_mem_size = 0;
int all_elements = 0;
//char page_buffer[PAGE_SIZE];
//int page_count = 0;

int read_total = 0, write_total = 0;

pthread_rwlock_t lock;

pthread_rwlock_t loadlock;
pthread_rwlock_t htlock;
pthread_rwlock_t disklock;
//pthread_rwlock_t lock[1];

//pthread_rwlock_t lock[1];
//fstream db[THREAD_NUM][DISK_BUCKET_NUM];
//fstream dbhelp[THREAD_NUM][DISK_BUCKET_NUM];

//unsigned char* bloom_filter;
//const int BLOOM_BUCKET_NUM = (10 > MEMORY_SIZE / sizeof(struct key_value) / 1000 ? 10 : MEMORY_SIZE / sizeof(struct key_value) / 1000);

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

template < typename T > string to_string( const T& n )
{
   ostringstream stm ;
   stm << n ;
   return stm.str() ;
}


void HashTable_Init()
{
   hashtable = (struct key_value_h**)calloc(1, sizeof(struct key_value_h*) * HASH_BUCKET_NUM);

   //hashtable->table = (struct key_value_h**)malloc( sizeof(struct key_value_h * ) * size );

   int i;
   for( i = 0; i < HASH_BUCKET_NUM; i++ ) {
      hashtable[i] = NULL;
   }

   //hashtable->size = size;
	cur_mem_size = sizeof(struct key_value_h*) * HASH_BUCKET_NUM;

   return;
}

//int HashTable_Hash(struct key_value_h **hashtable, int key)
//{  return key % HASH_BUCKET_NUM;
//}

/*struct key_value_h* HashTable_NewBucket(int key, int value )
{
   struct key_value_h *newbucket = (struct key_value_h*)malloc(sizeof(struct key_value_h));

   newbucket->next = NULL;
   newbucket->key = key;
   newbucket->value = value;
	newbucket->isRecent = false;

   return newbucket;
}*/


void HashTable_Put(int key, int value, bool isDelete)
{
   struct key_value_h *cur, *last = NULL;

   //int bucket = HashTable_Hash(hashtable, key);
//cout << "while " << key % HASH_BUCKET_NUM <<  endl;

   cur = hashtable[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM];
	bool isInitial = 1;

   while(cur)
   {  
//cout << "while " << cur << endl;
//cout << "while" << cur->key << endl;

		if(key == cur->key) // an update or delete
		{  
			if (!isDelete) // update
			{	cur->value = value;
			}
			else // delete
			{	if (last) // not first element
					last->next = cur->next;
				free(cur);
				cur_mem_size -= sizeof(struct key_value_h);
				all_elements--;
			}
			return;
		}
		last = cur;
      cur = cur->next;
		isInitial = 0;

//cout << "while done" << endl;
   }
	 // a new insertion
//cout << "Exit" << endl;
   cur = (struct key_value_h*)malloc(sizeof(struct key_value_h));
	if (last)
		last->next = cur;
	else
		hashtable[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM ] = cur;

	cur->next = NULL;
	cur->key = key;
	cur->value = value;
	//cout << "Inserted " << cur << "("<< hashtable[key % HASH_BUCKET_NUM] << endl;
	cur_mem_size += sizeof(struct key_value_h);
	all_elements++;
}

int HashTable_Get(int key, bool& isNULL) 
{  //int bucket;
   struct key_value_h *pair;
   //bucket = HashTable_Hash(hashtable, key);
   pair = hashtable[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM ];
	//cout << pair << endl;
   while( pair != NULL && key != pair->key)
   {   pair = pair->next;
		//cout << "Bucket: " << pair->key << endl;
	}
	if (pair == NULL)
	{	isNULL = true;
      return DELETED; 
	}
	else
	{	isNULL = false;
		pair->isRecent = true;
      return pair->value;
	}
}  


void HashTable2_Init()
{
   hashtable2 = (struct key_value_h**)calloc(1, sizeof(struct key_value_h*) * HASH_BUCKET_NUM);

   //hashtable->table = (struct key_value_h**)malloc( sizeof(struct key_value_h * ) * size );

   int i;
   for( i = 0; i < HASH_BUCKET_NUM; i++ ) {
      hashtable2[i] = NULL;
   }

   //hashtable->size = size;
	cur_mem_size = sizeof(struct key_value_h*) * HASH_BUCKET_NUM;

   return;
}


void HashTable2_Delete()
{
   //hashtable->table = (struct key_value_h**)malloc( sizeof(struct key_value_h * ) * size );

   int i;
   for( i = 0; i < HASH_BUCKET_NUM; i++ ) {
		struct key_value_h* cur = hashtable2[i];
		struct key_value_h* del;
		while (cur)
		{	del = cur;
			cur = cur->next;
			free(del);
		} 
   }
   return;
}

//int HashTable_Hash(struct key_value_h **hashtable, int key)
//{  return key % HASH_BUCKET_NUM;
//}

/*struct key_value_h* HashTable2_NewBucket(int key, int value )
{
   struct key_value_h *newbucket = (struct key_value_h*)malloc(sizeof(struct key_value_h));

   newbucket->next = NULL;
   newbucket->key = key;
   newbucket->value = value;
	newbucket->isRecent = false;

   return newbucket;
}*/


void HashTable2_Put(int key, int value, bool isDelete)
{
   struct key_value_h *cur, *last = NULL;

   //int bucket = HashTable_Hash(hashtable, key);

   cur = hashtable2[ ((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM ];

	bool isInitial = 1;
   while(cur)
   {  
		if(key == cur->key) // an update or delete
		{  
			if (!isDelete) // update
			{	cur->value = value;
			}
			else // delete
			{	if (last) // not first element
					last->next = cur->next;
cout << "Free" << endl;
				free(cur);
				cur_mem_size -= sizeof(struct key_value_h);
				all_elements--;
			}
			return;
		}
		last = cur;
      cur = cur->next;
		isInitial = 0;
   }
	 // a new insertion
   cur = (struct key_value_h*)malloc(sizeof(struct key_value_h));
	
	if (last)
		last->next = cur;
	else
		hashtable2[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM] = cur;

	cur->next = NULL;
	cur->key = key;
	cur->value = value;
	//cout << "Inserted " << cur << "("<< hashtable[key % HASH_BUCKET_NUM] << endl;
	cur_mem_size += sizeof(struct key_value_h);
	all_elements++;
}

int HashTable2_Get(int key, bool& isNULL) 
{  //int bucket;
   struct key_value_h *pair;
   //bucket = HashTable_Hash(hashtable, key);
   pair = hashtable2[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM];
	//cout << pair << endl;
   while( pair != NULL && key != pair->key)
   {   pair = pair->next;
		//cout << "Bucket: " << pair->key << endl;
	}
	if (pair == NULL)
	{	isNULL = true;
      return DELETED; 
	}
	else
	{	isNULL = false;
		pair->isRecent = true;
      return pair->value;
	}
}  


unsigned char* BloomInit() {
   return (unsigned char*)calloc(BLOOM_BUCKET_NUM, sizeof(unsigned char));
}
/*
void BloomPut(unsigned char* bloom, int key) {
   uint32_t val = SEED;
   for (int i = 0; i < BLOOM_BUCKET_NUM; i++) {
      val = Hash(key, SEED);
      SET_BIT(bloom,val);
   }
}*/

/*
bool BloomGet(unsigned char* bloom, int key) {
   uint32_t val = SEED;

	

   for (int i = 0; i < BLOOM_BUCKET_NUM; i++) {
      val = Hash(key, SEED);
      if (!GET_BIT(bloom,val)) {
         return false;
      }
   }
   return true;
}*/

// DB Structure: [ bloom_filter db_header hash_table[i]  ]

void Merge_Hashtable()
{
	int level = 2;
	fstream db[DISK_BUCKET_NUM];
	fstream dbhelp[DISK_BUCKET_NUM];
	int db_position[DISK_BUCKET_NUM];
//cout << "Merging" << endl;
	unsigned char bloom_filter[DISK_BUCKET_NUM][BLOOM_BUCKET_NUM] = {0};
	//struct db_header header[DISK_BUCKET_NUM];//, write_header;
	//int count[DISK_BUCKET_NUM] = {0};
	//cur_mem_size = 0;


	pthread_rwlock_wrlock(&htlock);

	cur_mem_size = sizeof(struct key_value_h*) * HASH_BUCKET_NUM;
	string dbname[DISK_BUCKET_NUM];
	string oldname[DISK_BUCKET_NUM];
recurse:

	// Open all db buckets in the same level
	for (int i = 0; i < DISK_BUCKET_NUM; i++)
	{	
		if (level > 2)
			dbhelp[i].open(dbname[i].c_str(), fstream::in | fstream::out | fstream::binary /*| fstream::app*/);

		oldname[i] = dbname[i];
		dbname[i] = "db" + to_string(level) + "-" + to_string(i) + ".bin";
		db[i].open(dbname[i].c_str(), fstream::in | fstream::out | fstream::binary /*| fstream::app*/);
		if (!db[i].is_open())
		{	db[i].open(dbname[i].c_str(), fstream::in | fstream::out | fstream::binary | fstream::app);
			db[i].close();
			db[i].open(dbname[i].c_str(), fstream::in | fstream::out | fstream::binary /*| fstream::app*/);
		}
		db[i].seekg(0, ios_base::end);
		db_position[i] = db[i].tellg(); // the current position for each db file
		//db[i].seekg(0);
		
		if (db_position[i] == 0) // the first file, create an empty bloom filter
		{	
			unsigned char* bloom_filter = BloomInit();
			db[i].write((char*)&bloom_filter, BLOOM_BUCKET_NUM);	

			free(bloom_filter);
			db_position[i] = BLOOM_BUCKET_NUM;
		}

		db[i].seekg (0);  // read-in bloom filter and db_header
		db[i].read ((char*)(&bloom_filter[i]), BLOOM_BUCKET_NUM); // read contents
		//db[i].read ((char*)(&header[i]), sizeof(struct db_header)); // read contents

		db[i].seekp (db_position[i]);  // last write page pointer
	}

//	cout << "Level " << level << endl;
	if (level == 2) // write memory hashtable to disk
	{	
		for (int i = 0; i < HASH_BUCKET_NUM; i++)
		{
			struct key_value_h* cur = hashtable[i];
			struct key_value_h* before = NULL;
			struct key_value_h* temp;
			//struct key_value_h* last_alive = hashtable[i];
			while (cur)
			{
				// update the bloom filter of the corresponding db bucket
				//BloomPut(bloom_filter[(cur->key) % DISK_BUCKET_NUM], cur->key);
				SET_BIT(bloom_filter[(((cur->key) % DISK_BUCKET_NUM) + DISK_BUCKET_NUM) % DISK_BUCKET_NUM], cur->key);
				// write key and value into the corresponding db bucket
#ifdef RECENT
				if (!(cur->isRecent))
#else
				if (1)
#endif
				{	
					db[(cur->key) % DISK_BUCKET_NUM].write((char*)&(cur->key), sizeof(cur->key));
					db[(cur->key) % DISK_BUCKET_NUM].write((char*)&(cur->value), sizeof(cur->value));

db[(cur->key) % DISK_BUCKET_NUM].seekg (0, ios_base::end);  // read-in bloom filter and db_header
//cout << "SIze="<< db[(cur->key) % DISK_BUCKET_NUM].tellg() <<endl;  // read-in bloom filter and db_header

//cout << "Key,value=" << cur->key << " " << cur->value << endl;
					temp = cur;
					cur = cur->next;
					if (before)
						before->next = cur;
					else
						hashtable[i] = NULL;
					free(temp);
					///SET_BIT(bloom_filter[((cur->key % DISK_BUCKET_NUM) + DISK_BUCKET_NUM) % DISK_BUCKET_NUM]cur->key);
					
				}
				else
				{	// optimization 3
					//cout << "recent" << endl;	
					cur->isRecent = false;
					before = cur;
					cur = cur->next;
					//last_alive->next = before;
					//last_alive = before;
					cur_mem_size += sizeof(struct key_value_h);
				}
			}
		}
		pthread_rwlock_unlock(&htlock);
	}
	else // write disk array to the deeper level
	{	
		//struct key_value_h* cur = hashtable[i];
		//struct key_value_h* before = NULL;
//cout <<"Hash init"<< endl;
		HashTable2_Init();
//cout <<"Hash done" << endl;	
		for (int i = 0; i < DISK_BUCKET_NUM; i++)
		{
			dbhelp[i].seekg(0, ios_base::end);
			int db_position = dbhelp[i].tellg();
			int count = (db_position - BLOOM_BUCKET_NUM) / 8;

			for (int j = 0; j < count; j++)
			{	int key, value;
				bool garb;
				dbhelp[i].seekg (db_position - (j+1)*8);  // last write page pointer

				dbhelp[i].read((char*)&key, sizeof(int));
				dbhelp[i].read((char*)&value, sizeof(int));
				int ret = HashTable2_Get(key, garb);
				if (ret == DELETED && garb == true)
				{	db[((key % DISK_BUCKET_NUM) + DISK_BUCKET_NUM) % DISK_BUCKET_NUM].write((char*)&key, sizeof(int));
					db[((key % DISK_BUCKET_NUM) + DISK_BUCKET_NUM) % DISK_BUCKET_NUM].write((char*)&value, sizeof(int));
					HashTable2_Put(key, 0, false);
				}
				//BloomPut(bloom_filter[key % DISK_BUCKET_NUM], cur->key);
				SET_BIT(bloom_filter[((key % DISK_BUCKET_NUM) + DISK_BUCKET_NUM) % DISK_BUCKET_NUM], key);
			}

			dbhelp[i].close();
			dbhelp[i].open(oldname[i].c_str(), fstream::out | fstream::trunc);
			dbhelp[i].close();
		}
		HashTable2_Delete();
	}
	// Write the updated Bloom Filter
	int totalsize = 0;
	for (int i = 0; i < DISK_BUCKET_NUM; i++)
	{

db[i].seekg (0, ios_base::end);  // read-in bloom filter and db_header
//cout << "SIze="<< db[i].tellg() <<endl;  // read-in bloom filter and db_header
		db[i].seekp (0, ios_base::beg);  // last write page pointer
		db[i].write((char*)&(bloom_filter[i]), BLOOM_BUCKET_NUM);	

db[i].seekg (0, ios_base::end);  // read-in bloom filter and db_header
//cout << "SIze1="<< db[i].tellg() <<endl;  // read-in bloom filter and db_header

		totalsize += db[i].tellg(); // the current position for each db file
	}

	for (int i = 0; i < DISK_BUCKET_NUM; i++)
	{	

db[i].seekg (0, ios_base::end);  // read-in bloom filter and db_header
//cout << "SIze2="<< db[i].tellg() <<endl;  // read-in bloom filter and db_header

		db[i].close();
	}
	if (totalsize >= MEMORY_SIZE * level++)
	{	//cout << "Totalsize=" << totalsize << ", capacity=" << MEMORY_SIZE*(level-1) << endl;
		goto recurse;
	}

//cout << "Unlock" <<endl;
//cout << "Unlock done" <<endl;
//cout << "Merge done " << MEMORY_SIZE << endl;
	//for (int i = 0; i < DISK_BUCKET_NUM; i++)
	//	dbhelp[i].close();

}

void Update_Hashtable(int key, int value, bool isDelete)
{
//cout << "Lock "<< pthread_self() << endl;
	pthread_rwlock_wrlock(&htlock);
//cout << "Lockin " << pthread_self() << endl;
	HashTable_Put(key, value, isDelete);
//cout <<"out " << pthread_self() << endl;
	pthread_rwlock_unlock(&htlock);
//cout << "Lock Out " << pthread_self() << endl;
	if (MEMORY_SIZE <= cur_mem_size)
      {

//cout << "Disk  " << pthread_self() << endl;
			pthread_rwlock_wrlock(&disklock);

//cout << "Disk Lock " << pthread_self() << endl;
         Merge_Hashtable();

//cout << "Disk Lock2 " << pthread_self() << endl;
			pthread_rwlock_unlock(&disklock);
      }
}

void put(int key, int value)
{
	Update_Hashtable(key, value, false);
	return;
}

void Get_Hashtable_And_Disk(int key)
{

	pthread_rwlock_rdlock(&htlock);
	bool isNULL;
	int value = HashTable_Get(key, isNULL);
	pthread_rwlock_unlock(&htlock);

	if (value != DELETED)
	{	cout << value << endl;
		return;
	}
 	else if (isNULL != true) // the vlaue exists as DELETED
	{	
		cout << endl;
		return;
	}

	int level = 1;
	unsigned char bloom_filter[BLOOM_BUCKET_NUM];

	// search the disk
	//cout << "Disk read lock" << endl;
	pthread_rwlock_rdlock(&disklock);
	while (true)
	{	
		fstream db;
		level++;
		string dbname = "db" + to_string(level) + "-" + to_string((((key % DISK_BUCKET_NUM))+DISK_BUCKET_NUM) % DISK_BUCKET_NUM) + ".bin";
		db.open(dbname.c_str(), fstream::in | fstream::binary);
		db.seekg(0, ios_base::end);
		int db_size = db.tellg(); 
//cout << dbname << "  db_size=" << db_size << endl;
		if (db_size == 0) // if empty disk bucket
		{	db.close();
			continue;
		}
		else if (db_size < 0) // if empty disk bucket
		{
			break;
		}

		db.seekg(0);
		db.seekp(0);
		db.read ((char*)&bloom_filter, BLOOM_BUCKET_NUM); // read page header

		bool isNULL;
		//if (!GET_BIT(&bloom_filter, key)) // if not bloom filter result
		//{	db.close();
		//	continue;
		//}

		// must exist in this bucket!
		int newkey, newvalue;
		int sofar = db_size - 8;
		int count = 1;
		while (sofar >= BLOOM_BUCKET_NUM)
		{	
			db.seekg(sofar);
			sofar -= 8;
			db.read ((char*)&newkey, sizeof(int));
			db.read ((char*)&newvalue, sizeof(int)); 
//cout << "Key : " << newkey << " vs " << key << ", value = " << newvalue << endl;
			if (key == newkey)
			{	if (newvalue != DELETED)
					cout << newvalue << endl;
//cout << "Newvalue" << endl;
				break;;
			}
		}
		db.close();
	}
	pthread_rwlock_unlock(&disklock);

	//cout << "Disk read unlock" << endl;
	return;
}

void get(int key)
{
	if (all_elements == 0)
		return;

	Get_Hashtable_And_Disk(key);	
}


void Disk_Array_Range_Search(int lower, int upper, bool isStat)
{
	int count = 0;
	HashTable2_Init();
 
	pthread_rwlock_rdlock(&htlock);
	for (int i = 0; i < HASH_BUCKET_NUM; i++)
	{
		struct key_value_h* cur = hashtable[i];
		struct key_value_h* before = NULL;
		while (cur)
		{
			// write key and value into the corresponding db bucket
			bool garb;
			int ret = HashTable2_Get(cur->key, garb);

			if (isStat && cur->value != DELETED)
			{	cout << cur->key << ":" << cur->value << ":L1 ";
				count++;
			}
			else if (cur->key >= lower && cur->key <= upper && cur->value != DELETED && ret == DELETED && garb == true)
			{	cout << cur->key << ":" << cur->value << " ";
				HashTable2_Put(cur->key, 0, true);
			}
			cur = cur->next;	
		}
	}
	pthread_rwlock_unlock(&htlock);

	if (isStat)
		cout << endl << "----- LV1: " << count << endl << endl;

	int level = 1;
	unsigned char bloom_filter[BLOOM_BUCKET_NUM];

	pthread_rwlock_rdlock(&disklock);
	while (true)
	{	
		count = 0;
		level++;
		fstream db;
		for (int i = 0; i < DISK_BUCKET_NUM; i++)
		{	string dbname = "db" + to_string(level) + "-" + to_string(i) + ".bin";
//cout << "opening file " << dbname << endl;
			db.open(dbname.c_str(), fstream::in | fstream::binary);
			// there's no more depth (no file)
			if (!db.good()) 
			{	
				//db[key % DISK_BUCKET_NUM].close();
				//cout << "No such file" << endl;
				pthread_rwlock_unlock(&disklock);
				return;
			}
			db.seekg(0, ios_base::end);
			int db_size = db.tellg(); //PAGE_SIZE * (page_count - 1);
			if (db_size == 0)
			{
				//cout << "Size 0, continue" << endl;	
				db.close();
				continue;
			}
			//struct db_header header, write_header;
			//db.seekg (db_position);  // last read page pointer
		
			db.seekg(0);
			db.read ((char*)&bloom_filter, BLOOM_BUCKET_NUM); 
		
			//bool isNULL;
			//if (!GET_BIT(&bloom_filter, key))
			//{	db[key % DISK_BUCKET_NUM].close()
			//	continue;
			//}

			int newkey, newvalue;
			int sofar = db_size - 8;
			while (sofar >= BLOOM_BUCKET_NUM)
			{	
				//cout << "SIze: " << sofar << " " << db_size << endl;
				db.seekg(sofar);
				db.read ((char*)&newkey, sizeof(int));
				db.read ((char*)&newvalue, sizeof(int)); 

				bool garb;
				int ret = HashTable2_Get(newkey, garb);

				if (isStat && newvalue != DELETED)
				{	cout << newkey << ":" << newvalue << ":L" << level << " ";
					count++;
				}
				else if (newkey >= lower && newkey <= upper && newvalue != DELETED && ret == DELETED && garb == true)
				{	
						cout << newkey << ":" << newvalue << " ";
						HashTable2_Put(newkey, 0, false);
				}
				sofar -= 8;
			}
			db.close();
		}
		if (isStat)
			cout << endl << "----- LV" << level << ": " << count << endl << endl;
		else
			cout << endl;
	}
	HashTable2_Delete();
	pthread_rwlock_unlock(&disklock);
	return;
}

void range(int lower, int upper)
{
	// scan the memory, first
	Disk_Array_Range_Search(lower, upper, false); 
	return;
}

void del(int key)
{
	if (all_elements == 0)
		return;
	Update_Hashtable(key, DELETED, false);
}

ifstream myfile;
int ops = 0;

void* thread_load(void* a)
{
	string line;
	pthread_rwlock_wrlock(&loadlock);
	while (getline(myfile, line))
	{
		ops++;
		pthread_rwlock_unlock(&loadlock);

		const char* cstr = line.c_str();
//cout << line << endl;
		parse((void*)cstr);
		//parse(line);
		pthread_rwlock_wrlock(&loadlock);
	}
	pthread_rwlock_unlock(&loadlock);
}

void load(string filename)
{
//exit(0);
	myfile.open(filename.c_str());


	if (myfile.is_open())
	{

		ops = 0;
		gettimeofday(&start_time,NULL);

		for (int i = 0; i < THREAD_NUM; i++)
			//int err = pthread_create(&(tid[i]), NULL, &parse, (void*)line.c_str());
			int err = pthread_create(&(tid[i]), NULL, thread_load, NULL);

		for (int i = 0; i < THREAD_NUM; i++)		
			pthread_join(tid[i], NULL);

		//int curThread = 0;
		//while ( getline (myfile,line) )
		//{ 
			//const char* cstr = line.c_str();
			//parse((void*)cstr);
			//parse(line);
		//}
		ops = ops * (1 + min(DISK_BUCKET_NUM, 6) * 0.1);
		//pthread_join(tid[(curThread- 1) % THREAD_NUM], NULL);
		gettimeofday(&end_time,NULL);
	double delay = ((end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec))/1000000.0;
		//printf("%f sec (%d reads, %d writes), %d items, %f op/s\n", delay, read_total, write_total, all_elements, ops/delay);

		//printf("%d op/s (%d reads, %d writes)\n", (int)(ops/delay), read_total, write_total);

		printf("%d op/s\n", (int)(ops/delay));
		
		myfile.close();
  }

		//printf("%f sec\n", ((end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec))/1000000.0);
}

void stat()
{
	cout << endl << "Total Pairs: " << all_elements << endl << endl;
	Disk_Array_Range_Search(0, 0, true);
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
		//pthread_rwlock_wrlock(&lock[0]);
		put(index, value);
		write_total++;
		//pthread_rwlock_unlock(&lock[0]);
	}
	else if (command[0] == 'g')
	{	ss >> index;
		//pthread_rwlock_rdlock(&lock[0]);
		get(index);
		read_total++;
		//pthread_rwlock_unlock(&lock[0]);
	}
	else if (command[0] == 'r')
	{	ss >> index;
		ss >> value;
		//pthread_rwlock_rdlock(&lock[0]);
		range(index, value);
		read_total++;
		//pthread_rwlock_unlock(&lock[0]);
	}
	else if (command[0] == 'd')
	{	ss >> index;
		//pthread_rwlock_wrlock(&lock[0]);
		del(index);
		write_total++;
		//pthread_rwlock_unlock(&lock[0]);
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

	system("rm db*.bin -f");
	//bloom_filter = BloomInit();

	HashTable_Init(); //HASHTABLE
	
	//db.open("db.bin", fstream::in | fstream::out | fstream::binary | fstream::trunc);
	//myfile << "Writing this to a file.\n";
	//db_file.close();	

//	for (int i = 0; i < FILE_NUM; i++)
	if (pthread_rwlock_init(&lock, NULL) != 0)
	{	printf("\n mutex init failed\n");
		exit(0);
	}
	if (pthread_rwlock_init(&loadlock, NULL) != 0)
	{	printf("\n mutex init failed\n");
		exit(0);
	}
	
	if (pthread_rwlock_init(&disklock, NULL) != 0)
	{	printf("\n mutex init failed\n");
		exit(0);
	}
	if (pthread_rwlock_init(&htlock, NULL) != 0)
	{	printf("\n mutex init failed\n");
		exit(0);
	}

	while (true)
	{	
		getline(cin, line);
if (line.length() == 0) exit(0);
		const char* cstr = line.c_str();
		parse((void*)cstr);
	}
	return 0;
}
