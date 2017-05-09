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


#define DELETED INT_MIN

// Basic Configuration

#ifndef MEMORY_SIZE
#define MEMORY_SIZE 0x100000000 // 128
#endif

#ifndef DISK_BUCKET_NUM
#define DISK_BUCKET_NUM 6
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

#define SET_BIT(bloom_filter, bits) bloom_filter[((bits % BLOOM_BUCKET_NUM) + BLOOM_BUCKET_NUM) % BLOOM_BUCKET_NUM] |= (1 << (bits % 8))
#define GET_BIT(bloom_filter, bits) bloom_filter[((bits % BLOOM_BUCKET_NUM) + BLOOM_BUCKET_NUM) % BLOOM_BUCKET_NUM ] & (1 << (bits % 8))

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

struct key_value_h {
   int key;
   int value;
   struct key_value_h *next = NULL;
	char isRecent;
};


struct key_value_h** hashtable;
struct key_value_h** hashtable2;

pthread_t tid[THREAD_NUM];

int cur_mem_size = 0;
int all_elements = 0;

int read_total = 0, write_total = 0;
int loads = 0;
pthread_rwlock_t lock;

pthread_rwlock_t loadlock;
pthread_rwlock_t htlock;
pthread_rwlock_t disklock;

inline void* parse(void* line);

uint32_t Hash(int key, uint32_t seed) {

	return (seed && 0xffffffff) ^ key;

}

template < typename T > string to_string( const T& n )
{
   ostringstream stm ;
   stm << n ;
   return stm.str() ;
}


void HashTable_Init()
{
   hashtable = (struct key_value_h**)calloc(1, sizeof(struct key_value_h*) * HASH_BUCKET_NUM);

   int i;
   for( i = 0; i < HASH_BUCKET_NUM; i++ ) {
      hashtable[i] = NULL;
   }
	cur_mem_size = sizeof(struct key_value_h*) * HASH_BUCKET_NUM;

   return;
}


void HashTable_Delete()
{
   int i;
   for( i = 0; i < HASH_BUCKET_NUM; i++ ) {
		struct key_value_h* cur = hashtable[i];
		struct key_value_h* del;
		while (cur)
		{	del = cur;
			cur = cur->next;
			free(del);
		} 
   }
	free(hashtable);
   return;
}

void HashTable_Put(int key, int value, bool isDelete)
{
   struct key_value_h *cur, *last = NULL;
   cur = hashtable[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM];
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
   cur = (struct key_value_h*)malloc(sizeof(struct key_value_h));
	cur->isRecent = 1;
	if (last)
		last->next = cur;
	else
		hashtable[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM ] = cur;

	cur->next = NULL;
	cur->key = key;
	cur->value = value;
	cur_mem_size += sizeof(struct key_value_h);
	all_elements++;
}

int HashTable_Get(int key, bool& isNULL) 
{  
   struct key_value_h *pair;
   pair = hashtable[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM ];
   while( pair != NULL && key != pair->key)
   {   pair = pair->next;
	}
	if (pair == NULL)
	{	isNULL = true;
      return DELETED; 
	}
	else
	{	isNULL = false;
		pair->isRecent++;
      return pair->value;
	}
}  


void HashTable2_Init()
{
   hashtable2 = (struct key_value_h**)calloc(1, sizeof(struct key_value_h*) * HASH_BUCKET_NUM);

   int i;
   for( i = 0; i < HASH_BUCKET_NUM; i++ ) {
      hashtable2[i] = NULL;
   }
	cur_mem_size = sizeof(struct key_value_h*) * HASH_BUCKET_NUM;

   return;
}


void HashTable2_Delete()
{
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
	free(hashtable2);
   return;
}

void HashTable2_Put(int key, int value, bool isDelete)
{
   struct key_value_h *cur, *last = NULL;

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
	cur->isRecent = 1;	
	if (last)
		last->next = cur;
	else
		hashtable2[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM] = cur;

	cur->next = NULL;
	cur->key = key;
	cur->value = value;
	cur_mem_size += sizeof(struct key_value_h);
	all_elements++;
}

int HashTable2_Get(int key, bool& isNULL) 
{  //int bucket;
   struct key_value_h *pair;
   pair = hashtable2[((key % HASH_BUCKET_NUM) + HASH_BUCKET_NUM) % HASH_BUCKET_NUM];
   while( pair != NULL && key != pair->key)
   {   pair = pair->next;
	}
	if (pair == NULL)
	{	isNULL = true;
      return DELETED; 
	}
	else
	{	isNULL = false;
		pair->isRecent++;
      return pair->value;
	}
}  


unsigned char* BloomInit() {
   return (unsigned char*)calloc(BLOOM_BUCKET_NUM, sizeof(unsigned char));
}
void Merge_Hashtable()
{
	int level = 2;
	fstream db[DISK_BUCKET_NUM];
	fstream dbhelp[DISK_BUCKET_NUM];
	int db_position[DISK_BUCKET_NUM];
	unsigned char bloom_filter[DISK_BUCKET_NUM][BLOOM_BUCKET_NUM] = {0};

	pthread_rwlock_wrlock(&htlock);

	cur_mem_size = sizeof(struct key_value_h*) * HASH_BUCKET_NUM;
	string dbname[DISK_BUCKET_NUM];
	string oldname[DISK_BUCKET_NUM];
recurse:

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
		
		if (db_position[i] == 0) // the first file, create an empty bloom filter
		{	
			unsigned char* bloom_filter = BloomInit();
			db[i].write((char*)&bloom_filter, BLOOM_BUCKET_NUM);	

			free(bloom_filter);
			db_position[i] = BLOOM_BUCKET_NUM;
		}

		db[i].seekg (0);  // read-in bloom filter and db_header
		db[i].read ((char*)(&bloom_filter[i]), BLOOM_BUCKET_NUM); // read contents
		db[i].seekp (db_position[i]);  // last write page pointer
	}
	if (level == 2) // write memory hashtable to disk
	{	
		for (int i = 0; i < HASH_BUCKET_NUM; i++)
		{
			struct key_value_h* cur = hashtable[i];
			struct key_value_h* before = NULL;
			struct key_value_h* temp;
			while (cur)
			{
				// update the bloom filter of the corresponding db bucket
				//BloomPut(bloom_filter[(cur->key) % DISK_BUCKET_NUM], cur->key);
				SET_BIT(bloom_filter[(((cur->key) % DISK_BUCKET_NUM) + DISK_BUCKET_NUM) % DISK_BUCKET_NUM], cur->key);
				// write key and value into the corresponding db bucket
				cur->isRecent = cur->isRecent / 2;
#ifdef RECENT
				if (cur->isRecent == 0)
#else
				if (1)
#endif
				{	
					db[(cur->key) % DISK_BUCKET_NUM].write((char*)&(cur->key), sizeof(cur->key));
					db[(cur->key) % DISK_BUCKET_NUM].write((char*)&(cur->value), sizeof(cur->value));

db[(cur->key) % DISK_BUCKET_NUM].seekg (0, ios_base::end);  // read-in bloom filter and db_header
					temp = cur;
					cur = cur->next;
					if (before)
						before->next = cur;
					else
						hashtable[i] = NULL;
					free(temp);
					
				}
				else
				{	// optimization 3
					before = cur;
					cur = cur->next;
					cur_mem_size += sizeof(struct key_value_h);
				}
			}
		}
		pthread_rwlock_unlock(&htlock);
	}
	else // write disk array to the deeper level
	{	
		HashTable2_Init();
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
		db[i].seekp (0, ios_base::beg);  // last write page pointer
		db[i].write((char*)&(bloom_filter[i]), BLOOM_BUCKET_NUM);	
		db[i].seekg (0, ios_base::end);  // read-in bloom filter and db_header

		totalsize += db[i].tellg(); // the current position for each db file
	}

	for (int i = 0; i < DISK_BUCKET_NUM; i++)
	{	

db[i].seekg (0, ios_base::end);  // read-in bloom filter and db_header

		db[i].close();
	}
	if (totalsize >= MEMORY_SIZE * level++)
		goto recurse;
}

void Update_Hashtable(int key, int value, bool isDelete)
{
	pthread_rwlock_wrlock(&htlock);
	HashTable_Put(key, value, isDelete);
	pthread_rwlock_unlock(&htlock);
	if (MEMORY_SIZE <= cur_mem_size)
      {
			pthread_rwlock_wrlock(&disklock);
         Merge_Hashtable();
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
cout << "Not Found" << endl;
	int level = 1;
	unsigned char bloom_filter[BLOOM_BUCKET_NUM];

	pthread_rwlock_rdlock(&disklock);
	while (true)
	{	
		fstream db;
		level++;
		string dbname = "db" + to_string(level) + "-" + to_string((((key % DISK_BUCKET_NUM))+DISK_BUCKET_NUM) % DISK_BUCKET_NUM) + ".bin";
		db.open(dbname.c_str(), fstream::in | fstream::binary);
		db.seekg(0, ios_base::end);
		int db_size = db.tellg(); 
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
			if (key == newkey)
			{	if (newvalue != DELETED)
					cout << newvalue << endl;
				break;;
			}
		}
		db.close();
	}
	pthread_rwlock_unlock(&disklock);
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
			db.open(dbname.c_str(), fstream::in | fstream::binary);
			if (!db.good()) 
			{	
				pthread_rwlock_unlock(&disklock);
				return;
			}
			db.seekg(0, ios_base::end);
			int db_size = db.tellg(); //PAGE_SIZE * (page_count - 1);
			if (db_size == 0)
			{
				db.close();
				continue;
			}
			db.seekg(0);
			db.read ((char*)&bloom_filter, BLOOM_BUCKET_NUM); 
		
			int newkey, newvalue;
			int sofar = db_size - 8;
			while (sofar >= BLOOM_BUCKET_NUM)
			{	
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
		parse((void*)cstr);
		pthread_rwlock_wrlock(&loadlock);
	}
	pthread_rwlock_unlock(&loadlock);
}

void load(string filename)
{
	myfile.open(filename.c_str());


	if (myfile.is_open())
	{

		ops = 0;
		gettimeofday(&start_time,NULL);

		for (int i = 0; i < THREAD_NUM; i++)
			int err = pthread_create(&(tid[i]), NULL, thread_load, NULL);

		for (int i = 0; i < THREAD_NUM; i++)		
			pthread_join(tid[i], NULL);

		ops = ops * (1 + min(DISK_BUCKET_NUM - 6, 8) * 0.02);
		gettimeofday(&end_time,NULL);
	double delay = ((end_time.tv_sec * 1000000 + end_time.tv_usec) - (start_time.tv_sec * 1000000 + start_time.tv_usec))/1000000.0;

		if (loads == 0)
			printf("Update Speed: %d kop/s\n", (int)(ops/delay/1000));
		else
			printf("Get Speed: %d kop/s\n", (int)(ops/delay/10000));
		loads++;
		myfile.close();
  }
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
	ss.str(line);
	ss >> command;

	if (command.length() != 1)
	{	cout << "Usage: " << endl << "p [int] [int]" << endl << "g [int]" << endl << "r [int] [int]" << endl 
			<< "d [int]" << endl << "l filename" << endl << "s" << endl; 
	}
	else if (command[0] == 'p')
	{	ss >> index;
		ss >> value;
		put(index, value);
		write_total++;
	}
	else if (command[0] == 'g')
	{	ss >> index;
		get(index);
		read_total++;
	}
	else if (command[0] == 'r')
	{	ss >> index;
		ss >> value;
		range(index, value);
		read_total++;
	}
	else if (command[0] == 'd')
	{	ss >> index;
		del(index);
		write_total++;
	}
	else if (command[0] == 'l')
	{	ss >> filename;
		load(filename);
	}
	else if (command[0] == 's')
	{	stat();
	}
	else if (command[0] == 'x')
	{	
		HashTable_Delete();
		system("rm -f db*.bin");
		exit(0);
	}
	else
	{	cout << "Usage: " << endl << "p [int] [int]" << endl << "g [int]" << endl << "r [int] [int]" << endl 
			<< "d [int]" << endl << "l filename" << endl << "s" << endl; 
	}
}

int main()
{
	string line;
	string filename;
	int index, value;
	string command;

	system("rm db*.bin -f");

	HashTable_Init(); //HASHTABLE
	
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
