
# MEMORY_SIZE = 0xffffffff
# THREAD_NUM = 64
# DISK_BUCKET_NUM = 3
# BLOOM_BUCKET_NUM = 1024
# HASH_BUCKET_NUM = 5000000

# Test Scripts: buffer, array, disk, hash, cache, thread

all: test array buffer thread disk hash cache
#	g++ -o b db.cpp -pthread
	g++ -o a db-ht.cpp -pthread

test:
	./generator --puts=10000 --gets=10000 --gaussian-ranges > text.txt
	cat text.txt | grep p > put.txt
	cat text.txt | grep g > get.txt	
	./generator --puts=4000000 --gets=4000000 --gaussian-ranges > text2.txt
	cat text2.txt | grep p > put2.txt
	cat text2.txt | grep g > get2.txt 
	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0 --gaussian-ranges > text3.txt
	cat text3.txt | grep p > put0.0.txt
	cat text3.txt | grep g > get0.0.txt 
#	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.1 --gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put0.1.txt
#	cat text3.txt | grep g > get0.1.txt 
	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.2 --gaussian-ranges > text3.txt
	cat text3.txt | grep p > put0.2.txt
	cat text3.txt | grep g > get0.2.txt 
#	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.3 --gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put0.3.txt
#	cat text3.txt | grep g > get0.3.txt 
	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.4 --gaussian-ranges > text3.txt
	cat text3.txt | grep p > put0.4.txt
	cat text3.txt | grep g > get0.4.txt
#	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.5 --gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put0.5.txt
#	cat text3.txt | grep g > get0.5.txt
	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.6 --gaussian-ranges > text3.txt
	cat text3.txt | grep p > put0.6.txt
	cat text3.txt | grep g > get0.6.txt
#	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.7 --gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put0.7.txt
#	cat text3.txt | grep g > get0.7.txt
	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.8 --gaussian-ranges > text3.txt
	cat text3.txt | grep p > put0.8.txt
	cat text3.txt | grep g > get0.8.txt 
#	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 0.9 --gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put0.9.txt
#	cat text3.txt | grep g > get0.9.txt 
	./generator --puts=4000000 --gets=4000000 --gets-misses-ratio 1.0 --gaussian-ranges > text3.txt
	cat text3.txt | grep p > put1.0.txt
	cat text3.txt | grep g > get1.0.txt 
	./generator --puts=4000000 --gets=4000000 --gets-skewness 0 --gets-misses-ratio 0 --gaussian-ranges > text3.txt
	cat text3.txt | grep p > put2_0.0.txt
	cat text3.txt | grep g > get2_0.0.txt 
#	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.1  --gets-misses-ratio 0--gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put2_0.1.txt
#	cat text3.txt | grep g > get2_0.1.txt 
	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.2 --gets-misses-ratio 0--gaussian-ranges > text3.txt
	cat text3.txt | grep p > put2_0.2.txt
	cat text3.txt | grep g > get2_0.2.txt 
#	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.3 --gets-misses-ratio 0--gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put2_0.3.txt
#	cat text3.txt | grep g > get2_0.3.txt 
	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.4 --gets-misses-ratio 0--gaussian-ranges > text3.txt
	cat text3.txt | grep p > put2_0.4.txt
	cat text3.txt | grep g > get2_0.4.txt
#	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.5 --gets-misses-ratio 0--gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put2_0.5.txt
#	cat text3.txt | grep g > get2_0.5.txt
	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.6 --gets-misses-ratio 0--gaussian-ranges > text3.txt
	cat text3.txt | grep p > put2_0.6.txt
	cat text3.txt | grep g > get2_0.6.txt
#	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.7 --gets-misses-ratio 0--gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put2_0.7.txt
#	cat text3.txt | grep g > get2_0.7.txt
	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.8 --gets-misses-ratio 0--gaussian-ranges > text3.txt
	cat text3.txt | grep p > put2_0.8.txt
	cat text3.txt | grep g > get2_0.8.txt 
#	./generator --puts=4000000 --gets=4000000 --gets-skewness 0.9 --gets-misses-ratio 0--gaussian-ranges > text3.txt
#	cat text3.txt | grep p > put2_0.9.txt
#	cat text3.txt | grep g > get2_0.9.txt 
	./generator --puts=4000000 --gets=4000000 --gets-skewness 1.0 --gets-misses-ratio 0--gaussian-ranges > text3.txt
	cat text3.txt | grep p > put2_1.0.txt
	cat text3.txt | grep g > get2_1.0.txt 
array: db.cpp
	g++ -DMEMORY_SIZE=0x1000 -DRECENT -o array_4kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x4000 -DRECENT -o array_16kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x10000 -DRECENT -o array_64kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x40000 -DRECENT -o array_256kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x100000 -DRECENT -o array_1mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x400000 -DRECENT -o array_4mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x1000000 -DRECENT -o array_16mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x4000000 -DRECENT -o array_64mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x10000000 -DRECENT -o array_128mb db-ht.cpp -pthread

buffer: db-ht.cpp
	g++ -DMEMORY_SIZE=0x1000 -DRECENT -o buffer_4kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x4000 -DRECENT -o buffer_16kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x10000 -DRECENT -o buffer_64kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x40000 -DRECENT -o buffer_256kb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x100000 -DRECENT -o buffer_1mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x400000 -DRECENT -o buffer_4mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x1000000 -DRECENT -o buffer_16mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x4000000 -DRECENT -o buffer_64mb db-ht.cpp -pthread
	g++ -DMEMORY_SIZE=0x10000000 -DRECENT -o buffer_128mb db-ht.cpp -pthread

thread: db-ht.cpp
	g++ -DTHREAD_NUM=1 -DRECENT -o thread_1 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=2 -DRECENT -o thread_2 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=4 -DRECENT -o thread_4 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=8 -DRECENT -o thread_8 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=16 -DRECENT -o thread_16 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=32 -DRECENT -o thread_32 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=64 -DRECENT -o thread_64 db-ht.cpp -pthread

disk: db-ht.cpp
	g++ -DDISK_BUCKET_NUM=1 -DRECENT -o disk_1 db-ht.cpp -pthread
	g++ -DDISK_BUCKET_NUM=2 -DRECENT -o disk_2 db-ht.cpp -pthread
	g++ -DDISK_BUCKET_NUM=3 -DRECENT -o disk_3 db-ht.cpp -pthread
	g++ -DDISK_BUCKET_NUM=4 -DRECENT -o disk_4 db-ht.cpp -pthread
	g++ -DDISK_BUCKET_NUM=5 -DRECENT -o disk_5 db-ht.cpp -pthread
	g++ -DDISK_BUCKET_NUM=6 -DRECENT -o disk_6 db-ht.cpp -pthread
	g++ -DDISK_BUCKET_NUM=7 -DRECENT -o disk_7 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=64 -DDISK_BUCKET_NUM=8 -DRECENT -o disk_8 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=64 -DDISK_BUCKET_NUM=9 -DRECENT -o disk_9 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=64 -DDISK_BUCKET_NUM=10 -DRECENT -o disk_10 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=64 -DDISK_BUCKET_NUM=11 -DRECENT -o disk_11 db-ht.cpp -pthread
	g++ -DTHREAD_NUM=64 -DDISK_BUCKET_NUM=12 -DRECENT -o disk_12 db-ht.cpp -pthread

hash: db-ht.cpp
	g++ -DHASH_SHIFT=5 -DRECENT -o hash_5 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=6 -DRECENT -o hash_6 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=7 -DRECENT -o hash_7 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=8 -DRECENT -o hash_8 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=9 -DRECENT -o hash_9 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=10 -DRECENT -o hash_10 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=11 -DRECENT -o hash_11 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=12 -DRECENT -o hash_12 db-ht.cpp -pthread
	g++ -DHASH_SHIFT=13 -DRECENT -o hash_13 db-ht.cpp -pthread
cache: db-ht.cpp
	g++ -DTHREAD_NUM=64 -DRECENT -o cache_yes db-ht.cpp -pthread
	g++ -DTHREAD_NUM=64 -o cache_no db-ht.cpp -pthread

.PHONY:
clean:
	rm -f buffer_* thread_* hash_* disk_* cache_* array_*

