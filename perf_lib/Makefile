all: librperf.cpp
	g++ -c -g -O3 -fPIC -std=c++14 librperf.cpp
	g++ -g -shared -o librperf.so librperf.o

install:
	cp librperf.so /usr/lib/librperf.so

clean:
	rm -f /usr/lib/librperf.so
	rm -f *.so *.o

