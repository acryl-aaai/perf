ROOT=$PWD

cd $ROOT/perf_src/SOURCES_origin/libibverbs-41mlnx1
./autogen.sh
CFLAGS=-Wno-error ./configure --prefix=/usr/ --libdir=/usr/lib/ --sysconfdir=/etc/
make clean
make && sudo make install
cd $ROOT
sleep 1

cd $ROOT/perf_src/SOURCES_origin/libmlx5-41mlnx1
./autogen.sh
CFLAGS=-Wno-error ./configure --prefix=/usr/ --libdir=/usr/lib/ --sysconfdir=/etc/
make clean
make && sudo make install
cd $ROOT
sleep 1

cd $ROOT/perf_src/SOURCES_origin/perftest-4.5.0.mlnxlibs
./autogen.sh
CFLAGS=-Wno-error ./configure --prefix=/usr/ --libdir=/usr/lib/ --sysconfdir=/etc/
make clean
make && sudo make install
cd $ROOT
