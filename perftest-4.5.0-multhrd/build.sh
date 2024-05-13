echo Start Building perftest
sleep 2
make clean
./autogen.sh
./configure --prefix=/usr/ --libdir=/usr/lib/ --sysconfdir=/etc/
make -j 8
