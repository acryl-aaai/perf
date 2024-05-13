echo 
echo Start Building perftest
## remove shared object
sleep 2
make clean
./autogen.sh
sleep 2
./configure --prefix=/usr/ --libdir=/usr/lib/ --sysconfdir=/etc/
sleep 2
make -j 8
sleep 2
sudo make install

