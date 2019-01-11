go-ethereum单点运行并开启profiling

1. 安装go环境
2. 安装bls依赖
   
   ```
     cd $GOPATH/src
     mkdir -p github.com/herumi
     cd github.com/herumi
     git clone https://github.com/herumi/obsolete-bls-all-in-one
     cd obsolete-bls-all-in-one && make all
     cd bls && make lib
     cd ../../../bitbucket.org/lamuguo/random-beacon/
     ./run.sh

   ```
3. build geth
   ```
     cd ${projectDir}
     make all
   ```

4. init genesis

   ```
     ${projectDir}/build/bin/geth --dataDir ${dataDir} init ${projectDir}/horae/scripts/genesis.json
   ```
   
5. import boot replica

   ```
     echo yourpassword > ${passwordfilepath}
     #echo '123' > ${projectDir}/horae/password
     $projectDir}/build/bin/geth --datadir ${dataDir} account import ${projectDir}"/horae/scripts/account.txt" --password ${passwordfilepath}
   ```

6. run geth 

   ```
     ${projectDir}/build/bin/geth --datadir ${dataDir} --ebpassword ${yourpasswordfilepath} --identity "private etherum" --ipaddress=127.0.0.1 --rpc --rpcport "8545" --rpccorsdomain "*" --port "30303" --rpcaddr="0.0.0.0" --rpcapi "db,eth,net,web3,personal" --networkid "8765" --etherbase=0x4adbad663ac29392f0cfd27f35f4a7d7b585a6da --mine  --pprof
   ```
   
_或者可以直接运行${projectDir}/horae/scripts/run.sh_