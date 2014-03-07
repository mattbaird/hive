 #!/bin/bash          
thrift --gen go -out . fb303.thrift

cd fb303
go install
cd ..
thrift --gen go -out . hive_metastore.thrift
cd hive_metastore
go install
cd ..
thrift --gen go -out . queryplan.thrift
cd queryplan
go install
cd ..
thrift --gen go -out . hive_service.thrift
thrift --gen go -out . serde.thrift
cd serde
go install
cd ..


#mv hive_service/ttypes.go .
#mv hive_service/ThriftHive.go .
#go install