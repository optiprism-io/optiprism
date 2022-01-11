# exprtree


env FNP_ - future name prefix

```
/common/ - global types
	kv.rs - common storage traints
	event.rs - + protobuf?
/storage/
	grpc.rs - grpc service
	event.rs - rocksdb, common kv implementation
/platform/
	/http/
		event.rs - http endpoints
	/event.rs - validation, etc
/cmd/
	/storage/
		main.rs - http server, etc
	/app/
		main.rs - http server, etc
```
