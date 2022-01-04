# exprtree


env FNP_ - future name prefix

```
/common/ - global types
/storage/
	event/.rs - rocksdb, etc
	...
/platform/
	/http/
		event.rs - endpoints
	/event/
		mod.rs - structs
		provider.rs - validation, etc
		...
/cmd/
	/app/
		main.rs - http server, etc
```
