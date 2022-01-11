# exprtree


env FNP_ - future name prefix

```
/common/ - global types
	kv.rs - common storage traints
	event.rs - + protobuf?
/storage/
	grpc.rs - grpc service
	event.rs - rocksdb, common kv implementation
/metadata/ - internal api + business entities storage
	/api/
		event.rs - http endpoints
	/event.rs - validation, etc
/platform/ - public api + business logic
	/api/
		event.rs - http endpoints
	/event.rs - validation, etc
/cmd/
	/storage/
		main.rs - http server, etc
	/metadata/
		main.rs - http server, etc
	/app/
		main.rs - http server, etc
```


```
/common/ - глобальные модули, юзающиеся в нескольких пакетах
	event.rs - + protobuf? - а зачем он, кстати?
/storage/ - хранилище MergeTree. Хранит данные в колоночном формате
	...
/query/ - движок запросов к /storage
	...
/metadata/ - kv-хранилище
	mod.rs - Интерфейс. put, get, delete, list. тут пока не точно
	grpc.rs - grpc-интерфейс
	event.rs - реализация круда евентов
/platform/ - public api + business logic
	/api/
		/http/
			event.rs - эндпоинты
	event.rs - сущность, бизнес логика, валидация и тп
/cmd/
	main.rs - запускалка ноды. Запускалка одна и конфиг один общий для простоты. Просто какие-то компоненты можно не запускать опционально
```
