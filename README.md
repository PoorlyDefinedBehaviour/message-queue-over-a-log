## About

A message queue built over a log.

```
curl localhost:8000/receive?batch_size=10 -H "X-Client-ID: 1"
curl localhost:8000/enqueue -H "Content-Type: application/json" -d '[{"payload": "a"}]'
```