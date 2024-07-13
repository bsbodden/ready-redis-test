from ready_redis import ReadyRedis
from redis import Redis
from redisvl.index import SearchIndex
from redisvl.query import VectorQuery
import numpy as np
import os

with ReadyRedis.get(port=6380) as redis:
    print(redis.ping())

    redis.set("foo", "bar")
    print(redis.get("foo"))

    # You can use any Redis method directly
    redis.lpush("mylist", 1, 2, 3)
    print(redis.lrange("mylist", 0, -1))

    schema = {
        "index": {
            "name": "user_simple",
            "prefix": "user_simple_docs",
        },
        "fields": [
            {"name": "user", "type": "tag"},
            {"name": "credit_score", "type": "tag"},
            {"name": "job", "type": "text"},
            {"name": "age", "type": "numeric"},
            {
                "name": "user_embedding",
                "type": "vector",
                "attrs": {
                    "dims": 3,
                    "distance_metric": "cosine",
                    "algorithm": "flat",
                    "datatype": "float32"
                }
            }
        ]
    }

    data = [
        {
            'user': 'john',
            'age': 1,
            'job': 'engineer',
            'credit_score': 'high',
            'user_embedding': np.array([0.1, 0.1, 0.5], dtype=np.float32).tobytes()
        },
        {
            'user': 'mary',
            'age': 2,
            'job': 'doctor',
            'credit_score': 'low',
            'user_embedding': np.array([0.1, 0.1, 0.5], dtype=np.float32).tobytes()
        },
        {
            'user': 'joe',
            'age': 3,
            'job': 'dentist',
            'credit_score': 'medium',
            'user_embedding': np.array([0.9, 0.9, 0.1], dtype=np.float32).tobytes()
        }
    ]

    # the Redis client in ReadyRedis seems to be ok until you try to query (it succesfully creates the index and loads the data)
    index = SearchIndex.from_dict(schema, redis_client = Redis.from_url("redis://localhost:6380")) # this works
    #index = SearchIndex.from_dict(schema, redis_client = redis.client()) # this doesn't - don't know why
    print(f"type {type(redis.client())}")
    #index.set_client(redis.client()) # this doesn't - don't know why
    #index.set_client(Redis.from_url("redis://localhost:6380")) # this works

    index.create(overwrite=True)

    keys = index.load(data)

    print(keys)

    # Add more data
    new_data = [{
        'user': 'tyler',
        'age': 9,
        'job': 'engineer',
        'credit_score': 'high',
        'user_embedding': np.array([0.1, 0.3, 0.5], dtype=np.float32).tobytes()
    }]
    keys = index.load(new_data)

    print(keys)

    os.system("rvl index listall --url=redis://localhost:6380")

    os.system("rvl index info -i user_simple --url=redis://localhost:6380")

    query = VectorQuery(
        vector=[0.1, 0.1, 0.5],
        vector_field_name="user_embedding",
        return_fields=["user", "age", "job", "credit_score", "vector_distance"],
        num_results=3
    )

    results = index.query(query)
    print(results)