---
navigation_title: Examples
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/_retrievers_examples.html
applies_to:
  stack:
  serverless:
products:
  - id: elasticsearch
---

# Retrievers examples [retrievers-examples]

Learn how to combine different retrievers in these hands-on examples.


## Add example data [retrievers-examples-setup]

To begin with, let's create the `retrievers_example` index, and add some documents to it.
We will set `number_of_shards=1` for our examples to ensure consistent and reproducible ordering.

```console
PUT retrievers_example
{
    "settings": {
        "number_of_shards": 1
    },
   "mappings": {
       "properties": {
           "vector": {
               "type": "dense_vector",
               "dims": 3,
               "similarity": "l2_norm",
               "index": true,
               "index_options": {
                    "type": "flat"
               }
           },
           "text": {
               "type": "text",
               "copy_to": "text_semantic"
           },
           "text_semantic": {
               "type": "semantic_text"
           },
           "year": {
               "type": "integer"
           },
           "topic": {
               "type": "keyword"
           },
           "timestamp": {
               "type": "date"
           }
       }
   }
}

POST /retrievers_example/_doc/1
{
 "vector": [0.23, 0.67, 0.89],
 "text": "Large language models are revolutionizing information retrieval by boosting search precision, deepening contextual understanding, and reshaping user experiences in data-rich environments.",
 "year": 2024,
 "topic": ["llm", "ai", "information_retrieval"],
 "timestamp": "2021-01-01T12:10:30"
}

POST /retrievers_example/_doc/2
{
 "vector": [0.12, 0.56, 0.78],
 "text": "Artificial intelligence is transforming medicine, from advancing diagnostics and tailoring treatment plans to empowering predictive patient care for improved health outcomes.",
 "year": 2023,
 "topic": ["ai", "medicine"],
 "timestamp": "2022-01-01T12:10:30"
}

POST /retrievers_example/_doc/3
{
 "vector": [0.45, 0.32, 0.91],
  "text": "AI is redefining security by enabling advanced threat detection, proactive risk analysis, and dynamic defenses against increasingly sophisticated cyber threats.",
 "year": 2024,
 "topic": ["ai", "security"],
 "timestamp": "2023-01-01T12:10:30"
}

POST /retrievers_example/_doc/4
{
 "vector": [0.34, 0.21, 0.98],
 "text": "Elastic introduces Elastic AI Assistant, the open, generative AI sidekick powered by ESRE to democratize cybersecurity and enable users of every skill level.",
 "year": 2023,
 "topic": ["ai", "elastic", "assistant"],
 "timestamp": "2024-01-01T12:10:30"
}

POST /retrievers_example/_doc/5
{
 "vector": [0.11, 0.65, 0.47],
 "text": "Learn how to spin up a deployment on Elastic Cloud and use Elastic Observability to gain deeper insight into the behavior of your applications and systems.",
 "year": 2024,
 "topic": ["documentation", "observability", "elastic"],
 "timestamp": "2025-01-01T12:10:30"
}

POST /retrievers_example/_refresh
```

Now that we have our documents in place, let’s try to run some queries using retrievers.


## Example: Combining query and kNN with RRF [retrievers-examples-combining-standard-knn-retrievers-with-rrf]

First, let’s examine how to combine two different types of queries: a `kNN` query and a `query_string` query.
While these queries may produce scores in different ranges, we can use Reciprocal Rank Fusion (`rrf`) to combine the results and generate a merged final result list.

To implement this in the retriever framework, we start with the top-level element: our `rrf` retriever.
This retriever operates on top of two other retrievers: a `knn` retriever and a `standard` retriever. Our query structure would look like this:

```console
GET /retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "retrievers": [
                {
                    "standard": {
                        "query": {
                            "query_string": {
                                "query": "(information retrieval) OR (artificial intelligence)",
                                "default_field": "text"
                            }
                        }
                    }
                },
                {
                    "knn": {
                        "field": "vector",
                        "query_vector": [
                            0.23,
                            0.67,
                            0.89
                        ],
                        "k": 3,
                        "num_candidates": 5
                    }
                }
            ],
            "rank_window_size": 10,
            "rank_constant": 1
        }
    },
    "_source": false
}
```

This returns the following response based on the final rrf score for each result.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 0.8333334,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.8333334
            },
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 0.8333334
            },
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 0.25
            }
        ]
    }
}
```

::::



## Example: Hybrid search with linear retriever [retrievers-examples-linear-retriever]

A different, and more intuitive, way to provide hybrid search, is to linearly combine the top documents of different retrievers using a weighted sum of the original scores.
Since, as above, the scores could lie in different ranges, we can also specify a `normalizer` that would ensure that all scores for the top ranked documents of a retriever lie in a specific range.

To implement this, we define a `linear` retriever, and along with a set of retrievers that will generate the heterogeneous results sets that we will combine.
We will solve a problem similar to the above, by merging the results of a `standard` and a `knn` retriever.
As the `standard` retriever’s scores are based on BM25 and are not strictly bounded, we will also define a `minmax` normalizer to ensure that the scores lie in the [0, 1] range.
We will apply the same normalizer to `knn` as well to ensure that we capture the importance of each document within the result set.

So, let’s now specify the `linear` retriever whose final score is computed as follows:

```text
score = weight(standard) * score(standard) + weight(knn) * score(knn)
score = 2 * score(standard) + 1.5 * score(knn)
```

```console
GET /retrievers_example/_search
{
    "retriever": {
        "linear": {
            "retrievers": [
                {
                    "retriever": {
                        "standard": {
                            "query": {
                                "query_string": {
                                    "query": "(information retrieval) OR (artificial intelligence)",
                                    "default_field": "text"
                                }
                            }
                        }
                    },
                    "weight": 2,
                    "normalizer": "minmax"
                },
                {
                    "retriever": {
                        "knn": {
                            "field": "vector",
                            "query_vector": [
                                0.23,
                                0.67,
                                0.89
                            ],
                            "k": 3,
                            "num_candidates": 5
                        }
                    },
                    "weight": 1.5,
                    "normalizer": "minmax"
                }
            ],
            "rank_window_size": 10
        }
    },
    "_source": false
}
```

This returns the following response based on the normalized weighted score for each result.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 3.5,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 3.5
            },
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 2.3
            },
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 0.1
            }
        ]
    }
}
```

::::


By normalizing scores and leveraging `function_score` queries, we can also implement more complex ranking strategies, such as sorting results based on their timestamps, assign the timestamp as a score, and then normalizing this score to [0, 1].
Then, we can easily combine the above with a `knn` retriever as follows:

```console
GET /retrievers_example/_search
{
    "retriever": {
        "linear": {
            "retrievers": [
                {
                    "retriever": {
                        "standard": {
                            "query": {
                                "function_score": {
                                    "query": {
                                        "term": {
                                            "topic": "ai"
                                        }
                                    },
                                    "functions": [
                                        {
                                            "script_score": {
                                                "script": {
                                                    "source": "doc['timestamp'].value.millis"
                                                }
                                            }
                                        }
                                    ],
                                    "boost_mode": "replace"
                                }
                            },
                            "sort": {
                                "timestamp": {
                                    "order": "asc"
                                }
                            }
                        }
                    },
                    "weight": 2,
                    "normalizer": "minmax"
                },
                {
                    "retriever": {
                        "knn": {
                            "field": "vector",
                            "query_vector": [
                                0.23,
                                0.67,
                                0.89
                            ],
                            "k": 3,
                            "num_candidates": 5
                        }
                    },
                    "weight": 1.5
                }
            ],
            "rank_window_size": 10
        }
    },
    "_source": false
}
```

Which would return the following results:

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 4,
            "relation": "eq"
        },
        "max_score": 3.5,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 3.5
            },
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 2.0
            },
            {
                "_index": "retrievers_example",
                "_id": "4",
                "_score": 1.1
            },
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.1
            }
        ]
    }
}
```

::::


## Example: RRF with the multi-field query format [retrievers-examples-rrf-multi-field-query-format]
```yaml {applies_to}
stack: ga 9.1
```

There's an even simpler way to execute a hybrid search though: We can use the [multi-field query format](/reference/elasticsearch/rest-apis/retrievers.md#multi-field-query-format), which allows us to query multiple fields without explicitly specifying inner retrievers.

One of the major challenges with hybrid search is normalizing the scores across matches on all field types.
Scores from [`text`](/reference/elasticsearch/mapping-reference/text.md) and [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) fields don't always fall in the same range, so we need to normalize the ranks across matches on these fields to generate a result set.
For example, BM25 scores from `text` fields are unbounded, while vector similarity scores from `text_embedding` models are bounded between [0, 1].
The multi-field query format [handles this normalization for us automatically](/reference/elasticsearch/rest-apis/retrievers.md#multi-field-field-grouping).

The following example uses the multi-field query format to query every field specified in the `index.query.default_field` index setting, which is set to `*` by default.
This default value will cause the retriever to query every field that either:

- Supports term queries, such as `keyword` and `text` fields
- Is a `semantic_text` field

In this example, that would translate to the `text`, `text_semantic`, `year`, `topic`, and `timestamp` fields.

```console
GET /retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "query": "artificial intelligence"
        }
    }
}     
```

This returns the following response based on the final rrf score for each result.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 0.8333334,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.8333334
            },
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 0.8333334
            },
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 0.25
            }
        ]
    }
}
```

::::

We can also use the `fields` parameter to explicitly specify the fields to query.
The following example uses the multi-field query format to query the `text` and `text_semantic` fields.

```console
GET /retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "query": "artificial intelligence",
            "fields": ["text", "text_semantic"]
        }
    }
}     
```

::::{note}
The `fields` parameter also accepts [wildcard field patterns](/reference/elasticsearch/rest-apis/retrievers.md#multi-field-wildcard-field-patterns).
::::

This returns the following response based on the final rrf score for each result.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 0.8333334,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.8333334
            },
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 0.8333334
            },
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 0.25
            }
        ]
    }
}
```

::::


## Example: Linear retriever with the multi-field query format [retrievers-examples-linear-multi-field-query-format]
```yaml {applies_to}
stack: ga 9.1
```

We can also use the [multi-field query format](/reference/elasticsearch/rest-apis/retrievers.md#multi-field-query-format) with the `linear` retriever.
It works much the same way as [on the `rrf` retriever](#retrievers-examples-rrf-multi-field-query-format), with a couple key differences:

- We can use `^` notation to specify a [per-field boost](/reference/elasticsearch/rest-apis/retrievers.md#multi-field-field-boosting)
- We must set the `normalizer` parameter to specify the normalization method used to combine [field group scores](/reference/elasticsearch/rest-apis/retrievers.md#multi-field-field-grouping)

The following example uses the `linear` retriever to query the `text`, `text_semantic`, and `topic` fields, with a boost of 2 on the `topic` field:

```console
GET /retrievers_example/_search
{
    "retriever": {
        "linear": {
            "query": "artificial intelligence",
            "fields": ["text", "text_semantic", "topic^2"],
            "normalizer": "minmax"
        }
    }
}     
```

This returns the following response based on the normalized score for each result:

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 2.0,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 2.0
            },
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 1.2
            },
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 0.1
            }
        ]
    }
}
```

::::

## Example: Grouping results by year with `collapse` [retrievers-examples-collapsing-retriever-results]

In our result set, we have many documents with the same `year` value. We can clean this up using the `collapse` parameter with our retriever. This, as with the standard [collapse](/reference/elasticsearch/rest-apis/collapse-search-results.md) feature,
enables grouping results by any field and returns only the highest-scoring document from each group. In this example we’ll collapse our results based on the `year` field.

```console
GET /retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "retrievers": [
                {
                    "standard": {
                        "query": {
                            "query_string": {
                                "query": "(information retrieval) OR (artificial intelligence)",
                                "default_field": "text"
                            }
                        }
                    }
                },
                {
                    "knn": {
                        "field": "vector",
                        "query_vector": [
                            0.23,
                            0.67,
                            0.89
                        ],
                        "k": 3,
                        "num_candidates": 5
                    }
                }
            ],
            "rank_window_size": 10,
            "rank_constant": 1
        }
    },
    "collapse": {
        "field": "year",
        "inner_hits": {
            "name": "topic related documents",
            "_source": [
                "year"
            ]
        }
    },
    "_source": false
}
```

This returns the following response with collapsed results.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 0.8333334,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.8333334,
                "fields": {
                    "year": [
                        2024
                    ]
                },
                "inner_hits": {
                    "topic related documents": {
                        "hits": {
                            "total": {
                                "value": 2,
                                "relation": "eq"
                            },
                            "max_score": 0.8333334,
                            "hits": [
                                {
                                    "_index": "retrievers_example",
                                    "_id": "1",
                                    "_score": 0.8333334,
                                    "_source": {
                                        "year": 2024
                                    }
                                },
                                {
                                    "_index": "retrievers_example",
                                    "_id": "3",
                                    "_score": 0.25,
                                    "_source": {
                                        "year": 2024
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 0.8333334,
                "fields": {
                    "year": [
                        2023
                    ]
                },
                "inner_hits": {
                    "topic related documents": {
                        "hits": {
                            "total": {
                                "value": 1,
                                "relation": "eq"
                            },
                            "max_score": 0.8333334,
                            "hits": [
                                {
                                    "_index": "retrievers_example",
                                    "_id": "2",
                                    "_score": 0.8333334,
                                    "_source": {
                                        "year": 2023
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        ]
    }
}
```

::::



## Example: Highlighting results based on nested sub-retrievers [retrievers-examples-highlighting-retriever-results]

Highlighting is now also available for nested sub-retrievers matches. For example, consider the same `rrf` retriever as above, with a `knn` and `standard` retriever as its sub-retrievers. We can specify a `highlight` section, as defined in the [highlighting](/reference/elasticsearch/rest-apis/highlighting.md) documentation, and compute highlights for the top results.

```console
GET /retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "retrievers": [
                {
                    "standard": {
                        "query": {
                            "query_string": {
                                "query": "(information retrieval) OR (artificial intelligence)",
                                "default_field": "text"
                            }
                        }
                    }
                },
                {
                    "knn": {
                        "field": "vector",
                        "query_vector": [
                            0.23,
                            0.67,
                            0.89
                        ],
                        "k": 3,
                        "num_candidates": 5
                    }
                }
            ],
            "rank_window_size": 10,
            "rank_constant": 1
        }
    },
    "highlight": {
        "fields": {
            "text": {
                "fragment_size": 150,
                "number_of_fragments": 3
            }
        }
    },
    "_source": false
}
```

This would highlight the `text` field, based on the matches produced by the `standard` retriever. The highlighted snippets would then be included in the response as usual, i.e. under each search hit.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 0.8333334,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.8333334,
                "highlight": {
                    "text": [
                        "Large language models are revolutionizing <em>information</em> <em>retrieval</em> by boosting search precision, deepening contextual understanding, and reshaping user experiences"
                    ]
                }
            },
            {
                "_index": "retrievers_example",
                "_id": "2",
                "_score": 0.8333334,
                "highlight": {
                    "text": [
                        "<em>Artificial</em> <em>intelligence</em> is transforming medicine, from advancing diagnostics and tailoring treatment plans to empowering predictive patient care for improved"
                    ]
                }
            },
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 0.25
            }
        ]
    }
}
```

::::



## Example: Computing inner hits from nested sub-retrievers [retrievers-examples-inner-hits-retriever-results]

We can also define `inner_hits` to be computed on any of the sub-retrievers, and propagate those computations to the top level compound retriever. For example, let’s create a new index with a `knn` field, nested under the `nested_field` field, and index a couple of documents.

```console
PUT retrievers_example_nested
{
    "settings": {
         "number_of_shards": 1
     },
    "mappings": {
        "properties": {
            "nested_field": {
                "type": "nested",
                "properties": {
                    "paragraph_id": {
                        "type": "keyword"
                    },
                    "nested_vector": {
                        "type": "dense_vector",
                        "dims": 3,
                        "similarity": "l2_norm",
                        "index": true,
                        "index_options": {
                            "type": "flat"
                        }
                    }
                }
            },
            "topic": {
                "type": "keyword"
            }
        }
    }
}

POST /retrievers_example_nested/_doc/1
{
    "nested_field": [
        {
            "paragraph_id": "1a",
            "nested_vector": [
                -1.12,
                -0.59,
                0.78
            ]
        },
        {
            "paragraph_id": "1b",
            "nested_vector": [
                -0.12,
                1.56,
                0.42
            ]
        },
        {
            "paragraph_id": "1c",
            "nested_vector": [
                1,
                -1,
                0
            ]
        }
    ],
    "topic": [
        "ai"
    ]
}

POST /retrievers_example_nested/_doc/2
{
    "nested_field": [
        {
            "paragraph_id": "2a",
            "nested_vector": [
                0.23,
                1.24,
                0.65
            ]
        }
    ],
    "topic": [
        "information_retrieval"
    ]
}

POST /retrievers_example_nested/_doc/3
{
    "topic": [
        "ai"
    ]
}

POST /retrievers_example_nested/_refresh
```

Now we can run an `rrf` retriever query and also compute [inner hits](/reference/elasticsearch/rest-apis/retrieve-inner-hits.md) for the `nested_field.nested_vector` field, based on the `knn` query specified.

```console
GET /retrievers_example_nested/_search
{
    "retriever": {
        "rrf": {
            "retrievers": [
                {
                    "standard": {
                        "query": {
                            "nested": {
                                "path": "nested_field",
                                "inner_hits": {
                                    "name": "nested_vector",
                                    "_source": false,
                                    "fields": [
                                        "nested_field.paragraph_id"
                                    ]
                                },
                                "query": {
                                    "knn": {
                                        "field": "nested_field.nested_vector",
                                        "query_vector": [
                                            1,
                                            0,
                                            0.5
                                        ],
                                        "k": 10
                                    }
                                }
                            }
                        }
                    }
                },
                {
                    "standard": {
                        "query": {
                            "term": {
                                "topic": "ai"
                            }
                        }
                    }
                }
            ],
            "rank_window_size": 10,
            "rank_constant": 1
        }
    },
    "_source": [
        "topic"
    ]
}
```

This would propagate the `inner_hits` defined for the `knn` query to the `rrf` retriever, and compute inner hits for `rrf`'s top results.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 3,
            "relation": "eq"
        },
        "max_score": 1.0,
        "hits": [
            {
                "_index": "retrievers_example_nested",
                "_id": "1",
                "_score": 1.0,
                "_source": {
                    "topic": [
                        "ai"
                    ]
                },
                "inner_hits": {
                    "nested_vector": {
                        "hits": {
                            "total": {
                                "value": 3,
                                "relation": "eq"
                            },
                            "max_score": 0.44444445,
                            "hits": [
                                {
                                    "_index": "retrievers_example_nested",
                                    "_id": "1",
                                    "_nested": {
                                        "field": "nested_field",
                                        "offset": 2
                                    },
                                    "_score": 0.44444445,
                                    "fields": {
                                        "nested_field": [
                                            {
                                                "paragraph_id": [
                                                    "1c"
                                                ]
                                            }
                                        ]
                                    }
                                },
                                {
                                    "_index": "retrievers_example_nested",
                                    "_id": "1",
                                    "_nested": {
                                        "field": "nested_field",
                                        "offset": 1
                                    },
                                    "_score": 0.21301977,
                                    "fields": {
                                        "nested_field": [
                                            {
                                                "paragraph_id": [
                                                    "1b"
                                                ]
                                            }
                                        ]
                                    }
                                },
                                {
                                    "_index": "retrievers_example_nested",
                                    "_id": "1",
                                    "_nested": {
                                        "field": "nested_field",
                                        "offset": 0
                                    },
                                    "_score": 0.16889325,
                                    "fields": {
                                        "nested_field": [
                                            {
                                                "paragraph_id": [
                                                    "1a"
                                                ]
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            {
                "_index": "retrievers_example_nested",
                "_id": "2",
                "_score": 0.33333334,
                "_source": {
                    "topic": [
                        "information_retrieval"
                    ]
                },
                "inner_hits": {
                    "nested_vector": {
                        "hits": {
                            "total": {
                                "value": 1,
                                "relation": "eq"
                            },
                            "max_score": 0.31715825,
                            "hits": [
                                {
                                    "_index": "retrievers_example_nested",
                                    "_id": "2",
                                    "_nested": {
                                        "field": "nested_field",
                                        "offset": 0
                                    },
                                    "_score": 0.31715825,
                                    "fields": {
                                        "nested_field": [
                                            {
                                                "paragraph_id": [
                                                    "2a"
                                                ]
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                }
            },
            {
                "_index": "retrievers_example_nested",
                "_id": "3",
                "_score": 0.33333334,
                "_source": {
                    "topic": [
                        "ai"
                    ]
                },
                "inner_hits": {
                    "nested_vector": {
                        "hits": {
                            "total": {
                                "value": 0,
                                "relation": "eq"
                            },
                            "max_score": null,
                            "hits": []
                        }
                    }
                }
            }
        ]
    }
}
```

::::


Note: if using more than one `inner_hits` we need to provide custom names for each `inner_hits` so that they are unique across all retrievers within the request.


## Example: Combine RRF with aggregations [retrievers-examples-rrf-and-aggregations]

Retrievers support both composability and most of the standard `_search` functionality. For instance, we can compute aggregations with the `rrf` retriever. When using a compound retriever, the aggregations are computed based on its nested retrievers. In the following example, the `terms` aggregation for the `topic` field will include all results, not just the top `rank_window_size`, from the 2 nested retrievers, i.e. all documents whose `year` field is greater than 2023, and whose `topic` field matches the term `elastic`.

```console
GET retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "retrievers": [
                {
                    "standard": {
                        "query": {
                            "range": {
                                "year": {
                                    "gt": 2023
                                }
                            }
                        }
                    }
                },
                {
                    "standard": {
                        "query": {
                            "term": {
                                "topic": "elastic"
                            }
                        }
                    }
                }
            ],
            "rank_window_size": 10,
            "rank_constant": 1
        }
    },
    "_source": false,
    "aggs": {
        "topics": {
            "terms": {
                "field": "topic"
            }
        }
    }
}
```

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 4,
            "relation": "eq"
        },
        "max_score": 0.5833334,
        "hits": [
            {
                "_index": "retrievers_example",
                "_id": "5",
                "_score": 0.5833334
            },
            {
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.5
            },
            {
                "_index": "retrievers_example",
                "_id": "4",
                "_score": 0.5
            },
            {
                "_index": "retrievers_example",
                "_id": "3",
                "_score": 0.33333334
            }
        ]
    },
    "aggregations": {
        "topics": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {
                    "key": "ai",
                    "doc_count": 3
                },
                {
                    "key": "elastic",
                    "doc_count": 2
                },
                {
                    "key": "assistant",
                    "doc_count": 1
                },
                {
                    "key": "documentation",
                    "doc_count": 1
                },
                {
                    "key": "information_retrieval",
                    "doc_count": 1
                },
                {
                    "key": "llm",
                    "doc_count": 1
                },
                {
                    "key": "observability",
                    "doc_count": 1
                },
                {
                    "key": "security",
                    "doc_count": 1
                }
            ]
        }
    }
}
```

::::



## Example: Explainability with multiple retrievers [retrievers-examples-explain-multiple-rrf]

By adding `explain: true` to the request, each retriever will now provide a detailed explanation of all the steps and calculations required to compute the final score. Composability is fully supported in the context of `explain`, and each retriever will provide its own explanation, as shown in the example below.

```console
GET /retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "retrievers": [
                {
                    "standard": {
                        "query": {
                            "term": {
                                "topic": "elastic"
                            }
                        }
                    }
                },
                {
                    "rrf": {
                        "retrievers": [
                            {
                                "standard": {
                                    "query": {
                                        "query_string": {
                                            "query": "(information retrieval) OR (artificial intelligence)",
                                            "default_field": "text"
                                        }
                                    }
                                }
                            },
                            {
                                "knn": {
                                    "field": "vector",
                                    "query_vector": [
                                        0.23,
                                        0.67,
                                        0.89
                                    ],
                                    "k": 3,
                                    "num_candidates": 5
                                }
                            }
                        ],
                        "rank_window_size": 10,
                        "rank_constant": 1
                    }
                }
            ],
            "rank_window_size": 10,
            "rank_constant": 1
        }
    },
    "_source": false,
    "size": 1,
    "explain": true
}
```

The output of which, albeit a bit verbose, will provide all the necessary info to assist in debugging and reason with ranking.

::::{dropdown} Example response
```console-result
{
    "took": 42,
    "timed_out": false,
    "_shards": {
        "total": 1,
        "successful": 1,
        "skipped": 0,
        "failed": 0
    },
    "hits": {
        "total": {
            "value": 5,
            "relation": "eq"
        },
        "max_score": 0.5,
        "hits": [
            {
                "_shard": "[retrievers_example][0]",
                "_node": "jnrdZFKS3abUgWVsVdj2Vg",
                "_index": "retrievers_example",
                "_id": "1",
                "_score": 0.5,
                "_explanation": {
                    "value": 0.5,
                    "description": "rrf score: [0.5] computed for initial ranks [0, 1] with rankConstant: [1] as sum of [1 / (rank + rankConstant)] for each query",
                    "details": [
                        {
                            "value": 0.0,
                            "description": "rrf score: [0], result not found in query at index [0]",
                            "details": []
                        },
                        {
                            "value": 1,
                            "description": "rrf score: [0.5], for rank [1] in query at index [1] computed as [1 / (1 + 1)], for matching query with score",
                            "details": [
                                {
                                    "value": 0.8333334,
                                    "description": "rrf score: [0.8333334] computed for initial ranks [2, 1] with rankConstant: [1] as sum of [1 / (rank + rankConstant)] for each query",
                                    "details": [
                                        {
                                            "value": 2,
                                            "description": "rrf score: [0.33333334], for rank [2] in query at index [0] computed as [1 / (2 + 1)], for matching query with score",
                                            "details": [
                                                {
                                                    "value": 2.8129659,
                                                    "description": "sum of:",
                                                    "details": [
                                                        {
                                                            "value": 1.4064829,
                                                            "description": "weight(text:information in 0) [PerFieldSimilarity], result of:",
                                                            "details": [
                                                                ***
                                                            ]
                                                        },
                                                        {
                                                            "value": 1.4064829,
                                                            "description": "weight(text:retrieval in 0) [PerFieldSimilarity], result of:",
                                                            "details": [
                                                                ***
                                                            ]
                                                        }
                                                    ]
                                                }
                                            ]
                                        },
                                        {
                                            "value": 1,
                                            "description": "rrf score: [0.5], for rank [1] in query at index [1] computed as [1 / (1 + 1)], for matching query with score",
                                            "details": [
                                                {
                                                    "value": 1,
                                                    "description": "doc [0] with an original score of [1.0] is at rank [1] from the following source queries.",
                                                    "details": [
                                                        {
                                                            "value": 1.0,
                                                            "description": "found vector with calculated similarity: 1.0",
                                                            "details": []
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            }
        ]
    }
}
```

::::



## Example: Rerank results of an RRF retriever [retrievers-examples-text-similarity-reranker-on-top-of-rrf]

To demonstrate the full functionality of retrievers, the following examples also require access to a [semantic reranking model](docs-content://solutions/search/ranking/semantic-reranking.md) set up using the [Elastic inference APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-inference).

In this example we’ll set up a reranking service and use it with the `text_similarity_reranker` retriever to rerank our top results.

```console
PUT _inference/rerank/my-rerank-model
{
 "service": "cohere",
 "service_settings": {
   "model_id": "rerank-english-v3.0",
   "api_key": "{{COHERE_API_KEY}}"
 }
}
```

Let’s start by reranking the results of the `rrf` retriever in our previous example.

```console
GET retrievers_example/_search
{
    "retriever": {
        "text_similarity_reranker": {
            "retriever": {
                "rrf": {
                    "retrievers": [
                        {
                            "standard": {
                                "query": {
                                    "query_string": {
                                        "query": "(information retrieval) OR (artificial intelligence)",
                                        "default_field": "text"
                                    }
                                }
                            }
                        },
                        {
                            "knn": {
                                "field": "vector",
                                "query_vector": [
                                    0.23,
                                    0.67,
                                    0.89
                                ],
                                "k": 3,
                                "num_candidates": 5
                            }
                        }
                    ],
                    "rank_window_size": 10,
                    "rank_constant": 1
                }
            },
            "field": "text",
            "inference_id": "my-rerank-model",
            "inference_text": "What are the state of the art applications of AI in information retrieval?"
        }
    },
    "_source": false
}
```


## Example: RRF with semantic reranker [retrievers-examples-rrf-ranking-on-text-similarity-reranker-results]

For this example, we’ll replace the rrf’s `standard` retriever with the `text_similarity_reranker` retriever, using the `my-rerank-model` reranker we previously configured. Since this is a reranker, it needs an initial pool of documents to work with. In this case, we’ll rerank the top `rank_window_size` documents matching the  `ai` topic.

```console
GET /retrievers_example/_search
{
    "retriever": {
        "rrf": {
            "retrievers": [
                {
                    "knn": {
                        "field": "vector",
                        "query_vector": [
                            0.23,
                            0.67,
                            0.89
                        ],
                        "k": 3,
                        "num_candidates": 5
                    }
                },
                {
                    "text_similarity_reranker": {
                        "retriever": {
                            "standard": {
                                "query": {
                                    "term": {
                                        "topic": "ai"
                                    }
                                }
                            }
                        },
                        "field": "text",
                        "inference_id": "my-rerank-model",
                        "inference_text": "Can I use generative AI to identify user intent and improve search relevance?"
                    }
                }
            ],
            "rank_window_size": 10,
            "rank_constant": 1
        }
    },
    "_source": false
}
```


## Example: Chaining multiple semantic rerankers [retrievers-examples-chaining-text-similarity-reranker-retrievers]

Full composability means we can chain together multiple retrievers of the same type. For instance, imagine we have a computationally expensive reranker that’s specialized for AI content. We can rerank the results of a `text_similarity_reranker` using another `text_similarity_reranker` retriever. Each reranker can operate on different fields and/or use different inference services.

```console
GET retrievers_example/_search
{
    "retriever": {
        "text_similarity_reranker": {
            "retriever": {
                "text_similarity_reranker": {
                    "retriever": {
                        "knn": {
                            "field": "vector",
                            "query_vector": [
                                0.23,
                                0.67,
                                0.89
                            ],
                            "k": 3,
                            "num_candidates": 5
                        }
                    },
                    "rank_window_size": 100,
                    "field": "text",
                    "inference_id": "my-rerank-model",
                    "inference_text": "What are the state of the art applications of AI in information retrieval?"
                }
            },
            "rank_window_size": 10,
            "field": "text",
            "inference_id": "my-other-more-expensive-rerank-model",
            "inference_text": "Applications of Large Language Models in technology and their impact on user satisfaction"
        }
    },
    "_source": false
}
```

Note that our example applies two reranking steps. First, we rerank the top 100 documents from the `knn` search using the `my-rerank-model` reranker. Then we pick the top 10 results and rerank them using the more fine-grained `my-other-more-expensive-rerank-model`.

