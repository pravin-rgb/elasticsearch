% This is generated by ESQL's AbstractFunctionTestCase. Do not edit it. See ../README.md for how to regenerate it.

**Example**

```esql
FROM employees
| STATS sample = SAMPLE(gender, 5)
```

| sample:keyword |
| --- |
| [F, M, M, F, M] |


