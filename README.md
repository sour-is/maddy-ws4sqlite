# maddy-ws4sqlite
ws4sqlite interfaces for maddy.email

# config

```
table.ws4sql_query aliases {
    url    http://example.com/db1
    lookup "SELECT alias FROM aliases WHERE email = :key"
    list   "SELECT email FROM aliases"
    add    "INSERT INTO aliases (alias, email) VALUES (:key, :value)"
    set    "UPDATE aliases SET email = :value WHERE alias = :key"
    del    "DELETE FROM aliases WHERE alias = :key "
}

table.ws4sql_cache aliases {
    ... same as above ...

    cache_size 1000
}
```
