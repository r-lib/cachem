
- [cachem](#cachem)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Cache types](#cache-types)
    - [`cache_mem()`](#cache_mem)
    - [`cache_disk()`](#cache_disk)
  - [Cache API](#cache-api)
  - [Pruning](#pruning)
  - [Layered caches](#layered-caches)

<!-- README.md is generated from README.Rmd. Please edit that file -->

# cachem

<!-- badges: start -->

[![R build
status](https://github.com/r-lib/cachem/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/r-lib/cachem/actions)
<!-- badges: end -->

The **cachem** R package provides objects creating and managing caches.
These cache objects are key-value stores, but unlike other basic
key-value stores, they have built-in support for memory and age limits
so that they won’t have unbounded growth.

The cache objects in **cachem** differ from some other key-value stores
in the following ways:

- The cache objects provide automatic pruning so that they remain within
  memory limits.
- Fetching a non-existing object returns a sentinel value. An
  alternative is to simply return `NULL`. This is what R lists and
  environments do, but it is ambiguous whether the value really is
  `NULL`, or if it is not present. Another alternative is to throw an
  exception when fetching a non-existent object. However, this results
  in more complicated code, as every `get()` needs to be wrapped in a
  `tryCatch()`.

## Installation

To install the CRAN version:

``` r
install.packages("cachem")
```

You can install the development version from with:

``` r
if (!require("remotes")) install.packages("remotes")
remotes::install_github("r-lib/cachem")
```

## Usage

To create a memory-based cache, call `cache_mem()`.

``` r
library(cachem)
m <- cache_mem()
```

Add arbitrary R objects to the cache using `$set(key, value)`:

``` r
m$set("abc123", c("Hello", "world"))
m$set("xyz", function() message("Goodbye"))
```

The `key` must be a string consisting of lowercase letters, numbers, and
the underscore (`_`) and hyphen (`-`) characters. (Upper-case characters
are not allowed because some storage backends do not distinguish between
lowercase and uppercase letters.) The `value` can be any R object.

Get the values with `$get()`:

``` r
m$get("abc123")
#> [1] "Hello" "world"

m$get("xyz")
#> function() message("Goodbye")
```

If you call `get()` on a key that doesn’t exists, it will return a
`key_missing()` sentinel value:

``` r
m$get("dog")
#> <Key Missing>
```

A common usage pattern is to call `get()`, and then check if the result
is a `key_missing` object:

``` r
value <- m$get(key)

if (is.key_missing(value)) {
  # Cache miss - do something
} else {
  # Cache hit - do another thing
}
```

The reason for doing this (instead of calling `$exists(key)` and then
`$get(key)`) is that for some storage backends, there is a potential
race condition: the object could be removed from the cache between the
`exists()` and `get()` calls. For example:

- If multiple R processes have `cache_disk`s that share the same
  directory, one process could remove an object from the cache in
  between the `exists()` and `get()` calls in another process, resulting
  in an error.
- If you use a `cache_mem` with a `max_age`, it’s possible for an object
  to be present when you call `exists()`, but for its age to exceed
  `max_age` by the time `get()` is called. In that case, the `get()`
  will return a `key_missing()` object.

``` r
# Avoid this pattern, due to a potential race condition!
if (m$exists(key)) {
  value <- m$get(key)
}
```

## Cache types

**cachem** comes with two kinds of cache objects: a memory cache, and a
disk cache.

### `cache_mem()`

The memory cache stores stores objects in memory, by simply keeping a
reference to each object. To create a memory cache:

``` r
m <- cache_mem()
```

The default size of the cache is 200MB, but this can be customized with
`max_size`:

``` r
m <- cache_mem(max_size = 10 * 1024^2)
```

It may also be useful to set a maximum age of objects. For example, if
you only want objects to stay for a maximum of one hour:

``` r
m <- cache_mem(max_size = 10 * 1024^2, max_age = 3600)
```

For more about how objects are evicted from the cache, see section
[Pruning](#pruning) below.

An advantage that the memory cache has over the disk cache (and any
other type of cache that stores the objects outside of the R process’s
memory), is that it does not need to serialize objects. Instead, it
merely stores references to the objects. This means that it can store
objects that other caches cannot, and with more efficient use of memory
– if two objects in the cache share some of their contents (such that
they refer to the same sub-object in memory), then `cache_mem` will not
create duplicate copies of the contents, as `cache_disk` would, since it
serializes the objects with the `serialize()` function.

Compared to the memory usage, the size *calculation* is not as
intelligent: if there are two objects that share contents, their sizes
are computed separately, even if they have items that share the exact
same represention in memory. This is done with the `object.size()`
function, which does not account for multiple references to the same
object in memory.

In short, a memory cache, if anything, over-counts the amount of memory
actually consumed. In practice, this means that if you set a 200MB limit
to the size of cache, and the cache *thinks* it has 200MB of contents,
the actual amount of memory consumed could be less than 200MB.

<details>
<summary>
Demonstration of memory over-counting from `object.size()`
</summary>

``` r
# Create a and b which both contain the same numeric vector.
x <- list(rnorm(1e5))
a <- list(1, x)
b <- list(2, x)

# Add to cache
m$set("a", a)
m$set("b", b)

# Each object is about 800kB in memory, so the cache_mem() will consider the
# total memory used to be 1600kB.
object.size(m$get("a"))
#> 800224 bytes
object.size(m$get("b"))
#> 800224 bytes
```

For reference, lobstr::obj_size can detect shared objects, and knows
that these objects share most of their memory.

``` r
lobstr::obj_size(m$get("a"))
#> 800.22 kB
lobstr::obj_size(list(m$get("a"), m$get("b")))
#> 800.41 kB
```

However, lobstr is not on CRAN, and if obj_size() were used to find the
incremental memory used when an object was added to the cache, it would
have to walk all objects in the cache every time a single object is
added. For these reasons, cache_mem uses `object.size()` to compute the
object sizes.

</details>

### `cache_disk()`

Disk caches are stored in a directory on disk. A disk cache is slower
than a memory cache, but can generally be larger. To create one:

``` r
d <- cache_disk()
```

By default, it creates a subdirectory of the R process’s temp directory,
and it will persist until the R process exits.

``` r
d$info()$dir
#>  "/tmp/Rtmp6h5iB3/cache_disk-d1901b2b615a"
```

Like a `cache_mem`, the `max_size`, `max_n`, `max_age` can be
customized. See section [Pruning](#pruning) below for more information.

Each object in the cache is stored as an RDS file on disk, using the
`serialize()` function.

``` r
d$set("abc", 100)
d$set("x01", list(1, 2, 3))

dir(d$info()$dir)
#> [1] "abc.rds" "x01.rds"
```

Since objects in a disk cache are serialized, they are subject to the
limitations of the `serialize()` function. For more information, see
section [Limitations of serialized
objects](#limitations-of-serialized-objects).

The storage directory can be specified with `dir`; it will be created if
necessary.

``` r
cache_disk(dir = "cachedir")
```

#### Sharing a disk cache among processes

Multiple R processes can use `disk_cache` objects that share the same
cache directory. To do this, simply point each `cache_disk` to the same
directory.

#### `disk_cache` pruning

For a `disk_cache`, pruning does not happen on every access, because
finding the size of files in the cache directory can take a nontrivial
amount of time. By default, pruning happens once every 20 times that
`$set()` is called, or if at least five seconds have elapsed since the
last pruning. The `prune_rate` controls how many times `$set()` must be
called before a pruning occurs. It defaults to 20; smaller values result
in more frequent pruning and larger values result in less frequent
pruning (but keep in mind pruning always occurs if it has been at least
five seconds since the last pruning).

#### Cleaning up the cache directory

The cache directory can be deleted by calling `$destroy()`. After it is
destroyed, the cache object can no longer be used.

``` r
d$destroy()
d$set("a", 1)  # Error
```

To create a `cache_disk` that will automatically delete its storage
directory when garbage collected, use `destroy_on_finalize=TRUE`:

``` r
d <- cache_disk(destroy_on_finalize = TRUE)
d$set("a", 1)

cachedir <- d$info()$dir
dir(cachedir)
#> [1] "a.rds"

# Remove reference to d and trigger a garbage collection
rm(d)
gc()

dir.exists(cachedir)
```

#### Using custom serialization functions

It is possible to use custom serialization functions rather than the
default of `writeRDS()` and `readRDS()` with the `write_fn`, `read_fn`
and `extension` arguments respectively. This could be used to use
alternative serialization formats like
[qs](https://github.com/traversc/qs), or specialized object formats
[fst](http://www.fstpackage.org/fst/) or parquet.

``` r
library(qs)

d <- cache_disk(read_fn = qs::qread, write_fn = qs::qsave, extension = ".qs")

d$set("a", list(1, 2, 3))

cachedir <- d$info()$dir
dir(cachedir)
#> [1] "a.qs"
d$get("a")
#> [[1]]
#> [1] 1
#>
#> [[2]]
#> [1] 2
#>
#> [[3]]
#> [1] 3
```

## Cache API

`cache_mem()` and `cache_disk()` support all of the methods listed
below. If you want to create a compatible caching object, it must have
at least the `get()` and `set()` methods:

- `get(key, missing = missing_)`: Get the object associated with `key`.
  The `missing` parameter allows customized behavior if the key is not
  present: it actually is an expression which is evaluated when there is
  a cache miss, and it could return a value or throw an error.
- `set(key, value)`: Set a key to a value.
- `exists(key)`: Check whether a particular key exists in the cache.
- `remove(key)`: Remove a key-value from the cache.

Some optional methods:

- `reset()`: Clear all objects from the cache.
- `keys()`: Return a character vector of all keys in the cache.
- `prune()`: Prune the cache. (Some types of caches may not prune on
  every access, and may temporarily grow past their limits, until the
  next pruning is triggered automatically, or manually with this
  function.)
- `size()`: Return the number of objects in the cache.

For these methods:

- `key`: can be any string with lowercase letters, numbers, underscore
  (`_`) and hyphen (`-`). Some storage backends may not be handle very
  long keys well. For example, with a `cache_disk()`, the key is used as
  a filename, and on some filesystems, very filenames may hit limits on
  path lengths.
- `value`: can be any R object, with some exceptions noted below.

#### Limitations of serialized objects

For any cache that serializes the object for storage outside of the R
process – in other words, any cache other than a `cache_mem()` – some
types of objects will not save and restore as well. Notably, reference
objects may consume more memory when restored, since R may not know to
deduplicate shared objects. External pointers are not be able to be
serialized, since they point to memory in the R process. See
`?serialize` for more information.

#### Read-only caches

It is possible to create a read-only cache by making the `set()`,
`remove()`, `reset()`, and `prune()` methods into no-ops. This can be
useful if sharing a cache with another R process which can write to the
cache. For example, one (or more) processes can write to the cache, and
other processes can read from it.

This function will wrap a cache object in a read-only wrapper. Note,
however, that code that uses such a cache must not require that `$set()`
actually sets a value in the cache. This is good practice anyway,
because with these cache objects, items can be pruned from them at any
time.

``` r
cache_readonly_wrap <- function(cache) {
  structure(
    list(
      get = cache$get,
      set = function(key, value) NULL,
      exists = cache$exists,
      keys = cache$keys,
      remove = function(key) NULL,
      reset = function() NULL,
      prune = function() NULL,
      size = cache$size
    ),
    class = c("cache_readonly", class(cache))
  )
}

mr <- cache_readonly_wrap(m)
```

## Pruning

The cache objects provided by cachem have automatic pruning. (Note that
pruning is not required by the API, so one could implement an
API-compatible cache without pruning.)

This section describes how pruning works for `cache_mem()` and
`cache_disk()`.

When the cache object is created, the maximum size (in bytes) is
specified by `max_size`. When the size of objects in the cache exceeds
`max_size`, objects will be pruned from the cache.

When objects are pruned from the cache, which ones are removed is
determined by the eviction policy, `evict`:

- **`lru`**: The least-recently-used objects will be removed from the
  cache, until it fits within the limit. This is the default and is
  appropriate for most cases.
- **`fifo`**: The oldest objects will be removed first.

It is also possible to set the maximum number of items that can be in
the cache, with `max_n`. By default this is set to `Inf`, or no limit.

The `max_age` parameter is somewhat different from `max_size` and
`max_n`. The latter two set limits on the cache store as a whole,
whereas `max_age` sets limits for each individual item; for each item,
if its age exceeds `max_age`, then it will be removed from the cache.

## Layered caches

Multiple caches can be composed into a single cache, using
`cache_layered()`. This can be used to create a multi-level cache. (Note
thate `cache_layered()` is currently experimental.) For example, we can
create a layered cache with a very fast 100MB memory cache and a larger
but slower 2GB disk cache:

``` r
m <- cache_mem(max_size = 100 * 1024^2)
d <- cache_disk(max_size = 2 * 1024^3)

cl <- cache_layered(m, d)
```

The layered cache will have the same API, with `$get()`, `$set()`, and
so on, so it can be used interchangeably with other caching objects.

For this example, we’ll recreate the `cache_layered` with logging
enabled, so that it will show cache hits and misses.

``` r
cl <- cache_layered(m, d, logfile = stderr())

# Each of the objects generated by rnorm() is about 40 MB
cl$set("a", rnorm(5e6))
cl$set("b", rnorm(5e6))
cl$set("c", rnorm(5e6))

# View the objects in each of the component caches
m$keys()
#> [1] "c" "b"
d$keys()
#> [1] "a" "b" "c"

# The layered cache reports having all keys
cl$keys()
#> [1] "c" "b" "a"
```

When `$get()` is called, it searches the first cache, and if it’s
missing there, it searches the next cache, and so on. If not found in
any caches, it returns `key_missing()`.

``` r
# Get object that exists in the memory cache
x <- cl$get("c")
#> [2020-10-23 13:11:09.985] cache_layered Get: c
#> [2020-10-23 13:11:09.985] cache_layered Get from cache_mem... hit

# Get object that doesn't exist in the memory cache
x <- cl$get("a")
#> [2020-10-23 13:13:10.968] cache_layered Get: a
#> [2020-10-23 13:13:10.969] cache_layered Get from cache_mem... miss
#> [2020-10-23 13:13:11.329] cache_layered Get from cache_disk... hit

# Object is not present in any component caches
cl$get("d")
#> [2020-10-23 13:13:40.197] cache_layered Get: d
#> [2020-10-23 13:13:40.197] cache_layered Get from cache_mem... miss
#> [2020-10-23 13:13:40.198] cache_layered Get from cache_disk... miss
#> <Key Missing>
```

Multiple cache objects can be layered this way. You could even add a
cache which uses a remote store, such as a network file system or even
AWS S3.
