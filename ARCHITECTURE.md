# Architecture: php-redis-session-abstract

## Purpose

A PHP session handler backed by Redis, designed for high-traffic e-commerce environments. Provides distributed session storage with support for Redis Sentinel, Redis Cluster, concurrency locking, and TLS connections. Used by Magento 2 as its primary session backend.

## Directory Structure

```
src/Cm/RedisSession/
  Handler.php                         — Core session handler implementing SessionHandlerInterface
  Handler/
    Config_Interface.php              — Interface for session configuration (host, port, db, timeout, etc.)
    Cluster_Config_Interface.php      — Additional config for Redis Cluster mode
    Config_Sentinel_Password_Interface.php — Config extension for Sentinel password auth
    Username_Config_Interface.php     — Config extension for Redis 6+ username/password auth
    Tls_Options_Config_Interface.php  — Config extension for TLS connection options
    Logger_Interface.php              — Interface for logging session events
  Concurrent_Connections_Exceeded_Exception.php — Thrown when max concurrent connections are reached
  Connection_Failed_Exception.php     — Thrown when Redis connection cannot be established
```

## Key Design Decisions

- **Optimistic locking with spin retry**: The handler acquires a session lock in Redis using a SETNX-style mechanism, retrying on failure to prevent concurrent writes from corrupting session data
- **Interface-driven config**: Configuration is expressed as interfaces rather than a concrete class, allowing integration frameworks (Magento, Symfony, etc.) to provide their own config objects
- **Sentinel support**: Automatic failover by querying Redis Sentinel for the current master before each connection attempt
- **Compression**: Session data is optionally compressed (LZF or GZIP) before storage to reduce Redis memory usage
- **Break-after-max-wait**: After a configurable maximum wait time, the handler can break the lock to prevent deadlocks at the cost of potential session data loss

## Extension Points

- Implement `Config_Interface` (and optionally `Cluster_Config_Interface`, `Username_Config_Interface`, `Tls_Options_Config_Interface`) to integrate with any configuration system
- Implement `Logger_Interface` to route session handler logs to your application's logger

## Dependency Flow

```
SessionHandlerInterface (PHP built-in)
  ← Handler (implements)
        ← Config_Interface (injected)
        ← Logger_Interface (injected)
        ← Redis / RedisCluster (phpredis extension)
```
