<?php

declare (strict_types=1);
/*
==New BSD License==

Copyright (c) 2013, Colin Mollenhour
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

   * Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
   * The name of Colin Mollenhour may not be used to endorse or promote products
     derived from this software without specific prior written permission.
   * Redistributions in any form must not change the Cm_RedisSession namespace.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
namespace Cm\Redis_Session;

/**
 * Redis session handler with optimistic locking.
 *
 * Features:
 *  - When a session's data exceeds the compression threshold the session data will be compressed.
 *  - Compression libraries supported are 'gzip', 'lzf', 'lz4' and 'snappy'.
 *  - Compression can be enabled, disabled, or reconfigured on the fly with no loss of session data.
 *  - Expiration is handled by Redis. No garbage collection needed.
 *  - Logs when sessions are not written due to not having or losing their lock.
 *  - Limits the number of concurrent lock requests.
 *
 * Locking Algorithm Properties:
 *  - Only one process may get a write lock on a session.
 *  - A process may lose it's lock if another process breaks it, in which case the session will not be written.
 *  - The lock may be broken after BREAK_AFTER seconds and the process that gets the lock is indeterminate.
 *  - Only MAX_CONCURRENCY processes may be waiting for a lock for the same session or else a ConcurrentConnectionsExceededException will be thrown.
 *  - Detects crashed processes to prevent session deadlocks (Linux only).
 *  - Detects inactive waiting processes to prevent false-positives in concurrency throttling.
 */
use Cm\Redis_Session\Handler\Cluster_Config_Interface;
use Cm\Redis_Session\Handler\Config_Interface;
use Cm\Redis_Session\Handler\Config_Sentinel_Password_Interface;
use Cm\Redis_Session\Handler\Logger_Interface;
use Cm\Redis_Session\Handler\Tls_Options_Config_Interface;
use Cm\Redis_Session\Handler\Username_Config_Interface;
class Handler implements \Session_Handler_Interface
{
    /**
     * Sleep 0.5 seconds between lock attempts (1,000,000 == 1 second)
     */
    public const SLEEP_TIME = 500000;
    /**
     * Try to detect zombies every this many tries
     */
    public const DETECT_ZOMBIES = 20;
    /**
     * Session prefix
     */
    public const SESSION_PREFIX = 'sess_';
    /**
     * Bots get shorter session lifetimes
     */
    public const BOT_REGEX = '/^alexa|^blitz\.io|bot|^browsermob|crawl|^curl|^facebookexternalhit|feed|google web preview|^ia_archiver|indexer|^java|jakarta|^libwww-perl|^load impact|^magespeedtest|monitor|^Mozilla$|nagios |^\.net|^pinterest|postrank|slurp|spider|uptime|^wget|yandex|^elb-healthchecker|binglocalsearch/i';
    /**
     * Default connection timeout
     */
    public const DEFAULT_TIMEOUT = 2.5;
    /**
     * Default connection retries
     */
    public const DEFAULT_RETRIES = 0;
    /**
     * Default compression threshold
     */
    public const DEFAULT_COMPRESSION_THRESHOLD = 2048;
    /**
     * Default compression library
     */
    public const DEFAULT_COMPRESSION_LIBRARY = 'gzip';
    /**
     * Default log level
     */
    public const DEFAULT_LOG_LEVEL = Logger_Interface::ALERT;
    /**
     * Maximum number of processes that can wait for a lock on one session
     */
    public const DEFAULT_MAX_CONCURRENCY = 6;
    /**
     * Try to break the lock after this many seconds
     */
    public const DEFAULT_BREAK_AFTER = 30;
    /**
     * Try to break lock for at most this many seconds
     */
    public const DEFAULT_FAIL_AFTER = 15;
    /**
     * The session lifetime for non-bots on the first write
     */
    public const DEFAULT_FIRST_LIFETIME = 600;
    /**
     * The session lifetime for bots on the first write
     */
    public const DEFAULT_BOT_FIRST_LIFETIME = 60;
    /**
     * The session lifetime for bots - shorter to prevent bots from wasting backend storage
     */
    public const DEFAULT_BOT_LIFETIME = 7200;
    /**
     * Redis backend limit
     */
    public const DEFAULT_MAX_LIFETIME = 2592000;
    /**
     * Default min lifetime
     */
    public const DEFAULT_MIN_LIFETIME = 60;
    /**
     * Default host
     */
    public const DEFAULT_HOST = '127.0.0.1';
    /**
     * Default port
     */
    public const DEFAULT_PORT = 6379;
    /**
     * Default database
     */
    public const DEFAULT_DATABASE = 0;
    /**
     * Default lifetime
     */
    public const DEFAULT_LIFETIME = 60;
    /**
     * @var \Credis_Client|\Credis_Cluster
     */
    protected $_redis;
    protected readonly bool $_use_pipeline;
    protected readonly bool $_use_cluster;
    /**
     * @var int
     */
    protected $_db_num;
    /**
     * @var string
     */
    protected $_compression_threshold;
    /**
     * @var string
     */
    protected $_compression_library;
    /**
     * @var int
     */
    protected $_max_concurrency;
    /**
     * @var int
     */
    protected $_break_after;
    /**
     * @var int
     */
    protected $_fail_after;
    protected bool $_use_locking;
    /**
     * @var boolean
     */
    protected $_has_lock;
    /**
     * Avoid infinite loops
     *
     * @var boolean
     */
    protected $_session_written;
    /**
     * Set expire time based on activity
     *
     * @var int
     */
    protected $_session_writes;
    /**
     * @var int
     */
    protected $_max_lifetime;
    /**
     * @var int
     */
    protected $_min_lifetime;
    /**
     * For debug or informational purposes
     *
     * @var int
     */
    protected $failed_lock_attempts = 0;
    protected \Cm\Redis_Session\Handler\Config_Interface $config;
    protected \Cm\Redis_Session\Handler\Logger_Interface $logger;
    /**
     * @var int
     */
    protected $_life_time;
    /**
     * @var null|array Callback method to call. It will receive 2 parameters: $userAgent, $isBot
     *
     * Use setBotCheckCallback() to set this value. The property is private to prevent arbitrary
     * third-party code from overriding the bot-detection behaviour without going through the
     * validated setter.
     */
    private static $_bot_check_callback;
    /**
     * Set a custom bot-detection callback.
     *
     * The callback receives two parameters: (string $userAgent, bool $isBot).
     *
     * @param array{0: object, 1: string}|callable $callback
     */
    public static function set_bot_check_callback(callable $callback): void
    {
        self::$_bot_check_callback = $callback;
    }
    /**
     * @var boolean
     */
    private $_read_only;
    /**
     * @param boolean $readOnly
     * @throws ConnectionFailedException
     */
    public function __construct(Config_Interface $config, Logger_Interface $logger, $read_only = false)
    {
        $this->config = $config;
        $this->logger = $logger;
        $this->logger->set_log_level($this->config->get_log_level() ?: self::DEFAULT_LOG_LEVEL);
        $time_start = microtime(true);
        // Database config
        $host = $this->config->get_host() ?: self::DEFAULT_HOST;
        $port = $this->config->get_port() ?: self::DEFAULT_PORT;
        $pass = $this->config->get_password() ?: null;
        $username = $this->config instanceof Username_Config_Interface ? $this->config->get_username() : null;
        $timeout = $this->config->get_timeout() ?: self::DEFAULT_TIMEOUT;
        $retries = $this->config->get_retries() ?: self::DEFAULT_RETRIES;
        $persistent = $this->config->get_persistent_identifier() ?: '';
        $this->_db_num = $this->config->get_database() ?: self::DEFAULT_DATABASE;
        $tls_options = $this->config instanceof Tls_Options_Config_Interface ? $this->config->get_tls_options() : null;
        // General config
        $this->_read_only = $read_only;
        $this->_compression_threshold = $this->config->get_compression_threshold() ?: self::DEFAULT_COMPRESSION_THRESHOLD;
        $this->_compression_library = $this->config->get_compression_library() ?: self::DEFAULT_COMPRESSION_LIBRARY;
        $this->_max_concurrency = $this->config->get_max_concurrency() ?: self::DEFAULT_MAX_CONCURRENCY;
        $this->_fail_after = $this->config->get_fail_after() ?: self::DEFAULT_FAIL_AFTER;
        $this->_max_lifetime = $this->config->get_max_lifetime() ?: self::DEFAULT_MAX_LIFETIME;
        $this->_min_lifetime = $this->config->get_min_lifetime() ?: self::DEFAULT_MIN_LIFETIME;
        $this->_use_locking = !$this->config->get_disable_locking();
        // Use sleep time multiplier so fail after time is in seconds
        $this->_fail_after = (int) round(1000000 / self::SLEEP_TIME * $this->_fail_after);
        // Sentinel config
        $sentinel_servers = $this->config->get_sentinel_servers();
        $sentinel_master = $this->config->get_sentinel_master();
        $sentinel_verify_master = $this->config->get_sentinel_verify_master();
        $sentinel_connect_retries = $this->config->get_sentinel_connect_retries();
        $sentinel_password = $this->config instanceof Config_Sentinel_Password_Interface ? $this->config->get_sentinel_password() : $pass;
        // Connect and authenticate
        if ($sentinel_servers && $sentinel_master) {
            $this->_use_pipeline = true;
            $this->_use_cluster = false;
            $servers = preg_split('/\s*,\s*/', trim($sentinel_servers), -1, PREG_SPLIT_NO_EMPTY);
            $sentinel = null;
            $exception = null;
            for ($i = 0; $i <= $sentinel_connect_retries; $i++) {
                // Try to connect to sentinels in round-robin fashion
                foreach ($servers as $server) {
                    try {
                        $sentinel_client = new \Credis_Client($server, null, $timeout, $persistent);
                        $sentinel_client->force_standalone();
                        $sentinel_client->set_max_connect_retries(0);
                        if ($sentinel_password) {
                            try {
                                $sentinel_client->auth($sentinel_password);
                            } catch (\Credis_Exception $e) {
                                // Prevent throwing exception if Sentinel has no password set (error messages are different between redis 5 and redis 6)
                                if ($e->get_code() !== 0 || strpos($e->get_message(), 'ERR Client sent AUTH, but no password is set') === false && strpos($e->get_message(), 'ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?') === false) {
                                    throw $e;
                                }
                            }
                        }
                        $sentinel = new \Credis_Sentinel($sentinel_client);
                        $sentinel->set_client_timeout($timeout)->set_client_persistent($persistent);
                        $redis_master = $sentinel->get_master_client($sentinel_master);
                        if ($pass) {
                            $redis_master->auth($pass, $username);
                        }
                        // Verify connected server is actually master as per Sentinel client spec
                        if ($sentinel_verify_master) {
                            $role_data = $redis_master->role();
                            if (!$role_data || $role_data[0] != 'master') {
                                usleep(100000);
                                // Sleep 100ms and try again
                                $redis_master = $sentinel->get_master_client($sentinel_master);
                                if ($pass) {
                                    $redis_master->auth($pass, $username);
                                }
                                $role_data = $redis_master->role();
                                if (!$role_data || $role_data[0] != 'master') {
                                    throw new \Exception('Unable to determine master redis server.');
                                }
                            }
                        }
                        if (($this->_db_num || $persistent) && !$this->_use_cluster) {
                            $redis_master->select(0);
                        }
                        $this->_redis = $redis_master;
                        break 2;
                    } catch (\Exception $e) {
                        unset($sentinel_client);
                        $exception = $e;
                    }
                }
            }
            unset($sentinel);
            if (!$this->_redis) {
                throw new Connection_Failed_Exception('Unable to connect to a Redis: ' . $exception->get_message(), 0, $exception);
            }
        } else {
            if ($config instanceof Cluster_Config_Interface && $config->is_cluster()) {
                $this->_redis = new \Credis_Cluster($config->get_cluster_name(), $config->get_cluster_seeds(), $timeout, 0, $config->get_cluster_use_persistent_connection(), $pass, $username, $tls_options);
                $this->_use_pipeline = false;
                $this->_use_cluster = true;
            } else {
                $this->_redis = new \Credis_Client($host, $port, $timeout, $persistent, 0, $pass, $username, $tls_options);
                $this->_use_pipeline = true;
                $this->_use_cluster = false;
            }
            $this->_redis->set_max_connect_retries($retries);
            if ($this->has_connection() == false) {
                throw new Connection_Failed_Exception('Unable to connect to Redis');
            }
        }
        // Destructor order cannot be predicted
        $this->_redis->set_close_on_destruct(false);
        if ($this->_use_cluster) {
            $this->_log(sprintf('%s initialized for connection to %s after %.5f seconds', get_class($this), !empty($this->_redis->get_cluster_seeds()) ? var_export($this->_redis->get_cluster_seeds(), true) : $this->_redis->get_cluster_name(), microtime(true) - $time_start));
        } else {
            $this->_log(sprintf('%s initialized for connection to %s:%s after %.5f seconds', get_class($this), $this->_redis->get_host(), $this->_redis->get_port(), microtime(true) - $time_start));
        }
    }
    /**
     * Open session
     *
     * @param string $savePath ignored
     * @param string $sessionName ignored
     * @return bool
     * @SuppressWarnings(PHPMD.UnusedFormalParameter)
     */
    #[\Return_Type_Will_Change]
    public function open($save_path, $session_name)
    {
        return true;
    }
    /**
     * @param $msg
     * @param $level
     */
    protected function _log($msg, $level = Logger_Interface::DEBUG)
    {
        $this->logger->log("{$this->_get_pid()}: {$msg}", $level);
    }
    /**
     * Check Redis connection
     */
    protected function has_connection(): bool
    {
        try {
            $this->_redis->connect();
            $this->_log('Connected to Redis');
            return true;
        } catch (\Exception $e) {
            $this->logger->log_exception($e);
            $this->_log('Unable to connect to Redis');
            return false;
        }
    }
    /**
     * Set/unset read only flag
     *
     * @param boolean $readOnly
     */
    public function set_read_only($read_only): self
    {
        $this->_read_only = $read_only;
        return $this;
    }
    /**
     * Fetch session data
     *
     * @param string $sessionId
     * @return string
     * @throws ConcurrentConnectionsExceededException
     */
    #[\Return_Type_Will_Change]
    public function read($session_id)
    {
        // Get lock on session. Increment the "lock" field and if the new value is 1, we have the lock.
        $session_id = self::SESSION_PREFIX . $session_id;
        $tries = $waiting = $lock = 0;
        $lock_pid = $old_lock_pid = null;
        // Restart waiting for lock when current lock holder changes
        $detect_zombies = false;
        $break_after = $this->_get_break_after();
        $time_start = microtime(true);
        $this->_log(sprintf('Attempting to take lock on ID %s', $session_id));
        if (!$this->_use_cluster) {
            $this->_redis->select($this->_db_num);
        }
        while ($this->_use_locking && !$this->_read_only) {
            // Increment lock value for this session and retrieve the new value
            $old_lock = $lock;
            $lock = $this->_redis->h_incr_by($session_id, 'lock', 1);
            // Get the pid of the process that has the lock
            if ($lock != 1 && $tries + 1 >= $break_after) {
                $lock_pid = $this->_redis->h_get($session_id, 'pid');
            }
            // If we got the lock, update with our pid and reset lock and expiration
            if ($lock == 1 || $tries >= $break_after && $old_lock_pid == $lock_pid) {
                $this->_has_lock = true;
                break;
            } elseif (!$waiting) {
                $i = 0;
                do {
                    $waiting = $this->_redis->h_incr_by($session_id, 'wait', 1);
                } while (++$i < $this->_max_concurrency && $waiting < 1);
            } else {
                // Detect broken sessions (e.g. caused by fatal errors)
                if ($detect_zombies) {
                    $detect_zombies = false;
                    // Lock shouldn't be less than old lock (another process broke the lock)
                    if ($lock > $old_lock && $lock + 1 < $old_lock + $waiting) {
                        // Reset session to fresh state
                        $this->_log(sprintf('Detected zombie waiter after %.5f seconds for ID %s (%d waiting)', microtime(true) - $time_start, $session_id, $waiting), Logger_Interface::INFO);
                        $waiting = $this->_redis->h_incr_by($session_id, 'wait', -1);
                        continue;
                    }
                }
                // Limit concurrent lock waiters to prevent server resource hogging
                if ($waiting >= $this->_max_concurrency) {
                    // Overloaded sessions get 503 errors
                    try {
                        $this->_redis->h_incr_by($session_id, 'wait', -1);
                        $this->_session_written = true;
                        // Prevent session from getting written
                        $session_info = $this->_redis->h_m_get($session_id, ['writes', 'req']);
                    } catch (Exception $e) {
                        $this->_log("{$e}", Logger_Interface::WARNING);
                    }
                    $this->_log(sprintf('Session concurrency exceeded for ID %s; displaying HTTP 503 (%s waiting, %s total ' . 'requests) - Locked URL: %s', $session_id, $waiting, $session_info['writes'] ?? '-', $session_info['req'] ?? '-'), Logger_Interface::WARNING);
                    throw new Concurrent_Connections_Exceeded_Exception();
                }
            }
            $tries++;
            $old_lock_pid = $lock_pid;
            $sleep_time = self::SLEEP_TIME;
            // Detect dead lock waiters
            if ($tries % self::DETECT_ZOMBIES == 1) {
                $detect_zombies = true;
                $sleep_time += 10000;
                // sleep + 0.01 seconds
            }
            // Detect dead lock holder every 10 seconds (only works on same node as lock holder)
            if ($tries % self::DETECT_ZOMBIES == 0) {
                $this->_log(sprintf('Checking for zombies after %.5f seconds of waiting...', microtime(true) - $time_start));
                $pid = $this->_redis->h_get($session_id, 'pid');
                if ($pid && !$this->_pid_exists($pid)) {
                    // Allow a live process to get the lock
                    $this->_redis->h_set($session_id, 'lock', 0);
                    $this->_log(sprintf('Detected zombie process (%s) for %s (%s waiting)', $pid, $session_id, $waiting), Logger_Interface::INFO);
                    continue;
                }
            }
            // Timeout
            if ($tries >= $break_after + $this->_fail_after) {
                $this->_has_lock = false;
                $this->_log(sprintf('Giving up on read lock for ID %s after %.5f seconds (%d attempts)', $session_id, microtime(true) - $time_start, $tries), Logger_Interface::NOTICE);
                break;
            } else {
                $this->_log(sprintf('Waiting %.2f seconds for lock on ID %s (%d tries, lock pid is %s, %.5f seconds elapsed)', $sleep_time / 1000000, $session_id, $tries, $lock_pid, microtime(true) - $time_start));
                usleep($sleep_time);
            }
        }
        $this->failed_lock_attempts = $tries;
        // Session can be read even if it was not locked by this pid!
        $time_start2 = microtime(true);
        [$session_data, $session_writes] = array_values($this->_redis->h_m_get($session_id, ['data', 'writes']));
        $this->_log(sprintf('Data read for ID %s in %.5f seconds', $session_id, microtime(true) - $time_start2));
        $this->_session_writes = (int) $session_writes;
        // This process is no longer waiting for a lock
        if ($tries > 0) {
            $this->_redis->h_incr_by($session_id, 'wait', -1);
        }
        // This process has the lock, save the pid
        if ($this->_has_lock) {
            $set_data = ['pid' => $this->_get_pid(), 'lock' => 1];
            // Save request data in session so if a lock is broken we can know which page it was for debugging
            if (empty($_SERVER['REQUEST_METHOD'])) {
                $set_data['req'] = @$_SERVER['SCRIPT_NAME'];
            } else {
                $set_data['req'] = $_SERVER['REQUEST_METHOD'] . ' ' . @$_SERVER['SERVER_NAME'] . @$_SERVER['REQUEST_URI'];
            }
            if ($lock != 1) {
                $this->_log(sprintf("Successfully broke lock for ID %s after %.5f seconds (%d attempts). Lock: %d\nLast request of " . 'broken lock: %s', $session_id, microtime(true) - $time_start, $tries, $lock, $this->_redis->h_get($session_id, 'req')), Logger_Interface::INFO);
            }
        }
        if ($this->_use_pipeline) {
            // Set session data and expiration
            $this->_redis->pipeline();
        }
        if (!empty($set_data)) {
            $this->_redis->h_m_set($session_id, $set_data);
        }
        $this->_redis->expire($session_id, 3600 * 6);
        // Expiration will be set to correct value when session is written
        if ($this->_use_pipeline) {
            $this->_redis->exec();
        }
        // Reset flag in case of multiple session read/write operations
        $this->_session_written = false;
        return $session_data ? (string) $this->_decode_data($session_data) : '';
    }
    /**
     * Update session
     *
     * @param string $sessionId
     * @param string $sessionData
     * @return boolean
     */
    #[\Return_Type_Will_Change]
    public function write($session_id, $session_data)
    {
        if ($this->_session_written || $this->_read_only) {
            $this->_log(sprintf(($this->_session_written ? 'Repeated' : 'Read-only') . ' session write detected; skipping for ID %s', $session_id));
            return true;
        }
        $this->_session_written = true;
        $time_start = microtime(true);
        // Do not overwrite the session if it is locked by another pid
        try {
            if ($this->_db_num && !$this->_use_cluster) {
                $this->_redis->select($this->_db_num);
            }
            // Prevent conflicts with other connections?
            if (!$this->_use_locking || (!($pid = $this->_redis->h_get('sess_' . $session_id, 'pid')) || $pid == $this->_get_pid())) {
                $this->_write_raw_session($session_id, $session_data, $this->get_life_time());
                $this->_log(sprintf('Data written to ID %s in %.5f seconds', $session_id, microtime(true) - $time_start));
            } else if ($this->_has_lock) {
                $this->_log(sprintf('Did not write session for ID %s: another process took the lock.', $session_id), Logger_Interface::WARNING);
            } else {
                $this->_log(sprintf('Did not write session for ID %s: unable to acquire lock.', $session_id), Logger_Interface::WARNING);
            }
        } catch (\Exception $e) {
            $this->logger->log_exception($e);
            return false;
        }
        return true;
    }
    /**
     * Destroy session
     *
     * @param string $sessionId
     * @return boolean
     */
    #[\Return_Type_Will_Change]
    public function destroy($session_id)
    {
        $this->_log(sprintf('Destroying ID %s', $session_id));
        if ($this->_use_pipeline) {
            $this->_redis->pipeline();
        }
        if ($this->_db_num && !$this->_use_cluster) {
            $this->_redis->select($this->_db_num);
        }
        $this->_redis->unlink(self::SESSION_PREFIX . $session_id);
        if ($this->_use_pipeline) {
            $this->_redis->exec();
        }
        return true;
    }
    /**
     * Overridden to prevent calling getLifeTime at shutdown
     *
     * @return bool
     */
    #[\Return_Type_Will_Change]
    public function close()
    {
        $this->_log('Closing connection');
        if ($this->_redis) {
            $this->_redis->close();
        }
        return true;
    }
    /**
     * Garbage collection
     *
     * @param int $maxLifeTime ignored
     * @return boolean
     */
    #[\Return_Type_Will_Change]
    public function gc($max_life_time)
    {
        return true;
    }
    /**
     * Get the number of failed lock attempts
     *
     * @return int
     */
    public function get_failed_lock_attempts()
    {
        return $this->failed_lock_attempts;
    }
    public static function is_bot_agent($user_agent)
    {
        $is_bot = !$user_agent || preg_match(self::BOT_REGEX, $user_agent);
        if (is_array(self::$_bot_check_callback) && isset(self::$_bot_check_callback[0]) && self::$_bot_check_callback[1] && method_exists(self::$_bot_check_callback[0], self::$_bot_check_callback[1])) {
            return (bool) call_user_func_array(self::$_bot_check_callback, [$user_agent, $is_bot]);
        }
        return $is_bot;
    }
    /**
     * Get lock lifetime
     *
     * @return int|mixed
     */
    protected function get_life_time()
    {
        if (is_null($this->_life_time)) {
            $life_time = null;
            // Detect bots by user agent
            $bot_lifetime = is_null($this->config->get_bot_lifetime()) ? self::DEFAULT_BOT_LIFETIME : $this->config->get_bot_lifetime();
            if ($bot_lifetime) {
                $user_agent = empty($_SERVER['HTTP_USER_AGENT']) ? false : $_SERVER['HTTP_USER_AGENT'];
                if (self::is_bot_agent($user_agent)) {
                    $this->_log(sprintf('Bot detected for user agent: %s', $user_agent));
                    $bot_first_lifetime = is_null($this->config->get_bot_first_lifetime()) ? self::DEFAULT_BOT_FIRST_LIFETIME : $this->config->get_bot_first_lifetime();
                    if ($this->_session_writes <= 1 && $bot_first_lifetime) {
                        $life_time = $bot_first_lifetime * (1 + $this->_session_writes);
                    } else {
                        $life_time = $bot_lifetime;
                    }
                }
            }
            // Use different lifetime for first write
            if ($life_time === null && $this->_session_writes <= 1) {
                $first_lifetime = is_null($this->config->get_first_lifetime()) ? self::DEFAULT_FIRST_LIFETIME : $this->config->get_first_lifetime();
                if ($first_lifetime) {
                    $life_time = $first_lifetime * (1 + $this->_session_writes);
                }
            }
            // Neither bot nor first write
            if ($life_time === null) {
                $life_time = $this->config->get_lifetime();
            }
            $this->_life_time = $life_time;
            if ($this->_life_time < $this->_min_lifetime) {
                $this->_life_time = $this->_min_lifetime;
            }
            if ($this->_life_time > $this->_max_lifetime) {
                $this->_life_time = $this->_max_lifetime;
            }
        }
        return $this->_life_time;
    }
    /**
     * Encode data
     *
     * @param string $data
     * @return string
     */
    protected function _encode_data($data)
    {
        $original_data_size = strlen($data);
        if ($this->_compression_threshold > 0 && $this->_compression_library != 'none' && $original_data_size >= $this->_compression_threshold) {
            $this->_log(sprintf('Compressing %s bytes with %s', $original_data_size, $this->_compression_library));
            $time_start = microtime(true);
            $prefix = ':' . substr($this->_compression_library, 0, 2) . ':';
            switch ($this->_compression_library) {
                case 'snappy':
                    $data = snappy_compress($data);
                    break;
                case 'lzf':
                    $data = lzf_compress($data);
                    break;
                case 'lz4':
                    $data = lz4_compress($data);
                    $prefix = ':l4:';
                    break;
                case 'gzip':
                    $data = gzcompress($data, 1);
                    break;
            }
            if ($data) {
                $data = $prefix . $data;
                $this->_log(sprintf('Data compressed by %.1f percent in %.5f seconds', $original_data_size == 0 ? 0 : 100 - strlen($data) / $original_data_size * 100, microtime(true) - $time_start));
            } else {
                $this->_log(sprintf('Could not compress session data using %s', $this->_compression_library), Logger_Interface::WARNING);
            }
        }
        return $data;
    }
    /**
     * Decode data
     *
     * @param string $data
     * @return string
     */
    protected function _decode_data($data)
    {
        switch (substr($data, 0, 4)) {
            // asking the data which library it uses allows for transparent changes of libraries
            case ':sn:':
                $data = snappy_uncompress(substr($data, 4));
                break;
            case ':lz:':
                $data = lzf_decompress(substr($data, 4));
                break;
            case ':l4:':
                $data = lz4_uncompress(substr($data, 4));
                break;
            case ':gz:':
                $data = gzuncompress(substr($data, 4));
                break;
        }
        return $data;
    }
    /**
     * Write session data to Redis
     *
     * @param $id
     * @param $data
     * @param $lifetime
     * @throws \Exception
     */
    protected function _write_raw_session(string $id, $data, $lifetime)
    {
        $session_id = 'sess_' . $id;
        if ($this->_use_pipeline) {
            $this->_redis->pipeline();
        }
        if (!$this->_use_cluster) {
            $this->_redis->select($this->_db_num);
        }
        $this->_redis->h_m_set($session_id, ['data' => $this->_encode_data($data), 'lock' => 0]);
        $this->_redis->h_incr_by($session_id, 'writes', 1);
        $this->_redis->expire($session_id, min((int) $lifetime, (int) $this->_max_lifetime));
        if ($this->_use_pipeline) {
            $this->_redis->exec();
        }
    }
    /**
     * Get pid
     */
    protected function _get_pid(): string
    {
        return gethostname() . '|' . getmypid();
    }
    /**
     * Check if pid exists
     *
     * @param $pid
     * @return bool
     */
    protected function _pid_exists($pid)
    {
        [$host, $pid] = explode('|', $pid);
        if (PHP_OS != 'Linux' || $host != gethostname()) {
            return true;
        }
        return @file_exists('/proc/' . $pid);
    }
    /**
     * Get break time, calculated later than other config settings due to requiring session name to be set
     *
     * @return int
     */
    protected function _get_break_after()
    {
        // Has break after already been calculated? Only fetch from config once, then reuse variable.
        if (!$this->_break_after) {
            // Fetch relevant setting from config using session name
            $this->_break_after = (float) ($this->config->get_break_after() ?: self::DEFAULT_BREAK_AFTER);
            // Use sleep time multiplier so break time is in seconds
            $this->_break_after = (int) round(1000000 / self::SLEEP_TIME * $this->_break_after);
        }
        return $this->_break_after;
    }
}