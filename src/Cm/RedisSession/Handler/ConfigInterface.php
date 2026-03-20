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
namespace Cm\Redis_Session\Handler;

interface Config_Interface
{
    /**
     * Get log level
     *
     * @return int
     */
    public function get_log_level();
    /**
     * Get host, can be absolute path if using unix socket
     *
     * @return string
     */
    public function get_host();
    /**
     * Get port
     *
     * @return int
     */
    public function get_port();
    /**
     * Get database number
     *
     * @return int
     */
    public function get_database();
    /**
     * Get password
     *
     * @return string
     */
    public function get_password();
    /**
     * Get connection timeout
     *
     * @return float
     */
    public function get_timeout();
    /**
     * Get connection retries
     *
     * @return float
     */
    public function get_retries();
    /**
     * Get unique string for persistent connections, if empty persistent connection is not used
     *
     * @return string
     */
    public function get_persistent_identifier();
    /**
     * Get compression threshold
     *
     * @return int
     */
    public function get_compression_threshold();
    /**
     * Get compression library (gzip, lzf, lz4 or snappy)
     *
     * @return string
     */
    public function get_compression_library();
    /**
     * Maximum number of processes that can wait for a lock on one session
     *
     * @return int
     */
    public function get_max_concurrency();
    /**
     * Get the normal session lifetime
     *
     * @return int
     */
    public function get_lifetime();
    /**
     * Get the maximum session lifetime
     *
     * @return int
     */
    public function get_max_lifetime();
    /**
     * Get the minimum session lifetime
     *
     * @return int
     */
    public function get_min_lifetime();
    /**
     * Disable session locking entirely
     *
     * @return bool
     */
    public function get_disable_locking();
    /**
     * Get lifetime of session for bots on subsequent writes, 0 to disable
     *
     * @return int
     */
    public function get_bot_lifetime();
    /**
     * Get lifetime of session for bots on the first write, 0 to disable
     *
     * @return int
     */
    public function get_bot_first_lifetime();
    /**
     * Get lifetime of session for non-bots on the first write, 0 to disable
     *
     * @return int
     */
    public function get_first_lifetime();
    /**
     * Get number of seconds to wait before trying to break the lock
     *
     * @return int
     */
    public function get_break_after();
    /**
     * Get number of seconds to wait before completely failing to break the lock
     *
     * @return int
     */
    public function get_fail_after();
    /**
     * Get list of redis sentinels
     *
     * @return string
     */
    public function get_sentinel_servers();
    /**
     * Get sentinel master name
     *
     * @return string
     */
    public function get_sentinel_master();
    /**
     * Verify master status flag
     *
     * @return string
     */
    public function get_sentinel_verify_master();
    /**
     * Connection retries for sentinels
     *
     * @return string
     */
    public function get_sentinel_connect_retries();
}